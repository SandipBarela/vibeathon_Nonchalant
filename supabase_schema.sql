-- ============================================================================
-- ResQMesh: Intelligent Humanitarian Logistics & Emergency Dispatch Platform
-- Production PostgreSQL DDL, Row Level Security, Indexes, Triggers, & Stored Procedures
-- ============================================================================

-- 0. EXTENSIONS & PREREQUISITES
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. DROP EXISTING OBJECTS (CLEAN RE-INIT)
DROP TRIGGER IF EXISTS trg_shelter_saturation_reroute ON shelters;
DROP FUNCTION IF EXISTS handle_shelter_saturation();
DROP FUNCTION IF EXISTS find_alternative_shelter(UUID, UUID);
DROP FUNCTION IF EXISTS sweep_stale_claims();
DROP FUNCTION IF EXISTS heartbeat_claim(UUID, UUID);
DROP FUNCTION IF EXISTS split_and_assign_batch(UUID, INT);

DROP TABLE IF EXISTS audit_logs CASCADE;
DROP TABLE IF EXISTS road_network_edges CASCADE;
DROP TABLE IF EXISTS sub_batches CASCADE;
DROP TABLE IF EXISTS batches CASCADE;
DROP TABLE IF EXISTS shelters CASCADE;

-- 2. TABLE DEFINITIONS

-- 2.1 SHELTERS
CREATE TABLE shelters (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    latitude NUMERIC(10, 7) NOT NULL,
    longitude NUMERIC(10, 7) NOT NULL,
    capacity_total INT NOT NULL CHECK (capacity_total >= 0),
    capacity_remaining INT NOT NULL CHECK (capacity_remaining >= 0),
    status TEXT NOT NULL CHECK (status IN ('open', 'filled', 'closed')) DEFAULT 'open',
    utilities JSONB NOT NULL DEFAULT '{"clean_water": true, "cooking_fuel": true}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2.2 BATCHES
CREATE TABLE batches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    donor_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    donor_name TEXT NOT NULL DEFAULT 'Emergency Relief Kitchen',
    cargo_type TEXT NOT NULL CHECK (cargo_type IN ('cooked_meals', 'raw_rations', 'infant_formula', 'medical_kits')),
    total_units INT NOT NULL CHECK (total_units > 0),
    shelf_life_minutes INT NOT NULL CHECK (shelf_life_minutes > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    dependencies JSONB NOT NULL DEFAULT '[]'::jsonb, -- e.g. ["clean_water", "cooking_fuel"]
    pickup_latitude NUMERIC(10, 7) NOT NULL DEFAULT 40.7128,
    pickup_longitude NUMERIC(10, 7) NOT NULL DEFAULT -74.0060,
    pickup_address TEXT NOT NULL DEFAULT 'Central Depot Hub A',
    status TEXT NOT NULL CHECK (status IN ('pending', 'split', 'completed', 'expired')) DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2.3 SUB-BATCHES (SCOOTER/BIKE MICRO-PAYLOADS <= 40 UNITS)
CREATE TABLE sub_batches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_batch_id UUID NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    units INT NOT NULL CHECK (units > 0 AND units <= 40),
    shelter_id UUID REFERENCES shelters(id) ON DELETE SET NULL,
    rider_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    rider_name TEXT NULL,
    claim_status TEXT NOT NULL CHECK (
        claim_status IN ('unassigned', 'reserved', 'in_transit', 'delivered', 'revoked', 'rerouted', 'at_risk')
    ) DEFAULT 'unassigned',
    claimed_at TIMESTAMPTZ NULL,
    heartbeat_at TIMESTAMPTZ NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    pickup_otp VARCHAR(4) NOT NULL,
    dropoff_otp VARCHAR(4) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2.4 ROAD NETWORK EDGES (OFFLINE GRAPH BACKUP)
CREATE TABLE road_network_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_node TEXT NOT NULL,
    to_node TEXT NOT NULL,
    distance_meters INT NOT NULL,
    elevation_meters INT NOT NULL DEFAULT 5,
    is_submerged BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2.5 AUDIT LOGS (TELEMETRY & CHAOS EXPERIMENTS)
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    actor_id UUID NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. INDEXES FOR HIGH-THROUGHPUT DISPATCH & TELEMETRY
CREATE INDEX idx_shelters_status_capacity ON shelters(status, capacity_remaining);
CREATE INDEX idx_sub_batches_claim_status ON sub_batches(claim_status);
CREATE INDEX idx_sub_batches_heartbeat ON sub_batches(claim_status, heartbeat_at);
CREATE INDEX idx_sub_batches_expires ON sub_batches(expires_at);
CREATE INDEX idx_sub_batches_parent ON sub_batches(parent_batch_id);
CREATE INDEX idx_sub_batches_shelter ON sub_batches(shelter_id);

-- 4. ROW LEVEL SECURITY (RLS) POLICIES
ALTER TABLE shelters ENABLE ROW LEVEL SECURITY;
ALTER TABLE batches ENABLE ROW LEVEL SECURITY;
ALTER TABLE sub_batches ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE road_network_edges ENABLE ROW LEVEL SECURITY;

-- Allow public read access to operational logistics data during disaster state
CREATE POLICY "Public read shelters" ON shelters FOR SELECT USING (true);
CREATE POLICY "Shelters admin update" ON shelters FOR ALL USING (true);

CREATE POLICY "Public read batches" ON batches FOR SELECT USING (true);
CREATE POLICY "Donors insert batches" ON batches FOR INSERT WITH CHECK (true);
CREATE POLICY "Donors update batches" ON batches FOR UPDATE USING (true);

CREATE POLICY "Public read sub_batches" ON sub_batches FOR SELECT USING (true);
CREATE POLICY "Riders update sub_batches" ON sub_batches FOR UPDATE USING (true);
CREATE POLICY "Dispatch insert sub_batches" ON sub_batches FOR INSERT WITH CHECK (true);

CREATE POLICY "Public read roads" ON road_network_edges FOR SELECT USING (true);
CREATE POLICY "Public read audit_logs" ON audit_logs FOR SELECT USING (true);
CREATE POLICY "Public insert audit_logs" ON audit_logs FOR INSERT WITH CHECK (true);

-- 5. STORED PROCEDURES & CORE RPCs

-- 5.1 Dynamic Splitter Engine
-- Fragments bulk donations into <= 40 units micro-payloads, allocates to open compatible shelters
CREATE OR REPLACE FUNCTION split_and_assign_batch(
    p_batch_id UUID,
    p_max_vehicle_capacity INT DEFAULT 40
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_batch RECORD;
    v_remaining_units INT;
    v_chunk_size INT;
    v_shelter RECORD;
    v_pickup_otp VARCHAR(4);
    v_dropoff_otp VARCHAR(4);
    v_assigned_count INT := 0;
    v_created_sub_batches JSONB := '[]'::jsonb;
    v_sub_id UUID;
BEGIN
    SELECT * INTO v_batch FROM batches WHERE id = p_batch_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Batch with ID % not found', p_batch_id;
    END IF;

    IF v_batch.status <> 'pending' THEN
        RAISE EXCEPTION 'Batch % is already processed (status: %)', p_batch_id, v_batch.status;
    END IF;

    v_remaining_units := v_batch.total_units;

    -- Iterate candidate open shelters compatible with batch dependencies
    FOR v_shelter IN
        SELECT s.id, s.name, s.capacity_remaining, s.utilities
        FROM shelters s
        WHERE s.status = 'open'
          AND s.capacity_remaining > 0
          -- Dependency match: All keys in batch.dependencies must exist and be TRUE in shelter.utilities
          AND (
            SELECT COALESCE(bool_and(COALESCE((s.utilities->>dep)::boolean, false)), true)
            FROM jsonb_array_elements_text(v_batch.dependencies) AS dep
          )
        ORDER BY s.capacity_remaining DESC
    LOOP
        EXIT WHEN v_remaining_units <= 0;

        WHILE v_shelter.capacity_remaining > 0 AND v_remaining_units > 0 LOOP
            -- Max chunk is 40 units or remaining capacity or remaining units
            v_chunk_size := LEAST(p_max_vehicle_capacity, v_remaining_units, v_shelter.capacity_remaining);

            -- Generate random 4-digit OTPs
            v_pickup_otp := LPAD(FLOOR(RANDOM() * 10000)::TEXT, 4, '0');
            v_dropoff_otp := LPAD(FLOOR(RANDOM() * 10000)::TEXT, 4, '0');
            v_sub_id := gen_random_uuid();

            INSERT INTO sub_batches (
                id,
                parent_batch_id,
                units,
                shelter_id,
                claim_status,
                expires_at,
                pickup_otp,
                dropoff_otp
            ) VALUES (
                v_sub_id,
                v_batch.id,
                v_chunk_size,
                v_shelter.id,
                'unassigned',
                v_batch.expires_at,
                v_pickup_otp,
                v_dropoff_otp
            );

            -- Decrement temporary capacity
            UPDATE shelters
            SET capacity_remaining = capacity_remaining - v_chunk_size,
                status = CASE WHEN capacity_remaining - v_chunk_size <= 0 THEN 'filled' ELSE 'open' END
            WHERE id = v_shelter.id;

            v_shelter.capacity_remaining := v_shelter.capacity_remaining - v_chunk_size;
            v_remaining_units := v_remaining_units - v_chunk_size;
            v_assigned_count := v_assigned_count + 1;

            v_created_sub_batches := v_created_sub_batches || jsonb_build_object(
                'sub_batch_id', v_sub_id,
                'units', v_chunk_size,
                'shelter_id', v_shelter.id,
                'shelter_name', v_shelter.name
            );
        END LOOP;
    END LOOP;

    -- Update parent batch status
    UPDATE batches
    SET status = CASE WHEN v_remaining_units = 0 THEN 'split' ELSE 'pending' END
    WHERE id = p_batch_id;

    -- Audit trail
    INSERT INTO audit_logs (event_type, payload)
    VALUES ('BATCH_SPLIT_AND_ASSIGN', jsonb_build_object(
        'batch_id', p_batch_id,
        'assigned_sub_batches', v_assigned_count,
        'unassigned_units_remaining', v_remaining_units
    ));

    RETURN jsonb_build_object(
        'success', true,
        'batch_id', p_batch_id,
        'sub_batches_created', v_assigned_count,
        'unassigned_units_remaining', v_remaining_units,
        'sub_batches', v_created_sub_batches
    );
END;
$$;

-- 5.2 Heartbeat Claim Revocation Engine
-- Transmits courier heartbeat and updates status
CREATE OR REPLACE FUNCTION heartbeat_claim(
    p_sub_batch_id UUID,
    p_rider_id UUID
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_sub RECORD;
BEGIN
    SELECT * INTO v_sub FROM sub_batches WHERE id = p_sub_batch_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('success', false, 'error', 'Sub-batch not found');
    END IF;

    IF v_sub.rider_id IS NOT NULL AND v_sub.rider_id <> p_rider_id THEN
        RETURN jsonb_build_object('success', false, 'error', 'Rider identity mismatch');
    END IF;

    UPDATE sub_batches
    SET heartbeat_at = NOW(),
        updated_at = NOW()
    WHERE id = p_sub_batch_id;

    RETURN jsonb_build_object('success', true, 'heartbeat_at', NOW());
END;
$$;

-- 5.3 Stale Reservation Sweeper (Dead-Man Engine)
-- Resets stalled reservations without heartbeats (> 5 mins) back to unassigned
CREATE OR REPLACE FUNCTION sweep_stale_claims()
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_revoked_count INT := 0;
    v_sub RECORD;
BEGIN
    FOR v_sub IN
        SELECT id, units, shelter_id, rider_id
        FROM sub_batches
        WHERE claim_status = 'reserved'
          AND (heartbeat_at IS NULL OR heartbeat_at < NOW() - INTERVAL '5 minutes')
          AND expires_at > NOW()
    LOOP
        UPDATE sub_batches
        SET claim_status = 'unassigned',
            rider_id = NULL,
            rider_name = NULL,
            claimed_at = NULL,
            heartbeat_at = NULL,
            updated_at = NOW()
        WHERE id = v_sub.id;

        v_revoked_count := v_revoked_count + 1;

        INSERT INTO audit_logs (event_type, payload, actor_id)
        VALUES ('DEAD_MAN_HEARTBEAT_REVOKED', jsonb_build_object(
            'sub_batch_id', v_sub.id,
            'former_rider_id', v_sub.rider_id,
            'reason', 'Heartbeat timeout > 5 minutes'
        ), v_sub.rider_id);
    END LOOP;

    RETURN jsonb_build_object(
        'success', true,
        'revoked_count', v_revoked_count,
        'timestamp', NOW()
    );
END;
$$;

-- 5.4 Dynamic Reroute Resolver
-- Finds alternative shelters when primary destination suffers mid-journey saturation or closure
CREATE OR REPLACE FUNCTION find_alternative_shelter(
    p_sub_batch_id UUID,
    p_exclude_shelter_id UUID
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_sub RECORD;
    v_parent RECORD;
    v_new_shelter RECORD;
BEGIN
    SELECT * INTO v_sub FROM sub_batches WHERE id = p_sub_batch_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('success', false, 'error', 'Sub-batch not found');
    END IF;

    SELECT * INTO v_parent FROM batches WHERE id = v_sub.parent_batch_id;

    -- Locate candidate open shelter with remaining capacity and utility match
    SELECT s.* INTO v_new_shelter
    FROM shelters s
    WHERE s.id <> p_exclude_shelter_id
      AND s.status = 'open'
      AND s.capacity_remaining >= v_sub.units
      AND (
        SELECT COALESCE(bool_and(COALESCE((s.utilities->>dep)::boolean, false)), true)
        FROM jsonb_array_elements_text(v_parent.dependencies) AS dep
      )
    ORDER BY s.capacity_remaining DESC
    LIMIT 1;

    IF FOUND THEN
        -- Reroute sub-batch
        UPDATE sub_batches
        SET shelter_id = v_new_shelter.id,
            claim_status = 'rerouted',
            updated_at = NOW()
        WHERE id = p_sub_batch_id;

        -- Decrement new shelter capacity
        UPDATE shelters
        SET capacity_remaining = capacity_remaining - v_sub.units,
            status = CASE WHEN capacity_remaining - v_sub.units <= 0 THEN 'filled' ELSE 'open' END
        WHERE id = v_new_shelter.id;

        -- Release capacity on original shelter if it was reserved
        UPDATE shelters
        SET capacity_remaining = LEAST(capacity_total, capacity_remaining + v_sub.units)
        WHERE id = p_exclude_shelter_id;

        INSERT INTO audit_logs (event_type, payload)
        VALUES ('DYNAMIC_REROUTE_SUCCESS', jsonb_build_object(
            'sub_batch_id', p_sub_batch_id,
            'old_shelter_id', p_exclude_shelter_id,
            'new_shelter_id', v_new_shelter.id,
            'new_shelter_name', v_new_shelter.name
        ));

        RETURN jsonb_build_object(
            'success', true,
            'rerouted', true,
            'new_shelter_id', v_new_shelter.id,
            'new_shelter_name', v_new_shelter.name
        );
    ELSE
        -- No viable alternative found; mark as AT RISK
        UPDATE sub_batches
        SET claim_status = 'at_risk',
            updated_at = NOW()
        WHERE id = p_sub_batch_id;

        INSERT INTO audit_logs (event_type, payload)
        VALUES ('DYNAMIC_REROUTE_FAILED_AT_RISK', jsonb_build_object(
            'sub_batch_id', p_sub_batch_id,
            'failed_shelter_id', p_exclude_shelter_id,
            'reason', 'No compatible alternative shelter with sufficient capacity and utilities'
        ));

        RETURN jsonb_build_object(
            'success', true,
            'rerouted', false,
            'status', 'at_risk',
            'warning', 'No compatible shelters found'
        );
    END IF;
END;
$$;

-- 5.5 Trigger: Intercept Shelter Saturation / Sudden Closure
CREATE OR REPLACE FUNCTION handle_shelter_saturation()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_in_transit RECORD;
BEGIN
    -- Detect saturation or sudden closure
    IF (NEW.capacity_remaining <= 0 OR NEW.status IN ('filled', 'closed')) AND 
       (OLD.capacity_remaining > 0 OR OLD.status = 'open') THEN
        
        -- Identify all couriers currently in transit to this saturated shelter
        FOR v_in_transit IN
            SELECT id
            FROM sub_batches
            WHERE shelter_id = NEW.id
              AND claim_status IN ('in_transit', 'reserved')
        LOOP
            -- Automatically seek alternative candidate
            PERFORM find_alternative_shelter(v_in_transit.id, NEW.id);
        END LOOP;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_shelter_saturation_reroute
AFTER UPDATE OF capacity_remaining, status ON shelters
FOR EACH ROW
EXECUTE FUNCTION handle_shelter_saturation();

-- 6. SEED DATA (REALISTIC HUMANITARIAN CRISIS SIMULATION)
INSERT INTO shelters (id, name, latitude, longitude, capacity_total, capacity_remaining, status, utilities) VALUES
('11111111-1111-1111-1111-111111111111', 'Red Cross Mercy High Shelter', 40.730610, -73.935242, 250, 140, 'open', '{"clean_water": true, "cooking_fuel": true}'::jsonb),
('22222222-2222-2222-2222-222222222222', 'St. Jude Community Hall', 40.741895, -73.989308, 120, 25, 'open', '{"clean_water": true, "cooking_fuel": false}'::jsonb),
('33333333-3333-3333-3333-333333333333', 'Harbor Evacuation Haven A', 40.718217, -74.007801, 300, 190, 'open', '{"clean_water": true, "cooking_fuel": true}'::jsonb),
('44444444-4444-4444-4444-444444444444', 'Metro Civic Gymnasium B', 40.758896, -73.985130, 80, 0, 'filled', '{"clean_water": false, "cooking_fuel": false}'::jsonb),
('55555555-5555-5555-5555-555555555555', 'East River Relief Station 4', 40.728157, -73.971249, 150, 95, 'open', '{"clean_water": true, "cooking_fuel": true}'::jsonb);

INSERT INTO batches (id, donor_name, cargo_type, total_units, shelf_life_minutes, expires_at, dependencies, pickup_latitude, pickup_longitude, pickup_address, status) VALUES
('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'Grand Central Central Kitchen', 'cooked_meals', 120, 75, NOW() + INTERVAL '75 minutes', '["clean_water", "cooking_fuel"]'::jsonb, 40.752726, -73.977229, 'Vanderbilt Ave & 42nd St', 'split'),
('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'Downtown Community Pantry', 'raw_rations', 80, 240, NOW() + INTERVAL '240 minutes', '["clean_water"]'::jsonb, 40.711200, -74.012500, 'Greenwich St & Liberty St', 'pending');

-- Micro-payload sub-batches for Batch A
INSERT INTO sub_batches (id, parent_batch_id, units, shelter_id, rider_name, claim_status, expires_at, pickup_otp, dropoff_otp, claimed_at, heartbeat_at) VALUES
('sub00001-0000-0000-0000-000000000001', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 40, '11111111-1111-1111-1111-111111111111', 'Courier Marcus (Bike 12)', 'in_transit', NOW() + INTERVAL '62 minutes', '4821', '8912', NOW() - INTERVAL '12 minutes', NOW() - INTERVAL '15 seconds'),
('sub00002-0000-0000-0000-000000000002', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 40, '33333333-3333-3333-3333-333333333333', 'Courier Sarah (Scooter 04)', 'in_transit', NOW() + INTERVAL '58 minutes', '7104', '3341', NOW() - INTERVAL '18 minutes', NOW() - INTERVAL '40 seconds'),
('sub00003-0000-0000-0000-000000000003', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 40, '55555555-5555-5555-5555-555555555555', NULL, 'unassigned', NOW() + INTERVAL '75 minutes', '9028', '1182', NULL, NULL);

-- Seed Roadway Network Nodes for Offline Dijkstra solver
INSERT INTO road_network_edges (from_node, to_node, distance_meters, elevation_meters, is_submerged) VALUES
('HUB_CENTRAL', 'NODE_BROADWAY_42', 450, 8, false),
('NODE_BROADWAY_42', 'NODE_LEXINGTON_34', 600, 7, false),
('NODE_LEXINGTON_34', 'SHELTER_RED_CROSS', 1200, 4, false),
('NODE_BROADWAY_42', 'NODE_9TH_AVE_CANAL', 1500, 2, false),
('NODE_9TH_AVE_CANAL', 'SHELTER_HARBOR_HAVEN', 800, 3, false),
('NODE_LEXINGTON_34', 'SHELTER_EAST_RIVER', 950, 1, false);

-- Enable Realtime publication for Supabase Realtime listeners
ALTER PUBLICATION supabase_realtime ADD TABLE shelters, batches, sub_batches, audit_logs;
