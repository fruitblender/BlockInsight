-- =============================================================================
-- schema/01_init_schemas_and_base_tables.sql
-- Reconstruction DDL for missing PostgreSQL schemas and base tables
-- =============================================================================

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS validation;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;

-- -----------------------------------------------------------------------------
-- Ingestion Meta Tables (public schema)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public.ingestion_batches (
    batch_id INTEGER PRIMARY KEY,
    dataset_name VARCHAR(128) NOT NULL,
    file_name VARCHAR(256) NOT NULL,
    status VARCHAR(64) NOT NULL,
    started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    row_count INTEGER
);

CREATE TABLE IF NOT EXISTS public.ingestion_files (
    file_id SERIAL PRIMARY KEY,
    batch_id INTEGER REFERENCES public.ingestion_batches(batch_id),
    dataset_name VARCHAR(128) NOT NULL,
    file_name VARCHAR(256) NOT NULL,
    status VARCHAR(64) NOT NULL,
    started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    row_count INTEGER
);

-- -----------------------------------------------------------------------------
-- Staging Layer Tables & Auto-Populate Triggers
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS staging.nodes_raw (
    node_id VARCHAR(64) PRIMARY KEY,
    ip VARCHAR(45),
    port INTEGER,
    country VARCHAR(64),
    asn VARCHAR(64),
    node_type VARCHAR(64),
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    batch_id INTEGER
);

CREATE OR REPLACE VIEW staging.nodes AS SELECT * FROM staging.nodes_raw;

CREATE TABLE IF NOT EXISTS staging.peer_connections_raw (
    connection_id VARCHAR(128) PRIMARY KEY,
    timestamp_start TIMESTAMPTZ,
    timestamp_end TIMESTAMPTZ,
    src_node_id VARCHAR(64),
    dst_node_id VARCHAR(64),
    src_ip VARCHAR(45),
    dst_ip VARCHAR(45),
    src_port INTEGER,
    dst_port INTEGER,
    direction VARCHAR(32),
    batch_id INTEGER
);

CREATE OR REPLACE VIEW staging.peer_connections AS SELECT * FROM staging.peer_connections_raw;

CREATE TABLE IF NOT EXISTS staging.transaction_observations_raw (
    observation_id VARCHAR(128) PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    observer_id VARCHAR(64),
    src_ip VARCHAR(45),
    src_port INTEGER,
    dst_ip VARCHAR(45),
    dst_port INTEGER,
    txid VARCHAR(64),
    message_type VARCHAR(64),
    peer_id VARCHAR(64),
    connection_id VARCHAR(128),
    propagation_delay_ms NUMERIC,
    direction VARCHAR(32),
    sequence_number INTEGER,
    batch_id INTEGER
);

CREATE OR REPLACE VIEW staging.transaction_observations AS SELECT * FROM staging.transaction_observations_raw;

CREATE TABLE IF NOT EXISTS staging.bitcoin_transactions_raw (
    txid VARCHAR(64) PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    inputs INTEGER,
    outputs INTEGER,
    fee NUMERIC,
    size INTEGER,
    ratio_fee_size NUMERIC,
    rarity_score NUMERIC,
    tema VARCHAR(128),
    description_ia TEXT,
    batch_id INTEGER,
    raw_data JSONB
);

-- Trigger to automatically construct raw_data JSONB from scalar columns inserted by load_bitcoin_transactions.py
CREATE OR REPLACE FUNCTION staging.populate_bitcoin_transactions_raw_json()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.raw_data IS NULL THEN
        NEW.raw_data := jsonb_build_object(
            'txid', NEW.txid,
            'timestamp', NEW.timestamp,
            'inputs', NEW.inputs,
            'outputs', NEW.outputs,
            'fee', NEW.fee,
            'size', NEW.size,
            'ratio_fee_size', NEW.ratio_fee_size,
            'rarity_score', NEW.rarity_score,
            'tema', NEW.tema,
            'description_ia', NEW.description_ia
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_populate_bitcoin_transactions_raw_json ON staging.bitcoin_transactions_raw;
CREATE TRIGGER trg_populate_bitcoin_transactions_raw_json
BEFORE INSERT OR UPDATE ON staging.bitcoin_transactions_raw
FOR EACH ROW
EXECUTE FUNCTION staging.populate_bitcoin_transactions_raw_json();

CREATE OR REPLACE VIEW staging.transactions_raw AS SELECT * FROM staging.bitcoin_transactions_raw;
CREATE OR REPLACE VIEW staging.transactions AS SELECT * FROM staging.bitcoin_transactions_raw;

CREATE TABLE IF NOT EXISTS staging.network_events_raw (
    event_id VARCHAR(128) PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    event_type VARCHAR(64),
    node_id VARCHAR(64),
    peer_id VARCHAR(64),
    connection_id VARCHAR(128),
    batch_id INTEGER
);

CREATE OR REPLACE VIEW staging.network_events AS SELECT * FROM staging.network_events_raw;

-- -----------------------------------------------------------------------------
-- Validation Layer Table
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS validation.quarantine (
    quarantine_id SERIAL PRIMARY KEY,
    batch_id INTEGER,
    source_table VARCHAR(128),
    record_id VARCHAR(128),
    validation_rule VARCHAR(128),
    error_message TEXT,
    raw_record JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Core Layer Tables
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.nodes (
    node_id VARCHAR(64) PRIMARY KEY,
    ip VARCHAR(45),
    port INTEGER,
    country VARCHAR(64),
    asn VARCHAR(64),
    node_type VARCHAR(64),
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    batch_id INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS core.transactions (
    txid VARCHAR(64) PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    input_count INTEGER,
    output_count INTEGER,
    fee NUMERIC(16, 8),
    size INTEGER,
    rarity_score NUMERIC(10, 6),
    ratio_fee_size NUMERIC(16, 8),
    tema VARCHAR(128),
    description_ia TEXT,
    batch_id INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS core.peer_connections (
    connection_id VARCHAR(128) PRIMARY KEY,
    timestamp_start TIMESTAMPTZ,
    timestamp_end TIMESTAMPTZ,
    src_node_id VARCHAR(64) REFERENCES core.nodes(node_id),
    dst_node_id VARCHAR(64) REFERENCES core.nodes(node_id),
    src_ip VARCHAR(45),
    dst_ip VARCHAR(45),
    src_port INTEGER,
    dst_port INTEGER,
    direction VARCHAR(32),
    batch_id INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS core.transaction_observations (
    observation_id VARCHAR(128) PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    observer_id VARCHAR(64) REFERENCES core.nodes(node_id),
    src_ip VARCHAR(45),
    src_port INTEGER,
    dst_ip VARCHAR(45),
    dst_port INTEGER,
    txid VARCHAR(64) REFERENCES core.transactions(txid),
    message_type VARCHAR(64),
    peer_id VARCHAR(64),
    connection_id VARCHAR(128),
    propagation_delay_ms NUMERIC(12, 4),
    direction VARCHAR(32),
    sequence_number INTEGER,
    batch_id INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS core.network_events (
    event_id VARCHAR(128) PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    event_type VARCHAR(64),
    node_id VARCHAR(64) REFERENCES core.nodes(node_id),
    peer_id VARCHAR(64),
    connection_id VARCHAR(128),
    batch_id INTEGER DEFAULT 1
);
