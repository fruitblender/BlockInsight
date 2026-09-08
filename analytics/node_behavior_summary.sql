-- ============================================================
-- Phase 5 - Step 2: Node Behavior Analytical Summary
-- View: analytics.node_behavior_summary
-- Grain: ONE ROW PER (batch_id, node_id)
-- Base: core.nodes (Preserves all 150 nodes in Batch 1)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.node_behavior_summary AS
WITH conn_edges AS (
    SELECT
        batch_id,
        src_node_id AS node_id,
        dst_node_id AS peer_id,
        ROUND((EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000)::numeric, 3) AS duration_ms,
        1 AS is_outgoing,
        0 AS is_incoming
    FROM core.peer_connections
    UNION ALL
    SELECT
        batch_id,
        dst_node_id AS node_id,
        src_node_id AS peer_id,
        ROUND((EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000)::numeric, 3) AS duration_ms,
        0 AS is_outgoing,
        1 AS is_incoming
    FROM core.peer_connections
),
conn_agg AS (
    SELECT
        batch_id,
        node_id,
        SUM(is_outgoing) AS outgoing_connections,
        SUM(is_incoming) AS incoming_connections,
        COUNT(*) AS total_connections,
        COUNT(DISTINCT peer_id) AS peer_degree,
        ROUND(AVG(duration_ms), 3) AS avg_connection_duration_ms,
        MIN(duration_ms) AS min_connection_duration_ms,
        MAX(duration_ms) AS max_connection_duration_ms
    FROM conn_edges
    GROUP BY batch_id, node_id
),
obs_agg AS (
    SELECT
        batch_id,
        observer_id AS node_id,
        COUNT(*) AS observations_as_observer,
        COUNT(DISTINCT txid) AS transactions_observed,
        COUNT(DISTINCT peer_id) AS unique_peers_as_observer,
        ROUND(AVG(propagation_delay_ms), 3) AS avg_observer_delay_ms,
        MIN(propagation_delay_ms) AS min_observer_delay_ms,
        MAX(propagation_delay_ms) AS max_observer_delay_ms
    FROM core.transaction_observations
    GROUP BY batch_id, observer_id
),
peer_agg AS (
    SELECT
        batch_id,
        peer_id AS node_id,
        COUNT(*) AS propagations_as_peer,
        COUNT(DISTINCT txid) AS transactions_propagated,
        COUNT(DISTINCT observer_id) AS unique_observers_as_peer
    FROM core.transaction_observations
    WHERE peer_id IS NOT NULL
    GROUP BY batch_id, peer_id
)
SELECT
    n.batch_id,
    n.node_id,
    n.ip,
    n.port,
    n.country,
    n.asn,
    n.node_type,

    -- Metric Group 1: Topology / Degree
    COALESCE(c.outgoing_connections, 0) AS outgoing_connections,
    COALESCE(c.incoming_connections, 0) AS incoming_connections,
    COALESCE(c.total_connections, 0) AS total_connections,
    COALESCE(c.peer_degree, 0) AS peer_degree,
    c.avg_connection_duration_ms,
    c.min_connection_duration_ms,
    c.max_connection_duration_ms,

    -- Metric Group 2: Observer Activity
    COALESCE(o.observations_as_observer, 0) AS observations_as_observer,
    COALESCE(o.transactions_observed, 0) AS transactions_observed,
    COALESCE(o.unique_peers_as_observer, 0) AS unique_peers_as_observer,
    o.avg_observer_delay_ms,
    o.min_observer_delay_ms,
    o.max_observer_delay_ms,

    -- Metric Group 3: Propagation Activity
    COALESCE(p.propagations_as_peer, 0) AS propagations_as_peer,
    COALESCE(p.transactions_propagated, 0) AS transactions_propagated,
    COALESCE(p.unique_observers_as_peer, 0) AS unique_observers_as_peer,

    -- Metric Group 4: Activity Ratio
    ROUND(
        (COALESCE(p.propagations_as_peer, 0)::numeric / NULLIF(o.observations_as_observer, 0)),
        4
    ) AS propagation_to_observation_ratio

FROM core.nodes n
LEFT JOIN conn_agg c
    ON n.node_id = c.node_id AND n.batch_id = c.batch_id
LEFT JOIN obs_agg o
    ON n.node_id = o.node_id AND n.batch_id = o.batch_id
LEFT JOIN peer_agg p
    ON n.node_id = p.node_id AND n.batch_id = p.batch_id;
