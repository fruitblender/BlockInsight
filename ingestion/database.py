import psycopg


def get_connection():
    return psycopg.connect(
        host="172.23.8.56",
        port=5432,
        dbname="bitcoin_monitor",
        user="postgres",
        password="1305"
    )


def initialize_database():
    """Create analytics tables required by operational inference and alerts."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;

        CREATE TABLE IF NOT EXISTS analytics.node_risk_scores (
            batch_id INTEGER NOT NULL,
            node_id VARCHAR(64) NOT NULL,
            risk_score NUMERIC(6, 2) NOT NULL,
            confidence NUMERIC(5, 3) NOT NULL,
            risk_level VARCHAR(20) NOT NULL,
            explanation TEXT NOT NULL,
            shap_factors JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, node_id)
        );

        CREATE TABLE IF NOT EXISTS analytics.transaction_risk_scores (
            batch_id INTEGER NOT NULL,
            txid VARCHAR(64) NOT NULL,
            risk_score NUMERIC(6, 2) NOT NULL,
            confidence NUMERIC(5, 3) NOT NULL,
            risk_level VARCHAR(20) NOT NULL,
            explanation TEXT NOT NULL,
            shap_factors JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, txid)
        );

        CREATE TABLE IF NOT EXISTS analytics.node_clusters (
            batch_id INTEGER NOT NULL,
            node_id VARCHAR(64) NOT NULL,
            cluster_id INTEGER NOT NULL,
            is_noise BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, node_id)
        );

        CREATE TABLE IF NOT EXISTS analytics.cluster_summaries (
            batch_id INTEGER NOT NULL,
            cluster_id INTEGER NOT NULL,
            node_count INTEGER NOT NULL,
            is_noise BOOLEAN NOT NULL,
            avg_peer_degree NUMERIC(10, 2),
            avg_connections NUMERIC(10, 2),
            avg_observer_delay_ms NUMERIC(12, 3),
            top_country VARCHAR(10),
            top_node_type VARCHAR(20),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, cluster_id)
        );

        CREATE TABLE IF NOT EXISTS analytics.node_graph_metrics (
            batch_id INTEGER NOT NULL,
            node_id VARCHAR(64) NOT NULL,
            degree_centrality NUMERIC(10, 6) NOT NULL,
            betweenness_centrality NUMERIC(10, 6) NOT NULL,
            closeness_centrality NUMERIC(10, 6) NOT NULL,
            community_id INTEGER NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, node_id)
        );

        CREATE TABLE IF NOT EXISTS analytics.model_registry (
            model_id VARCHAR(64) NOT NULL PRIMARY KEY,
            model_name VARCHAR(100) NOT NULL,
            model_version VARCHAR(20) NOT NULL,
            algorithm VARCHAR(50) NOT NULL,
            target_entity VARCHAR(50) NOT NULL,
            hyperparameters JSONB NOT NULL,
            feature_names JSONB NOT NULL,
            metrics JSONB NOT NULL,
            artifact_path VARCHAR(255) NOT NULL,
            is_production BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS analytics.alerts (
            alert_id VARCHAR(64) NOT NULL PRIMARY KEY,
            batch_id INTEGER NOT NULL,
            entity_type VARCHAR(20) NOT NULL,
            entity_id VARCHAR(64) NOT NULL,
            risk_score NUMERIC(6, 2) NOT NULL,
            confidence NUMERIC(5, 3) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            title VARCHAR(255) NOT NULL,
            description TEXT NOT NULL,
            evidence JSONB NOT NULL,
            status VARCHAR(30) NOT NULL DEFAULT 'OPEN',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_alerts_risk
            ON analytics.alerts (risk_score DESC, confidence DESC);
        CREATE INDEX IF NOT EXISTS idx_alerts_entity
            ON analytics.alerts (entity_type, entity_id);
    """)
    conn.commit()
    cur.close()
    conn.close()
