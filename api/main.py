"""
Step 8: FastAPI Backend (SRS §34)
Exposes RESTful endpoints for the Bitcoin Monitor React Dashboard.
Serves analytical summaries, ranked alerts, entity profiles, transaction propagation timelines,
graph topologies (for interactive link analysis), and model registry metadata.
"""
import sys
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# Add ingestion directory to path for database connection
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1

app = FastAPI(
    title="Bitcoin Monitor Analytical API",
    description="Offline Bitcoin P2P Network Propagation & Anomaly Detection REST Service",
    version="1.0.0"
)

# Allow React dashboard frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "service": "Bitcoin Monitor Analytical API",
        "status": "online",
        "batch_id": BATCH_ID,
        "docs_url": "/docs"
    }


@app.get("/api/overview")
def get_overview():
    """Returns high-level KPI metrics for the Executive Overview dashboard."""
    conn = get_connection()
    cur = conn.cursor()

    # Total nodes
    cur.execute("SELECT COUNT(*) FROM core.nodes WHERE batch_id = %s;", (BATCH_ID,))
    total_nodes = cur.fetchone()[0]

    # Total transactions
    cur.execute("SELECT COUNT(*) FROM core.transactions WHERE batch_id = %s;", (BATCH_ID,))
    total_txs = cur.fetchone()[0]

    # Total observations
    cur.execute("SELECT COUNT(*) FROM core.transaction_observations WHERE batch_id = %s;", (BATCH_ID,))
    total_obs = cur.fetchone()[0]

    # Alert counts
    cur.execute("""
        SELECT 
            COUNT(*) AS total_alerts,
            COUNT(*) FILTER (WHERE severity = 'CRITICAL') AS critical_alerts,
            COUNT(*) FILTER (WHERE severity = 'HIGH') AS high_alerts,
            COUNT(*) FILTER (WHERE entity_type = 'NODE') AS node_alerts,
            COUNT(*) FILTER (WHERE entity_type = 'TRANSACTION') AS tx_alerts
        FROM analytics.alerts
        WHERE batch_id = %s;
    """, (BATCH_ID,))
    alert_row = cur.fetchone()

    # Global propagation stats
    cur.execute("""
        SELECT 
            ROUND(AVG(avg_propagation_delay_ms), 2),
            ROUND(AVG(observed_propagation_span_ms), 2)
        FROM analytics.transaction_propagation_summary
        WHERE batch_id = %s;
    """, (BATCH_ID,))
    prop_stats = cur.fetchone()

    # Cluster / Community counts
    cur.execute("SELECT COUNT(DISTINCT community_id) FROM analytics.node_features WHERE batch_id = %s;", (BATCH_ID,))
    comm_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT community_id) FROM analytics.node_features WHERE batch_id = %s AND community_id >= 0;", (BATCH_ID,))
    cluster_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    return {
        "batch_id": BATCH_ID,
        "network": {
            "total_nodes": total_nodes,
            "total_transactions": total_txs,
            "total_observations": total_obs,
            "communities_count": comm_count,
            "dbscan_clusters_count": cluster_count
        },
        "alerts": {
            "total": alert_row[0] if alert_row else 0,
            "critical": alert_row[1] if alert_row else 0,
            "high": alert_row[2] if alert_row else 0,
            "node_alerts": alert_row[3] if alert_row else 0,
            "transaction_alerts": alert_row[4] if alert_row else 0
        },
        "propagation": {
            "avg_propagation_delay_ms": float(prop_stats[0]) if prop_stats and prop_stats[0] else 0.0,
            "avg_propagation_span_ms": float(prop_stats[1]) if prop_stats and prop_stats[1] else 0.0
        }
    }


@app.get("/api/alerts")
def get_alerts(
    severity: Optional[str] = None,
    entity_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Returns ranked list of alerts filtered by severity and entity type."""
    conn = get_connection()
    cur = conn.cursor()

    conditions = ["batch_id = %s"]
    params = [BATCH_ID]

    if severity:
        conditions.append("severity = %s")
        params.append(severity.upper())
    if entity_type:
        conditions.append("entity_type = %s")
        params.append(entity_type.upper())

    where_clause = " AND ".join(conditions)
    query = f"""
        SELECT 
            alert_id, entity_type, entity_id, risk_score, confidence,
            severity, title, description, status, created_at, evidence
        FROM analytics.alerts
        WHERE {where_clause}
        ORDER BY risk_score DESC, confidence DESC
        LIMIT %s OFFSET %s;
    """
    params.extend([limit, offset])

    cur.execute(query, tuple(params))
    rows = cur.fetchall()

    alerts = []
    for r in rows:
        alerts.append({
            "alert_id": r[0],
            "entity_type": r[1],
            "entity_id": r[2],
            "risk_score": float(r[3]),
            "confidence": float(r[4]),
            "severity": r[5],
            "title": r[6],
            "description": r[7],
            "status": r[8],
            "created_at": r[9].isoformat() if r[9] else None,
            "evidence": r[10]
        })

    cur.close()
    conn.close()
    return {"count": len(alerts), "alerts": alerts}


@app.get("/api/alerts/{alert_id}")
def get_alert_detail(alert_id: str):
    """Returns full alert detail and evidence."""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            alert_id, entity_type, entity_id, risk_score, confidence,
            severity, title, description, status, created_at, evidence
        FROM analytics.alerts
        WHERE alert_id = %s;
    """, (alert_id,))
    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Alert not found")

    return {
        "alert_id": row[0],
        "entity_type": row[1],
        "entity_id": row[2],
        "risk_score": float(row[3]),
        "confidence": float(row[4]),
        "severity": row[5],
        "title": row[6],
        "description": row[7],
        "status": row[8],
        "created_at": row[9].isoformat() if row[9] else None,
        "evidence": row[10]
    }


@app.get("/api/nodes")
def get_nodes(limit: int = 150, offset: int = 0):
    """Returns list of nodes with their behavioral risk scores and topology metrics."""
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT 
            nf.node_id, nf.ip, nf.country, nf.asn, nf.node_type,
            nf.peer_degree, nf.total_connections, nf.observations_as_observer,
            nf.propagations_as_peer, nf.avg_observer_delay_ms,
            COALESCE(nrs.risk_score, 0.0) AS risk_score,
            COALESCE(nrs.confidence, 0.0) AS confidence,
            COALESCE(nrs.risk_level, 'LOW') AS risk_level,
            COALESCE(nf.community_id, 0) AS cluster_id,
            COALESCE(nf.community_id, 0) AS community_id,
            COALESCE(nf.degree_centrality, 0.0) AS degree_centrality
        FROM analytics.node_features nf
        LEFT JOIN analytics.node_risk_scores nrs ON nf.node_id = nrs.node_id AND nf.batch_id = nrs.batch_id
        WHERE nf.batch_id = %s
        ORDER BY risk_score DESC
        LIMIT %s OFFSET %s;
    """
    cur.execute(query, (BATCH_ID, limit, offset))
    rows = cur.fetchall()

    nodes = []
    for r in rows:
        nodes.append({
            "node_id": r[0],
            "ip": r[1],
            "country": r[2],
            "asn": r[3],
            "node_type": r[4],
            "peer_degree": r[5],
            "total_connections": r[6],
            "observations_as_observer": r[7],
            "propagations_as_peer": r[8],
            "avg_observer_delay_ms": float(r[9]),
            "risk_score": float(r[10]),
            "confidence": float(r[11]),
            "risk_level": r[12],
            "cluster_id": r[13],
            "community_id": r[14],
            "degree_centrality": float(r[15])
        })

    cur.close()
    conn.close()
    return {"count": len(nodes), "nodes": nodes}


@app.get("/api/nodes/{node_id}")
def get_node_detail(node_id: str):
    """Returns comprehensive profile for a node, including SHAP explanation, neighbors, and observations."""
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT 
            nf.node_id, nf.ip, nf.country, nf.asn, nf.node_type,
            nf.peer_degree, nf.outgoing_connections, nf.incoming_connections,
            nf.total_connections, nf.avg_connection_duration_ms,
            nf.observations_as_observer, nf.transactions_observed,
            nf.unique_peers_as_observer, nf.avg_observer_delay_ms,
            nf.propagations_as_peer, nf.transactions_propagated,
            nf.unique_observers_as_peer, nf.propagation_observation_ratio,
            nf.ip_diversity,
            COALESCE(nrs.risk_score, 0.0) AS risk_score,
            COALESCE(nrs.confidence, 0.0) AS confidence,
            COALESCE(nrs.risk_level, 'LOW') AS risk_level,
            COALESCE(nrs.explanation, 'Normal behavior') AS explanation,
            nrs.shap_factors,
            COALESCE(nf.community_id, 0) AS cluster_id,
            FALSE AS is_noise,
            COALESCE(nf.degree_centrality, 0.0) AS degree_centrality,
            COALESCE(nf.betweenness_centrality, 0.0) AS betweenness_centrality,
            COALESCE(nf.closeness_centrality, 0.0) AS closeness_centrality,
            COALESCE(nf.community_id, 0) AS community_id
        FROM analytics.node_features nf
        LEFT JOIN analytics.node_risk_scores nrs ON nf.node_id = nrs.node_id AND nf.batch_id = nrs.batch_id
        WHERE nf.node_id = %s AND nf.batch_id = %s;
    """
    cur.execute(query, (node_id, BATCH_ID))
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Node not found")

    # Fetch neighbors (connected peers)
    cur.execute("""
        SELECT 
            dst_node_id AS peer_id, dst_ip AS peer_ip, 'OUTGOING' AS direction
        FROM core.peer_connections
        WHERE src_node_id = %s AND batch_id = %s
        UNION
        SELECT 
            src_node_id AS peer_id, src_ip AS peer_ip, 'INCOMING' AS direction
        FROM core.peer_connections
        WHERE dst_node_id = %s AND batch_id = %s;
    """, (node_id, BATCH_ID, node_id, BATCH_ID))
    neighbors = [{"peer_id": r[0], "peer_ip": r[1], "direction": r[2]} for r in cur.fetchall()]

    cur.close()
    conn.close()

    return {
        "node_id": row[0],
        "ip": row[1],
        "country": row[2],
        "asn": row[3],
        "node_type": row[4],
        "topology": {
            "peer_degree": row[5],
            "outgoing_connections": row[6],
            "incoming_connections": row[7],
            "total_connections": row[8],
            "avg_connection_duration_ms": float(row[9]),
            "degree_centrality": float(row[26]),
            "betweenness_centrality": float(row[27]),
            "closeness_centrality": float(row[28]),
            "community_id": row[29]
        },
        "behavior": {
            "observations_as_observer": row[10],
            "transactions_observed": row[11],
            "unique_peers_as_observer": row[12],
            "avg_observer_delay_ms": float(row[13]),
            "propagations_as_peer": row[14],
            "transactions_propagated": row[15],
            "unique_observers_as_peer": row[16],
            "propagation_observation_ratio": float(row[17]),
            "ip_diversity": row[18]
        },
        "risk": {
            "risk_score": float(row[19]),
            "confidence": float(row[20]),
            "risk_level": row[21],
            "explanation": row[22],
            "shap_factors": row[23] if isinstance(row[23], dict) else (row[23] if row[23] else {}),
            "cluster_id": row[24],
            "is_cluster_noise": row[25]
        },
        "neighbors": neighbors
    }


@app.get("/api/transactions")
def get_transactions(limit: int = 50, offset: int = 0):
    """Returns list of transactions with propagation summary and risk scores."""
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT 
            tf.txid, tf.fee, tf.size, tf.ratio_fee_size, tf.rarity_score,
            tf.total_observations, tf.unique_observers, tf.unique_peers,
            tf.avg_propagation_delay_ms, tf.observed_propagation_span_ms,
            COALESCE(trs.risk_score, 0.0) AS risk_score,
            COALESCE(trs.confidence, 0.0) AS confidence,
            COALESCE(trs.risk_level, 'LOW') AS risk_level
        FROM analytics.transaction_features tf
        LEFT JOIN analytics.transaction_risk_scores trs ON tf.txid = trs.txid AND tf.batch_id = trs.batch_id
        WHERE tf.batch_id = %s
        ORDER BY risk_score DESC
        LIMIT %s OFFSET %s;
    """
    cur.execute(query, (BATCH_ID, limit, offset))
    rows = cur.fetchall()

    txs = []
    for r in rows:
        txs.append({
            "txid": r[0],
            "fee": float(r[1]),
            "size": r[2],
            "ratio_fee_size": float(r[3]),
            "rarity_score": float(r[4]),
            "total_observations": r[5],
            "unique_observers": r[6],
            "unique_peers": r[7],
            "avg_propagation_delay_ms": float(r[8]),
            "observed_propagation_span_ms": float(r[9]),
            "risk_score": float(r[10]),
            "confidence": float(r[11]),
            "risk_level": r[12]
        })

    cur.close()
    conn.close()
    return {"count": len(txs), "transactions": txs}


@app.get("/api/transactions/{txid}")
def get_transaction_detail(txid: str):
    """Returns full transaction propagation details and chronological observations timeline."""
    conn = get_connection()
    cur = conn.cursor()

    # Fetch transaction metadata and risk
    cur.execute("""
        SELECT 
            tf.txid, tf.fee, tf.size, tf.ratio_fee_size, tf.rarity_score,
            tf.inputs, tf.outputs, tf.total_observations, tf.unique_observers,
            tf.unique_peers, tf.unique_observer_countries, tf.unique_peer_countries,
            tf.min_propagation_delay_ms, tf.max_propagation_delay_ms,
            tf.avg_propagation_delay_ms, tf.median_propagation_delay_ms,
            tf.observed_propagation_span_ms, tf.creation_to_first_observation_ms,
            tf.inv_ratio, tf.tx_ratio,
            COALESCE(trs.risk_score, 0.0) AS risk_score,
            COALESCE(trs.confidence, 0.0) AS confidence,
            COALESCE(trs.risk_level, 'LOW') AS risk_level,
            COALESCE(trs.explanation, 'Normal propagation') AS explanation,
            trs.shap_factors
        FROM analytics.transaction_features tf
        LEFT JOIN analytics.transaction_risk_scores trs ON tf.txid = trs.txid AND tf.batch_id = trs.batch_id
        WHERE tf.txid = %s AND tf.batch_id = %s;
    """, (txid, BATCH_ID))
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Transaction not found")

    # Fetch chronological propagation timeline
    cur.execute("""
        SELECT 
            observation_id, sequence_number, observation_timestamp,
            observer_id, observer_ip, observer_country, observer_node_type,
            peer_id, peer_ip, peer_country, peer_node_type,
            propagation_delay_ms, message_type
        FROM analytics.transaction_propagation
        WHERE txid = %s AND batch_id = %s
        ORDER BY sequence_number ASC, observation_timestamp ASC;
    """, (txid, BATCH_ID))
    obs_rows = cur.fetchall()

    timeline = []
    for o in obs_rows:
        timeline.append({
            "observation_id": o[0],
            "sequence_number": o[1],
            "observation_timestamp": o[2].isoformat() if o[2] else None,
            "observer_id": o[3],
            "observer_ip": o[4],
            "observer_country": o[5],
            "observer_node_type": o[6],
            "peer_id": o[7],
            "peer_ip": o[8],
            "peer_country": o[9],
            "peer_node_type": o[10],
            "propagation_delay_ms": float(o[11]) if o[11] else 0.0,
            "message_type": o[12]
        })

    cur.close()
    conn.close()

    return {
        "txid": row[0],
        "fee": float(row[1]),
        "size": row[2],
        "ratio_fee_size": float(row[3]),
        "rarity_score": float(row[4]),
        "inputs": row[5],
        "outputs": row[6],
        "propagation_metrics": {
            "total_observations": row[7],
            "unique_observers": row[8],
            "unique_peers": row[9],
            "unique_observer_countries": row[10],
            "unique_peer_countries": row[11],
            "min_propagation_delay_ms": float(row[12]),
            "max_propagation_delay_ms": float(row[13]),
            "avg_propagation_delay_ms": float(row[14]),
            "median_propagation_delay_ms": float(row[15]),
            "observed_propagation_span_ms": float(row[16]),
            "creation_to_first_observation_ms": float(row[17]),
            "inv_ratio": float(row[18]),
            "tx_ratio": float(row[19])
        },
        "risk": {
            "risk_score": float(row[20]),
            "confidence": float(row[21]),
            "risk_level": row[22],
            "explanation": row[23],
            "shap_factors": row[24] if isinstance(row[24], dict) else (row[24] if row[24] else {})
        },
        "timeline": timeline
    }


@app.get("/api/graph")
def get_graph_data(limit_nodes: int = 150):
    """Returns nodes and links formatted for interactive force-directed graph visualization (react-force-graph)."""
    conn = get_connection()
    cur = conn.cursor()

    # Get nodes
    cur.execute("""
        SELECT 
            nf.node_id, nf.ip, nf.country, nf.node_type,
            COALESCE(nrs.risk_score, 0.0) AS risk_score,
            COALESCE(nrs.risk_level, 'LOW') AS risk_level,
            COALESCE(nf.community_id, 0) AS community_id,
            COALESCE(nf.degree_centrality, 0.0) AS degree_centrality
        FROM analytics.node_features nf
        LEFT JOIN analytics.node_risk_scores nrs ON nf.node_id = nrs.node_id AND nf.batch_id = nrs.batch_id
        WHERE nf.batch_id = %s
        LIMIT %s;
    """, (BATCH_ID, limit_nodes))
    node_rows = cur.fetchall()

    graph_nodes = []
    valid_node_ids = set()
    for r in node_rows:
        valid_node_ids.add(r[0])
        graph_nodes.append({
            "id": r[0],
            "name": f"{r[0]} ({r[1]})",
            "ip": r[1],
            "country": r[2],
            "node_type": r[3],
            "risk_score": float(r[4]),
            "risk_level": r[5],
            "community_id": r[6],
            "val": max(2.0, float(r[7]) * 20.0)  # sizing
        })

    # Get edges from peer_connections
    cur.execute("""
        SELECT src_node_id, dst_node_id
        FROM core.peer_connections
        WHERE batch_id = %s;
    """, (BATCH_ID,))
    edge_rows = cur.fetchall()

    graph_links = []
    for src, dst in edge_rows:
        if src in valid_node_ids and dst in valid_node_ids:
            graph_links.append({
                "source": src,
                "target": dst,
                "type": "connected_to"
            })

    cur.close()
    conn.close()

    return {
        "nodes": graph_nodes,
        "links": graph_links
    }


@app.get("/api/clusters")
def get_clusters():
    """Returns cluster summaries from DBSCAN or community aggregations from node_features."""
    conn = get_connection()
    cur = conn.cursor()
    clusters = []

    try:
        cur.execute("""
            SELECT 
                cluster_id, node_count, is_noise, avg_peer_degree,
                avg_connections, avg_observer_delay_ms, top_country, top_node_type
            FROM analytics.cluster_summaries
            WHERE batch_id = %s
            ORDER BY cluster_id ASC;
        """, (BATCH_ID,))
        rows = cur.fetchall()
        for r in rows:
            clusters.append({
                "cluster_id": r[0],
                "node_count": r[1],
                "is_noise": r[2],
                "avg_peer_degree": float(r[3]) if r[3] else 0.0,
                "avg_connections": float(r[4]) if r[4] else 0.0,
                "avg_observer_delay_ms": float(r[5]) if r[5] else 0.0,
                "top_country": r[6],
                "top_node_type": r[7]
            })
    except Exception:
        conn.rollback()
        # Fallback: derive community cluster summaries from analytics.node_features
        try:
            cur.execute("""
                SELECT 
                    community_id AS cluster_id,
                    COUNT(*) AS node_count,
                    FALSE AS is_noise,
                    ROUND(AVG(peer_degree), 2) AS avg_peer_degree,
                    ROUND(AVG(total_connections), 2) AS avg_connections,
                    ROUND(AVG(avg_observer_delay_ms), 3) AS avg_observer_delay_ms,
                    MODE() WITHIN GROUP (ORDER BY country) AS top_country,
                    MODE() WITHIN GROUP (ORDER BY node_type) AS top_node_type
                FROM analytics.node_features
                WHERE batch_id = %s
                GROUP BY community_id
                ORDER BY community_id ASC;
            """, (BATCH_ID,))
            rows = cur.fetchall()
            for r in rows:
                clusters.append({
                    "cluster_id": r[0],
                    "node_count": r[1],
                    "is_noise": r[2],
                    "avg_peer_degree": float(r[3]) if r[3] else 0.0,
                    "avg_connections": float(r[4]) if r[4] else 0.0,
                    "avg_observer_delay_ms": float(r[5]) if r[5] else 0.0,
                    "top_country": r[6],
                    "top_node_type": r[7]
                })
        except Exception:
            conn.rollback()
            clusters = []
    finally:
        cur.close()
        conn.close()

    return {"clusters": clusters}


@app.get("/api/models")
def get_models():
    """Returns registered ML models or empty list if model_registry table is absent."""
    conn = get_connection()
    cur = conn.cursor()
    models = []

    try:
        cur.execute("""
            SELECT 
                model_id, model_name, model_version, algorithm, target_entity,
                hyperparameters, feature_names, metrics, is_production, created_at
            FROM analytics.model_registry
            ORDER BY created_at DESC;
        """)
        rows = cur.fetchall()
        for r in rows:
            models.append({
                "model_id": r[0],
                "model_name": r[1],
                "model_version": r[2],
                "algorithm": r[3],
                "target_entity": r[4],
                "hyperparameters": r[5],
                "feature_names": r[6],
                "metrics": r[7],
                "is_production": r[8],
                "created_at": r[9].isoformat() if r[9] else None
            })
    except Exception:
        conn.rollback()
        models = []
    finally:
        cur.close()
        conn.close()

    return {"models": models}


from pydantic import BaseModel

class PipelineRunRequest(BaseModel):
    batch_id: int = 1
    threshold: float = 65.0


class FullPipelineRunRequest(BaseModel):
    data_dir: Optional[str] = None
    batch_id: int = 1
    threshold: float = 65.0
    clean: bool = False


@app.post("/api/pipeline/run")
def trigger_pipeline(req: PipelineRunRequest):
    """Triggers the automated operational pipeline (Stages 2-5: Features to ML alerts)."""
    sys.path.append(str(Path(__file__).resolve().parents[1] / "pipeline"))
    from orchestrator import run_full_pipeline
    summary = run_full_pipeline(batch_id=req.batch_id, alert_threshold=req.threshold)
    return summary


@app.post("/api/pipeline/run-full")
def trigger_full_pipeline(req: FullPipelineRunRequest):
    """Triggers the COMPLETE 10-stage operational pipeline from raw data files to predictions."""
    sys.path.append(str(Path(__file__).resolve().parents[1] / "pipeline"))
    from run_full_pipeline import run_full_pipeline as run_e2e_pipeline
    summary = run_e2e_pipeline(
        data_dir=req.data_dir,
        batch_id=req.batch_id,
        threshold=req.threshold,
        clean=req.clean
    )
    return summary

