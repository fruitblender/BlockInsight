"""
Step 2: Heterogeneous Graph Construction & Topological Metrics
Builds NetworkX heterogeneous graph (Nodes, Transactions, Connections, Observations).
Computes degree_centrality, betweenness_centrality, closeness_centrality, and Louvain community_id.
Persists graph metrics to analytics.node_graph_metrics and exports graph to graph/bitcoin_network.graphml.
"""
import sys
import os
from pathlib import Path
import networkx as nx
import community as community_louvain

# Add ingestion directory to path for database connection
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))
from database import get_connection

BATCH_ID = 1


def build_and_export_graph():
    print("============================================================")
    print("   STEP 2: GRAPH CONSTRUCTION & TOPOLOGICAL ANALYSIS")
    print("============================================================\n")

    conn = get_connection()
    cur = conn.cursor()

    # 1. Fetch all 150 nodes
    cur.execute("""
        SELECT node_id, ip, port, country, asn, node_type
        FROM core.nodes
        WHERE batch_id = %s;
    """, (BATCH_ID,))
    nodes = cur.fetchall()

    # 2. Fetch all transactions
    cur.execute("""
        SELECT txid, fee, size, rarity_score
        FROM core.transactions
        WHERE batch_id = %s;
    """, (BATCH_ID,))
    txs = cur.fetchall()

    # 3. Fetch all peer connections
    cur.execute("""
        SELECT src_node_id, dst_node_id, 
               ROUND((EXTRACT(EPOCH FROM (timestamp_end - timestamp_start)) * 1000)::numeric, 3) as duration_ms
        FROM core.peer_connections
        WHERE batch_id = %s;
    """, (BATCH_ID,))
    connections = cur.fetchall()

    # 4. Fetch all observations
    cur.execute("""
        SELECT observer_id, peer_id, txid, propagation_delay_ms, message_type
        FROM core.transaction_observations
        WHERE batch_id = %s;
    """, (BATCH_ID,))
    observations = cur.fetchall()

    print(f"Loaded: {len(nodes)} nodes, {len(txs)} txs, {len(connections)} peer connections, {len(observations)} observations.")

    # 5. Build full heterogeneous graph
    G = nx.MultiDiGraph()

    for n_id, ip, port, country, asn, n_type in nodes:
        G.add_node(n_id, entity_type="node", ip=str(ip), port=port or 8333, country=country or "UNKNOWN", asn=asn or "UNKNOWN", node_type=n_type)

    for txid, fee, size, rarity in txs:
        G.add_node(txid, entity_type="transaction", fee=float(fee), size=int(size), rarity_score=float(rarity))

    for src, dst, dur in connections:
        G.add_edge(src, dst, relationship="connected_to", duration_ms=float(dur) if dur else 0.0)

    for obs, peer, txid, delay, msg_type in observations:
        if obs:
            G.add_edge(obs, txid, relationship="observed", delay_ms=float(delay) if delay else 0.0, message_type=msg_type)
        if peer:
            G.add_edge(peer, txid, relationship="propagated", delay_ms=float(delay) if delay else 0.0, message_type=msg_type)

    print(f"Full Heterogeneous Graph: {G.number_of_nodes()} total nodes, {G.number_of_edges()} total edges.")

    # 6. Extract P2P Node topology graph (undirected) to compute node centrality & Louvain communities
    G_p2p = nx.Graph()
    for n_id, _, _, _, _, _ in nodes:
        G_p2p.add_node(n_id)

    for src, dst, _ in connections:
        G_p2p.add_edge(src, dst)

    # Calculate topological metrics on P2P network
    print("\nComputing topological centrality and Louvain community partition...")
    deg_cent = nx.degree_centrality(G_p2p)
    bet_cent = nx.betweenness_centrality(G_p2p)
    close_cent = nx.closeness_centrality(G_p2p)
    
    # Louvain community detection
    # If graph has disconnected components or zero edges, handle gracefully
    try:
        partition = community_louvain.best_partition(G_p2p, random_state=42)
    except Exception as e:
        print(f"Warning running Louvain partition: {e}. Falling back to default partition.")
        partition = {n: 0 for n in G_p2p.nodes()}

    unique_communities = len(set(partition.values()))
    print(f"  Louvain detected {unique_communities} communities.")

    # 7. Persist metrics to analytics.node_graph_metrics
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        DROP TABLE IF EXISTS analytics.node_graph_metrics CASCADE;
        CREATE TABLE analytics.node_graph_metrics (
            batch_id INTEGER NOT NULL,
            node_id VARCHAR(64) NOT NULL,
            degree_centrality NUMERIC(10, 6) NOT NULL,
            betweenness_centrality NUMERIC(10, 6) NOT NULL,
            closeness_centrality NUMERIC(10, 6) NOT NULL,
            community_id INTEGER NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (batch_id, node_id)
        );
    """)
    conn.commit()

    insert_metrics_sql = """
        INSERT INTO analytics.node_graph_metrics (
            batch_id, node_id, degree_centrality, betweenness_centrality, closeness_centrality, community_id
        ) VALUES (%s, %s, %s, %s, %s, %s);
    """
    for n_id, _, _, _, _, _ in nodes:
        cur.execute(insert_metrics_sql, (
            BATCH_ID,
            n_id,
            round(deg_cent.get(n_id, 0.0), 6),
            round(bet_cent.get(n_id, 0.0), 6),
            round(close_cent.get(n_id, 0.0), 6),
            int(partition.get(n_id, 0))
        ))
    conn.commit()

    # Also alter analytics.node_features to add graph metric columns if not already present
    cur.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'node_features' AND column_name = 'degree_centrality') THEN
                ALTER TABLE analytics.node_features ADD COLUMN degree_centrality NUMERIC(10, 6) DEFAULT 0.0;
                ALTER TABLE analytics.node_features ADD COLUMN betweenness_centrality NUMERIC(10, 6) DEFAULT 0.0;
                ALTER TABLE analytics.node_features ADD COLUMN closeness_centrality NUMERIC(10, 6) DEFAULT 0.0;
                ALTER TABLE analytics.node_features ADD COLUMN community_id INTEGER DEFAULT 0;
            END IF;
        END $$;
    """)
    conn.commit()

    # Update analytics.node_features with graph metrics
    cur.execute("""
        UPDATE analytics.node_features nf
        SET 
            degree_centrality = ngm.degree_centrality,
            betweenness_centrality = ngm.betweenness_centrality,
            closeness_centrality = ngm.closeness_centrality,
            community_id = ngm.community_id
        FROM analytics.node_graph_metrics ngm
        WHERE nf.batch_id = ngm.batch_id AND nf.node_id = ngm.node_id
          AND nf.batch_id = %s;
    """, (BATCH_ID,))
    conn.commit()

    # 8. Export graph to graph/bitcoin_network.graphml
    graph_path = Path(__file__).resolve().parent / "bitcoin_network.graphml"
    # NetworkX write_graphml requires all attribute values to be basic types
    nx.write_graphml(G, str(graph_path))
    print(f"Exported graph to {graph_path} ({os.path.getsize(graph_path) / 1024:.1f} KB)")

    cur.close()
    conn.close()
    print(">>> STEP 2 GRAPH CONSTRUCTION & METRICS COMPLETE! <<<")


if __name__ == "__main__":
    build_and_export_graph()
