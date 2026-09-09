"""
ChainWatch Graph Construction

Builds a heterogeneous NetworkX graph from the validated core tables.

Entities:
    - IP
    - Transaction (TXID)
    - Network Node / Observer
    - ASN
    - Country

Relationships:
    - Network Node -> IP              has_ip
    - IP -> ASN                       belongs_to
    - IP -> Country                   located_in
    - Observer Node -> Transaction    observed
    - IP -> Transaction               observed
    - Network Node -> Network Node    peer_connection

Wallet/Address relationships are intentionally NOT created because
the current PostgreSQL core schema does not contain wallet,
input-address, or output-address data.
"""

import json
import sys
from pathlib import Path

import networkx as nx

# Allow importing database.py from ingestion/
sys.path.append(
    str(Path(__file__).resolve().parents[1] / "ingestion")
)

from database import get_connection


OUTPUT_DIR = Path(__file__).resolve().parent
GRAPH_FILE = OUTPUT_DIR / "chainwatch_graph.graphml"
JSON_FILE = OUTPUT_DIR / "chainwatch_graph.json"


def add_node(graph, entity_id, entity_type, **attributes):
    """
    Add a node only once while preserving useful attributes.

    If the node already exists, existing attributes are updated
    with any non-None values supplied in this call.
    """

    if entity_id not in graph:
        graph.add_node(
            entity_id,
            entity_type=entity_type,
            **attributes
        )
    else:
        for key, value in attributes.items():
            if value is not None:
                graph.nodes[entity_id][key] = value


def build_graph():

    conn = get_connection()
    cur = conn.cursor()

    # MultiDiGraph allows multiple observations/connections
    # between the same pair of entities.
    graph = nx.MultiDiGraph(
        name="ChainWatch Heterogeneous Entity Graph"
    )

    print("=" * 70)
    print("CHAINWATCH GRAPH CONSTRUCTION")
    print("=" * 70)
    print()

    # ================================================================
    # 1. LOAD NETWORK NODES
    # ================================================================

    print("Loading network nodes...")

    cur.execute("""
        SELECT
            node_id,
            ip,
            port,
            country,
            asn,
            node_type,
            first_seen,
            last_seen
        FROM core.nodes;
    """)

    nodes = cur.fetchall()

    for (
        node_id,
        ip,
        port,
        country,
        asn,
        node_type,
        first_seen,
        last_seen
    ) in nodes:

        network_node_id = f"node:{node_id}"

        # ------------------------------------------------------------
        # Network Node
        # ------------------------------------------------------------

        add_node(
            graph,
            network_node_id,
            "NODE",
            node_id=node_id,
            ip=str(ip) if ip is not None else None,
            port=port,
            country=country,
            asn=asn,
            node_type=node_type,
            first_seen=(
                str(first_seen)
                if first_seen is not None
                else None
            ),
            last_seen=(
                str(last_seen)
                if last_seen is not None
                else None
            ),
        )

        # ------------------------------------------------------------
        # IP entity
        # ------------------------------------------------------------

        if ip is not None:

            ip_value = str(ip)
            ip_entity = f"ip:{ip_value}"

            add_node(
                graph,
                ip_entity,
                "IP",
                ip=ip_value
            )

            graph.add_edge(
                network_node_id,
                ip_entity,
                edge_type="has_ip"
            )

        # ------------------------------------------------------------
        # ASN entity
        # ------------------------------------------------------------

        if asn is not None:

            asn_value = str(asn)
            asn_entity = f"asn:{asn_value}"

            add_node(
                graph,
                asn_entity,
                "ASN",
                asn=asn_value
            )

            if ip is not None:

                graph.add_edge(
                    f"ip:{str(ip)}",
                    asn_entity,
                    edge_type="belongs_to"
                )

        # ------------------------------------------------------------
        # Country entity
        # ------------------------------------------------------------

        if country is not None:

            country_value = str(country)
            country_entity = f"country:{country_value}"

            add_node(
                graph,
                country_entity,
                "COUNTRY",
                country=country_value
            )

            if ip is not None:

                graph.add_edge(
                    f"ip:{str(ip)}",
                    country_entity,
                    edge_type="located_in"
                )

    print(f"  Network nodes loaded: {len(nodes)}")
    print()

    # ================================================================
    # 2. LOAD TRANSACTIONS
    # ================================================================

    print("Loading transactions...")

    cur.execute("""
        SELECT
            txid,
            timestamp,
            fee,
            size,
            rarity_score,
            tema
        FROM core.transactions;
    """)

    transactions = cur.fetchall()

    for (
        txid,
        timestamp,
        fee,
        size,
        rarity_score,
        tema
    ) in transactions:

        tx_entity = f"tx:{txid}"

        add_node(
            graph,
            tx_entity,
            "TXID",
            txid=txid,
            timestamp=(
                str(timestamp)
                if timestamp is not None
                else None
            ),
            fee=fee,
            size=size,
            rarity_score=rarity_score,
            tema=tema
        )

    print(f"  Transactions loaded: {len(transactions)}")
    print()

    # ================================================================
    # 3. LOAD TRANSACTION OBSERVATIONS
    #
    # core.transaction_observations has no batch_id.
    # Therefore the complete current core table is used.
    # ================================================================

    print("Loading transaction observations...")

    cur.execute("""
        SELECT
            observation_id,
            timestamp,
            observer_id,
            src_ip,
            dst_ip,
            txid,
            message_type,
            peer_id,
            propagation_delay_ms,
            direction,
            sequence_number
        FROM core.transaction_observations;
    """)

    observations = cur.fetchall()

    observation_edges = 0
    ip_observation_edges = 0

    for (
        observation_id,
        timestamp,
        observer_id,
        src_ip,
        dst_ip,
        txid,
        message_type,
        peer_id,
        propagation_delay_ms,
        direction,
        sequence_number
    ) in observations:

        if txid is None:
            continue

        tx_entity = f"tx:{txid}"

        # Make sure transaction exists in graph.
        add_node(
            graph,
            tx_entity,
            "TXID",
            txid=txid
        )

        # ------------------------------------------------------------
        # Observer -> Transaction
        # ------------------------------------------------------------

        if observer_id is not None:

            observer_entity = f"node:{observer_id}"

            add_node(
                graph,
                observer_entity,
                "NODE",
                node_id=observer_id
            )

            graph.add_edge(
                observer_entity,
                tx_entity,
                edge_type="observed",
                observation_id=observation_id,
                timestamp=(
                    str(timestamp)
                    if timestamp is not None
                    else None
                ),
                delay_ms=propagation_delay_ms,
                message_type=message_type,
                direction=direction,
                sequence_number=sequence_number
            )

            observation_edges += 1

        # ------------------------------------------------------------
        # Source IP -> Transaction
        # ------------------------------------------------------------

        if src_ip is not None:

            ip_value = str(src_ip)
            ip_entity = f"ip:{ip_value}"

            add_node(
                graph,
                ip_entity,
                "IP",
                ip=ip_value
            )

            graph.add_edge(
                ip_entity,
                tx_entity,
                edge_type="observed",
                observation_id=observation_id,
                timestamp=(
                    str(timestamp)
                    if timestamp is not None
                    else None
                ),
                delay_ms=propagation_delay_ms,
                message_type=message_type,
                direction=direction,
                sequence_number=sequence_number
            )

            ip_observation_edges += 1

        # ------------------------------------------------------------
        # Peer node
        #
        # The peer is already represented as a network NODE.
        # We do not create a peer -> transaction edge here because
        # the observation itself represents the observer's receipt
        # of the transaction from that peer.
        # ------------------------------------------------------------

        if peer_id is not None:

            peer_entity = f"node:{peer_id}"

            add_node(
                graph,
                peer_entity,
                "NODE",
                node_id=peer_id
            )

    print(f"  Observations loaded: {len(observations)}")
    print(
        f"  Observer -> TX edges created: "
        f"{observation_edges}"
    )
    print(
        f"  IP -> TX observation edges created: "
        f"{ip_observation_edges}"
    )
    print()

    # ================================================================
    # 4. LOAD PEER CONNECTION TOPOLOGY
    #
    # core.peer_connections has no batch_id.
    # Therefore the complete current core table is used.
    # ================================================================

    print("Loading peer connection topology...")

    cur.execute("""
        SELECT
            connection_id,
            timestamp_start,
            timestamp_end,
            src_node_id,
            dst_node_id,
            src_ip,
            dst_ip,
            src_port,
            dst_port,
            direction
        FROM core.peer_connections;
    """)

    connections = cur.fetchall()

    connection_edges = 0

    for (
        connection_id,
        timestamp_start,
        timestamp_end,
        src_node_id,
        dst_node_id,
        src_ip,
        dst_ip,
        src_port,
        dst_port,
        direction
    ) in connections:

        if src_node_id is None or dst_node_id is None:
            continue

        src_entity = f"node:{src_node_id}"
        dst_entity = f"node:{dst_node_id}"

        # Make sure both nodes exist.
        add_node(
            graph,
            src_entity,
            "NODE",
            node_id=src_node_id
        )

        add_node(
            graph,
            dst_entity,
            "NODE",
            node_id=dst_node_id
        )

        graph.add_edge(
            src_entity,
            dst_entity,
            edge_type="peer_connection",
            connection_id=connection_id,
            timestamp_start=(
                str(timestamp_start)
                if timestamp_start is not None
                else None
            ),
            timestamp_end=(
                str(timestamp_end)
                if timestamp_end is not None
                else None
            ),
            src_ip=(
                str(src_ip)
                if src_ip is not None
                else None
            ),
            dst_ip=(
                str(dst_ip)
                if dst_ip is not None
                else None
            ),
            src_port=src_port,
            dst_port=dst_port,
            direction=direction
        )

        connection_edges += 1

    print(f"  Peer connections loaded: {len(connections)}")
    print(
        f"  Connection edges created: "
        f"{connection_edges}"
    )
    print()

    # ================================================================
    # 5. GRAPH SUMMARY
    # ================================================================

    print("=" * 70)
    print("GRAPH SUMMARY")
    print("=" * 70)

    print(
        f"Total graph nodes : "
        f"{graph.number_of_nodes()}"
    )

    print(
        f"Total graph edges : "
        f"{graph.number_of_edges()}"
    )

    print()

    # ------------------------------------------------------------
    # Node type counts
    # ------------------------------------------------------------

    print("--- Node Types ---")

    node_type_counts = {}

    for _, attributes in graph.nodes(data=True):

        entity_type = attributes.get(
            "entity_type",
            "UNKNOWN"
        )

        node_type_counts[entity_type] = (
            node_type_counts.get(entity_type, 0) + 1
        )

    for entity_type, count in sorted(
        node_type_counts.items()
    ):

        print(
            f"  {entity_type:10s}: {count}"
        )

    print()

    # ------------------------------------------------------------
    # Edge type counts
    # ------------------------------------------------------------

    print("--- Edge Types ---")

    edge_type_counts = {}

    for _, _, attributes in graph.edges(
        data=True
    ):

        edge_type = attributes.get(
            "edge_type",
            "UNKNOWN"
        )

        edge_type_counts[edge_type] = (
            edge_type_counts.get(edge_type, 0) + 1
        )

    for edge_type, count in sorted(
        edge_type_counts.items()
    ):

        print(
            f"  {edge_type:18s}: {count}"
        )

    print()

    # ================================================================
    # 6. BASIC GRAPH ANALYTICS
    # ================================================================

    print("--- Basic Graph Analytics ---")

    # ------------------------------------------------------------
    # Degree
    # ------------------------------------------------------------

    degree = dict(graph.degree())

    if degree:

        max_degree_node = max(
            degree,
            key=degree.get
        )

        print(
            f"  Highest-degree entity : "
            f"{max_degree_node}"
        )

        print(
            f"  Highest degree        : "
            f"{degree[max_degree_node]}"
        )

    # ------------------------------------------------------------
    # Connected components
    # ------------------------------------------------------------

    # Convert to undirected graph because connected components
    # are evaluated without considering edge direction.
    undirected_graph = graph.to_undirected()

    components = list(
        nx.connected_components(
            undirected_graph
        )
    )

    print(
        f"  Connected components  : "
        f"{len(components)}"
    )

    if components:

        largest_component = max(
            components,
            key=len
        )

        print(
            f"  Largest component     : "
            f"{len(largest_component)} nodes"
        )

    # ------------------------------------------------------------
    # Degree centrality
    # ------------------------------------------------------------

    if graph.number_of_nodes() > 1:

        degree_centrality = nx.degree_centrality(
            undirected_graph
        )

        top_centrality = sorted(
            degree_centrality.items(),
            key=lambda item: item[1],
            reverse=True
        )[:5]

        print()
        print(
            "  Top 5 degree-centrality entities:"
        )

        for entity, score in top_centrality:

            print(
                f"    {entity:35s} "
                f"{score:.6f}"
            )

    print()

    # ================================================================
    # 7. SAVE GRAPH ARTIFACTS
    # ================================================================

    print("--- Saving Graph Artifacts ---")

    # ------------------------------------------------------------
    # GraphML
    # ------------------------------------------------------------

    nx.write_graphml(
        graph,
        GRAPH_FILE
    )

    # ------------------------------------------------------------
    # JSON
    # ------------------------------------------------------------

    node_link_data = nx.node_link_data(
        graph
    )

    with open(
        JSON_FILE,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            node_link_data,
            f,
            indent=2,
            default=str
        )

    print(
        f"  GraphML : {GRAPH_FILE}"
    )

    print(
        f"  JSON    : {JSON_FILE}"
    )

    print()

    # ================================================================
    # 8. CURRENT GRAPH SCOPE
    # ================================================================

    print("--- Current Graph Scope ---")

    print("  IP           : supported")
    print("  TXID         : supported")
    print("  Network Node : supported")
    print("  ASN          : supported")
    print("  Country      : supported")
    print("  Peer Links   : supported")
    print("  Wallet       : NOT AVAILABLE in current core schema")
    print()

    print("=" * 70)
    print("GRAPH CONSTRUCTION COMPLETE")
    print("=" * 70)

    cur.close()
    conn.close()

    return graph


if __name__ == "__main__":
    build_graph()