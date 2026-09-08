"""
Master Transformation Runner.
Executes all transformations in dependency order for a given batch.
"""
import sys
from pathlib import Path

# Allow importing database.py from the ingestion directory
sys.path.append(str(Path(__file__).resolve().parents[1] / "ingestion"))

from transform_nodes import transform_nodes
from transform_transactions import transform_transactions
from transform_peer_connections import transform_peer_connections
from transform_transaction_observations import transform_transaction_observations
from transform_network_events import transform_network_events

def run_all_transformations():
    print("============================================================")
    print("        STARTING FULL DATA TRANSFORMATION PIPELINE          ")
    print("============================================================\n")

    print("[1/5] Transforming Nodes...")
    transform_nodes()
    print()

    print("[2/5] Transforming Transactions...")
    transform_transactions()
    print()

    print("[3/5] Transforming Peer Connections...")
    transform_peer_connections()
    print()

    print("[4/5] Transforming Transaction Observations...")
    transform_transaction_observations()
    print()

    print("[5/5] Transforming Network Events...")
    transform_network_events()
    print()

    print("============================================================")
    print("             TRANSFORMATION PIPELINE COMPLETED              ")
    print("============================================================")

if __name__ == "__main__":
    run_all_transformations()
