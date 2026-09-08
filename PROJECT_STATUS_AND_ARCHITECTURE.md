# Bitcoin Monitor — Project Status, Directory Inventory & Architecture Report

> **Project Name**: Bitcoin Monitor  
> **Repository Root**: `c:\Users\Anushree Sawant\prozekt\bitcoin_monitor`  
> **Current Date**: September 2026  
> **Active Status**: Phase 1 through Phase 12 (End-to-End Pipeline, ML, API, React Dashboard) are **100% COMPLETE**. The system is fully operational.

---

source .venv/bin/activate## 1. Project Overview & Mission

**Bitcoin Monitor** is an offline, production-grade Bitcoin network monitoring and transaction propagation analytics pipeline. It ingests, validates, transforms, correlates, and analyzes multi-layered Bitcoin data:
1. **Transaction Layer**: On-chain Bitcoin transactions (whales, standard transactions, fees, sizes, script complexity, rarity).
2. **Network Layer**: P2P gossip observations, peer connections, node topology, and network connection events.

### Core Architectural Principles
- **Zero Data Loss**: Raw data is ingested into `staging`, strictly validated against domain rules, invalid records are routed to `validation.quarantine`, and only clean data enters `core`.
- **Reproducibility & Multi-Batch Support**: Every core and analytical entity carries `batch_id` to allow incremental, batch-wise analytics without data contamination.
- **Empirical Grounding**: All rules, models, and baseline thresholds are derived strictly from observed synthetic data rather than arbitrary theoretical assumptions.
- **Strict Terminology & Privacy**: Node locations and IP addresses represent observational vantage points and routing peers, never physical wallet owners or individuals.

---

## 2. Directory Structure & File Inventory

Below is the complete file tree of the workspace, including the exact role and responsibility of each script and asset.

```
bitcoin_monitor/
│
├── data/
│   ├── incoming/                               # Raw incoming data files for Batch 1
│   │   ├── bitcoin_synthetic_whales.json       # 500 synthetic Bitcoin transactions (JSON)
│   │   ├── nodes.csv                           # 150 Bitcoin P2P network nodes (CSV)
│   │   ├── peer_connections.csv                # 441 P2P network connection sessions (CSV)
│   │   ├── transaction_observations.csv        # 3,940 P2P gossip message observations (CSV)
│   │   └── network_events.csv                  # 441 network lifecycle events (CSV)
│   └── processed/                              # Archive directory for processed batches
│
├── ingestion/                                  # Phase 1: Database ingestion & staging
│   ├── database.py                             # Central PostgreSQL connection factory (psycopg v3)
│   ├── test_connection.py                      # Quick connectivity check script
│   ├── load_nodes.py                           # Ingests nodes.csv -> staging.nodes_raw
│   ├── load_peer_connections.py                # Ingests peer_connections.csv -> staging.peer_connections_raw
│   ├── load_transaction_observations.py        # Ingests transaction_observations.csv -> staging.transaction_observations_raw
│   ├── load_bitcoin_transactions.py            # Ingests bitcoin_synthetic_whales.json -> staging.bitcoin_transactions_raw
│   ├── load_network_events.py                  # Ingests network_events.csv -> staging.network_events_raw
│   │
│   └── validation/                             # Phase 1: Ingestion Data Quality & Validation
│       ├── common.py                           # Validation helpers, regex checks, quarantine loggers
│       ├── validate_nodes.py                   # 150 nodes validated against IP, port, type, country rules
│       ├── validate_peer_connections.py        # 441 connections validated against timestamps, directions
│       ├── validate_transaction_observations.py# 3,940 observations validated against protocol & delay rules
│       ├── validate_bitcoin_transactions.py    # 500 transactions validated against txid, fee, size rules
│       ├── validate_network_events.py          # 441 network events validated against event types
│       └── audit_validation.py                 # Master audit script (Verified 0 quarantined rows)
│
├── transformation/                             # Phase 2: Staging -> Core Normalization & Indexing
│   ├── add_batch_id_to_core.py                 # Schema migration adding batch_id NOT NULL to all core tables
│   ├── transform_nodes.py                      # Loads validated nodes into core.nodes
│   ├── transform_transactions.py               # Loads validated transactions into core.transactions
│   ├── transform_peer_connections.py           # Loads validated connections into core.peer_connections
│   ├── transform_transaction_observations.py   # Loads observations with NULLIF(TRIM(connection_id))
│   ├── transform_network_events.py             # Loads network events into core.network_events
│   ├── run_transformation.py                   # Orchestrator executing all core transformations
│   ├── verify_transformation.py                # 100% row-count, PK, and FK reconciliation script
│   └── create_indexes.py                       # Phase 3: Created 12 B-Tree performance indexes on core
│
├── correlation/                                # Phase 4: Cross-Layer Entity Correlation (Stages A–E)
│   ├── stage_a_tx_observations.py              # Stage A: Correlates observations to transactions (3,940 -> 498 txs)
│   ├── stage_b_observer_node.py                # Stage B: Correlates observer nodes (10 monitoring nodes)
│   ├── stage_c_peer_node.py                    # Stage C: Correlates peer nodes (43 propagating peers)
│   ├── stage_d_peer_connections.py             # Stage D: Temporal connection interval alignment (100% DURING)
│   └── stage_e_propagation_timeline.py         # Stage E: Reconstructs chronological propagation sequence
│
├── analytics/                                  # Phase 5: Analytical Modeling & Summaries
│   ├── inspect_for_analytics.py                # Exploratory inspection for analytical views
│   ├── inspect_for_node_summary.py             # Exploratory inspection for node behavior aggregation
│   ├── create_transaction_propagation_view.py  # Builds analytics.transaction_propagation (Grain: 1 row/obs)
│   ├── create_transaction_propagation_summary.py# Builds analytics.transaction_propagation_summary (Grain: 1 row/tx)
│   ├── node_behavior_summary.sql               # Pure SQL definition of analytics.node_behavior_summary
│   ├── create_node_behavior_summary.py         # Builds view & executes 13-point audit (150 rows)
│   └── analyze_distributions.py                # Calculates percentiles (P50–P99) across nodes & txs
│
├── features/                                   # Phase 6: ML Feature Engineering
│   └── build_features.py                       # Engineers ML features for nodes and transactions
│
├── graph/                                      # Phase 7: Graph Analysis
│   ├── build_graph.py                          # Builds NetworkX graph from nodes & connections
│   └── bitcoin_network.graphml                 # Exported GraphML file
│
├── ml/                                         # Phase 8: Machine Learning Models
│   ├── train_clustering.py                     # Trains DBSCAN clustering model
│   ├── train_anomaly_detector.py               # Trains Isolation Forest anomaly models
│   ├── model_registry.py                       # MLflow style model registry logic
│   ├── score_and_explain.py                    # Scores historical data and generates SHAP explanations
│   └── predict_operational.py                  # Operational inference on new data
│
├── models/                                     # Serialized ML artifacts
│   └── *.joblib                                # Scikit-learn models, scalers, feature names
│
├── alerts/                                     # Phase 9: Alert Generation
│   └── generate_alerts.py                      # Generates high-severity alerts based on ML scores
│
├── pipeline/                                   # Phase 10: Unified Orchestration
│   ├── orchestrator.py                         # Base orchestration utilities
│   └── run_full_pipeline.py                    # Master script triggering all 10 stages in < 40 seconds
│
├── api/                                        # Phase 11: FastAPI Backend
│   └── main.py                                 # REST API endpoints (Overview, Nodes, TXs, Alerts, Pipeline)
│
├── dashboard/                                  # Phase 12: React Frontend
│   ├── src/components/                         # Reusable UI components
│   └── src/views/                              # React views (Dashboard, Nodes, TXs, Models, Graph, Alerts)
│
├── logs/                                       # Execution logs directory
└── PROJECT_STATUS_AND_ARCHITECTURE.md          # This comprehensive status report
```

---

## 3. Database Schemas & Entity Model

The PostgreSQL database `bitcoin_monitor` is divided into four cleanly isolated schemas:

```
[ staging ] ----( Validators )----> [ validation.quarantine ]
     |
     v ( Clean records only )
[  core   ] ----( Indexes & FKs )--> [ correlation (Stages A-E) ]
     |
     v
[ analytics ] -> transaction_propagation (3,940 rows)
              -> transaction_propagation_summary (498 rows)
              -> node_behavior_summary (150 rows)
              -> (Next: baseline_profiles)
```

### 1. `staging` Schema
- Stores raw tabular data ingested directly from JSON and CSV files before any business mutations.
- Tables:
  - `staging.nodes_raw` (150 rows)
  - `staging.peer_connections_raw` (441 rows)
  - `staging.transaction_observations_raw` (3,940 rows)
  - `staging.bitcoin_transactions_raw` (500 rows)
  - `staging.network_events_raw` (441 rows)

### 2. `validation` Schema
- Houses the error quarantine engine.
- Table: `validation.quarantine`
  - Captures any malformed, corrupted, or out-of-spec record with table name, record identifier, failed validation rule, and raw payload.
  - **Audit Status**: **0 quarantined rows** across all 5 datasets in Batch 1.

### 3. `core` Schema
- Normalized, strongly-typed relational model with complete referential integrity.
- Primary Keys, Foreign Keys, and `batch_id bigint NOT NULL` on every entity.
- Tables:
  - `core.nodes` (150 rows)
  - `core.transactions` (500 rows)
  - `core.peer_connections` (441 rows)
  - `core.transaction_observations` (3,940 rows)
  - `core.network_events` (441 rows)
- **18 Total Indexes**: Including 12 high-performance foreign key and timestamp indexes created in Phase 3.

### 4. `analytics` Schema
- Materialized views and analytical reporting models built on top of `core`.
- **`analytics.transaction_propagation`** (Grain: 1 row per observation, 3,940 rows):
  - Denormalizes transaction metadata, observer node details, peer node details, connection context, and propagation delay.
  - Calculates intra-transaction observation sequence (`ROW_NUMBER() OVER (PARTITION BY batch_id, txid ORDER BY observation_timestamp)`).
- **`analytics.transaction_propagation_summary`** (Grain: 1 row per transaction, 498 rows):
  - Summarizes first observer, first peer, total observers, min/avg/median/max propagation delays, full propagation span, and message type ratios (`inv` vs `tx`).
- **`analytics.node_behavior_summary`** (Grain: 1 row per node, 150 rows):
  - Preserves every node from `core.nodes` using independent CTEs.
  - Measures degree, active connections, total observations, total propagations, propagation/observation ratios, and average latency.

---

## 4. Phase-by-Phase Progress & Audit Results

### Phase 1: Ingestion & Validation — **100% COMPLETE**
- Implemented 5 dedicated loaders and 5 validator scripts.
- Enforced strict domain validation checks:
  - Hexadecimal format, length (64 chars), and uniqueness of `txid`.
  - IPv4 format, port ranges (1024–65535), and ISO 3166-1 alpha-2 country codes for nodes.
  - Connection start/end interval integrity (`end_time > start_time`).
  - Physical timing constraint: Observation timestamp strictly $\ge$ Transaction creation timestamp.
- Executed `ingestion/validation/audit_validation.py`: **ALL 5 DATASETS 100% CLEAN (0 QUARANTINED ROWS)**.

### Phase 2: Core Transformation — **100% COMPLETE**
- Migrated all `core` tables to support multi-batch processing via `add_batch_id_to_core.py`.
- Developed transform scripts with explicit data cleaning:
  - Discovered that `connection_id` in `transaction_observations.csv` was an empty string; transformed via `NULLIF(TRIM(connection_id), '')` to satisfy PostgreSQL foreign key constraints to `core.peer_connections`.
- Verified via `transformation/verify_transformation.py`:
  - `core.nodes`: 150 / 150 rows
  - `core.transactions`: 500 / 500 rows
  - `core.peer_connections`: 441 / 441 rows
  - `core.transaction_observations`: 3,940 / 3,940 rows
  - `core.network_events`: 441 / 441 rows
  - **Zero missing foreign keys, zero duplicate primary keys**.

### Phase 3: Indexing Optimization — **100% COMPLETE**
- Executed `transformation/create_indexes.py`.
- Created 12 targeted B-Tree indexes:
  - `idx_tx_obs_txid_batch`, `idx_tx_obs_observer_batch`, `idx_tx_obs_peer_batch`, `idx_tx_obs_timestamp`
  - `idx_peer_conn_source_batch`, `idx_peer_conn_target_batch`, `idx_peer_conn_times`
  - `idx_tx_timestamp_batch`, `idx_nodes_type_batch`, `idx_net_events_conn_batch`, `idx_net_events_node_batch`
- Query execution plans verified with `EXPLAIN` (index scans confirmed).

### Phase 4: Entity Correlation (Stages A–E) — **100% COMPLETE**
Executed correlation scripts verifying relational links between transaction and network layers:
- **Stage A (`stage_a_tx_observations.py`)**: 3,940 observations linked to 498 active transactions (2 transactions in `core.transactions` had 0 network observations; verified as unpropagated).
- **Stage B (`stage_b_observer_node.py`)**: Exactly 10 nodes act as observers (all 10 belong exclusively to `node_type = 'monitoring_node'`).
- **Stage C (`stage_c_peer_node.py`)**: Exactly 43 nodes act as propagating peers across 4 peer categories (`high_degree_peer`, `standard_peer`, `anomalous_peer`, `low_degree_peer`).
- **Stage D (`stage_d_peer_connections.py`)**: 100% of observations occurred during an active peer connection window (`DURING_CONNECTION`).
- **Stage E (`stage_e_propagation_timeline.py`)**: Successfully reconstructed chronological propagation timelines.
  - Delay formula mathematically verified: $\text{delay\_ms} = (\text{observation\_time} - \text{tx\_time}) \times 1000$.
  - 3,917 observations were `inv` messages, 23 were full `tx` broadcasts.

### Phase 5: Analytical Modeling — **100% COMPLETE**
- **Step 1 (Exploration)**: Profiled distributions and join dynamics (`inspect_for_analytics.py`, `inspect_for_node_summary.py`).
- **Step 2 (Transaction Views)**: Created `analytics.transaction_propagation` (3,940 rows) and `analytics.transaction_propagation_summary` (498 rows).
- **Step 3 (Node View & 13-Point Audit)**:
  - Created `analytics.node_behavior_summary` using isolated CTEs.
  - Passed all 13 audit checks:
    - 150 distinct nodes preserved.
    - $3,940$ observations and $3,940$ propagations reconciled with zero data loss.
    - $441$ incoming and $441$ outgoing connection edges reconciled.
    - Zero negative latencies, zero division-by-zero errors.
- **Step 4 (Statistical Distribution Analysis)**:
  - Calculated empirical distributions (P50, P75, P90, P95, P99, Min, Max, Mean, StdDev) across all nodes and transactions via `analytics/analyze_distributions.py`.

### Phases 6-10: ML, Alerts, and Pipeline Orchestration — **100% COMPLETE**
- **Feature Engineering (Phase 6)**: Created aggregate numerical features for nodes and transactions.
- **Graph Modeling (Phase 7)**: Built NetworkX topology and exported `bitcoin_network.graphml`.
- **Machine Learning (Phase 8)**: Trained unsupervised models (Isolation Forest for anomalies, DBSCAN for clustering). Serialized to `/models/`.
- **Alerts Generation (Phase 9)**: Implemented automated alert creation for high-risk entities.
- **Unified Pipeline (Phase 10)**: Created `pipeline/run_full_pipeline.py` which sequences all stages (ingestion $\rightarrow$ alerts) in $\sim$38 seconds. Includes batch ID synchronization and directory cleaning.

### Phases 11-12: API & Dashboard — **100% COMPLETE**
- **FastAPI Backend (Phase 11)**: Implemented `api/main.py` serving overview stats, paginated nodes/transactions, alerts, and providing a `/api/pipeline/run-full` trigger endpoint.
- **React Frontend (Phase 12)**: Replaced planned Streamlit UI with a full React SPA using Vite, Recharts, and Tailwind. Features real-time visualization of network data.

---

## 5. Key Empirical Data Discoveries

1. **`connection_id` Foreign Key Nuance**:
   - In raw observation data, `connection_id` is blank. Rather than synthesizing dummy connection IDs, we cast blanks to SQL `NULL`, preserving strict relational validity while reflecting observational reality.
2. **IP Equality Disconnect**:
   - `src_ip` and `dst_ip` on observations represent ephemeral socket endpoints and do NOT match the static listener IPs in `core.nodes`. All network joins must resolve via `observer_id` and `peer_id`.
3. **Traffic Direction Invariant**:
   - All 441 peer connections in Batch 1 are logged with direction `outbound` (monitoring nodes dialing peers).
   - All 3,940 transaction observations are logged as `inbound` (monitoring nodes receiving broadcasts from peers).
4. **Node Roles Division**:
   - Exactly 10 nodes are observers (`monitoring_node`).
   - Exactly 43 nodes are propagating peers.
   - 97 nodes are quiet peers during Batch 1 (zero propagations observed by our monitors). Preserving all 150 nodes in `analytics.node_behavior_summary` avoids sampling bias.
5. **Propagation Latency Norms**:
   - Median delay across all observations: **$98.96\text{ ms}$**.
   - Normal operating envelope: **$46.24\text{ ms}$ (P05) to $167.01\text{ ms}$ (P95)**.
   - Outliers / High Tail: **$> 223.60\text{ ms}$ (P99)** with absolute max at **$373.92\text{ ms}$**.

---

## 6. What's Next on the Roadmap

| Phase / Step | Name | Status | Immediate Action |
|---|---|---|---|
| **Phase 6** | **ML Feature Engineering** | **COMPLETE** | Extracted node & tx features. |
| **Phase 7** | **Graph Analysis** | **COMPLETE** | NetworkX topology modeling. |
| **Phase 8** | **Machine Learning**| **COMPLETE** | Isolation Forest & DBSCAN trained, tested & saved. |
| **Phase 9** | **Alerts Engine** | **COMPLETE** | Automated threshold-based alert generation. |
| **Phase 10** | **Pipeline Orchestration** | **COMPLETE** | `run_full_pipeline.py` created for 38-second E2E execution. |
| **Phase 11** | **REST API** | **COMPLETE** | FastAPI service up and running. |
| **Phase 12** | **React Dashboard** | **COMPLETE** | Frontend UI operational (switched from Streamlit to React). |
| **Future** | **Operational Data Ingestion** | **NEXT** | Automate ingestion of live/new data batches and invoke `predict_operational.py` for continuous monitoring. |
