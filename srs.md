# Software Requirements Specification (SRS)
## AI-Powered Monitoring & Analysis of Bitcoin Transaction Traffic
### Smart India Hackathon — Offline Network–Blockchain Correlation & Investigative Analytics Platform

---

## 1. Document Control

| Field | Value |
|---|---|
| Document Title | SRS — AI-Powered Monitoring & Analysis of Bitcoin Transaction Traffic |
| Project Codename | **ChainWatch** (working name — placeholder, rename as desired) |
| Prepared For | Smart India Hackathon (SIH) Submission |
| Document Type | Software Requirements Specification |
| Target Deployment | Offline, single-node Linux workstation |
| Status | Draft v1.0 |

## 2. Revision History

| Version | Date | Author | Description |
|---|---|---|---|
| 0.1 | Draft | Team | Initial skeleton from problem statement |
| 1.0 | Draft | Team | Full SRS covering architecture, ML, graph, dashboard, offline ops |

---

## 3. Executive Summary

ChainWatch is an **offline, Linux-deployable investigative analytics platform** that ingests synthetic Bitcoin transaction metadata (blockchain-layer) together with synthetic network-observation metadata (P2P-layer), correlates the two layers using timestamp/TXID/observer evidence, builds a heterogeneous entity graph (IP, wallet, transaction, ASN, country), engineers behavioral features, and applies **genuine trained machine-learning models** (Isolation Forest for anomaly detection, HDBSCAN/DBSCAN for entity clustering) to produce a **ranked, explainable list of investigative leads**. Every alert is explainable via SHAP-based feature attribution and traceable back to the exact transactions and network observations that produced it. The system never claims wallet ownership from an IP address, never claims total network visibility, and never issues an automatic accusation — it only prioritizes leads for human investigators.

The system is built entirely from a lightweight, student-implementable Python stack (FastAPI, Pandas/Polars, DuckDB/SQLite, scikit-learn, NetworkX, react/Plotly, SHAP, joblib) and is designed to run with **zero Internet dependency** after initial setup.

## 4. Introduction

### 4.1 Purpose
This SRS defines the functional and non-functional requirements, architecture, data model, ML design, and operational behavior of ChainWatch, to be built by a student team for SIH, and to be directly implementable from this document.

### 4.2 Intended Audience
Student developers (backend, ML, frontend/dashboard), SIH evaluators, project mentors.

### 4.3 Document Conventions
- Functional requirements: `FR-<MODULE>-<NNN>`
- Non-functional requirements: `NFR-<CATEGORY>-<NNN>`
- "Shall" = mandatory; "should" = recommended; "may" = optional.

## 5. Problem Statement (Restated)

Bitcoin's pseudonymous P2P design allows illicit actors to move and launder funds while evading conventional financial surveillance. The system must ingest bulk synthetic transaction/network metadata, correlate network-layer observations with blockchain-layer data, apply AI/ML to detect anomalies and cluster entities, and produce prioritized, explainable investigative leads via a dashboard — entirely offline, on Linux, using a synthetic dataset.

## 6. Objectives

1. Ingest/parse bulk CSV/JSON/XML metadata (timestamp, IPs/ports, TXID, addresses, amounts, fee, script type).
2. Build an entity/transaction graph linking IPs, wallets, transactions.
3. Implement a genuine trained ML detection pipeline (not rules alone).
4. Generate ranked, explainable alerts with confidence/risk scores.
5. Present findings via an offline dashboard / link-analysis view.

## 7. Scope

### 7.1 In Scope
- Synthetic data ingestion and P2P propagation simulation.
- Network–blockchain correlation engine (evidence-based, not attribution).
- Heterogeneous graph construction and analytics.
- Feature engineering, Isolation Forest anomaly detection, HDBSCAN/DBSCAN clustering.
- SHAP-based explainability and human-readable alert reasons.
- Streamlit/Plotly dashboard for investigation and link analysis.
- Controlled, versioned, periodic batch retraining pipeline.
- Fully offline Linux deployment.

### 7.2 Out of Scope
- Real Bitcoin Core node deployment or live mempool/network connection.
- Deanonymization or proof of real-world identity/ownership.
- Automatic legal action, freezing of funds, or law-enforcement reporting.
- Distributed/cluster infrastructure (Kubernetes, Spark), deep learning, or microservices.
- Processing real seized data (prototype uses synthetic data only).

## 8. Stakeholders

| Stakeholder | Interest |
|---|---|
| SIH Evaluation Panel | Technical correctness, working prototype, explainability |
| Investigator (end user persona) | Usable alerts, evidence traceability |
| Data Analyst | Dataset import, quality, model performance |
| Administrator | Model lifecycle, retraining, rollback |
| Student Dev Team | Buildable, testable, demonstrable system |

## 9. Definitions and Acronyms

| Term | Meaning |
|---|---|
| TXID | Transaction ID, unique hash identifying a Bitcoin transaction |
| UTXO | Unspent Transaction Output |
| ASN | Autonomous System Number (network ownership identifier) |
| Fan-in/Fan-out | Number of distinct input/output counterparties of a wallet |
| Observer node | A simulated monitoring node that records network-layer observations |
| Propagation | Spread of a transaction announcement across P2P peers |
| Correlation | Evidence linking a network observation to a blockchain transaction |
| Attribution | (Explicitly NOT claimed) — asserting a real-world identity or ownership |
| Anomaly score | Model output indicating statistical unusualness, not guilt |
| SHAP | SHapley Additive exPlanations — feature attribution method |

## 10. Background: Bitcoin Blockchain and P2P Network

### 10.1 Blockchain Layer
The Bitcoin blockchain is a public, append-only ledger of transactions. Each transaction has a TXID, a set of inputs (referencing previous outputs / addresses), a set of outputs (new addresses + amounts), and a fee. **The blockchain itself contains no IP address information** — it only records financial movement between addresses.

### 10.2 P2P Network Layer
Bitcoin nodes relay transactions to each other over a gossip-style P2P network before they are mined into a block. A node that is directly connected to a transaction's originator (or an early relayer) may observe the announcement earlier and can record the peer connection's IP/port/timestamp. This is **network telemetry**, separate from the blockchain data.

### 10.3 Why the Two Layers Must Be Correlated, Not Conflated
A node only sees traffic from its own connected peers — a small, non-representative slice of the global network. An IP that relayed a transaction is not necessarily the transaction's creator (it may be a relaying peer, a wallet-service server, an exchange node, or traffic behind NAT/VPN/Tor). Therefore network observations are **corroborating evidence**, and correlation confidence must be modeled explicitly rather than assumed.

### 10.4 Bitcoin Core — Architectural Analogy (Not Implemented)
Bitcoin Core is the reference full-node implementation. A running node maintains active P2P connections with peers and can log metadata about those connections (peer IP, port, connection time, addr messages). However:
- Bitcoin Core has **no mechanism** to map a wallet address to the IP of the person who controls it.
- Network-layer visibility (via `getpeerinfo`, network logs) and blockchain-layer data (via the ledger) are **structurally separate data sources** inside Bitcoin Core itself.
- ChainWatch does **not** implement or embed Bitcoin Core. It **replicates the relevant observation behavior** (peers seeing transaction announcements at different times) through a synthetic P2P propagation simulator (Section 17), because the SIH dataset is synthetic and the deployment must be offline.

## 11. System Overview

ChainWatch is a single-machine, modular Python application composed of a data pipeline, a correlation/graph engine, an ML subsystem, and a Streamlit dashboard, backed by DuckDB/SQLite for storage. It operates in two modes:
1. **Batch analysis mode** — ingest a dataset, run the full pipeline, produce alerts.
2. **Investigation mode** — browse entities, transactions, graph, and alert evidence via the dashboard.
A separate **offline periodic retraining pipeline** allows the ML models to be updated in a controlled, versioned manner as new operational datasets are supplied.

## 12. Assumptions

- All input data (transaction metadata, network observations) is synthetic, either SIH-provided or generated by the team's own simulator.
- The Linux host has Python 3.10+, sufficient disk space, and no active Internet connection is required at runtime.
- Users are trained investigators/analysts who understand that alerts are leads, not verdicts.
- GeoIP/ASN databases are downloaded once during preparation and stored locally.

## 13. Constraints

- No real blockchain/network connectivity at runtime (must run air-gapped).
- No deep learning frameworks unless clearly justified (none required here).
- No Kubernetes/Spark/microservices — single-process or lightly-multi-process student-scale architecture only.
- Must run demonstrably on a single Linux laptop within hackathon demo constraints (dataset sizes in the 10K–1M row range, not billions).

---

## 14. System Architecture

### 14.1 High-Level Pipeline

```
Synthetic/Provided Dataset
        │
        ▼
   Data Ingestion  ──►  Parsing + Validation + Normalization
        │
        ├────────────────────┬─────────────────────┐
        ▼                    ▼                      │
  Blockchain Layer      Network Layer                │
  (TXID/wallet/amount)  (IP/port/timing)              │
        │                    │                        │
        └─────────┬──────────┘                        │
                   ▼                                   │
           Correlation Engine  ◄────── GeoIP/ASN Enrichment
                   │
                   ▼
        Entity/Transaction Graph
                   │
                   ▼
           Feature Engineering
                   │
        ┌──────────┴──────────┐
        ▼                     ▼
 Anomaly Detection      Entity Clustering
 (Isolation Forest)     (HDBSCAN/DBSCAN)
        └──────────┬──────────┘
                   ▼
             Risk Scoring
                   ▼
             Explainability (SHAP)
                   ▼
             Ranked Alerts
                   ▼
              Dashboard (Streamlit)
```

### 14.2 Model-Update (MLOps) Pipeline

```
Operational Data → Archive → Feature Generation
        │
        ▼
Historical + Recent Data (retained baseline + new batch)
        │
        ▼
Candidate Model Training
        │
        ▼
   Evaluation ──► compare vs current production model
     /      \
  PASS      FAIL
   │          │
   ▼          ▼
Deploy new   Keep current
model (v+1)  model, log rejection
(retain old
for rollback)
```
Retraining runs **on demand or on a scheduled batch cadence** (e.g., weekly, or after N new operational datasets) — never continuously/online — to prevent uncontrolled drift and to keep every deployed model auditable and reproducible.

### 14.3 Component/Module View
See Section 21 for full per-module functional requirements. Modules communicate through the DuckDB/SQLite store and well-defined Python interfaces (or FastAPI endpoints if a service boundary is desired), not through hidden global state.

### 14.4 Technology Stack and Rationale

| Layer | Technology | Rationale |
|---|---|---|
| Backend/API | FastAPI | Async, typed, minimal, easy for a student team; optional — a pure CLI/Streamlit app is also acceptable for MVP |
| Data processing | Pandas / Polars | Mature, well-documented; Polars for larger synthetic datasets if needed |
| Storage | DuckDB or SQLite | Embedded, zero-ops, SQL-queryable, fits offline single-node deployment |
| P2P/graph modeling | NetworkX | Simple graph API sufficient at hackathon scale; PyVis/Cytoscape.js for visualization |
| Event simulation | asyncio / SimPy | Lightweight discrete-event simulation for propagation delays |
| ML | scikit-learn (IsolationForest, DBSCAN/HDBSCAN) | Well-understood, fast to train, interpretable, no GPU required |
| Explainability | SHAP | Model-agnostic, supports tree-based Isolation Forest, instance + global explanations |
| Dashboard | Streamlit + Plotly | Rapid, Python-only, no separate frontend build step, runs offline |
| GeoIP | Offline GeoLite2 (or equivalent offline-licensed DB) | No live lookups needed; downloaded once during setup |
| Persistence | joblib | Standard scikit-learn model serialization |
| Testing | pytest | Standard Python testing |
| Deployment | Linux + venv (+ optional Docker) | Simple, reproducible, no orchestration overhead |

Deep learning, Kubernetes, Spark, and microservices are explicitly excluded — the dataset scale and problem statement do not require them, and they would add operational risk for a time-boxed student project.

---

## 15. Data Architecture

### 15.1 Data Categories

| Category | Description |
|---|---|
| **Raw Blockchain Data** | TXID, inputs, outputs, addresses, amounts, fee, script type, block/tx timestamps |
| **Raw Network Data** | src/dst IP & port, timestamp, observer ID, propagation metadata |
| **Enrichment Data** | GeoIP country, ASN (derived by lookup from IP, not "raw" input) |
| **Derived Behavioral Features** | Computed from raw + enrichment (frequency, velocity, fan-in/out, etc.) — see 15.2 |
| **Correlation Artifacts** | Correlation confidence scores, evidence links |
| **Graph Artifacts** | Nodes/edges of the entity/transaction graph |
| **ML Artifacts** | Anomaly scores, cluster IDs, SHAP values, model versions |
| **Operational Artifacts** | Alerts, audit logs, model registry entries |

### 15.2 Raw Fields vs Derived Features (explicit distinction)

**Raw fields (given directly in the dataset):**
`timestamp, src_ip, dst_ip, src_port, dst_port, txid, input_addresses[], output_addresses[], input_amounts[], output_amounts[], fee, script_type, observer_id`

**Enrichment fields (looked up, not originally present):**
`geo_country, asn, asn_org`

**Derived behavioral features (computed):**
`tx_frequency, tx_velocity, total_volume, avg_amount, amount_variance, fan_in, fan_out, wallet_degree, unique_counterparties, ip_diversity, asn_diversity, geo_diversity, observation_count, propagation_delay, propagation_delay_variance, num_observers_per_txid, temporal_burstiness, graph_degree_centrality, graph_betweenness_centrality, community_id`

### 15.3 Dataset Strategy — Training / Validation / Operational

| Category | Purpose | Example Split (illustrative) |
|---|---|---|
| **A. Training** | Fit the initial Isolation Forest / clustering baseline | Jan 1–20 |
| **B. Validation/Test** | Evaluate before first deployment; tune thresholds | Jan 21–25 (val), Jan 26–30 (test) |
| **C. Operational** | Real analysis input after deployment; also candidate future training material | Feb onward |

**Why temporal separation instead of random row splitting:** Bitcoin transaction behavior is time-correlated (bursts, campaigns, evolving laundering patterns). Random row splitting would let information from a "future" burst leak into training via shared wallets/IPs active across the whole period, producing an over-optimistic evaluation. A strict temporal holdout better simulates real deployment, where the model must generalize to *unseen future* behavior.

Operational datasets are imported via local file transfer (USB/removable media, local file drop folder) — **no network transfer or Internet access is required or permitted at analysis time.**

---

## 16. Detailed Functional Requirements (Overview)

Full requirement catalogue is distributed across Sections 17–31 and summarized in the Traceability Matrix (Section 43). Requirement ID prefixes used: `ING` (ingestion), `NORM` (normalization), `GEO` (enrichment), `SIM` (P2P simulation), `NET` (network observation), `BLK` (blockchain), `COR` (correlation), `GRA` (graph), `FEAT` (features), `ML` (ML core), `ANOM` (anomaly), `CLUS` (clustering), `RISK` (scoring), `EXP` (explainability), `ALT` (alerts), `DASH` (dashboard), `MLOPS` (model lifecycle), `ARCH` (archive), `CFG` (configuration), `AUD` (audit/logging), `API` (REST), `SEC` (security).

---

## 17. Network Propagation Simulation

### 17.1 Purpose
Because the deployment must be fully offline and the SIH dataset is synthetic, ChainWatch includes its **own synthetic P2P propagation simulator** to generate realistic network-observation data where none is supplied, and to let the team stress-test correlation logic.

### 17.2 Simulator Behavior
- Generates N synthetic nodes with synthetic IP:port identifiers.
- Builds a configurable random/topological P2P graph (e.g., Erdős–Rényi or a small-world graph via NetworkX) representing peer connections.
- Selects one or more "origin" nodes per synthetic transaction.
- Propagates a transaction announcement outward along graph edges using randomized, configurable per-edge delays (e.g., 10–200 ms, log-normal distribution).
- Designates a configurable subset of nodes as **"observer" nodes** that log every transaction announcement they receive, with a timestamp and their own node ID — these logs become the synthetic **network observation records**.
- Preserves TXID linkage so each observation record can be joined back to the corresponding blockchain transaction.

### 17.3 Example
```
TX123 originates at N1
N1 → N8  : 30 ms
N1 → N20 : 50 ms
N8 → N31 : 25 ms
N20 → N42: 40 ms
```
Observer nodes N8, N20, N31, N42 each log an observation of TX123 with a different timestamp. **This is a partial, observer-dependent view — not the complete global propagation path.** No single observer (and no single ChainWatch deployment) sees the full graph; this limitation is deliberately preserved in the simulator output so downstream correlation/ML logic is trained and evaluated against realistic partial visibility.

### 17.4 Why This Enables Offline Operation
Because the simulator runs entirely locally and requires no live Bitcoin network connection, it can generate arbitrarily large "network observation" datasets for development, testing, and demo purposes without any Internet dependency, while still producing data whose statistical shape resembles real P2P propagation.

### 17.5 Functional Requirements
- **FR-SIM-001**: The system shall generate a configurable synthetic P2P topology (node count, edge density, topology type).
- **FR-SIM-002**: The system shall assign each synthetic node a synthetic IP:port pair, optionally with associated synthetic ASN/country.
- **FR-SIM-003**: The system shall propagate a transaction announcement from an origin node across the topology with randomized per-edge delay.
- **FR-SIM-004**: The system shall record an observation (txid, observer_id, timestamp) for every observer node reached during propagation.
- **FR-SIM-005**: The system shall support configuring the subset/fraction of nodes acting as observers.
- **FR-SIM-006**: The system shall persist propagation-observation records in the same schema used for externally supplied network-observation data, so the correlation engine is agnostic to data origin.

---

## 18. Data Ingestion and Processing

### 18.1 Purpose
Accept bulk CSV/JSON/XML datasets (blockchain transactions and/or network observations), validate structure, and normalize into the internal schema.

### 18.2 Functional Requirements
- **FR-ING-001**: The system shall ingest datasets in CSV, JSON, and XML formats.
- **FR-ING-002**: The system shall validate presence of minimum required fields (Section 15.2) before accepting a dataset.
- **FR-ING-003**: The system shall reject or quarantine records with malformed timestamps, invalid IP formats, or non-numeric amounts, logging the reason.
- **FR-ING-004**: The system shall support partial ingestion — valid rows are loaded even if some rows in the same file fail validation, with a per-file ingestion report (rows accepted/rejected/reasons).
- **FR-NORM-001**: The system shall normalize timestamps to UTC ISO-8601.
- **FR-NORM-002**: The system shall normalize wallet addresses and TXIDs to a canonical case/format.
- **FR-NORM-003**: The system shall de-duplicate exact-duplicate transaction/observation records on ingestion.
- **FR-GEO-001**: The system shall enrich every IP with country and ASN using an offline GeoIP database.
- **FR-GEO-002**: The system shall flag IPs not resolvable in the offline GeoIP database as "unresolved" rather than failing ingestion.

### 18.3 Failure Cases
Missing minimum fields → dataset rejected with a clear error listing missing columns. Corrupt file encoding → row-level skip with audit log entry. GeoIP database absent → enrichment fields set to `null`/`"unknown"`, pipeline continues (enrichment is best-effort, not blocking).

---

## 19. Network–Blockchain Correlation

### 19.1 Correlation Principle
Correlation links a **network observation** (IP, port, timestamp, observer) to a **blockchain transaction** (TXID) via shared TXID and temporal proximity — it produces an **evidence score**, never an ownership claim.

### 19.2 Correlation Signals
- Exact TXID match between observation record and transaction record (mandatory join key).
- Temporal alignment: observation timestamp within a configurable window (e.g., ±T seconds) of the transaction's first-seen/broadcast time.
- Number of independent observers reporting the same TXID (more observers → higher confidence the propagation pattern is genuine, not noise).
- Observation consistency across observers (similar delay ordering vs. topology expectations).
- Propagation delay/variance characteristics (unusually uniform or unusually erratic delays may indicate synthetic anomalies worth flagging separately).
- IP/ASN/geo enrichment consistency (e.g., multiple observations from clustered ASNs vs. widely dispersed).

### 19.3 Correlation Confidence Score
A composite score (0–1) computed from a weighted combination of the signals above (e.g., logistic combination or simple weighted sum, tunable in configuration), **not a proof of attribution**. Example weighting (illustrative, configurable):

| Signal | Weight |
|---|---|
| TXID exact match present | required (gating) |
| Temporal proximity (closer = higher) | 0.35 |
| Number of independent observers | 0.30 |
| Observer consistency | 0.20 |
| Propagation delay plausibility | 0.15 |

### 19.4 Correlation vs. Attribution — Explicit Statement
> **Correlation** answers: "Is there evidence linking this network observation to this transaction, and how strong is it?"
> **Attribution** would answer: "Who controls this wallet?" — **ChainWatch does not perform attribution.** An IP correlated with a TXID may belong to a relaying peer, a node operator, a wallet-service backend, a VPN exit, or a Tor relay — not necessarily the transaction's originator.

### 19.5 Functional Requirements
- **FR-COR-001**: The system shall join network observations to blockchain transactions via TXID.
- **FR-COR-002**: The system shall compute a temporal-proximity component using a configurable time window.
- **FR-COR-003**: The system shall compute a correlation confidence score per (IP, TXID) pair using the weighted model above.
- **FR-COR-004**: The system shall store all contributing signal values alongside the final confidence score for auditability.
- **FR-COR-005**: The system shall never emit a field or label implying wallet ownership or identity attribution.

---

## 20. Entity/Transaction Graph

### 20.1 Node Types
`IP`, `Wallet/Address`, `Transaction (TXID)`, `ASN`, `Country`, `Observer` (optional).

### 20.2 Edge Types

| Edge | Direction | Weighted | Notes |
|---|---|---|---|
| IP —observed→ TXID | directed | yes (confidence) | from correlation engine |
| TXID —contains-input→ Wallet | directed | no | blockchain fact |
| TXID —creates-output→ Wallet | directed | no | blockchain fact |
| IP —belongs-to→ ASN | directed | no | enrichment fact |
| IP —located-in→ Country | directed | no | enrichment fact |
| Wallet —transacts-with→ Wallet | undirected/weighted | yes (volume, count) | derived, via shared TXID |
| Observer —observed→ TXID | directed | yes (timestamp) | from simulation/observation data |

All edges carry timestamp and metadata (e.g., amount, confidence, delay) as edge attributes.

### 20.3 Graph Analytics
- Degree (wallet_degree, IP fan-out).
- Connected components (clusters of related wallets/IPs/transactions).
- Centrality (degree, betweenness) as anomaly-detection features.
- Community detection (e.g., Louvain via `python-louvain`/NetworkX) to group related entities.

### 20.4 Functional Requirements
- **FR-GRA-001**: The system shall construct a heterogeneous graph with the node/edge types above using NetworkX (or an equivalent embedded graph library).
- **FR-GRA-002**: The system shall support neighborhood queries (k-hop expansion from any entity).
- **FR-GRA-003**: The system shall compute degree, centrality, and community-membership features per node.
- **FR-GRA-004**: The system shall export subgraphs for visualization (PyVis/Cytoscape.js-compatible JSON).
- **FR-GRA-005**: The system shall support filtering the graph by time range, entity type, and confidence threshold.

---

## 21. Feature Engineering

### 21.1 Feature Groups
1. **Volume/velocity features**: tx_frequency, tx_velocity, total_volume, avg_amount, amount_variance.
2. **Topology features**: fan_in, fan_out, wallet_degree, unique_counterparties.
3. **Diversity features**: ip_diversity, asn_diversity, geo_diversity.
4. **Network/observation features**: observation_count, propagation_delay, propagation_delay_variance, num_observers_per_txid.
5. **Temporal features**: temporal_burstiness (e.g., coefficient of variation of inter-transaction times).
6. **Graph features**: degree_centrality, betweenness_centrality, community_id.

### 21.2 Preprocessing
- Missing-value imputation (median for numeric, "unknown" bucket for categorical enrichment fields).
- Feature scaling (StandardScaler/RobustScaler) prior to Isolation Forest/DBSCAN, since both are distance/split sensitive.
- Categorical encoding for script_type, country, ASN where used as auxiliary (not primary) inputs.
- Feature versioning: every feature-table snapshot tagged with a `feature_version` used consistently between training and inference.

### 21.3 Functional Requirements
- **FR-FEAT-001**: The system shall compute the full feature set (Section 21.1) per wallet entity per analysis window.
- **FR-FEAT-002**: The system shall persist a versioned feature table linked to the source dataset and feature_version.
- **FR-FEAT-003**: The system shall apply identical preprocessing/scaling transforms at training and inference time (persisted scaler object).

---

## 22. AI/ML Architecture

### 22.1 Design Principle
ChainWatch's core detection is a **genuinely trained ML pipeline**, not a rule engine. Rules/thresholds may be used only as **supporting evidence** layered on top of ML outputs (e.g., a rule-based tag "fan_out > 50" shown alongside the model's anomaly score), never as the primary detection mechanism.

### 22.2 Primary Model — Isolation Forest (Anomaly Detection)
- **Input**: the scaled feature vector per wallet entity (Section 21).
- **Training**: unsupervised, fit on Training-period data; contamination parameter tuned via validation-period injected-anomaly evaluation (Section 42).
- **Inference**: produces a raw anomaly score per entity; converted to a 0–100 normalized "risk contribution" for display.
- **Output semantics**: **An anomaly score measures statistical unusualness relative to the training distribution — it is *not* the probability that the entity is engaged in criminal activity.** This distinction is enforced in all UI copy and documentation.

### 22.3 Secondary Model — HDBSCAN/DBSCAN (Entity Clustering)
- Groups wallets/IPs with similar behavioral fingerprints into clusters (e.g., "high-frequency low-value mixing-like behavior").
- Entities labeled as noise (outliers, in DBSCAN terms) are cross-referenced with the anomaly detector — noise + high anomaly score is a stronger combined signal.
- Cluster IDs are surfaced in the dashboard as an additional explanatory dimension ("this wallet behaves like cluster #4, which also contains 12 other flagged wallets").

### 22.4 Model Persistence
All trained models (Isolation Forest, clustering model, fitted scaler) are serialized via `joblib` and stored in a **model registry** (Section 31) with metadata: training window, feature_version, hyperparameters, evaluation metrics, model_version.

### 22.5 Terminology Discipline
Only the following terms are used in outputs and UI: `anomaly score`, `investigation priority`, `risk score`, `confidence`, `investigative lead`. Terms such as "confirmed criminal", "guilty", or "proven laundering" are **never** produced by the system.

### 22.6 Functional Requirements
- **FR-ML-001**: The system shall train an Isolation Forest model on the scaled Training-period feature table.
- **FR-ML-002**: The system shall train a DBSCAN/HDBSCAN clustering model on the same feature space.
- **FR-ML-003**: The system shall persist trained models with joblib, tagged with a unique model_version.
- **FR-ML-004**: The system shall apply the persisted scaler/model pair consistently at inference time.
- **FR-ML-005**: The system shall never present anomaly score as a probability of criminality in any output or documentation string.

---

## 23. Anomaly Detection (Use Case Detail)

### 23.1 Target Behavioral Patterns (synthetic, illustrative)
- Unusually high transaction velocity (many transactions in a short window).
- Unusually high fan-out (one wallet distributing to many destinations rapidly — "peeling chain"-like behavior).
- Unusually high fan-in (many sources consolidating into one wallet — "collection" pattern).
- Rapid movement of funds across a short chain of wallets ("hot-potato" layering).
- Unusually high unique-counterparty count for the entity's historical baseline.
- Unusual geographic/ASN diversity of correlated network observations.
- Unusual propagation/observation behavior (e.g., very few observers despite high value, or implausible delay uniformity).
- Burst activity (temporal clustering of transactions after a dormant period).
- Abnormal combinations of network + blockchain behavior (e.g., high value + observations only from a single ASN).

### 23.2 Explicit Non-Claim
Each of the above is a **behavioral pattern that correlates with laundering/evasion techniques described in AML literature**, not proof of illegality. The system output is always framed as **"high-priority investigative lead,"** never **"confirmed criminal wallet."**

---

## 24. Entity Clustering
Covered in Section 22.3. Additional requirement:
- **FR-CLUS-001**: The system shall assign a `cluster_id` (or `-1` for noise/outlier) to every entity processed.
- **FR-CLUS-002**: The system shall compute per-cluster summary statistics (size, average risk score, dominant features) for dashboard display.

---

## 25. Risk Scoring

### 25.1 Composition
`risk_score (0–100)` = weighted combination of:
- Normalized anomaly score (primary driver).
- Clustering signal (noise-label boost / cluster-risk-profile boost).
- Correlation confidence (network-evidence strength).
- Optional supporting rule tags (bounded additive boost, capped so rules cannot dominate the ML-driven score).

`confidence (0–1)` reflects the reliability of the evidence itself (data completeness, number of observations, correlation confidence) — distinct from the risk_score.

### 25.2 Functional Requirements
- **FR-RISK-001**: The system shall compute a risk_score (0–100) per flagged entity from the weighted components above.
- **FR-RISK-002**: The system shall compute a confidence value (0–1) reflecting evidentiary completeness, separate from risk_score.
- **FR-RISK-003**: The weighting configuration shall be externally configurable (not hardcoded) to allow tuning without code changes.

---

## 26. Explainability

### 26.1 Requirement
The dashboard must always be able to answer: **"Why was this entity flagged?"**

### 26.2 Explanation Content
- Top contributing features (via SHAP values for the Isolation Forest model).
- Anomaly score and how it compares to the population distribution.
- Cluster membership and cluster profile summary.
- Graph evidence (subgraph snippet showing relevant edges).
- Network observation evidence (which IPs/observers, correlation confidence).
- Related transactions (list with amounts/timestamps).
- Temporal evidence (burst timeline chart).

### 26.3 SHAP Usage
SHAP (TreeExplainer-compatible, since Isolation Forest is tree-based) provides:
- **Global explanations**: which features drive anomaly scores across the whole dataset (model-level trust/debugging).
- **Instance-level explanations**: for one flagged wallet, which specific features pushed its score up (investigator-facing).

### 26.4 Human-Readable Layer
Because investigators should not need to interpret raw SHAP plots, the system also generates a plain-language reason list, e.g.:
> "Flagged mainly because: transaction velocity is 6.2× the population average; fan-out (41) is unusually high; observed from 5 independent network observers across 3 countries."

### 26.5 Functional Requirements
- **FR-EXP-001**: The system shall compute SHAP values for every flagged entity's anomaly score.
- **FR-EXP-002**: The system shall generate a plain-language top-N reasons list per alert from SHAP feature rankings.
- **FR-EXP-003**: The system shall display both the SHAP visualization and the plain-language explanation in the dashboard.

---

## 27. Alert Generation and Prioritization

### 27.1 Alert Object Schema
```json
{
  "alert_id": "ALT-000123",
  "entity_id": "wallet_9f3a...",
  "entity_type": "wallet",
  "risk_score": 91,
  "confidence": 0.88,
  "severity": "HIGH",
  "anomaly_score": 0.73,
  "cluster_id": 4,
  "top_contributing_features": ["tx_velocity", "fan_out", "num_observers_per_txid"],
  "supporting_transactions": ["txid_a1..", "txid_b2.."],
  "supporting_network_observations": ["obs_1001", "obs_1002"],
  "related_IPs": ["203.0.113.5", "198.51.100.9"],
  "related_wallets": ["wallet_2b.."],
  "countries": ["IN", "NL"],
  "ASNs": ["AS12345", "AS64500"],
  "timestamp": "2026-01-18T10:32:00Z",
  "model_version": "iforest_v3_2026-01-20"
}
```

### 27.2 Ranked Alert Example
```
Priority 1 — Wallet W123 — Risk Score: 91/100 — Confidence: 0.88
Reasons:
 • anomalous transaction velocity
 • high fan-out
 • high counterparty count
 • multiple independent network observations
 • unusual propagation timing
```

### 27.3 Traceability
Every alert field links back to concrete underlying rows (transaction records, observation records, graph edges) via stored IDs — nothing in an alert is a "black box" number; every field is reproducible by re-running the pipeline on the same dataset/model_version.

### 27.4 Functional Requirements
- **FR-ALT-001**: The system shall generate one alert object per entity whose risk_score exceeds a configurable threshold.
- **FR-ALT-002**: The system shall rank alerts by risk_score, with confidence as a tiebreaker.
- **FR-ALT-003**: The system shall persist full evidence linkage (transaction IDs, observation IDs) for every alert.
- **FR-ALT-004**: The system shall support alert status tracking (`new`, `under_review`, `dismissed`, `escalated`) set by investigators, not automatically.

---

## 28. Dashboard Requirements

1. **Overview**: total transactions, wallets, IPs, observed peers, total alerts, high-priority alert count (summary cards).
2. **Alert list**: sortable/filterable table (by severity, risk_score, confidence, status, date range).
3. **Entity investigation**: wallet profile — transaction history, counterparties, anomaly score trend, cluster membership, related IP observations.
4. **Transaction investigation**: TXID detail — inputs, outputs, amounts, fee, all network observations, propagation timeline chart.
5. **Graph/link analysis**: interactive IP↔TXID↔Wallet graph view (PyVis/Cytoscape.js embedded in Streamlit via HTML component), with node/edge highlighting and filtering by confidence/time.
6. **Network observation view**: observers, peers, IP list, propagation timeline for a selected TXID.
7. **Model information panel**: model_version, training period, sample count, algorithm, feature_version, evaluation metrics (precision/recall on injected anomalies, silhouette score).

### 28.1 Functional Requirements
- **FR-DASH-001**: The dashboard shall render all seven views above using locally bundled assets only (no CDN dependency).
- **FR-DASH-002**: The dashboard shall allow drilling from any alert directly into the entity, transaction, and graph views for that alert's evidence.
- **FR-DASH-003**: The dashboard shall visibly label all risk/anomaly figures with a tooltip clarifying they are not proof of criminal activity.

---

## 29. Offline Architecture

| Requires Internet (prep-time only) | Fully Offline (runtime) |
|---|---|
| Downloading Python packages (pip/conda) | Running ingestion, correlation, graph, ML inference |
| Downloading GeoIP database (GeoLite2 or equivalent) | Dashboard rendering |
| Cloning the code repository | Model training/retraining on local data |
| — | Alert generation and investigation workflows |
| — | Importing new operational datasets (via local file/removable media) |

- **NFR-OFF-001**: The system shall operate with zero outbound network calls during ingestion, correlation, ML inference, retraining, and dashboard use.
- **NFR-OFF-002**: All frontend assets (JS/CSS for graph visualization) shall be bundled locally, not fetched from a CDN.
- **NFR-OFF-003**: The GeoIP database shall be loaded from a local file path configured at setup time.

---

## 30. Operational Data Handling

New operational datasets are supplied post-deployment via local file drop or removable media. They pass through the same ingestion/validation/normalization pipeline as training data (Section 18), are analyzed immediately using the **current production model** (no auto-retraining on ingestion), and are archived for potential future retraining batches (Section 31).

- **FR-ARCH-001**: The system shall archive every ingested operational dataset with ingestion timestamp and source label.
- **FR-ARCH-002**: The system shall never modify the production model as a side effect of analyzing operational data — retraining is a separate, explicit action (Section 31).

---

## 31. Periodic Model Retraining & Drift Management

### 31.1 Controlled Batch Retraining Workflow
```
Operational data → validation → archival → feature extraction
   → merge with retained historical baseline → train candidate model
   → evaluate → compare with production model
   → deploy only if validation criteria met → retain previous model for rollback
```
This is **explicitly not** unrestricted real-time/online learning. Retraining is triggered manually by an administrator or on a scheduled cadence (e.g., weekly/monthly), never automatically per-record.

### 31.2 Drift Concepts
- **Data drift**: input feature distributions shift over time (e.g., typical transaction volume changes) — monitored via statistical distance metrics (e.g., population stability index) between recent operational features and the training baseline.
- **Concept drift**: the relationship between features and "anomalous" behavior itself changes (harder to detect unsupervised; monitored indirectly via rising false-positive/false-negative rates on injected-anomaly checks).
- **Model drift**: general term for degradation in model quality over time due to either of the above.

### 31.3 Safeguards Against Learning Suspicious Behavior as Normal
- Retraining candidate models are evaluated against a **held-out, untouched historical baseline** containing known injected anomalies (Section 42) — if the candidate model's detection quality on this fixed benchmark degrades, it fails evaluation and is not deployed.
- High-risk alerts marked `escalated` or `under_review` by investigators are **excluded from unlabeled retraining batches** (or down-weighted) so that active investigation subjects don't get "normalized" into the next model simply because they remain in the dataset.
- Baseline retention: a portion of original training-period data is always retained and blended with new data, preventing the model from drifting entirely toward newly observed patterns (mitigating catastrophic forgetting).

### 31.4 Versioning & Rollback
Every model is stored in a registry with a monotonically increasing `model_version`, its training window, feature_version, hyperparameters, and evaluation metrics. The dashboard's "Model Information" panel always shows the active version. Rollback is a simple registry pointer change back to a previous `model_version` — no retraining required.

### 31.5 Functional Requirements
- **FR-MLOPS-001**: The system shall support triggering a candidate retraining job on demand or on a configured schedule.
- **FR-MLOPS-002**: The system shall evaluate every candidate model against a fixed injected-anomaly benchmark before promotion.
- **FR-MLOPS-003**: The system shall only promote a candidate model to production if it meets or exceeds configured evaluation thresholds vs. the current production model.
- **FR-MLOPS-004**: The system shall retain all previous model versions and support one-step rollback.
- **FR-MLOPS-005**: The system shall exclude or down-weight entities under active investigation from unlabeled retraining data.

---

## 32. Model Versioning and Drift Management
(Consolidated into Section 31; Model Registry schema in Section 33.)

---

## 33. Database Design

### 33.1 Logical Schema (DuckDB/SQLite)

```
transactions(txid PK, timestamp, fee, script_type, feature_version, source_dataset_id FK)
transaction_inputs(id PK, txid FK -> transactions.txid, wallet_address, amount)
transaction_outputs(id PK, txid FK -> transactions.txid, wallet_address, amount)

network_observations(obs_id PK, txid FK -> transactions.txid, observer_id FK -> peers.peer_id,
                      src_ip, dst_ip, src_port, dst_port, timestamp, propagation_delay_ms)

peers(peer_id PK, ip, port, is_observer BOOLEAN, first_seen, last_seen)
ips(ip PK, first_seen, last_seen, asn FK -> asn_metadata.asn, country)
asn_metadata(asn PK, asn_org, country)

wallet_features(wallet_address PK, feature_version FK, tx_frequency, tx_velocity,
                 total_volume, avg_amount, amount_variance, fan_in, fan_out,
                 wallet_degree, unique_counterparties, computed_at)

network_features(ip PK, feature_version FK, ip_diversity, asn_diversity, geo_diversity,
                  observation_count, propagation_delay_avg, propagation_delay_var,
                  num_observers_per_txid_avg, computed_at)

graph_entities(entity_id PK, entity_type, first_seen, last_seen)
graph_relationships(rel_id PK, src_entity_id FK, dst_entity_id FK, rel_type,
                     weight, confidence, timestamp, metadata_json)

alerts(alert_id PK, entity_id FK -> graph_entities.entity_id, entity_type, risk_score,
       confidence, severity, anomaly_score, cluster_id, model_version FK,
       status, created_at, top_contributing_features_json,
       supporting_transactions_json, supporting_network_observations_json)

model_versions(model_version PK, algorithm, training_window_start, training_window_end,
               feature_version, hyperparameters_json, evaluation_metrics_json,
               is_production BOOLEAN, created_at)

operational_datasets(dataset_id PK, source_label, ingestion_timestamp, row_count,
                      rejected_row_count, status)

audit_logs(log_id PK, actor, action, target_entity, timestamp, details_json)
```

### 33.2 Key Relationships
- `transactions.txid` is referenced by `transaction_inputs`, `transaction_outputs`, `network_observations`.
- `network_observations.observer_id` references `peers`.
- `graph_relationships` references `graph_entities` twice (src/dst), forming the graph edge table.
- `alerts.model_version` references `model_versions`, ensuring every alert is reproducible against a specific model snapshot.

### 33.3 Indexing
Indexes on `transactions.timestamp`, `network_observations.txid`, `network_observations.timestamp`, `graph_relationships.src_entity_id`/`dst_entity_id`, `alerts.risk_score`, `alerts.status`.

---

## 34. API Design (FastAPI — optional service boundary)

| Endpoint | Purpose |
|---|---|
| `POST /datasets/import` | Upload/ingest a CSV/JSON/XML dataset; returns ingestion report |
| `GET /datasets` | List ingested datasets with status |
| `GET /transactions/{txid}` | Full transaction detail incl. observations |
| `GET /wallets/{wallet_id}` | Wallet profile, features, alerts |
| `GET /ips/{ip}` | IP profile, enrichment, associated observations |
| `GET /alerts` | Ranked/filterable alert list |
| `GET /alerts/{alert_id}` | Full alert detail incl. evidence + explanation |
| `GET /graph/entity/{entity_id}` | k-hop subgraph around an entity |
| `GET /models` | List model registry entries |
| `POST /models/train` | Trigger candidate model training |
| `POST /models/evaluate` | Evaluate a candidate model against benchmark |
| `POST /models/promote` | Promote an evaluated candidate to production |

Each endpoint validates input types/ranges (e.g., `wallet_id` format), returns structured error objects (`400` for invalid input, `404` for unknown entity, `422` for validation failure), and logs the request to `audit_logs` for administrative actions (train/evaluate/promote).

---

## 35. Use Cases

**UC-1: Data analyst imports dataset**
Actor: Analyst · Preconditions: file available locally · Trigger: `POST /datasets/import` or dashboard upload · Main flow: validate → normalize → enrich → store → ingestion report shown · Alternate: partial-row failure → report lists rejected rows · Postcondition: dataset available for correlation/graph/feature steps · Errors: missing required columns → import rejected.

**UC-2: Investigator searches TXID** → transaction detail + observation timeline shown; error if TXID not found.

**UC-3: Investigator searches wallet** → wallet profile, feature values, related alerts shown.

**UC-4: Investigator searches IP** → IP profile, enrichment, correlated TXIDs shown.

**UC-5: Investigator investigates an alert** → full evidence (Section 27.1 fields) + SHAP explanation + graph snippet shown; investigator sets status (`under_review`/`dismissed`/`escalated`).

**UC-6: Investigator explores graph** → interactive subgraph, filter by time/confidence/type, expand neighborhood.

**UC-7: Analyst reviews model performance** → Model Information panel: metrics, training window, version history.

**UC-8: Administrator performs model update** → trigger training → review evaluation report → promote or reject → rollback available.

**UC-9: System handles malformed dataset** → ingestion validates, quarantines bad rows, reports reasons, does not crash pipeline.

**UC-10: System works while completely offline** → all above use cases function with network interface disabled; verified in offline test suite (Section 41.10).

---

## 36. Data Flow Diagrams (Scenario Narratives)

1. **CSV import**: file → parser → validator → normalizer → GeoIP enrich → DuckDB store → ingestion report.
2. **JSON network observations import**: file → schema check → normalizer → join-key (txid) check → store in `network_observations`.
3. **Correlate observation with TXID**: observation row + transaction row → temporal window check → confidence scoring → `graph_relationships` (IP→TXID edge) written.
4. **Construct graph**: read transactions + observations + enrichment → build nodes/edges → persist `graph_entities`/`graph_relationships`.
5. **Run anomaly detection**: read `wallet_features` → scale → Isolation Forest inference → anomaly_score written.
6. **Generate explainable alert**: anomaly_score + cluster_id + correlation confidence → risk scoring → SHAP explanation → `alerts` row created.
7. **Investigator opens alert**: dashboard queries `alerts` + linked transactions/observations/graph → renders evidence views.
8. **New operational dataset offline import**: file copied via removable media → same ingestion path as (1)/(2) → archived in `operational_datasets`.
9. **Candidate model trained**: merged historical+operational features → `POST /models/train` → new `model_versions` row (is_production=false).
10. **Model evaluated and promoted**: `POST /models/evaluate` runs benchmark → metrics stored → if thresholds met, `POST /models/promote` flips `is_production` flag, previous version retained for rollback.

---

## 37. Graph/Data Relationships
(See Section 20 for full node/edge catalogue and Section 33.1 for physical schema — intentionally consolidated to avoid duplication.)

---

## 38. Security and Privacy

- Local access control: dashboard/API optionally protected by a local username/password or OS-level access restriction (no cloud auth dependency).
- Data integrity: checksums on imported datasets; audit log entries are append-only.
- Secure storage: database file permissions restricted to the application user.
- Minimal collection: only fields defined in the schema are stored; no extraneous personal data collected (synthetic data has none regardless).
- Audit logs: every administrative action (import, train, evaluate, promote, rollback, status change) is logged with actor, timestamp, and details.

## 39. Ethical and Legal Considerations

- **Human-in-the-loop only**: the system never takes automatic enforcement action (no auto-freezing, no auto-reporting).
- **No automatic accusation**: outputs are always framed as leads requiring human review, using the terminology discipline in Section 22.5.
- **Explicit limitation of IP attribution**: documented prominently in the dashboard, this SRS, and the technical write-up — an IP correlated with a TXID is not proof of ownership.
- **False positive/negative handling**: every alert can be dismissed with a reason, feeding back into administrator awareness (not automatically into model retraining, to avoid contamination — Section 31.3).
- **Synthetic data only**: the prototype never processes real seized data; this is stated in the write-up and enforced by only accepting datasets matching the synthetic schema.

---

## 40. Non-Functional Requirements

| ID | Category | Requirement |
|---|---|---|
| NFR-PERF-001 | Performance | The system shall ingest a 100,000-row CSV dataset in under 60 seconds on a standard laptop (4-core, 8GB RAM). |
| NFR-PERF-002 | Performance | Alert-list dashboard queries shall return within 2 seconds for up to 50,000 alerts. |
| NFR-PERF-003 | Performance | Graph rendering for a 500-node/1500-edge subgraph shall render within 3 seconds. |
| NFR-PERF-004 | Performance | ML inference (Isolation Forest) shall score 100,000 entities in under 30 seconds. |
| NFR-SCALE-001 | Scalability | The system shall support at least 1,000,000 transactions and 500,000 network observations in a single DuckDB/SQLite database without redesign. |
| NFR-REL-001 | Reliability | Malformed rows shall never crash the ingestion pipeline; they shall be quarantined and reported. |
| NFR-REL-002 | Reliability | Model rollback shall complete in under 5 seconds (registry pointer change only). |
| NFR-USAB-001 | Usability | An investigator shall be able to reach full evidence for any alert within 3 clicks from the alert list. |
| NFR-OFF-001..003 | Offline | See Section 29. |
| NFR-MAINT-001 | Maintainability | Each module (Section 21 list) shall be independently unit-testable with no more than one direct dependency on another module's internals. |
| NFR-PORT-001 | Portability | The system shall run on any mainstream Linux distribution (Ubuntu/Debian-based) with Python 3.10+. |
| NFR-SEC-001 | Security | Local access control shall be configurable and enabled by default in production configuration. |

---

## 41. Testing Strategy

1. **Unit testing**: per-module functions (parsers, feature calculators, scoring formulas) — pytest.
2. **Integration testing**: ingestion → correlation → graph pipeline end-to-end on a small fixture dataset.
3. **Data validation testing**: malformed CSV/JSON/XML fixtures verify quarantine behavior.
4. **Correlation testing**: synthetic cases with known TXID/observation pairs verify expected confidence scores.
5. **Graph testing**: verify node/edge counts and types against a hand-constructed small graph fixture.
6. **ML testing**: verify Isolation Forest flags injected synthetic anomalies (Section 42) above a threshold.
7. **Explainability testing**: verify SHAP values sum consistently with model output (additivity property) and that plain-language reasons reference real top features.
8. **Dashboard testing**: Streamlit component smoke tests; verify alert drill-down links resolve to correct entities.
9. **Performance testing**: benchmark ingestion/inference against NFR-PERF targets using generated large synthetic datasets.
10. **Offline testing**: run full test suite with network interface disabled/firewalled to confirm zero external calls.
11. **Model regression testing**: candidate model must not underperform the fixed injected-anomaly benchmark relative to production model before promotion.
12. **End-to-end testing**: full scenario from dataset import to alert display in the dashboard, automated where feasible (e.g., via Streamlit testing utilities or a scripted pipeline run + DB assertions).

Example test case: *"Given a synthetic wallet with fan_out artificially set to 50 (population mean 3, std 2), the Isolation Forest anomaly_score for that wallet shall rank in the top 1% of all scored entities."*

---

## 42. ML Evaluation Strategy

Because the primary detector is unsupervised, evaluation relies on:
- **Synthetic ground truth via injected anomalies**: a known set of synthetic "anomalous" behavioral patterns (Section 23.1) is deliberately injected into validation/test data at known entity IDs.
- **Precision/Recall/False-Positive-Rate/False-Negative-Rate** computed against this injected ground truth (not against real-world labels, which don't exist for a synthetic prototype).
- **ROC-AUC/PR-AUC** computed only where the injected-label distribution makes it meaningful (reasonably balanced or at least non-trivial positive rate).
- **Silhouette score** for clustering quality (HDBSCAN/DBSCAN).
- **Cluster stability**: re-running clustering on bootstrapped samples and measuring label agreement (e.g., Adjusted Rand Index) across runs.
- **Anomaly ranking quality**: precision@K for the top-K ranked alerts.
- **Temporal holdout evaluation**: always evaluate on a later time period than training (Section 15.3), never randomly-split rows.

**Explicit limitation**: because there is no real-world "ground truth" label for actual criminal behavior in a synthetic dataset, these metrics measure the model's ability to detect *known injected statistical anomalies* — they are a proxy for, not a guarantee of, real-world detection performance. This limitation is stated in the technical write-up.

---

## 43. Requirements Traceability Matrix (excerpt)

| SIH Objective | Feature | Requirement IDs | Module | Test Case | Dashboard Evidence |
|---|---|---|---|---|---|
| 1. Ingest bulk metadata | Ingestion/validation/normalization | FR-ING-001..004, FR-NORM-001..003 | Data Ingestion, Normalization | Data validation tests | Overview panel row counts |
| 2. Entity/transaction graph | Graph construction | FR-GRA-001..005 | Graph Construction Engine | Graph tests | Graph/link analysis view |
| 3. Working ML model | Anomaly detection + clustering | FR-ML-001..005, FR-CLUS-001..002 | Anomaly Detection Engine, Clustering Engine | ML tests | Model information panel |
| 4. Explainable ranked alerts | Risk scoring + explainability + alerts | FR-RISK-001..003, FR-EXP-001..003, FR-ALT-001..004 | Risk Scoring, Explainability, Alert Management | Explainability tests | Alert list + entity investigation view |
| 5. Dashboard/visualization | Dashboard | FR-DASH-001..003 | Dashboard/Visualization Module | Dashboard tests | All dashboard views |
| Offline operation | Offline architecture | NFR-OFF-001..003 | All modules | Offline tests | Model info shows no network calls |
| Controlled retraining | MLOps pipeline | FR-MLOPS-001..005 | Model Training/Evaluation/Registry | Model regression tests | Model information panel version history |

---

## 44. Deployment Architecture

Single Linux host, Python virtual environment (`venv`), local DuckDB/SQLite file, local GeoIP database file, Streamlit app served on `localhost` (optionally exposed on LAN only for demo). Optional Docker container acceptable **only if** the image is pre-built/exported and loaded offline (`docker load`), preserving the no-Internet-at-runtime requirement. No orchestration layer.

## 45. Implementation Roadmap

| Phase | Deliverable | Dependencies | Technologies | Acceptance Criteria |
|---|---|---|---|---|
| 1 | Dataset schema + synthetic data generator | — | Python, Faker/custom generator | Generates valid CSV/JSON matching schema |
| 2 | P2P topology + propagation simulator | Phase 1 | NetworkX, asyncio/SimPy | Produces observation records with plausible delays |
| 3 | Ingestion + normalization | Phase 1 | Pandas/Polars, DuckDB/SQLite | Passes ingestion test suite |
| 4 | Correlation engine | Phase 3 | Python | Correlation confidence matches hand-verified fixtures |
| 5 | Graph construction | Phase 4 | NetworkX | Graph node/edge counts match fixture expectations |
| 6 | Feature engineering | Phase 5 | Pandas | Feature table populated and versioned |
| 7 | ML anomaly detection | Phase 6 | scikit-learn (IsolationForest) | Injected anomalies rank in top percentile |
| 8 | Entity clustering | Phase 6 | scikit-learn/HDBSCAN | Silhouette score above baseline threshold |
| 9 | Explainability + risk scoring | Phases 7–8 | SHAP | Every alert has non-empty top-feature explanation |
| 10 | Dashboard | Phases 1–9 | Streamlit, Plotly, PyVis | All 7 dashboard views functional |
| 11 | Periodic retraining/model management | Phases 7–10 | joblib, FastAPI (optional) | Promote/rollback demonstrated end-to-end |
| 12 | Testing + offline Linux deployment | All | pytest | Full suite passes with network disabled |

## 46. MVP Scope

**MVP (must-have for SIH demo):** CSV/JSON ingestion, GeoIP enrichment, synthetic propagation simulator, correlation engine, graph construction, core feature set, Isolation Forest anomaly detection, basic DBSCAN clustering, risk scoring, SHAP-based alert explanation, Streamlit dashboard with Overview + Alert List + Entity Investigation + Graph view, offline operation.

## 47. Future Enhancements (Advanced, time-permitting)

XML ingestion support, HDBSCAN (vs. DBSCAN) for adaptive density clustering, Cytoscape.js-based richer graph UI, FastAPI service layer with authentication, automated scheduled retraining, model drift dashboards (PSI charts), multi-user role-based access control, export-to-PDF investigation reports.

## 48. Limitations

- All data is synthetic; findings do not represent real-world criminal activity.
- Any single deployment observes only its own simulated/provided observer set — never the full Bitcoin network.
- An IP address correlated with a TXID does **not** prove wallet ownership.
- NAT, VPNs, Tor, and shared/dynamic IPs mean multiple wallets can appear behind one IP, and one wallet's traffic can appear from many IPs over time — correlation confidence must be read with this in mind.
- Propagation observations are inherently partial; absence of an observation is not evidence of absence of a transaction.
- Timestamp uncertainty (clock skew, logging granularity) affects temporal-proximity correlation.
- Unsupervised anomaly detection produces false positives (unusual-but-legitimate behavior, e.g., exchange hot wallets) and false negatives (well-disguised laundering that mimics normal statistical patterns).
- Adversarial actors aware of the detection logic could adapt behavior to evade it.
- Any retrained model can drift if operational data distributions shift substantially from the training baseline; mitigations exist (Section 31.3) but do not eliminate the risk.
- Synthetic-data bias: patterns learned from a synthetic generator may not generalize perfectly to real transaction data.

## 49. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Team underestimates ML scope, defaults to rule-only detection | MVP explicitly requires a trained Isolation Forest; graded/reviewed against FR-ML-005 |
| Dashboard graph view too slow with large graphs | Cap default subgraph size, allow filtering before rendering |
| Overclaiming attribution in demo narrative | Fixed terminology discipline (Section 22.5) + UI tooltips (FR-DASH-003) |
| Retraining silently "normalizes" flagged behavior | Exclusion/down-weighting safeguard (FR-MLOPS-005) |
| Offline requirement broken by a stray library needing network | Explicit offline test (Section 41.10) run before every milestone demo |

## 50. Acceptance Criteria

- All FR/NFR IDs in Sections 17–40 implemented and covered by at least one automated test.
- End-to-end demo: import synthetic dataset → view populated dashboard with ranked, explainable alerts → drill into one alert to see full evidence chain → trigger a retraining cycle → promote or roll back a model — all without any network connection.
- No output string in the system claims wallet ownership, network omniscience, or confirmed criminality.

## 51. Conclusion

ChainWatch demonstrates that a genuinely ML-driven, explainable, and evidence-traceable Bitcoin transaction monitoring prototype can be built by a student team using a lightweight, entirely offline Python stack. By strictly separating network-layer observation from blockchain-layer fact, modeling correlation confidence rather than attribution, and enforcing careful terminology throughout, the system satisfies every objective in the SIH problem statement while remaining honest about the real limitations of partial network visibility and unsupervised anomaly detection — producing prioritized investigative leads for human review, not automated verdicts.

---

*End of SRS.*
