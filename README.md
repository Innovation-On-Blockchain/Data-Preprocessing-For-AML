# Ethereum AML Data Collection and Graph Construction

## Overview

This repository implements **Step 1** of an Ethereum anti-money laundering (AML) research pipeline:
**data collection, labeling, and graph construction**.

The goal of this stage is to build a **clean, reproducible, and well-scoped transaction graph dataset** from public Ethereum data and regulatory sanctions lists.
All machine learning and modeling tasks are intentionally out of scope for this repository.

The output of this pipeline is a set of **schema-defined Parquet files** that serve as the canonical data source for downstream analysis and Python-based GNN training.

---

## Objectives

This project aims to:

- Collect sanctioned and high-risk Ethereum addresses from official regulatory sources
- Extract a controlled slice of Ethereum transaction data relevant to those addresses
- Construct a directed, aggregated transaction graph
- Enrich nodes with contract/EOA classification and degree metadata
- Produce reproducible, portable datasets suitable for graph learning workflows

The design prioritizes **clarity, reproducibility, and controlled scope** over exhaustive data coverage.

---

## Pipeline Stages

The pipeline runs in four sequential stages. Each stage writes its output to disk, and the pipeline supports crash recovery and resumability between stages.

| Stage | Description | Output |
|-------|-------------|--------|
| 1. Fetch Labels | Download OFAC-sanctioned Ethereum addresses | `data/labels/labels.parquet` |
| 2. Fetch Transactions | Collect transactions for labeled addresses and their top-K 1-hop counterparties | `data/raw/transactions_raw.parquet` |
| 3. Aggregate | Aggregate raw transactions into weekly directed edges | `data/processed/edges_weekly.parquet` |
| 4. Build Nodes | Generate node metadata (contract detection, degree, hub classification) | `data/processed/nodes.parquet` |

### Crash Recovery

- **Stage 2** writes counterparty transactions incrementally to chunk files (`data/raw/counterparty_chunks/`) with a `progress.json` checkpoint. If the process is interrupted, restarting resumes from the last completed chunk.
- **Stage 4** supports a disk cache (`data/metadata/contract_cache.csv`) for RPC-based contract detection, allowing interrupted runs to skip already-resolved addresses.

---

## Data Sources

### Address Labels

- **OFAC Specially Designated Nationals (SDN) List**
  U.S. Department of the Treasury, Office of Foreign Assets Control
  https://sanctionslist.ofac.treas.gov/Home/SdnList

Only Ethereum addresses explicitly listed in official regulatory materials are treated as labeled positives.
Unlisted addresses are considered unlabeled rather than benign.

### Transaction Data

- **Ethereum Mainnet**
- Accessed via **Alchemy RPC APIs** (`getAssetTransfers`)
- Supports multiple API keys with automatic rotation on rate-limit (429) errors

### Contract Detection

Contract vs. EOA classification can be performed via:

1. **RPC-based** (default): Calls `eth_getCode` for each address via Alchemy. Slow for large address sets but always up-to-date.
2. **Local CSV lookup** (`--contracts-dir`): Loads pre-exported contract addresses (e.g., from Google BigQuery's `bigquery-public-data.crypto_ethereum.contracts` table). Completes in seconds with no API calls.
3. **Offline mode** (`--offline`): Skips contract detection entirely; marks all addresses as EOA.

---

## Dataset Scope

To keep the dataset feasible and interpretable, the pipeline operates on a **restricted network slice**:

- Start from labeled (sanctioned) addresses
- Include transactions involving those addresses
- Expand to **one-hop counterparties** (top-K per labeled address, ranked by transaction count, total value, or recency)
- Restrict to a **fixed time window** (configurable, default 600 weeks)
- Known high-volume exchange and DEX router addresses are excluded from counterparty expansion to reduce noise
- A pagination safety limit (100K transfers per address per block range) caps data from ultra-high-volume addresses

---

## Output Datasets

All outputs are written in **Parquet format** with explicit schemas.

### `labels.parquet`

Sanctioned address labels.

| Column        | Type    | Description |
|--------------|---------|-------------|
| address      | string  | Ethereum address (EIP-55 checksummed) |
| label        | int     | 1 = sanctioned |
| label_source | string  | Source identifier (e.g., OFAC_SDN) |
| source_url   | string  | Reference URL |
| retrieved_at | string  | UTC timestamp |

### `transactions_raw.parquet`

Raw transaction records for the selected slice (labeled + counterparty transactions, deduplicated).

| Column          | Type    | Description |
|-----------------|---------|-------------|
| tx_hash         | string  | Transaction hash |
| block_timestamp | string  | ISO-8601 UTC timestamp (e.g., `2015-08-07T05:01:09+00:00`) |
| from_address    | string  | Sender address |
| to_address      | string  | Receiver address |
| value_wei       | string  | Transferred value in wei |

### `edges_weekly.parquet`

Aggregated directed transaction graph with weekly time windows.

| Column            | Type    | Description |
|-------------------|---------|-------------|
| src               | string  | Sender address |
| dst               | string  | Receiver address |
| week_start        | string  | Aggregation window start date |
| tx_count          | int64   | Number of transactions in window |
| total_value_wei   | string  | Sum of transferred value in wei |

### `nodes.parquet`

Node-level metadata for every unique address in the graph.

| Column      | Type    | Description |
|-------------|---------|-------------|
| address     | string  | Ethereum address |
| is_contract | bool    | True if smart contract |
| degree_in   | int64   | Number of unique incoming counterparties |
| degree_out  | int64   | Number of unique outgoing counterparties |
| node_type   | string  | `eoa`, `contract`, or `hub` (top 0.1% by total degree) |

---

## Usage

### Prerequisites

- Rust toolchain (stable)
- One or more Alchemy API keys (set in `.env`)

### Configuration

Create a `.env` file with your Alchemy API keys:

```
ALCHEMY_API_KEY_1=your_key_here
ALCHEMY_API_KEY_2=your_key_here
```

The pipeline rotates keys automatically on rate-limit errors.

### Build

```bash
cargo build --release
```

### Run the Full Pipeline

```bash
./target/release/eth-aml-pipeline run
```

Options:
- `-w, --weeks <N>` — Time window in weeks (default: 600)
- `-k, --top-k <N>` — Top-K counterparties per labeled address (default: 500)
- `--skip-transactions` — Skip transaction fetching (use existing data)
- `--skip-nodes` — Skip node building (use existing data)
- `--strict` — Fail if scale limits are exceeded

### Run Individual Stages

```bash
# Fetch labels only
./target/release/eth-aml-pipeline fetch-labels

# Fetch transactions only
./target/release/eth-aml-pipeline fetch-transactions

# Aggregate transactions into edges
./target/release/eth-aml-pipeline aggregate

# Build node metadata (with local contract CSV)
./target/release/eth-aml-pipeline build-nodes --contracts-dir path/to/csv_directory/

# Build node metadata (offline, no contract detection)
./target/release/eth-aml-pipeline build-nodes --offline

# Check pipeline status
./target/release/eth-aml-pipeline status
```

---

## Project Structure

```
src/
  main.rs               — CLI entry point and pipeline orchestration
  config.rs             — Configuration, API key pool, rate limit settings
  fetch_labels.rs       — OFAC SDN label fetching and parsing
  fetch_transactions.rs — Transaction collection with chunk-based crash recovery
  aggregate_graph.rs    — Weekly edge aggregation
  build_nodes.rs        — Node metadata generation and contract detection
  rpc.rs                — Alchemy RPC client with retry, key rotation, and batching
  ethereum.rs           — Address validation utilities
  schemas.rs            — Parquet schema definitions and data types
  lib.rs                — Library re-exports

data/
  labels/               — Sanctioned address labels
  raw/                  — Raw transaction data and chunk files
  processed/            — Aggregated edges and node metadata
  metadata/             — Run metadata and caches
```

---

## Dataset Statistics (Current Run)

| Metric | Count |
|--------|-------|
| Labeled addresses (OFAC) | 82 |
| 1-hop counterparties | 3,213 |
| Raw transactions | 90,728,163 |
| Weekly edges | 60,706,382 |
| Unique addresses (nodes) | 33,895,542 |
| Contracts | 721,891 |
| EOAs | 33,173,651 |
| Hubs (top 0.1%) | 34,925 |
