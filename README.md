# Ethereum AML Data Collection and Graph Construction

## Overview

This repository implements **Step 1** of an Ethereum anti-money laundering (AML) research pipeline:  
**data collection, labeling, and graph construction**.

The goal of this stage is to build a **clean, reproducible, and well-scoped transaction graph dataset** from public Ethereum data and regulatory sanctions lists.  
All machine learning and modeling tasks are intentionally out of scope for this repository.

The output of this pipeline is a set of **schema-defined Parquet files** that serve as the canonical data source for downstream analysis and Python-based training.

---

## Objectives

This project aims to:

- Collect sanctioned and high-risk Ethereum addresses from official regulatory sources
- Extract a controlled slice of Ethereum transaction data relevant to those addresses
- Construct a directed, aggregated transaction graph
- Produce reproducible, portable datasets suitable for graph learning workflows

The design prioritizes **clarity, reproducibility, and controlled scope** over exhaustive data coverage.

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
- Accessed via **Alchemy RPC APIs**
- Transaction semantics follow official Etherscan documentation  
  https://info.etherscan.com/viewing-transactions-on-etherscan/

---

## Dataset Scope

To keep the dataset feasible and interpretable, the pipeline operates on a **restricted network slice**:

- Start from labeled (sanctioned) addresses
- Include transactions involving those addresses
- Expand to **one-hop counterparties only**
- Restrict to a **fixed time window** (configurable)
- Include **normal ETH transfers only**
- Exclude token transfers, logs, traces, and failed transactions

This design ensures a meaningful AML-relevant dataset while avoiding unbounded data growth.

---

## Output Datasets

All outputs are written in **Parquet format** with compression and explicit schemas.

### `labels.parquet`

Sanctioned address labels.

| Column        | Type    | Description |
|--------------|---------|-------------|
| address      | string  | Ethereum address (EIP-55 checksummed) |
| label        | int     | 1 = sanctioned |
| label_source | string  | Source identifier (e.g., OFAC_SDN) |
| source_url   | string  | Reference URL |
| retrieved_at | string  | UTC timestamp |

---

### `transactions_raw.parquet` (temporary)

Raw transaction records for the selected slice.

| Column          | Type    | Description |
|-----------------|---------|-------------|
| tx_hash         | string  | Transaction hash |
| block_timestamp | string  | Block timestamp |
| from_address    | string  | Sender address |
| to_address      | string  | Receiver address |
| value_wei       | uint128 | Transferred value |

This file is considered **intermediate** and may be deleted after aggregation.

---

### `edges_weekly.parquet`

Aggregated directed transaction graph.

| Column            | Type    | Description |
|-------------------|---------|-------------|
| src               | string  | Sender address |
| dst               | string  | Receiver address |
| week_start        | date    | Aggregation window |
| tx_count          | int     | Number of transactions |
| total_value_wei   | uint128 | Sum of transferred value |

---

### `nodes.parquet`

Node-level metadata.

| Column      | Type    | Description |
|-------------|---------|-------------|
| address     | string  | Ethereum address |
| is_contract | bool    | True if smart contract |
| degree_in   | int     | Incoming degree |
| degree_out  | int     | Outgoing degree |
| node_type   | string  | eoa, contract, or hub |

High-degree nodes (e.g., exchanges or mixers) are identified heuristically and labeled as hubs to avoid dominance effects.

---

## Project Structure

