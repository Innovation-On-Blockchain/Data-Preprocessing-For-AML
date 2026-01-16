//! Ethereum AML Data Pipeline Library
//!
//! A production-quality Rust pipeline for collecting and labeling Ethereum
//! transaction data for Anti-Money Laundering (AML) analysis.
//!
//! # Pipeline Stages
//!
//! 1. **Label Collection** ([`fetch_labels`]): Fetches sanctioned addresses from OFAC SDN list
//! 2. **Transaction Collection** ([`fetch_transactions`]): Collects transactions for labeled addresses and 1-hop counterparties
//! 3. **Graph Aggregation** ([`aggregate_graph`]): Aggregates transactions into weekly edge summaries
//! 4. **Node Metadata** ([`build_nodes`]): Builds node metadata including contract detection and hub classification
//!
//! # Output Files
//!
//! - `labels.parquet`: Labeled addresses with provenance
//! - `transactions_raw.parquet`: Raw transaction data
//! - `edges_weekly.parquet`: Aggregated edge data
//! - `nodes.parquet`: Node metadata
//!
//! # Example
//!
//! ```no_run
//! use eth_aml_pipeline::config::PipelineConfig;
//! use eth_aml_pipeline::fetch_labels::OfacFetcher;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = PipelineConfig::load()?;
//!     let fetcher = OfacFetcher::new();
//!     let labels = fetcher.fetch_labels().await?;
//!     println!("Fetched {} sanctioned addresses", labels.len());
//!     Ok(())
//! }
//! ```

pub mod aggregate_graph;
pub mod build_nodes;
pub mod config;
pub mod ethereum;
pub mod fetch_labels;
pub mod fetch_transactions;
pub mod rpc;
pub mod schemas;

// Re-export commonly used types
pub use config::PipelineConfig;
pub use schemas::{LabeledAddress, NodeMetadata, RawTransaction, RunMetadata, WeeklyEdge};
