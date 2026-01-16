//! Data schemas for the AML pipeline.
//!
//! All schemas are defined as Rust structs with Parquet serialization support.
//! This module serves as the canonical schema definition for the entire pipeline.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// Schema version for tracking changes
pub const SCHEMA_VERSION: &str = "1.0.0";

// ============================================================================
// PART A: Label Schema
// ============================================================================

/// Labeled Ethereum address from regulatory sources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LabeledAddress {
    /// EIP-55 checksummed Ethereum address
    pub address: String,

    /// Label value (1 = sanctioned, future: other labels)
    pub label: i32,

    /// Source identifier (e.g., "OFAC_SDN")
    pub label_source: String,

    /// URL where data was retrieved
    pub source_url: String,

    /// Timestamp of data retrieval (UTC ISO-8601)
    pub retrieved_at: DateTime<Utc>,
}

impl LabeledAddress {
    pub const LABEL_SANCTIONED: i32 = 1;
}

// ============================================================================
// PART B: Transaction Schema
// ============================================================================

/// Raw Ethereum transaction (minimal fields for AML analysis)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTransaction {
    /// Transaction hash (0x-prefixed, lowercase)
    pub tx_hash: String,

    /// Block timestamp (UTC)
    pub block_timestamp: DateTime<Utc>,

    /// Sender address (EIP-55 checksummed)
    pub from_address: String,

    /// Recipient address (EIP-55 checksummed)
    pub to_address: String,

    /// Transaction value in wei (stored as string for u256 compatibility)
    pub value_wei: String,
}

// ============================================================================
// PART C: Edge Schema (Aggregated Graph)
// ============================================================================

/// Aggregated edge in the transaction graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeeklyEdge {
    /// Source address (EIP-55 checksummed)
    pub src: String,

    /// Destination address (EIP-55 checksummed)
    pub dst: String,

    /// Start of aggregation period
    pub week_start: NaiveDate,

    /// Number of transactions in period
    pub tx_count: i64,

    /// Total value transferred in wei (string for u128+ values)
    pub total_value_wei: String,
}

// ============================================================================
// PART D: Node Schema
// ============================================================================

/// Node metadata for graph analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    /// EIP-55 checksummed address
    pub address: String,

    /// Whether address is a contract
    pub is_contract: bool,

    /// Number of incoming edges (unique senders)
    pub degree_in: i64,

    /// Number of outgoing edges (unique recipients)
    pub degree_out: i64,

    /// Node classification
    pub node_type: NodeType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    /// Externally Owned Account
    Eoa,
    /// Smart Contract
    Contract,
    /// High-degree node (top 0.1% by total degree)
    Hub,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Eoa => write!(f, "eoa"),
            NodeType::Contract => write!(f, "contract"),
            NodeType::Hub => write!(f, "hub"),
        }
    }
}

// ============================================================================
// Metadata Schema
// ============================================================================

/// Run metadata for reproducibility and auditing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetadata {
    /// Schema version used
    pub schema_version: String,

    /// Pipeline run timestamp
    pub run_timestamp: DateTime<Utc>,

    /// Start of data collection window
    pub window_start: DateTime<Utc>,

    /// End of data collection window
    pub window_end: DateTime<Utc>,

    /// Record counts by type
    pub record_counts: RecordCounts,

    /// Git commit hash (if available)
    pub git_commit: Option<String>,

    /// Pipeline version
    pub pipeline_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecordCounts {
    pub labeled_addresses: usize,
    pub raw_transactions: usize,
    pub weekly_edges: usize,
    pub nodes: usize,
}

impl RunMetadata {
    pub fn new(window_start: DateTime<Utc>, window_end: DateTime<Utc>) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            run_timestamp: Utc::now(),
            window_start,
            window_end,
            record_counts: RecordCounts::default(),
            git_commit: get_git_commit(),
            pipeline_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

fn get_git_commit() -> Option<String> {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        })
}

// ============================================================================
// Polars Schema Definitions
// ============================================================================

use polars::prelude::*;

/// Create Polars schema for labels.parquet
pub fn labels_schema() -> Schema {
    Schema::from_iter([
        Field::new("address".into(), DataType::String),
        Field::new("label".into(), DataType::Int32),
        Field::new("label_source".into(), DataType::String),
        Field::new("source_url".into(), DataType::String),
        Field::new("retrieved_at".into(), DataType::String), // ISO-8601 string
    ])
}

/// Create Polars schema for transactions_raw.parquet
pub fn transactions_schema() -> Schema {
    Schema::from_iter([
        Field::new("tx_hash".into(), DataType::String),
        Field::new("block_timestamp".into(), DataType::String), // ISO-8601 string
        Field::new("from_address".into(), DataType::String),
        Field::new("to_address".into(), DataType::String),
        Field::new("value_wei".into(), DataType::String), // String for u256
    ])
}

/// Create Polars schema for edges_weekly.parquet
pub fn edges_schema() -> Schema {
    Schema::from_iter([
        Field::new("src".into(), DataType::String),
        Field::new("dst".into(), DataType::String),
        Field::new("week_start".into(), DataType::Date),
        Field::new("tx_count".into(), DataType::Int64),
        Field::new("total_value_wei".into(), DataType::String), // String for u128+
    ])
}

/// Create Polars schema for nodes.parquet
pub fn nodes_schema() -> Schema {
    Schema::from_iter([
        Field::new("address".into(), DataType::String),
        Field::new("is_contract".into(), DataType::Boolean),
        Field::new("degree_in".into(), DataType::Int64),
        Field::new("degree_out".into(), DataType::Int64),
        Field::new("node_type".into(), DataType::String),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_type_display() {
        assert_eq!(NodeType::Eoa.to_string(), "eoa");
        assert_eq!(NodeType::Contract.to_string(), "contract");
        assert_eq!(NodeType::Hub.to_string(), "hub");
    }

    #[test]
    fn test_schema_version() {
        assert!(!SCHEMA_VERSION.is_empty());
    }
}
