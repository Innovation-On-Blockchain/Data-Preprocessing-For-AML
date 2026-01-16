//! Configuration management for the AML pipeline.
//!
//! Supports loading from environment variables, config files, and CLI arguments.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Main pipeline configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Alchemy API key for Ethereum RPC
    pub alchemy_api_key: String,

    /// Base URL for Alchemy (mainnet)
    #[serde(default = "default_alchemy_url")]
    pub alchemy_base_url: String,

    /// Time window configuration
    pub time_window: TimeWindowConfig,

    /// Rate limiting configuration
    #[serde(default)]
    pub rate_limits: RateLimitConfig,

    /// Output directory paths
    #[serde(default)]
    pub paths: PathConfig,

    /// Aggregation settings
    #[serde(default)]
    pub aggregation: AggregationConfig,

    /// Scale control settings for V2 GNN-ready datasets
    #[serde(default)]
    pub scale: ScaleConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindowConfig {
    /// Start of collection window (ISO-8601)
    pub start: DateTime<Utc>,

    /// End of collection window (ISO-8601)
    pub end: DateTime<Utc>,
}

impl Default for TimeWindowConfig {
    fn default() -> Self {
        let end = Utc::now();
        let start = end - Duration::weeks(8);
        Self { start, end }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum requests per second
    #[serde(default = "default_rps")]
    pub requests_per_second: u32,

    /// Batch size for RPC calls
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Base delay for exponential backoff (ms)
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: default_rps(),
            batch_size: default_batch_size(),
            max_retries: default_max_retries(),
            base_delay_ms: default_base_delay_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    #[serde(default = "default_raw_dir")]
    pub raw_dir: PathBuf,

    #[serde(default = "default_processed_dir")]
    pub processed_dir: PathBuf,

    #[serde(default = "default_labels_dir")]
    pub labels_dir: PathBuf,

    #[serde(default = "default_metadata_dir")]
    pub metadata_dir: PathBuf,
}

impl Default for PathConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            raw_dir: default_raw_dir(),
            processed_dir: default_processed_dir(),
            labels_dir: default_labels_dir(),
            metadata_dir: default_metadata_dir(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationConfig {
    /// Granularity for time aggregation
    #[serde(default = "default_granularity")]
    pub granularity: AggregationGranularity,

    /// Percentile threshold for hub classification (e.g., 99.9 = top 0.1%)
    #[serde(default = "default_hub_percentile")]
    pub hub_percentile: f64,
}

impl Default for AggregationConfig {
    fn default() -> Self {
        Self {
            granularity: default_granularity(),
            hub_percentile: default_hub_percentile(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AggregationGranularity {
    Daily,
    Weekly,
    Monthly,
}

impl Default for AggregationGranularity {
    fn default() -> Self {
        Self::Weekly
    }
}

// Default value functions
fn default_alchemy_url() -> String {
    "https://eth-mainnet.g.alchemy.com/v2".to_string()
}

fn default_rps() -> u32 {
    25 // Conservative for free tier
}

fn default_batch_size() -> usize {
    100
}

fn default_max_retries() -> u32 {
    5
}

fn default_base_delay_ms() -> u64 {
    1000
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("data")
}

fn default_raw_dir() -> PathBuf {
    PathBuf::from("data/raw")
}

fn default_processed_dir() -> PathBuf {
    PathBuf::from("data/processed")
}

fn default_labels_dir() -> PathBuf {
    PathBuf::from("data/labels")
}

fn default_metadata_dir() -> PathBuf {
    PathBuf::from("data/metadata")
}

fn default_granularity() -> AggregationGranularity {
    AggregationGranularity::Weekly
}

fn default_hub_percentile() -> f64 {
    99.9
}

// ============================================================================
// V2 Scale Control Configuration
// ============================================================================

/// Scale control settings for V2 GNN-ready datasets
///
/// Controls dataset size to target:
/// - 50,000 – 200,000 nodes
/// - 1,000,000 – 10,000,000 directed edges
/// - 100 – 500 labeled positive addresses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScaleConfig {
    /// Maximum counterparties to expand per labeled address (top-K by value or tx count)
    /// Recommended: 100-300 for V2 scale
    #[serde(default = "default_top_k_counterparties")]
    pub top_k_counterparties: usize,

    /// Ranking strategy for selecting top-K counterparties
    #[serde(default)]
    pub counterparty_ranking: CounterpartyRanking,

    /// Maximum total nodes (soft cap - stops expansion if exceeded)
    #[serde(default = "default_max_nodes")]
    pub max_nodes: usize,

    /// Maximum total edges (soft cap - logs warning if exceeded)
    #[serde(default = "default_max_edges")]
    pub max_edges: usize,

    /// Minimum labeled addresses required (fail if fewer found)
    #[serde(default = "default_min_labeled")]
    pub min_labeled_addresses: usize,

    /// Maximum labeled addresses (truncate if more found)
    #[serde(default = "default_max_labeled")]
    pub max_labeled_addresses: usize,

    /// Maximum time window in weeks (hard cap)
    #[serde(default = "default_max_weeks")]
    pub max_time_window_weeks: i64,

    /// Whether to fail if scale bounds are exceeded
    #[serde(default = "default_strict_bounds")]
    pub strict_bounds: bool,
}

impl Default for ScaleConfig {
    fn default() -> Self {
        Self {
            top_k_counterparties: default_top_k_counterparties(),
            counterparty_ranking: CounterpartyRanking::default(),
            max_nodes: default_max_nodes(),
            max_edges: default_max_edges(),
            min_labeled_addresses: default_min_labeled(),
            max_labeled_addresses: default_max_labeled(),
            max_time_window_weeks: default_max_weeks(),
            strict_bounds: default_strict_bounds(),
        }
    }
}

/// Strategy for ranking counterparties when selecting top-K
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CounterpartyRanking {
    /// Rank by total value transferred (sum of all transactions)
    #[default]
    TotalValue,
    /// Rank by number of transactions
    TransactionCount,
    /// Rank by most recent transaction first
    MostRecent,
}

// Scale control defaults
fn default_top_k_counterparties() -> usize {
    200 // Balance between coverage and scale
}

fn default_max_nodes() -> usize {
    200_000 // Upper bound for V2 target
}

fn default_max_edges() -> usize {
    10_000_000 // Upper bound for V2 target
}

fn default_min_labeled() -> usize {
    10 // Minimum viable for training
}

fn default_max_labeled() -> usize {
    500 // Upper bound per spec
}

fn default_max_weeks() -> i64 {
    12 // Hard cap on time window
}

fn default_strict_bounds() -> bool {
    false // Warn by default, don't fail
}

impl ScaleConfig {
    /// Validate scale configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.top_k_counterparties == 0 {
            anyhow::bail!("top_k_counterparties must be > 0");
        }
        if self.max_nodes < 1000 {
            anyhow::bail!("max_nodes must be >= 1000");
        }
        if self.max_edges < 10000 {
            anyhow::bail!("max_edges must be >= 10000");
        }
        if self.max_time_window_weeks < 1 || self.max_time_window_weeks > 52 {
            anyhow::bail!("max_time_window_weeks must be between 1 and 52");
        }
        Ok(())
    }

    /// Check if node count is within bounds
    pub fn check_node_count(&self, count: usize) -> ScaleCheckResult {
        if count > self.max_nodes {
            if self.strict_bounds {
                ScaleCheckResult::Exceeded
            } else {
                ScaleCheckResult::Warning
            }
        } else if count < 50_000 {
            ScaleCheckResult::BelowTarget
        } else {
            ScaleCheckResult::Ok
        }
    }

    /// Check if edge count is within bounds
    pub fn check_edge_count(&self, count: usize) -> ScaleCheckResult {
        if count > self.max_edges {
            if self.strict_bounds {
                ScaleCheckResult::Exceeded
            } else {
                ScaleCheckResult::Warning
            }
        } else if count < 1_000_000 {
            ScaleCheckResult::BelowTarget
        } else {
            ScaleCheckResult::Ok
        }
    }
}

/// Result of a scale check
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleCheckResult {
    /// Within target bounds
    Ok,
    /// Below minimum target (might want more data)
    BelowTarget,
    /// Warning - above soft cap but not failing
    Warning,
    /// Exceeded hard cap (will fail if strict_bounds is true)
    Exceeded,
}

impl PipelineConfig {
    /// Load configuration from environment and optional config file
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();

        let alchemy_api_key = std::env::var("ALCHEMY_API_KEY")
            .map_err(|_| anyhow::anyhow!("ALCHEMY_API_KEY environment variable not set"))?;

        let config = Self {
            alchemy_api_key,
            alchemy_base_url: std::env::var("ALCHEMY_BASE_URL")
                .unwrap_or_else(|_| default_alchemy_url()),
            time_window: TimeWindowConfig::default(),
            rate_limits: RateLimitConfig::default(),
            paths: PathConfig::default(),
            aggregation: AggregationConfig::default(),
            scale: ScaleConfig::default(),
        };

        Ok(config)
    }

    /// Load from a TOML config file with environment overrides
    pub fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();

        let contents = std::fs::read_to_string(path)?;
        let mut config: Self = ::toml::from_str(&contents)?;

        // Environment variables override file settings
        if let Ok(key) = std::env::var("ALCHEMY_API_KEY") {
            config.alchemy_api_key = key;
        }
        if let Ok(url) = std::env::var("ALCHEMY_BASE_URL") {
            config.alchemy_base_url = url;
        }

        Ok(config)
    }

    /// Get full Alchemy RPC URL
    pub fn alchemy_rpc_url(&self) -> String {
        format!("{}/{}", self.alchemy_base_url, self.alchemy_api_key)
    }

    /// Ensure all output directories exist
    pub fn ensure_directories(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.paths.raw_dir)?;
        std::fs::create_dir_all(&self.paths.processed_dir)?;
        std::fs::create_dir_all(&self.paths.labels_dir)?;
        std::fs::create_dir_all(&self.paths.metadata_dir)?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_time_window() {
        let tw = TimeWindowConfig::default();
        assert!(tw.end > tw.start);
        // Should be approximately 8 weeks
        let diff = tw.end - tw.start;
        assert!(diff.num_weeks() >= 7 && diff.num_weeks() <= 9);
    }
}
