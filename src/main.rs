//! Ethereum AML Data Pipeline CLI
//!
//! A production-quality pipeline for collecting and labeling Ethereum
//! transaction data for Anti-Money Laundering (AML) analysis.

use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use eth_aml_pipeline::aggregate_graph::{aggregate_with_large_values, read_edges_parquet};
use eth_aml_pipeline::build_nodes::NodeBuilder;
use eth_aml_pipeline::config::{PipelineConfig, ScaleCheckResult};
use eth_aml_pipeline::fetch_labels::{read_labels_parquet, write_labels_parquet, OfacFetcher};
use eth_aml_pipeline::fetch_transactions::{
    read_transactions_parquet, write_transactions_parquet, TransactionCollector,
};
use eth_aml_pipeline::schemas::RunMetadata;

#[derive(Parser)]
#[command(name = "eth-aml-pipeline")]
#[command(author = "AML Pipeline Team")]
#[command(version)]
#[command(about = "Ethereum AML data collection and labeling pipeline", long_about = None)]
struct Cli {
    /// Path to configuration file (optional, uses env vars if not provided)
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Output directory for data files
    #[arg(short, long, global = true, default_value = "data")]
    output_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Fetch labeled addresses from OFAC SDN list
    FetchLabels {
        /// Output file path (relative to labels dir)
        #[arg(short, long, default_value = "labels.parquet")]
        output: String,
    },

    /// Collect transactions for labeled addresses and 1-hop counterparties
    FetchTransactions {
        /// Input labels file
        #[arg(short, long, default_value = "data/labels/labels.parquet")]
        labels: PathBuf,

        /// Output file path
        #[arg(short, long, default_value = "transactions_raw.parquet")]
        output: String,

        /// Time window in weeks (max 600, default=all history since Ethereum genesis 2015)
        #[arg(short, long, default_value = "600")]
        weeks: i64,

        /// Top-K counterparties per labeled address
        #[arg(short = 'k', long, default_value = "500")]
        top_k: usize,

        /// Fail if scale limits exceeded (otherwise just warn)
        #[arg(long)]
        strict: bool,
    },

    /// Aggregate transactions into weekly edges
    Aggregate {
        /// Input transactions file
        #[arg(short, long, default_value = "data/raw/transactions_raw.parquet")]
        transactions: PathBuf,

        /// Output file path
        #[arg(short, long, default_value = "edges_weekly.parquet")]
        output: String,
    },

    /// Build node metadata from edges and labels
    BuildNodes {
        /// Input edges file
        #[arg(short, long, default_value = "data/processed/edges_weekly.parquet")]
        edges: PathBuf,

        /// Input labels file
        #[arg(short, long, default_value = "data/labels/labels.parquet")]
        labels: PathBuf,

        /// Output file path
        #[arg(short, long, default_value = "nodes.parquet")]
        output: String,

        /// Run in offline mode (skip contract detection)
        #[arg(long)]
        offline: bool,
    },

    /// Run the complete pipeline
    Run {
        /// Time window in weeks (max 600, default=all history since Ethereum genesis 2015)
        #[arg(short, long, default_value = "600")]
        weeks: i64,

        /// Top-K counterparties per labeled address
        #[arg(short = 'k', long, default_value = "500")]
        top_k: usize,

        /// Skip transaction fetching (use existing data)
        #[arg(long)]
        skip_transactions: bool,

        /// Skip node building (use existing data)
        #[arg(long)]
        skip_nodes: bool,

        /// Fail if scale limits exceeded (otherwise just warn)
        #[arg(long)]
        strict: bool,
    },

    /// Show pipeline status and data counts
    Status,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose { Level::DEBUG } else { Level::INFO };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(false)
        .compact()
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .context("Failed to set tracing subscriber")?;

    // Load configuration
    let mut config = match &cli.config {
        Some(path) => PipelineConfig::load_from_file(path)
            .with_context(|| format!("Failed to load config from {:?}", path))?,
        None => PipelineConfig::load().context("Failed to load config from environment")?,
    };

    // Override output directory if specified
    config.paths.data_dir = cli.output_dir.clone();
    config.paths.raw_dir = cli.output_dir.join("raw");
    config.paths.processed_dir = cli.output_dir.join("processed");
    config.paths.labels_dir = cli.output_dir.join("labels");
    config.paths.metadata_dir = cli.output_dir.join("metadata");

    // Ensure directories exist
    config.ensure_directories()?;

    match cli.command {
        Commands::FetchLabels { output } => {
            cmd_fetch_labels(&config, &output).await?;
        }
        Commands::FetchTransactions {
            labels,
            output,
            weeks,
            top_k,
            strict,
        } => {
            cmd_fetch_transactions(&mut config, &labels, &output, weeks, top_k, strict).await?;
        }
        Commands::Aggregate {
            transactions,
            output,
        } => {
            cmd_aggregate(&config, &transactions, &output)?;
        }
        Commands::BuildNodes {
            edges,
            labels,
            output,
            offline,
        } => {
            cmd_build_nodes(&config, &edges, &labels, &output, offline).await?;
        }
        Commands::Run {
            weeks,
            top_k,
            skip_transactions,
            skip_nodes,
            strict,
        } => {
            cmd_run_pipeline(&mut config, weeks, top_k, skip_transactions, skip_nodes, strict).await?;
        }
        Commands::Status => {
            cmd_status(&config)?;
        }
    }

    Ok(())
}

async fn cmd_fetch_labels(config: &PipelineConfig, output: &str) -> Result<()> {
    info!("=== Fetching OFAC Labels ===");

    let fetcher = OfacFetcher::new();
    let labels = fetcher
        .fetch_labels()
        .await
        .context("Failed to fetch OFAC labels")?;

    info!("Fetched {} sanctioned addresses", labels.len());

    let output_path = config.paths.labels_dir.join(output);
    write_labels_parquet(&labels, &output_path).context("Failed to write labels parquet")?;

    // Write metadata
    let mut metadata = RunMetadata::new(config.time_window.start, config.time_window.end);
    metadata.record_counts.labeled_addresses = labels.len();
    let metadata_path = config.paths.metadata_dir.join("labels_metadata.json");
    metadata.save(&metadata_path)?;

    info!("Labels saved to {:?}", output_path);
    info!("Metadata saved to {:?}", metadata_path);

    Ok(())
}

async fn cmd_fetch_transactions(
    config: &mut PipelineConfig,
    labels_path: &PathBuf,
    output: &str,
    weeks: i64,
    top_k: usize,
    strict: bool,
) -> Result<()> {
    info!("=== Fetching Transactions ===");

    // Validate and clamp time window
    let max_weeks = config.scale.max_time_window_weeks;
    let actual_weeks = weeks.min(max_weeks);
    if weeks > max_weeks {
        warn!(
            "Requested {} weeks exceeds max {} weeks, using {}",
            weeks, max_weeks, actual_weeks
        );
    }

    // Update configuration with CLI overrides
    config.time_window.end = Utc::now();
    config.time_window.start = config.time_window.end - Duration::weeks(actual_weeks);
    config.scale.top_k_counterparties = top_k;
    config.scale.strict_bounds = strict;

    info!(
        "Time window: {} to {} ({} weeks)",
        config.time_window.start.format("%Y-%m-%d"),
        config.time_window.end.format("%Y-%m-%d"),
        actual_weeks
    );
    info!(
        "Scale controls: top_k={}, max_nodes={}, strict={}",
        config.scale.top_k_counterparties,
        config.scale.max_nodes,
        config.scale.strict_bounds
    );

    // Load labels
    let labels =
        read_labels_parquet(labels_path).context("Failed to read labels - run fetch-labels first")?;

    // Apply label count limits
    let label_count = labels.len();
    if label_count < config.scale.min_labeled_addresses {
        anyhow::bail!(
            "Only {} labeled addresses found, minimum required is {}",
            label_count,
            config.scale.min_labeled_addresses
        );
    }

    let labels: Vec<_> = if label_count > config.scale.max_labeled_addresses {
        warn!(
            "Truncating {} labeled addresses to max {}",
            label_count, config.scale.max_labeled_addresses
        );
        labels
            .into_iter()
            .take(config.scale.max_labeled_addresses)
            .collect()
    } else {
        labels
    };

    let labeled_addresses: Vec<String> = labels.iter().map(|l| l.address.clone()).collect();
    info!("Using {} labeled addresses", labeled_addresses.len());

    // Collect transactions
    let collector = TransactionCollector::new(config.clone());

    let (mut transactions, counterparties) = collector
        .collect_labeled_transactions(&labeled_addresses)
        .await
        .context("Failed to collect labeled transactions")?;

    info!(
        "Collected {} transactions with {} counterparties (top-{} per address)",
        transactions.len(),
        counterparties.len(),
        top_k
    );

    // Collect counterparty transactions
    if !counterparties.is_empty() {
        info!("Collecting counterparty transactions...");
        let counterparty_txs = collector
            .collect_counterparty_transactions(&labeled_addresses, &counterparties)
            .await
            .context("Failed to collect counterparty transactions")?;

        info!(
            "Collected {} additional counterparty transactions",
            counterparty_txs.len()
        );
        transactions.extend(counterparty_txs);
    }

    // Write output
    let output_path = config.paths.raw_dir.join(output);
    write_transactions_parquet(&transactions, &output_path)
        .context("Failed to write transactions")?;

    // Write metadata
    let mut metadata = RunMetadata::new(config.time_window.start, config.time_window.end);
    metadata.record_counts.raw_transactions = transactions.len();
    let metadata_path = config.paths.metadata_dir.join("transactions_metadata.json");
    metadata.save(&metadata_path)?;

    info!("Transactions saved to {:?}", output_path);
    info!("Metadata saved to {:?}", metadata_path);

    Ok(())
}

fn cmd_aggregate(config: &PipelineConfig, transactions_path: &PathBuf, output: &str) -> Result<()> {
    info!("=== Aggregating Transactions ===");

    if !transactions_path.exists() {
        anyhow::bail!(
            "Transactions file not found: {:?}. Run fetch-transactions first.",
            transactions_path
        );
    }

    let output_path = config.paths.processed_dir.join(output);

    let edge_count = aggregate_with_large_values(transactions_path, &output_path)
        .context("Failed to aggregate transactions")?;

    // Write metadata
    let mut metadata = RunMetadata::new(config.time_window.start, config.time_window.end);
    metadata.record_counts.weekly_edges = edge_count;
    let metadata_path = config.paths.metadata_dir.join("edges_metadata.json");
    metadata.save(&metadata_path)?;

    info!("Aggregated {} edges to {:?}", edge_count, output_path);
    info!("Metadata saved to {:?}", metadata_path);

    Ok(())
}

async fn cmd_build_nodes(
    config: &PipelineConfig,
    edges_path: &PathBuf,
    labels_path: &PathBuf,
    output: &str,
    offline: bool,
) -> Result<()> {
    info!("=== Building Node Metadata ===");

    if !edges_path.exists() {
        anyhow::bail!(
            "Edges file not found: {:?}. Run aggregate first.",
            edges_path
        );
    }

    let builder = if offline {
        info!("Running in offline mode (skipping contract detection)");
        NodeBuilder::offline(config.aggregation.hub_percentile)
    } else {
        NodeBuilder::new(config)
    };

    let output_path = config.paths.processed_dir.join(output);

    let node_count = builder
        .build_nodes(edges_path, labels_path, &output_path)
        .await
        .context("Failed to build nodes")?;

    // Write metadata
    let mut metadata = RunMetadata::new(config.time_window.start, config.time_window.end);
    metadata.record_counts.nodes = node_count;
    let metadata_path = config.paths.metadata_dir.join("nodes_metadata.json");
    metadata.save(&metadata_path)?;

    info!("Built {} nodes to {:?}", node_count, output_path);
    info!("Metadata saved to {:?}", metadata_path);

    Ok(())
}

async fn cmd_run_pipeline(
    config: &mut PipelineConfig,
    weeks: i64,
    top_k: usize,
    skip_transactions: bool,
    skip_nodes: bool,
    strict: bool,
) -> Result<()> {
    info!("=== Running Complete Pipeline ===");
    info!(
        "Parameters: weeks={}, top_k={}, strict={}",
        weeks, top_k, strict
    );

    let labels_path = config.paths.labels_dir.join("labels.parquet");
    let transactions_path = config.paths.raw_dir.join("transactions_raw.parquet");
    let edges_path = config.paths.processed_dir.join("edges_weekly.parquet");
    let nodes_path = config.paths.processed_dir.join("nodes.parquet");

    // Step 1: Fetch labels
    info!("\n--- Step 1: Fetch Labels ---");
    cmd_fetch_labels(config, "labels.parquet").await?;

    // Step 2: Fetch transactions
    if skip_transactions && transactions_path.exists() {
        info!("\n--- Step 2: Skipping transaction fetch (using existing data) ---");
    } else {
        info!("\n--- Step 2: Fetch Transactions ---");
        cmd_fetch_transactions(config, &labels_path, "transactions_raw.parquet", weeks, top_k, strict).await?;
    }

    // Step 3: Aggregate
    info!("\n--- Step 3: Aggregate Transactions ---");
    cmd_aggregate(config, &transactions_path, "edges_weekly.parquet")?;

    // Step 4: Build nodes
    if skip_nodes {
        info!("\n--- Step 4: Skipping node building ---");
    } else {
        info!("\n--- Step 4: Build Node Metadata ---");
        cmd_build_nodes(config, &edges_path, &labels_path, "nodes.parquet", false).await?;
    }

    // Step 5: Scale validation
    info!("\n--- Step 5: Scale Validation ---");
    validate_scale(config, &edges_path, &nodes_path, strict)?;

    // Final summary
    info!("\n=== Pipeline Complete ===");
    cmd_status(config)?;

    Ok(())
}

/// Validate final dataset scale against V2 targets
fn validate_scale(
    config: &PipelineConfig,
    edges_path: &PathBuf,
    nodes_path: &PathBuf,
    strict: bool,
) -> Result<()> {
    let edge_count = if edges_path.exists() {
        read_edges_parquet(edges_path)
            .map(|v| v.len())
            .unwrap_or(0)
    } else {
        0
    };

    let node_count = if nodes_path.exists() {
        eth_aml_pipeline::build_nodes::read_nodes_parquet(nodes_path)
            .map(|v| v.len())
            .unwrap_or(0)
    } else {
        0
    };

    info!("Final scale: {} nodes, {} edges", node_count, edge_count);

    // Check node scale
    match config.scale.check_node_count(node_count) {
        ScaleCheckResult::Ok => {
            info!("Node count within V2 target range (50K-200K)");
        }
        ScaleCheckResult::BelowTarget => {
            warn!(
                "Node count {} below V2 target minimum (50K). Consider expanding time window or top-K.",
                node_count
            );
        }
        ScaleCheckResult::Warning => {
            warn!(
                "Node count {} exceeds soft cap {} but continuing",
                node_count, config.scale.max_nodes
            );
        }
        ScaleCheckResult::Exceeded => {
            if strict {
                anyhow::bail!(
                    "Node count {} exceeds max {} (strict mode)",
                    node_count,
                    config.scale.max_nodes
                );
            }
        }
    }

    // Check edge scale
    match config.scale.check_edge_count(edge_count) {
        ScaleCheckResult::Ok => {
            info!("Edge count within V2 target range (1M-10M)");
        }
        ScaleCheckResult::BelowTarget => {
            warn!(
                "Edge count {} below V2 target minimum (1M). Consider expanding time window or top-K.",
                edge_count
            );
        }
        ScaleCheckResult::Warning => {
            warn!(
                "Edge count {} exceeds soft cap {} but continuing",
                edge_count, config.scale.max_edges
            );
        }
        ScaleCheckResult::Exceeded => {
            if strict {
                anyhow::bail!(
                    "Edge count {} exceeds max {} (strict mode)",
                    edge_count,
                    config.scale.max_edges
                );
            }
        }
    }

    Ok(())
}

fn cmd_status(config: &PipelineConfig) -> Result<()> {
    info!("=== Pipeline Status ===");
    info!("Data directory: {:?}", config.paths.data_dir);

    let files = [
        ("Labels", config.paths.labels_dir.join("labels.parquet")),
        (
            "Transactions",
            config.paths.raw_dir.join("transactions_raw.parquet"),
        ),
        (
            "Edges",
            config.paths.processed_dir.join("edges_weekly.parquet"),
        ),
        ("Nodes", config.paths.processed_dir.join("nodes.parquet")),
    ];

    for (name, path) in files {
        if path.exists() {
            let metadata = std::fs::metadata(&path)?;
            let size_kb = metadata.len() / 1024;

            // Try to read record count
            let count = match name {
                "Labels" => read_labels_parquet(&path)
                    .map(|v| v.len())
                    .unwrap_or(0),
                "Transactions" => read_transactions_parquet(&path)
                    .map(|v| v.len())
                    .unwrap_or(0),
                "Edges" => read_edges_parquet(&path)
                    .map(|v| v.len())
                    .unwrap_or(0),
                "Nodes" => eth_aml_pipeline::build_nodes::read_nodes_parquet(&path)
                    .map(|v| v.len())
                    .unwrap_or(0),
                _ => 0,
            };

            info!(
                "  {} {:?}: {} records ({} KB)",
                "✓", name, count, size_kb
            );
        } else {
            info!("  {} {}: not found", "✗", name);
        }
    }

    // Check for metadata files
    let metadata_files = [
        "labels_metadata.json",
        "transactions_metadata.json",
        "edges_metadata.json",
        "nodes_metadata.json",
    ];

    info!("\nMetadata files:");
    for file in metadata_files {
        let path = config.paths.metadata_dir.join(file);
        if path.exists() {
            info!("  {} {}", "✓", file);
        } else {
            info!("  {} {}", "✗", file);
        }
    }

    Ok(())
}
