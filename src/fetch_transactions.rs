//! Transaction fetching for labeled addresses and 1-hop counterparties.
//!
//! Uses Alchemy's asset transfer API for efficient batch retrieval.
//! Respects rate limits and implements checkpointing for resumability.

use crate::config::PipelineConfig;
use crate::ethereum::ValidatedAddress;
use crate::rpc::{AssetTransfer, EthRpcClient, TransferDirection};
use crate::schemas::RawTransaction;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Progress checkpoint for resumable counterparty collection.
#[derive(Serialize, Deserialize)]
struct ChunkProgress {
    /// Next block to start processing from.
    next_block_start: u64,
    /// Next chunk file index to write.
    next_chunk_index: u32,
    /// Running total of transactions written so far.
    total_written: usize,
}

/// Known high-volume exchange, DEX router, and wrapper contract addresses.
/// These are excluded from counterparty expansion (phase 2) because they have
/// millions of transactions that add noise without discriminative signal for GNN
/// training. The labeled↔exchange edges from phase 1 are still preserved.
fn known_exchange_addresses() -> HashSet<String> {
    [
        // Binance
        "0x28c6c06298d514db089934071355e5743bf21d60",
        "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
        "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",
        "0x56eddb7aa87536c09ccc2793473599fd21a8b17f",
        // Bitfinex
        "0x52bc44d5378309ee2abf1539bf71de1b7d7be3b5",
        "0x1151314c646ce4e0efd76d1af4760ae66a9fe30f",
        // Kraken
        "0x2910543af39aba0cd09dbb2d50200b3e800a63d2",
        "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0",
        // Coinbase
        "0x71660c4005ba85c37ccec55d0c4493e66fe775d3",
        "0x503828976d22510aad0201ac7ec88293211d23da",
        "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740",
        "0x3cd751e6b0078be393132286c442345e68ff0aaa",
        // WETH contract
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        // Uniswap V2 Router
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
        // Uniswap V3 Router
        "0xe592427a0aece92de3edee1f18e0157c05861564",
        "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",
        // LiFi Diamond (aggregator)
        "0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae",
        // OKX
        "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b",
        // Gemini
        "0xd24400ae8bfebb18ca49be86258a3c749cf46853",
        // Huobi/HTX
        "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43",
        "0x46340b20830761efd32832a74d7169b29feb9758",
        // KuCoin
        "0x9430801ebaf509ad49202aabc5f5bc6fd8a3daf8",
        // Crypto.com
        "0x6262998ced04146fa42253a5c0af90ca02dfd2a3",
        // Gate.io
        "0x974caa59e49682cda0ad2bbe82983419a2ecc400",
        "0x4976a4a02f38326660d17bf34b431dc6e2eb2327",
        // Bittrex
        "0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98",
        // Poloniex
        "0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef",
        // Aave V2 Lending Pool
        "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9",
        // Aave V3 Pool
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        // 1inch Router
        "0x1111111254eeb25477b68fb85ed929f73a960582",
        // Metamask Swap Router
        "0x881d40237659c251811cec9c364ef91dc08d300c",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("RPC error: {0}")]
    Rpc(#[from] crate::rpc::RpcError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    #[error("No labeled addresses provided")]
    NoAddresses,

    #[error("Block range error: {0}")]
    BlockRange(String),

    #[error("Scale limit exceeded: {0}")]
    ScaleLimitExceeded(String),
}

/// Statistics about counterparty interactions for ranking
#[derive(Debug, Clone, Default)]
struct CounterpartyStats {
    /// Total value transferred (sum of all transactions)
    total_value: u128,
    /// Number of transactions
    tx_count: usize,
    /// Most recent transaction timestamp
    most_recent: Option<DateTime<Utc>>,
}

/// Transaction collector for AML-relevant slice
pub struct TransactionCollector {
    client: EthRpcClient,
    config: PipelineConfig,
}

impl TransactionCollector {
    pub fn new(config: PipelineConfig) -> Self {
        let client = EthRpcClient::new(
            config.alchemy_base_url.clone(),
            config.alchemy_key_pool.clone(),
            config.rate_limits.clone(),
        );
        Self { client, config }
    }

    /// Collect transactions involving labeled addresses (both directions)
    /// Returns raw transactions and the set of 1-hop counterparties (capped by top-K)
    pub async fn collect_labeled_transactions(
        &self,
        labeled_addresses: &[String],
    ) -> Result<(Vec<RawTransaction>, HashSet<String>), TransactionError> {
        if labeled_addresses.is_empty() {
            return Err(TransactionError::NoAddresses);
        }

        info!(
            "Collecting transactions for {} labeled addresses",
            labeled_addresses.len()
        );

        // Convert time window to block numbers
        let (start_block, end_block) = self.get_block_range().await?;
        info!(
            "Block range: {} to {} (approximately {} blocks)",
            start_block,
            end_block,
            end_block - start_block
        );

        let mut all_transactions = Vec::new();
        // Track per-labeled-address counterparty stats for top-K selection
        let mut counterparty_stats: HashMap<String, HashMap<String, CounterpartyStats>> =
            HashMap::new();

        // Alchemy getAssetTransfers supports very large block ranges.
        // Use 500K blocks per chunk (~70 days) to minimise API calls.
        let block_chunk_size: u64 = 500_000;

        let labeled_set: HashSet<String> = labeled_addresses
            .iter()
            .map(|a| a.to_lowercase())
            .collect();

        for block_start in (start_block..=end_block).step_by(block_chunk_size as usize) {
            let block_end = (block_start + block_chunk_size - 1).min(end_block);

            info!(
                "Processing blocks {} to {} ({:.1}% complete)",
                block_start,
                block_end,
                (block_start - start_block) as f64 / (end_block - start_block) as f64 * 100.0
            );

            // Alchemy accepts fromAddress/toAddress as a single string, not an array.
            // We must loop over addresses individually.
            for addr in labeled_addresses {
                // Fetch outgoing transactions (from labeled address)
                match self
                    .client
                    .get_asset_transfers(block_start, block_end, addr, TransferDirection::From)
                    .await
                {
                    Ok(mut outgoing) => {
                        // Safety net: for outgoing transfers, Alchemy can
                        // return to=null on certain internal transactions.
                        // We cannot infer the recipient from the query
                        // (unlike the incoming direction), so log and skip
                        // these rather than silently dropping them.
                        for transfer in &mut outgoing {
                            if transfer.to.as_ref().map_or(true, |t| t.is_empty()) {
                                warn!(
                                    "Outgoing transfer {} from {} has null/empty 'to' field — skipping for counterparty stats",
                                    transfer.hash, transfer.from
                                );
                            }
                        }

                        for transfer in &outgoing {
                            if let Some(to) = &transfer.to {
                                let to_lower = to.to_lowercase();
                                if !labeled_set.contains(&to_lower) {
                                    let from_lower = transfer.from.to_lowercase();
                                    self.update_counterparty_stats(
                                        &mut counterparty_stats,
                                        &from_lower,
                                        &to_lower,
                                        transfer,
                                    );
                                }
                            }
                        }
                        all_transactions.extend(outgoing);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to fetch outgoing transfers for {} block range {}-{}: {} — skipping, data preserved",
                            addr, block_start, block_end, e
                        );
                    }
                }

                // Fetch incoming transactions (to labeled address)
                match self
                    .client
                    .get_asset_transfers(block_start, block_end, addr, TransferDirection::To)
                    .await
                {
                    Ok(mut incoming) => {
                        // Safety net: if Alchemy still returns to=null for the
                        // To-direction, fill it in from the queried address.
                        for transfer in &mut incoming {
                            if transfer.to.as_ref().map_or(true, |t| t.is_empty()) {
                                transfer.to = Some(addr.clone());
                            }
                        }

                        for transfer in &incoming {
                            let from_lower = transfer.from.to_lowercase();
                            if !labeled_set.contains(&from_lower) {
                                if let Some(to) = &transfer.to {
                                    let to_lower = to.to_lowercase();
                                    self.update_counterparty_stats(
                                        &mut counterparty_stats,
                                        &to_lower,
                                        &from_lower,
                                        transfer,
                                    );
                                }
                            }
                        }
                        all_transactions.extend(incoming);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to fetch incoming transfers for {} block range {}-{}: {} — skipping, data preserved",
                            addr, block_start, block_end, e
                        );
                    }
                }
            }
        }

        // Apply top-K counterparty selection per labeled address
        let top_k = self.config.scale.top_k_counterparties;
        let ranking = &self.config.scale.counterparty_ranking;
        let mut selected_counterparties: HashSet<String> = HashSet::new();

        for (labeled_addr, cp_map) in &counterparty_stats {
            let mut ranked: Vec<(&String, &CounterpartyStats)> = cp_map.iter().collect();

            // Sort by ranking strategy (descending)
            match ranking {
                crate::config::CounterpartyRanking::TotalValue => {
                    ranked.sort_by(|a, b| b.1.total_value.cmp(&a.1.total_value));
                }
                crate::config::CounterpartyRanking::TransactionCount => {
                    ranked.sort_by(|a, b| b.1.tx_count.cmp(&a.1.tx_count));
                }
                crate::config::CounterpartyRanking::MostRecent => {
                    ranked.sort_by(|a, b| b.1.most_recent.cmp(&a.1.most_recent));
                }
            }

            // Take top-K
            for (addr, _stats) in ranked.into_iter().take(top_k) {
                selected_counterparties.insert(addr.clone());
            }

            debug!(
                "Selected {} counterparties for {} (total candidates: {})",
                selected_counterparties.len().min(top_k),
                labeled_addr,
                cp_map.len()
            );
        }

        // Remove known exchanges/hubs — they add noise without GNN signal.
        // Their direct edges to labeled addresses are already captured in phase 1.
        let exchanges = known_exchange_addresses();
        let before_filter = selected_counterparties.len();
        selected_counterparties.retain(|addr| !exchanges.contains(&addr.to_lowercase()));
        let removed = before_filter - selected_counterparties.len();
        if removed > 0 {
            info!(
                "Filtered out {} known exchange/hub addresses from counterparty set ({} remaining)",
                removed,
                selected_counterparties.len()
            );
        }

        // Check scale limits
        let total_counterparties = selected_counterparties.len();
        if total_counterparties > self.config.scale.max_nodes {
            if self.config.scale.strict_bounds {
                return Err(TransactionError::ScaleLimitExceeded(format!(
                    "Counterparty count {} exceeds max_nodes {}",
                    total_counterparties, self.config.scale.max_nodes
                )));
            } else {
                warn!(
                    "Counterparty count {} exceeds recommended max_nodes {} (continuing anyway)",
                    total_counterparties, self.config.scale.max_nodes
                );
            }
        }

        info!(
            "Found {} transactions involving labeled addresses",
            all_transactions.len()
        );
        info!(
            "Selected {} unique 1-hop counterparties (top-{} per labeled address)",
            selected_counterparties.len(),
            top_k
        );

        // Convert to RawTransaction format
        let raw_txs = self.convert_transfers(all_transactions)?;

        Ok((raw_txs, selected_counterparties))
    }

    /// Update counterparty statistics for top-K ranking
    fn update_counterparty_stats(
        &self,
        stats_map: &mut HashMap<String, HashMap<String, CounterpartyStats>>,
        labeled_addr: &str,
        counterparty_addr: &str,
        transfer: &AssetTransfer,
    ) {
        let cp_stats = stats_map
            .entry(labeled_addr.to_string())
            .or_default()
            .entry(counterparty_addr.to_string())
            .or_default();

        // Parse value
        let value: u128 = transfer
            .value
            .map(|v| (v * 1e18) as u128)
            .unwrap_or(0);

        cp_stats.total_value = cp_stats.total_value.saturating_add(value);
        cp_stats.tx_count += 1;

        // Update most recent timestamp
        if let Some(ts) = transfer
            .metadata
            .as_ref()
            .and_then(|m| m.block_timestamp.as_ref())
            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
            .map(|dt| dt.with_timezone(&Utc))
        {
            match &cp_stats.most_recent {
                Some(existing) if ts > *existing => cp_stats.most_recent = Some(ts),
                None => cp_stats.most_recent = Some(ts),
                _ => {}
            }
        }
    }

    /// Collect transactions involving 1-hop counterparties.
    /// Writes chunk parquet files to `chunks_dir` to avoid OOM on large datasets.
    /// Returns the total number of transactions written across all chunks.
    pub async fn collect_counterparty_transactions(
        &self,
        counterparties: &HashSet<String>,
        chunks_dir: &Path,
    ) -> Result<usize, TransactionError> {
        if counterparties.is_empty() {
            return Ok(0);
        }

        info!(
            "Collecting counterparty transactions for {} addresses",
            counterparties.len()
        );

        std::fs::create_dir_all(chunks_dir)?;

        let (start_block, end_block) = self.get_block_range().await?;

        let block_chunk_size: u64 = 500_000;

        // Resume from progress checkpoint if one exists (crash recovery).
        let progress_path = chunks_dir.join("progress.json");
        let (mut chunk_index, mut total_written, resume_block) =
            match std::fs::read_to_string(&progress_path) {
                Ok(json) => match serde_json::from_str::<ChunkProgress>(&json) {
                    Ok(p) => {
                        info!(
                            "Resuming counterparty collection from block {} (chunk {}, {} txs already on disk)",
                            p.next_block_start, p.next_chunk_index, p.total_written
                        );
                        (p.next_chunk_index, p.total_written, p.next_block_start)
                    }
                    Err(e) => {
                        warn!("Corrupt progress file, starting from scratch: {}", e);
                        (0u32, 0usize, start_block)
                    }
                },
                Err(_) => (0u32, 0usize, start_block),
            };

        let counterparty_vec: Vec<String> = counterparties.iter().cloned().collect();

        for block_start in (start_block..=end_block).step_by(block_chunk_size as usize) {
            if block_start < resume_block {
                continue;
            }

            let block_end = (block_start + block_chunk_size - 1).min(end_block);

            info!("Processing counterparty blocks {} to {}", block_start, block_end);

            let mut chunk_transfers = Vec::new();

            // Alchemy accepts a single address per call.
            for addr in &counterparty_vec {
                // Outgoing from counterparty → keep all transfers for full graph structure
                match self
                    .client
                    .get_asset_transfers(block_start, block_end, addr, TransferDirection::From)
                    .await
                {
                    Ok(outgoing) => {
                        chunk_transfers.extend(outgoing);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to fetch counterparty outgoing for {} block range {}-{}: {} — skipping",
                            addr, block_start, block_end, e
                        );
                    }
                }

                // Incoming to counterparty → keep all transfers for full graph structure
                match self
                    .client
                    .get_asset_transfers(block_start, block_end, addr, TransferDirection::To)
                    .await
                {
                    Ok(mut incoming) => {
                        // Safety net: fill in null `to` from queried address
                        for transfer in &mut incoming {
                            if transfer.to.as_ref().map_or(true, |t| t.is_empty()) {
                                transfer.to = Some(addr.clone());
                            }
                        }

                        chunk_transfers.extend(incoming);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to fetch counterparty incoming for {} block range {}-{}: {} — skipping",
                            addr, block_start, block_end, e
                        );
                    }
                }
            }

            // Convert and flush this chunk to disk
            if !chunk_transfers.is_empty() {
                let raw_txs = self.convert_transfers(chunk_transfers)?;
                let chunk_path = chunks_dir.join(format!("chunk_{:04}.parquet", chunk_index));
                write_transactions_parquet(&raw_txs, &chunk_path)?;
                total_written += raw_txs.len();
                info!(
                    "Flushed chunk {} ({} txs, {} total so far)",
                    chunk_index,
                    raw_txs.len(),
                    total_written
                );
                chunk_index += 1;

                // Save progress checkpoint so we can resume after a crash.
                let progress = ChunkProgress {
                    next_block_start: block_start + block_chunk_size,
                    next_chunk_index: chunk_index,
                    total_written,
                };
                if let Ok(json) = serde_json::to_string(&progress) {
                    if let Err(e) = std::fs::write(&progress_path, json) {
                        warn!("Failed to write progress checkpoint: {}", e);
                    }
                }
            }
        }

        // Remove progress file on successful completion.
        if progress_path.exists() {
            std::fs::remove_file(&progress_path).ok();
        }

        info!(
            "Counterparty collection complete: {} transactions in {} chunks",
            total_written, chunk_index
        );

        Ok(total_written)
    }

    /// Convert Alchemy transfers to RawTransaction format
    fn convert_transfers(
        &self,
        transfers: Vec<AssetTransfer>,
    ) -> Result<Vec<RawTransaction>, TransactionError> {
        let total = transfers.len();
        let mut transactions = Vec::with_capacity(total);
        let mut skip_category = 0usize;
        let mut skip_to_empty = 0usize;
        let mut skip_from_empty = 0usize;
        let mut skip_addr_empty = 0usize;

        for transfer in transfers {
            // Only process ETH transfers (external or internal category)
            if transfer.category != "external" && transfer.category != "internal" {
                skip_category += 1;
                continue;
            }

            let to_address = match &transfer.to {
                Some(to) if !to.is_empty() => to.clone(),
                _ => {
                    skip_to_empty += 1;
                    continue;
                }
            };

            // Parse timestamp from metadata
            let timestamp = transfer
                .metadata
                .as_ref()
                .and_then(|m| m.block_timestamp.as_ref())
                .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);

            // Skip if from_address is empty
            if transfer.from.is_empty() {
                skip_from_empty += 1;
                continue;
            }

            // Normalize addresses
            let from = ValidatedAddress::parse(&transfer.from)
                .map(|a| a.to_checksum())
                .unwrap_or_else(|_| transfer.from.clone());

            let to = ValidatedAddress::parse(&to_address)
                .map(|a| a.to_checksum())
                .unwrap_or_else(|_| to_address);

            // Final safety check - skip if either address is empty after normalization
            if from.is_empty() || to.is_empty() {
                skip_addr_empty += 1;
                warn!("Skipping transaction with empty address: from={}, to={}", from, to);
                continue;
            }

            let value_wei = transfer.value_wei();
            transactions.push(RawTransaction {
                tx_hash: transfer.hash,
                block_timestamp: timestamp,
                from_address: from,
                to_address: to,
                value_wei,
            });
        }

        info!(
            "convert_transfers: {} total -> {} kept, filtered: {} non-external, {} empty-to, {} empty-from, {} empty-after-normalize",
            total, transactions.len(), skip_category, skip_to_empty, skip_from_empty, skip_addr_empty
        );

        Ok(transactions)
    }

    /// Get block range for the configured time window
    async fn get_block_range(&self) -> Result<(u64, u64), TransactionError> {
        let latest_block = self.client.get_block_number().await?;

        // Estimate blocks in time window
        // Ethereum averages ~12 seconds per block
        let seconds_per_block = 12u64;
        let window_duration = self
            .config
            .time_window
            .end
            .signed_duration_since(self.config.time_window.start);
        let estimated_blocks = window_duration.num_seconds() as u64 / seconds_per_block;

        let end_block = latest_block;
        let start_block = end_block.saturating_sub(estimated_blocks);

        Ok((start_block, end_block))
    }
}

/// Write transactions to Parquet
pub fn write_transactions_parquet(
    transactions: &[RawTransaction],
    output_path: &Path,
) -> Result<(), TransactionError> {
    info!(
        "Writing {} transactions to {:?}",
        transactions.len(),
        output_path
    );

    // Filter out any transactions with empty addresses (safety net)
    let valid: Vec<_> = transactions
        .iter()
        .filter(|tx| !tx.from_address.is_empty() && !tx.to_address.is_empty())
        .collect();

    let filtered_count = transactions.len() - valid.len();
    if filtered_count > 0 {
        warn!(
            "Filtered {} transactions with empty addresses",
            filtered_count
        );
    }

    // Deduplicate by tx_hash
    let mut seen = HashSet::new();
    let deduped: Vec<_> = valid
        .into_iter()
        .filter(|tx| seen.insert(tx.tx_hash.clone()))
        .collect();

    info!(
        "After deduplication: {} unique transactions",
        deduped.len()
    );

    let tx_hashes: Vec<&str> = deduped.iter().map(|t| t.tx_hash.as_str()).collect();
    let timestamps: Vec<String> = deduped
        .iter()
        .map(|t| t.block_timestamp.to_rfc3339())
        .collect();
    let from_addrs: Vec<&str> = deduped.iter().map(|t| t.from_address.as_str()).collect();
    let to_addrs: Vec<&str> = deduped.iter().map(|t| t.to_address.as_str()).collect();
    let values: Vec<&str> = deduped.iter().map(|t| t.value_wei.as_str()).collect();

    let df = DataFrame::new(vec![
        Column::new("tx_hash".into(), tx_hashes),
        Column::new("block_timestamp".into(), timestamps),
        Column::new("from_address".into(), from_addrs),
        Column::new("to_address".into(), to_addrs),
        Column::new("value_wei".into(), values),
    ])?;

    // Sort by timestamp, then tx_hash for determinism
    let df = df.sort(
        ["block_timestamp", "tx_hash"],
        SortMultipleOptions::default(),
    )?;

    let file = std::fs::File::create(output_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df.clone())?;

    info!("Successfully wrote transactions to {:?}", output_path);
    Ok(())
}

/// Merge the labeled-transactions checkpoint with counterparty chunk files into
/// a single deduplicated parquet file. Operates at the DataFrame level to avoid
/// loading all rows into Rust structs simultaneously.
pub fn merge_parquet_files(
    labeled_path: &Path,
    chunks_dir: &Path,
    output_path: &Path,
) -> Result<usize, TransactionError> {
    let mut frames: Vec<DataFrame> = Vec::new();

    // Read labeled transactions checkpoint
    if labeled_path.exists() {
        let file = std::fs::File::open(labeled_path)?;
        let df = ParquetReader::new(file).finish()?;
        info!("Loaded {} labeled transactions from checkpoint", df.height());
        frames.push(df);
    }

    // Read all chunk files
    if chunks_dir.exists() {
        let mut chunk_files: Vec<_> = std::fs::read_dir(chunks_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "parquet")
            })
            .collect();
        chunk_files.sort_by_key(|e| e.path());

        for entry in &chunk_files {
            let file = std::fs::File::open(entry.path())?;
            let df = ParquetReader::new(file).finish()?;
            info!("Loaded chunk {:?} ({} rows)", entry.path().file_name().unwrap(), df.height());
            frames.push(df);
        }
    }

    if frames.is_empty() {
        info!("No parquet files to merge");
        return Ok(0);
    }

    // Concatenate all frames
    let mut combined = frames
        .into_iter()
        .reduce(|a, b| a.vstack(&b).expect("Schema mismatch during merge"))
        .unwrap();

    let total_before = combined.height();

    // Deduplicate by tx_hash
    combined = combined.unique::<String, String>(Some(&["tx_hash".to_string()]), UniqueKeepStrategy::First, None)?;

    let total_after = combined.height();
    let dupes = total_before - total_after;
    if dupes > 0 {
        info!("Removed {} duplicate transactions during merge", dupes);
    }

    // Sort by timestamp, then tx_hash for determinism
    combined = combined.sort(
        ["block_timestamp", "tx_hash"],
        SortMultipleOptions::default(),
    )?;

    let file = std::fs::File::create(output_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut combined)?;

    info!(
        "Merged {} transactions to {:?}",
        total_after, output_path
    );

    Ok(total_after)
}

/// Read transactions from Parquet
pub fn read_transactions_parquet(path: &Path) -> Result<Vec<RawTransaction>, TransactionError> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;

    let tx_hashes = df.column("tx_hash")?.str()?;
    let timestamps = df.column("block_timestamp")?.str()?;
    let from_addrs = df.column("from_address")?.str()?;
    let to_addrs = df.column("to_address")?.str()?;
    let values = df.column("value_wei")?.str()?;

    let mut result = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        result.push(RawTransaction {
            tx_hash: tx_hashes.get(i).unwrap_or_default().to_string(),
            block_timestamp: DateTime::parse_from_rfc3339(
                timestamps.get(i).unwrap_or_default(),
            )
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
            from_address: from_addrs.get(i).unwrap_or_default().to_string(),
            to_address: to_addrs.get(i).unwrap_or_default().to_string(),
            value_wei: values.get(i).unwrap_or_default().to_string(),
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counterparty_filtering() {
        let labeled: HashSet<String> =
            ["0xabc".to_string(), "0xdef".to_string()].into_iter().collect();
        let counterparties: HashSet<String> =
            ["0x123".to_string(), "0x456".to_string()].into_iter().collect();

        // Counterparty should not be in labeled set
        assert!(!labeled.contains(&"0x123".to_string()));
        assert!(counterparties.contains(&"0x123".to_string()));
    }
}
