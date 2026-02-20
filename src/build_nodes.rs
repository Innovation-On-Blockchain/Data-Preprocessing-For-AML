//! Node metadata generation module.
//!
//! Builds node metadata table from edges and labels, including:
//! - Contract detection via eth_getCode
//! - Degree calculation (in/out)
//! - Hub classification (top 0.1% by total degree)

use crate::config::PipelineConfig;
use crate::ethereum::ValidatedAddress;
use crate::rpc::EthRpcClient;
use crate::schemas::{NodeMetadata, NodeType};
use alloy_primitives::Address;
use polars::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    #[error("RPC error: {0}")]
    Rpc(#[from] crate::rpc::RpcError),

    #[error("Invalid address: {0}")]
    InvalidAddress(String),
}

/// Node metadata builder
pub struct NodeBuilder {
    client: Option<EthRpcClient>,
    /// Pre-loaded set of known contract addresses (from CSV). When present,
    /// `detect_contracts` does a local HashSet lookup instead of RPC calls.
    known_contracts: Option<HashSet<String>>,
    hub_percentile: f64,
}

impl NodeBuilder {
    pub fn new(config: &PipelineConfig) -> Self {
        let client = EthRpcClient::new(
            config.alchemy_base_url.clone(),
            config.alchemy_key_pool.clone(),
            config.rate_limits.clone(),
        );
        Self {
            client: Some(client),
            known_contracts: None,
            hub_percentile: config.aggregation.hub_percentile,
        }
    }

    /// Create a builder without RPC (for testing or offline mode)
    pub fn offline(hub_percentile: f64) -> Self {
        Self {
            client: None,
            known_contracts: None,
            hub_percentile,
        }
    }

    /// Create a builder that loads contract addresses from all CSV files in a directory.
    /// Each CSV should have an address column (lines starting with `0x` are treated as addresses).
    pub fn from_contracts_dir(dir_path: &Path, hub_percentile: f64) -> Result<Self, NodeError> {
        use std::io::{BufRead, BufReader};

        let mut contracts = HashSet::new();
        let mut file_count = 0usize;

        let mut entries: Vec<_> = std::fs::read_dir(dir_path)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "csv")
            })
            .collect();
        entries.sort_by_key(|e| e.path());

        for entry in &entries {
            let file = std::fs::File::open(entry.path())?;
            let reader = BufReader::new(file);

            for line in reader.lines() {
                let line = line?;
                // CSV rows may be: just an address, or "address,..." columns.
                // Take the first comma-separated field.
                let field = line.split(',').next().unwrap_or("").trim().to_lowercase();
                if field.starts_with("0x") && field.len() == 42 {
                    contracts.insert(field);
                }
            }
            file_count += 1;

            if file_count % 20 == 0 {
                info!(
                    "Loaded {}/{} CSV files ({} addresses so far)",
                    file_count,
                    entries.len(),
                    contracts.len()
                );
            }
        }

        info!(
            "Loaded {} contract addresses from {} CSV files in {:?}",
            contracts.len(),
            file_count,
            dir_path
        );

        Ok(Self {
            client: None,
            known_contracts: Some(contracts),
            hub_percentile,
        })
    }

    /// Build node metadata from edges and labels files
    pub async fn build_nodes(
        &self,
        edges_path: &Path,
        labels_path: &Path,
        output_path: &Path,
    ) -> Result<usize, NodeError> {
        info!("Building node metadata from {:?} and {:?}", edges_path, labels_path);

        // Collect all unique addresses from edges
        let (addresses, degree_in, degree_out) = self.extract_addresses_and_degrees(edges_path)?;

        // Add addresses from labels that might not appear in edges
        let label_addresses = self.extract_label_addresses(labels_path)?;

        let all_addresses: HashSet<String> = addresses
            .union(&label_addresses)
            .cloned()
            .collect();

        info!("Found {} unique addresses", all_addresses.len());

        // Determine contract status for each address
        let contract_status = self.detect_contracts(&all_addresses).await?;

        // Calculate hub threshold (top 0.1% = 99.9th percentile)
        let hub_threshold = self.calculate_hub_threshold(&degree_in, &degree_out);
        info!("Hub threshold (top {:.1}%): {} total degree",
              100.0 - self.hub_percentile, hub_threshold);

        // Build node metadata
        let mut nodes: Vec<NodeMetadata> = Vec::with_capacity(all_addresses.len());

        for addr in &all_addresses {
            let in_deg = *degree_in.get(addr).unwrap_or(&0);
            let out_deg = *degree_out.get(addr).unwrap_or(&0);
            let total_deg = in_deg + out_deg;

            let is_contract = *contract_status.get(addr).unwrap_or(&false);

            // Determine node type
            let node_type = if total_deg >= hub_threshold {
                NodeType::Hub
            } else if is_contract {
                NodeType::Contract
            } else {
                NodeType::Eoa
            };

            nodes.push(NodeMetadata {
                address: addr.clone(),
                is_contract,
                degree_in: in_deg,
                degree_out: out_deg,
                node_type,
            });
        }

        // Sort by address for determinism
        nodes.sort_by(|a, b| a.address.cmp(&b.address));

        // Write to Parquet
        self.write_nodes_parquet(&nodes, output_path)?;

        info!("Built metadata for {} nodes", nodes.len());
        Ok(nodes.len())
    }

    /// Extract unique addresses and compute degree from edges
    fn extract_addresses_and_degrees(
        &self,
        edges_path: &Path,
    ) -> Result<(HashSet<String>, HashMap<String, i64>, HashMap<String, i64>), NodeError> {
        let file = std::fs::File::open(edges_path)?;
        let df = ParquetReader::new(file).finish()?;

        let srcs = df.column("src")?.str()?;
        let dsts = df.column("dst")?.str()?;
        let tx_counts = df.column("tx_count")?.i64()?;

        let mut addresses = HashSet::new();
        let mut degree_in: HashMap<String, i64> = HashMap::new();
        let mut degree_out: HashMap<String, i64> = HashMap::new();

        for i in 0..df.height() {
            let src = srcs.get(i).unwrap_or_default().to_string();
            let dst = dsts.get(i).unwrap_or_default().to_string();
            let _count = tx_counts.get(i).unwrap_or(1);

            addresses.insert(src.clone());
            addresses.insert(dst.clone());

            // Degree is number of unique counterparties, not tx count
            // But we weight by edge existence
            *degree_out.entry(src).or_insert(0) += 1;
            *degree_in.entry(dst).or_insert(0) += 1;
        }

        Ok((addresses, degree_in, degree_out))
    }

    /// Extract addresses from labels file
    fn extract_label_addresses(&self, labels_path: &Path) -> Result<HashSet<String>, NodeError> {
        if !labels_path.exists() {
            return Ok(HashSet::new());
        }

        let file = std::fs::File::open(labels_path)?;
        let df = ParquetReader::new(file).finish()?;

        let addresses = df.column("address")?.str()?;

        let mut result = HashSet::new();
        for i in 0..df.height() {
            if let Some(addr) = addresses.get(i) {
                result.insert(addr.to_string());
            }
        }

        Ok(result)
    }

    /// Load cached contract detection results from disk.
    fn load_contract_cache(cache_path: &Path) -> HashMap<String, bool> {
        match std::fs::read_to_string(cache_path) {
            Ok(contents) => {
                let mut cache = HashMap::new();
                for line in contents.lines() {
                    if let Some((addr, val)) = line.split_once(',') {
                        cache.insert(addr.to_string(), val == "1");
                    }
                }
                info!("Loaded {} cached contract detection results", cache.len());
                cache
            }
            Err(_) => HashMap::new(),
        }
    }

    /// Append a batch of results to the cache file.
    fn append_contract_cache(
        cache_file: &mut std::io::BufWriter<std::fs::File>,
        entries: &[(String, bool)],
    ) {
        use std::io::Write;
        for (addr, is_contract) in entries {
            let _ = writeln!(cache_file, "{},{}", addr, if *is_contract { "1" } else { "0" });
        }
        let _ = cache_file.flush();
    }

    /// Detect which addresses are contracts.
    /// Uses local CSV lookup if available, otherwise falls back to eth_getCode RPC calls
    /// with disk caching for resumability.
    async fn detect_contracts(
        &self,
        addresses: &HashSet<String>,
    ) -> Result<HashMap<String, bool>, NodeError> {
        let mut result = HashMap::new();

        // Fast path: local CSV-based lookup (no RPC calls).
        if let Some(known) = &self.known_contracts {
            let mut contract_count = 0usize;
            for addr in addresses {
                let is_contract = known.contains(&addr.to_lowercase());
                if is_contract {
                    contract_count += 1;
                }
                result.insert(addr.clone(), is_contract);
            }
            info!(
                "Local contract lookup complete: {} contracts, {} EOAs (from {} known contracts)",
                contract_count,
                addresses.len() - contract_count,
                known.len()
            );
            return Ok(result);
        }

        let client = match &self.client {
            Some(c) => c,
            None => {
                // Offline mode: assume all are EOAs
                warn!("Running in offline mode - assuming all addresses are EOAs");
                for addr in addresses {
                    result.insert(addr.clone(), false);
                }
                return Ok(result);
            }
        };

        info!("Detecting contracts for {} addresses...", addresses.len());

        // Load cached results from previous runs.
        let cache_path = Path::new("data/metadata/contract_cache.csv");
        let cached = Self::load_contract_cache(cache_path);

        // Separate already-cached from uncached addresses.
        let mut need_query: Vec<String> = Vec::new();
        for addr in addresses {
            if let Some(&val) = cached.get(addr) {
                result.insert(addr.clone(), val);
            } else {
                need_query.push(addr.clone());
            }
        }

        if need_query.is_empty() {
            info!("All {} addresses found in cache, skipping RPC calls", result.len());
            return Ok(result);
        }

        info!(
            "{} addresses cached, {} remaining to query via RPC",
            result.len(),
            need_query.len()
        );

        // Convert to alloy addresses
        let mut addr_map: HashMap<Address, String> = HashMap::new();
        let mut valid_addresses: Vec<Address> = Vec::new();

        for addr_str in &need_query {
            if let Ok(_validated) = ValidatedAddress::parse(addr_str) {
                let alloy_addr: Address = addr_str.parse().unwrap_or_default();
                addr_map.insert(alloy_addr, addr_str.clone());
                valid_addresses.push(alloy_addr);
            } else {
                warn!("Invalid address skipped: {}", addr_str);
                result.insert(addr_str.clone(), false);
            }
        }

        // Open cache file for appending new results.
        let cache_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(cache_path)?;
        let mut cache_writer = std::io::BufWriter::new(cache_file);

        // Batch detect contracts
        // eth_getCode = 26 CU each. 18 × 26 = 468 CU per batch, under Alchemy's 500 CU/s limit.
        let batch_size = 18;
        let total_batches = (valid_addresses.len() + batch_size - 1) / batch_size;

        for (batch_idx, chunk) in valid_addresses.chunks(batch_size).enumerate() {
            if batch_idx % 1000 == 0 {
                info!(
                    "Contract detection progress: {}/{} batches ({:.1}%), {} addresses resolved",
                    batch_idx,
                    total_batches,
                    (batch_idx as f64 / total_batches as f64) * 100.0,
                    result.len()
                );
            }

            match client.batch_get_code(chunk).await {
                Ok(codes) => {
                    let mut new_entries = Vec::new();
                    for (addr, is_contract) in codes {
                        if let Some(addr_str) = addr_map.get(&addr) {
                            result.insert(addr_str.clone(), is_contract);
                            new_entries.push((addr_str.clone(), is_contract));
                        }
                    }
                    Self::append_contract_cache(&mut cache_writer, &new_entries);
                }
                Err(e) => {
                    warn!("Batch contract detection failed: {}", e);
                    // Fall back to individual queries or mark as unknown
                    let mut new_entries = Vec::new();
                    for addr in chunk {
                        if let Some(addr_str) = addr_map.get(addr) {
                            result.insert(addr_str.clone(), false);
                            new_entries.push((addr_str.clone(), false));
                        }
                    }
                    Self::append_contract_cache(&mut cache_writer, &new_entries);
                }
            }
        }

        info!(
            "Contract detection complete: {} contracts, {} EOAs",
            result.values().filter(|&&v| v).count(),
            result.values().filter(|&&v| !v).count()
        );

        Ok(result)
    }

    /// Calculate the hub threshold (minimum degree to be classified as hub)
    fn calculate_hub_threshold(
        &self,
        degree_in: &HashMap<String, i64>,
        degree_out: &HashMap<String, i64>,
    ) -> i64 {
        // Calculate total degrees
        let mut total_degrees: Vec<i64> = degree_in
            .keys()
            .chain(degree_out.keys())
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|addr| {
                degree_in.get(addr).unwrap_or(&0) + degree_out.get(addr).unwrap_or(&0)
            })
            .collect();

        if total_degrees.is_empty() {
            return i64::MAX;
        }

        total_degrees.sort_unstable();

        // Calculate percentile index
        let percentile_idx = ((self.hub_percentile / 100.0) * total_degrees.len() as f64) as usize;
        let percentile_idx = percentile_idx.min(total_degrees.len() - 1);

        total_degrees[percentile_idx]
    }

    /// Write nodes to Parquet
    fn write_nodes_parquet(
        &self,
        nodes: &[NodeMetadata],
        output_path: &Path,
    ) -> Result<(), NodeError> {
        let addresses: Vec<&str> = nodes.iter().map(|n| n.address.as_str()).collect();
        let is_contracts: Vec<bool> = nodes.iter().map(|n| n.is_contract).collect();
        let degrees_in: Vec<i64> = nodes.iter().map(|n| n.degree_in).collect();
        let degrees_out: Vec<i64> = nodes.iter().map(|n| n.degree_out).collect();
        let node_types: Vec<String> = nodes.iter().map(|n| n.node_type.to_string()).collect();

        let df = DataFrame::new(vec![
            Column::new("address".into(), addresses),
            Column::new("is_contract".into(), is_contracts),
            Column::new("degree_in".into(), degrees_in),
            Column::new("degree_out".into(), degrees_out),
            Column::new("node_type".into(), node_types),
        ])?;

        let file = std::fs::File::create(output_path)?;
        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df.clone())?;

        info!("Wrote {} nodes to {:?}", nodes.len(), output_path);
        Ok(())
    }
}

/// Read nodes from Parquet
pub fn read_nodes_parquet(path: &Path) -> Result<Vec<NodeMetadata>, NodeError> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;

    let addresses = df.column("address")?.str()?;
    let is_contracts = df.column("is_contract")?.bool()?;
    let degrees_in = df.column("degree_in")?.i64()?;
    let degrees_out = df.column("degree_out")?.i64()?;
    let node_types = df.column("node_type")?.str()?;

    let mut result = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        let node_type_str = node_types.get(i).unwrap_or("eoa");
        let node_type = match node_type_str {
            "hub" => NodeType::Hub,
            "contract" => NodeType::Contract,
            _ => NodeType::Eoa,
        };

        result.push(NodeMetadata {
            address: addresses.get(i).unwrap_or_default().to_string(),
            is_contract: is_contracts.get(i).unwrap_or(false),
            degree_in: degrees_in.get(i).unwrap_or(0),
            degree_out: degrees_out.get(i).unwrap_or(0),
            node_type,
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hub_threshold_calculation() {
        let mut degree_in = HashMap::new();
        let mut degree_out = HashMap::new();

        // Create 1000 addresses with varying degrees
        for i in 0..1000 {
            let addr = format!("0x{:040x}", i);
            degree_in.insert(addr.clone(), i as i64);
            degree_out.insert(addr, (i * 2) as i64);
        }

        let builder = NodeBuilder::offline(99.9);
        let threshold = builder.calculate_hub_threshold(&degree_in, &degree_out);

        // 99.9th percentile of 1000 items should be around index 999
        assert!(threshold > 0);
    }

    #[test]
    fn test_node_type_display() {
        assert_eq!(NodeType::Eoa.to_string(), "eoa");
        assert_eq!(NodeType::Contract.to_string(), "contract");
        assert_eq!(NodeType::Hub.to_string(), "hub");
    }
}
