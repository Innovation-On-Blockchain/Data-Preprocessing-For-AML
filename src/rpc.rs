//! Ethereum JSON-RPC client with rate limiting, key rotation, and retry logic.
//!
//! Provides a robust wrapper around Alchemy RPC with:
//! - Rate limiting (compute units aware)
//! - Automatic API key rotation on 429 rate-limit errors
//! - Infinite exponential backoff (the program never exits on transient errors)
//! - Batch request support
//! - Proper error handling

use crate::config::{AlchemyKeyPool, RateLimitConfig};
use alloy_primitives::{Address, Bytes, TxHash, U256};
use governor::{Quota, RateLimiter};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

#[derive(Error, Debug)]
pub enum RpcError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON-RPC error: code={code}, message={message}")]
    JsonRpc { code: i64, message: String },

    #[error("Deserialization error: {0}")]
    Deserialize(#[from] serde_json::Error),

    #[error("Rate limit exceeded after retries")]
    RateLimitExceeded,

    #[error("Invalid response: {0}")]
    InvalidResponse(String),
}

/// JSON-RPC request structure
#[derive(Debug, Serialize)]
struct JsonRpcRequest<'a, P: Serialize> {
    jsonrpc: &'static str,
    method: &'a str,
    params: P,
    id: u64,
}

/// JSON-RPC response structure
#[derive(Debug, Deserialize)]
struct JsonRpcResponse<R> {
    #[allow(dead_code)]
    jsonrpc: String,
    result: Option<R>,
    error: Option<JsonRpcError>,
    id: u64,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

/// Ethereum block with transaction hashes
#[derive(Debug, Deserialize)]
pub struct Block {
    pub number: Option<String>,
    pub timestamp: Option<String>,
    pub transactions: Vec<TxHash>,
}

/// Ethereum transaction receipt (minimal fields)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionReceipt {
    pub transaction_hash: TxHash,
    pub status: Option<String>,
    pub block_number: Option<String>,
}

/// Full transaction data
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub hash: TxHash,
    pub block_number: Option<String>,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub input: Bytes,
}

/// Rate-limited Ethereum RPC client with key rotation.
pub struct EthRpcClient {
    client: reqwest::Client,
    base_url: String,
    key_pool: AlchemyKeyPool,
    rate_limiter: RateLimiter<
        governor::state::NotKeyed,
        governor::state::InMemoryState,
        governor::clock::DefaultClock,
    >,
    semaphore: Arc<Semaphore>,
    config: RateLimitConfig,
    request_id: std::sync::atomic::AtomicU64,
}

impl EthRpcClient {
    pub fn new(base_url: String, key_pool: AlchemyKeyPool, config: RateLimitConfig) -> Self {
        let quota = Quota::per_second(
            NonZeroU32::new(config.requests_per_second).unwrap_or(NonZeroU32::new(1).unwrap()),
        );
        let rate_limiter = RateLimiter::direct(quota);

        // Concurrent request limit
        let semaphore = Arc::new(Semaphore::new(config.batch_size.max(1)));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .pool_max_idle_per_host(20)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            base_url,
            key_pool,
            rate_limiter,
            semaphore,
            config,
            request_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Build the full RPC URL using the current active key.
    fn rpc_url(&self) -> String {
        format!("{}/{}", self.base_url, self.key_pool.current_key())
    }

    fn next_id(&self) -> u64 {
        self.request_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Determine if an error is transient (should be retried).
    fn is_transient_error(code: i64, message: &str) -> bool {
        // -32005 = rate limit / resource unavailable
        // -32603 = internal JSON-RPC error (often transient on Alchemy)
        // -32000 = server error (often transient)
        matches!(code, -32005 | -32603 | -32000)
            || message.to_lowercase().contains("rate")
            || message.to_lowercase().contains("limit")
            || message.to_lowercase().contains("capacity")
            || message.to_lowercase().contains("temporarily")
            || message.to_lowercase().contains("timeout")
    }

    /// Make a single RPC call with **infinite** retry + key rotation.
    /// This method will NEVER return an error for transient failures;
    /// it keeps retrying with exponential backoff and key rotation forever.
    async fn call<P: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: P,
    ) -> Result<R, RpcError> {
        let mut delay = Duration::from_millis(self.config.base_delay_ms);
        let max_delay = Duration::from_secs(120);
        let mut attempt: u64 = 0;

        loop {
            attempt += 1;

            // Wait for rate limiter
            self.rate_limiter.until_ready().await;

            let _permit = self.semaphore.acquire().await.unwrap();

            let request = JsonRpcRequest {
                jsonrpc: "2.0",
                method,
                params: &params,
                id: self.next_id(),
            };

            let url = self.rpc_url();

            let send_result = self
                .client
                .post(&url)
                .json(&request)
                .send()
                .await;

            let response = match send_result {
                Ok(resp) => resp,
                Err(e) => {
                    // Network error — always transient, rotate key and retry
                    warn!(
                        "[attempt {}] Network error on {}: {} — rotating key and retrying in {:?}",
                        attempt, method, e, delay
                    );
                    self.key_pool.rotate();
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                    continue;
                }
            };

            // Handle HTTP 429 — rotate key and retry
            if response.status().as_u16() == 429 {
                warn!(
                    "[attempt {}] HTTP 429 rate limited on {} — rotating key and retrying in {:?}",
                    attempt, method, delay
                );
                self.key_pool.rotate();
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max_delay);
                continue;
            }

            // Handle other HTTP errors
            if response.status().is_server_error() {
                warn!(
                    "[attempt {}] HTTP {} on {} — retrying in {:?}",
                    attempt, response.status(), method, delay
                );
                self.key_pool.rotate();
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max_delay);
                continue;
            }

            // Parse JSON
            let json_response: JsonRpcResponse<R> = match response.json().await {
                Ok(j) => j,
                Err(e) => {
                    warn!(
                        "[attempt {}] Failed to parse JSON for {}: {} — retrying in {:?}",
                        attempt, method, e, delay
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                    continue;
                }
            };

            // Handle JSON-RPC errors
            if let Some(error) = json_response.error {
                if Self::is_transient_error(error.code, &error.message) {
                    warn!(
                        "[attempt {}] Transient RPC error on {}: code={}, msg={} — rotating key, retrying in {:?}",
                        attempt, method, error.code, error.message, delay
                    );
                    self.key_pool.rotate();
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                    continue;
                }
                // Permanent error — return it
                return Err(RpcError::JsonRpc {
                    code: error.code,
                    message: error.message,
                });
            }

            // Success — extract result
            match json_response.result {
                Some(result) => return Ok(result),
                None => {
                    warn!(
                        "[attempt {}] Missing result in response for {} — retrying in {:?}",
                        attempt, method, delay
                    );
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                    continue;
                }
            }
        }
    }

    /// Batch RPC calls with retry + key rotation.
    pub async fn batch_call<P: Serialize + Clone, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params_list: Vec<P>,
    ) -> Result<Vec<R>, RpcError> {
        let mut results = Vec::with_capacity(params_list.len());

        for chunk in params_list.chunks(self.config.batch_size.max(1)) {
            let mut delay = Duration::from_millis(self.config.base_delay_ms);
            let max_delay = Duration::from_secs(120);
            let mut attempt: u64 = 0;

            let chunk_results: Vec<R> = loop {
                attempt += 1;
                self.rate_limiter.until_ready().await;

                let requests: Vec<JsonRpcRequest<'_, &P>> = chunk
                    .iter()
                    .enumerate()
                    .map(|(i, p)| JsonRpcRequest {
                        jsonrpc: "2.0",
                        method,
                        params: p,
                        id: i as u64,
                    })
                    .collect();

                let url = self.rpc_url();

                let send_result = self
                    .client
                    .post(&url)
                    .json(&requests)
                    .send()
                    .await;

                let response = match send_result {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(
                            "[batch attempt {}] Network error: {} — rotating key, retrying in {:?}",
                            attempt, e, delay
                        );
                        self.key_pool.rotate();
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(max_delay);
                        continue;
                    }
                };

                if response.status().as_u16() == 429 || response.status().is_server_error() {
                    warn!(
                        "[batch attempt {}] HTTP {} — rotating key, retrying in {:?}",
                        attempt, response.status(), delay
                    );
                    self.key_pool.rotate();
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                    continue;
                }

                let responses: Vec<JsonRpcResponse<R>> = match response.json().await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(
                            "[batch attempt {}] JSON parse error: {} — retrying in {:?}",
                            attempt, e, delay
                        );
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(max_delay);
                        continue;
                    }
                };

                let mut sorted = responses;
                sorted.sort_by_key(|r| r.id);

                let mut chunk_res = Vec::new();
                for resp in sorted {
                    if let Some(error) = resp.error {
                        warn!("Batch item error: {} - {}", error.code, error.message);
                        continue;
                    }
                    if let Some(result) = resp.result {
                        chunk_res.push(result);
                    }
                }
                break chunk_res;
            };

            results.extend(chunk_results);
        }

        Ok(results)
    }

    /// Get latest block number
    pub async fn get_block_number(&self) -> Result<u64, RpcError> {
        let result: String = self.call("eth_blockNumber", ()).await?;
        u64::from_str_radix(result.trim_start_matches("0x"), 16)
            .map_err(|_| RpcError::InvalidResponse("Invalid block number".into()))
    }

    /// Get block by number (with transaction hashes only)
    pub async fn get_block_by_number(&self, block_number: u64) -> Result<Option<Block>, RpcError> {
        let hex_block = format!("0x{:x}", block_number);
        self.call("eth_getBlockByNumber", (hex_block, false)).await
    }

    /// Get transaction by hash
    pub async fn get_transaction(&self, tx_hash: &TxHash) -> Result<Option<Transaction>, RpcError> {
        let hash_str = format!("{:?}", tx_hash);
        self.call("eth_getTransactionByHash", [hash_str]).await
    }

    /// Get transaction receipt
    pub async fn get_transaction_receipt(
        &self,
        tx_hash: &TxHash,
    ) -> Result<Option<TransactionReceipt>, RpcError> {
        let hash_str = format!("{:?}", tx_hash);
        self.call("eth_getTransactionReceipt", [hash_str]).await
    }

    /// Get code at address (to determine if contract)
    pub async fn get_code(&self, address: &Address) -> Result<Bytes, RpcError> {
        let addr_str = format!("{:?}", address);
        self.call("eth_getCode", (addr_str, "latest")).await
    }

    /// Batch get code for multiple addresses
    pub async fn batch_get_code(&self, addresses: &[Address]) -> Result<Vec<(Address, bool)>, RpcError> {
        let params: Vec<(String, &str)> = addresses
            .iter()
            .map(|a| (format!("{:?}", a), "latest"))
            .collect();

        let results: Vec<Bytes> = self.batch_call("eth_getCode", params).await?;

        Ok(addresses
            .iter()
            .zip(results)
            .map(|(addr, code)| (*addr, code.len() > 0))
            .collect())
    }

    /// Get transactions in a block range using alchemy_getAssetTransfers.
    ///
    /// NOTE: Alchemy's API accepts `fromAddress`/`toAddress` as a **single string**,
    /// not an array. Pass one address per call; the caller loops over addresses.
    ///
    /// Automatically paginates through all results using `pageKey`.
    /// Safety limit of 100 pages (100K transfers) per call to prevent unbounded looping.
    pub async fn get_asset_transfers(
        &self,
        from_block: u64,
        to_block: u64,
        address: &str,
        direction: TransferDirection,
    ) -> Result<Vec<AssetTransfer>, RpcError> {
        let from_hex = format!("0x{:x}", from_block);
        let to_hex = format!("0x{:x}", to_block);

        let mut all_transfers = Vec::new();
        let mut page_key: Option<String> = None;
        let max_pages: u32 = 100;

        for page in 0..max_pages {
            let mut params = match direction {
                TransferDirection::From => serde_json::json!({
                    "fromBlock": from_hex,
                    "toBlock": to_hex,
                    "fromAddress": address,
                    "category": ["external", "internal"],
                    "withMetadata": true,
                    "excludeZeroValue": true,
                    "maxCount": "0x3e8"
                }),
                TransferDirection::To => serde_json::json!({
                    "fromBlock": from_hex,
                    "toBlock": to_hex,
                    "toAddress": address,
                    "category": ["external", "internal"],
                    "withMetadata": true,
                    "excludeZeroValue": true,
                    "maxCount": "0x3e8"
                }),
            };

            if let Some(ref key) = page_key {
                params["pageKey"] = serde_json::json!(key);
            }

            let response: AssetTransfersResponse = self
                .call("alchemy_getAssetTransfers", [params])
                .await?;

            let page_count = response.transfers.len();
            all_transfers.extend(response.transfers);

            match response.page_key {
                Some(key) => {
                    debug!(
                        "Address {} page {}: {} transfers (continuing, total so far: {})",
                        address, page + 1, page_count, all_transfers.len()
                    );
                    page_key = Some(key);
                }
                None => {
                    if page > 0 {
                        debug!(
                            "Address {} pagination complete: {} pages, {} total transfers",
                            address, page + 1, all_transfers.len()
                        );
                    } else {
                        debug!(
                            "Got {} transfers for address {} (single page)",
                            all_transfers.len(), address
                        );
                    }
                    break;
                }
            }

            if page == max_pages - 1 {
                warn!(
                    "Address {} hit pagination safety limit ({} pages, {} transfers). Some transfers may be missing.",
                    address, max_pages, all_transfers.len()
                );
            }
        }

        Ok(all_transfers)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TransferDirection {
    From,
    To,
}

#[derive(Debug, Deserialize)]
pub struct AssetTransfersResponse {
    pub transfers: Vec<AssetTransfer>,
    #[serde(rename = "pageKey")]
    pub page_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetTransfer {
    pub hash: String,
    pub from: String,
    pub to: Option<String>,
    pub value: Option<f64>,
    pub asset: Option<String>,
    pub category: String,
    pub block_num: String,
    pub metadata: Option<TransferMetadata>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferMetadata {
    pub block_timestamp: Option<String>,
}

impl AssetTransfer {
    /// Convert value to wei (string for large numbers)
    pub fn value_wei(&self) -> String {
        self.value
            .map(|v| {
                let wei = v * 1e18;
                format!("{:.0}", wei)
            })
            .unwrap_or_else(|| "0".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_asset_transfer_value_conversion() {
        let transfer = AssetTransfer {
            hash: "0x123".to_string(),
            from: "0x1".to_string(),
            to: Some("0x2".to_string()),
            value: Some(1.5),
            asset: Some("ETH".to_string()),
            category: "external".to_string(),
            block_num: "0x1".to_string(),
            metadata: None,
        };

        let wei = transfer.value_wei();
        assert_eq!(wei, "1500000000000000000");
    }

    #[test]
    fn test_is_transient_error() {
        assert!(EthRpcClient::is_transient_error(-32005, "rate limit exceeded"));
        assert!(EthRpcClient::is_transient_error(-32603, "internal error"));
        assert!(EthRpcClient::is_transient_error(0, "Rate limit reached"));
        assert!(!EthRpcClient::is_transient_error(-32600, "invalid request"));
    }
}
