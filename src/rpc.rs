//! Ethereum JSON-RPC client with rate limiting and retry logic.
//!
//! Provides a robust wrapper around Alchemy RPC with:
//! - Rate limiting (compute units aware)
//! - Exponential backoff with jitter
//! - Batch request support
//! - Proper error handling

use crate::config::RateLimitConfig;
use alloy_primitives::{Address, Bytes, TxHash, U256};
use backoff::ExponentialBackoffBuilder;
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

/// Rate-limited Ethereum RPC client
pub struct EthRpcClient {
    client: reqwest::Client,
    rpc_url: String,
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
    pub fn new(rpc_url: String, config: RateLimitConfig) -> Self {
        let quota = Quota::per_second(NonZeroU32::new(config.requests_per_second).unwrap());
        let rate_limiter = RateLimiter::direct(quota);

        // Concurrent request limit
        let semaphore = Arc::new(Semaphore::new(config.batch_size));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            rpc_url,
            rate_limiter,
            semaphore,
            config,
            request_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    fn next_id(&self) -> u64 {
        self.request_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Make a single RPC call with retry logic
    async fn call<P: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: P,
    ) -> Result<R, RpcError> {
        let backoff = ExponentialBackoffBuilder::default()
            .with_initial_interval(Duration::from_millis(self.config.base_delay_ms))
            .with_max_interval(Duration::from_secs(30))
            .with_max_elapsed_time(Some(Duration::from_secs(120)))
            .build();

        let op = || async {
            // Wait for rate limit
            self.rate_limiter.until_ready().await;

            let _permit = self.semaphore.acquire().await.unwrap();

            let request = JsonRpcRequest {
                jsonrpc: "2.0",
                method,
                params: &params,
                id: self.next_id(),
            };

            let response = self
                .client
                .post(&self.rpc_url)
                .json(&request)
                .send()
                .await
                .map_err(|e| {
                    if e.is_timeout() || e.is_connect() {
                        backoff::Error::transient(RpcError::Http(e))
                    } else {
                        backoff::Error::permanent(RpcError::Http(e))
                    }
                })?;

            if response.status() == 429 {
                warn!("Rate limited, backing off...");
                return Err(backoff::Error::transient(RpcError::RateLimitExceeded));
            }

            let json_response: JsonRpcResponse<R> =
                response.json().await.map_err(|e| {
                    backoff::Error::permanent(RpcError::Http(e))
                })?;

            if let Some(error) = json_response.error {
                // Some errors are transient
                if error.code == -32005 || error.message.contains("rate") {
                    return Err(backoff::Error::transient(RpcError::JsonRpc {
                        code: error.code,
                        message: error.message,
                    }));
                }
                return Err(backoff::Error::permanent(RpcError::JsonRpc {
                    code: error.code,
                    message: error.message,
                }));
            }

            json_response
                .result
                .ok_or_else(|| {
                    backoff::Error::permanent(RpcError::InvalidResponse(
                        "Missing result in response".into(),
                    ))
                })
        };

        backoff::future::retry(backoff, op).await
    }

    /// Batch RPC calls (Alchemy supports up to 100 per batch)
    pub async fn batch_call<P: Serialize + Clone, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params_list: Vec<P>,
    ) -> Result<Vec<R>, RpcError> {
        let mut results = Vec::with_capacity(params_list.len());

        for chunk in params_list.chunks(self.config.batch_size) {
            // Wait for rate limit (batch counts as one request for CU purposes)
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

            let response = self
                .client
                .post(&self.rpc_url)
                .json(&requests)
                .send()
                .await?;

            if response.status() == 429 {
                warn!("Batch rate limited");
                return Err(RpcError::RateLimitExceeded);
            }

            let responses: Vec<JsonRpcResponse<R>> = response.json().await?;

            // Sort by id to maintain order
            let mut sorted_responses = responses;
            sorted_responses.sort_by_key(|r| r.id);

            for resp in sorted_responses {
                if let Some(error) = resp.error {
                    warn!("Batch item error: {} - {}", error.code, error.message);
                    continue;
                }
                if let Some(result) = resp.result {
                    results.push(result);
                }
            }
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

    /// Get transactions in a block range (using alchemy_getAssetTransfers for efficiency)
    /// This is Alchemy-specific but much more efficient than scanning blocks
    pub async fn get_asset_transfers(
        &self,
        from_block: u64,
        to_block: u64,
        addresses: &[String],
        direction: TransferDirection,
    ) -> Result<Vec<AssetTransfer>, RpcError> {
        let from_hex = format!("0x{:x}", from_block);
        let to_hex = format!("0x{:x}", to_block);

        let params = match direction {
            TransferDirection::From => serde_json::json!({
                "fromBlock": from_hex,
                "toBlock": to_hex,
                "fromAddress": addresses,
                "category": ["external"],
                "withMetadata": true,
                "excludeZeroValue": true,
                "maxCount": "0x3e8" // 1000
            }),
            TransferDirection::To => serde_json::json!({
                "fromBlock": from_hex,
                "toBlock": to_hex,
                "toAddress": addresses,
                "category": ["external"],
                "withMetadata": true,
                "excludeZeroValue": true,
                "maxCount": "0x3e8"
            }),
        };

        let response: AssetTransfersResponse = self
            .call("alchemy_getAssetTransfers", [params])
            .await?;

        debug!(
            "Got {} transfers for {} addresses",
            response.transfers.len(),
            addresses.len()
        );

        Ok(response.transfers)
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
                // Convert ETH to wei (multiply by 10^18)
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
}
