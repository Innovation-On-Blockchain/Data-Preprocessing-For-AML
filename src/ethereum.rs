//! Ethereum address validation and utilities.
//!
//! Provides EIP-55 checksum validation and address normalization.

use alloy_primitives::Address;
use regex::Regex;
use std::sync::LazyLock;
use thiserror::Error;

/// Regex pattern for Ethereum addresses (case-insensitive)
static ETH_ADDRESS_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)0x[a-fA-F0-9]{40}").expect("Invalid regex pattern")
});

/// Regex for extracting addresses from OFAC data (may have different formats)
static OFAC_ADDRESS_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    // OFAC sometimes lists addresses without 0x prefix or with extra formatting
    Regex::new(r"(?i)(?:0x)?([a-fA-F0-9]{40})").expect("Invalid regex pattern")
});

#[derive(Error, Debug)]
pub enum AddressError {
    #[error("Invalid address format: {0}")]
    InvalidFormat(String),

    #[error("Invalid checksum for address: {0}")]
    InvalidChecksum(String),

    #[error("Address too short or long: {0}")]
    InvalidLength(String),
}

/// Validated Ethereum address with EIP-55 checksum
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValidatedAddress(Address);

impl ValidatedAddress {
    /// Parse and validate an Ethereum address string
    pub fn parse(input: &str) -> Result<Self, AddressError> {
        let trimmed = input.trim();

        // Normalize: add 0x prefix if missing
        let normalized = if trimmed.starts_with("0x") || trimmed.starts_with("0X") {
            trimmed.to_string()
        } else {
            format!("0x{}", trimmed)
        };

        // Validate length
        if normalized.len() != 42 {
            return Err(AddressError::InvalidLength(input.to_string()));
        }

        // Parse using alloy (handles checksum validation internally)
        let address: Address = normalized
            .parse()
            .map_err(|_| AddressError::InvalidFormat(input.to_string()))?;

        Ok(Self(address))
    }

    /// Get the EIP-55 checksummed string representation
    pub fn to_checksum(&self) -> String {
        self.0.to_checksum(None)
    }

    /// Get the inner alloy Address
    pub fn inner(&self) -> &Address {
        &self.0
    }

    /// Check if address is the zero address
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl std::fmt::Display for ValidatedAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_checksum())
    }
}

impl From<Address> for ValidatedAddress {
    fn from(addr: Address) -> Self {
        Self(addr)
    }
}

/// Extract all Ethereum addresses from a text blob
pub fn extract_addresses(text: &str) -> Vec<String> {
    ETH_ADDRESS_PATTERN
        .find_iter(text)
        .map(|m| m.as_str().to_string())
        .collect()
}

/// Extract addresses from OFAC-formatted data (may lack 0x prefix)
pub fn extract_ofac_addresses(text: &str) -> Vec<String> {
    OFAC_ADDRESS_PATTERN
        .captures_iter(text)
        .filter_map(|cap| {
            cap.get(1).map(|m| format!("0x{}", m.as_str()))
        })
        .collect()
}

/// Validate and normalize a list of addresses, deduplicating
pub fn validate_and_dedupe(addresses: Vec<String>) -> Vec<ValidatedAddress> {
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();

    for addr in addresses {
        if let Ok(validated) = ValidatedAddress::parse(&addr) {
            if !validated.is_zero() && seen.insert(validated.to_checksum()) {
                result.push(validated);
            }
        }
    }

    // Sort for determinism
    result.sort_by(|a, b| a.to_checksum().cmp(&b.to_checksum()));
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_address() {
        let addr = ValidatedAddress::parse("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")
            .expect("Should parse valid address");
        assert!(!addr.is_zero());
    }

    #[test]
    fn test_lowercase_address() {
        let addr = ValidatedAddress::parse("0xd8da6bf26964af9d7eed9e03e53415d37aa96045")
            .expect("Should parse lowercase address");
        // Should produce valid checksum
        assert!(addr.to_checksum().contains('A') || addr.to_checksum().contains('B'));
    }

    #[test]
    fn test_no_prefix() {
        let addr = ValidatedAddress::parse("d8da6bf26964af9d7eed9e03e53415d37aa96045")
            .expect("Should parse address without 0x prefix");
        assert!(!addr.is_zero());
    }

    #[test]
    fn test_invalid_address() {
        assert!(ValidatedAddress::parse("0xinvalid").is_err());
        assert!(ValidatedAddress::parse("0x123").is_err());
        assert!(ValidatedAddress::parse("").is_err());
    }

    #[test]
    fn test_extract_addresses() {
        let text = "Send to 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045 or 0x0000000000000000000000000000000000000000";
        let addrs = extract_addresses(text);
        assert_eq!(addrs.len(), 2);
    }

    #[test]
    fn test_dedupe() {
        let addresses = vec![
            "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045".to_string(),
            "0xd8da6bf26964af9d7eed9e03e53415d37aa96045".to_string(), // Same, different case
            "0x0000000000000000000000000000000000000000".to_string(), // Zero address
        ];
        let validated = validate_and_dedupe(addresses);
        assert_eq!(validated.len(), 1); // Deduped and zero filtered
    }
}
