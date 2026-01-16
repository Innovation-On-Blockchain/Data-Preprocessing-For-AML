//! OFAC SDN list fetching and parsing.
//!
//! Fetches labeled Ethereum addresses from the OFAC Specially Designated Nationals list.
//! Supports multiple data formats (XML, CSV) and extracts ETH addresses.

use crate::ethereum::{extract_ofac_addresses, validate_and_dedupe};
use crate::schemas::LabeledAddress;
use chrono::Utc;
use polars::prelude::*;
use quick_xml::de::from_str as xml_from_str;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;
use thiserror::Error;
use tracing::{info, warn};

/// OFAC data source URLs
pub const OFAC_SDN_XML_URL: &str = "https://sanctionslist.ofac.treas.gov/Home/SdnList?format=xml";
pub const OFAC_SDN_CSV_URL: &str = "https://sanctionslist.ofac.treas.gov/Home/SdnList?format=csv";

/// Alternative: Direct XML file
pub const OFAC_SDN_XML_DIRECT: &str = "https://www.treasury.gov/ofac/downloads/sdn.xml";

/// OFAC advanced features list (contains crypto addresses)
pub const OFAC_SDN_ADVANCED_XML: &str =
    "https://www.treasury.gov/ofac/downloads/sanctions/1.0/sdn_advanced.xml";

#[derive(Error, Debug)]
pub enum OfacError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("XML parsing failed: {0}")]
    XmlError(String),

    #[error("CSV parsing failed: {0}")]
    CsvError(#[from] csv::Error),

    #[error("No Ethereum addresses found in OFAC data")]
    NoAddressesFound,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
}

/// OFAC SDN XML structure (simplified for ETH address extraction)
#[derive(Debug, Deserialize)]
#[serde(rename = "sdnList")]
struct SdnList {
    #[serde(rename = "sdnEntry", default)]
    entries: Vec<SdnEntry>,
}

#[derive(Debug, Deserialize)]
struct SdnEntry {
    #[serde(rename = "uid")]
    _uid: Option<i64>,

    #[serde(rename = "sdnType")]
    _sdn_type: Option<String>,

    #[serde(rename = "idList")]
    id_list: Option<IdList>,

    #[serde(rename = "programList")]
    _program_list: Option<ProgramList>,
}

#[derive(Debug, Deserialize)]
struct IdList {
    #[serde(rename = "id", default)]
    ids: Vec<IdEntry>,
}

#[derive(Debug, Deserialize)]
struct IdEntry {
    #[serde(rename = "idType")]
    id_type: Option<String>,

    #[serde(rename = "idNumber")]
    id_number: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProgramList {
    #[serde(rename = "program", default)]
    _programs: Vec<String>,
}

/// Advanced SDN XML structure (contains digital currency addresses)
#[derive(Debug, Deserialize)]
#[serde(rename = "Sanctions")]
struct SanctionsList {
    #[serde(rename = "DistinctParty", default)]
    parties: Vec<DistinctParty>,
}

#[derive(Debug, Deserialize)]
struct DistinctParty {
    #[serde(rename = "Feature", default)]
    features: Vec<Feature>,
}

#[derive(Debug, Deserialize)]
struct Feature {
    #[serde(rename = "FeatureType")]
    feature_type: Option<FeatureType>,

    #[serde(rename = "VersionDetail", default)]
    version_details: Vec<VersionDetail>,
}

#[derive(Debug, Deserialize)]
struct FeatureType {
    #[serde(rename = "$value")]
    value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VersionDetail {
    #[serde(rename = "DetailValue")]
    detail_value: Option<String>,
}

/// OFAC label fetcher
pub struct OfacFetcher {
    client: reqwest::Client,
}

impl OfacFetcher {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .user_agent("eth-aml-pipeline/1.0")
                .build()
                .expect("Failed to build HTTP client"),
        }
    }

    /// Fetch and parse OFAC SDN data, extracting Ethereum addresses
    pub async fn fetch_labels(&self) -> Result<Vec<LabeledAddress>, OfacError> {
        info!("Fetching OFAC SDN data...");
        let retrieved_at = Utc::now();

        let mut all_addresses: HashSet<String> = HashSet::new();

        // Try multiple sources for robustness
        let sources = [
            (OFAC_SDN_ADVANCED_XML, "advanced_xml"),
            (OFAC_SDN_XML_DIRECT, "sdn_xml"),
            (OFAC_SDN_CSV_URL, "csv"),
        ];

        for (url, source_type) in sources {
            match self.fetch_from_url(url, source_type).await {
                Ok(addresses) => {
                    info!(
                        "Found {} raw addresses from {} ({})",
                        addresses.len(),
                        source_type,
                        url
                    );
                    all_addresses.extend(addresses);
                }
                Err(e) => {
                    warn!("Failed to fetch from {}: {}", url, e);
                }
            }
        }

        if all_addresses.is_empty() {
            return Err(OfacError::NoAddressesFound);
        }

        // Validate and deduplicate
        let validated = validate_and_dedupe(all_addresses.into_iter().collect());
        info!(
            "Validated and deduplicated to {} unique Ethereum addresses",
            validated.len()
        );

        // Convert to LabeledAddress records
        let labels: Vec<LabeledAddress> = validated
            .into_iter()
            .map(|addr| LabeledAddress {
                address: addr.to_checksum(),
                label: LabeledAddress::LABEL_SANCTIONED,
                label_source: "OFAC_SDN".to_string(),
                source_url: OFAC_SDN_ADVANCED_XML.to_string(),
                retrieved_at,
            })
            .collect();

        Ok(labels)
    }

    async fn fetch_from_url(&self, url: &str, source_type: &str) -> Result<Vec<String>, OfacError> {
        let response = self.client.get(url).send().await?;

        if !response.status().is_success() {
            return Err(OfacError::HttpError(
                response.error_for_status().unwrap_err(),
            ));
        }

        let content = response.text().await?;

        match source_type {
            "advanced_xml" => self.parse_advanced_xml(&content),
            "sdn_xml" => self.parse_sdn_xml(&content),
            "csv" => self.parse_csv(&content),
            _ => Ok(extract_ofac_addresses(&content)),
        }
    }

    fn parse_advanced_xml(&self, content: &str) -> Result<Vec<String>, OfacError> {
        // The advanced XML has a different structure with digital currency addresses
        // Look for features with type "Digital Currency Address"
        let mut addresses = Vec::new();

        // Try structured parsing first
        if let Ok(sanctions) = xml_from_str::<SanctionsList>(content) {
            for party in sanctions.parties {
                for feature in party.features {
                    let is_crypto = feature
                        .feature_type
                        .as_ref()
                        .and_then(|ft| ft.value.as_ref())
                        .map(|v| {
                            v.contains("Digital Currency")
                                || v.contains("Ethereum")
                                || v.contains("ETH")
                        })
                        .unwrap_or(false);

                    if is_crypto {
                        for detail in feature.version_details {
                            if let Some(value) = detail.detail_value {
                                addresses.extend(extract_ofac_addresses(&value));
                            }
                        }
                    }
                }
            }
        }

        // Also do regex extraction as fallback
        // OFAC lists addresses in format like "ETH: 0x..."
        addresses.extend(extract_ofac_addresses(content));

        Ok(addresses)
    }

    fn parse_sdn_xml(&self, content: &str) -> Result<Vec<String>, OfacError> {
        let mut addresses = Vec::new();

        // Try structured parsing
        if let Ok(sdn_list) = xml_from_str::<SdnList>(content) {
            for entry in sdn_list.entries {
                if let Some(id_list) = entry.id_list {
                    for id in id_list.ids {
                        // Look for digital currency identifiers
                        let is_crypto = id
                            .id_type
                            .as_ref()
                            .map(|t| {
                                t.contains("Digital Currency")
                                    || t.contains("Ethereum")
                                    || t.contains("Crypto")
                            })
                            .unwrap_or(false);

                        if is_crypto {
                            if let Some(number) = id.id_number {
                                addresses.extend(extract_ofac_addresses(&number));
                            }
                        }
                    }
                }
            }
        }

        // Fallback: regex extraction
        addresses.extend(extract_ofac_addresses(content));

        Ok(addresses)
    }

    fn parse_csv(&self, content: &str) -> Result<Vec<String>, OfacError> {
        // OFAC CSV has addresses in various columns
        // Just extract all addresses from the entire content
        Ok(extract_ofac_addresses(content))
    }
}

impl Default for OfacFetcher {
    fn default() -> Self {
        Self::new()
    }
}

/// Write labeled addresses to Parquet file
pub fn write_labels_parquet(
    labels: &[LabeledAddress],
    output_path: &Path,
) -> Result<(), OfacError> {
    info!("Writing {} labels to {:?}", labels.len(), output_path);

    // Build columns
    let addresses: Vec<&str> = labels.iter().map(|l| l.address.as_str()).collect();
    let label_values: Vec<i32> = labels.iter().map(|l| l.label).collect();
    let sources: Vec<&str> = labels.iter().map(|l| l.label_source.as_str()).collect();
    let urls: Vec<&str> = labels.iter().map(|l| l.source_url.as_str()).collect();
    let timestamps: Vec<String> = labels
        .iter()
        .map(|l| l.retrieved_at.to_rfc3339())
        .collect();

    // Create DataFrame
    let df = DataFrame::new(vec![
        Column::new("address".into(), addresses),
        Column::new("label".into(), label_values),
        Column::new("label_source".into(), sources),
        Column::new("source_url".into(), urls),
        Column::new("retrieved_at".into(), timestamps),
    ])?;

    // Sort by address for determinism
    let df = df.sort(["address"], SortMultipleOptions::default())?;

    // Write to Parquet with compression
    let file = std::fs::File::create(output_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df.clone())?;

    info!("Successfully wrote labels to {:?}", output_path);
    Ok(())
}

/// Read labels from Parquet file
pub fn read_labels_parquet(path: &Path) -> Result<Vec<LabeledAddress>, OfacError> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;

    let addresses = df.column("address")?.str()?;
    let labels = df.column("label")?.i32()?;
    let sources = df.column("label_source")?.str()?;
    let urls = df.column("source_url")?.str()?;
    let timestamps = df.column("retrieved_at")?.str()?;

    let mut result = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        result.push(LabeledAddress {
            address: addresses.get(i).unwrap_or_default().to_string(),
            label: labels.get(i).unwrap_or(0),
            label_source: sources.get(i).unwrap_or_default().to_string(),
            source_url: urls.get(i).unwrap_or_default().to_string(),
            retrieved_at: chrono::DateTime::parse_from_rfc3339(
                timestamps.get(i).unwrap_or_default(),
            )
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_eth_addresses_from_ofac_format() {
        // OFAC typically formats as "ETH: 0x..." or just the address
        let text = r#"
            Digital Currency Address - ETH: 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
            Another address: 0x1234567890123456789012345678901234567890
        "#;

        let addresses = extract_ofac_addresses(text);
        assert_eq!(addresses.len(), 2);
    }
}
