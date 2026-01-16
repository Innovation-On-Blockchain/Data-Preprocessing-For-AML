//! Graph aggregation module.
//!
//! Aggregates raw transactions into weekly edge summaries.
//! Uses Polars lazy evaluation for memory-efficient processing.

use crate::config::AggregationGranularity;
use crate::schemas::WeeklyEdge;
use chrono::{Datelike, NaiveDate};
use polars::prelude::*;
use std::path::Path;
use thiserror::Error;
use tracing::info;

#[derive(Error, Debug)]
pub enum AggregationError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    #[error("Invalid data: {0}")]
    InvalidData(String),
}

/// Aggregate transactions into weekly edges
pub fn aggregate_to_weekly_edges(
    transactions_path: &Path,
    output_path: &Path,
    granularity: AggregationGranularity,
) -> Result<usize, AggregationError> {
    info!("Aggregating transactions from {:?}", transactions_path);

    // Read transactions using lazy evaluation
    let lf = LazyFrame::scan_parquet(transactions_path, Default::default())?;

    // Add week_start column based on block_timestamp
    let lf = lf.with_column(
        col("block_timestamp")
            .cast(DataType::String)
            .str()
            .to_datetime(
                Some(TimeUnit::Milliseconds),
                None,
                StrptimeOptions::default(),
                lit("raise"),
            )
            .alias("ts_datetime"),
    );

    // Extract date and compute period start
    let lf = match granularity {
        AggregationGranularity::Weekly => {
            lf.with_column(
                col("ts_datetime")
                    .dt()
                    .truncate(lit("1w"))
                    .dt()
                    .date()
                    .alias("period_start"),
            )
        }
        AggregationGranularity::Daily => {
            lf.with_column(
                col("ts_datetime")
                    .dt()
                    .date()
                    .alias("period_start"),
            )
        }
        AggregationGranularity::Monthly => {
            lf.with_column(
                col("ts_datetime")
                    .dt()
                    .truncate(lit("1mo"))
                    .dt()
                    .date()
                    .alias("period_start"),
            )
        }
    };

    // Group by (from, to, period_start) and aggregate
    let aggregated = lf
        .group_by([
            col("from_address").alias("src"),
            col("to_address").alias("dst"),
            col("period_start").alias("week_start"),
        ])
        .agg([
            col("tx_hash").count().alias("tx_count"),
            // For value aggregation, we need to handle large numbers
            // Since values are strings, we'll count transactions and note that
            // proper value aggregation requires numeric handling
            col("value_wei").first().alias("sample_value"),
        ]);

    // Collect and sort
    let mut df = aggregated.collect()?;

    // Sort for determinism
    df = df.sort(
        ["src", "dst", "week_start"],
        SortMultipleOptions::default(),
    )?;

    let row_count = df.height();
    info!("Aggregated to {} edges", row_count);

    // For proper value aggregation, we need to handle this separately
    // since Polars doesn't natively support u256 arithmetic
    // In production, you'd use a UDF or process in chunks

    // Write output
    let file = std::fs::File::create(output_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df)?;

    info!("Wrote aggregated edges to {:?}", output_path);
    Ok(row_count)
}

/// Alternative implementation using manual aggregation for large value support
pub fn aggregate_with_large_values(
    transactions_path: &Path,
    output_path: &Path,
) -> Result<usize, AggregationError> {
    info!("Aggregating with large value support from {:?}", transactions_path);

    // Read all transactions
    let file = std::fs::File::open(transactions_path)?;
    let df = ParquetReader::new(file).finish()?;

    // Use a HashMap for aggregation to properly handle u128 values
    use std::collections::HashMap;

    #[derive(Hash, Eq, PartialEq)]
    struct EdgeKey {
        src: String,
        dst: String,
        week_start: NaiveDate,
    }

    struct EdgeValue {
        tx_count: i64,
        total_value: u128,
    }

    let mut aggregation: HashMap<EdgeKey, EdgeValue> = HashMap::new();

    let from_addrs = df.column("from_address")?.str()?;
    let to_addrs = df.column("to_address")?.str()?;
    let timestamps = df.column("block_timestamp")?.str()?;
    let values = df.column("value_wei")?.str()?;

    for i in 0..df.height() {
        let src = from_addrs.get(i).unwrap_or_default().to_string();
        let dst = to_addrs.get(i).unwrap_or_default().to_string();

        // Parse timestamp and get week start
        let ts_str = timestamps.get(i).unwrap_or_default();
        let week_start = parse_timestamp_to_week_start(ts_str);

        // Parse value as u128
        let value_str = values.get(i).unwrap_or("0");
        let value: u128 = value_str.parse().unwrap_or(0);

        let key = EdgeKey { src, dst, week_start };
        let entry = aggregation.entry(key).or_insert(EdgeValue {
            tx_count: 0,
            total_value: 0,
        });
        entry.tx_count += 1;
        entry.total_value = entry.total_value.saturating_add(value);
    }

    // Convert to vectors for DataFrame
    let mut srcs = Vec::with_capacity(aggregation.len());
    let mut dsts = Vec::with_capacity(aggregation.len());
    let mut week_starts = Vec::with_capacity(aggregation.len());
    let mut tx_counts = Vec::with_capacity(aggregation.len());
    let mut total_values = Vec::with_capacity(aggregation.len());

    let mut entries: Vec<_> = aggregation.into_iter().collect();
    entries.sort_by(|a, b| {
        (&a.0.src, &a.0.dst, &a.0.week_start).cmp(&(&b.0.src, &b.0.dst, &b.0.week_start))
    });

    for (key, value) in entries {
        srcs.push(key.src);
        dsts.push(key.dst);
        week_starts.push(key.week_start.to_string());
        tx_counts.push(value.tx_count);
        total_values.push(value.total_value.to_string());
    }

    let df = DataFrame::new(vec![
        Column::new("src".into(), srcs),
        Column::new("dst".into(), dsts),
        Column::new("week_start".into(), week_starts),
        Column::new("tx_count".into(), tx_counts),
        Column::new("total_value_wei".into(), total_values),
    ])?;

    let row_count = df.height();
    info!("Aggregated to {} edges with large value support", row_count);

    let file = std::fs::File::create(output_path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df.clone())?;

    info!("Wrote aggregated edges to {:?}", output_path);
    Ok(row_count)
}

/// Parse ISO timestamp string to week start date
fn parse_timestamp_to_week_start(ts_str: &str) -> NaiveDate {
    let dt = chrono::DateTime::parse_from_rfc3339(ts_str)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|_| chrono::Utc::now().date_naive());

    // Get Monday of the week
    let days_from_monday = dt.weekday().num_days_from_monday();
    dt - chrono::Duration::days(days_from_monday as i64)
}

/// Read edges from Parquet
pub fn read_edges_parquet(path: &Path) -> Result<Vec<WeeklyEdge>, AggregationError> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;

    let srcs = df.column("src")?.str()?;
    let dsts = df.column("dst")?.str()?;
    let week_starts = df.column("week_start")?.str()?;
    let tx_counts = df.column("tx_count")?.i64()?;
    let total_values = df.column("total_value_wei")?.str()?;

    let mut result = Vec::with_capacity(df.height());

    for i in 0..df.height() {
        result.push(WeeklyEdge {
            src: srcs.get(i).unwrap_or_default().to_string(),
            dst: dsts.get(i).unwrap_or_default().to_string(),
            week_start: NaiveDate::parse_from_str(
                week_starts.get(i).unwrap_or("1970-01-01"),
                "%Y-%m-%d",
            )
            .unwrap_or_else(|_| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            tx_count: tx_counts.get(i).unwrap_or(0),
            total_value_wei: total_values.get(i).unwrap_or_default().to_string(),
        });
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_week_start_calculation() {
        // Wednesday 2024-01-03 should have week start Monday 2024-01-01
        let date = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let days_from_monday = date.weekday().num_days_from_monday();
        let week_start = date - chrono::Duration::days(days_from_monday as i64);
        assert_eq!(week_start, NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }

    #[test]
    fn test_parse_timestamp() {
        let ts = "2024-01-03T12:00:00+00:00";
        let week_start = parse_timestamp_to_week_start(ts);
        assert_eq!(week_start, NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }
}
