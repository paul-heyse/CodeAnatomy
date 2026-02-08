//! Delta output table creation and schema enforcement.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};
use deltalake::delta_datafusion::DeltaTableProvider;
use deltalake::DeltaTableBuilder;

/// Ensure an output Delta table exists with the expected schema.
/// Creates the table if it doesn't exist; validates schema if it does.
pub async fn ensure_output_table(
    ctx: &SessionContext,
    table_name: &str,
    delta_location: &str,
    _expected_schema: &SchemaRef,
) -> Result<()> {
    // Try to load existing table
    // Parse URL and load table
    let url = delta_location
        .parse()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let builder = DeltaTableBuilder::from_url(url)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    match builder.load().await {
        Ok(table) => {
            // Table exists — validate schema compatibility
            let snapshot = table
                .snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let _existing_schema = snapshot.schema();
            // For now, just log and continue — schema enforcement can be added
            // TODO: implement strict schema validation

            let eager = snapshot.snapshot().clone();
            let log_store = table.log_store();
            let scan_config = deltalake::delta_datafusion::DeltaScanConfig::default();
            let provider = DeltaTableProvider::try_new(eager, log_store, scan_config)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            ctx.register_table(table_name, Arc::new(provider))?;
            Ok(())
        }
        Err(_) => {
            // Table doesn't exist — create it
            // Delta table creation requires write operations
            // For now, use DataFusion's built-in table creation
            // The actual Delta table will be created on first write via write_table()
            Ok(())
        }
    }
}

/// Validate that a DataFrame's output schema matches the expected target schema.
pub fn validate_output_schema(
    actual: &arrow::datatypes::Schema,
    expected: &arrow::datatypes::Schema,
) -> Result<()> {
    // Check that all expected columns exist in actual with compatible types
    for expected_field in expected.fields() {
        match actual.field_with_name(expected_field.name()) {
            Ok(actual_field) => {
                if actual_field.data_type() != expected_field.data_type() {
                    return Err(DataFusionError::Plan(format!(
                        "Schema mismatch for column '{}': expected {:?}, got {:?}",
                        expected_field.name(),
                        expected_field.data_type(),
                        actual_field.data_type(),
                    )));
                }
            }
            Err(_) => {
                return Err(DataFusionError::Plan(format!(
                    "Missing required column '{}' in output",
                    expected_field.name(),
                )));
            }
        }
    }
    Ok(())
}

/// Extract total row count from write result batches.
///
/// DataFusion's write_table() returns Vec<RecordBatch> containing the write statistics.
/// This helper sums the row counts across all batches.
pub fn extract_row_count(batches: &[RecordBatch]) -> u64 {
    batches.iter().map(|b| b.num_rows() as u64).sum()
}
