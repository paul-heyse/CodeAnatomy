//! Delta output table creation and schema enforcement.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};
use datafusion_ext::delta_control_plane::{
    delta_provider_from_session_request, DeltaProviderFromSessionRequest, DeltaScanOverrides,
};
use datafusion_ext::DeltaFeatureGate;
use deltalake::delta_datafusion::DeltaScanConfig;
use deltalake::kernel::{
    ArrayType as DeltaArrayType, DataType as DeltaDataType, MapType as DeltaMapType,
    PrimitiveType, StructField as DeltaStructField, StructType as DeltaStructType,
};
use deltalake::{ensure_table_uri, DeltaTable};

fn map_arrow_type(data_type: &ArrowDataType) -> Result<DeltaDataType> {
    match data_type {
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            Ok(DeltaDataType::Primitive(PrimitiveType::String))
        }
        ArrowDataType::Int64 | ArrowDataType::UInt64 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Long))
        }
        ArrowDataType::Int32 | ArrowDataType::UInt32 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Integer))
        }
        ArrowDataType::Int16 | ArrowDataType::UInt16 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Short))
        }
        ArrowDataType::Int8 | ArrowDataType::UInt8 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Byte))
        }
        ArrowDataType::Float32 => Ok(DeltaDataType::Primitive(PrimitiveType::Float)),
        ArrowDataType::Float64 => Ok(DeltaDataType::Primitive(PrimitiveType::Double)),
        ArrowDataType::Boolean => Ok(DeltaDataType::Primitive(PrimitiveType::Boolean)),
        ArrowDataType::Binary
        | ArrowDataType::LargeBinary
        | ArrowDataType::FixedSizeBinary(_)
        | ArrowDataType::BinaryView => Ok(DeltaDataType::Primitive(PrimitiveType::Binary)),
        ArrowDataType::Date32 | ArrowDataType::Date64 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Date))
        }
        ArrowDataType::Timestamp(_, tz) => {
            if tz.is_some() {
                Ok(DeltaDataType::Primitive(PrimitiveType::Timestamp))
            } else {
                Ok(DeltaDataType::Primitive(PrimitiveType::TimestampNtz))
            }
        }
        ArrowDataType::Decimal128(precision, scale)
        | ArrowDataType::Decimal256(precision, scale) => {
            if *scale < 0 {
                return Err(DataFusionError::Plan(format!(
                    "Negative decimal scale is not supported for Delta output schema: {scale}"
                )));
            }
            let decimal = PrimitiveType::decimal(*precision, *scale as u8).map_err(|err| {
                DataFusionError::Plan(format!(
                    "Invalid decimal precision/scale for Delta output schema: {err}"
                ))
            })?;
            Ok(DeltaDataType::Primitive(decimal))
        }
        ArrowDataType::Struct(fields) => {
            let converted = fields
                .iter()
                .map(|field| map_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>>>()?;
            let struct_type = DeltaStructType::try_new(converted).map_err(|err| {
                DataFusionError::Plan(format!(
                    "Failed to build Delta struct type from Arrow schema: {err}"
                ))
            })?;
            Ok(DeltaDataType::Struct(Box::new(struct_type)))
        }
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::ListView(field)
        | ArrowDataType::LargeListView(field)
        | ArrowDataType::FixedSizeList(field, _) => {
            let element_type = map_arrow_type(field.data_type())?;
            Ok(DeltaDataType::Array(Box::new(DeltaArrayType::new(
                element_type,
                field.is_nullable(),
            ))))
        }
        ArrowDataType::Map(entries, _) => {
            let ArrowDataType::Struct(fields) = entries.data_type() else {
                return Err(DataFusionError::Plan(
                    "Arrow map entries must be represented as struct(key, value)".to_string(),
                ));
            };
            if fields.len() != 2 {
                return Err(DataFusionError::Plan(format!(
                    "Arrow map entries struct must contain exactly 2 fields, found {}",
                    fields.len()
                )));
            }
            let key_type = map_arrow_type(fields[0].data_type())?;
            let value_type = map_arrow_type(fields[1].data_type())?;
            Ok(DeltaDataType::Map(Box::new(DeltaMapType::new(
                key_type,
                value_type,
                fields[1].is_nullable(),
            ))))
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported Arrow type for Delta output schema: {data_type:?}"
        ))),
    }
}

fn map_arrow_field(field: &ArrowField) -> Result<DeltaStructField> {
    Ok(DeltaStructField::new(
        field.name(),
        map_arrow_type(field.data_type())?,
        field.is_nullable(),
    ))
}

fn map_arrow_schema(schema: &ArrowSchema) -> Result<Vec<DeltaStructField>> {
    schema
        .fields()
        .iter()
        .map(|field| map_arrow_field(field.as_ref()))
        .collect()
}

/// Ensure an output Delta table exists with the expected schema.
/// Creates the table if it doesn't exist; validates schema if it does.
pub async fn ensure_output_table(
    ctx: &SessionContext,
    table_name: &str,
    delta_location: &str,
    expected_schema: &SchemaRef,
) -> Result<()> {
    let table_uri = ensure_table_uri(delta_location)
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    let mut table = DeltaTable::try_from_url(table_uri)
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

    if table.version().is_none() {
        let columns = map_arrow_schema(expected_schema.as_ref())?;
        table = table
            .create()
            .with_table_name(table_name)
            .with_columns(columns)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
    }

    let scan_config = DeltaScanConfig::default();
    let scan_overrides = DeltaScanOverrides {
        file_column_name: scan_config.file_column_name,
        enable_parquet_pushdown: Some(scan_config.enable_parquet_pushdown),
        schema_force_view_types: Some(scan_config.schema_force_view_types),
        wrap_partition_values: Some(scan_config.wrap_partition_values),
        schema: scan_config.schema,
    };
    let (provider, _snapshot, _scan_config, _pruned_files, predicate_error) =
        delta_provider_from_session_request(DeltaProviderFromSessionRequest {
            session_ctx: ctx,
            table_uri: delta_location,
            storage_options: None,
            version: table.version(),
            timestamp: None,
            predicate: None,
            overrides: scan_overrides,
            gate: Some(DeltaFeatureGate::default()),
        })
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    if let Some(error) = predicate_error {
        return Err(DataFusionError::Plan(format!(
            "Delta predicate parsing failed while registering output '{table_name}': {error}"
        )));
    }
    validate_output_schema(provider.schema().as_ref(), expected_schema.as_ref())?;
    ctx.register_table(table_name, Arc::new(provider))?;
    Ok(())
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
                if !actual_field
                    .data_type()
                    .equals_datatype(expected_field.data_type())
                {
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

/// Post-write metadata outcome from a Delta table.
///
/// Captures best-effort metadata about the write: delta version, files
/// added, and bytes written. All fields are optional because metadata
/// capture must never block the pipeline.
pub struct WriteOutcome {
    /// Delta table version after the write commit.
    pub delta_version: Option<i64>,
    /// Number of files added by the write operation.
    pub files_added: Option<u64>,
    /// Total bytes written across all added files.
    pub bytes_written: Option<u64>,
}

/// Best-effort read-back of Delta table metadata after a write.
///
/// Attempts to load the Delta log at `delta_location` and extract the
/// latest version plus file-level statistics. If any step fails, returns
/// a `WriteOutcome` with all `None` fields -- metadata capture failure
/// must never block the pipeline.
pub async fn read_write_outcome(delta_location: &str) -> WriteOutcome {
    let table_uri = match ensure_table_uri(delta_location) {
        Ok(uri) => uri,
        Err(_) => {
            return WriteOutcome {
                delta_version: None,
                files_added: None,
                bytes_written: None,
            };
        }
    };

    let mut table = match DeltaTable::try_from_url(table_uri).await {
        Ok(t) => t,
        Err(_) => {
            return WriteOutcome {
                delta_version: None,
                files_added: None,
                bytes_written: None,
            };
        }
    };

    // Load the latest snapshot so version() returns the current version.
    if table.load().await.is_err() {
        return WriteOutcome {
            delta_version: None,
            files_added: None,
            bytes_written: None,
        };
    }

    let delta_version = table.version();

    // Attempt to read file metadata from the snapshot for files_added / bytes_written.
    // Uses the same log_data() API as providers/snapshot.rs::snapshot_metadata().
    let (files_added, bytes_written) = match table.snapshot() {
        Ok(snapshot) => {
            let eager = snapshot.snapshot();
            let log_data = eager.log_data();
            let count = log_data.iter().count() as u64;
            let total_bytes: i64 = log_data.iter().map(|file_view| file_view.size()).sum();
            (Some(count), Some(total_bytes as u64))
        }
        Err(_) => (None, None),
    };

    WriteOutcome {
        delta_version,
        files_added,
        bytes_written,
    }
}

/// Build commit properties from output target metadata and determinism hashes.
///
/// Merges user-supplied write metadata with codeanatomy provenance hashes.
#[derive(Debug, Clone)]
pub struct LineageContext {
    pub spec_hash: [u8; 32],
    pub envelope_hash: [u8; 32],
    pub planning_surface_hash: [u8; 32],
    pub provider_identity_hash: [u8; 32],
    pub rulepack_fingerprint: [u8; 32],
    pub rulepack_profile: String,
    pub runtime_profile_name: Option<String>,
    pub run_started_at_rfc3339: String,
    pub runtime_lineage_tags: BTreeMap<String, String>,
}

pub fn build_commit_properties(
    target: &crate::spec::outputs::OutputTarget,
    lineage: &LineageContext,
) -> std::collections::BTreeMap<String, String> {
    let mut props = target.write_metadata.clone();
    props.insert("codeanatomy.spec_hash".into(), hex::encode(lineage.spec_hash));
    props.insert(
        "codeanatomy.envelope_hash".into(),
        hex::encode(lineage.envelope_hash),
    );
    props.insert(
        "codeanatomy.planning_surface_hash".into(),
        hex::encode(lineage.planning_surface_hash),
    );
    props.insert(
        "codeanatomy.provider_identity_hash".into(),
        hex::encode(lineage.provider_identity_hash),
    );
    props.insert(
        "codeanatomy.rulepack_fingerprint".into(),
        hex::encode(lineage.rulepack_fingerprint),
    );
    props.insert(
        "codeanatomy.rulepack_profile".into(),
        lineage.rulepack_profile.clone(),
    );
    props.insert(
        "codeanatomy.runtime_profile".into(),
        lineage
            .runtime_profile_name
            .clone()
            .unwrap_or_else(|| "none".to_string()),
    );
    props.insert(
        "codeanatomy.run_started_at".into(),
        lineage.run_started_at_rfc3339.clone(),
    );
    for (key, value) in &lineage.runtime_lineage_tags {
        props.insert(format!("codeanatomy.lineage_tag.{key}"), value.clone());
    }
    props
}

/// Build Delta commit options from output-target policy and provenance hashes.
pub fn build_delta_commit_options(
    target: &crate::spec::outputs::OutputTarget,
    lineage: &LineageContext,
) -> datafusion_ext::DeltaCommitOptions {
    let metadata: HashMap<String, String> = build_commit_properties(target, lineage)
        .into_iter()
        .collect();
    datafusion_ext::DeltaCommitOptions {
        metadata,
        max_retries: target.max_commit_retries.map(i64::from),
        ..datafusion_ext::DeltaCommitOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::outputs::{MaterializationMode, OutputTarget};
    use std::collections::BTreeMap;

    fn sample_target() -> OutputTarget {
        let mut metadata = BTreeMap::new();
        metadata.insert("user.tag".to_string(), "v1".to_string());
        OutputTarget {
            table_name: "out".to_string(),
            delta_location: Some("/tmp/out".to_string()),
            source_view: "view_out".to_string(),
            columns: vec!["id".to_string()],
            materialization_mode: MaterializationMode::Append,
            partition_by: vec!["id".to_string()],
            write_metadata: metadata,
            max_commit_retries: Some(7),
        }
    }

    fn sample_lineage(spec_hash: [u8; 32], envelope_hash: [u8; 32]) -> LineageContext {
        let mut tags = BTreeMap::new();
        tags.insert("team".to_string(), "graph".to_string());
        LineageContext {
            spec_hash,
            envelope_hash,
            planning_surface_hash: [0x33u8; 32],
            provider_identity_hash: [0x44u8; 32],
            rulepack_fingerprint: [0x55u8; 32],
            rulepack_profile: "Default".to_string(),
            runtime_profile_name: Some("small".to_string()),
            run_started_at_rfc3339: "2026-02-09T00:00:00Z".to_string(),
            runtime_lineage_tags: tags,
        }
    }

    #[test]
    fn test_build_commit_properties_merges_user_and_codeanatomy_keys() {
        let target = sample_target();
        let spec_hash = [0xABu8; 32];
        let envelope_hash = [0xCDu8; 32];
        let lineage = sample_lineage(spec_hash, envelope_hash);

        let props = build_commit_properties(&target, &lineage);
        assert_eq!(props.get("user.tag"), Some(&"v1".to_string()));
        assert_eq!(
            props.get("codeanatomy.spec_hash"),
            Some(&hex::encode(spec_hash))
        );
        assert_eq!(
            props.get("codeanatomy.envelope_hash"),
            Some(&hex::encode(envelope_hash))
        );
        assert_eq!(
            props.get("codeanatomy.planning_surface_hash"),
            Some(&hex::encode([0x33u8; 32]))
        );
        assert_eq!(
            props.get("codeanatomy.provider_identity_hash"),
            Some(&hex::encode([0x44u8; 32]))
        );
        assert_eq!(
            props.get("codeanatomy.lineage_tag.team"),
            Some(&"graph".to_string())
        );
    }

    #[test]
    fn test_build_delta_commit_options_maps_retries_and_metadata() {
        let target = sample_target();
        let spec_hash = [0x11u8; 32];
        let envelope_hash = [0x22u8; 32];
        let lineage = sample_lineage(spec_hash, envelope_hash);

        let options = build_delta_commit_options(&target, &lineage);
        assert_eq!(options.max_retries, Some(7));
        assert_eq!(options.metadata.get("user.tag"), Some(&"v1".to_string()));
        assert_eq!(
            options.metadata.get("codeanatomy.spec_hash"),
            Some(&hex::encode(spec_hash))
        );
        assert_eq!(
            options.metadata.get("codeanatomy.envelope_hash"),
            Some(&hex::encode(envelope_hash))
        );
    }
}
