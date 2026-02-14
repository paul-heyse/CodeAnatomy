//! Table registration for extraction inputs.
//!
//! All inputs are registered as native Delta providers. No Arrow Dataset fallback.
//! Reuses `datafusion_ext::delta_control_plane` for registration.

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_common::Result as DFResult;
use datafusion_expr::Expr;
use datafusion_ext::delta_control_plane::{
    delta_provider_from_session_request, DeltaProviderFromSessionRequest, DeltaScanOverrides,
};
use datafusion_ext::DeltaFeatureGate;
use deltalake::delta_datafusion::DeltaScanConfig;
use serde::{Deserialize, Serialize};

use crate::providers::pushdown_contract::{self, PushdownProbe};
use crate::providers::scan_config::{
    infer_capabilities, standard_scan_config, validate_scan_config,
};
use crate::schema::introspection::hash_arrow_schema;
use crate::spec::relations::InputRelation;

/// Delta protocol and schema compatibility facts for a registered source.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeltaCompatibilityFacts {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
    pub column_mapping_mode: Option<String>,
    pub partition_columns: Vec<String>,
}

/// Registration record for the session envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRegistration {
    pub name: String,
    pub delta_version: i64,
    pub schema_hash: [u8; 32],
    pub provider_identity: [u8; 32],
    pub capabilities: crate::providers::scan_config::ProviderCapabilities,
    pub compatibility: DeltaCompatibilityFacts,
}

fn provider_identity_key(
    logical_name: &str,
    delta_location: &str,
    delta_version: i64,
    scan_config: &DeltaScanConfig,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(logical_name.as_bytes());
    hasher.update(b"\0");
    hasher.update(delta_location.as_bytes());
    hasher.update(b"\0");
    hasher.update(delta_version.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(scan_config.enable_parquet_pushdown.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(scan_config.schema_force_view_types.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(scan_config.wrap_partition_values.to_string().as_bytes());
    hasher.update(b"\0");
    if let Some(file_col) = scan_config.file_column_name.as_deref() {
        hasher.update(file_col.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn compatibility_facts(
    snapshot: &datafusion_ext::delta_protocol::DeltaSnapshotInfo,
) -> DeltaCompatibilityFacts {
    DeltaCompatibilityFacts {
        min_reader_version: snapshot.min_reader_version,
        min_writer_version: snapshot.min_writer_version,
        reader_features: snapshot.reader_features.clone(),
        writer_features: snapshot.writer_features.clone(),
        column_mapping_mode: snapshot
            .table_properties
            .get("delta.columnMapping.mode")
            .cloned(),
        partition_columns: snapshot.partition_columns.clone(),
    }
}

/// Register all extraction inputs as native Delta providers.
///
/// Reuses DeltaLake's DeltaTableBuilder for table loading and
/// DeltaTableProvider for registration.
///
/// Inputs are sorted by name for deterministic registration order.
///
/// # Arguments
/// * `ctx` - DataFusion session context
/// * `inputs` - Extraction input specifications
///
/// # Returns
/// Vector of registration records with name, version, and schema hash.
///
/// # Errors
/// Returns error if:
/// - Delta table load fails
/// - Snapshot access fails
/// - Provider creation fails
/// - Table registration fails
pub async fn register_extraction_inputs(
    ctx: &SessionContext,
    inputs: &[InputRelation],
) -> DFResult<Vec<TableRegistration>> {
    let mut registrations = Vec::new();

    // Sort for deterministic registration order
    let mut sorted = inputs.to_vec();
    sorted.sort_by(|a, b| a.logical_name.cmp(&b.logical_name));

    for input in &sorted {
        let requested_scan_config = standard_scan_config(input.requires_lineage);
        validate_scan_config(&requested_scan_config)
            .map_err(datafusion_common::DataFusionError::Plan)?;
        let overrides = DeltaScanOverrides {
            file_column_name: requested_scan_config.file_column_name.clone(),
            enable_parquet_pushdown: Some(requested_scan_config.enable_parquet_pushdown),
            schema_force_view_types: Some(requested_scan_config.schema_force_view_types),
            wrap_partition_values: Some(requested_scan_config.wrap_partition_values),
            schema: requested_scan_config.schema.clone(),
        };
        let (provider, snapshot, resolved_scan_config, _pruned_files, predicate_error) =
            delta_provider_from_session_request(DeltaProviderFromSessionRequest {
                session_ctx: ctx,
                table_uri: &input.delta_location,
                storage_options: None,
                version: input.version_pin,
                timestamp: None,
                predicate: None,
                overrides,
                gate: Some(DeltaFeatureGate::default()),
            })
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        if let Some(error) = predicate_error {
            return Err(datafusion_common::DataFusionError::Plan(format!(
                "Delta predicate parsing failed for '{}': {error}",
                input.logical_name
            )));
        }
        validate_scan_config(&resolved_scan_config)
            .map_err(datafusion_common::DataFusionError::Plan)?;

        // Hash the schema for envelope tracking
        let provider_schema = provider.schema();
        let schema_hash = hash_arrow_schema(provider_schema.as_ref());
        let capabilities = infer_capabilities(&resolved_scan_config);

        // Register as table in the session context
        ctx.register_table(&input.logical_name, Arc::new(provider))?;

        registrations.push(TableRegistration {
            name: input.logical_name.clone(),
            delta_version: snapshot.version,
            schema_hash,
            provider_identity: provider_identity_key(
                &input.logical_name,
                &input.delta_location,
                snapshot.version,
                &resolved_scan_config,
            ),
            capabilities,
            compatibility: compatibility_facts(&snapshot),
        });
    }

    Ok(registrations)
}

/// Probe pushdown capabilities for a registered provider.
///
/// Call after registration to capture the provider's filter pushdown contract.
/// Retrieves the provider from the session catalog and invokes
/// `supports_filters_pushdown` with the given filter expressions.
///
/// # Arguments
/// * `ctx` - DataFusion session context with the table already registered
/// * `table_name` - Name of the registered table to probe
/// * `filters` - Filter expressions to test against the provider
///
/// # Returns
/// A `PushdownProbe` with per-filter statuses.
///
/// # Errors
/// Returns error if:
/// - The table is not registered in the session
/// - The provider's `supports_filters_pushdown` call fails
pub async fn probe_provider_pushdown(
    ctx: &SessionContext,
    table_name: &str,
    filters: &[Expr],
) -> DFResult<PushdownProbe> {
    let provider = ctx.table_provider(table_name).await?;
    pushdown_contract::probe_pushdown(table_name, provider.as_ref(), filters)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registration_record_serialization() {
        let record = TableRegistration {
            name: "test_table".to_string(),
            delta_version: 42,
            schema_hash: [0u8; 32],
            provider_identity: [1u8; 32],
            capabilities: crate::providers::scan_config::ProviderCapabilities {
                predicate_pushdown: true,
                projection_pushdown: true,
                partition_pruning: true,
            },
            compatibility: DeltaCompatibilityFacts {
                min_reader_version: 1,
                min_writer_version: 2,
                reader_features: vec![],
                writer_features: vec![],
                column_mapping_mode: None,
                partition_columns: vec![],
            },
        };

        // Test serde round-trip
        let json = serde_json::to_string(&record).unwrap();
        let deserialized: TableRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(record.name, deserialized.name);
        assert_eq!(record.delta_version, deserialized.delta_version);
        assert_eq!(record.schema_hash, deserialized.schema_hash);
        assert_eq!(record.provider_identity, deserialized.provider_identity);
        assert!(deserialized.capabilities.predicate_pushdown);
        assert_eq!(deserialized.compatibility.min_reader_version, 1);
    }

    #[test]
    fn test_input_sorting() {
        let inputs = vec![
            InputRelation {
                logical_name: "z_table".to_string(),
                delta_location: "/data/z".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
            InputRelation {
                logical_name: "a_table".to_string(),
                delta_location: "/data/a".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
            InputRelation {
                logical_name: "m_table".to_string(),
                delta_location: "/data/m".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
        ];

        let mut sorted = inputs.clone();
        sorted.sort_by(|a, b| a.logical_name.cmp(&b.logical_name));

        assert_eq!(sorted[0].logical_name, "a_table");
        assert_eq!(sorted[1].logical_name, "m_table");
        assert_eq!(sorted[2].logical_name, "z_table");
    }
}
