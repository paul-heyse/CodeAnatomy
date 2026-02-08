//! Table registration for extraction inputs.
//!
//! All inputs are registered as native Delta providers. No Arrow Dataset fallback.
//! Reuses DeltaLake's DeltaTableProvider for registration.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_common::Result as DFResult;
use deltalake::delta_datafusion::{DeltaScanConfig, DeltaScanConfigBuilder, DeltaTableProvider};
use deltalake::{ensure_table_uri, DeltaTableBuilder};
use serde::{Deserialize, Serialize};

use crate::schema::introspection::hash_arrow_schema;
use crate::spec::relations::InputRelation;

/// Registration record for the session envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRegistration {
    pub name: String,
    pub delta_version: i64,
    pub schema_hash: [u8; 32],
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
        // Load Delta table with optional version pin
        let table_url = ensure_table_uri(&input.delta_location).map_err(|e| {
            datafusion_common::DataFusionError::External(Box::new(e))
        })?;

        let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|e| {
            datafusion_common::DataFusionError::External(Box::new(e))
        })?;

        if let Some(version) = input.version_pin {
            builder = builder.with_version(version);
        }

        let table = builder.load().await.map_err(|e| {
            datafusion_common::DataFusionError::External(Box::new(e))
        })?;

        // Get snapshot metadata
        let snapshot = table.snapshot().map_err(|e| {
            datafusion_common::DataFusionError::External(Box::new(e))
        })?;
        let version = snapshot.version();
        let eager_snapshot = snapshot.snapshot().clone();
        let log_store = table.log_store();

        // Build scan config with lineage tracking if needed
        let scan_config = build_scan_config(&eager_snapshot, input.requires_lineage)?;

        // Create native Delta provider
        let provider = DeltaTableProvider::try_new(eager_snapshot, log_store, scan_config)
            .map_err(|e| {
            datafusion_common::DataFusionError::External(Box::new(e))
        })?;

        // Hash the schema for envelope tracking
        let schema_hash = hash_arrow_schema(Arc::as_ref(&provider.schema()));

        // Register as table in the session context
        ctx.register_table(&input.logical_name, Arc::new(provider))?;

        registrations.push(TableRegistration {
            name: input.logical_name.clone(),
            delta_version: version,
            schema_hash,
        });
    }

    Ok(registrations)
}

/// Build a standardized scan config for an extraction input.
///
/// If lineage tracking is required, adds `__source_file` column via
/// DeltaScanConfigBuilder.
///
/// # Arguments
/// * `snapshot` - Delta table eager snapshot (for builder validation)
/// * `requires_lineage` - Whether to track source file lineage
///
/// # Returns
/// Configured DeltaScanConfig instance.
///
/// # Errors
/// Returns error if DeltaScanConfigBuilder fails (e.g., schema incompatibility).
fn build_scan_config(
    snapshot: &deltalake::kernel::EagerSnapshot,
    requires_lineage: bool,
) -> DFResult<DeltaScanConfig> {
    if requires_lineage {
        // Use builder to add file column for lineage tracking
        DeltaScanConfigBuilder::new()
            .with_file_column_name(&"__source_file".to_string())
            .build(snapshot)
            .map_err(|e| {
                datafusion_common::DataFusionError::External(Box::new(e))
            })
    } else {
        // Default config without file column
        Ok(DeltaScanConfig::default())
    }
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
        };

        // Test serde round-trip
        let json = serde_json::to_string(&record).unwrap();
        let deserialized: TableRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(record.name, deserialized.name);
        assert_eq!(record.delta_version, deserialized.delta_version);
        assert_eq!(record.schema_hash, deserialized.schema_hash);
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
