//! Session envelope for deterministic session state capture.
//!
//! Captures the complete runtime configuration snapshot of a DataFusion session.

use std::collections::BTreeMap;

use arrow::array::Array;
use datafusion::execution::context::SessionContext;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

use crate::session::capture::capture_df_settings;

/// Complete deterministic session state snapshot.
///
/// Captures all configuration, registered functions, and version metadata
/// for reproducible execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEnvelope {
    /// DataFusion version (e.g., "51.0.0")
    pub datafusion_version: String,
    /// DeltaLake version (e.g., "0.30.1")
    pub delta_rs_version: String,
    /// CodeAnatomy engine version
    pub codeanatomy_version: String,
    /// Complete DataFusion configuration snapshot
    pub config_snapshot: BTreeMap<String, String>,
    /// Number of target partitions
    pub target_partitions: u32,
    /// Batch size for record batches
    pub batch_size: u32,
    /// Memory pool size in bytes
    pub memory_pool_bytes: u64,
    /// Whether spilling is enabled
    pub spill_enabled: bool,
    /// Registered table names (sorted)
    pub table_registrations: Vec<String>,
    /// Registered function names (sorted)
    pub registered_functions: Vec<String>,
    /// BLAKE3 hash of the execution spec
    pub spec_hash: [u8; 32],
    /// BLAKE3 fingerprint of the rulepack
    pub rulepack_fingerprint: [u8; 32],
    /// BLAKE3 hash of the planning surface manifest
    pub planning_surface_hash: [u8; 32],
    /// BLAKE3 hash of registered provider identities
    pub provider_identity_hash: [u8; 32],
    /// BLAKE3 hash of this envelope (computed after all fields set)
    pub envelope_hash: [u8; 32],
}

impl SessionEnvelope {
    /// Captures the complete session state from a SessionContext.
    ///
    /// # Arguments
    ///
    /// * `ctx` - SessionContext to capture state from
    /// * `spec_hash` - Hash of the execution spec
    /// * `rulepack_fingerprint` - Fingerprint of the rulepack
    /// * `memory_pool_bytes` - Memory pool size
    /// * `spill_enabled` - Whether spilling is enabled
    /// * `planning_surface_hash` - BLAKE3 hash of the planning surface manifest
    /// * `provider_identity_hash` - BLAKE3 hash of provider identities
    ///
    /// # Returns
    ///
    /// Complete session envelope with all metadata
    pub async fn capture(
        ctx: &SessionContext,
        spec_hash: [u8; 32],
        rulepack_fingerprint: [u8; 32],
        memory_pool_bytes: u64,
        spill_enabled: bool,
        planning_surface_hash: [u8; 32],
        provider_identity_hash: [u8; 32],
    ) -> Result<Self> {
        // Capture version information
        let datafusion_version = datafusion::DATAFUSION_VERSION.to_string();
        let delta_rs_version = deltalake::crate_version().to_string();
        let codeanatomy_version = env!("CARGO_PKG_VERSION").to_string();

        let config_snapshot = capture_df_settings(ctx).await?;

        // Extract session config values
        let state = ctx.state();
        let session_config = state.config();
        let target_partitions = session_config.target_partitions() as u32;
        let batch_size = session_config.batch_size() as u32;

        // Collect registered functions via programmatic API (avoids SHOW FUNCTIONS
        // which internally uses array_agg and may not be available with custom rules)
        let mut registered_functions = Vec::new();
        {
            let state = ctx.state();
            for name in state.scalar_functions().keys() {
                registered_functions.push(name.clone());
            }
            for name in state.aggregate_functions().keys() {
                registered_functions.push(name.clone());
            }
            for name in state.window_functions().keys() {
                registered_functions.push(name.clone());
            }
        }
        registered_functions.sort();

        // Query registered tables from information_schema.tables
        let tables_df = ctx
            .sql("SELECT table_name FROM information_schema.tables ORDER BY table_name")
            .await?;
        let table_batches = tables_df.collect().await?;

        let mut table_registrations = Vec::new();
        for batch in table_batches {
            let table_names = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Expected StringArray for table names".into(),
                    )
                })?;

            for i in 0..batch.num_rows() {
                if !table_names.is_null(i) {
                    table_registrations.push(table_names.value(i).to_string());
                }
            }
        }

        // Compute envelope hash (hash of all fields except envelope_hash itself)
        let envelope_hash = Self::compute_envelope_hash(
            &datafusion_version,
            &delta_rs_version,
            &codeanatomy_version,
            &config_snapshot,
            target_partitions,
            batch_size,
            memory_pool_bytes,
            spill_enabled,
            &table_registrations,
            &registered_functions,
            &spec_hash,
            &rulepack_fingerprint,
            &planning_surface_hash,
            &provider_identity_hash,
        );

        Ok(Self {
            datafusion_version,
            delta_rs_version,
            codeanatomy_version,
            config_snapshot,
            target_partitions,
            batch_size,
            memory_pool_bytes,
            spill_enabled,
            table_registrations,
            registered_functions,
            spec_hash,
            rulepack_fingerprint,
            planning_surface_hash,
            provider_identity_hash,
            envelope_hash,
        })
    }

    /// Compute a deterministic provider-identity aggregate hash.
    ///
    /// Accepts `(table_name, identity_hash)` tuples and hashes them in sorted
    /// order so registration order does not affect identity.
    pub fn hash_provider_identities(provider_identities: &[(String, [u8; 32])]) -> [u8; 32] {
        let mut sorted: Vec<(&str, [u8; 32])> = provider_identities
            .iter()
            .map(|(table_name, identity_hash)| (table_name.as_str(), *identity_hash))
            .collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0).then(a.1.cmp(&b.1)));

        let mut hasher = blake3::Hasher::new();
        for (table_name, identity_hash) in sorted {
            hasher.update(table_name.as_bytes());
            hasher.update(b"\0");
            hasher.update(&identity_hash);
        }
        *hasher.finalize().as_bytes()
    }

    /// Computes BLAKE3 hash of the envelope contents.
    ///
    /// Hashes all fields in deterministic order to produce a stable fingerprint.
    #[allow(clippy::too_many_arguments)]
    fn compute_envelope_hash(
        datafusion_version: &str,
        delta_rs_version: &str,
        codeanatomy_version: &str,
        config_snapshot: &BTreeMap<String, String>,
        target_partitions: u32,
        batch_size: u32,
        memory_pool_bytes: u64,
        spill_enabled: bool,
        table_registrations: &[String],
        registered_functions: &[String],
        spec_hash: &[u8; 32],
        rulepack_fingerprint: &[u8; 32],
        planning_surface_hash: &[u8; 32],
        provider_identity_hash: &[u8; 32],
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();

        // Hash version strings
        hasher.update(datafusion_version.as_bytes());
        hasher.update(delta_rs_version.as_bytes());
        hasher.update(codeanatomy_version.as_bytes());

        // Hash config snapshot (already sorted via BTreeMap)
        for (key, value) in config_snapshot {
            hasher.update(key.as_bytes());
            hasher.update(value.as_bytes());
        }

        // Hash numeric config
        hasher.update(&target_partitions.to_le_bytes());
        hasher.update(&batch_size.to_le_bytes());
        hasher.update(&memory_pool_bytes.to_le_bytes());
        hasher.update(&[spill_enabled as u8]);

        // Hash registrations (already sorted)
        for table in table_registrations {
            hasher.update(table.as_bytes());
        }
        for function in registered_functions {
            hasher.update(function.as_bytes());
        }

        // Hash input hashes
        hasher.update(spec_hash);
        hasher.update(rulepack_fingerprint);

        // Hash planning surface identity
        hasher.update(planning_surface_hash);
        hasher.update(provider_identity_hash);

        *hasher.finalize().as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_envelope_hash_determinism() {
        let config = BTreeMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);

        let hash1 = SessionEnvelope::compute_envelope_hash(
            "51.0.0",
            "0.30.1",
            "0.1.0",
            &config,
            8,
            8192,
            2 * 1024 * 1024 * 1024,
            true,
            &["table1".to_string(), "table2".to_string()],
            &["fn1".to_string(), "fn2".to_string()],
            &[0u8; 32],
            &[1u8; 32],
            &[2u8; 32],
            &[3u8; 32],
        );

        let hash2 = SessionEnvelope::compute_envelope_hash(
            "51.0.0",
            "0.30.1",
            "0.1.0",
            &config,
            8,
            8192,
            2 * 1024 * 1024 * 1024,
            true,
            &["table1".to_string(), "table2".to_string()],
            &["fn1".to_string(), "fn2".to_string()],
            &[0u8; 32],
            &[1u8; 32],
            &[2u8; 32],
            &[3u8; 32],
        );

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_envelope_hash_changes_with_config() {
        let config1 = BTreeMap::from([("key1".to_string(), "value1".to_string())]);
        let config2 = BTreeMap::from([("key1".to_string(), "value2".to_string())]);

        let hash1 = SessionEnvelope::compute_envelope_hash(
            "51.0.0",
            "0.30.1",
            "0.1.0",
            &config1,
            8,
            8192,
            2 * 1024 * 1024 * 1024,
            true,
            &[],
            &[],
            &[0u8; 32],
            &[1u8; 32],
            &[0u8; 32],
            &[3u8; 32],
        );

        let hash2 = SessionEnvelope::compute_envelope_hash(
            "51.0.0",
            "0.30.1",
            "0.1.0",
            &config2,
            8,
            8192,
            2 * 1024 * 1024 * 1024,
            true,
            &[],
            &[],
            &[0u8; 32],
            &[1u8; 32],
            &[0u8; 32],
            &[3u8; 32],
        );

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_envelope_hash_changes_with_planning_surface() {
        let config = BTreeMap::from([("key1".to_string(), "value1".to_string())]);

        let hash1 = SessionEnvelope::compute_envelope_hash(
            "51.0.0",
            "0.30.1",
            "0.1.0",
            &config,
            8,
            8192,
            2 * 1024 * 1024 * 1024,
            true,
            &[],
            &[],
            &[0u8; 32],
            &[1u8; 32],
            &[0u8; 32],
            &[3u8; 32],
        );

        let hash2 = SessionEnvelope::compute_envelope_hash(
            "51.0.0",
            "0.30.1",
            "0.1.0",
            &config,
            8,
            8192,
            2 * 1024 * 1024 * 1024,
            true,
            &[],
            &[],
            &[0u8; 32],
            &[1u8; 32],
            &[99u8; 32],
            &[3u8; 32],
        );

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_provider_identity_hash_deterministic() {
        let ids = vec![("b".to_string(), [2u8; 32]), ("a".to_string(), [1u8; 32])];
        let ids_reordered = vec![("a".to_string(), [1u8; 32]), ("b".to_string(), [2u8; 32])];
        assert_eq!(
            SessionEnvelope::hash_provider_identities(&ids),
            SessionEnvelope::hash_provider_identities(&ids_reordered)
        );
    }

    #[test]
    fn test_envelope_hash_changes_with_provider_identity() {
        let config = BTreeMap::from([("key1".to_string(), "value1".to_string())]);
        let hash1 = SessionEnvelope::compute_envelope_hash(
            "51.0.0",
            "0.30.1",
            "0.1.0",
            &config,
            8,
            8192,
            2 * 1024 * 1024 * 1024,
            true,
            &[],
            &[],
            &[0u8; 32],
            &[1u8; 32],
            &[2u8; 32],
            &[3u8; 32],
        );
        let hash2 = SessionEnvelope::compute_envelope_hash(
            "51.0.0",
            "0.30.1",
            "0.1.0",
            &config,
            8,
            8192,
            2 * 1024 * 1024 * 1024,
            true,
            &[],
            &[],
            &[0u8; 32],
            &[1u8; 32],
            &[2u8; 32],
            &[4u8; 32],
        );
        assert_ne!(hash1, hash2);
    }
}
