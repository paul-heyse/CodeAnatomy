//! Planning-surface manifest for deterministic session identity.
//!
//! `PlanningSurfaceManifest` captures the planning-time registrations
//! (file formats, table factories, expression planners, function factory,
//! query planner) as a serializable, hashable manifest. The digest
//! is reusable across envelope, plan bundles, and artifact diffs.
//!
//! All name vectors are sorted before hashing to ensure determinism
//! regardless of registration order.

use datafusion_common::config::TableOptions;
use serde::{Deserialize, Serialize};

use crate::session::planning_surface::PlanningSurfaceSpec;

/// Deterministic manifest of the planning surface configuration.
///
/// Captures the identity of all planning-time registrations so that
/// changes to the planning surface (e.g., adding a file format or
/// swapping a query planner) produce a different hash.
///
/// All name vectors are sorted before hashing to ensure order-independence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanningSurfaceManifest {
    /// DataFusion version (e.g., "51.0.0").
    pub datafusion_version: String,
    /// Whether default features are enabled on the session state builder.
    pub default_features_enabled: bool,
    /// Registered file format names (sorted for determinism).
    pub file_format_names: Vec<String>,
    /// Registered table factory names (sorted for determinism).
    pub table_factory_names: Vec<String>,
    /// Registered expression planner names (sorted for determinism).
    pub expr_planner_names: Vec<String>,
    /// Name of the installed function factory, if any.
    pub function_factory_name: Option<String>,
    /// Name of the installed query planner, if any.
    pub query_planner_name: Option<String>,
    /// Whether Delta extension codecs were enabled for the session.
    pub delta_codec_enabled: bool,
    /// BLAKE3 digest of the serialized table options configuration.
    pub table_options_digest: [u8; 32],
}

impl PlanningSurfaceManifest {
    /// Compute a deterministic BLAKE3 hash of this manifest.
    ///
    /// Name vectors are sorted before serialization to ensure that
    /// registration order does not affect the hash. The manifest is
    /// serialized to canonical JSON and then hashed.
    pub fn hash(&self) -> [u8; 32] {
        // Clone and sort all name vectors for order-independent hashing.
        let mut canonical = self.clone();
        canonical.file_format_names.sort();
        canonical.table_factory_names.sort();
        canonical.expr_planner_names.sort();

        let bytes = serde_json::to_vec(&canonical).expect("planning manifest serializable");
        *blake3::hash(&bytes).as_bytes()
    }
}

/// Build a deterministic manifest from a planning surface specification.
pub fn manifest_from_surface(spec: &PlanningSurfaceSpec) -> PlanningSurfaceManifest {
    PlanningSurfaceManifest {
        datafusion_version: datafusion::DATAFUSION_VERSION.to_string(),
        default_features_enabled: spec.enable_default_features,
        file_format_names: spec
            .file_formats
            .iter()
            .map(|f| std::any::type_name_of_val(f.as_ref()).to_string())
            .collect(),
        table_factory_names: spec
            .table_factories
            .iter()
            .map(|(name, factory)| {
                format!("{name}:{}", std::any::type_name_of_val(factory.as_ref()))
            })
            .collect(),
        expr_planner_names: spec
            .expr_planners
            .iter()
            .map(|planner| std::any::type_name_of_val(planner.as_ref()).to_string())
            .collect(),
        function_factory_name: spec
            .function_factory
            .as_ref()
            .map(|factory| std::any::type_name_of_val(factory.as_ref()).to_string()),
        query_planner_name: spec
            .query_planner
            .as_ref()
            .map(|planner| std::any::type_name_of_val(planner.as_ref()).to_string()),
        delta_codec_enabled: spec.delta_codec_enabled,
        table_options_digest: hash_table_options(spec.table_options.as_ref()),
    }
}

fn hash_table_options(options: Option<&TableOptions>) -> [u8; 32] {
    let Some(value) = options else {
        return [0u8; 32];
    };

    let mut entries = value
        .entries()
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect::<Vec<_>>();
    entries.sort();

    let mut hasher = blake3::Hasher::new();
    for (key, val) in entries {
        hasher.update(key.as_bytes());
        hasher.update(b"=");
        hasher.update(val.unwrap_or_default().as_bytes());
        hasher.update(b"\n");
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> PlanningSurfaceManifest {
        PlanningSurfaceManifest {
            datafusion_version: "51.0.0".to_string(),
            default_features_enabled: true,
            file_format_names: vec!["parquet".to_string(), "csv".to_string(), "json".to_string()],
            table_factory_names: vec!["delta".to_string(), "memory".to_string()],
            expr_planner_names: vec!["span_alignment".to_string()],
            function_factory_name: Some("sql_macro".to_string()),
            query_planner_name: None,
            delta_codec_enabled: false,
            table_options_digest: [42u8; 32],
        }
    }

    #[test]
    fn test_hash_determinism() {
        let manifest = sample_manifest();
        let hash1 = manifest.hash();
        let hash2 = manifest.hash();
        assert_eq!(hash1, hash2, "same manifest must produce same hash");
        assert_ne!(hash1, [0u8; 32], "hash must be non-zero");
    }

    #[test]
    fn test_hash_order_independence() {
        let mut manifest_a = sample_manifest();
        manifest_a.file_format_names = vec!["csv".to_string(), "parquet".to_string(), "json".to_string()];

        let mut manifest_b = sample_manifest();
        manifest_b.file_format_names = vec!["json".to_string(), "csv".to_string(), "parquet".to_string()];

        assert_eq!(
            manifest_a.hash(),
            manifest_b.hash(),
            "reordered file_format_names must produce same hash"
        );
    }

    #[test]
    fn test_hash_order_independence_table_factories() {
        let mut manifest_a = sample_manifest();
        manifest_a.table_factory_names = vec!["memory".to_string(), "delta".to_string()];

        let mut manifest_b = sample_manifest();
        manifest_b.table_factory_names = vec!["delta".to_string(), "memory".to_string()];

        assert_eq!(
            manifest_a.hash(),
            manifest_b.hash(),
            "reordered table_factory_names must produce same hash"
        );
    }

    #[test]
    fn test_hash_order_independence_expr_planners() {
        let mut manifest_a = sample_manifest();
        manifest_a.expr_planner_names = vec!["beta".to_string(), "alpha".to_string()];

        let mut manifest_b = sample_manifest();
        manifest_b.expr_planner_names = vec!["alpha".to_string(), "beta".to_string()];

        assert_eq!(
            manifest_a.hash(),
            manifest_b.hash(),
            "reordered expr_planner_names must produce same hash"
        );
    }

    #[test]
    fn test_hash_changes_with_different_version() {
        let manifest_a = sample_manifest();
        let mut manifest_b = sample_manifest();
        manifest_b.datafusion_version = "52.0.0".to_string();

        assert_ne!(
            manifest_a.hash(),
            manifest_b.hash(),
            "different versions must produce different hashes"
        );
    }

    #[test]
    fn test_hash_changes_with_different_formats() {
        let manifest_a = sample_manifest();
        let mut manifest_b = sample_manifest();
        manifest_b.file_format_names.push("avro".to_string());

        assert_ne!(
            manifest_a.hash(),
            manifest_b.hash(),
            "different file formats must produce different hashes"
        );
    }

    #[test]
    fn test_hash_changes_with_function_factory() {
        let manifest_a = sample_manifest();
        let mut manifest_b = sample_manifest();
        manifest_b.function_factory_name = None;

        assert_ne!(
            manifest_a.hash(),
            manifest_b.hash(),
            "function factory presence must affect hash"
        );
    }

    #[test]
    fn test_hash_changes_with_delta_codec_enabled() {
        let manifest_a = sample_manifest();
        let mut manifest_b = sample_manifest();
        manifest_b.delta_codec_enabled = true;

        assert_ne!(
            manifest_a.hash(),
            manifest_b.hash(),
            "delta codec enablement must affect planning surface hash"
        );
    }

    #[test]
    fn test_hash_changes_with_table_options_digest() {
        let manifest_a = sample_manifest();
        let mut manifest_b = sample_manifest();
        manifest_b.table_options_digest = [99u8; 32];

        assert_ne!(
            manifest_a.hash(),
            manifest_b.hash(),
            "different table options digest must produce different hashes"
        );
    }

    #[test]
    fn test_serde_round_trip() {
        let manifest = sample_manifest();
        let json = serde_json::to_string(&manifest).expect("serialize");
        let deserialized: PlanningSurfaceManifest =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(manifest, deserialized);
    }

    #[test]
    fn test_empty_manifest_hashes() {
        let manifest = PlanningSurfaceManifest {
            datafusion_version: String::new(),
            default_features_enabled: false,
            file_format_names: vec![],
            table_factory_names: vec![],
            expr_planner_names: vec![],
            function_factory_name: None,
            query_planner_name: None,
            delta_codec_enabled: false,
            table_options_digest: [0u8; 32],
        };
        let hash = manifest.hash();
        // Even an empty manifest should produce a non-zero hash
        // (the JSON structure itself has content).
        assert_ne!(hash, [0u8; 32]);
    }
}
