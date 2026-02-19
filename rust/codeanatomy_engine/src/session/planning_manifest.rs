//! Planning-surface manifest for deterministic session identity.
//!
//! `PlanningSurfaceManifest` captures the planning-time registrations
//! (file formats, table factories, expression planners, function factory,
//! query planner) as a serializable, hashable manifest. The digest
//! is reusable across envelope, plan bundles, and artifact diffs.
//!
//! All name vectors are sorted before hashing to ensure determinism
//! regardless of registration order.

use std::collections::BTreeMap;

use datafusion::prelude::SessionContext;
use datafusion_common::config::TableOptions;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

use crate::session::capture::{
    capture_df_settings, GovernancePolicy, PLANNING_AFFECTING_CONFIG_KEYS,
};
use crate::session::planning_surface::{
    PlanningSurfacePolicyV1, PlanningSurfaceSpec, TableFactoryEntry,
};

/// Deterministic manifest of the planning surface configuration.
///
/// Captures the identity of all planning-time registrations so that
/// changes to the planning surface (e.g., adding a file format or
/// swapping a query planner) produce a different hash.
///
/// All name vectors are sorted before hashing to ensure order-independence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanningSurfaceManifest {
    /// DataFusion version (e.g., "52.1.0").
    pub datafusion_version: String,
    /// Whether default features are enabled on the session state builder.
    pub default_features_enabled: bool,
    /// Registered file format names (sorted for determinism).
    pub file_format_names: Vec<String>,
    /// Registered table factory names (sorted for determinism).
    pub table_factory_names: Vec<String>,
    /// Registered expression planner names (sorted for determinism).
    pub expr_planner_names: Vec<String>,
    /// Registered relation planner names (sorted for determinism).
    pub relation_planner_names: Vec<String>,
    /// Name of the installed type planner, if any.
    pub type_planner_name: Option<String>,
    /// Name of the installed function factory, if any.
    pub function_factory_name: Option<String>,
    /// Name of the installed query planner, if any.
    pub query_planner_name: Option<String>,
    /// Function registry identity snapshot.
    pub function_registry_identity: FunctionRegistryIdentity,
    /// Planning-affecting config key snapshot.
    pub planning_config_keys: BTreeMap<String, String>,
    /// Default catalog identity.
    pub default_catalog: Option<String>,
    /// Default schema identity.
    pub default_schema: Option<String>,
    /// Explicit table-factory allowlist entries (type + identity).
    pub table_factory_allowlist: Vec<TableFactoryManifestEntry>,
    /// Catalog provider identity snapshots.
    pub catalog_provider_identities: Vec<CatalogProviderIdentity>,
    /// Schema provider identity snapshots.
    pub schema_provider_identities: Vec<SchemaProviderIdentity>,
    /// Extension governance policy.
    pub extension_policy: String,
    /// Whether Delta extension codecs were enabled for the session.
    pub delta_codec_enabled: bool,
    /// BLAKE3 digest of the serialized table options configuration.
    pub table_options_digest: [u8; 32],
    /// Typed planning policy payload used for cross-layer parity checks.
    pub typed_policy: Option<PlanningSurfacePolicyV1>,
}

/// Canonical planning-surface manifest (v2) shared with Python surfaces.
///
/// This projection keeps only the cross-layer determinism boundary fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PlanningSurfaceManifestV2 {
    pub expr_planners: Vec<String>,
    pub relation_planners: Vec<String>,
    pub type_planners: Vec<String>,
    pub table_factories: Vec<String>,
    pub planning_config_keys: BTreeMap<String, String>,
}

impl PlanningSurfaceManifestV2 {
    /// Compute deterministic hash of the v2 manifest projection.
    pub fn hash(&self) -> [u8; 32] {
        let mut canonical = self.clone();
        canonical.expr_planners.sort();
        canonical.relation_planners.sort();
        canonical.type_planners.sort();
        canonical.table_factories.sort();
        let bytes = serde_json::to_vec(&canonical).expect("planning manifest v2 serializable");
        *blake3::hash(&bytes).as_bytes()
    }
}

impl From<&PlanningSurfaceManifest> for PlanningSurfaceManifestV2 {
    fn from(value: &PlanningSurfaceManifest) -> Self {
        let expr_planners = if let Some(policy) = &value.typed_policy {
            if !policy.expr_planner_names.is_empty() {
                policy.expr_planner_names.clone()
            } else {
                value.expr_planner_names.clone()
            }
        } else {
            value.expr_planner_names.clone()
        };
        let relation_planners = if let Some(policy) = &value.typed_policy {
            if policy.relation_planner_enabled {
                vec!["codeanatomy_relation".to_string()]
            } else {
                value.relation_planner_names.clone()
            }
        } else {
            value.relation_planner_names.clone()
        };
        let type_planners = if let Some(policy) = &value.typed_policy {
            if policy.type_planner_enabled {
                vec!["codeanatomy_type".to_string()]
            } else {
                value.type_planner_name.iter().cloned().collect::<Vec<_>>()
            }
        } else {
            value.type_planner_name.iter().cloned().collect::<Vec<_>>()
        };
        let table_factories = value
            .table_factory_names
            .iter()
            .map(|entry| {
                entry
                    .split_once(':')
                    .map(|(name, _)| name.to_string())
                    .unwrap_or_else(|| entry.clone())
            })
            .collect::<Vec<_>>();
        Self {
            expr_planners,
            relation_planners,
            type_planners,
            table_factories,
            planning_config_keys: value.planning_config_keys.clone(),
        }
    }
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
        canonical.relation_planner_names.sort();
        canonical.table_factory_allowlist.sort_by(|a, b| {
            a.factory_type
                .cmp(&b.factory_type)
                .then(a.identity_hash.cmp(&b.identity_hash))
        });
        canonical.catalog_provider_identities.sort_by(|a, b| {
            a.catalog_name
                .cmp(&b.catalog_name)
                .then(a.provider_type.cmp(&b.provider_type))
                .then(a.identity_hash.cmp(&b.identity_hash))
        });
        canonical.schema_provider_identities.sort_by(|a, b| {
            a.catalog_name
                .cmp(&b.catalog_name)
                .then(a.schema_name.cmp(&b.schema_name))
                .then(a.provider_type.cmp(&b.provider_type))
                .then(a.identity_hash.cmp(&b.identity_hash))
        });

        let bytes = serde_json::to_vec(&canonical).expect("planning manifest serializable");
        *blake3::hash(&bytes).as_bytes()
    }

    /// Return the cross-layer v2 projection used by Python parity contracts.
    pub fn to_v2(&self) -> PlanningSurfaceManifestV2 {
        PlanningSurfaceManifestV2::from(self)
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
        relation_planner_names: spec
            .relation_planners
            .iter()
            .map(|planner| std::any::type_name_of_val(planner.as_ref()).to_string())
            .collect(),
        type_planner_name: spec
            .type_planner
            .as_ref()
            .map(|planner| std::any::type_name_of_val(planner.as_ref()).to_string()),
        function_factory_name: spec
            .function_factory
            .as_ref()
            .map(|factory| std::any::type_name_of_val(factory.as_ref()).to_string()),
        query_planner_name: spec
            .query_planner
            .as_ref()
            .map(|planner| std::any::type_name_of_val(planner.as_ref()).to_string()),
        function_registry_identity: FunctionRegistryIdentity::default(),
        planning_config_keys: spec.planning_config_keys.clone(),
        default_catalog: spec
            .planning_config_keys
            .get("datafusion.catalog.default_catalog")
            .cloned(),
        default_schema: spec
            .planning_config_keys
            .get("datafusion.catalog.default_schema")
            .cloned(),
        table_factory_allowlist: spec
            .table_factory_allowlist
            .iter()
            .map(TableFactoryManifestEntry::from)
            .collect(),
        catalog_provider_identities: Vec::new(),
        schema_provider_identities: Vec::new(),
        extension_policy: extension_policy_name(&spec.extension_policy).to_string(),
        delta_codec_enabled: spec.delta_codec_enabled,
        table_options_digest: hash_table_options(spec.table_options.as_ref()),
        typed_policy: spec.typed_policy.clone(),
    }
}

/// Build a canonical v2 planning manifest projection from a planning surface.
pub fn manifest_v2_from_surface(spec: &PlanningSurfaceSpec) -> PlanningSurfaceManifestV2 {
    manifest_from_surface(spec).to_v2()
}

/// Build a deterministic manifest from planning surface plus live session state.
pub async fn manifest_from_surface_with_context(
    spec: &PlanningSurfaceSpec,
    ctx: &SessionContext,
) -> Result<PlanningSurfaceManifest> {
    let mut manifest = manifest_from_surface(spec);
    manifest.function_registry_identity = capture_function_registry(ctx);
    manifest.planning_config_keys = capture_planning_config_keys(ctx).await?;
    manifest.default_catalog = manifest
        .planning_config_keys
        .get("datafusion.catalog.default_catalog")
        .cloned();
    manifest.default_schema = manifest
        .planning_config_keys
        .get("datafusion.catalog.default_schema")
        .cloned();
    manifest.catalog_provider_identities = capture_catalog_provider_identities(ctx);
    manifest.schema_provider_identities = capture_schema_provider_identities(ctx);
    Ok(manifest)
}

/// Build a canonical v2 planning manifest projection with live session state.
pub async fn manifest_v2_from_surface_with_context(
    spec: &PlanningSurfaceSpec,
    ctx: &SessionContext,
) -> Result<PlanningSurfaceManifestV2> {
    let manifest = manifest_from_surface_with_context(spec, ctx).await?;
    Ok(manifest.to_v2())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct FunctionRegistryIdentity {
    pub scalar_functions: Vec<(String, String)>,
    pub aggregate_functions: Vec<(String, String)>,
    pub window_functions: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableFactoryManifestEntry {
    pub factory_type: String,
    pub identity_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogProviderIdentity {
    pub catalog_name: String,
    pub provider_type: String,
    pub schema_names: Vec<String>,
    pub identity_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaProviderIdentity {
    pub catalog_name: String,
    pub schema_name: String,
    pub provider_type: String,
    pub table_names: Vec<String>,
    pub identity_hash: [u8; 32],
}

impl From<&TableFactoryEntry> for TableFactoryManifestEntry {
    fn from(value: &TableFactoryEntry) -> Self {
        Self {
            factory_type: value.factory_type.clone(),
            identity_hash: value.identity_hash,
        }
    }
}

fn extension_policy_name(policy: &GovernancePolicy) -> &'static str {
    policy.as_str()
}

fn capture_function_registry(ctx: &SessionContext) -> FunctionRegistryIdentity {
    let state = ctx.state();
    let mut scalar_functions = state
        .scalar_functions()
        .keys()
        .map(|name| (name.clone(), "scalar".to_string()))
        .collect::<Vec<_>>();
    let mut aggregate_functions = state
        .aggregate_functions()
        .keys()
        .map(|name| (name.clone(), "aggregate".to_string()))
        .collect::<Vec<_>>();
    let mut window_functions = state
        .window_functions()
        .keys()
        .map(|name| (name.clone(), "window".to_string()))
        .collect::<Vec<_>>();
    scalar_functions.sort();
    aggregate_functions.sort();
    window_functions.sort();
    FunctionRegistryIdentity {
        scalar_functions,
        aggregate_functions,
        window_functions,
    }
}

fn capture_catalog_provider_identities(ctx: &SessionContext) -> Vec<CatalogProviderIdentity> {
    let state = ctx.state();
    let list = state.catalog_list();
    let mut catalog_names = list.catalog_names();
    catalog_names.sort();

    let mut entries = Vec::new();
    for catalog_name in catalog_names {
        let Some(catalog) = list.catalog(&catalog_name) else {
            continue;
        };
        let mut schema_names = catalog.schema_names();
        schema_names.sort();
        let provider_type = std::any::type_name_of_val(catalog.as_ref()).to_string();
        let identity_hash = hash_identity_parts(
            std::iter::once(catalog_name.as_str())
                .chain(std::iter::once(provider_type.as_str()))
                .chain(schema_names.iter().map(String::as_str)),
        );
        entries.push(CatalogProviderIdentity {
            catalog_name,
            provider_type,
            schema_names,
            identity_hash,
        });
    }
    entries.sort_by(|a, b| {
        a.catalog_name
            .cmp(&b.catalog_name)
            .then(a.provider_type.cmp(&b.provider_type))
            .then(a.identity_hash.cmp(&b.identity_hash))
    });
    entries
}

fn capture_schema_provider_identities(ctx: &SessionContext) -> Vec<SchemaProviderIdentity> {
    let state = ctx.state();
    let list = state.catalog_list();
    let mut catalog_names = list.catalog_names();
    catalog_names.sort();

    let mut entries = Vec::new();
    for catalog_name in catalog_names {
        let Some(catalog) = list.catalog(&catalog_name) else {
            continue;
        };
        let mut schema_names = catalog.schema_names();
        schema_names.sort();
        for schema_name in schema_names {
            let Some(schema) = catalog.schema(&schema_name) else {
                continue;
            };
            let mut table_names = schema.table_names();
            table_names.sort();
            let provider_type = std::any::type_name_of_val(schema.as_ref()).to_string();
            let identity_hash = hash_identity_parts(
                std::iter::once(catalog_name.as_str())
                    .chain(std::iter::once(schema_name.as_str()))
                    .chain(std::iter::once(provider_type.as_str()))
                    .chain(table_names.iter().map(String::as_str)),
            );
            entries.push(SchemaProviderIdentity {
                catalog_name: catalog_name.clone(),
                schema_name,
                provider_type,
                table_names,
                identity_hash,
            });
        }
    }
    entries.sort_by(|a, b| {
        a.catalog_name
            .cmp(&b.catalog_name)
            .then(a.schema_name.cmp(&b.schema_name))
            .then(a.provider_type.cmp(&b.provider_type))
            .then(a.identity_hash.cmp(&b.identity_hash))
    });
    entries
}

async fn capture_planning_config_keys(ctx: &SessionContext) -> Result<BTreeMap<String, String>> {
    let settings = capture_df_settings(ctx).await?;
    Ok(settings
        .into_iter()
        .filter(|(key, _)| PLANNING_AFFECTING_CONFIG_KEYS.contains(&key.as_str()))
        .collect())
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

fn hash_identity_parts<'a>(parts: impl IntoIterator<Item = &'a str>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> PlanningSurfaceManifest {
        PlanningSurfaceManifest {
            datafusion_version: "52.1.0".to_string(),
            default_features_enabled: true,
            file_format_names: vec!["parquet".to_string(), "csv".to_string(), "json".to_string()],
            table_factory_names: vec!["delta".to_string(), "memory".to_string()],
            expr_planner_names: vec!["span_alignment".to_string()],
            relation_planner_names: vec!["codeanatomy_relation".to_string()],
            type_planner_name: Some("codeanatomy_type".to_string()),
            function_factory_name: Some("sql_macro".to_string()),
            query_planner_name: None,
            function_registry_identity: FunctionRegistryIdentity::default(),
            planning_config_keys: BTreeMap::new(),
            default_catalog: Some("codeanatomy".to_string()),
            default_schema: Some("public".to_string()),
            table_factory_allowlist: vec![],
            catalog_provider_identities: vec![],
            schema_provider_identities: vec![],
            extension_policy: "permissive".to_string(),
            delta_codec_enabled: false,
            table_options_digest: [42u8; 32],
            typed_policy: Some(PlanningSurfacePolicyV1 {
                enable_default_features: true,
                expr_planner_names: vec!["codeanatomy_domain".to_string()],
                relation_planner_enabled: true,
                type_planner_enabled: true,
            }),
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
        manifest_a.file_format_names =
            vec!["csv".to_string(), "parquet".to_string(), "json".to_string()];

        let mut manifest_b = sample_manifest();
        manifest_b.file_format_names =
            vec!["json".to_string(), "csv".to_string(), "parquet".to_string()];

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
        manifest_b.datafusion_version = "52.2.0".to_string();

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
            relation_planner_names: vec![],
            type_planner_name: None,
            function_factory_name: None,
            query_planner_name: None,
            function_registry_identity: FunctionRegistryIdentity::default(),
            planning_config_keys: BTreeMap::new(),
            default_catalog: None,
            default_schema: None,
            table_factory_allowlist: vec![],
            catalog_provider_identities: vec![],
            schema_provider_identities: vec![],
            extension_policy: "permissive".to_string(),
            delta_codec_enabled: false,
            table_options_digest: [0u8; 32],
            typed_policy: None,
        };
        let hash = manifest.hash();
        // Even an empty manifest should produce a non-zero hash
        // (the JSON structure itself has content).
        assert_ne!(hash, [0u8; 32]);
    }
}
