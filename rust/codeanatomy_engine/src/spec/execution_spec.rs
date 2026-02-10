//! Root SemanticExecutionSpec — the immutable contract between Python and Rust.

use serde::{Deserialize, Serialize};
use crate::compiler::cache_policy::CachePlacementPolicy;
use crate::executor::maintenance::MaintenanceSchedule;
use crate::rules::overlay::RuleOverlayProfile;
use crate::session::runtime_profiles::RuntimeProfileSpec;
use crate::spec::join_graph::JoinGraph;
use crate::spec::outputs::OutputTarget;
use crate::spec::parameters::TypedParameter;
use crate::spec::relations::{InputRelation, ViewDefinition};
use crate::spec::runtime::RuntimeConfig;
use crate::spec::rule_intents::{RuleIntent, RulepackProfile};

/// Current semantic spec schema version.
pub const SPEC_SCHEMA_VERSION: u32 = 4;

/// The root specification for semantic execution planning.
///
/// This is the immutable contract between Python and Rust. Python builds it;
/// Rust consumes it. Nothing else crosses the boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticExecutionSpec {
    pub version: u32,
    pub input_relations: Vec<InputRelation>,
    pub view_definitions: Vec<ViewDefinition>,
    pub join_graph: JoinGraph,
    pub output_targets: Vec<OutputTarget>,
    pub rule_intents: Vec<RuleIntent>,
    pub rulepack_profile: RulepackProfile,
    #[serde(default)]
    pub runtime: RuntimeConfig,

    // --- Wave 3 expansion fields (all default for backward compatibility) ---

    /// Typed parameter bindings for parametric plan compilation.
    #[serde(default)]
    pub typed_parameters: Vec<TypedParameter>,

    /// Per-execution rule overlay (additions, exclusions, priority overrides).
    #[serde(default)]
    pub rule_overlay: Option<RuleOverlayProfile>,

    /// Deterministic runtime profile capturing all session knobs.
    #[serde(default)]
    pub runtime_profile: Option<RuntimeProfileSpec>,

    /// Policy-based cache placement for the compiled plan DAG.
    #[serde(default)]
    pub cache_policy: Option<CachePlacementPolicy>,

    /// Post-execution Delta maintenance schedule.
    #[serde(default)]
    pub maintenance: Option<MaintenanceSchedule>,

    /// Canonical BLAKE3 hash of this spec (computed on creation).
    #[serde(skip)]
    pub spec_hash: [u8; 32],
}

impl SemanticExecutionSpec {
    /// Create a new spec and compute its canonical hash.
    pub fn new(
        version: u32,
        input_relations: Vec<InputRelation>,
        view_definitions: Vec<ViewDefinition>,
        join_graph: JoinGraph,
        output_targets: Vec<OutputTarget>,
        rule_intents: Vec<RuleIntent>,
        rulepack_profile: RulepackProfile,
    ) -> Self {
        let version = version.max(SPEC_SCHEMA_VERSION);
        let mut spec = Self {
            version,
            input_relations,
            view_definitions,
            join_graph,
            output_targets,
            rule_intents,
            rulepack_profile,
            runtime: RuntimeConfig::default(),
            typed_parameters: vec![],
            rule_overlay: None,
            runtime_profile: None,
            cache_policy: None,
            maintenance: None,
            spec_hash: [0u8; 32],
        };

        // Compute and store canonical hash
        spec.spec_hash = crate::spec::hashing::hash_spec(&spec);

        spec
    }

    /// Get the canonical hash of this spec.
    pub fn hash(&self) -> &[u8; 32] {
        &self.spec_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::relations::{ViewDefinition, ViewTransform, SchemaContract};
    use std::collections::BTreeMap;

    #[test]
    fn test_spec_creation_computes_hash() {
        let spec = create_minimal_spec();

        // Hash should be computed (not all zeros)
        assert_ne!(spec.spec_hash, [0u8; 32], "Hash must be computed on creation");
    }

    #[test]
    fn test_spec_hash_deterministic() {
        let spec1 = create_minimal_spec();
        let spec2 = create_minimal_spec();

        assert_eq!(spec1.spec_hash, spec2.spec_hash, "Identical specs must have identical hashes");
    }

    #[test]
    fn test_spec_serialization_roundtrip() {
        let original = create_minimal_spec();
        let original_hash = original.spec_hash;

        // Serialize to JSON
        let json = serde_json::to_string(&original).unwrap();

        // Deserialize back
        let mut deserialized: SemanticExecutionSpec = serde_json::from_str(&json).unwrap();

        // Hash is skipped in serialization, so recompute
        deserialized.spec_hash = crate::spec::hashing::hash_spec(&deserialized);

        assert_eq!(original_hash, deserialized.spec_hash, "Roundtrip must preserve hash");
    }

    fn create_minimal_spec() -> SemanticExecutionSpec {
        let mut schema = BTreeMap::new();
        schema.insert("id".to_string(), "Int64".to_string());

        SemanticExecutionSpec::new(
            1,
            vec![],
            vec![
                ViewDefinition {
                    name: "test_view".to_string(),
                    view_kind: "project".to_string(),
                    view_dependencies: vec![],
                    transform: ViewTransform::Project {
                        source: "input".to_string(),
                        columns: vec!["id".to_string()],
                    },
                    output_schema: SchemaContract { columns: schema },
                }
            ],
            JoinGraph::default(),
            vec![],
            vec![],
            RulepackProfile::Default,
        )
    }
}
