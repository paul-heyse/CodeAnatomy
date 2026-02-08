//! Canonical BLAKE3 hashing for deterministic spec identity.
//!
//! Strategy:
//! 1. Convert spec -> serde_json::Value
//! 2. Recursively convert to CanonicalValue with BTreeMap ordering
//! 3. Serialize CanonicalValue -> bytes
//! 4. BLAKE3 hash the bytes

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::spec::execution_spec::SemanticExecutionSpec;

/// Compute canonical BLAKE3 hash of a SemanticExecutionSpec.
pub fn hash_spec(spec: &SemanticExecutionSpec) -> [u8; 32] {
    let canonical_bytes = build_canonical_form(spec);
    let mut hasher = blake3::Hasher::new();
    hasher.update(&canonical_bytes);
    *hasher.finalize().as_bytes()
}

/// Build canonical byte representation of spec.
fn build_canonical_form(spec: &SemanticExecutionSpec) -> Vec<u8> {
    // Convert spec to serde_json::Value
    let json_value = serde_json::to_value(spec)
        .expect("SemanticExecutionSpec must serialize to JSON");

    // Canonicalize to ensure deterministic ordering
    let canonical = canonicalize_value(json_value);

    // Serialize to bytes (compact, no whitespace)
    serde_json::to_vec(&canonical)
        .expect("CanonicalValue must serialize to bytes")
}

/// Canonical value representation with sorted keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum CanonicalValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<CanonicalValue>),
    Object(BTreeMap<String, CanonicalValue>),
}

/// Recursively canonicalize a JSON value to ensure deterministic ordering.
fn canonicalize_value(value: serde_json::Value) -> CanonicalValue {
    match value {
        serde_json::Value::Null => CanonicalValue::Null,
        serde_json::Value::Bool(b) => CanonicalValue::Bool(b),
        serde_json::Value::Number(n) => CanonicalValue::Number(n),
        serde_json::Value::String(s) => CanonicalValue::String(s),
        serde_json::Value::Array(arr) => {
            CanonicalValue::Array(
                arr.into_iter()
                    .map(canonicalize_value)
                    .collect()
            )
        }
        serde_json::Value::Object(obj) => {
            let mut canonical_map = BTreeMap::new();
            for (k, v) in obj {
                canonical_map.insert(k, canonicalize_value(v));
            }
            CanonicalValue::Object(canonical_map)
        }
    }
}

/// Determinism contract for replay validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismContract {
    pub spec_hash: [u8; 32],
    pub envelope_hash: [u8; 32],
}

impl DeterminismContract {
    /// Validate replay by comparing spec and envelope hashes.
    pub fn is_replay_valid(&self, other: &DeterminismContract) -> bool {
        self.spec_hash == other.spec_hash && self.envelope_hash == other.envelope_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::execution_spec::SemanticExecutionSpec;
    use crate::spec::relations::{InputRelation, ViewDefinition, ViewTransform, SchemaContract};
    use crate::spec::join_graph::JoinGraph;
    use crate::spec::runtime::RuntimeConfig;
    use crate::spec::rule_intents::RulepackProfile;
    use std::collections::BTreeMap;

    #[test]
    fn test_hash_spec_deterministic() {
        let spec1 = create_test_spec();
        let spec2 = create_test_spec();

        let hash1 = hash_spec(&spec1);
        let hash2 = hash_spec(&spec2);

        assert_eq!(hash1, hash2, "Identical specs must produce identical hashes");
    }

    #[test]
    fn test_hash_spec_sensitivity() {
        let spec1 = create_test_spec();
        let mut spec2 = create_test_spec();

        // Modify version
        spec2.version = 2;

        let hash1 = hash_spec(&spec1);
        let hash2 = hash_spec(&spec2);

        assert_ne!(hash1, hash2, "Different specs must produce different hashes");
    }

    #[test]
    fn test_canonical_ordering() {
        // Test that object key ordering is deterministic
        let mut map1 = serde_json::Map::new();
        map1.insert("z".to_string(), serde_json::json!("last"));
        map1.insert("a".to_string(), serde_json::json!("first"));

        let mut map2 = serde_json::Map::new();
        map2.insert("a".to_string(), serde_json::json!("first"));
        map2.insert("z".to_string(), serde_json::json!("last"));

        let canonical1 = canonicalize_value(serde_json::Value::Object(map1));
        let canonical2 = canonicalize_value(serde_json::Value::Object(map2));

        let bytes1 = serde_json::to_vec(&canonical1).unwrap();
        let bytes2 = serde_json::to_vec(&canonical2).unwrap();

        assert_eq!(bytes1, bytes2, "Object key ordering must be canonical");
    }

    fn create_test_spec() -> SemanticExecutionSpec {
        let mut schema = BTreeMap::new();
        schema.insert("id".to_string(), "Int64".to_string());

        SemanticExecutionSpec {
            version: 1,
            input_relations: vec![
                InputRelation {
                    logical_name: "test_input".to_string(),
                    delta_location: "/tmp/delta".to_string(),
                    requires_lineage: false,
                    version_pin: None,
                }
            ],
            view_definitions: vec![
                ViewDefinition {
                    name: "test_view".to_string(),
                    view_kind: "normalize".to_string(),
                    view_dependencies: vec![],
                    transform: ViewTransform::Project {
                        source: "test_input".to_string(),
                        columns: vec!["id".to_string()],
                    },
                    output_schema: SchemaContract { columns: schema },
                }
            ],
            join_graph: JoinGraph::default(),
            output_targets: vec![],
            rule_intents: vec![],
            rulepack_profile: RulepackProfile::Default,
            parameter_templates: vec![],
            runtime: RuntimeConfig::default(),
            typed_parameters: vec![],
            rule_overlay: None,
            runtime_profile: None,
            cache_policy: None,
            maintenance: None,
            spec_hash: [0u8; 32],
        }
    }
}
