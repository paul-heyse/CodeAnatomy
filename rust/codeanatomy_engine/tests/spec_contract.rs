//! WS1: SemanticExecutionSpec Contract Tests
//!
//! This module validates the SemanticExecutionSpec structs, serialization,
//! and canonical hashing behavior. Tests verify:
//!
//! 1. Struct construction and field access
//! 2. JSON serialization/deserialization roundtrip
//! 3. Canonical hash determinism
//! 4. Hash sensitivity to changes
//! 5. Tagged enum serialization (ViewTransform)
//! 6. DeterminismContract replay validation

mod common;

use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::hashing::{hash_spec, DeterminismContract};
use codeanatomy_engine::spec::join_graph::{JoinConstraint, JoinEdge, JoinGraph};
use codeanatomy_engine::spec::outputs::MaterializationMode;
use codeanatomy_engine::spec::relations::{
    AggregationExpr, InputRelation, JoinKeyPair, JoinType, SchemaContract, ViewDefinition,
    ViewTransform,
};
use codeanatomy_engine::spec::runtime::RuntimeTunerMode;
use codeanatomy_engine::spec::rule_intents::{
    RuleClass, RuleIntent, RulepackProfile,
};
use std::collections::BTreeMap;

/// Test 1: Basic spec construction and field access
#[test]
fn test_spec_construction() {
    let spec = create_test_spec();

    assert_eq!(spec.version, 4);
    assert_eq!(spec.input_relations.len(), 1);
    assert_eq!(spec.view_definitions.len(), 1);
    assert_eq!(spec.rulepack_profile, RulepackProfile::Default);
    assert!(!spec.runtime.compliance_capture);
    assert_eq!(spec.runtime.tuner_mode, RuntimeTunerMode::Off);
    assert_ne!(spec.spec_hash, [0u8; 32], "Hash must be computed");
}

/// Test 2: JSON serialization roundtrip
#[test]
fn test_spec_serialization_roundtrip() {
    let original = create_test_spec();
    let original_hash = original.spec_hash;

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&original).unwrap();
    assert!(!json.is_empty());

    // Deserialize back
    let mut deserialized: SemanticExecutionSpec = serde_json::from_str(&json).unwrap();

    // Recompute hash (skipped during serialization)
    deserialized.spec_hash = hash_spec(&deserialized);

    // Verify hash matches
    assert_eq!(
        original_hash, deserialized.spec_hash,
        "Roundtrip must preserve hash"
    );
}

/// Test 3: Hash determinism
#[test]
fn test_hash_determinism() {
    let spec1 = create_test_spec();
    let spec2 = create_test_spec();

    assert_eq!(
        spec1.spec_hash, spec2.spec_hash,
        "Identical specs must produce identical hashes"
    );
}

/// Test 4: Hash sensitivity to changes
#[test]
fn test_hash_sensitivity() {
    let mut spec1 = create_test_spec();
    let mut spec2 = create_test_spec();

    // Modify version
    spec2.version = 2;
    spec2.spec_hash = hash_spec(&spec2);

    assert_ne!(
        spec1.spec_hash, spec2.spec_hash,
        "Different specs must produce different hashes"
    );

    // Modify input relation
    spec1.input_relations[0].delta_location = "/different/path".to_string();
    spec1.spec_hash = hash_spec(&spec1);

    assert_ne!(
        spec1.spec_hash, spec2.spec_hash,
        "Different input relations must produce different hashes"
    );
}

/// Test 5: ViewTransform tagged enum serialization
#[test]
fn test_view_transform_serialization() {
    // Test Normalize variant
    let normalize = ViewTransform::Normalize {
        source: "input".to_string(),
        id_columns: vec!["id".to_string()],
        span_columns: Some(("bstart".to_string(), "bend".to_string())),
        text_columns: vec!["text".to_string()],
    };

    let json = serde_json::to_string(&normalize).unwrap();
    assert!(json.contains(r#""kind":"Normalize"#));
    assert!(json.contains(r#""source":"input"#));

    let deserialized: ViewTransform = serde_json::from_str(&json).unwrap();
    match deserialized {
        ViewTransform::Normalize { source, .. } => assert_eq!(source, "input"),
        _ => panic!("Expected Normalize variant"),
    }

    // Test Relate variant
    let relate = ViewTransform::Relate {
        left: "left_view".to_string(),
        right: "right_view".to_string(),
        join_type: JoinType::Inner,
        join_keys: vec![JoinKeyPair {
            left_key: "id".to_string(),
            right_key: "id".to_string(),
        }],
    };

    let json = serde_json::to_string(&relate).unwrap();
    assert!(json.contains(r#""kind":"Relate"#));

    // Test Union variant
    let union = ViewTransform::Union {
        sources: vec!["view1".to_string(), "view2".to_string()],
        discriminator_column: Some("source".to_string()),
        distinct: true,
    };

    let json = serde_json::to_string(&union).unwrap();
    assert!(json.contains(r#""kind":"Union"#));

    // Test Project variant
    let project = ViewTransform::Project {
        source: "base".to_string(),
        columns: vec!["col1".to_string(), "col2".to_string()],
    };

    let json = serde_json::to_string(&project).unwrap();
    assert!(json.contains(r#""kind":"Project"#));

    // Test Filter variant
    let filter = ViewTransform::Filter {
        source: "base".to_string(),
        predicate: "value > 10".to_string(),
    };

    let json = serde_json::to_string(&filter).unwrap();
    assert!(json.contains(r#""kind":"Filter"#));

    // Test Aggregate variant
    let aggregate = ViewTransform::Aggregate {
        source: "base".to_string(),
        group_by: vec!["category".to_string()],
        aggregations: vec![AggregationExpr {
            column: "value".to_string(),
            function: "sum".to_string(),
            alias: "total".to_string(),
        }],
    };

    let json = serde_json::to_string(&aggregate).unwrap();
    assert!(json.contains(r#""kind":"Aggregate"#));
}

/// Test 6: DeterminismContract replay validation
#[test]
fn test_determinism_contract() {
    let spec = create_test_spec();
    let spec_hash = *spec.hash();

    // Create two identical contracts
    let contract1 = DeterminismContract {
        spec_hash,
        envelope_hash: [1u8; 32],
    };

    let contract2 = DeterminismContract {
        spec_hash,
        envelope_hash: [1u8; 32],
    };

    assert!(
        contract1.is_replay_valid(&contract2),
        "Identical contracts must be replay valid"
    );

    // Different envelope hash
    let contract3 = DeterminismContract {
        spec_hash,
        envelope_hash: [2u8; 32],
    };

    assert!(
        !contract1.is_replay_valid(&contract3),
        "Different envelope hashes must be replay invalid"
    );
}

/// Test 7: JoinGraph construction
#[test]
fn test_join_graph() {
    let join_graph = JoinGraph {
        edges: vec![JoinEdge {
            left_relation: "nodes".to_string(),
            right_relation: "edges".to_string(),
            join_type: JoinType::Inner,
            left_keys: vec!["id".to_string()],
            right_keys: vec!["node_id".to_string()],
        }],
        constraints: vec![JoinConstraint {
            name: "unique_node_id".to_string(),
            constraint_type: "unique".to_string(),
            relations: vec!["nodes".to_string()],
            columns: vec!["id".to_string()],
        }],
    };

    assert_eq!(join_graph.edges.len(), 1);
    assert_eq!(join_graph.constraints.len(), 1);

    // Verify serialization
    let json = serde_json::to_string(&join_graph).unwrap();
    let deserialized: JoinGraph = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.edges.len(), 1);
}

/// Test 8: OutputTarget with MaterializationMode
#[test]
fn test_output_target() {
    let target = common::output_target(
        "cpg_nodes",
        "normalized_nodes",
        &["id", "type"],
        MaterializationMode::Overwrite,
    );

    let json = serde_json::to_string(&target).unwrap();
    assert!(json.contains("Overwrite"));

    let deserialized: codeanatomy_engine::spec::outputs::OutputTarget =
        serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.table_name, "cpg_nodes");
}

/// Test 9: RuleIntent with RuleClass
#[test]
fn test_rule_intent() {
    let rule = RuleIntent {
        name: "enforce_span_consistency".to_string(),
        class: RuleClass::SemanticIntegrity,
        params: serde_json::json!({"mode": "strict"}),
    };

    let json = serde_json::to_string(&rule).unwrap();
    assert!(json.contains("SemanticIntegrity"));

    let deserialized: RuleIntent = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.name, "enforce_span_consistency");
}

/// Test 10: TypedParameter serialization
#[test]
fn test_typed_parameter() {
    use codeanatomy_engine::spec::parameters::{ParameterTarget, ParameterValue, TypedParameter};

    let parameter = TypedParameter {
        label: Some("file_filter".to_string()),
        target: ParameterTarget::FilterEq {
            base_table: Some("files".to_string()),
            filter_column: "path".to_string(),
        },
        value: ParameterValue::Utf8("src/main.rs".to_string()),
    };

    let json = serde_json::to_string(&parameter).unwrap();
    let deserialized: TypedParameter = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.label, Some("file_filter".to_string()));
}

/// Test 11: Full spec with all components
#[test]
fn test_full_spec_integration() {
    let spec = create_full_spec();

    // Verify all components present
    assert_eq!(spec.input_relations.len(), 2);
    assert_eq!(spec.view_definitions.len(), 3);
    assert_eq!(spec.join_graph.edges.len(), 1);
    assert_eq!(spec.output_targets.len(), 1);
    assert_eq!(spec.rule_intents.len(), 1);
    assert!(spec.typed_parameters.is_empty());

    // Verify serialization roundtrip
    let json = serde_json::to_string_pretty(&spec).unwrap();
    let mut deserialized: SemanticExecutionSpec = serde_json::from_str(&json).unwrap();
    deserialized.spec_hash = hash_spec(&deserialized);

    assert_eq!(spec.spec_hash, deserialized.spec_hash);
}

/// Test 12: Nested unknown fields are rejected.
#[test]
fn test_spec_nested_unknown_fields_rejected() {
    let payload = serde_json::json!({
        "version": 4,
        "input_relations": [
            {
                "logical_name": "input",
                "delta_location": "/tmp/input",
                "requires_lineage": false,
                "version_pin": null,
                "unexpected_field": true
            }
        ],
        "view_definitions": [
            {
                "name": "view1",
                "view_kind": "project",
                "view_dependencies": [],
                "transform": {
                    "kind": "Project",
                    "source": "input",
                    "columns": ["id"]
                },
                "output_schema": {
                    "columns": {
                        "id": "Int64"
                    }
                }
            }
        ],
        "join_graph": {
            "edges": [],
            "constraints": []
        },
        "output_targets": [
            {
                "table_name": "out",
                "source_view": "view1",
                "columns": ["id"],
                "materialization_mode": "Overwrite"
            }
        ],
        "rule_intents": [],
        "rulepack_profile": "Default"
    });
    let result: Result<SemanticExecutionSpec, _> = serde_json::from_value(payload);
    assert!(result.is_err());
}

/// Test 13: Enum mismatches return deserialization errors.
#[test]
fn test_spec_enum_mismatch_rejected() {
    let payload = serde_json::json!({
        "version": 4,
        "input_relations": [],
        "view_definitions": [],
        "join_graph": { "edges": [], "constraints": [] },
        "output_targets": [],
        "rule_intents": [],
        "rulepack_profile": "NotARealProfile"
    });
    let result: Result<SemanticExecutionSpec, _> = serde_json::from_value(payload);
    assert!(result.is_err());
}

/// Test 14: Required fields are enforced.
#[test]
fn test_spec_missing_required_field_rejected() {
    let payload = serde_json::json!({
        "version": 4,
        "input_relations": [],
        "join_graph": { "edges": [], "constraints": [] },
        "output_targets": [],
        "rule_intents": [],
        "rulepack_profile": "Default"
    });
    let result: Result<SemanticExecutionSpec, _> = serde_json::from_value(payload);
    assert!(result.is_err());
}

/// Test 15: Runtime defaults apply when omitted.
#[test]
fn test_spec_runtime_defaults_when_omitted() {
    let payload = serde_json::json!({
        "version": 4,
        "input_relations": [],
        "view_definitions": [],
        "join_graph": { "edges": [], "constraints": [] },
        "output_targets": [],
        "rule_intents": [],
        "rulepack_profile": "Default"
    });
    let spec: SemanticExecutionSpec = serde_json::from_value(payload).expect("valid spec");
    assert!(!spec.runtime.compliance_capture);
    assert_eq!(spec.runtime.tuner_mode, RuntimeTunerMode::Off);
}

/// Test 16: Runtime unknown fields are rejected.
#[test]
fn test_spec_runtime_unknown_field_rejected() {
    let payload = serde_json::json!({
        "version": 4,
        "input_relations": [],
        "view_definitions": [],
        "join_graph": { "edges": [], "constraints": [] },
        "output_targets": [],
        "rule_intents": [],
        "rulepack_profile": "Default",
        "runtime": {
            "compliance_capture": true,
            "tuner_mode": "Observe",
            "unexpected": true
        }
    });
    let result: Result<SemanticExecutionSpec, _> = serde_json::from_value(payload);
    assert!(result.is_err());
}

/// Test 17: TracingConfig rejects unknown fields.
#[test]
fn test_spec_runtime_tracing_unknown_field_rejected() {
    let payload = serde_json::json!({
        "version": 4,
        "input_relations": [],
        "view_definitions": [],
        "join_graph": { "edges": [], "constraints": [] },
        "output_targets": [],
        "rule_intents": [],
        "rulepack_profile": "Default",
        "runtime": {
            "tracing": {
                "enabled": true,
                "unexpected_tracing_key": "boom"
            }
        }
    });
    let result: Result<SemanticExecutionSpec, _> = serde_json::from_value(payload);
    assert!(result.is_err());
}

/// Test 18: TraceExportPolicy rejects unknown fields.
#[test]
fn test_spec_runtime_export_policy_unknown_field_rejected() {
    let payload = serde_json::json!({
        "version": 4,
        "input_relations": [],
        "view_definitions": [],
        "join_graph": { "edges": [], "constraints": [] },
        "output_targets": [],
        "rule_intents": [],
        "rulepack_profile": "Default",
        "runtime": {
            "tracing": {
                "export_policy": {
                    "traces_sampler": "parentbased_always_on",
                    "unknown_export_policy_field": 1
                }
            }
        }
    });
    let result: Result<SemanticExecutionSpec, _> = serde_json::from_value(payload);
    assert!(result.is_err());
}

// Helper: Create minimal test spec
fn create_test_spec() -> SemanticExecutionSpec {
    let mut schema = BTreeMap::new();
    schema.insert("id".to_string(), "Int64".to_string());

    SemanticExecutionSpec::new(
        1,
        vec![InputRelation {
            logical_name: "test_input".to_string(),
            delta_location: "/tmp/delta".to_string(),
            requires_lineage: false,
            version_pin: None,
        }],
        vec![ViewDefinition {
            name: "test_view".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "test_input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: SchemaContract { columns: schema },
        }],
        JoinGraph::default(),
        vec![],
        vec![],
        RulepackProfile::Default,
    )
}

// Helper: Create full spec with all components
fn create_full_spec() -> SemanticExecutionSpec {
    let mut schema1 = BTreeMap::new();
    schema1.insert("id".to_string(), "Int64".to_string());
    schema1.insert("name".to_string(), "Utf8".to_string());

    let mut schema2 = BTreeMap::new();
    schema2.insert("id".to_string(), "Int64".to_string());
    schema2.insert("value".to_string(), "Int64".to_string());

    let mut schema3 = BTreeMap::new();
    schema3.insert("id".to_string(), "Int64".to_string());
    schema3.insert("name".to_string(), "Utf8".to_string());
    schema3.insert("value".to_string(), "Int64".to_string());

    SemanticExecutionSpec::new(
        1,
        vec![
            InputRelation {
                logical_name: "nodes".to_string(),
                delta_location: "/data/nodes".to_string(),
                requires_lineage: true,
                version_pin: Some(42),
            },
            InputRelation {
                logical_name: "edges".to_string(),
                delta_location: "/data/edges".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
        ],
        vec![
            ViewDefinition {
                name: "normalized_nodes".to_string(),
                view_kind: "normalize".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Normalize {
                    source: "nodes".to_string(),
                    id_columns: vec!["id".to_string()],
                    span_columns: None,
                    text_columns: vec!["name".to_string()],
                },
                output_schema: SchemaContract {
                    columns: schema1.clone(),
                },
            },
            ViewDefinition {
                name: "filtered_edges".to_string(),
                view_kind: "filter".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Filter {
                    source: "edges".to_string(),
                    predicate: "value > 10".to_string(),
                },
                output_schema: SchemaContract {
                    columns: schema2.clone(),
                },
            },
            ViewDefinition {
                name: "joined_data".to_string(),
                view_kind: "relate".to_string(),
                view_dependencies: vec!["normalized_nodes".to_string(), "filtered_edges".to_string()],
                transform: ViewTransform::Relate {
                    left: "normalized_nodes".to_string(),
                    right: "filtered_edges".to_string(),
                    join_type: JoinType::Inner,
                    join_keys: vec![JoinKeyPair {
                        left_key: "id".to_string(),
                        right_key: "id".to_string(),
                    }],
                },
                output_schema: SchemaContract {
                    columns: schema3.clone(),
                },
            },
        ],
        JoinGraph {
            edges: vec![JoinEdge {
                left_relation: "nodes".to_string(),
                right_relation: "edges".to_string(),
                join_type: JoinType::Inner,
                left_keys: vec!["id".to_string()],
                right_keys: vec!["id".to_string()],
            }],
            constraints: vec![JoinConstraint {
                name: "unique_node_id".to_string(),
                constraint_type: "unique".to_string(),
                relations: vec!["nodes".to_string()],
                columns: vec!["id".to_string()],
            }],
        },
        vec![common::output_target(
            "cpg_output",
            "joined_data",
            &["id", "name", "value"],
            MaterializationMode::Overwrite,
        )],
        vec![RuleIntent {
            name: "span_consistency".to_string(),
            class: RuleClass::SemanticIntegrity,
            params: serde_json::json!({"strict": true}),
        }],
        RulepackProfile::Default,
    )
}
