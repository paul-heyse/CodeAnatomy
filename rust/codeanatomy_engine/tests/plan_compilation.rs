mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use codeanatomy_engine::compiler::param_compiler::apply_typed_parameters;
use codeanatomy_engine::compiler::plan_bundle::{PlanBundleArtifact, ProviderIdentity};
use codeanatomy_engine::compiler::plan_compiler::SemanticPlanCompiler;
use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::join_graph::JoinGraph;
use codeanatomy_engine::spec::outputs::MaterializationMode;
use codeanatomy_engine::spec::relations::{InputRelation, SchemaContract, ViewDefinition, ViewTransform};
use codeanatomy_engine::spec::rule_intents::RulepackProfile;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

#[tokio::test]
async fn test_plan_compiler_builds_single_output_plan() {
    let ctx = SessionContext::new();
    let input_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let input_batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    ctx.register_table(
        "input_table",
        Arc::new(MemTable::try_new(input_schema, vec![vec![input_batch]]).unwrap()),
    )
    .unwrap();

    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), "Int64".to_string());
    let spec = SemanticExecutionSpec::new(
        1,
        vec![InputRelation {
            logical_name: "input_table".to_string(),
            delta_location: "/tmp/input_table".to_string(),
            requires_lineage: false,
            version_pin: None,
        }],
        vec![ViewDefinition {
            name: "projected".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input_table".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: SchemaContract { columns },
        }],
        JoinGraph::default(),
        vec![common::output_target(
            "out_table",
            "projected",
            &["id"],
            MaterializationMode::Overwrite,
        )],
        vec![],
        RulepackProfile::Default,
    );

    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans = compiler.compile().await.unwrap();
    assert_eq!(output_plans.len(), 1);
}

#[tokio::test]
async fn test_plan_compiler_builds_multi_output_plan() {
    let ctx = SessionContext::new();
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let input_batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    ctx.register_table(
        "input_table",
        Arc::new(MemTable::try_new(input_schema, vec![vec![input_batch]]).unwrap()),
    )
    .unwrap();

    let mut id_cols = BTreeMap::new();
    id_cols.insert("id".to_string(), "Int64".to_string());
    let mut name_cols = BTreeMap::new();
    name_cols.insert("name".to_string(), "Utf8".to_string());

    let spec = SemanticExecutionSpec::new(
        1,
        vec![InputRelation {
            logical_name: "input_table".to_string(),
            delta_location: "/tmp/input_table".to_string(),
            requires_lineage: false,
            version_pin: None,
        }],
        vec![
            ViewDefinition {
                name: "ids_view".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input_table".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: SchemaContract { columns: id_cols },
            },
            ViewDefinition {
                name: "names_view".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input_table".to_string(),
                    columns: vec!["name".to_string()],
                },
                output_schema: SchemaContract { columns: name_cols },
            },
        ],
        JoinGraph::default(),
        vec![
            common::output_target(
                "ids_out",
                "ids_view",
                &["id"],
                MaterializationMode::Overwrite,
            ),
            common::output_target(
                "names_out",
                "names_view",
                &["name"],
                MaterializationMode::Overwrite,
            ),
        ],
        vec![],
        RulepackProfile::Default,
    );

    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans = compiler.compile().await.unwrap();
    assert_eq!(output_plans.len(), 2);
    // Verify both output targets are present
    let table_names: Vec<&str> = output_plans.iter().map(|(t, _)| t.table_name.as_str()).collect();
    assert!(table_names.contains(&"ids_out"));
    assert!(table_names.contains(&"names_out"));
}

#[tokio::test]
async fn test_plan_compiler_rejects_cyclic_dependencies() {
    let ctx = SessionContext::new();
    let input_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let input_batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    ctx.register_table(
        "input_table",
        Arc::new(MemTable::try_new(input_schema, vec![vec![input_batch]]).unwrap()),
    )
    .unwrap();

    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), "Int64".to_string());

    let spec = SemanticExecutionSpec::new(
        1,
        vec![InputRelation {
            logical_name: "input_table".to_string(),
            delta_location: "/tmp/input".to_string(),
            requires_lineage: false,
            version_pin: None,
        }],
        vec![
            ViewDefinition {
                name: "view_a".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec!["view_b".to_string()],
                transform: ViewTransform::Project {
                    source: "view_b".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: SchemaContract { columns: columns.clone() },
            },
            ViewDefinition {
                name: "view_b".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec!["view_a".to_string()],
                transform: ViewTransform::Project {
                    source: "view_a".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: SchemaContract { columns },
            },
        ],
        JoinGraph::default(),
        vec![common::output_target(
            "out",
            "view_a",
            &["id"],
            MaterializationMode::Overwrite,
        )],
        vec![],
        RulepackProfile::Default,
    );

    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let result = compiler.compile().await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Cycle detected"), "Expected cycle detection error, got: {err_msg}");
}

// ---------------------------------------------------------------------------
// Scope 8: Typed parameter compile path (apply_typed_parameters exists and callable)
// ---------------------------------------------------------------------------

/// Scope 8: apply_typed_parameters is importable and callable with empty params.
#[tokio::test]
async fn test_apply_typed_parameters_exists_and_callable() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    ctx.register_table(
        "test_input",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();

    let df = ctx.table("test_input").await.unwrap();

    // Empty params should return the DataFrame unchanged.
    let result = apply_typed_parameters(df, &[]).await;
    assert!(
        result.is_ok(),
        "apply_typed_parameters with empty params must succeed"
    );

    let batches = result.unwrap().collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "empty params must not filter any rows");
}

// ---------------------------------------------------------------------------
// Scope 4: PlanBundleArtifact has planning_surface_hash and provider_identities
// ---------------------------------------------------------------------------

/// Scope 4: PlanBundleArtifact contains planning_surface_hash and provider_identities fields.
///
/// This is a structural/compile-time assertion that verifies these fields exist
/// and are accessible on the artifact struct.
#[test]
fn test_plan_bundle_artifact_has_required_scope4_fields() {
    // Construct a minimal PlanBundleArtifact to verify field presence.
    let artifact = PlanBundleArtifact {
        artifact_version: 1,
        p0_digest: [0u8; 32],
        p1_digest: [0u8; 32],
        p2_digest: [0u8; 32],
        p0_text: None,
        p1_text: None,
        p2_text: None,
        explain_verbose: vec![],
        explain_analyze: vec![],
        rulepack_fingerprint: [0u8; 32],
        provider_identities: vec![
            ProviderIdentity {
                table_name: "test_table".to_string(),
                identity_hash: [42u8; 32],
            },
        ],
        schema_fingerprints: codeanatomy_engine::compiler::plan_bundle::SchemaFingerprints {
            p0_schema_hash: [0u8; 32],
            p1_schema_hash: [0u8; 32],
            p2_schema_hash: [0u8; 32],
        },
        planning_surface_hash: [99u8; 32],
        substrait_bytes: None,
        sql_text: None,
        delta_codec_logical_bytes: None,
        delta_codec_physical_bytes: None,
    };

    // Scope 4: provider_identities field exists and is populated.
    assert_eq!(artifact.provider_identities.len(), 1);
    assert_eq!(artifact.provider_identities[0].table_name, "test_table");

    // Scope 3/4: planning_surface_hash field exists.
    assert_eq!(artifact.planning_surface_hash, [99u8; 32]);

    // Serialization includes both fields.
    let json = serde_json::to_string(&artifact).unwrap();
    assert!(
        json.contains("planning_surface_hash"),
        "serialized artifact must contain planning_surface_hash"
    );
    assert!(
        json.contains("provider_identities"),
        "serialized artifact must contain provider_identities"
    );
}
