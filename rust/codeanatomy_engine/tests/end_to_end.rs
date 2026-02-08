use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use codeanatomy_engine::compiler::plan_compiler::SemanticPlanCompiler;
use codeanatomy_engine::executor::runner::execute_and_materialize;
use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::join_graph::JoinGraph;
use codeanatomy_engine::spec::outputs::{MaterializationMode, OutputTarget};
use codeanatomy_engine::spec::relations::{InputRelation, SchemaContract, ViewDefinition, ViewTransform};
use codeanatomy_engine::spec::rule_intents::RulepackProfile;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

#[tokio::test]
async fn test_end_to_end_compile_and_materialize() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let output_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    )
    .unwrap();

    ctx.register_table(
        "input",
        Arc::new(MemTable::try_new(schema.clone(), vec![vec![input_batch]]).unwrap()),
    )
    .unwrap();
    ctx.register_table(
        "out",
        Arc::new(MemTable::try_new(schema.clone(), vec![vec![output_batch]]).unwrap()),
    )
    .unwrap();

    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), "Int64".to_string());
    let spec = SemanticExecutionSpec::new(
        1,
        vec![InputRelation {
            logical_name: "input".to_string(),
            delta_location: "/tmp/input".to_string(),
            requires_lineage: false,
            version_pin: None,
        }],
        vec![ViewDefinition {
            name: "view_out".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: SchemaContract { columns },
        }],
        JoinGraph::default(),
        vec![OutputTarget {
            table_name: "out".to_string(),
            delta_location: None,
            source_view: "view_out".to_string(),
            columns: vec!["id".to_string()],
            materialization_mode: MaterializationMode::Append,
            partition_by: vec![],
            write_metadata: std::collections::BTreeMap::new(),
            max_commit_retries: None,
        }],
        vec![],
        RulepackProfile::Default,
        vec![],
    );

    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans = compiler.compile().await.unwrap();
    let spec_hash = [0u8; 32];
    let envelope_hash = [0u8; 32];
    let results = execute_and_materialize(&ctx, output_plans, &spec_hash, &envelope_hash).await.unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_materialization_result_has_extended_fields() {
    let result = codeanatomy_engine::executor::result::MaterializationResult {
        table_name: "test".to_string(),
        rows_written: 100,
        partition_count: 4,
        delta_version: Some(3),
        files_added: Some(4),
        bytes_written: Some(1024),
    };
    assert_eq!(result.delta_version, Some(3));
    assert_eq!(result.files_added, Some(4));
    assert_eq!(result.bytes_written, Some(1024));

    // Verify serialization includes new fields
    let json = serde_json::to_string(&result).unwrap();
    assert!(json.contains("delta_version"));
    assert!(json.contains("files_added"));
    assert!(json.contains("bytes_written"));
}

#[test]
fn test_commit_properties_include_hashes() {
    use codeanatomy_engine::executor::delta_writer::build_commit_properties;
    use codeanatomy_engine::spec::outputs::{OutputTarget, MaterializationMode};
    use std::collections::BTreeMap;

    let mut metadata = BTreeMap::new();
    metadata.insert("user.tag".to_string(), "v1".to_string());

    let target = OutputTarget {
        table_name: "test".to_string(),
        delta_location: None,
        source_view: "view1".to_string(),
        columns: vec!["id".to_string()],
        materialization_mode: MaterializationMode::Overwrite,
        partition_by: vec![],
        write_metadata: metadata,
        max_commit_retries: None,
    };

    let spec_hash = [0xABu8; 32];
    let envelope_hash = [0xCDu8; 32];

    let props = build_commit_properties(&target, &spec_hash, &envelope_hash);

    assert!(props.contains_key("codeanatomy.spec_hash"));
    assert!(props.contains_key("codeanatomy.envelope_hash"));
    assert!(props.contains_key("user.tag"));
    assert_eq!(props.get("user.tag").unwrap(), "v1");
    assert_eq!(props.get("codeanatomy.spec_hash").unwrap(), &hex::encode([0xABu8; 32]));
}

#[tokio::test]
async fn test_write_idempotency_append_mode() {
    // Test that executing the same plan twice produces consistent results
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    // First execution setup
    let input_batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 20]))],
    )
    .unwrap();
    let output_batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    )
    .unwrap();

    ctx.register_table(
        "input",
        Arc::new(MemTable::try_new(schema.clone(), vec![vec![input_batch1]]).unwrap()),
    )
    .unwrap();
    ctx.register_table(
        "out",
        Arc::new(MemTable::try_new(schema.clone(), vec![vec![output_batch1]]).unwrap()),
    )
    .unwrap();

    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), "Int64".to_string());
    let spec = SemanticExecutionSpec::new(
        1,
        vec![InputRelation {
            logical_name: "input".to_string(),
            delta_location: "/tmp/input".to_string(),
            requires_lineage: false,
            version_pin: None,
        }],
        vec![ViewDefinition {
            name: "view_out".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: SchemaContract { columns },
        }],
        JoinGraph::default(),
        vec![OutputTarget {
            table_name: "out".to_string(),
            delta_location: None,
            source_view: "view_out".to_string(),
            columns: vec!["id".to_string()],
            materialization_mode: MaterializationMode::Append,
            partition_by: vec![],
            write_metadata: std::collections::BTreeMap::new(),
            max_commit_retries: None,
        }],
        vec![],
        RulepackProfile::Default,
        vec![],
    );

    // First execution
    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans = compiler.compile().await.unwrap();
    let results1 = execute_and_materialize(&ctx, output_plans, &spec.spec_hash, &[0u8; 32]).await.unwrap();
    assert_eq!(results1.len(), 1);
    let first_row_count = results1[0].rows_written;

    // Second execution with fresh tables
    let input_batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 20]))],
    )
    .unwrap();
    let output_batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
    )
    .unwrap();

    let _ = ctx.deregister_table("input");
    let _ = ctx.deregister_table("out");
    let _ = ctx.deregister_table("view_out");

    ctx.register_table(
        "input",
        Arc::new(MemTable::try_new(schema.clone(), vec![vec![input_batch2]]).unwrap()),
    )
    .unwrap();
    ctx.register_table(
        "out",
        Arc::new(MemTable::try_new(schema.clone(), vec![vec![output_batch2]]).unwrap()),
    )
    .unwrap();

    let compiler2 = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans2 = compiler2.compile().await.unwrap();
    let results2 = execute_and_materialize(&ctx, output_plans2, &spec.spec_hash, &[0u8; 32]).await.unwrap();
    assert_eq!(results2.len(), 1);

    // Both runs should write the same number of rows (deterministic behavior)
    assert_eq!(first_row_count, results2[0].rows_written);
    // And both should have written at least 1 row
    assert!(first_row_count > 0);
}

#[test]
fn test_profile_driven_rulepack_construction() {
    use codeanatomy_engine::rules::rulepack::RulepackFactory;
    use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};
    use codeanatomy_engine::spec::rule_intents::{RuleClass, RuleIntent, RulepackProfile};

    let env = EnvironmentProfile::from_class(EnvironmentClass::Small);
    let intents = vec![RuleIntent {
        name: "strict_safety".to_string(),
        class: RuleClass::Safety,
        params: serde_json::Value::Null,
    }];

    let low_latency = RulepackFactory::build_ruleset(&RulepackProfile::LowLatency, &intents, &env);
    let default = RulepackFactory::build_ruleset(&RulepackProfile::Default, &intents, &env);
    let strict = RulepackFactory::build_ruleset(&RulepackProfile::Strict, &intents, &env);

    // Verify all three profiles can build rulesets
    assert!(low_latency.total_count() > 0);
    assert!(default.total_count() > 0);
    assert!(strict.total_count() > 0);

    // Verify fingerprints are computed (non-zero)
    assert_ne!(low_latency.fingerprint, [0u8; 32]);
    assert_ne!(default.fingerprint, [0u8; 32]);
    assert_ne!(strict.fingerprint, [0u8; 32]);

    // Verify that different profiles can produce different rule counts
    // (Though specific ordering is not guaranteed, all should be valid)
    let total_counts = vec![
        low_latency.total_count(),
        default.total_count(),
        strict.total_count(),
    ];
    assert!(total_counts.iter().all(|&c| c > 0));
}
