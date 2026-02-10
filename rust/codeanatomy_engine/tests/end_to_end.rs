mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use codeanatomy_engine::compiler::plan_compiler::SemanticPlanCompiler;
use codeanatomy_engine::executor::delta_writer::LineageContext;
use codeanatomy_engine::executor::orchestration::prepare_execution_context;
use codeanatomy_engine::executor::pipeline::execute_pipeline;
use codeanatomy_engine::executor::runner::execute_and_materialize;
use codeanatomy_engine::executor::warnings::{RunWarning, WarningCode, WarningStage};
use codeanatomy_engine::session::factory::SessionFactory;
use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::join_graph::JoinGraph;
use codeanatomy_engine::spec::outputs::{MaterializationMode, OutputTarget};
use codeanatomy_engine::spec::relations::{InputRelation, SchemaContract, ViewDefinition, ViewTransform};
use codeanatomy_engine::spec::rule_intents::RulepackProfile;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

fn test_lineage(spec_hash: [u8; 32], envelope_hash: [u8; 32]) -> LineageContext {
    LineageContext {
        spec_hash,
        envelope_hash,
        planning_surface_hash: [0u8; 32],
        provider_identity_hash: [0u8; 32],
        rulepack_fingerprint: [0u8; 32],
        rulepack_profile: "Default".to_string(),
        runtime_profile_name: None,
        run_started_at_rfc3339: "2026-02-09T00:00:00Z".to_string(),
        runtime_lineage_tags: BTreeMap::new(),
    }
}

async fn seed_delta_input(location: &str) {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    ctx.register_table(
        "seed_input",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();

    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), "Int64".to_string());
    let spec = SemanticExecutionSpec::new(
        1,
        vec![InputRelation {
            logical_name: "seed_input".to_string(),
            delta_location: "/tmp/seed_input".to_string(),
            requires_lineage: false,
            version_pin: None,
        }],
        vec![ViewDefinition {
            name: "seed_view".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "seed_input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: SchemaContract { columns },
        }],
        JoinGraph::default(),
        vec![OutputTarget {
            table_name: "seed_out".to_string(),
            delta_location: Some(location.to_string()),
            source_view: "seed_view".to_string(),
            columns: vec!["id".to_string()],
            materialization_mode: MaterializationMode::Overwrite,
            partition_by: vec![],
            write_metadata: BTreeMap::new(),
            max_commit_retries: Some(1),
        }],
        vec![],
        RulepackProfile::Default,
    );

    let output_plans = SemanticPlanCompiler::new(&ctx, &spec).compile().await.unwrap();
    let lineage = test_lineage([7u8; 32], [8u8; 32]);
    execute_and_materialize(&ctx, output_plans, &lineage)
        .await
        .unwrap();
}

fn build_delta_parity_spec(input_location: String, output_location: String) -> SemanticExecutionSpec {
    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), "Int64".to_string());
    let mut spec = SemanticExecutionSpec::new(
        1,
        vec![InputRelation {
            logical_name: "input".to_string(),
            delta_location: input_location,
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
            table_name: "out_delta".to_string(),
            delta_location: Some(output_location),
            source_view: "view_out".to_string(),
            columns: vec!["id".to_string()],
            materialization_mode: MaterializationMode::Overwrite,
            partition_by: vec![],
            write_metadata: BTreeMap::new(),
            max_commit_retries: Some(2),
        }],
        vec![],
        RulepackProfile::Default,
    );
    spec.runtime.compliance_capture = true;
    spec
}

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
    );

    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans = compiler.compile().await.unwrap();
    let spec_hash = [0u8; 32];
    let envelope_hash = [0u8; 32];
    let lineage = test_lineage(spec_hash, envelope_hash);
    let results = execute_and_materialize(&ctx, output_plans, &lineage).await.unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_materialization_result_has_extended_fields() {
    let result = codeanatomy_engine::executor::result::MaterializationResult {
        table_name: "test".to_string(),
        delta_location: Some("/tmp/test".to_string()),
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
    let lineage = test_lineage(spec_hash, envelope_hash);
    let props = build_commit_properties(&target, &lineage);

    assert!(props.contains_key("codeanatomy.spec_hash"));
    assert!(props.contains_key("codeanatomy.envelope_hash"));
    assert!(props.contains_key("user.tag"));
    assert_eq!(props.get("user.tag").unwrap(), "v1");
    assert_eq!(props.get("codeanatomy.spec_hash").unwrap(), &hex::encode([0xABu8; 32]));
}

#[tokio::test]
async fn test_delta_history_contains_lineage_metadata() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let input_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    ctx.register_table(
        "input",
        Arc::new(MemTable::try_new(schema.clone(), vec![vec![input_batch]]).unwrap()),
    )
    .unwrap();

    let out_dir = tempfile::tempdir().unwrap();
    let delta_path = out_dir.path().join("out_delta");
    let delta_location = delta_path.to_string_lossy().to_string();

    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), "Int64".to_string());
    let mut write_metadata = BTreeMap::new();
    write_metadata.insert("user.tag".to_string(), "v1".to_string());
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
            table_name: "out_delta".to_string(),
            delta_location: Some(delta_location.clone()),
            source_view: "view_out".to_string(),
            columns: vec!["id".to_string()],
            materialization_mode: MaterializationMode::Append,
            partition_by: vec![],
            write_metadata,
            max_commit_retries: Some(3),
        }],
        vec![],
        RulepackProfile::Default,
    );

    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans = compiler.compile().await.unwrap();
    let spec_hash = [0xABu8; 32];
    let envelope_hash = [0xCDu8; 32];
    let lineage = test_lineage(spec_hash, envelope_hash);
    let results = execute_and_materialize(&ctx, output_plans, &lineage)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].delta_version.is_some());

    let expected_spec_hash = hex::encode(spec_hash);
    let expected_envelope_hash = hex::encode(envelope_hash);
    let log_dir = delta_path.join("_delta_log");
    let mut found_lineage = false;
    for entry in std::fs::read_dir(&log_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let content = std::fs::read_to_string(&path).unwrap();
        if content.contains("codeanatomy.spec_hash")
            && content.contains(&expected_spec_hash)
            && content.contains("codeanatomy.envelope_hash")
            && content.contains(&expected_envelope_hash)
            && content.contains("user.tag")
            && content.contains("v1")
        {
            found_lineage = true;
            break;
        }
    }

    assert!(
        found_lineage,
        "expected committed Delta log metadata to include codeanatomy lineage fields"
    );
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
    );

    // First execution
    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans = compiler.compile().await.unwrap();
    let lineage1 = test_lineage(spec.spec_hash, [0u8; 32]);
    let results1 = execute_and_materialize(&ctx, output_plans, &lineage1).await.unwrap();
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
    let lineage2 = test_lineage(spec.spec_hash, [0u8; 32]);
    let results2 = execute_and_materialize(&ctx, output_plans2, &lineage2).await.unwrap();
    assert_eq!(results2.len(), 1);

    // Both runs should write the same number of rows (deterministic behavior)
    assert_eq!(first_row_count, results2[0].rows_written);
    // And both should have written at least 1 row
    assert!(first_row_count > 0);
}

// ---------------------------------------------------------------------------
// Scope 10: RunResult.warnings field is accessible and starts empty
// ---------------------------------------------------------------------------

/// Scope 10: RunResult has a warnings field that starts empty by default.
#[test]
fn test_run_result_warnings_accessible_and_empty() {
    let result = codeanatomy_engine::executor::result::RunResult::builder()
        .with_spec_hash([0u8; 32])
        .with_envelope_hash([0u8; 32])
        .with_rulepack_fingerprint([0u8; 32])
        .started_now()
        .build();

    assert!(
        result.warnings.is_empty(),
        "default RunResult must have empty warnings"
    );
}

/// Scope 10: RunResult warnings can be populated via builder.
#[test]
fn test_run_result_warnings_can_be_populated() {
    let result = codeanatomy_engine::executor::result::RunResult::builder()
        .with_spec_hash([0u8; 32])
        .with_envelope_hash([0u8; 32])
        .with_rulepack_fingerprint([0u8; 32])
        .add_warning(RunWarning::new(
            WarningCode::PlanBundleRuntimeCaptureFailed,
            WarningStage::PlanBundle,
            "plan bundle capture failed: test error",
        ))
        .add_warning(RunWarning::new(
            WarningCode::MaintenanceFailed,
            WarningStage::Maintenance,
            "maintenance skipped: no locations",
        ))
        .started_now()
        .build();

    assert_eq!(result.warnings.len(), 2);
    assert!(result.warnings[0].message.contains("plan bundle capture failed"));
    assert!(result.warnings[1].message.contains("maintenance skipped"));
}

#[tokio::test]
async fn test_pipeline_includes_preflight_warnings() {
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
            write_metadata: BTreeMap::new(),
            max_commit_retries: None,
        }],
        vec![],
        RulepackProfile::Default,
    );

    let outcome = codeanatomy_engine::executor::pipeline::execute_pipeline(
        &ctx,
        &spec,
        [1u8; 32],
        [2u8; 32],
        vec![],
        [3u8; 32],
        vec![RunWarning::new(
            WarningCode::ReservedProfileKnobIgnored,
            WarningStage::RuntimeProfile,
            "reserved knob warning",
        )],
    )
    .await
    .unwrap();
    assert!(
        outcome
            .run_result
            .warnings
            .iter()
            .any(|warning| warning.message.contains("reserved knob warning"))
    );
    let summary = outcome
        .run_result
        .trace_metrics_summary
        .as_ref()
        .expect("trace metrics summary should exist for executed plans");
    assert!(summary.warning_count_total >= 1);
    assert!(
        summary
            .warning_counts_by_code
            .contains_key("reserved_profile_knob_ignored")
    );
}

// ---------------------------------------------------------------------------
// Scope 13: Pipeline module exists, PipelineOutcome type is importable
// ---------------------------------------------------------------------------

/// Scope 13: PipelineOutcome type is importable from executor::pipeline.
///
/// This is a compile-time assertion. The test verifies that the pipeline
/// module exists and the PipelineOutcome struct is accessible.
#[test]
fn test_pipeline_outcome_type_is_importable() {
    use codeanatomy_engine::executor::pipeline::PipelineOutcome;

    // Type-level assertion: PipelineOutcome is a struct with known fields.
    // We cannot construct one without a full execution, but we can verify
    // the type is importable and its fields are accessible by name.
    fn _assert_outcome_fields(outcome: &PipelineOutcome) {
        let _run_result = &outcome.run_result;
        let _collected = &outcome.collected_metrics;
    }

    // Compile-time proof: execute_pipeline is importable.
    // We do not call it (requires full session setup), but the import
    // proves the pipeline module is publicly accessible.
    let _ = codeanatomy_engine::executor::pipeline::execute_pipeline;
}

#[test]
fn test_profile_driven_rulepack_construction() {
    use codeanatomy_engine::rules::rulepack::RulepackFactory;
    use codeanatomy_engine::spec::rule_intents::{RuleClass, RuleIntent, RulepackProfile};

    let env = common::test_environment_profile();
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
    let total_counts = [
        low_latency.total_count(),
        default.total_count(),
        strict.total_count(),
    ];
    assert!(total_counts.iter().all(|&c| c > 0));
}

#[tokio::test]
async fn test_prepared_context_and_pipeline_identity_surfaces_are_stable() {
    let temp_dir = tempfile::tempdir().unwrap();
    let input_location = temp_dir.path().join("input_delta");
    let output_location = temp_dir.path().join("output_delta");

    seed_delta_input(&input_location.to_string_lossy()).await;

    let spec = build_delta_parity_spec(
        input_location.to_string_lossy().to_string(),
        output_location.to_string_lossy().to_string(),
    );
    let factory = SessionFactory::new(common::test_environment_profile());
    let ruleset = common::empty_ruleset();

    let prepared_a = prepare_execution_context(&factory, &spec, &ruleset, None)
        .await
        .unwrap();
    let prepared_b = prepare_execution_context(&factory, &spec, &ruleset, None)
        .await
        .unwrap();

    assert_eq!(
        prepared_a.envelope.envelope_hash,
        prepared_b.envelope.envelope_hash
    );
    assert_eq!(
        prepared_a.envelope.planning_surface_hash,
        prepared_b.envelope.planning_surface_hash
    );
    assert_eq!(prepared_a.provider_identities, prepared_b.provider_identities);
    assert!(
        !prepared_a.provider_identities.is_empty(),
        "expected at least one provider identity from registered input relations"
    );

    let mut sorted = prepared_a.provider_identities.clone();
    sorted.sort_by(|left, right| left.table_name.cmp(&right.table_name));
    assert_eq!(prepared_a.provider_identities, sorted);

    let outcome = execute_pipeline(
        &prepared_a.ctx,
        &spec,
        prepared_a.envelope.envelope_hash,
        ruleset.fingerprint,
        prepared_a.provider_identities.clone(),
        prepared_a.envelope.planning_surface_hash,
        prepared_a.preflight_warnings.clone(),
    )
    .await
    .unwrap();

    assert!(
        !outcome.run_result.plan_bundles.is_empty(),
        "compliance capture should produce persisted plan bundle artifacts"
    );
    for bundle in &outcome.run_result.plan_bundles {
        assert_eq!(
            bundle.planning_surface_hash,
            prepared_a.envelope.planning_surface_hash
        );
        assert_eq!(bundle.provider_identities, prepared_a.provider_identities);
    }
}
