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
        }],
        vec![],
        RulepackProfile::Default,
        vec![],
    );

    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans = compiler.compile().await.unwrap();
    let results = execute_and_materialize(&ctx, output_plans).await.unwrap();
    assert_eq!(results.len(), 1);
}
