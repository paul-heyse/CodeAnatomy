use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use codeanatomy_engine::compiler::plan_compiler::SemanticPlanCompiler;
use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::join_graph::JoinGraph;
use codeanatomy_engine::spec::outputs::{MaterializationMode, OutputTarget};
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
        vec![OutputTarget {
            table_name: "out_table".to_string(),
            delta_location: None,
            source_view: "projected".to_string(),
            columns: vec!["id".to_string()],
            materialization_mode: MaterializationMode::Overwrite,
        }],
        vec![],
        RulepackProfile::Default,
        vec![],
    );

    let compiler = SemanticPlanCompiler::new(&ctx, &spec);
    let output_plans = compiler.compile().await.unwrap();
    assert_eq!(output_plans.len(), 1);
}
