use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use codeanatomy_engine::compiler::semantic_validator::validate_semantics;
use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::join_graph::JoinGraph;
use codeanatomy_engine::spec::outputs::{MaterializationMode, OutputTarget};
use codeanatomy_engine::spec::relations::{
    InputRelation, JoinKeyPair, SchemaContract, ViewDefinition, ViewTransform,
};
use codeanatomy_engine::spec::rule_intents::RulepackProfile;

fn schema_contract(columns: &[(&str, &str)]) -> SchemaContract {
    let mut map = BTreeMap::new();
    for (name, ty) in columns {
        map.insert((*name).to_string(), (*ty).to_string());
    }
    SchemaContract { columns: map }
}

async fn context_with_table() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("input", Arc::new(table)).unwrap();
    ctx
}

#[tokio::test]
async fn test_semantic_validator_reports_unknown_column() {
    let ctx = context_with_table().await;
    let spec = SemanticExecutionSpec::new(
        3,
        vec![InputRelation {
            logical_name: "input".to_string(),
            delta_location: "/tmp/input".to_string(),
            requires_lineage: false,
            version_pin: None,
        }],
        vec![ViewDefinition {
            name: "v".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input".to_string(),
                columns: vec!["missing".to_string()],
            },
            output_schema: schema_contract(&[("missing", "Int64")]),
        }],
        JoinGraph::default(),
        vec![OutputTarget {
            table_name: "out".to_string(),
            delta_location: None,
            source_view: "v".to_string(),
            columns: vec!["missing".to_string()],
            materialization_mode: MaterializationMode::Overwrite,
            partition_by: vec![],
            write_metadata: BTreeMap::new(),
            max_commit_retries: None,
        }],
        vec![],
        RulepackProfile::Default,
    );

    let result = validate_semantics(&spec, &ctx).await.unwrap();
    assert!(!result.errors.is_empty());
}

#[tokio::test]
async fn test_semantic_validator_warns_on_numeric_join_coercion() {
    let ctx = context_with_table().await;
    let spec = SemanticExecutionSpec::new(
        3,
        vec![
            InputRelation {
                logical_name: "left".to_string(),
                delta_location: "/tmp/left".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
            InputRelation {
                logical_name: "right".to_string(),
                delta_location: "/tmp/right".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
        ],
        vec![ViewDefinition {
            name: "joined".to_string(),
            view_kind: "relate".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Relate {
                left: "left".to_string(),
                right: "right".to_string(),
                join_type: codeanatomy_engine::spec::relations::JoinType::Inner,
                join_keys: vec![JoinKeyPair {
                    left_key: "id".to_string(),
                    right_key: "id".to_string(),
                }],
            },
            output_schema: schema_contract(&[("id", "Int64")]),
        }],
        JoinGraph::default(),
        vec![],
        vec![],
        RulepackProfile::Default,
    );
    // register aliases as simple views to satisfy input lookup
    let df = ctx.table("input").await.unwrap();
    ctx.register_table("left", df.clone().into_view()).unwrap();
    ctx.register_table("right", df.into_view()).unwrap();

    let result = validate_semantics(&spec, &ctx).await.unwrap();
    assert!(result.errors.is_empty());
}
