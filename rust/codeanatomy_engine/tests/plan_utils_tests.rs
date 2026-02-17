use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use codeanatomy_engine::compiler::plan_utils::{
    blake3_hash_bytes, compute_view_fanout, normalize_logical, normalize_physical, view_index,
};
use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::join_graph::JoinGraph;
use codeanatomy_engine::spec::outputs::{MaterializationMode, OutputTarget};
use codeanatomy_engine::spec::relations::{SchemaContract, ViewDefinition, ViewTransform};
use codeanatomy_engine::spec::rule_intents::RulepackProfile;

fn schema_contract() -> SchemaContract {
    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), "Int64".to_string());
    SchemaContract { columns }
}

#[test]
fn compute_view_fanout_counts_view_and_output_refs() {
    let views = vec![
        ViewDefinition {
            name: "base".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: schema_contract(),
        },
        ViewDefinition {
            name: "filtered".to_string(),
            view_kind: "filter".to_string(),
            view_dependencies: vec!["base".to_string()],
            transform: ViewTransform::Filter {
                source: "base".to_string(),
                predicate: "id > 0".to_string(),
            },
            output_schema: schema_contract(),
        },
    ];
    let outputs = vec![OutputTarget {
        table_name: "out".to_string(),
        delta_location: None,
        source_view: "base".to_string(),
        columns: vec![],
        materialization_mode: MaterializationMode::Overwrite,
        partition_by: vec![],
        write_metadata: BTreeMap::new(),
        max_commit_retries: None,
    }];
    let spec = SemanticExecutionSpec::new(
        4,
        vec![],
        views,
        JoinGraph::default(),
        outputs,
        vec![],
        RulepackProfile::Default,
    );

    let fanout = compute_view_fanout(&spec);
    assert_eq!(fanout.get("base"), Some(&2));
    assert_eq!(fanout.get("filtered"), Some(&0));

    let index = view_index(&spec);
    assert!(index.contains_key("base"));
    assert!(index.contains_key("filtered"));
}

#[tokio::test]
async fn normalize_and_hash_helpers_are_deterministic() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let df = ctx
        .table("t")
        .await
        .unwrap()
        .filter(datafusion::prelude::col("id").gt_eq(datafusion::prelude::lit(2)))
        .unwrap();
    let logical = df.logical_plan().clone();
    let physical = ctx.state().create_physical_plan(&logical).await.unwrap();

    let logical_text = normalize_logical(&logical);
    let logical_text_2 = normalize_logical(&logical);
    assert_eq!(logical_text, logical_text_2);

    let physical_text = normalize_physical(physical.as_ref());
    let physical_text_2 = normalize_physical(physical.as_ref());
    assert_eq!(physical_text, physical_text_2);

    let digest_a = blake3_hash_bytes(logical_text.as_bytes());
    let digest_b = blake3_hash_bytes(logical_text_2.as_bytes());
    assert_eq!(digest_a, digest_b);
}
