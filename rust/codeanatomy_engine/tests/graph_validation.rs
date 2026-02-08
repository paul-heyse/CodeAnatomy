use std::collections::BTreeMap;

use codeanatomy_engine::compiler::graph_validator::validate_graph;
use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::join_graph::JoinGraph;
use codeanatomy_engine::spec::outputs::{MaterializationMode, OutputTarget};
use codeanatomy_engine::spec::relations::{InputRelation, SchemaContract, ViewDefinition, ViewTransform};
use codeanatomy_engine::spec::rule_intents::RulepackProfile;

fn minimal_schema() -> SchemaContract {
    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), "Int64".to_string());
    SchemaContract { columns }
}

#[test]
fn test_graph_validator_rejects_duplicate_output_tables() {
    let spec = SemanticExecutionSpec::new(
        1,
        vec![InputRelation {
            logical_name: "input".to_string(),
            delta_location: "/tmp/input".to_string(),
            requires_lineage: false,
            version_pin: None,
        }],
        vec![ViewDefinition {
            name: "v1".to_string(),
            view_kind: "filter".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Filter {
                source: "input".to_string(),
                predicate: "TRUE".to_string(),
            },
            output_schema: minimal_schema(),
        }],
        JoinGraph::default(),
        vec![
            OutputTarget {
                table_name: "dup".to_string(),
                delta_location: None,
                source_view: "v1".to_string(),
                columns: vec!["id".to_string()],
                materialization_mode: MaterializationMode::Overwrite,
                partition_by: vec![],
                write_metadata: std::collections::BTreeMap::new(),
                max_commit_retries: None,
            },
            OutputTarget {
                table_name: "dup".to_string(),
                delta_location: None,
                source_view: "v1".to_string(),
                columns: vec!["id".to_string()],
                materialization_mode: MaterializationMode::Append,
                partition_by: vec![],
                write_metadata: std::collections::BTreeMap::new(),
                max_commit_retries: None,
            },
        ],
        vec![],
        RulepackProfile::Default,
        vec![],
    );

    let result = validate_graph(&spec);
    assert!(result.is_err());
}
