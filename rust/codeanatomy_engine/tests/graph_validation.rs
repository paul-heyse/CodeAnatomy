mod common;

use codeanatomy_engine::compiler::graph_validator::validate_graph;
use codeanatomy_engine::spec::execution_spec::SemanticExecutionSpec;
use codeanatomy_engine::spec::join_graph::JoinGraph;
use codeanatomy_engine::spec::outputs::MaterializationMode;
use codeanatomy_engine::spec::relations::{InputRelation, SchemaContract, ViewDefinition, ViewTransform};
use codeanatomy_engine::spec::rule_intents::RulepackProfile;

fn minimal_schema() -> SchemaContract {
    common::schema_contract(&[("id", "Int64")])
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
            common::output_target("dup", "v1", &["id"], MaterializationMode::Overwrite),
            common::output_target("dup", "v1", &["id"], MaterializationMode::Append),
        ],
        vec![],
        RulepackProfile::Default,
    );

    let result = validate_graph(&spec);
    assert!(result.is_err());
}
