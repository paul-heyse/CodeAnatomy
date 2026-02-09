//! WS4.5: Graph validation before compilation.
//!
//! Ensures the view dependency graph is well-formed before attempting compilation.

use datafusion_common::{DataFusionError, Result};
use std::collections::HashSet;

use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::relations::ViewTransform;

/// Extract source references from a ViewTransform.
fn extract_sources(transform: &ViewTransform) -> Vec<&str> {
    match transform {
        ViewTransform::Normalize { source, .. } => vec![source.as_str()],
        ViewTransform::Relate { left, right, .. } => vec![left.as_str(), right.as_str()],
        ViewTransform::Union { sources, .. } => sources.iter().map(|s| s.as_str()).collect(),
        ViewTransform::Project { source, .. } => vec![source.as_str()],
        ViewTransform::Filter { source, .. } => vec![source.as_str()],
        ViewTransform::Aggregate { source, .. } => vec![source.as_str()],
        ViewTransform::IncrementalCdf { source, .. } => vec![source.as_str()],
        ViewTransform::Metadata { source } => vec![source.as_str()],
        ViewTransform::FileManifest { source } => vec![source.as_str()],
    }
}

/// Validate the view dependency graph for well-formedness.
///
/// Checks:
/// 1. No duplicate view names
/// 2. All view_dependencies resolve to known views
/// 3. All source references in transforms resolve to views OR input relations
///
/// Returns Ok(()) if valid, Err(DataFusionError) with diagnostic message if invalid.
pub fn validate_graph(spec: &SemanticExecutionSpec) -> Result<()> {
    // Collect all known view names
    let mut view_names: HashSet<&str> = HashSet::new();
    for view in &spec.view_definitions {
        if !view_names.insert(view.name.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate view name: '{}'",
                view.name
            )));
        }
    }

    // Collect all known input relation names
    let input_names: HashSet<&str> = spec
        .input_relations
        .iter()
        .map(|r| r.logical_name.as_str())
        .collect();

    // Validate each view
    for view in &spec.view_definitions {
        // Check view_dependencies resolve to known views
        for dep in &view.view_dependencies {
            if !view_names.contains(dep.as_str()) {
                return Err(DataFusionError::Plan(format!(
                    "View '{}' declares dependency on unknown view '{}'",
                    view.name, dep
                )));
            }
        }

        // Check transform sources resolve to views OR input relations
        let sources = extract_sources(&view.transform);
        for source in sources {
            if !view_names.contains(source) && !input_names.contains(source) {
                return Err(DataFusionError::Plan(format!(
                    "View '{}' references unknown source '{}' (not a view or input relation)",
                    view.name, source
                )));
            }
        }
    }

    // Validate output targets
    let mut output_table_names: HashSet<&str> = HashSet::new();
    for target in &spec.output_targets {
        if !output_table_names.insert(target.table_name.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate output target table_name: '{}'",
                target.table_name
            )));
        }
        if !view_names.contains(target.source_view.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Output target '{}' references unknown source_view '{}'",
                target.table_name, target.source_view
            )));
        }
    }

    // Validate parameter mode exclusivity: typed and template modes are mutually exclusive.
    if !spec.typed_parameters.is_empty() && !spec.parameter_templates.is_empty() {
        return Err(DataFusionError::Plan(
            "Parameter mode conflict: spec contains both typed_parameters and \
             parameter_templates. Use typed_parameters (canonical) or \
             parameter_templates (deprecated transitional), not both."
                .to_string(),
        ));
    }

    // Validate parameter templates (deprecated transitional path)
    let mut template_names: HashSet<&str> = HashSet::new();
    for template in &spec.parameter_templates {
        if !template_names.insert(template.name.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate parameter template name: '{}'",
                template.name
            )));
        }
        if !view_names.contains(template.base_table.as_str())
            && !input_names.contains(template.base_table.as_str())
        {
            return Err(DataFusionError::Plan(format!(
                "Parameter template '{}' references unknown base_table '{}'",
                template.name, template.base_table
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::execution_spec::SemanticExecutionSpec;
    use crate::spec::join_graph::JoinGraph;
    use crate::spec::relations::{InputRelation, SchemaContract, ViewDefinition, ViewTransform};
    use crate::spec::rule_intents::RulepackProfile;
    use std::collections::BTreeMap;

    fn create_test_spec(
        inputs: Vec<InputRelation>,
        views: Vec<ViewDefinition>,
    ) -> SemanticExecutionSpec {
        SemanticExecutionSpec::new(
            1,
            inputs,
            views,
            JoinGraph::default(),
            vec![],
            vec![],
            RulepackProfile::Default,
            vec![],
        )
    }

    fn minimal_schema() -> SchemaContract {
        let mut schema = BTreeMap::new();
        schema.insert("id".to_string(), "Int64".to_string());
        SchemaContract { columns: schema }
    }

    #[test]
    fn test_validate_empty_spec() {
        let spec = create_test_spec(vec![], vec![]);
        assert!(validate_graph(&spec).is_ok());
    }

    #[test]
    fn test_validate_duplicate_view_names() {
        let views = vec![
            ViewDefinition {
                name: "duplicate".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "duplicate".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
        ];

        let spec = create_test_spec(vec![], views);
        let result = validate_graph(&spec);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Duplicate view name"));
    }

    #[test]
    fn test_validate_unknown_view_dependency() {
        let views = vec![ViewDefinition {
            name: "view1".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec!["unknown_view".to_string()],
            transform: ViewTransform::Project {
                source: "input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: minimal_schema(),
        }];

        let spec = create_test_spec(vec![], views);
        let result = validate_graph(&spec);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("dependency on unknown view"));
    }

    #[test]
    fn test_validate_unknown_transform_source() {
        let views = vec![ViewDefinition {
            name: "view1".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "unknown_source".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: minimal_schema(),
        }];

        let spec = create_test_spec(vec![], views);
        let result = validate_graph(&spec);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("references unknown source"));
    }

    #[test]
    fn test_validate_valid_graph_with_input_relations() {
        let inputs = vec![InputRelation {
            logical_name: "input_table".to_string(),
            delta_location: "/path/to/delta".to_string(),
            requires_lineage: false,
            version_pin: None,
        }];

        let views = vec![ViewDefinition {
            name: "view1".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input_table".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: minimal_schema(),
        }];

        let spec = create_test_spec(inputs, views);
        assert!(validate_graph(&spec).is_ok());
    }

    #[test]
    fn test_validate_valid_graph_with_view_chain() {
        let inputs = vec![InputRelation {
            logical_name: "input_table".to_string(),
            delta_location: "/path/to/delta".to_string(),
            requires_lineage: false,
            version_pin: None,
        }];

        let views = vec![
            ViewDefinition {
                name: "view1".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input_table".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "view2".to_string(),
                view_kind: "filter".to_string(),
                view_dependencies: vec!["view1".to_string()],
                transform: ViewTransform::Filter {
                    source: "view1".to_string(),
                    predicate: "id > 0".to_string(),
                },
                output_schema: minimal_schema(),
            },
        ];

        let spec = create_test_spec(inputs, views);
        assert!(validate_graph(&spec).is_ok());
    }

    #[test]
    fn test_validate_relate_sources() {
        let inputs = vec![
            InputRelation {
                logical_name: "left_input".to_string(),
                delta_location: "/path/to/left".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
            InputRelation {
                logical_name: "right_input".to_string(),
                delta_location: "/path/to/right".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
        ];

        let views = vec![ViewDefinition {
            name: "joined".to_string(),
            view_kind: "relate".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Relate {
                left: "left_input".to_string(),
                right: "right_input".to_string(),
                join_type: crate::spec::relations::JoinType::Inner,
                join_keys: vec![],
            },
            output_schema: minimal_schema(),
        }];

        let spec = create_test_spec(inputs, views);
        assert!(validate_graph(&spec).is_ok());
    }

    #[test]
    fn test_validate_union_sources() {
        let inputs = vec![
            InputRelation {
                logical_name: "input1".to_string(),
                delta_location: "/path/to/input1".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
            InputRelation {
                logical_name: "input2".to_string(),
                delta_location: "/path/to/input2".to_string(),
                requires_lineage: false,
                version_pin: None,
            },
        ];

        let views = vec![ViewDefinition {
            name: "unioned".to_string(),
            view_kind: "union".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Union {
                sources: vec!["input1".to_string(), "input2".to_string()],
                discriminator_column: None,
                distinct: false,
            },
            output_schema: minimal_schema(),
        }];

        let spec = create_test_spec(inputs, views);
        assert!(validate_graph(&spec).is_ok());
    }
}
