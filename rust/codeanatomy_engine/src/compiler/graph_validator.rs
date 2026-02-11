//! WS4.5: Graph validation before compilation.
//!
//! Ensures the view dependency graph is well-formed before attempting compilation.

use datafusion_common::{DataFusionError, Result};
use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::providers::registration::TableRegistration;
use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::relations::{ViewDefinition, ViewTransform};

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
        ViewTransform::CpgEmit { sources, .. } => sources.iter().map(|s| s.as_str()).collect(),
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

    Ok(())
}

fn view_index(spec: &SemanticExecutionSpec) -> BTreeMap<&str, &ViewDefinition> {
    spec.view_definitions
        .iter()
        .map(|view| (view.name.as_str(), view))
        .collect()
}

fn resolve_inputs_for_source(
    source: &str,
    input_names: &HashSet<&str>,
    views: &BTreeMap<&str, &ViewDefinition>,
    visiting: &mut HashSet<String>,
    out: &mut BTreeSet<String>,
) {
    if input_names.contains(source) {
        out.insert(source.to_string());
        return;
    }
    let Some(view) = views.get(source) else {
        return;
    };
    if !visiting.insert(view.name.clone()) {
        return;
    }
    match &view.transform {
        ViewTransform::Normalize { source, .. }
        | ViewTransform::Project { source, .. }
        | ViewTransform::Filter { source, .. }
        | ViewTransform::Aggregate { source, .. }
        | ViewTransform::IncrementalCdf { source, .. }
        | ViewTransform::Metadata { source }
        | ViewTransform::FileManifest { source } => {
            resolve_inputs_for_source(source, input_names, views, visiting, out);
        }
        ViewTransform::CpgEmit { sources, .. } => {
            for source in sources {
                resolve_inputs_for_source(source, input_names, views, visiting, out);
            }
        }
        ViewTransform::Relate { left, right, .. } => {
            resolve_inputs_for_source(left, input_names, views, visiting, out);
            resolve_inputs_for_source(right, input_names, views, visiting, out);
        }
        ViewTransform::Union { sources, .. } => {
            for item in sources {
                resolve_inputs_for_source(item, input_names, views, visiting, out);
            }
        }
    }
    visiting.remove(&view.name);
}

/// Validate Delta protocol and column-mapping compatibility for join/union paths.
///
/// Returns non-fatal warning strings when protocol versions/features differ but
/// compatibility remains possible. Hard-incompatible column mapping combinations
/// are returned as planner errors.
pub fn validate_delta_compatibility(
    spec: &SemanticExecutionSpec,
    registrations: &[TableRegistration],
) -> Result<Vec<String>> {
    let registration_by_name: BTreeMap<&str, &TableRegistration> = registrations
        .iter()
        .map(|registration| (registration.name.as_str(), registration))
        .collect();
    let input_names: HashSet<&str> = spec
        .input_relations
        .iter()
        .map(|relation| relation.logical_name.as_str())
        .collect();
    let views = view_index(spec);

    let mut warnings = Vec::new();
    for view in &spec.view_definitions {
        let sources = match &view.transform {
            ViewTransform::Relate { left, right, .. } => vec![left.clone(), right.clone()],
            ViewTransform::Union { sources, .. } if sources.len() > 1 => sources.clone(),
            _ => continue,
        };

        let mut resolved_inputs = BTreeSet::new();
        for source in sources {
            resolve_inputs_for_source(
                &source,
                &input_names,
                &views,
                &mut HashSet::new(),
                &mut resolved_inputs,
            );
        }
        if resolved_inputs.len() < 2 {
            continue;
        }

        let compat = resolved_inputs
            .iter()
            .filter_map(|name| registration_by_name.get(name.as_str()).copied())
            .collect::<Vec<_>>();
        if compat.len() < 2 {
            continue;
        }

        let column_modes: BTreeSet<String> = compat
            .iter()
            .map(|registration| {
                registration
                    .compatibility
                    .column_mapping_mode
                    .clone()
                    .unwrap_or_else(|| "none".to_string())
            })
            .collect();
        if column_modes.len() > 1 {
            return Err(DataFusionError::Plan(format!(
                "Delta column mapping mode mismatch in view '{}': {:?}",
                view.name, column_modes
            )));
        }

        let reader_versions: BTreeSet<i32> = compat
            .iter()
            .map(|registration| registration.compatibility.min_reader_version)
            .collect();
        let writer_versions: BTreeSet<i32> = compat
            .iter()
            .map(|registration| registration.compatibility.min_writer_version)
            .collect();
        if reader_versions.len() > 1 || writer_versions.len() > 1 {
            warnings.push(format!(
                "Delta protocol version drift in view '{}': reader={:?} writer={:?}",
                view.name, reader_versions, writer_versions
            ));
        }
    }

    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::registration::{DeltaCompatibilityFacts, TableRegistration};
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

    #[test]
    fn test_delta_compatibility_rejects_column_mapping_mismatch_for_join() {
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
        let registrations = vec![
            TableRegistration {
                name: "left_input".to_string(),
                delta_version: 1,
                schema_hash: [0u8; 32],
                provider_identity: [1u8; 32],
                capabilities: crate::providers::scan_config::ProviderCapabilities::default(),
                compatibility: DeltaCompatibilityFacts {
                    min_reader_version: 1,
                    min_writer_version: 2,
                    reader_features: vec![],
                    writer_features: vec![],
                    column_mapping_mode: Some("name".to_string()),
                    partition_columns: vec![],
                },
            },
            TableRegistration {
                name: "right_input".to_string(),
                delta_version: 1,
                schema_hash: [0u8; 32],
                provider_identity: [2u8; 32],
                capabilities: crate::providers::scan_config::ProviderCapabilities::default(),
                compatibility: DeltaCompatibilityFacts {
                    min_reader_version: 1,
                    min_writer_version: 2,
                    reader_features: vec![],
                    writer_features: vec![],
                    column_mapping_mode: Some("id".to_string()),
                    partition_columns: vec![],
                },
            },
        ];

        let result = validate_delta_compatibility(&spec, &registrations);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("column mapping mode mismatch"));
    }

    #[test]
    fn test_delta_compatibility_emits_protocol_drift_warning_for_union() {
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
        let registrations = vec![
            TableRegistration {
                name: "input1".to_string(),
                delta_version: 1,
                schema_hash: [0u8; 32],
                provider_identity: [1u8; 32],
                capabilities: crate::providers::scan_config::ProviderCapabilities::default(),
                compatibility: DeltaCompatibilityFacts {
                    min_reader_version: 1,
                    min_writer_version: 2,
                    reader_features: vec![],
                    writer_features: vec![],
                    column_mapping_mode: None,
                    partition_columns: vec![],
                },
            },
            TableRegistration {
                name: "input2".to_string(),
                delta_version: 1,
                schema_hash: [0u8; 32],
                provider_identity: [2u8; 32],
                capabilities: crate::providers::scan_config::ProviderCapabilities::default(),
                compatibility: DeltaCompatibilityFacts {
                    min_reader_version: 3,
                    min_writer_version: 4,
                    reader_features: vec![],
                    writer_features: vec![],
                    column_mapping_mode: None,
                    partition_columns: vec![],
                },
            },
        ];

        let warnings = validate_delta_compatibility(&spec, &registrations).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("protocol version drift"));
    }
}
