//! Cost-aware cache boundary insertion.
//!
//! Replaces fixed fanout >= 3 threshold with cost-aware heuristics:
//! - fanout >= 3: always cache
//! - fanout == 2 AND (join OR aggregate): cache (expensive to recompute)
//! - fanout == 2 AND simple (filter/project): skip (cheap)
//! - fanout == 1: never cache

use datafusion::prelude::*;
use datafusion_common::Result;
use std::collections::HashMap;

use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::relations::ViewTransform;

/// Compute fanout (downstream reference count) for each view.
fn compute_fanout(spec: &SemanticExecutionSpec) -> HashMap<String, usize> {
    let mut fanout: HashMap<String, usize> = HashMap::new();

    // Initialize all views with zero fanout
    for view in &spec.view_definitions {
        fanout.insert(view.name.clone(), 0);
    }

    // Count references from view dependencies
    for view in &spec.view_definitions {
        for dep in &view.view_dependencies {
            *fanout.entry(dep.clone()).or_insert(0) += 1;
        }
    }

    // Count references from output targets
    for target in &spec.output_targets {
        *fanout.entry(target.source_view.clone()).or_insert(0) += 1;
    }

    fanout
}

/// Check if a transform is expensive (join or aggregate).
fn is_expensive_transform(transform: &ViewTransform) -> bool {
    matches!(transform, ViewTransform::Relate { .. } | ViewTransform::Aggregate { .. })
}

/// Determine if a view should be cached based on fanout and transform cost.
///
/// Decision logic:
/// - fanout >= 3: always cache
/// - fanout == 2 AND expensive (join/aggregate): cache
/// - fanout == 2 AND simple (filter/project/normalize/union): skip
/// - fanout <= 1: never cache
fn should_cache(fanout: usize, transform: &ViewTransform) -> bool {
    match fanout {
        0 | 1 => false,
        2 => is_expensive_transform(transform),
        _ => true, // fanout >= 3
    }
}

/// Insert cache boundaries for views based on fanout and cost.
///
/// For each view that should be cached:
/// 1. Materialize via df.cache().await?
/// 2. Deregister original view
/// 3. Re-register cached DataFrame as a new view
///
/// Returns count of views cached.
pub async fn insert_cache_boundaries(
    ctx: &SessionContext,
    spec: &SemanticExecutionSpec,
) -> Result<usize> {
    let fanout = compute_fanout(spec);
    let mut cached_count = 0;

    for view in &spec.view_definitions {
        let view_fanout = fanout.get(&view.name).copied().unwrap_or(0);

        if should_cache(view_fanout, &view.transform) {
            // Get current DataFrame
            let df = ctx.table(&view.name).await?;

            // Materialize it
            let cached = df.cache().await?;

            // Deregister original
            ctx.deregister_table(&view.name)?;

            // Re-register as cached view
            let cached_view = cached.into_view();
            ctx.register_table(&view.name, cached_view)?;

            cached_count += 1;
        }
    }

    Ok(cached_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::execution_spec::SemanticExecutionSpec;
    use crate::spec::join_graph::JoinGraph;
    use crate::spec::outputs::{MaterializationMode, OutputTarget};
    use crate::spec::relations::{JoinType, SchemaContract, ViewDefinition, ViewTransform};
    use crate::spec::rule_intents::RulepackProfile;
    use std::collections::BTreeMap;

    fn minimal_schema() -> SchemaContract {
        let mut schema = BTreeMap::new();
        schema.insert("id".to_string(), "Int64".to_string());
        SchemaContract { columns: schema }
    }

    #[test]
    fn test_compute_fanout_no_dependencies() {
        let views = vec![
            ViewDefinition {
                name: "view1".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "view2".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
        ];

        let spec = SemanticExecutionSpec::new(
            1,
            vec![],
            views,
            JoinGraph::default(),
            vec![],
            vec![],
            RulepackProfile::Default,
            vec![],
        );

        let fanout = compute_fanout(&spec);
        assert_eq!(fanout.get("view1"), Some(&0));
        assert_eq!(fanout.get("view2"), Some(&0));
    }

    #[test]
    fn test_compute_fanout_with_dependencies() {
        let views = vec![
            ViewDefinition {
                name: "base".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "derived1".to_string(),
                view_kind: "filter".to_string(),
                view_dependencies: vec!["base".to_string()],
                transform: ViewTransform::Filter {
                    source: "base".to_string(),
                    predicate: "id > 0".to_string(),
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "derived2".to_string(),
                view_kind: "filter".to_string(),
                view_dependencies: vec!["base".to_string()],
                transform: ViewTransform::Filter {
                    source: "base".to_string(),
                    predicate: "id < 100".to_string(),
                },
                output_schema: minimal_schema(),
            },
        ];

        let spec = SemanticExecutionSpec::new(
            1,
            vec![],
            views,
            JoinGraph::default(),
            vec![],
            vec![],
            RulepackProfile::Default,
            vec![],
        );

        let fanout = compute_fanout(&spec);
        assert_eq!(fanout.get("base"), Some(&2)); // Referenced by derived1 and derived2
        assert_eq!(fanout.get("derived1"), Some(&0));
        assert_eq!(fanout.get("derived2"), Some(&0));
    }

    #[test]
    fn test_compute_fanout_with_outputs() {
        let views = vec![ViewDefinition {
            name: "view1".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: minimal_schema(),
        }];

        let outputs = vec![OutputTarget {
            table_name: "output1".to_string(),
            delta_location: None,
            source_view: "view1".to_string(),
            columns: vec!["id".to_string()],
            materialization_mode: MaterializationMode::Overwrite,
        }];

        let spec = SemanticExecutionSpec::new(
            1,
            vec![],
            views,
            JoinGraph::default(),
            outputs,
            vec![],
            RulepackProfile::Default,
            vec![],
        );

        let fanout = compute_fanout(&spec);
        assert_eq!(fanout.get("view1"), Some(&1)); // Referenced by output1
    }

    #[test]
    fn test_is_expensive_transform() {
        // Expensive transforms
        assert!(is_expensive_transform(&ViewTransform::Relate {
            left: "l".to_string(),
            right: "r".to_string(),
            join_type: JoinType::Inner,
            join_keys: vec![],
        }));

        assert!(is_expensive_transform(&ViewTransform::Aggregate {
            source: "s".to_string(),
            group_by: vec![],
            aggregations: vec![],
        }));

        // Cheap transforms
        assert!(!is_expensive_transform(&ViewTransform::Project {
            source: "s".to_string(),
            columns: vec![],
        }));

        assert!(!is_expensive_transform(&ViewTransform::Filter {
            source: "s".to_string(),
            predicate: "true".to_string(),
        }));

        assert!(!is_expensive_transform(&ViewTransform::Normalize {
            source: "s".to_string(),
            id_columns: vec![],
            span_columns: None,
            text_columns: vec![],
        }));

        assert!(!is_expensive_transform(&ViewTransform::Union {
            sources: vec![],
            discriminator_column: None,
            distinct: false,
        }));
    }

    #[test]
    fn test_should_cache_fanout_zero_or_one() {
        let transform = ViewTransform::Project {
            source: "s".to_string(),
            columns: vec![],
        };

        assert!(!should_cache(0, &transform));
        assert!(!should_cache(1, &transform));
    }

    #[test]
    fn test_should_cache_fanout_two_simple() {
        let transform = ViewTransform::Project {
            source: "s".to_string(),
            columns: vec![],
        };

        assert!(!should_cache(2, &transform)); // Simple transform, skip caching
    }

    #[test]
    fn test_should_cache_fanout_two_expensive() {
        let transform = ViewTransform::Relate {
            left: "l".to_string(),
            right: "r".to_string(),
            join_type: JoinType::Inner,
            join_keys: vec![],
        };

        assert!(should_cache(2, &transform)); // Expensive transform, should cache
    }

    #[test]
    fn test_should_cache_fanout_three_or_more() {
        let simple_transform = ViewTransform::Project {
            source: "s".to_string(),
            columns: vec![],
        };

        let expensive_transform = ViewTransform::Relate {
            left: "l".to_string(),
            right: "r".to_string(),
            join_type: JoinType::Inner,
            join_keys: vec![],
        };

        assert!(should_cache(3, &simple_transform)); // Always cache fanout >= 3
        assert!(should_cache(3, &expensive_transform));
        assert!(should_cache(10, &simple_transform));
    }
}
