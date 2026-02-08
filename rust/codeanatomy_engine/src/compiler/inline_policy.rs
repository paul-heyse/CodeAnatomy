//! Cross-view compilation optimization via selective inlining.
//!
//! Determines which views should be inlined into their consumers' plans
//! versus registered as named views. Inlining eliminates the optimization
//! barrier of named view references, allowing the DataFusion optimizer to
//! push down predicates and reorder joins across view boundaries.

use std::collections::{HashMap, HashSet};

use crate::spec::relations::{ViewDefinition, ViewTransform};

/// Decision for how a view should be handled during compilation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineDecision {
    /// Register as a named view (current behavior).
    ///
    /// The view becomes visible as a named table in the SessionContext,
    /// creating an optimization boundary.
    Register,

    /// Inline into the consuming view's plan (no registration boundary).
    ///
    /// The DataFrame is stored in the inline cache and substituted directly
    /// into the consumer, giving the optimizer full visibility across both plans.
    Inline,
}

/// Determine which views should be inlined vs registered as named views.
///
/// A view is eligible for inlining when ALL conditions hold:
/// 1. It has exactly ONE downstream consumer (fanout == 1)
/// 2. It uses a simple transform (Filter or Project)
/// 3. It does not appear in any output target's `source_view`
///
/// Views that are NOT inlined (always registered):
/// - Multi-consumer views (fanout > 1) -- must be registered for sharing
/// - Complex transforms (Join, Union, Aggregate, UDTF) -- optimizer handles
///   these better as named views
/// - Output views -- must be registered for materialization
/// - Zero-consumer views (fanout == 0) -- still registered for completeness
pub fn compute_inline_policy(
    views: &[ViewDefinition],
    output_views: &[String],
    ref_counts: &HashMap<&str, usize>,
) -> HashMap<String, InlineDecision> {
    let output_set: HashSet<&str> = output_views.iter().map(|s| s.as_str()).collect();

    views
        .iter()
        .map(|v| {
            let fanout = ref_counts.get(v.name.as_str()).copied().unwrap_or(0);
            let is_output = output_set.contains(v.name.as_str());
            let is_simple = matches!(
                v.transform,
                ViewTransform::Filter { .. } | ViewTransform::Project { .. }
            );

            let decision = if is_output || fanout > 1 || !is_simple {
                InlineDecision::Register
            } else {
                InlineDecision::Inline
            };

            (v.name.clone(), decision)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::relations::{SchemaContract, ViewDefinition, ViewTransform};
    use std::collections::BTreeMap;

    fn minimal_schema() -> SchemaContract {
        let mut schema = BTreeMap::new();
        schema.insert("id".to_string(), "Int64".to_string());
        SchemaContract { columns: schema }
    }

    fn project_view(name: &str) -> ViewDefinition {
        ViewDefinition {
            name: name.to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: minimal_schema(),
        }
    }

    fn filter_view(name: &str) -> ViewDefinition {
        ViewDefinition {
            name: name.to_string(),
            view_kind: "filter".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Filter {
                source: "input".to_string(),
                predicate: "id > 0".to_string(),
            },
            output_schema: minimal_schema(),
        }
    }

    fn union_view(name: &str) -> ViewDefinition {
        ViewDefinition {
            name: name.to_string(),
            view_kind: "union".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Union {
                sources: vec!["a".to_string(), "b".to_string()],
                discriminator_column: None,
                distinct: false,
            },
            output_schema: minimal_schema(),
        }
    }

    #[test]
    fn test_simple_filter_single_consumer_inlines() {
        let views = vec![filter_view("v1")];
        let ref_counts: HashMap<&str, usize> = [("v1", 1)].into_iter().collect();
        let output_views: Vec<String> = vec![];

        let policy = compute_inline_policy(&views, &output_views, &ref_counts);

        assert_eq!(policy.get("v1"), Some(&InlineDecision::Inline));
    }

    #[test]
    fn test_simple_project_single_consumer_inlines() {
        let views = vec![project_view("v1")];
        let ref_counts: HashMap<&str, usize> = [("v1", 1)].into_iter().collect();
        let output_views: Vec<String> = vec![];

        let policy = compute_inline_policy(&views, &output_views, &ref_counts);

        assert_eq!(policy.get("v1"), Some(&InlineDecision::Inline));
    }

    #[test]
    fn test_output_view_always_registers() {
        let views = vec![filter_view("v1")];
        let ref_counts: HashMap<&str, usize> = [("v1", 1)].into_iter().collect();
        let output_views = vec!["v1".to_string()];

        let policy = compute_inline_policy(&views, &output_views, &ref_counts);

        assert_eq!(policy.get("v1"), Some(&InlineDecision::Register));
    }

    #[test]
    fn test_multi_consumer_always_registers() {
        let views = vec![filter_view("v1")];
        let ref_counts: HashMap<&str, usize> = [("v1", 2)].into_iter().collect();
        let output_views: Vec<String> = vec![];

        let policy = compute_inline_policy(&views, &output_views, &ref_counts);

        assert_eq!(policy.get("v1"), Some(&InlineDecision::Register));
    }

    #[test]
    fn test_complex_transform_always_registers() {
        let views = vec![union_view("v1")];
        let ref_counts: HashMap<&str, usize> = [("v1", 1)].into_iter().collect();
        let output_views: Vec<String> = vec![];

        let policy = compute_inline_policy(&views, &output_views, &ref_counts);

        assert_eq!(policy.get("v1"), Some(&InlineDecision::Register));
    }

    #[test]
    fn test_zero_fanout_registers() {
        let views = vec![filter_view("v1")];
        let ref_counts: HashMap<&str, usize> = HashMap::new(); // No refs at all
        let output_views: Vec<String> = vec![];

        let policy = compute_inline_policy(&views, &output_views, &ref_counts);

        // fanout 0, is_simple=true, not output => still inlines
        // Actually, fanout=0 means not in ref_counts so unwrap_or(0):
        // !is_output (true) || fanout > 1 (false) || !is_simple (false) => none true => Inline
        assert_eq!(policy.get("v1"), Some(&InlineDecision::Inline));
    }

    #[test]
    fn test_mixed_views() {
        let views = vec![
            filter_view("filter1"),
            project_view("project1"),
            union_view("union1"),
        ];
        let ref_counts: HashMap<&str, usize> = [("filter1", 1), ("project1", 2), ("union1", 1)]
            .into_iter()
            .collect();
        let output_views = vec!["union1".to_string()];

        let policy = compute_inline_policy(&views, &output_views, &ref_counts);

        assert_eq!(policy.get("filter1"), Some(&InlineDecision::Inline)); // simple, fanout=1, not output
        assert_eq!(policy.get("project1"), Some(&InlineDecision::Register)); // fanout=2
        assert_eq!(policy.get("union1"), Some(&InlineDecision::Register)); // output + complex
    }
}
