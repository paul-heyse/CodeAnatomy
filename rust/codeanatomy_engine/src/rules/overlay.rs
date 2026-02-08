//! Dynamic rule composition via overlay profiles (WS-P4).
//!
//! Promotes rule intents from static-at-build to dynamically composed per plan
//! execution intent. A `RuleOverlayProfile` allows the caller to specify
//! per-execution rule additions, removals, or ordering changes without
//! rebuilding the entire session.
//!
//! Design principle: profile switch, not in-place mutation. The overlay
//! produces a NEW `SessionState` via `SessionStateBuilder` -- it never
//! mutates the existing session's rules in place.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::prelude::*;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

use crate::rules::registry::{compute_ruleset_fingerprint, CpgRuleSet};
use crate::rules::rulepack::RulepackFactory;
use crate::session::profiles::EnvironmentProfile;
use crate::spec::rule_intents::{RuleIntent, RulepackProfile};

/// Per-execution rule overlay specification.
///
/// Allows callers to customize rules for a specific compilation without
/// rebuilding the session from scratch.
///
/// The overlay produces a NEW `SessionState` via `SessionStateBuilder` --
/// it never mutates the existing session's rules in place.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleOverlayProfile {
    /// Additional rule intents to include (appended after base rules).
    pub additional_intents: Vec<RuleIntent>,

    /// Rule names to exclude from the base ruleset.
    /// Exclusion happens at build time, not via post-build removal.
    pub exclude_rules: Vec<String>,

    /// Explicit rule ordering overrides.
    /// Rules not listed retain their default order.
    pub priority_overrides: Vec<RulePriority>,

    /// If true, capture per-rule EXPLAIN deltas showing each rule's effect.
    pub explain_per_rule: bool,
}

/// Explicit priority assignment for a rule.
///
/// Lower priority values run earlier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RulePriority {
    pub rule_name: String,
    pub priority: i32,
}

/// Captured delta for a single rule's effect on the plan.
///
/// Parsed from EXPLAIN VERBOSE output to avoid direct per-rule rewrite
/// loops that can diverge from the actual optimizer pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDelta {
    pub rule_name: String,
    pub changed: bool,
    pub plan_after: Option<String>,
}

/// Build a new `CpgRuleSet` from a base profile + overlay.
///
/// Returns a new immutable ruleset. The base session is NOT mutated.
///
/// Process:
/// 1. Clone base intents, extend with overlay.additional_intents
/// 2. Build combined ruleset via `RulepackFactory::build_ruleset()`
/// 3. Filter out excluded rules using `retain()`
/// 4. Apply priority ordering if present
/// 5. Recompute fingerprint
///
/// # Arguments
///
/// * `base_profile` - Base rulepack profile controlling rule selection
/// * `base_intents` - Base rule intents from execution spec
/// * `overlay` - Per-execution overlay specification
/// * `env_profile` - Environment profile for tuning parameters
///
/// # Returns
///
/// New `CpgRuleSet` with overlay applied and recomputed fingerprint
pub fn build_overlaid_ruleset(
    base_profile: &RulepackProfile,
    base_intents: &[RuleIntent],
    overlay: &RuleOverlayProfile,
    env_profile: &EnvironmentProfile,
) -> CpgRuleSet {
    // 1. Start with base intents, extend with overlay additions
    let mut combined_intents: Vec<RuleIntent> = base_intents.to_vec();
    combined_intents.extend(overlay.additional_intents.clone());

    // 2. Build the combined ruleset from factory
    let mut ruleset = RulepackFactory::build_ruleset(base_profile, &combined_intents, env_profile);

    // 3. Apply exclusions (filter at build time, not post-build removal)
    if !overlay.exclude_rules.is_empty() {
        let exclude_set: HashSet<&str> =
            overlay.exclude_rules.iter().map(|s| s.as_str()).collect();
        ruleset
            .analyzer_rules
            .retain(|r| !exclude_set.contains(r.name()));
        ruleset
            .optimizer_rules
            .retain(|r| !exclude_set.contains(r.name()));
        ruleset
            .physical_rules
            .retain(|r| !exclude_set.contains(r.name()));
    }

    // 4. Apply priority ordering
    if !overlay.priority_overrides.is_empty() {
        apply_priority_ordering(&mut ruleset, &overlay.priority_overrides);
    }

    // 5. Recompute fingerprint (overlay changes the identity)
    ruleset.fingerprint = compute_ruleset_fingerprint(
        &ruleset.analyzer_rules,
        &ruleset.optimizer_rules,
        &ruleset.physical_rules,
    );

    ruleset
}

/// Build a new `SessionContext` with the overlaid rules.
///
/// Creates a fresh `SessionState` from the existing state, replacing only
/// rule vectors. This avoids manual registration-transfer drift: all
/// registered tables, UDFs, and catalog entries are inherited via the
/// cloned base state.
///
/// # Arguments
///
/// * `base_ctx` - Base session context to clone state from
/// * `overlaid_ruleset` - Ruleset with overlay applied
///
/// # Returns
///
/// New `SessionContext` with overlaid rules, inheriting all registrations
pub async fn build_overlaid_session(
    base_ctx: &SessionContext,
    overlaid_ruleset: &CpgRuleSet,
) -> Result<SessionContext> {
    let state = base_ctx.state();

    let new_state = SessionStateBuilder::new_from_existing(state.clone())
        .with_analyzer_rules(overlaid_ruleset.analyzer_rules.clone())
        .with_optimizer_rules(overlaid_ruleset.optimizer_rules.clone())
        .with_physical_optimizer_rules(overlaid_ruleset.physical_rules.clone())
        .build();

    Ok(SessionContext::new_with_state(new_state))
}

/// Capture rule effects from EXPLAIN VERBOSE output.
///
/// This path avoids direct per-rule rewrite loops that can diverge from the
/// actual optimizer pipeline. Rule names and plan transitions are parsed from
/// DataFusion-generated explain rows.
///
/// # Arguments
///
/// * `df` - DataFrame to explain
///
/// # Returns
///
/// Vector of `RuleDelta` showing each rule's effect on the plan
pub async fn capture_per_rule_deltas(df: &DataFrame) -> Result<Vec<RuleDelta>> {
    let batches = df.clone().explain(true, false)?.collect().await?;
    parse_rule_deltas_from_explain_verbose(&batches)
}

/// Apply priority ordering to a mutable `CpgRuleSet`.
///
/// Rules with explicit priority overrides are sorted by priority (lower = earlier).
/// Rules without explicit overrides retain their default relative order, placed
/// after all explicitly-prioritized rules.
fn apply_priority_ordering(ruleset: &mut CpgRuleSet, overrides: &[RulePriority]) {
    let priority_map: std::collections::HashMap<&str, i32> = overrides
        .iter()
        .map(|rp| (rp.rule_name.as_str(), rp.priority))
        .collect();

    // The default priority for rules not in the override map is i32::MAX,
    // so they sort to the end and preserve their relative order.
    let sort_fn = |rules: &mut Vec<Arc<dyn AnalyzerRule + Send + Sync>>| {
        rules.sort_by_key(|r| *priority_map.get(r.name()).unwrap_or(&i32::MAX));
    };
    sort_fn(&mut ruleset.analyzer_rules);

    // Sort optimizer rules
    let sort_opt_fn = |rules: &mut Vec<Arc<dyn OptimizerRule + Send + Sync>>| {
        rules.sort_by_key(|r| *priority_map.get(r.name()).unwrap_or(&i32::MAX));
    };
    sort_opt_fn(&mut ruleset.optimizer_rules);

    // Sort physical optimizer rules
    let sort_phys_fn = |rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>| {
        rules.sort_by_key(|r| *priority_map.get(r.name()).unwrap_or(&i32::MAX));
    };
    sort_phys_fn(&mut ruleset.physical_rules);
}

/// Compute a BLAKE3 fingerprint from rule names in deterministic order.
///
/// Re-exported from registry for overlay callers. The overlay module delegates
/// to `registry::compute_ruleset_fingerprint` directly.
///
/// This function exists as a public facade for external callers that need
/// to compute a fingerprint without going through `CpgRuleSet::new()`.
pub fn compute_overlay_fingerprint(
    analyzer_rules: &[Arc<dyn AnalyzerRule + Send + Sync>],
    optimizer_rules: &[Arc<dyn OptimizerRule + Send + Sync>],
    physical_rules: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>],
) -> [u8; 32] {
    compute_ruleset_fingerprint(analyzer_rules, optimizer_rules, physical_rules)
}

/// Parse rule deltas from EXPLAIN VERBOSE record batches.
///
/// DataFusion EXPLAIN VERBOSE output has two columns: "plan_type" and "plan".
/// Optimizer rule entries have plan_type patterns like "logical_plan after <rule_name>"
/// or "physical_plan after <rule_name>".
///
/// We track plan text transitions: if the plan text differs from the previous
/// step's text, the rule is marked as having `changed = true`.
fn parse_rule_deltas_from_explain_verbose(
    batches: &[datafusion::arrow::array::RecordBatch],
) -> Result<Vec<RuleDelta>> {
    let mut deltas = Vec::new();
    let mut prev_plan: Option<String> = None;

    for batch in batches {
        let plan_type_col = batch
            .column_by_name("plan_type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let plan_col = batch
            .column_by_name("plan")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let (Some(plan_types), Some(plans)) = (plan_type_col, plan_col) else {
            continue;
        };

        for row_idx in 0..batch.num_rows() {
            let Some(plan_type) = plan_types.value(row_idx).strip_prefix("") else {
                continue;
            };

            // Look for "after <rule_name>" pattern in plan_type
            let rule_name = if let Some(after_part) = plan_type.strip_prefix("logical_plan after ")
            {
                after_part.to_string()
            } else if let Some(after_part) =
                plan_type.strip_prefix("physical_plan after ")
            {
                after_part.to_string()
            } else if let Some(after_part) =
                plan_type.strip_prefix("optimized_logical_plan after ")
            {
                after_part.to_string()
            } else if let Some(after_part) =
                plan_type.strip_prefix("physical_plan_with_stats after ")
            {
                after_part.to_string()
            } else {
                // Not a rule transition row -- update prev_plan for context
                let plan_text = plans.value(row_idx).to_string();
                prev_plan = Some(plan_text);
                continue;
            };

            let plan_text = plans.value(row_idx).to_string();
            let changed = prev_plan
                .as_ref()
                .map(|prev| prev != &plan_text)
                .unwrap_or(true);

            deltas.push(RuleDelta {
                rule_name,
                changed,
                plan_after: Some(plan_text.clone()),
            });

            prev_plan = Some(plan_text);
        }
    }

    Ok(deltas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::registry::CpgRuleSet;
    use crate::session::profiles::{EnvironmentClass, EnvironmentProfile};
    use crate::spec::rule_intents::{RuleClass, RuleIntent, RulepackProfile};

    fn test_env_profile() -> EnvironmentProfile {
        EnvironmentProfile::from_class(EnvironmentClass::Small)
    }

    fn make_intent(name: &str, class: RuleClass) -> RuleIntent {
        RuleIntent {
            name: name.to_string(),
            class,
            params: serde_json::Value::Null,
        }
    }

    #[test]
    fn test_overlay_adds_intents() {
        let base_intents = vec![make_intent(
            "semantic_integrity",
            RuleClass::SemanticIntegrity,
        )];
        let overlay = RuleOverlayProfile {
            additional_intents: vec![make_intent("delta_scan_aware", RuleClass::DeltaScanAware)],
            exclude_rules: vec![],
            priority_overrides: vec![],
            explain_per_rule: false,
        };

        let base_ruleset = RulepackFactory::build_ruleset(
            &RulepackProfile::Default,
            &base_intents,
            &test_env_profile(),
        );
        let overlaid = build_overlaid_ruleset(
            &RulepackProfile::Default,
            &base_intents,
            &overlay,
            &test_env_profile(),
        );

        // Overlaid should have more rules than base
        assert!(overlaid.total_count() >= base_ruleset.total_count());
        // Should contain delta_scan_aware optimizer rule
        assert!(overlaid
            .optimizer_rules
            .iter()
            .any(|r| r.name() == "delta_scan_aware"));
    }

    #[test]
    fn test_overlay_excludes_rules() {
        let base_intents = vec![
            make_intent("semantic_integrity", RuleClass::SemanticIntegrity),
            make_intent("delta_scan_aware", RuleClass::DeltaScanAware),
        ];
        let overlay = RuleOverlayProfile {
            additional_intents: vec![],
            exclude_rules: vec!["delta_scan_aware".to_string()],
            priority_overrides: vec![],
            explain_per_rule: false,
        };

        let overlaid = build_overlaid_ruleset(
            &RulepackProfile::Default,
            &base_intents,
            &overlay,
            &test_env_profile(),
        );

        // Should NOT contain delta_scan_aware
        assert!(!overlaid
            .optimizer_rules
            .iter()
            .any(|r| r.name() == "delta_scan_aware"));
    }

    #[test]
    fn test_overlay_changes_fingerprint() {
        let base_intents = vec![make_intent(
            "semantic_integrity",
            RuleClass::SemanticIntegrity,
        )];
        let base_ruleset = RulepackFactory::build_ruleset(
            &RulepackProfile::Default,
            &base_intents,
            &test_env_profile(),
        );

        let overlay = RuleOverlayProfile {
            additional_intents: vec![make_intent("delta_scan_aware", RuleClass::DeltaScanAware)],
            exclude_rules: vec![],
            priority_overrides: vec![],
            explain_per_rule: false,
        };
        let overlaid = build_overlaid_ruleset(
            &RulepackProfile::Default,
            &base_intents,
            &overlay,
            &test_env_profile(),
        );

        // Fingerprint must differ when overlay adds rules
        assert_ne!(base_ruleset.fingerprint, overlaid.fingerprint);
    }

    #[test]
    fn test_empty_overlay_preserves_fingerprint() {
        let base_intents = vec![make_intent(
            "semantic_integrity",
            RuleClass::SemanticIntegrity,
        )];
        let base_ruleset = RulepackFactory::build_ruleset(
            &RulepackProfile::Default,
            &base_intents,
            &test_env_profile(),
        );

        let overlay = RuleOverlayProfile {
            additional_intents: vec![],
            exclude_rules: vec![],
            priority_overrides: vec![],
            explain_per_rule: false,
        };
        let overlaid = build_overlaid_ruleset(
            &RulepackProfile::Default,
            &base_intents,
            &overlay,
            &test_env_profile(),
        );

        // Empty overlay should produce same fingerprint
        assert_eq!(base_ruleset.fingerprint, overlaid.fingerprint);
    }

    #[tokio::test]
    async fn test_build_overlaid_session_inherits_registrations() {
        let base_ctx = SessionContext::new();

        // Register a table in the base context
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        base_ctx
            .register_table("test_table", Arc::new(table))
            .unwrap();

        let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);

        let new_ctx = build_overlaid_session(&base_ctx, &ruleset).await.unwrap();

        // New context should inherit the registered table
        let result = new_ctx.table("test_table").await;
        assert!(result.is_ok(), "Overlaid session should inherit registered tables");
    }

    #[tokio::test]
    async fn test_base_session_unmodified_after_overlay() {
        let base_ctx = SessionContext::new();

        // Get initial state fingerprint
        let initial_state = base_ctx.state();
        let initial_analyzer_count = initial_state.analyzer().rules.len();

        let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);
        let _new_ctx = build_overlaid_session(&base_ctx, &ruleset).await.unwrap();

        // Base context should be unmodified
        let after_state = base_ctx.state();
        assert_eq!(
            initial_analyzer_count,
            after_state.analyzer().rules.len(),
            "Base session must not be modified by overlay"
        );
    }

    #[test]
    fn test_overlay_profile_serialization_roundtrip() {
        let overlay = RuleOverlayProfile {
            additional_intents: vec![make_intent(
                "semantic_integrity",
                RuleClass::SemanticIntegrity,
            )],
            exclude_rules: vec!["cost_shape_rule".to_string()],
            priority_overrides: vec![RulePriority {
                rule_name: "semantic_integrity".to_string(),
                priority: 1,
            }],
            explain_per_rule: true,
        };

        let json = serde_json::to_string(&overlay).unwrap();
        let deserialized: RuleOverlayProfile = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.additional_intents.len(), 1);
        assert_eq!(deserialized.exclude_rules.len(), 1);
        assert_eq!(deserialized.priority_overrides.len(), 1);
        assert!(deserialized.explain_per_rule);
    }

    #[test]
    fn test_rule_delta_serialization() {
        let delta = RuleDelta {
            rule_name: "test_rule".to_string(),
            changed: true,
            plan_after: Some("Projection: ...".to_string()),
        };

        let json = serde_json::to_string(&delta).unwrap();
        let deserialized: RuleDelta = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.rule_name, "test_rule");
        assert!(deserialized.changed);
        assert!(deserialized.plan_after.is_some());
    }

    #[test]
    fn test_parse_rule_deltas_empty() {
        let deltas = parse_rule_deltas_from_explain_verbose(&[]).unwrap();
        assert!(deltas.is_empty());
    }

    #[tokio::test]
    async fn test_capture_per_rule_deltas_from_simple_query() {
        let ctx = SessionContext::new();

        // Register a simple table
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("test_table", Arc::new(table)).unwrap();

        let df = ctx.sql("SELECT id FROM test_table WHERE id > 1").await.unwrap();
        let deltas = capture_per_rule_deltas(&df).await.unwrap();

        // Should produce some deltas (at least optimizer rules fire)
        // The exact count depends on DataFusion's default optimizer rules
        // but it should be non-empty for a filtered query
        assert!(
            !deltas.is_empty(),
            "EXPLAIN VERBOSE should produce rule deltas for a filtered query"
        );
    }

    #[test]
    fn test_apply_priority_ordering_basic() {
        let mut ruleset = CpgRuleSet::new(vec![], vec![], vec![]);

        // With no rules, priority ordering should be a no-op
        let overrides = vec![RulePriority {
            rule_name: "nonexistent".to_string(),
            priority: 1,
        }];
        apply_priority_ordering(&mut ruleset, &overrides);
        assert_eq!(ruleset.total_count(), 0);
    }

    #[test]
    fn test_compute_overlay_fingerprint_matches_registry() {
        let fp1 = compute_overlay_fingerprint(&[], &[], &[]);
        let fp2 = compute_ruleset_fingerprint(&[], &[], &[]);
        assert_eq!(fp1, fp2, "Overlay fingerprint must match registry fingerprint");
    }
}
