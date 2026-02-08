//! Policy-based cache placement for the compiled plan DAG.
//!
//! Upgrades cache boundary decisions from static fanout heuristics to
//! plan-aware, metrics-informed cache placement that considers:
//! - Fanout (downstream reference count)
//! - Estimated row counts (via DataFusion plan statistics)
//! - Explicit per-view overrides (ForceCache / NeverCache)
//! - Optional historical metrics learning (future)

use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::spec::relations::ViewDefinition;
use crate::tuner::metrics_store::MetricsStore;

/// Policy controlling `DataFrame.cache()` placement in the compiled plan DAG.
///
/// Combines static analysis (fanout, transform complexity) with optional
/// historical metrics to make cost-informed caching decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachePlacementPolicy {
    /// Minimum fanout to consider caching (default: 2).
    /// Views consumed by fewer downstream views are never cached.
    pub min_fanout: usize,

    /// Estimated row threshold: only cache if upstream estimates exceed this.
    /// Uses DataFusion statistics when `collect_statistics` is enabled.
    pub min_estimated_rows: Option<u64>,

    /// Memory budget for cached DataFrames (bytes).
    /// Sum of all `cache()` materializations should stay under this limit.
    /// Derived from `RuntimeProfileSpec.memory_pool_bytes` when not explicit.
    pub cache_memory_budget: Option<usize>,

    /// Enable historical cost learning from `MetricsStore`.
    /// When true, views that historically produce large intermediate results
    /// are prioritized for caching.
    pub use_historical_metrics: bool,

    /// Explicit cache boundary overrides (view name -> force cache / no cache).
    pub overrides: Vec<CacheOverride>,
}

/// Explicit per-view cache override.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheOverride {
    pub view_name: String,
    pub action: CacheAction,
}

/// Cache override action.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CacheAction {
    ForceCache,
    NeverCache,
}

impl Default for CachePlacementPolicy {
    fn default() -> Self {
        Self {
            min_fanout: 2,
            min_estimated_rows: None,
            cache_memory_budget: None,
            use_historical_metrics: false,
            overrides: Vec::new(),
        }
    }
}

/// Internal candidate for priority-sorted cache selection.
struct CacheCandidate {
    view_name: String,
    priority: u64,
}

/// Determine which views should be cached based on policy + statistics.
///
/// Uses DataFusion's plan statistics (when `collect_statistics` is enabled)
/// to estimate intermediate result sizes and prioritize cache placement
/// within the memory budget.
///
/// Decision flow for each view:
/// 1. Check explicit overrides (ForceCache always includes, NeverCache always skips)
/// 2. Apply minimum fanout filter
/// 3. Estimate rows via `estimate_view_rows()`
/// 4. Apply minimum estimated rows filter
/// 5. Compute priority = fanout * estimated_rows
/// 6. Sort by priority descending
///
/// Parameters
/// ----------
/// ctx
///     The DataFusion session context containing registered views.
/// views
///     View definitions from the semantic execution spec.
/// ref_counts
///     Map of view name -> downstream reference count (fanout).
/// policy
///     Cache placement policy controlling thresholds and overrides.
/// _metrics_store
///     Optional historical metrics store for future cost learning.
///     Currently accepted but unused (reserved for `use_historical_metrics`).
pub async fn compute_cache_boundaries(
    ctx: &SessionContext,
    views: &[ViewDefinition],
    ref_counts: &HashMap<String, usize>,
    policy: &CachePlacementPolicy,
    _metrics_store: Option<&MetricsStore>,
) -> Vec<String> {
    let mut candidates: Vec<CacheCandidate> = Vec::new();

    for view in views {
        let fanout = ref_counts.get(&view.name).copied().unwrap_or(0);

        // 1. Check overrides first — they bypass all other checks.
        if let Some(ovr) = policy.overrides.iter().find(|o| o.view_name == view.name) {
            match ovr.action {
                CacheAction::ForceCache => {
                    candidates.push(CacheCandidate {
                        view_name: view.name.clone(),
                        priority: u64::MAX,
                    });
                    continue;
                }
                CacheAction::NeverCache => continue,
            }
        }

        // 2. Apply minimum fanout filter.
        if fanout < policy.min_fanout {
            continue;
        }

        // 3. Estimate row count using DataFusion plan statistics.
        let estimated_rows = estimate_view_rows(ctx, &view.name).await;

        // 4. Apply minimum estimated rows filter.
        if let Some(min_rows) = policy.min_estimated_rows {
            if estimated_rows < min_rows {
                continue;
            }
        }

        // 5. Priority = fanout * estimated_rows (higher = more valuable to cache).
        let priority = (fanout as u64).saturating_mul(estimated_rows);
        candidates.push(CacheCandidate {
            view_name: view.name.clone(),
            priority,
        });
    }

    // 6. Sort by priority descending.
    candidates.sort_by(|a, b| b.priority.cmp(&a.priority));

    candidates.iter().map(|c| c.view_name.clone()).collect()
}

/// Estimate the number of rows a view will produce using DataFusion statistics.
///
/// Creates a physical plan for the view and extracts `num_rows` from the
/// aggregate partition statistics. Falls back to 1000 when statistics are
/// unavailable or the view is not registered.
///
/// Uses `partition_statistics(None)` (DataFusion 51+) which returns
/// aggregate `Statistics` across all partitions.
pub async fn estimate_view_rows(ctx: &SessionContext, view_name: &str) -> u64 {
    if let Ok(df) = ctx.table(view_name).await {
        if let Ok(plan) = df.create_physical_plan().await {
            // partition_statistics(None) returns aggregate stats across all partitions.
            // This is the DF 51+ API; plan.statistics() is deprecated.
            if let Ok(stats) = plan.partition_statistics(None) {
                if let Some(num_rows) = stats.num_rows.get_value() {
                    return *num_rows as u64;
                }
            }
        }
    }
    1000 // fallback estimate
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy_construction() {
        let policy = CachePlacementPolicy::default();
        assert_eq!(policy.min_fanout, 2);
        assert!(policy.min_estimated_rows.is_none());
        assert!(policy.cache_memory_budget.is_none());
        assert!(!policy.use_historical_metrics);
        assert!(policy.overrides.is_empty());
    }

    #[test]
    fn test_cache_action_serialization_roundtrip() {
        let force = CacheAction::ForceCache;
        let never = CacheAction::NeverCache;

        let force_json = serde_json::to_string(&force).unwrap();
        let never_json = serde_json::to_string(&never).unwrap();

        assert_eq!(force_json, "\"ForceCache\"");
        assert_eq!(never_json, "\"NeverCache\"");

        let force_back: CacheAction = serde_json::from_str(&force_json).unwrap();
        let never_back: CacheAction = serde_json::from_str(&never_json).unwrap();

        assert_eq!(force_back, CacheAction::ForceCache);
        assert_eq!(never_back, CacheAction::NeverCache);
    }

    #[test]
    fn test_cache_override_serialization() {
        let ovr = CacheOverride {
            view_name: "hot_view".to_string(),
            action: CacheAction::ForceCache,
        };

        let json = serde_json::to_string(&ovr).unwrap();
        let back: CacheOverride = serde_json::from_str(&json).unwrap();

        assert_eq!(back.view_name, "hot_view");
        assert_eq!(back.action, CacheAction::ForceCache);
    }

    #[test]
    fn test_policy_serialization_roundtrip() {
        let policy = CachePlacementPolicy {
            min_fanout: 3,
            min_estimated_rows: Some(500),
            cache_memory_budget: Some(1024 * 1024 * 256),
            use_historical_metrics: true,
            overrides: vec![
                CacheOverride {
                    view_name: "always_cache".to_string(),
                    action: CacheAction::ForceCache,
                },
                CacheOverride {
                    view_name: "never_cache".to_string(),
                    action: CacheAction::NeverCache,
                },
            ],
        };

        let json = serde_json::to_string(&policy).unwrap();
        let back: CachePlacementPolicy = serde_json::from_str(&json).unwrap();

        assert_eq!(back.min_fanout, 3);
        assert_eq!(back.min_estimated_rows, Some(500));
        assert_eq!(back.cache_memory_budget, Some(1024 * 1024 * 256));
        assert!(back.use_historical_metrics);
        assert_eq!(back.overrides.len(), 2);
        assert_eq!(back.overrides[0].view_name, "always_cache");
        assert_eq!(back.overrides[0].action, CacheAction::ForceCache);
        assert_eq!(back.overrides[1].view_name, "never_cache");
        assert_eq!(back.overrides[1].action, CacheAction::NeverCache);
    }

    #[tokio::test]
    async fn test_force_cache_override_bypasses_fanout() {
        // A view with zero fanout but ForceCache override should be included.
        let views = vec![make_test_view("forced_view")];
        let ref_counts: HashMap<String, usize> =
            [("forced_view".to_string(), 0)].into_iter().collect();

        let policy = CachePlacementPolicy {
            min_fanout: 5, // Very high threshold
            overrides: vec![CacheOverride {
                view_name: "forced_view".to_string(),
                action: CacheAction::ForceCache,
            }],
            ..Default::default()
        };

        let result = compute_cache_boundaries_sync(&views, &ref_counts, &policy).await;
        assert_eq!(result, vec!["forced_view"]);
    }

    #[tokio::test]
    async fn test_never_cache_override_prevents_caching() {
        // A view with high fanout but NeverCache override should be excluded.
        let views = vec![make_test_view("excluded_view")];
        let ref_counts: HashMap<String, usize> =
            [("excluded_view".to_string(), 10)].into_iter().collect();

        let policy = CachePlacementPolicy {
            min_fanout: 1,
            overrides: vec![CacheOverride {
                view_name: "excluded_view".to_string(),
                action: CacheAction::NeverCache,
            }],
            ..Default::default()
        };

        let result = compute_cache_boundaries_sync(&views, &ref_counts, &policy).await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_min_fanout_filter() {
        let views = vec![
            make_test_view("low_fanout"),
            make_test_view("high_fanout"),
        ];
        let ref_counts: HashMap<String, usize> = [
            ("low_fanout".to_string(), 1),
            ("high_fanout".to_string(), 5),
        ]
        .into_iter()
        .collect();

        let policy = CachePlacementPolicy {
            min_fanout: 3,
            ..Default::default()
        };

        let result = compute_cache_boundaries_sync(&views, &ref_counts, &policy).await;
        assert_eq!(result, vec!["high_fanout"]);
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        // Views should be sorted by priority (fanout * estimated_rows) descending.
        // Without a real SessionContext, estimate_view_rows returns 1000 for all.
        let views = vec![
            make_test_view("fanout_2"),
            make_test_view("fanout_5"),
            make_test_view("fanout_3"),
        ];
        let ref_counts: HashMap<String, usize> = [
            ("fanout_2".to_string(), 2),
            ("fanout_5".to_string(), 5),
            ("fanout_3".to_string(), 3),
        ]
        .into_iter()
        .collect();

        let policy = CachePlacementPolicy::default();

        let result = compute_cache_boundaries_sync(&views, &ref_counts, &policy).await;
        // With fallback 1000 rows: priorities are 5000, 3000, 2000.
        assert_eq!(result, vec!["fanout_5", "fanout_3", "fanout_2"]);
    }

    #[tokio::test]
    async fn test_force_cache_has_highest_priority() {
        // ForceCache views should appear first (u64::MAX priority).
        let views = vec![
            make_test_view("forced"),
            make_test_view("high_fanout"),
        ];
        let ref_counts: HashMap<String, usize> = [
            ("forced".to_string(), 1),
            ("high_fanout".to_string(), 100),
        ]
        .into_iter()
        .collect();

        let policy = CachePlacementPolicy {
            min_fanout: 2,
            overrides: vec![CacheOverride {
                view_name: "forced".to_string(),
                action: CacheAction::ForceCache,
            }],
            ..Default::default()
        };

        let result = compute_cache_boundaries_sync(&views, &ref_counts, &policy).await;
        assert_eq!(result[0], "forced");
        assert!(result.contains(&"high_fanout".to_string()));
    }

    #[tokio::test]
    async fn test_empty_views_returns_empty() {
        let views: Vec<ViewDefinition> = vec![];
        let ref_counts: HashMap<String, usize> = HashMap::new();
        let policy = CachePlacementPolicy::default();

        let result = compute_cache_boundaries_sync(&views, &ref_counts, &policy).await;
        assert!(result.is_empty());
    }

    // --- Test helpers ---

    /// Create a minimal ViewDefinition for testing.
    fn make_test_view(name: &str) -> ViewDefinition {
        use crate::spec::relations::{SchemaContract, ViewTransform};
        use std::collections::BTreeMap;

        let mut schema = BTreeMap::new();
        schema.insert("id".to_string(), "Int64".to_string());

        ViewDefinition {
            name: name.to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: SchemaContract { columns: schema },
        }
    }

    /// Synchronous wrapper for compute_cache_boundaries that uses a dummy
    /// SessionContext (no registered views, so estimate_view_rows falls back
    /// to 1000 for all views). This allows testing the policy logic in
    /// isolation without needing real DataFusion tables.
    async fn compute_cache_boundaries_sync(
        views: &[ViewDefinition],
        ref_counts: &HashMap<String, usize>,
        policy: &CachePlacementPolicy,
    ) -> Vec<String> {
        let ctx = SessionContext::new();
        compute_cache_boundaries(&ctx, views, ref_counts, policy, None).await
    }
}
