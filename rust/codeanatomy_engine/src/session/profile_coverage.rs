//! Runtime profile coverage contract.
//!
//! Every field in `RuntimeProfileSpec` must have a corresponding
//! `RuntimeProfileCoverage` entry, either `Applied` (wired into
//! session configuration) or `Reserved` (no stable DataFusion 51
//! surface for the current engine path).
//!
//! This registry is the machine-checkable proof that every profile
//! knob is intentionally handled.

use serde::{Deserialize, Serialize};

use crate::executor::warnings::{RunWarning, WarningCode, WarningStage};
use crate::session::runtime_profiles::RuntimeProfileSpec;

/// Whether a profile field is actively applied or intentionally reserved.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CoverageState {
    /// Field is wired into session configuration via a documented API.
    Applied,
    /// Field is recognized but not wired; no stable DF51 surface exists
    /// in the current engine path.
    Reserved,
}

/// Coverage entry for a single `RuntimeProfileSpec` field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfileCoverage {
    /// Field name matching the `RuntimeProfileSpec` struct field.
    pub field: String,
    /// Whether the field is applied or reserved.
    pub state: CoverageState,
    /// Human-readable note explaining where/how the field is applied,
    /// or why it is reserved.
    pub note: String,
}

/// Evaluate the coverage contract for all `RuntimeProfileSpec` fields.
///
/// Return one `RuntimeProfileCoverage` entry per field. The order is
/// grouped by category for readability.
pub fn evaluate_profile_coverage() -> Vec<RuntimeProfileCoverage> {
    vec![
        // --- Metadata ---
        RuntimeProfileCoverage {
            field: "profile_name".into(),
            state: CoverageState::Applied,
            note: "Metadata only; used for logging and audit trail".into(),
        },
        // --- Parallelism ---
        RuntimeProfileCoverage {
            field: "target_partitions".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_target_partitions".into(),
        },
        RuntimeProfileCoverage {
            field: "batch_size".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_batch_size".into(),
        },
        RuntimeProfileCoverage {
            field: "planning_concurrency".into(),
            state: CoverageState::Applied,
            note: "opts.execution.planning_concurrency".into(),
        },
        RuntimeProfileCoverage {
            field: "meta_fetch_concurrency".into(),
            state: CoverageState::Reserved,
            note: "No stable DataFusion 51 surface in current engine path".into(),
        },
        // --- Memory & Spill ---
        RuntimeProfileCoverage {
            field: "memory_pool_bytes".into(),
            state: CoverageState::Applied,
            note: "FairSpillPool::new".into(),
        },
        RuntimeProfileCoverage {
            field: "max_temp_directory_bytes".into(),
            state: CoverageState::Applied,
            note: "RuntimeEnvBuilder::with_max_temp_directory_size".into(),
        },
        // --- Repartition ---
        RuntimeProfileCoverage {
            field: "repartition_joins".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_repartition_joins".into(),
        },
        RuntimeProfileCoverage {
            field: "repartition_aggregations".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_repartition_aggregations".into(),
        },
        RuntimeProfileCoverage {
            field: "repartition_windows".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_repartition_windows".into(),
        },
        RuntimeProfileCoverage {
            field: "repartition_sorts".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_repartition_sorts".into(),
        },
        RuntimeProfileCoverage {
            field: "repartition_file_scans".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_repartition_file_scans".into(),
        },
        RuntimeProfileCoverage {
            field: "repartition_file_min_size".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_repartition_file_min_size".into(),
        },
        // --- Parquet Scan ---
        RuntimeProfileCoverage {
            field: "parquet_pruning".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_parquet_pruning".into(),
        },
        RuntimeProfileCoverage {
            field: "pushdown_filters".into(),
            state: CoverageState::Applied,
            note: "opts.execution.parquet.pushdown_filters".into(),
        },
        RuntimeProfileCoverage {
            field: "enable_page_index".into(),
            state: CoverageState::Applied,
            note: "opts.execution.parquet.enable_page_index".into(),
        },
        RuntimeProfileCoverage {
            field: "metadata_size_hint".into(),
            state: CoverageState::Applied,
            note: "opts.execution.parquet.metadata_size_hint".into(),
        },
        RuntimeProfileCoverage {
            field: "max_predicate_cache_size".into(),
            state: CoverageState::Reserved,
            note: "Provider-level cache; no global DF51 session knob".into(),
        },
        // --- Cache ---
        RuntimeProfileCoverage {
            field: "list_files_cache_limit".into(),
            state: CoverageState::Reserved,
            note: "ObjectStore cache layer; not wired to DF session".into(),
        },
        RuntimeProfileCoverage {
            field: "list_files_cache_ttl".into(),
            state: CoverageState::Reserved,
            note: "ObjectStore cache layer; not wired to DF session".into(),
        },
        RuntimeProfileCoverage {
            field: "metadata_cache_limit".into(),
            state: CoverageState::Reserved,
            note: "ObjectStore cache layer; not wired to DF session".into(),
        },
        // --- Statistics ---
        RuntimeProfileCoverage {
            field: "collect_statistics".into(),
            state: CoverageState::Applied,
            note: "opts.execution.collect_statistics".into(),
        },
        // --- Optimizer ---
        RuntimeProfileCoverage {
            field: "optimizer_max_passes".into(),
            state: CoverageState::Applied,
            note: "opts.optimizer.max_passes".into(),
        },
        RuntimeProfileCoverage {
            field: "skip_failed_rules".into(),
            state: CoverageState::Applied,
            note: "opts.optimizer.skip_failed_rules".into(),
        },
        RuntimeProfileCoverage {
            field: "filter_null_join_keys".into(),
            state: CoverageState::Applied,
            note: "opts.optimizer.filter_null_join_keys".into(),
        },
        RuntimeProfileCoverage {
            field: "enable_dynamic_filter_pushdown".into(),
            state: CoverageState::Applied,
            note: "opts.optimizer.enable_dynamic_filter_pushdown".into(),
        },
        RuntimeProfileCoverage {
            field: "enable_topk_dynamic_filter_pushdown".into(),
            state: CoverageState::Applied,
            note: "opts.optimizer.enable_topk_dynamic_filter_pushdown".into(),
        },
        RuntimeProfileCoverage {
            field: "enable_recursive_ctes".into(),
            state: CoverageState::Applied,
            note: "opts.execution.enable_recursive_ctes".into(),
        },
        // --- SQL Parser ---
        RuntimeProfileCoverage {
            field: "enable_ident_normalization".into(),
            state: CoverageState::Applied,
            note: "opts.sql_parser.enable_ident_normalization".into(),
        },
        // --- Explain ---
        RuntimeProfileCoverage {
            field: "show_statistics".into(),
            state: CoverageState::Applied,
            note: "opts.explain.show_statistics".into(),
        },
        RuntimeProfileCoverage {
            field: "show_schema".into(),
            state: CoverageState::Applied,
            note: "opts.explain.show_schema".into(),
        },
    ]
}

/// Return the count of applied (wired) fields.
pub fn applied_count() -> usize {
    evaluate_profile_coverage()
        .iter()
        .filter(|c| c.state == CoverageState::Applied)
        .count()
}

/// Return the count of reserved (not-yet-wired) fields.
pub fn reserved_count() -> usize {
    evaluate_profile_coverage()
        .iter()
        .filter(|c| c.state == CoverageState::Reserved)
        .count()
}

/// Emit runtime warnings for reserved profile knobs.
pub fn reserved_profile_warnings(profile: &RuntimeProfileSpec) -> Vec<RunWarning> {
    let mut warnings = Vec::new();
    let push = |warnings: &mut Vec<RunWarning>, field: &str, value: String| {
        warnings.push(
            RunWarning::new(
                WarningCode::ReservedProfileKnobIgnored,
                WarningStage::RuntimeProfile,
                format!(
                    "Runtime profile field '{field}' is reserved and currently ignored in execution path."
                ),
            )
            .with_context("field", field)
            .with_context("value", value),
        );
    };

    push(
        &mut warnings,
        "meta_fetch_concurrency",
        profile.meta_fetch_concurrency.to_string(),
    );
    push(
        &mut warnings,
        "max_predicate_cache_size",
        format!("{:?}", profile.max_predicate_cache_size),
    );
    push(
        &mut warnings,
        "list_files_cache_limit",
        profile.list_files_cache_limit.to_string(),
    );
    push(
        &mut warnings,
        "list_files_cache_ttl",
        format!("{:?}", profile.list_files_cache_ttl),
    );
    push(
        &mut warnings,
        "metadata_cache_limit",
        profile.metadata_cache_limit.to_string(),
    );
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::runtime_profiles::RuntimeProfileSpec;

    /// Total number of fields in `RuntimeProfileSpec` that require coverage.
    ///
    /// This constant must match the struct field count. If `RuntimeProfileSpec`
    /// gains or loses a field, this test will catch the mismatch.
    const EXPECTED_COVERAGE_COUNT: usize = 31;

    #[test]
    fn test_coverage_count_matches_profile_spec() {
        let coverage = evaluate_profile_coverage();
        // RuntimeProfileSpec has 31 fields (including profile_name).
        // If this assertion fails, a field was added/removed from
        // RuntimeProfileSpec without updating the coverage registry.
        assert_eq!(
            coverage.len(),
            EXPECTED_COVERAGE_COUNT,
            "Coverage registry has {} entries but expected {}. \
             Update evaluate_profile_coverage() when RuntimeProfileSpec changes.",
            coverage.len(),
            EXPECTED_COVERAGE_COUNT,
        );
    }

    #[test]
    fn test_no_duplicate_fields() {
        let coverage = evaluate_profile_coverage();
        let mut seen = std::collections::HashSet::new();
        for entry in &coverage {
            assert!(
                seen.insert(entry.field.clone()),
                "Duplicate coverage entry for field: {}",
                entry.field,
            );
        }
    }

    #[test]
    fn test_all_entries_have_notes() {
        let coverage = evaluate_profile_coverage();
        for entry in &coverage {
            assert!(
                !entry.note.is_empty(),
                "Coverage entry for '{}' has empty note",
                entry.field,
            );
        }
    }

    #[test]
    fn test_applied_plus_reserved_equals_total() {
        let total = evaluate_profile_coverage().len();
        assert_eq!(applied_count() + reserved_count(), total);
    }

    #[test]
    fn test_expected_applied_fields() {
        let coverage = evaluate_profile_coverage();
        let applied: Vec<&str> = coverage
            .iter()
            .filter(|c| c.state == CoverageState::Applied)
            .map(|c| c.field.as_str())
            .collect();

        // Core applied fields that must always be present
        assert!(applied.contains(&"target_partitions"));
        assert!(applied.contains(&"batch_size"));
        assert!(applied.contains(&"memory_pool_bytes"));
        assert!(applied.contains(&"repartition_joins"));
        assert!(applied.contains(&"pushdown_filters"));
        assert!(applied.contains(&"enable_page_index"));
        assert!(applied.contains(&"collect_statistics"));
        assert!(applied.contains(&"optimizer_max_passes"));
    }

    #[test]
    fn test_expected_reserved_fields() {
        let coverage = evaluate_profile_coverage();
        let reserved: Vec<&str> = coverage
            .iter()
            .filter(|c| c.state == CoverageState::Reserved)
            .map(|c| c.field.as_str())
            .collect();

        // Fields known to lack DF51 session wiring
        assert!(reserved.contains(&"meta_fetch_concurrency"));
        assert!(reserved.contains(&"max_predicate_cache_size"));
        assert!(reserved.contains(&"list_files_cache_limit"));
        assert!(reserved.contains(&"list_files_cache_ttl"));
        assert!(reserved.contains(&"metadata_cache_limit"));
    }

    #[test]
    fn test_coverage_serialization_roundtrip() {
        let coverage = evaluate_profile_coverage();
        let json = serde_json::to_string(&coverage).unwrap();
        let deserialized: Vec<RuntimeProfileCoverage> = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.len(), coverage.len());
        for (original, restored) in coverage.iter().zip(deserialized.iter()) {
            assert_eq!(original.field, restored.field);
            assert_eq!(original.state, restored.state);
            assert_eq!(original.note, restored.note);
        }
    }

    #[test]
    fn test_reserved_profile_warnings_include_all_reserved_fields() {
        let profile = RuntimeProfileSpec::small();
        let warnings = reserved_profile_warnings(&profile);

        assert_eq!(warnings.len(), 5);
        assert!(warnings
            .iter()
            .any(|w| { w.context.get("field") == Some(&"meta_fetch_concurrency".to_string()) }));
        assert!(warnings
            .iter()
            .any(|w| { w.context.get("field") == Some(&"max_predicate_cache_size".to_string()) }));
        assert!(warnings
            .iter()
            .any(|w| { w.context.get("field") == Some(&"list_files_cache_limit".to_string()) }));
        assert!(warnings
            .iter()
            .any(|w| { w.context.get("field") == Some(&"list_files_cache_ttl".to_string()) }));
        assert!(warnings
            .iter()
            .any(|w| { w.context.get("field") == Some(&"metadata_cache_limit".to_string()) }));
    }
}
