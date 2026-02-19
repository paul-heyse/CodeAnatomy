//! Deterministic runtime profiles for session configuration.
//!
//! `RuntimeProfileSpec` captures ALL session configuration knobs as a versioned,
//! fingerprinted object. This extends the existing `EnvironmentProfile` with
//! comprehensive session config including repartition, cache, statistics, and
//! optimizer knobs.
//!
//! Two identical profiles produce identical fingerprints. Profile fingerprints
//! are part of the envelope_hash determinism contract.

use serde::{Deserialize, Serialize};

/// Deterministic runtime profile that captures ALL session configuration
/// knobs as a versioned, fingerprinted object.
///
/// Extends the existing `EnvironmentProfile` with comprehensive session
/// config including repartition, cache, statistics, and optimizer knobs.
///
/// Profile presets (`small`, `medium`, `large`) provide sensible defaults
/// for common workload sizes. Custom profiles can be constructed directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfileSpec {
    /// Profile name for logging/audit (e.g., "small", "medium", "large").
    pub profile_name: String,

    // --- Parallelism ---
    /// Target number of partitions for DataFusion execution.
    pub target_partitions: usize,
    /// Batch size for Arrow record batches.
    pub batch_size: usize,
    /// Number of concurrent planning threads for UNION children.
    pub planning_concurrency: usize,
    /// Concurrency for metadata/statistics fetch operations.
    pub meta_fetch_concurrency: usize,

    // --- Memory & Spill ---
    /// Memory pool size in bytes for execution.
    pub memory_pool_bytes: usize,
    /// Maximum bytes for temporary spill directory.
    pub max_temp_directory_bytes: usize,

    // --- Repartition ---
    /// Whether to repartition before joins.
    pub repartition_joins: bool,
    /// Whether to repartition before aggregations.
    pub repartition_aggregations: bool,
    /// Whether to repartition before window functions.
    pub repartition_windows: bool,
    /// Whether to repartition before sorts.
    pub repartition_sorts: bool,
    /// Whether to repartition file scans across partitions.
    pub repartition_file_scans: bool,
    /// Minimum file size (bytes) before file-level repartitioning kicks in.
    pub repartition_file_min_size: usize,

    // --- Parquet Scan ---
    /// Enable Parquet predicate pruning.
    pub parquet_pruning: bool,
    /// Enable pushdown of filter predicates into Parquet scans.
    pub pushdown_filters: bool,
    /// Enable page-level index pruning in Parquet scans.
    pub enable_page_index: bool,
    /// Hint for Parquet metadata prefetch size (bytes).
    pub metadata_size_hint: usize,
    /// Maximum predicate cache size for Parquet scans.
    pub max_predicate_cache_size: Option<usize>,

    // --- Cache ---
    /// Maximum entries in the file listing cache.
    pub list_files_cache_limit: usize,
    /// TTL for file listing cache entries (e.g., "5m", "1h").
    pub list_files_cache_ttl: Option<String>,
    /// Maximum entries in the Parquet metadata cache.
    pub metadata_cache_limit: usize,

    // --- Statistics ---
    /// Whether to collect statistics during execution.
    pub collect_statistics: bool,

    // --- Optimizer ---
    /// Maximum number of optimizer passes.
    pub optimizer_max_passes: usize,
    /// Whether to skip failed optimizer rules instead of erroring.
    pub skip_failed_rules: bool,
    /// Whether to filter null join keys.
    pub filter_null_join_keys: bool,
    /// Enable dynamic filter pushdown.
    pub enable_dynamic_filter_pushdown: bool,
    /// Enable join-specific dynamic filter pushdown.
    pub enable_join_dynamic_filter_pushdown: bool,
    /// Enable aggregate-specific dynamic filter pushdown.
    pub enable_aggregate_dynamic_filter_pushdown: bool,
    /// Enable top-K dynamic filter pushdown.
    pub enable_topk_dynamic_filter_pushdown: bool,
    /// Enable sort pushdown.
    pub enable_sort_pushdown: bool,
    /// Allow symmetric joins without pruning.
    pub allow_symmetric_joins_without_pruning: bool,
    /// Enable recursive CTEs.
    pub enable_recursive_ctes: bool,

    // --- SQL Parser ---
    /// Enable identifier normalization (lowercase).
    pub enable_ident_normalization: bool,

    // --- Explain ---
    /// Show statistics in EXPLAIN output.
    pub show_statistics: bool,
    /// Show schema in EXPLAIN output.
    pub show_schema: bool,
}

impl RuntimeProfileSpec {
    /// Compute a deterministic fingerprint of the full profile.
    ///
    /// Two identical profiles produce identical fingerprints.
    /// Profile fingerprint is part of the envelope_hash determinism contract.
    pub fn fingerprint(&self) -> [u8; 32] {
        let serialized = serde_json::to_vec(self).expect("profile serializable");
        *blake3::hash(&serialized).as_bytes()
    }

    /// Small profile: development, CI, small repos (< 100 files).
    pub fn small() -> Self {
        Self {
            profile_name: "small".into(),
            target_partitions: 4,
            batch_size: 4096,
            planning_concurrency: 4,
            meta_fetch_concurrency: 8,
            memory_pool_bytes: 256 * 1024 * 1024, // 256 MB
            max_temp_directory_bytes: 1024 * 1024 * 1024, // 1 GB
            repartition_joins: true,
            repartition_aggregations: true,
            repartition_windows: true,
            repartition_sorts: true,
            repartition_file_scans: true,
            repartition_file_min_size: 32 * 1024 * 1024, // 32 MB
            parquet_pruning: true,
            pushdown_filters: true,
            enable_page_index: true,
            metadata_size_hint: 524288,
            max_predicate_cache_size: None,
            list_files_cache_limit: 1024 * 1024, // 1 MB
            list_files_cache_ttl: None,
            metadata_cache_limit: 50 * 1024 * 1024, // 50 MB
            collect_statistics: true,
            optimizer_max_passes: 3,
            skip_failed_rules: false,
            filter_null_join_keys: true,
            enable_dynamic_filter_pushdown: true,
            enable_join_dynamic_filter_pushdown: true,
            enable_aggregate_dynamic_filter_pushdown: true,
            enable_topk_dynamic_filter_pushdown: true,
            enable_sort_pushdown: true,
            allow_symmetric_joins_without_pruning: true,
            enable_recursive_ctes: true,
            enable_ident_normalization: false,
            show_statistics: true,
            show_schema: true,
        }
    }

    /// Medium profile: standard workloads (100-10K files).
    pub fn medium() -> Self {
        let mut p = Self::small();
        p.profile_name = "medium".into();
        p.target_partitions = 8;
        p.batch_size = 8192;
        p.planning_concurrency = 8;
        p.meta_fetch_concurrency = 32;
        p.memory_pool_bytes = 1024 * 1024 * 1024; // 1 GB
        p.max_temp_directory_bytes = 10 * 1024 * 1024 * 1024; // 10 GB
        p.repartition_file_min_size = 64 * 1024 * 1024;
        p
    }

    /// Large profile: large repos (10K+ files), production.
    pub fn large() -> Self {
        let mut p = Self::medium();
        p.profile_name = "large".into();
        p.target_partitions = 16;
        p.batch_size = 16384;
        p.planning_concurrency = 16;
        p.memory_pool_bytes = 4 * 1024 * 1024 * 1024; // 4 GB
        p.max_temp_directory_bytes = 100 * 1024 * 1024 * 1024; // 100 GB
        p.repartition_file_min_size = 128 * 1024 * 1024;
        p.metadata_cache_limit = 200 * 1024 * 1024; // 200 MB
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_profile_values() {
        let p = RuntimeProfileSpec::small();
        assert_eq!(p.profile_name, "small");
        assert_eq!(p.target_partitions, 4);
        assert_eq!(p.batch_size, 4096);
        assert_eq!(p.planning_concurrency, 4);
        assert_eq!(p.memory_pool_bytes, 256 * 1024 * 1024);
        assert!(p.repartition_sorts);
        assert!(p.repartition_file_scans);
        assert!(p.enable_page_index);
        assert!(p.pushdown_filters);
        assert!(p.collect_statistics);
        assert!(!p.enable_ident_normalization);
    }

    #[test]
    fn test_medium_profile_inherits_small() {
        let p = RuntimeProfileSpec::medium();
        assert_eq!(p.profile_name, "medium");
        assert_eq!(p.target_partitions, 8);
        assert_eq!(p.batch_size, 8192);
        assert_eq!(p.memory_pool_bytes, 1024 * 1024 * 1024);
        // Inherited from small
        assert!(p.repartition_sorts);
        assert!(p.enable_page_index);
    }

    #[test]
    fn test_large_profile_inherits_medium() {
        let p = RuntimeProfileSpec::large();
        assert_eq!(p.profile_name, "large");
        assert_eq!(p.target_partitions, 16);
        assert_eq!(p.batch_size, 16384);
        assert_eq!(p.memory_pool_bytes, 4 * 1024 * 1024 * 1024);
        assert_eq!(p.metadata_cache_limit, 200 * 1024 * 1024);
    }

    #[test]
    fn test_different_profiles_different_fingerprints() {
        let small = RuntimeProfileSpec::small();
        let medium = RuntimeProfileSpec::medium();
        let large = RuntimeProfileSpec::large();

        assert_ne!(small.fingerprint(), medium.fingerprint());
        assert_ne!(medium.fingerprint(), large.fingerprint());
        assert_ne!(small.fingerprint(), large.fingerprint());
    }

    #[test]
    fn test_same_profile_same_fingerprint() {
        let a = RuntimeProfileSpec::small();
        let b = RuntimeProfileSpec::small();
        assert_eq!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn test_fingerprint_changes_on_knob_change() {
        let a = RuntimeProfileSpec::small();
        let mut b = RuntimeProfileSpec::small();
        b.batch_size = 2048;
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let profile = RuntimeProfileSpec::medium();
        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: RuntimeProfileSpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.profile_name, "medium");
        assert_eq!(deserialized.target_partitions, 8);
        assert_eq!(deserialized.batch_size, 8192);
        assert_eq!(deserialized.fingerprint(), profile.fingerprint());
    }
}
