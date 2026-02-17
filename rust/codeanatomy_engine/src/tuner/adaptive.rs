//! Adaptive tuner with bounded auto-adjustment.
//!
//! Design principles:
//! 1. Explicit rollback policy on regressions (>2x elapsed time)
//! 2. NEVER mutate correctness-affecting options (optimizer rules, pushdown, etc.)
//! 3. Tune ONLY bounded execution knobs (partitions, batch_size, repartition flags)
//! 4. Stability window before proposing adjustments
//! 5. Bounded ranges (±25% for partitions, 1024..65536 for batch_size)

use serde::{Deserialize, Serialize};

/// Tuner operating mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TunerMode {
    /// Only observe metrics, never propose adjustments.
    ObserveOnly,
    /// Propose bounded adjustments based on metrics.
    BoundedAdapt,
}

/// Tunable execution configuration.
///
/// These are the ONLY knobs the tuner can adjust. Correctness-affecting options
/// (optimizer rules, analyzer rules, pushdown filters, ident normalization,
/// parquet pruning) are NEVER touched by the tuner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunerConfig {
    /// Target number of partitions for DataFusion execution.
    pub target_partitions: u32,
    /// Batch size for Arrow record batches.
    pub batch_size: u32,
    /// Whether to repartition before joins.
    pub repartition_joins: bool,
    /// Whether to repartition before aggregations.
    pub repartition_aggregations: bool,
}

impl Default for TunerConfig {
    fn default() -> Self {
        Self {
            target_partitions: 8,
            batch_size: 8192,
            repartition_joins: true,
            repartition_aggregations: true,
        }
    }
}

/// Execution metrics for a single query run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetrics {
    /// Total elapsed time in milliseconds.
    pub elapsed_ms: u64,
    /// Number of spill events during execution.
    pub spill_count: u32,
    /// Scan selectivity (rows_processed / rows_scanned, 0.0..1.0).
    pub scan_selectivity: f64,
    /// Peak memory usage in bytes.
    pub peak_memory_bytes: u64,
    /// Total rows processed.
    pub rows_processed: u64,
}

/// Bounded adaptive tuner.
///
/// Observes execution metrics and proposes configuration adjustments within
/// strict bounds. Never mutates correctness-affecting options.
#[derive(Debug, Clone)]
pub struct AdaptiveTuner {
    mode: TunerMode,
    current_config: TunerConfig,
    stable_config: TunerConfig,
    observation_count: u32,
    stability_window: u32,
    partitions_floor: u32,
    partitions_ceiling: u32,
    batch_size_floor: u32,
    batch_size_ceiling: u32,
    last_metrics: Option<ExecutionMetrics>,
    pending_rollback: bool,
}

impl AdaptiveTuner {
    /// Create a new adaptive tuner with the given initial configuration.
    ///
    /// Bounds are set at ±25% of initial target_partitions, and batch_size
    /// is bounded to 1024..65536.
    pub fn new(initial_config: TunerConfig) -> Self {
        let partitions_floor = (initial_config.target_partitions * 3 / 4).max(1);
        let partitions_ceiling = (initial_config.target_partitions * 5 / 4).max(2);
        let batch_size_floor = 1024_u32.max(initial_config.batch_size / 2);
        let batch_size_ceiling = 65536_u32.min(initial_config.batch_size * 2);

        Self {
            mode: TunerMode::BoundedAdapt,
            current_config: initial_config.clone(),
            stable_config: initial_config,
            observation_count: 0,
            stability_window: 3, // Require 3 observations before tuning
            partitions_floor,
            partitions_ceiling,
            batch_size_floor,
            batch_size_ceiling,
            last_metrics: None,
            pending_rollback: false,
        }
    }

    /// Create an observe-only tuner (never proposes adjustments).
    pub fn observe_only(initial_config: TunerConfig) -> Self {
        let mut tuner = Self::new(initial_config);
        tuner.mode = TunerMode::ObserveOnly;
        tuner
    }

    /// Record execution metrics and update tuner state.
    ///
    /// This mutates internal observation state but does not propose configuration
    /// changes directly. Call `propose_adjustment()` after recording to obtain a
    /// bounded candidate configuration.
    pub fn record_metrics(&mut self, metrics: &ExecutionMetrics) {
        self.observation_count += 1;

        // Check for regression first.
        if self.is_regression(metrics) {
            self.current_config = self.stable_config.clone();
            self.pending_rollback = true;
            self.last_metrics = Some(metrics.clone());
            return;
        }

        self.pending_rollback = false;
        self.last_metrics = Some(metrics.clone());
    }

    /// Propose a bounded configuration adjustment based on recorded metrics.
    ///
    /// This method is side-effect free; callers must commit accepted proposals
    /// through `apply_adjustment`.
    pub fn propose_adjustment(&self) -> Option<TunerConfig> {
        if self.pending_rollback {
            return Some(self.stable_config.clone());
        }

        // ObserveOnly mode never proposes adjustments
        if self.mode == TunerMode::ObserveOnly {
            return None;
        }

        // Wait for stability window before tuning (require N observations to pass)
        if self.observation_count <= self.stability_window {
            return None;
        }
        let metrics = self.last_metrics.as_ref()?;

        // Propose bounded adjustments based on metrics
        let mut proposed = self.current_config.clone();
        let mut changed = false;

        // Adjust partitions based on spill pressure
        if metrics.spill_count > 0 {
            // Reduce parallelism to reduce memory pressure
            let new_partitions = (proposed.target_partitions * 3 / 4).max(self.partitions_floor);
            if new_partitions != proposed.target_partitions {
                proposed.target_partitions = new_partitions;
                changed = true;
            }
        } else if metrics.spill_count == 0 && metrics.peak_memory_bytes > 0 {
            // No spills and we have memory headroom - consider increasing parallelism
            let memory_gb = metrics.peak_memory_bytes as f64 / 1_073_741_824.0;
            if memory_gb < 2.0 && proposed.target_partitions < self.partitions_ceiling {
                let new_partitions =
                    (proposed.target_partitions * 5 / 4).min(self.partitions_ceiling);
                if new_partitions != proposed.target_partitions {
                    proposed.target_partitions = new_partitions;
                    changed = true;
                }
            }
        }

        // Adjust batch size based on scan selectivity
        if metrics.scan_selectivity < 0.1 {
            // Very selective scans benefit from smaller batches
            let new_batch_size = (proposed.batch_size / 2).max(self.batch_size_floor);
            if new_batch_size != proposed.batch_size {
                proposed.batch_size = new_batch_size;
                changed = true;
            }
        } else if metrics.scan_selectivity > 0.8 {
            // High selectivity scans benefit from larger batches
            let new_batch_size = (proposed.batch_size * 3 / 2).min(self.batch_size_ceiling);
            if new_batch_size != proposed.batch_size {
                proposed.batch_size = new_batch_size;
                changed = true;
            }
        }

        // Adjust repartition flags based on rows processed
        if metrics.rows_processed < 1000 {
            // Small result sets don't benefit from repartitioning
            if proposed.repartition_joins || proposed.repartition_aggregations {
                proposed.repartition_joins = false;
                proposed.repartition_aggregations = false;
                changed = true;
            }
        } else if metrics.rows_processed > 100_000 {
            // Large result sets benefit from repartitioning
            if !proposed.repartition_joins || !proposed.repartition_aggregations {
                proposed.repartition_joins = true;
                proposed.repartition_aggregations = true;
                changed = true;
            }
        }

        changed.then_some(proposed)
    }

    /// Accept a proposed adjustment as the new current/stable configuration.
    pub fn apply_adjustment(&mut self, proposed: TunerConfig) {
        self.current_config = proposed.clone();
        self.stable_config = proposed;
        self.pending_rollback = false;
    }

    /// Check if the current metrics represent a regression from the last observation.
    ///
    /// Regression is defined as elapsed_ms > 2 * last_metrics.elapsed_ms.
    fn is_regression(&self, metrics: &ExecutionMetrics) -> bool {
        if let Some(ref last) = self.last_metrics {
            if last.elapsed_ms > 0 && metrics.elapsed_ms > last.elapsed_ms * 2 {
                return true;
            }
        }
        false
    }

    /// Get the current tuner configuration.
    pub fn current_config(&self) -> &TunerConfig {
        &self.current_config
    }

    /// Get the tuner operating mode.
    pub fn mode(&self) -> &TunerMode {
        &self.mode
    }

    /// Get the number of observations made.
    pub fn observation_count(&self) -> u32 {
        self.observation_count
    }

    /// Get the stability window size.
    pub fn stability_window(&self) -> u32 {
        self.stability_window
    }

    /// Get the last observed metrics, if any.
    pub fn last_metrics(&self) -> Option<&ExecutionMetrics> {
        self.last_metrics.as_ref()
    }

    /// Reset the tuner to the initial configuration.
    pub fn reset(&mut self, initial_config: TunerConfig) {
        let partitions_floor = (initial_config.target_partitions * 3 / 4).max(1);
        let partitions_ceiling = (initial_config.target_partitions * 5 / 4).max(2);
        let batch_size_floor = 1024_u32.max(initial_config.batch_size / 2);
        let batch_size_ceiling = 65536_u32.min(initial_config.batch_size * 2);

        self.current_config = initial_config.clone();
        self.stable_config = initial_config;
        self.observation_count = 0;
        self.partitions_floor = partitions_floor;
        self.partitions_ceiling = partitions_ceiling;
        self.batch_size_floor = batch_size_floor;
        self.batch_size_ceiling = batch_size_ceiling;
        self.last_metrics = None;
        self.pending_rollback = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observe_cycle(tuner: &mut AdaptiveTuner, metrics: &ExecutionMetrics) -> Option<TunerConfig> {
        tuner.record_metrics(metrics);
        tuner.propose_adjustment()
    }

    fn observe_and_apply(
        tuner: &mut AdaptiveTuner,
        metrics: &ExecutionMetrics,
    ) -> Option<TunerConfig> {
        let proposal = observe_cycle(tuner, metrics);
        if let Some(next) = proposal.clone() {
            tuner.apply_adjustment(next);
        }
        proposal
    }

    #[test]
    fn test_tuner_creation() {
        let config = TunerConfig {
            target_partitions: 8,
            batch_size: 8192,
            repartition_joins: true,
            repartition_aggregations: true,
        };
        let tuner = AdaptiveTuner::new(config.clone());

        assert_eq!(tuner.mode(), &TunerMode::BoundedAdapt);
        assert_eq!(tuner.current_config(), &config);
        assert_eq!(tuner.observation_count(), 0);
        assert_eq!(tuner.partitions_floor, 6);
        assert_eq!(tuner.partitions_ceiling, 10);
    }

    #[test]
    fn test_observe_only_mode() {
        let config = TunerConfig::default();
        let mut tuner = AdaptiveTuner::observe_only(config.clone());

        let metrics = ExecutionMetrics {
            elapsed_ms: 100,
            spill_count: 5,
            scan_selectivity: 0.5,
            peak_memory_bytes: 1_000_000,
            rows_processed: 1000,
        };

        let result = observe_cycle(&mut tuner, &metrics);
        assert!(
            result.is_none(),
            "ObserveOnly mode should not propose adjustments"
        );
        assert_eq!(tuner.current_config(), &config);
    }

    #[test]
    fn test_stability_window() {
        let config = TunerConfig::default();
        let mut tuner = AdaptiveTuner::new(config.clone());

        let metrics = ExecutionMetrics {
            elapsed_ms: 100,
            spill_count: 0,
            scan_selectivity: 0.5,
            peak_memory_bytes: 1_000_000,
            rows_processed: 1000,
        };

        // First observation - within stability window
        assert!(observe_cycle(&mut tuner, &metrics).is_none());
        assert_eq!(tuner.observation_count(), 1);

        // Second observation - still within window
        assert!(observe_cycle(&mut tuner, &metrics).is_none());
        assert_eq!(tuner.observation_count(), 2);

        // Third observation - still within window
        assert!(observe_cycle(&mut tuner, &metrics).is_none());
        assert_eq!(tuner.observation_count(), 3);
    }

    #[test]
    fn test_regression_detection() {
        let config = TunerConfig::default();
        let mut tuner = AdaptiveTuner::new(config.clone());

        let baseline_metrics = ExecutionMetrics {
            elapsed_ms: 100,
            spill_count: 0,
            scan_selectivity: 0.5,
            peak_memory_bytes: 1_000_000,
            rows_processed: 1000,
        };
        observe_and_apply(&mut tuner, &baseline_metrics);

        let regression_metrics = ExecutionMetrics {
            elapsed_ms: 250, // >2x of 100ms
            spill_count: 0,
            scan_selectivity: 0.5,
            peak_memory_bytes: 1_000_000,
            rows_processed: 1000,
        };

        let result = observe_cycle(&mut tuner, &regression_metrics);
        assert!(result.is_some(), "Should rollback on regression");
        assert_eq!(
            tuner.current_config(),
            &config,
            "Should rollback to stable config"
        );
    }

    #[test]
    fn test_spill_pressure_reduces_partitions() {
        let config = TunerConfig {
            target_partitions: 8,
            batch_size: 8192,
            repartition_joins: true,
            repartition_aggregations: true,
        };
        let mut tuner = AdaptiveTuner::new(config);

        // Get past stability window (need > stability_window observations)
        for _ in 0..4 {
            let metrics = ExecutionMetrics {
                elapsed_ms: 100,
                spill_count: 0,
                scan_selectivity: 0.5,
                peak_memory_bytes: 1_000_000,
                rows_processed: 1000,
            };
            observe_and_apply(&mut tuner, &metrics);
        }

        let spill_metrics = ExecutionMetrics {
            elapsed_ms: 100,
            spill_count: 5,
            scan_selectivity: 0.5,
            peak_memory_bytes: 1_000_000,
            rows_processed: 1000,
        };

        let result = observe_cycle(&mut tuner, &spill_metrics);
        assert!(result.is_some(), "Should propose adjustment on spills");
        let new_config = result.unwrap();
        assert!(
            new_config.target_partitions < 8,
            "Should reduce partitions on spills"
        );
        assert!(
            new_config.target_partitions >= tuner.partitions_floor,
            "Should respect floor"
        );
    }

    #[test]
    fn test_selective_scan_reduces_batch_size() {
        let config = TunerConfig::default();
        let mut tuner = AdaptiveTuner::new(config);

        // Get past stability window (need > stability_window observations)
        for _ in 0..4 {
            let metrics = ExecutionMetrics {
                elapsed_ms: 100,
                spill_count: 0,
                scan_selectivity: 0.5,
                peak_memory_bytes: 1_000_000,
                rows_processed: 1000,
            };
            observe_and_apply(&mut tuner, &metrics);
        }

        let selective_metrics = ExecutionMetrics {
            elapsed_ms: 100,
            spill_count: 0,
            scan_selectivity: 0.05, // Very selective
            peak_memory_bytes: 1_000_000,
            rows_processed: 1000,
        };

        let result = observe_cycle(&mut tuner, &selective_metrics);
        assert!(
            result.is_some(),
            "Should propose adjustment on selective scans"
        );
        let new_config = result.unwrap();
        assert!(
            new_config.batch_size < 8192,
            "Should reduce batch size on selective scans"
        );
        assert!(
            new_config.batch_size >= tuner.batch_size_floor,
            "Should respect floor"
        );
    }

    #[test]
    fn test_small_result_disables_repartition() {
        let config = TunerConfig::default();
        let mut tuner = AdaptiveTuner::new(config);

        // Get past stability window (need > stability_window observations)
        for _ in 0..4 {
            let metrics = ExecutionMetrics {
                elapsed_ms: 100,
                spill_count: 0,
                scan_selectivity: 0.5,
                peak_memory_bytes: 1_000_000,
                rows_processed: 50_000,
            };
            observe_and_apply(&mut tuner, &metrics);
        }

        let small_metrics = ExecutionMetrics {
            elapsed_ms: 100,
            spill_count: 0,
            scan_selectivity: 0.5,
            peak_memory_bytes: 1_000_000,
            rows_processed: 500, // Small result
        };

        let result = observe_cycle(&mut tuner, &small_metrics);
        assert!(
            result.is_some(),
            "Should propose adjustment for small results"
        );
        let new_config = result.unwrap();
        assert!(
            !new_config.repartition_joins && !new_config.repartition_aggregations,
            "Should disable repartitioning for small results"
        );
    }

    #[test]
    fn test_reset() {
        let config = TunerConfig::default();
        let mut tuner = AdaptiveTuner::new(config.clone());

        // Make some observations
        for _ in 0..3 {
            let metrics = ExecutionMetrics {
                elapsed_ms: 100,
                spill_count: 0,
                scan_selectivity: 0.5,
                peak_memory_bytes: 1_000_000,
                rows_processed: 1000,
            };
            observe_and_apply(&mut tuner, &metrics);
        }

        assert_eq!(tuner.observation_count(), 3);

        // Reset
        let new_config = TunerConfig {
            target_partitions: 16,
            batch_size: 4096,
            repartition_joins: false,
            repartition_aggregations: false,
        };
        tuner.reset(new_config.clone());

        assert_eq!(tuner.observation_count(), 0);
        assert_eq!(tuner.current_config(), &new_config);
        assert!(tuner.last_metrics().is_none());
    }
}
