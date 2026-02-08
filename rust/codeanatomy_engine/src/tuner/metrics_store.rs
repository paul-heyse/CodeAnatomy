//! Bounded metrics history for cross-run tuner learning.
//!
//! Stores up to `max_entries` historical `ExecutionMetrics` + `TunerConfig`
//! pairs keyed by spec hash. The tuner consults this history to detect
//! trends and make better configuration decisions across runs.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::tuner::adaptive::{ExecutionMetrics, TunerConfig};

/// Bounded metrics history for cross-run tuner learning.
///
/// Stores up to `max_entries` historical `ExecutionMetrics` + `TunerConfig`
/// pairs. When the store exceeds capacity, the oldest entry is evicted (FIFO).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsStore {
    /// Historical metrics entries, ordered by insertion time.
    pub entries: Vec<MetricsEntry>,
    /// Maximum number of entries to retain.
    pub max_entries: usize,
}

/// A single metrics observation with its associated configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsEntry {
    /// BLAKE3 hash of the execution spec that produced these metrics.
    pub spec_hash: [u8; 32],
    /// Observed execution metrics.
    pub metrics: ExecutionMetrics,
    /// Tuner configuration that was active during execution.
    pub config_used: TunerConfig,
    /// Tuner configuration recommended after observing these metrics.
    /// `None` if the tuner did not propose any adjustment.
    pub config_recommended: Option<TunerConfig>,
    /// When this observation was recorded.
    pub timestamp: DateTime<Utc>,
}

impl MetricsStore {
    /// Create a new metrics store with the given capacity.
    ///
    /// Parameters
    /// ----------
    /// max_entries
    ///     Maximum number of entries before FIFO eviction kicks in.
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Vec::new(),
            max_entries,
        }
    }

    /// Record a new metrics observation.
    ///
    /// If the store is at capacity, the oldest entry is evicted (FIFO).
    pub fn record(&mut self, entry: MetricsEntry) {
        self.entries.push(entry);
        if self.entries.len() > self.max_entries {
            self.entries.remove(0); // FIFO eviction
        }
    }

    /// Get historical metrics for a specific spec hash.
    ///
    /// Returns references to all entries matching the given spec hash,
    /// in insertion order (oldest first).
    pub fn history_for_spec(&self, spec_hash: &[u8; 32]) -> Vec<&MetricsEntry> {
        self.entries
            .iter()
            .filter(|e| &e.spec_hash == spec_hash)
            .collect()
    }

    /// Return the number of entries currently stored.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return true if the store contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuner::adaptive::{ExecutionMetrics, TunerConfig};

    fn make_entry(spec_hash: [u8; 32], elapsed_ms: u64) -> MetricsEntry {
        MetricsEntry {
            spec_hash,
            metrics: ExecutionMetrics {
                elapsed_ms,
                spill_count: 0,
                scan_selectivity: 1.0,
                peak_memory_bytes: 1024,
                rows_processed: 100,
            },
            config_used: TunerConfig::default(),
            config_recommended: None,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn test_new_store_is_empty() {
        let store = MetricsStore::new(10);
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_record_and_retrieve() {
        let mut store = MetricsStore::new(10);
        let hash = [1u8; 32];
        store.record(make_entry(hash, 100));
        store.record(make_entry(hash, 200));
        store.record(make_entry([2u8; 32], 300));

        let history = store.history_for_spec(&hash);
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].metrics.elapsed_ms, 100);
        assert_eq!(history[1].metrics.elapsed_ms, 200);
    }

    #[test]
    fn test_fifo_eviction() {
        let mut store = MetricsStore::new(3);
        let hash_a = [1u8; 32];
        let hash_b = [2u8; 32];

        store.record(make_entry(hash_a, 100));
        store.record(make_entry(hash_a, 200));
        store.record(make_entry(hash_b, 300));

        // Store is at capacity (3)
        assert_eq!(store.len(), 3);

        // Adding a fourth entry evicts the oldest
        store.record(make_entry(hash_b, 400));
        assert_eq!(store.len(), 3);

        // First entry (hash_a, 100) should be evicted
        let history_a = store.history_for_spec(&hash_a);
        assert_eq!(history_a.len(), 1);
        assert_eq!(history_a[0].metrics.elapsed_ms, 200);

        let history_b = store.history_for_spec(&hash_b);
        assert_eq!(history_b.len(), 2);
    }

    #[test]
    fn test_history_for_nonexistent_spec() {
        let store = MetricsStore::new(10);
        let history = store.history_for_spec(&[99u8; 32]);
        assert!(history.is_empty());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut store = MetricsStore::new(10);
        store.record(make_entry([1u8; 32], 100));

        let json = serde_json::to_string(&store).unwrap();
        let deserialized: MetricsStore = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized.max_entries, 10);
        assert_eq!(deserialized.entries[0].metrics.elapsed_ms, 100);
    }
}
