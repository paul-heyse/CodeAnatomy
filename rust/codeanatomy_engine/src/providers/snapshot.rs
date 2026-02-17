//! Snapshot resolution helpers.
//!
//! Version resolution, validation, and snapshot metadata extraction.

use datafusion_common::Result as DFResult;
use deltalake::kernel::EagerSnapshot;
use deltalake::DeltaTable;

/// Snapshot resolution mode.
///
/// - Latest: use current snapshot
/// - Pinned: load specific version
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    Latest,
    Pinned(i64),
}

/// Materialize an eager snapshot for builder-based scan configuration.
pub fn eager_snapshot(table: &DeltaTable) -> DFResult<EagerSnapshot> {
    let snapshot = table
        .snapshot()
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
    Ok(snapshot.snapshot().clone())
}

/// Get the version of the loaded table.
///
/// # Arguments
/// * `table` - Delta table instance
///
/// # Returns
/// Version number of the loaded snapshot.
///
/// # Errors
/// Returns error if snapshot access fails.
pub fn table_version(table: &DeltaTable) -> DFResult<i64> {
    let snapshot = table
        .snapshot()
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
    Ok(snapshot.version())
}

/// Validate that a table's version matches the expected pin.
///
/// Used for replay scenarios where exact version match is required.
///
/// # Arguments
/// * `table` - Delta table instance
/// * `expected` - Expected version number
///
/// # Returns
/// Ok if versions match.
///
/// # Errors
/// Returns error if:
/// - Snapshot access fails
/// - Version mismatch detected
pub fn validate_version_pin(table: &DeltaTable, expected: i64) -> DFResult<()> {
    let actual = table_version(table)?;
    if actual != expected {
        return Err(datafusion_common::DataFusionError::Plan(format!(
            "Delta table version mismatch: expected {expected}, got {actual}. \
             Replay requires exact version match."
        )));
    }
    Ok(())
}

/// Get snapshot metadata for debugging and diagnostics.
///
/// # Arguments
/// * `table` - Delta table instance
///
/// # Returns
/// Tuple of (version, num_files, size_bytes).
///
/// # Errors
/// Returns error if snapshot access fails.
pub fn snapshot_metadata(table: &DeltaTable) -> DFResult<(i64, usize, i64)> {
    let version = table_version(table)?;
    let eager = eager_snapshot(table)?;

    // Count files and sum sizes from log_data
    let num_files = eager.log_data().iter().count();
    let size_bytes: i64 = eager
        .log_data()
        .iter()
        .map(|file_view| file_view.size())
        .sum();

    Ok((version, num_files, size_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_mode_variants() {
        let latest = SnapshotMode::Latest;
        let pinned = SnapshotMode::Pinned(42);

        assert_eq!(latest, SnapshotMode::Latest);
        assert_eq!(pinned, SnapshotMode::Pinned(42));
        assert_ne!(latest, pinned);
    }

    #[test]
    fn test_snapshot_mode_pattern_matching() {
        let mode = SnapshotMode::Pinned(100);

        match mode {
            SnapshotMode::Latest => panic!("Expected pinned"),
            SnapshotMode::Pinned(v) => assert_eq!(v, 100),
        }
    }
}
