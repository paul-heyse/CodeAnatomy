//! WS-P11: Post-execution Delta maintenance integration.
//!
//! Orchestrates compact -> checkpoint -> vacuum -> cleanup -> constraints on output
//! tables after successful materialization. Operations execute in strict dependency
//! order to ensure recoverability at each step.
//!
//! The struct contracts (`MaintenanceSchedule`, `CompactPolicy`, `VacuumPolicy`,
//! `ConstraintSpec`) are the spec-driven API surface. The `execute_maintenance`
//! function is a stub that documents the integration path; the integration agent
//! wires the actual `datafusion_ext::delta_maintenance::*` calls.

use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

/// Non-negotiable safety minimum: 7 days retention.
///
/// The engine enforces this floor regardless of what the spec requests.
/// This prevents accidental data loss from misconfigured vacuum policies.
const MIN_VACUUM_RETENTION_HOURS: u64 = 168;

/// Spec-driven post-execution Delta maintenance schedule.
///
/// Runs maintenance operations on output tables AFTER successful
/// materialization. Operations execute in dependency order:
/// compact -> checkpoint -> vacuum -> cleanup -> constraints.
///
/// This order ensures:
/// 1. Compact creates larger files before vacuum removes old ones
/// 2. Checkpoint creates a recovery point before vacuum
/// 3. Vacuum only runs after checkpoint ensures recoverability
/// 4. Constraints are validated last on the final table state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceSchedule {
    /// Which output tables to maintain (empty = all output tables).
    #[serde(default)]
    pub target_tables: Vec<String>,

    /// Compact small files into larger ones.
    pub compact: Option<CompactPolicy>,

    /// Vacuum unreferenced files.
    pub vacuum: Option<VacuumPolicy>,

    /// Create checkpoints for faster log replay.
    #[serde(default)]
    pub checkpoint: bool,

    /// Clean up old metadata.
    #[serde(default)]
    pub metadata_cleanup: bool,

    /// Add/validate check constraints on output tables.
    #[serde(default)]
    pub constraints: Vec<ConstraintSpec>,

    /// Maximum tables to maintain concurrently.
    /// Default is 1 (serialized) for safety.
    #[serde(default = "default_max_parallel")]
    pub max_parallel_tables: usize,

    /// Require table quiescence / exclusive write window for destructive operations.
    #[serde(default)]
    pub require_table_quiescence: bool,
}

fn default_max_parallel() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactPolicy {
    /// Target file size in bytes (default: 512 MB).
    #[serde(default = "default_target_file_size")]
    pub target_file_size: u64,
}

fn default_target_file_size() -> u64 {
    512 * 1024 * 1024 // 512 MB
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacuumPolicy {
    /// Minimum retention in hours (MUST be >= 168 = 7 days).
    /// The engine enforces this minimum regardless of spec value.
    pub retention_hours: u64,

    /// Require vacuum protocol check feature on the Delta table.
    #[serde(default = "default_true")]
    pub require_protocol_check: bool,

    /// Dry run first to see what would be deleted before executing.
    #[serde(default)]
    pub dry_run_first: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintSpec {
    /// Constraint name (used for add/drop identification).
    pub name: String,
    /// SQL expression for the check constraint.
    pub expression: String,
}

/// Maintenance execution report for a single operation on a single table.
///
/// This is the engine-level report type. When the integration agent wires
/// the actual Delta API calls, each operation will also produce a
/// `DeltaMaintenanceReport` from `datafusion_ext`; this struct captures
/// the engine-level view for inclusion in `RunResult`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceReport {
    /// Name of the output table this operation targeted.
    pub table_name: String,
    /// Operation identifier (e.g., "compact", "vacuum", "checkpoint").
    pub operation: String,
    /// Whether the operation completed successfully.
    pub success: bool,
    /// Human-readable status message or error detail.
    pub message: Option<String>,
}

/// Execute post-materialization maintenance on output tables.
///
/// Operations run in strict order per table:
/// compact -> checkpoint -> vacuum -> metadata_cleanup -> constraints.
///
/// This order ensures:
/// 1. Compact creates larger files before vacuum removes old ones
/// 2. Checkpoint creates a recovery point before vacuum
/// 3. Vacuum only runs after checkpoint ensures recoverability
/// 4. Constraints are validated last on the final table state
///
/// # Integration path
///
/// The actual Delta operations delegate to `datafusion_ext::delta_maintenance::*`:
/// - `delta_optimize_compact()` for compaction
/// - `delta_create_checkpoint()` for checkpoints
/// - `delta_vacuum()` for vacuum (with enforced minimum retention)
/// - `delta_cleanup_metadata()` for metadata cleanup
/// - `delta_add_constraints()` for check constraints
///
/// The integration agent will wire these calls, replacing the stub reports
/// with real `DeltaMaintenanceReport` results. Parameters like
/// `DeltaFeatureGate` and `DeltaCommitOptions` will be threaded through
/// from the execution spec at that time.
pub async fn execute_maintenance(
    _ctx: &SessionContext,
    schedule: &MaintenanceSchedule,
    output_locations: &[(String, String)], // (table_name, delta_uri)
) -> Result<Vec<MaintenanceReport>> {
    let mut reports = Vec::new();

    let targets: Vec<&(String, String)> = if schedule.target_tables.is_empty() {
        output_locations.iter().collect()
    } else {
        output_locations
            .iter()
            .filter(|(name, _)| schedule.target_tables.contains(name))
            .collect()
    };

    for (table_name, _uri) in targets {
        // 1) Compact
        if let Some(_compact) = &schedule.compact {
            reports.push(MaintenanceReport {
                table_name: table_name.clone(),
                operation: "compact".to_string(),
                success: true,
                message: Some(
                    "Compact scheduled via delta_optimize_compact (stub)".to_string(),
                ),
            });
        }

        // 2) Checkpoint
        if schedule.checkpoint {
            reports.push(MaintenanceReport {
                table_name: table_name.clone(),
                operation: "checkpoint".to_string(),
                success: true,
                message: Some(
                    "Checkpoint scheduled via delta_create_checkpoint (stub)".to_string(),
                ),
            });
        }

        // 3) Vacuum (with enforced minimum retention)
        if let Some(vacuum) = &schedule.vacuum {
            let safe_retention = safe_vacuum_retention(vacuum.retention_hours);
            let dry_run_note = if vacuum.dry_run_first {
                " (dry_run_first=true)"
            } else {
                ""
            };
            reports.push(MaintenanceReport {
                table_name: table_name.clone(),
                operation: "vacuum".to_string(),
                success: true,
                message: Some(format!(
                    "Vacuum scheduled with retention_hours={safe_retention} \
                     (min enforced: {MIN_VACUUM_RETENTION_HOURS}){dry_run_note}"
                )),
            });
        }

        // 4) Metadata cleanup
        if schedule.metadata_cleanup {
            reports.push(MaintenanceReport {
                table_name: table_name.clone(),
                operation: "metadata_cleanup".to_string(),
                success: true,
                message: Some(
                    "Metadata cleanup scheduled via delta_cleanup_metadata (stub)".to_string(),
                ),
            });
        }

        // 5) Constraints
        for constraint in &schedule.constraints {
            reports.push(MaintenanceReport {
                table_name: table_name.clone(),
                operation: format!("add_constraint:{}", constraint.name),
                success: true,
                message: Some(format!(
                    "Constraint '{}' scheduled: {} (stub)",
                    constraint.name, constraint.expression
                )),
            });
        }
    }

    Ok(reports)
}

/// Validate vacuum retention policy and return the safe retention hours.
///
/// Returns the requested value if it meets the minimum, otherwise returns
/// `MIN_VACUUM_RETENTION_HOURS` (168 = 7 days). This function is the single
/// enforcement point for the retention floor.
pub fn safe_vacuum_retention(requested: u64) -> u64 {
    requested.max(MIN_VACUUM_RETENTION_HOURS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_vacuum_retention_enforces_minimum() {
        // Below minimum -> forced to 168
        assert_eq!(safe_vacuum_retention(0), 168);
        assert_eq!(safe_vacuum_retention(24), 168);
        assert_eq!(safe_vacuum_retention(167), 168);

        // At minimum -> unchanged
        assert_eq!(safe_vacuum_retention(168), 168);

        // Above minimum -> unchanged
        assert_eq!(safe_vacuum_retention(200), 200);
        assert_eq!(safe_vacuum_retention(720), 720);
    }

    #[test]
    fn test_maintenance_schedule_serde_defaults() {
        let json = r#"{
            "compact": { "target_file_size": 1073741824 },
            "vacuum": { "retention_hours": 336 },
            "checkpoint": true
        }"#;
        let schedule: MaintenanceSchedule = serde_json::from_str(json).unwrap();

        assert!(schedule.target_tables.is_empty());
        assert!(schedule.compact.is_some());
        assert_eq!(schedule.compact.unwrap().target_file_size, 1073741824);
        assert!(schedule.vacuum.is_some());
        assert_eq!(schedule.vacuum.unwrap().retention_hours, 336);
        assert!(schedule.checkpoint);
        assert!(!schedule.metadata_cleanup);
        assert!(schedule.constraints.is_empty());
        assert_eq!(schedule.max_parallel_tables, 1);
        assert!(!schedule.require_table_quiescence);
    }

    #[test]
    fn test_compact_policy_default_file_size() {
        let json = r#"{}"#;
        let policy: CompactPolicy = serde_json::from_str(json).unwrap();
        assert_eq!(policy.target_file_size, 512 * 1024 * 1024);
    }

    #[test]
    fn test_vacuum_policy_defaults() {
        let json = r#"{ "retention_hours": 168 }"#;
        let policy: VacuumPolicy = serde_json::from_str(json).unwrap();
        assert_eq!(policy.retention_hours, 168);
        assert!(policy.require_protocol_check); // default true
        assert!(!policy.dry_run_first); // default false
    }

    #[test]
    fn test_constraint_spec_round_trip() {
        let spec = ConstraintSpec {
            name: "positive_rows".to_string(),
            expression: "row_count > 0".to_string(),
        };
        let json = serde_json::to_string(&spec).unwrap();
        let round_tripped: ConstraintSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped.name, "positive_rows");
        assert_eq!(round_tripped.expression, "row_count > 0");
    }

    #[tokio::test]
    async fn test_execute_maintenance_targets_all_when_empty() {
        let schedule = MaintenanceSchedule {
            target_tables: vec![],
            compact: Some(CompactPolicy {
                target_file_size: 512 * 1024 * 1024,
            }),
            vacuum: None,
            checkpoint: false,
            metadata_cleanup: false,
            constraints: vec![],
            max_parallel_tables: 1,
            require_table_quiescence: false,
        };
        let locations = vec![
            ("table_a".to_string(), "/tmp/table_a".to_string()),
            ("table_b".to_string(), "/tmp/table_b".to_string()),
        ];
        let ctx = SessionContext::new();
        let reports = execute_maintenance(&ctx, &schedule, &locations)
            .await
            .unwrap();
        // Both tables should get compact reports
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].table_name, "table_a");
        assert_eq!(reports[1].table_name, "table_b");
    }

    #[tokio::test]
    async fn test_execute_maintenance_filters_by_target_tables() {
        let schedule = MaintenanceSchedule {
            target_tables: vec!["table_b".to_string()],
            compact: Some(CompactPolicy {
                target_file_size: 512 * 1024 * 1024,
            }),
            vacuum: None,
            checkpoint: false,
            metadata_cleanup: false,
            constraints: vec![],
            max_parallel_tables: 1,
            require_table_quiescence: false,
        };
        let locations = vec![
            ("table_a".to_string(), "/tmp/table_a".to_string()),
            ("table_b".to_string(), "/tmp/table_b".to_string()),
        ];
        let ctx = SessionContext::new();
        let reports = execute_maintenance(&ctx, &schedule, &locations)
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].table_name, "table_b");
    }

    #[tokio::test]
    async fn test_execute_maintenance_operation_order() {
        let schedule = MaintenanceSchedule {
            target_tables: vec![],
            compact: Some(CompactPolicy {
                target_file_size: 512 * 1024 * 1024,
            }),
            vacuum: Some(VacuumPolicy {
                retention_hours: 336,
                require_protocol_check: true,
                dry_run_first: false,
            }),
            checkpoint: true,
            metadata_cleanup: true,
            constraints: vec![ConstraintSpec {
                name: "check_rows".to_string(),
                expression: "row_count >= 0".to_string(),
            }],
            max_parallel_tables: 1,
            require_table_quiescence: false,
        };
        let locations = vec![("output".to_string(), "/tmp/output".to_string())];
        let ctx = SessionContext::new();
        let reports = execute_maintenance(&ctx, &schedule, &locations)
            .await
            .unwrap();

        // Strict order: compact, checkpoint, vacuum, metadata_cleanup, constraint
        assert_eq!(reports.len(), 5);
        assert_eq!(reports[0].operation, "compact");
        assert_eq!(reports[1].operation, "checkpoint");
        assert_eq!(reports[2].operation, "vacuum");
        assert_eq!(reports[3].operation, "metadata_cleanup");
        assert_eq!(reports[4].operation, "add_constraint:check_rows");
    }

    #[tokio::test]
    async fn test_execute_maintenance_vacuum_enforces_minimum_retention() {
        let schedule = MaintenanceSchedule {
            target_tables: vec![],
            compact: None,
            vacuum: Some(VacuumPolicy {
                retention_hours: 24, // Below minimum
                require_protocol_check: false,
                dry_run_first: false,
            }),
            checkpoint: false,
            metadata_cleanup: false,
            constraints: vec![],
            max_parallel_tables: 1,
            require_table_quiescence: false,
        };
        let locations = vec![("t".to_string(), "/tmp/t".to_string())];
        let ctx = SessionContext::new();
        let reports = execute_maintenance(&ctx, &schedule, &locations)
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        // The message should show the enforced minimum, not the requested 24
        assert!(reports[0]
            .message
            .as_ref()
            .unwrap()
            .contains("retention_hours=168"));
    }
}
