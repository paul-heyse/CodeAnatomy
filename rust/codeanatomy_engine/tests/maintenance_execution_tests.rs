use codeanatomy_engine::executor::maintenance::{
    execute_maintenance, safe_vacuum_retention, MaintenanceSchedule,
};
use datafusion::prelude::SessionContext;

#[test]
fn safe_vacuum_retention_enforces_minimum() {
    assert_eq!(safe_vacuum_retention(0), 168);
    assert_eq!(safe_vacuum_retention(24), 168);
    assert_eq!(safe_vacuum_retention(168), 168);
    assert_eq!(safe_vacuum_retention(336), 336);
}

#[tokio::test]
async fn execute_maintenance_no_enabled_operations_is_noop() {
    let schedule = MaintenanceSchedule {
        target_tables: vec![],
        compact: None,
        vacuum: None,
        checkpoint: false,
        metadata_cleanup: false,
        constraints: vec![],
        max_parallel_tables: 1,
        require_table_quiescence: false,
    };
    let output_locations = vec![("target".to_string(), "/tmp/target".to_string())];

    let reports = execute_maintenance(&SessionContext::new(), &schedule, &output_locations)
        .await
        .expect("no-op maintenance should succeed");
    assert!(reports.is_empty());
}

#[tokio::test]
async fn execute_maintenance_skips_non_targeted_tables() {
    let schedule = MaintenanceSchedule {
        target_tables: vec!["other".to_string()],
        compact: None,
        vacuum: None,
        checkpoint: false,
        metadata_cleanup: false,
        constraints: vec![],
        max_parallel_tables: 1,
        require_table_quiescence: false,
    };
    let output_locations = vec![("target".to_string(), "/tmp/target".to_string())];

    let reports = execute_maintenance(&SessionContext::new(), &schedule, &output_locations)
        .await
        .expect("non-targeted tables should be skipped");
    assert!(reports.is_empty());
}
