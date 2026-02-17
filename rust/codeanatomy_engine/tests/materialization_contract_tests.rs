use codeanatomy_engine::executor::result::{
    MaterializationResult, WriteOutcome, WriteOutcomeUnavailableReason,
};

#[test]
fn materialization_result_serializes_nested_captured_outcome() {
    let result = MaterializationResult {
        table_name: "target".to_string(),
        delta_location: Some("/tmp/target".to_string()),
        rows_written: 11,
        partition_count: 2,
        write_outcome: WriteOutcome::Captured {
            delta_version: 7,
            files_added: 3,
            bytes_written: 2048,
        },
    };

    let value = serde_json::to_value(&result).expect("serialize materialization result");
    assert_eq!(value["table_name"], "target");
    assert_eq!(value["rows_written"], 11);
    assert_eq!(value["write_outcome"]["status"], "captured");
    assert_eq!(value["write_outcome"]["delta_version"], 7);
    assert_eq!(value["write_outcome"]["files_added"], 3);
    assert_eq!(value["write_outcome"]["bytes_written"], 2048);
    assert!(value.get("delta_version").is_none());
    assert!(value.get("files_added").is_none());
    assert!(value.get("bytes_written").is_none());
}

#[test]
fn materialization_result_serializes_nested_unavailable_outcome() {
    let result = MaterializationResult {
        table_name: "target".to_string(),
        delta_location: None,
        rows_written: 0,
        partition_count: 0,
        write_outcome: WriteOutcome::Unavailable {
            reason: WriteOutcomeUnavailableReason::TableLoadFailed,
        },
    };

    let value = serde_json::to_value(&result).expect("serialize materialization result");
    assert_eq!(value["write_outcome"]["status"], "unavailable");
    assert_eq!(value["write_outcome"]["reason"], "table_load_failed");
}
