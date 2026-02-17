use codeanatomy_engine::executor::result::{WriteOutcome, WriteOutcomeUnavailableReason};

#[test]
fn write_outcome_serializes_captured_variant() {
    let outcome = WriteOutcome::Captured {
        delta_version: 42,
        files_added: 3,
        bytes_written: 1024,
    };

    let value = serde_json::to_value(&outcome).unwrap();
    assert_eq!(value["status"], "captured");
    assert_eq!(value["delta_version"], 42);
    assert_eq!(value["files_added"], 3);
    assert_eq!(value["bytes_written"], 1024);
}

#[test]
fn write_outcome_serializes_unavailable_variant() {
    let outcome = WriteOutcome::Unavailable {
        reason: WriteOutcomeUnavailableReason::SnapshotReadFailed,
    };

    let value = serde_json::to_value(&outcome).unwrap();
    assert_eq!(value["status"], "unavailable");
    assert_eq!(value["reason"], "snapshot_read_failed");
}
