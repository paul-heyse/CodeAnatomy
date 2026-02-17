use std::collections::BTreeMap;

use datafusion_ext::delta_observability::snapshot_info_as_values;
use datafusion_ext::delta_protocol::{snapshot_info_as_strings, DeltaSnapshotInfo};

fn sample_snapshot() -> DeltaSnapshotInfo {
    DeltaSnapshotInfo {
        table_uri: "file:///tmp/table".to_string(),
        version: 7,
        snapshot_timestamp: Some(1700000000),
        min_reader_version: 3,
        min_writer_version: 7,
        reader_features: vec!["deletionVectors".to_string()],
        writer_features: vec!["columnMapping".to_string()],
        table_properties: BTreeMap::new(),
        schema_json: "{\"type\":\"struct\"}".to_string(),
        partition_columns: vec!["dt".to_string()],
    }
}

#[test]
fn protocol_snapshot_payload_is_named_and_populated() {
    let payload = snapshot_info_as_strings(&sample_snapshot());
    assert_eq!(
        payload.get("table_uri"),
        Some(&"file:///tmp/table".to_string())
    );
    assert_eq!(payload.get("version"), Some(&"7".to_string()));
    assert_eq!(payload.get("min_reader_version"), Some(&"3".to_string()));
}

#[test]
fn observability_snapshot_payload_is_named_and_populated() {
    let payload = snapshot_info_as_values(&sample_snapshot());
    assert_eq!(payload["table_uri"], "file:///tmp/table");
    assert_eq!(payload["version"], 7);
    assert_eq!(payload["min_reader_version"], 3);
}
