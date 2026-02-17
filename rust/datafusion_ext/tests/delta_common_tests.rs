use datafusion_ext::delta_common::{eager_snapshot, parse_rfc3339};
use deltalake::errors::DeltaTableError;
use deltalake::kernel::EagerSnapshot;
use deltalake::DeltaTable;

#[test]
fn parse_rfc3339_accepts_valid_timestamp() {
    let parsed = parse_rfc3339("2024-01-01T12:34:56Z").expect("parse timestamp");
    assert_eq!(parsed.timestamp(), 1_704_112_496);
}

#[test]
fn parse_rfc3339_rejects_invalid_timestamp() {
    assert!(parse_rfc3339("not-a-timestamp").is_err());
}

#[test]
fn eager_snapshot_signature_is_stable() {
    let _fn_ptr: fn(&DeltaTable) -> Result<EagerSnapshot, DeltaTableError> = eager_snapshot;
}
