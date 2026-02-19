use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use datafusion_ext::delta_mutations::{DeltaDeleteRequest, DeltaUpdateRequest};
use datafusion_ext::delta_protocol::TableVersion;

#[test]
fn native_dml_request_shapes_are_stable() {
    let ctx = SessionContext::new();

    let delete = DeltaDeleteRequest {
        session_ctx: &ctx,
        table_uri: "memory://demo",
        storage_options: None,
        table_version: TableVersion::Latest,
        predicate: Some("id > 0".to_string()),
        gate: None,
        commit_options: None,
    };
    assert_eq!(delete.table_uri, "memory://demo");
    assert_eq!(delete.predicate.as_deref(), Some("id > 0"));

    let mut updates = HashMap::new();
    updates.insert("status".to_string(), "'active'".to_string());
    let update = DeltaUpdateRequest {
        session_ctx: &ctx,
        table_uri: "memory://demo",
        storage_options: None,
        table_version: TableVersion::Latest,
        predicate: Some("id = 42".to_string()),
        updates,
        gate: None,
        commit_options: None,
    };
    assert_eq!(update.updates.len(), 1);
    assert_eq!(update.predicate.as_deref(), Some("id = 42"));
}
