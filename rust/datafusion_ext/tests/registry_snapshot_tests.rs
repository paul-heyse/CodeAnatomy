use datafusion::prelude::SessionContext;
use datafusion_ext::registry::snapshot::registry_snapshot;
use datafusion_ext::registry_snapshot as legacy_snapshot;

#[test]
fn decomposed_snapshot_matches_legacy_facade() {
    let ctx = SessionContext::new();
    let state = ctx.state();
    let via_new = registry_snapshot(&state);
    let via_legacy = legacy_snapshot::registry_snapshot(&state);
    assert_eq!(via_new.scalar, via_legacy.scalar);
    assert_eq!(via_new.aggregate, via_legacy.aggregate);
    assert_eq!(via_new.window, via_legacy.window);
    assert_eq!(via_new.table, via_legacy.table);
}
