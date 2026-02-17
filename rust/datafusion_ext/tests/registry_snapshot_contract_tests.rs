use datafusion::prelude::SessionContext;

use datafusion_ext::{registry_snapshot, udf_registry};

#[test]
fn registry_snapshot_exposes_getter_contract() {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx).unwrap();

    let snapshot = registry_snapshot::registry_snapshot(&ctx.state());
    assert_eq!(
        snapshot.version(),
        datafusion_ext::registry::RegistrySnapshot::CURRENT_VERSION
    );
    assert!(!snapshot.scalar().is_empty());
    assert!(snapshot.aliases().len() <= snapshot.scalar().len() + snapshot.aggregate().len());
}

#[test]
fn hook_capabilities_have_non_empty_names() {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx).unwrap();

    let capabilities = registry_snapshot::snapshot_hook_capabilities(&ctx.state());
    assert!(!capabilities.is_empty());
    assert!(capabilities.iter().all(|entry| !entry.name.is_empty()));
}
