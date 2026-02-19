use std::path::Path;

#[test]
fn cache_metrics_payload_includes_schema_version() {
    let source = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("codeanatomy_ext")
            .join("cache_tables.rs"),
    )
    .expect("read cache_tables.rs");
    assert!(
        source.contains("\"schema_version\": 1"),
        "cache metrics payload must include schema_version"
    );
}
