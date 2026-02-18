use codeanatomy_engine::providers::{FilterPushdownStatus, PushdownProbe};

#[test]
fn pushdown_status_truth_table_roundtrip() {
    let statuses = [
        FilterPushdownStatus::Unsupported,
        FilterPushdownStatus::Inexact,
        FilterPushdownStatus::Exact,
    ];
    for status in statuses {
        let encoded = serde_json::to_string(&status).expect("encode status");
        let decoded: FilterPushdownStatus = serde_json::from_str(&encoded).expect("decode status");
        assert_eq!(status, decoded);
    }
}

#[test]
fn pushdown_probe_counts_preserve_exact_inexact_unsupported() {
    let probe = PushdownProbe {
        provider: "events".to_string(),
        filter_sql: vec![
            "id > 0".to_string(),
            "kind = 'def'".to_string(),
            "path IS NOT NULL".to_string(),
        ],
        statuses: vec![
            FilterPushdownStatus::Exact,
            FilterPushdownStatus::Inexact,
            FilterPushdownStatus::Unsupported,
        ],
    };

    let counts = probe.status_counts();
    assert_eq!(counts.exact, 1);
    assert_eq!(counts.inexact, 1);
    assert_eq!(counts.unsupported, 1);
    assert!(!probe.all_exact());
    assert!(probe.has_inexact());
    assert!(probe.has_unsupported());
}
