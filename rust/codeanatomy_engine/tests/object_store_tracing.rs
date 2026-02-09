#![cfg(feature = "tracing")]

use codeanatomy_engine::rules::registry::CpgRuleSet;
use codeanatomy_engine::executor::tracing as engine_tracing;
use codeanatomy_engine::session::factory::SessionFactory;
use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};
use codeanatomy_engine::spec::runtime::{RuleTraceMode, TracingConfig};

#[tokio::test]
async fn test_instrumented_file_store_registration_keeps_session_usable() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);
    let tracing_config = TracingConfig {
        enabled: true,
        rule_mode: RuleTraceMode::Disabled,
        instrument_object_store: true,
        ..TracingConfig::default()
    };

    let (ctx, _envelope) = factory
        .build_session(&ruleset, [1u8; 32], Some(&tracing_config))
        .await
        .expect("session should build with instrumented object store");

    let df = ctx.sql("SELECT 1 AS test").await.expect("query should parse");
    let batches = df.collect().await.expect("query should execute");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

#[tokio::test]
async fn test_store_registration_rejects_unsupported_scheme() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);
    let tracing_config = TracingConfig {
        enabled: true,
        instrument_object_store: true,
        ..TracingConfig::default()
    };
    let (ctx, _envelope) = factory
        .build_session(&ruleset, [2u8; 32], Some(&tracing_config))
        .await
        .expect("session should build");

    let err = engine_tracing::register_instrumented_stores_for_locations(
        &ctx,
        &tracing_config,
        &[String::from("ftp://example.com/table")],
    )
    .expect_err("unsupported scheme should fail");
    let message = err.to_string();
    assert!(
        message.contains("Unsupported object store scheme")
            || message.contains("Invalid Delta table location"),
        "unexpected error message: {message}",
    );
}

#[tokio::test]
async fn test_store_registration_deduplicates_same_scheme_root() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);
    let tracing_config = TracingConfig {
        enabled: true,
        instrument_object_store: true,
        ..TracingConfig::default()
    };
    let (ctx, _envelope) = factory
        .build_session(&ruleset, [3u8; 32], Some(&tracing_config))
        .await
        .expect("session should build");

    engine_tracing::register_instrumented_stores_for_locations(
        &ctx,
        &tracing_config,
        &[
            String::from("file:///tmp/a"),
            String::from("file:///tmp/b"),
            String::from("/tmp/c"),
        ],
    )
    .expect("duplicate roots should be handled deterministically");
}
