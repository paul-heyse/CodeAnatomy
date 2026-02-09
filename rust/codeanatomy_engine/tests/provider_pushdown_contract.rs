//! Scope 6: Dedicated pushdown contract integration tests.
//!
//! Validates PushdownProbe construction with known filter statuses
//! and serialization round-trip behavior.

use std::any::Any;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use codeanatomy_engine::providers::pushdown_contract::{
    FilterPushdownStatus, PushdownProbe, PushdownStatusCounts,
};
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;

#[derive(Debug)]
struct InexactPushdownProvider {
    inner: Arc<dyn TableProvider>,
}

#[async_trait]
impl TableProvider for InexactPushdownProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

/// Scope 6: PushdownProbe with all-exact statuses reports all_exact = true.
#[test]
fn test_pushdown_probe_all_exact_statuses() {
    let probe = PushdownProbe {
        provider: "parquet_table".to_string(),
        filter_sql: vec![
            "id > 100".to_string(),
            "category = 'A'".to_string(),
        ],
        statuses: vec![
            FilterPushdownStatus::Exact,
            FilterPushdownStatus::Exact,
        ],
    };

    assert!(probe.all_exact(), "all Exact statuses must report all_exact");
    assert!(!probe.has_unsupported());
    assert!(!probe.has_inexact());
}

/// Scope 6: PushdownProbe with mixed statuses reports correct predicates.
#[test]
fn test_pushdown_probe_mixed_statuses() {
    let probe = PushdownProbe {
        provider: "delta_table".to_string(),
        filter_sql: vec![
            "ts > '2026-01-01'".to_string(),
            "complex_udf(x) = 1".to_string(),
            "partition_col = 'abc'".to_string(),
        ],
        statuses: vec![
            FilterPushdownStatus::Inexact,
            FilterPushdownStatus::Unsupported,
            FilterPushdownStatus::Exact,
        ],
    };

    assert!(!probe.all_exact());
    assert!(probe.has_unsupported());
    assert!(probe.has_inexact());

    let counts = probe.status_counts();
    assert_eq!(counts.exact, 1);
    assert_eq!(counts.inexact, 1);
    assert_eq!(counts.unsupported, 1);
}

/// Scope 6: PushdownProbe serialization round-trip preserves all fields.
#[test]
fn test_pushdown_probe_serde_roundtrip_preserves_fields() {
    let original = PushdownProbe {
        provider: "roundtrip_test".to_string(),
        filter_sql: vec![
            "col_a > 10".to_string(),
            "col_b IS NOT NULL".to_string(),
            "col_c IN ('x', 'y')".to_string(),
        ],
        statuses: vec![
            FilterPushdownStatus::Exact,
            FilterPushdownStatus::Inexact,
            FilterPushdownStatus::Unsupported,
        ],
    };

    let json = serde_json::to_string_pretty(&original).unwrap();
    let deserialized: PushdownProbe = serde_json::from_str(&json).unwrap();

    // All fields must survive the round-trip.
    assert_eq!(original.provider, deserialized.provider);
    assert_eq!(original.filter_sql, deserialized.filter_sql);
    assert_eq!(original.statuses, deserialized.statuses);

    // Status counts must match.
    let orig_counts = original.status_counts();
    let deser_counts = deserialized.status_counts();
    assert_eq!(orig_counts.exact, deser_counts.exact);
    assert_eq!(orig_counts.inexact, deser_counts.inexact);
    assert_eq!(orig_counts.unsupported, deser_counts.unsupported);
}

/// Scope 6: FilterPushdownStatus individual variants serialize correctly.
#[test]
fn test_filter_pushdown_status_individual_serde() {
    for status in [
        FilterPushdownStatus::Unsupported,
        FilterPushdownStatus::Inexact,
        FilterPushdownStatus::Exact,
    ] {
        let json = serde_json::to_string(&status).unwrap();
        let back: FilterPushdownStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, back, "round-trip failed for {status:?}");
    }
}

/// Scope 6: PushdownStatusCounts default is all zeros.
#[test]
fn test_pushdown_status_counts_default() {
    let counts = PushdownStatusCounts::default();
    assert_eq!(counts.exact, 0);
    assert_eq!(counts.inexact, 0);
    assert_eq!(counts.unsupported, 0);
}

/// Scope 6: Empty probe is vacuously all_exact with no unsupported/inexact.
#[test]
fn test_empty_probe_is_vacuously_all_exact() {
    let probe = PushdownProbe {
        provider: "empty".to_string(),
        filter_sql: vec![],
        statuses: vec![],
    };

    assert!(probe.all_exact(), "empty probe is vacuously all_exact");
    assert!(!probe.has_unsupported());
    assert!(!probe.has_inexact());
}

/// Scope 6: `Inexact` pushdown keeps a residual filter in the optimized plan.
#[tokio::test]
async fn test_inexact_pushdown_preserves_residual_filter() {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let inner = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()) as Arc<dyn TableProvider>;
    let provider = Arc::new(InexactPushdownProvider { inner });
    ctx.register_table("inexact_t", provider).unwrap();

    let df = ctx
        .sql("SELECT id FROM inexact_t WHERE id > 1")
        .await
        .unwrap();
    let optimized = ctx.state().optimize(df.logical_plan()).unwrap();
    let optimized_text = format!("{}", optimized.display_indent());

    assert!(
        optimized_text.contains("Filter:"),
        "inexact pushdown must retain a residual Filter node; plan=\n{optimized_text}"
    );
    assert!(
        optimized_text.contains("TableScan: inexact_t"),
        "optimized plan should still scan the expected table; plan=\n{optimized_text}"
    );
    assert!(
        optimized_text.contains("partial_filters")
            || optimized_text.contains("full_filters")
            || optimized_text.contains("filters=["),
        "table scan should include pushed filter metadata for inexact pushdown; plan=\n{optimized_text}"
    );
}
