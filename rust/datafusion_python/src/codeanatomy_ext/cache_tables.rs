//! Cache-table registration surface.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Expr;
use datafusion_common::{DataFusionError, Result};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::helpers::{extract_session_ctx, json_to_py};

fn now_unix_ms_or(fallback: i64) -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(fallback)
}

fn now_unix_ms() -> i64 {
    now_unix_ms_or(-1)
}

#[derive(Debug, Clone)]
struct CacheSnapshotConfig {
    list_files_cache_ttl: Option<String>,
    list_files_cache_limit: Option<String>,
    metadata_cache_limit: Option<String>,
    predicate_cache_size: Option<String>,
}

impl CacheSnapshotConfig {
    fn from_map(config: Option<HashMap<String, String>>) -> Self {
        let mut config_map = config.unwrap_or_default();
        Self {
            list_files_cache_ttl: config_map.remove("list_files_cache_ttl"),
            list_files_cache_limit: config_map.remove("list_files_cache_limit"),
            metadata_cache_limit: config_map.remove("metadata_cache_limit"),
            predicate_cache_size: config_map.remove("predicate_cache_size"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CacheTableKind {
    ListFiles,
    Metadata,
    Predicate,
    Statistics,
}

#[derive(Debug)]
struct CacheTableFunction {
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    config: CacheSnapshotConfig,
    kind: CacheTableKind,
}

impl CacheTableFunction {
    fn cache_name(&self) -> &'static str {
        match self.kind {
            CacheTableKind::ListFiles => "list_files",
            CacheTableKind::Metadata => "metadata",
            CacheTableKind::Predicate => "predicate",
            CacheTableKind::Statistics => "statistics",
        }
    }

    fn config_ttl(&self) -> Option<&str> {
        match self.kind {
            CacheTableKind::ListFiles => self.config.list_files_cache_ttl.as_deref(),
            CacheTableKind::Metadata | CacheTableKind::Predicate | CacheTableKind::Statistics => {
                None
            }
        }
    }

    fn config_limit(&self) -> Option<&str> {
        match self.kind {
            CacheTableKind::ListFiles => self.config.list_files_cache_limit.as_deref(),
            CacheTableKind::Metadata => self.config.metadata_cache_limit.as_deref(),
            CacheTableKind::Predicate | CacheTableKind::Statistics => {
                self.config.predicate_cache_size.as_deref()
            }
        }
    }

    fn entry_count(&self) -> Option<i64> {
        match self.kind {
            CacheTableKind::ListFiles => self
                .runtime_env
                .cache_manager
                .get_list_files_cache()
                .map(|cache| cache.len() as i64),
            CacheTableKind::Metadata => {
                let cache = self.runtime_env.cache_manager.get_file_metadata_cache();
                Some(cache.list_entries().len() as i64)
            }
            CacheTableKind::Predicate | CacheTableKind::Statistics => self
                .runtime_env
                .cache_manager
                .get_file_statistic_cache()
                .map(|cache| cache.len() as i64),
        }
    }

    fn hit_count(&self) -> Option<i64> {
        if matches!(self.kind, CacheTableKind::Metadata) {
            let cache = self.runtime_env.cache_manager.get_file_metadata_cache();
            let hits: i64 = cache
                .list_entries()
                .values()
                .map(|entry| entry.hits as i64)
                .sum();
            return Some(hits);
        }
        None
    }

    fn make_table(&self) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("cache_name", DataType::Utf8, false),
            Field::new("event_time_unix_ms", DataType::Int64, false),
            Field::new("entry_count", DataType::Int64, true),
            Field::new("hit_count", DataType::Int64, true),
            Field::new("miss_count", DataType::Int64, true),
            Field::new("eviction_count", DataType::Int64, true),
            Field::new("config_ttl", DataType::Utf8, true),
            Field::new("config_limit", DataType::Utf8, true),
        ]));
        let cache_name = StringArray::from(vec![Some(self.cache_name())]);
        let event_time = Int64Array::from(vec![Some(now_unix_ms())]);
        let entry_count = Int64Array::from(vec![self.entry_count()]);
        let hit_count = Int64Array::from(vec![self.hit_count()]);
        let miss_count = Int64Array::from(vec![None]);
        let eviction_count = Int64Array::from(vec![None]);
        let config_ttl = StringArray::from(vec![self.config_ttl()]);
        let config_limit = StringArray::from(vec![self.config_limit()]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(cache_name),
                Arc::new(event_time),
                Arc::new(entry_count),
                Arc::new(hit_count),
                Arc::new(miss_count),
                Arc::new(eviction_count),
                Arc::new(config_ttl),
                Arc::new(config_limit),
            ],
        )
        .map_err(|err| DataFusionError::Plan(format!("Failed to build cache table: {err}")))?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(table))
    }
}

impl TableFunctionImpl for CacheTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn datafusion::catalog::TableProvider>> {
        if !args.is_empty() {
            return Err(DataFusionError::Plan(
                "Cache table functions do not accept arguments.".into(),
            ));
        }
        self.make_table()
    }
}

fn register_cache_table_functions(ctx: &SessionContext, config: CacheSnapshotConfig) -> Result<()> {
    let runtime_env = Arc::clone(ctx.state().runtime_env());
    let list_files = CacheTableFunction {
        runtime_env: Arc::clone(&runtime_env),
        config: config.clone(),
        kind: CacheTableKind::ListFiles,
    };
    let metadata = CacheTableFunction {
        runtime_env: Arc::clone(&runtime_env),
        config: config.clone(),
        kind: CacheTableKind::Metadata,
    };
    let predicate = CacheTableFunction {
        runtime_env: Arc::clone(&runtime_env),
        config: config.clone(),
        kind: CacheTableKind::Predicate,
    };
    let statistics = CacheTableFunction {
        runtime_env,
        config,
        kind: CacheTableKind::Statistics,
    };
    ctx.register_udtf("list_files_cache", Arc::new(list_files));
    ctx.register_udtf("metadata_cache", Arc::new(metadata));
    ctx.register_udtf("predicate_cache", Arc::new(predicate));
    ctx.register_udtf("statistics_cache", Arc::new(statistics));
    Ok(())
}

fn saturating_i64_from_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn saturating_i64_from_u64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(crate) fn runtime_execution_metrics_payload(ctx: &SessionContext) -> serde_json::Value {
    let runtime_env = Arc::clone(ctx.state().runtime_env());
    let metadata_entries = runtime_env
        .cache_manager
        .get_file_metadata_cache()
        .list_entries();
    let metadata_cache_hits: i64 = metadata_entries
        .values()
        .map(|entry| saturating_i64_from_usize(entry.hits))
        .sum();
    let list_files_cache_entries = runtime_env
        .cache_manager
        .get_list_files_cache()
        .map(|cache| saturating_i64_from_usize(cache.len()));
    let statistics_cache_entries = runtime_env
        .cache_manager
        .get_file_statistic_cache()
        .map(|cache| saturating_i64_from_usize(cache.len()));
    let metadata_cache_limit =
        saturating_i64_from_usize(runtime_env.cache_manager.get_metadata_cache_limit());
    let list_files_cache_limit =
        saturating_i64_from_usize(runtime_env.cache_manager.get_list_files_cache_limit());
    let list_files_cache_ttl_seconds = runtime_env
        .cache_manager
        .get_list_files_cache_ttl()
        .map(|ttl| saturating_i64_from_u64(ttl.as_secs()));
    let metadata_cache_entries = saturating_i64_from_usize(metadata_entries.len());
    let memory_reserved = saturating_i64_from_usize(runtime_env.memory_pool.reserved());
    let (memory_limit_kind, memory_limit_bytes) = match runtime_env.memory_pool.memory_limit() {
        datafusion::execution::memory_pool::MemoryLimit::Finite(value) => {
            ("finite", Some(saturating_i64_from_usize(value)))
        }
        datafusion::execution::memory_pool::MemoryLimit::Infinite => ("infinite", None),
        datafusion::execution::memory_pool::MemoryLimit::Unknown => ("unknown", None),
    };

    let mut rows = vec![
        serde_json::json!({
            "scope": "runtime",
            "metric_name": "memory_reserved_bytes",
            "metric_type": "gauge",
            "unit": "bytes",
            "value": memory_reserved,
        }),
        serde_json::json!({
            "scope": "runtime",
            "metric_name": "metadata_cache_limit_bytes",
            "metric_type": "gauge",
            "unit": "bytes",
            "value": metadata_cache_limit,
        }),
        serde_json::json!({
            "scope": "runtime",
            "metric_name": "metadata_cache_entries",
            "metric_type": "gauge",
            "unit": "count",
            "value": metadata_cache_entries,
        }),
        serde_json::json!({
            "scope": "runtime",
            "metric_name": "metadata_cache_hits",
            "metric_type": "counter",
            "unit": "count",
            "value": metadata_cache_hits,
        }),
        serde_json::json!({
            "scope": "runtime",
            "metric_name": "list_files_cache_limit_bytes",
            "metric_type": "gauge",
            "unit": "bytes",
            "value": list_files_cache_limit,
        }),
    ];
    if let Some(value) = list_files_cache_entries {
        rows.push(serde_json::json!({
            "scope": "runtime",
            "metric_name": "list_files_cache_entries",
            "metric_type": "gauge",
            "unit": "count",
            "value": value,
        }));
    }
    if let Some(value) = statistics_cache_entries {
        rows.push(serde_json::json!({
            "scope": "runtime",
            "metric_name": "statistics_cache_entries",
            "metric_type": "gauge",
            "unit": "count",
            "value": value,
        }));
    }
    if let Some(value) = list_files_cache_ttl_seconds {
        rows.push(serde_json::json!({
            "scope": "runtime",
            "metric_name": "list_files_cache_ttl_seconds",
            "metric_type": "gauge",
            "unit": "seconds",
            "value": value,
        }));
    }

    serde_json::json!({
        "schema_version": 1,
        "event_time_unix_ms": now_unix_ms(),
        "summary": {
            "memory_reserved_bytes": memory_reserved,
            "memory_limit_kind": memory_limit_kind,
            "memory_limit_bytes": memory_limit_bytes,
            "metadata_cache_limit_bytes": metadata_cache_limit,
            "list_files_cache_limit_bytes": list_files_cache_limit,
            "list_files_cache_ttl_seconds": list_files_cache_ttl_seconds,
            "metadata_cache_entries": metadata_cache_entries,
            "metadata_cache_hits": metadata_cache_hits,
            "list_files_cache_entries": list_files_cache_entries,
            "statistics_cache_entries": statistics_cache_entries,
        },
        "rows": rows,
    })
}

#[pyfunction]
pub(crate) fn runtime_execution_metrics_snapshot(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    json_to_py(
        py,
        &runtime_execution_metrics_payload(&extract_session_ctx(ctx)?),
    )
}

#[pyfunction]
pub(crate) fn register_cache_tables(
    ctx: &Bound<'_, PyAny>,
    config: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let snapshot_config = CacheSnapshotConfig::from_map(config);
    register_cache_table_functions(&extract_session_ctx(ctx)?, snapshot_config).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to register cache table functions: {err}"))
    })?;
    Ok(())
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(register_cache_tables, module)?)?;
    module.add_function(wrap_pyfunction!(
        runtime_execution_metrics_snapshot,
        module
    )?)?;
    Ok(())
}
