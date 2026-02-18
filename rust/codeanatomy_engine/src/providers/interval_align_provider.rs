//! Interval-align provider implementation.

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::{MemTable, TableType};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{dml::InsertOp, Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result, Statistics};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct IntervalTieBreaker {
    pub column: String,
    pub order: String,
}

/// Configuration contract for interval alignment provider planning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct IntervalAlignProviderConfig {
    pub mode: String,
    pub how: String,
    pub left_path_col: String,
    pub left_start_col: String,
    pub left_end_col: String,
    pub right_path_col: String,
    pub right_start_col: String,
    pub right_end_col: String,
    pub select_left: Vec<String>,
    pub select_right: Vec<String>,
    pub tie_breakers: Vec<IntervalTieBreaker>,
    pub emit_match_meta: bool,
    pub match_kind_col: String,
    pub match_score_col: String,
    pub right_suffix: String,
}

impl Default for IntervalAlignProviderConfig {
    fn default() -> Self {
        Self {
            mode: "CONTAINED_BEST".to_string(),
            how: "inner".to_string(),
            left_path_col: "path".to_string(),
            left_start_col: "bstart".to_string(),
            left_end_col: "bend".to_string(),
            right_path_col: "path".to_string(),
            right_start_col: "bstart".to_string(),
            right_end_col: "bend".to_string(),
            select_left: Vec::new(),
            select_right: Vec::new(),
            tie_breakers: Vec::new(),
            emit_match_meta: true,
            match_kind_col: "match_kind".to_string(),
            match_score_col: "match_score".to_string(),
            right_suffix: "__r".to_string(),
        }
    }
}

/// TableProvider wrapper for interval-aligned output.
#[derive(Debug)]
pub struct IntervalAlignProvider {
    inner: Arc<dyn TableProvider>,
    #[allow(dead_code)]
    config: IntervalAlignProviderConfig,
}

impl IntervalAlignProvider {
    pub fn new(inner: Arc<dyn TableProvider>, config: IntervalAlignProviderConfig) -> Self {
        Self { inner, config }
    }
}

#[async_trait::async_trait]
impl TableProvider for IntervalAlignProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
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
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_lit(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn select_columns(selected: &[String], available: &[String]) -> Vec<String> {
    if selected.is_empty() {
        return available.to_vec();
    }
    let available_set: HashSet<&str> = available.iter().map(String::as_str).collect();
    let mut out: Vec<String> = Vec::new();
    for name in selected {
        if available_set.contains(name.as_str()) && !out.iter().any(|item| item == name) {
            out.push(name.clone());
        }
    }
    if out.is_empty() {
        available.to_vec()
    } else {
        out
    }
}

fn resolve_right_aliases(
    right_columns: &[String],
    left_columns: &[String],
    suffix: &str,
    match_kind_col: &str,
    match_score_col: &str,
) -> Vec<(String, String)> {
    let mut used: HashSet<String> = left_columns.iter().cloned().collect();
    used.insert(match_kind_col.to_string());
    used.insert(match_score_col.to_string());
    let mut aliases: Vec<(String, String)> = Vec::with_capacity(right_columns.len());
    for original in right_columns {
        let mut candidate = if used.contains(original) {
            format!("{original}{suffix}")
        } else {
            original.clone()
        };
        while used.contains(candidate.as_str()) {
            candidate.push_str("_r");
        }
        used.insert(candidate.clone());
        aliases.push((original.clone(), candidate));
    }
    aliases
}

fn mode_condition(config: &IntervalAlignProviderConfig) -> String {
    let l_start = format!("l.{}", quote_ident(config.left_start_col.as_str()));
    let l_end = format!("l.{}", quote_ident(config.left_end_col.as_str()));
    let r_start = format!("r.{}", quote_ident(config.right_start_col.as_str()));
    let r_end = format!("r.{}", quote_ident(config.right_end_col.as_str()));
    match config.mode.trim().to_ascii_uppercase().as_str() {
        "EXACT" => format!("{r_start} = {l_start} AND {r_end} = {l_end}"),
        "OVERLAP_MAX" => format!("{r_start} < {l_end} AND {r_end} > {l_start}"),
        _ => format!("{r_start} <= {l_start} AND {r_end} >= {l_end}"),
    }
}

fn build_interval_align_sql(
    config: &IntervalAlignProviderConfig,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<String> {
    let left_available: Vec<String> = left_schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    let right_available: Vec<String> = right_schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();
    if left_available.is_empty() || right_available.is_empty() {
        return Err(DataFusionError::Plan(
            "interval_align_table requires non-empty left and right schemas".to_string(),
        ));
    }

    let left_keep = select_columns(config.select_left.as_slice(), left_available.as_slice());
    let right_keep = select_columns(config.select_right.as_slice(), right_available.as_slice());
    let right_aliases = resolve_right_aliases(
        right_keep.as_slice(),
        left_keep.as_slice(),
        config.right_suffix.as_str(),
        config.match_kind_col.as_str(),
        config.match_score_col.as_str(),
    );
    let left_available_set: HashSet<&str> = left_available.iter().map(String::as_str).collect();
    let right_available_set: HashSet<&str> = right_available.iter().map(String::as_str).collect();

    for required in [
        config.left_path_col.as_str(),
        config.left_start_col.as_str(),
        config.left_end_col.as_str(),
    ] {
        if !left_available_set.contains(required) {
            return Err(DataFusionError::Plan(format!(
                "Left input is missing required column: {required}"
            )));
        }
    }
    for required in [
        config.right_path_col.as_str(),
        config.right_start_col.as_str(),
        config.right_end_col.as_str(),
    ] {
        if !right_available_set.contains(required) {
            return Err(DataFusionError::Plan(format!(
                "Right input is missing required column: {required}"
            )));
        }
    }

    let join_mode = config.how.trim().to_ascii_lowercase();
    if join_mode != "inner" && join_mode != "left" {
        return Err(DataFusionError::Plan(format!(
            "interval_align_table unsupported how mode: {}",
            config.how
        )));
    }
    let join_keyword = if join_mode == "left" {
        "LEFT JOIN"
    } else {
        "JOIN"
    };

    let l_path = format!("l.{}", quote_ident(config.left_path_col.as_str()));
    let r_path = format!("r.{}", quote_ident(config.right_path_col.as_str()));
    let l_start = format!("l.{}", quote_ident(config.left_start_col.as_str()));
    let l_end = format!("l.{}", quote_ident(config.left_end_col.as_str()));
    let r_start = format!("r.{}", quote_ident(config.right_start_col.as_str()));
    let r_end = format!("r.{}", quote_ident(config.right_end_col.as_str()));
    let overlap_expr = format!(
        "CASE WHEN LEAST(CAST({l_end} AS BIGINT), CAST({r_end} AS BIGINT)) > \\
         GREATEST(CAST({l_start} AS BIGINT), CAST({r_start} AS BIGINT)) \\
         THEN CAST(LEAST(CAST({l_end} AS BIGINT), CAST({r_end} AS BIGINT)) - \\
              GREATEST(CAST({l_start} AS BIGINT), CAST({r_start} AS BIGINT)) AS DOUBLE) \\
         ELSE 0.0 END"
    );

    let mut order_exprs: Vec<String> = vec!["__score DESC".to_string()];
    for tie_breaker in config.tie_breakers.as_slice() {
        let column = tie_breaker.column.trim();
        if column.is_empty() {
            continue;
        }
        let order = if tie_breaker.order.trim().eq_ignore_ascii_case("descending") {
            "DESC"
        } else {
            "ASC"
        };
        if right_available_set.contains(column) {
            order_exprs.push(format!("r.{} {order}", quote_ident(column)));
        } else if left_available_set.contains(column) {
            order_exprs.push(format!("l.{} {order}", quote_ident(column)));
        }
    }
    order_exprs.push(format!("r.{} ASC", quote_ident(config.right_start_col.as_str())));
    order_exprs.push(format!("r.{} ASC", quote_ident(config.right_end_col.as_str())));

    let mut matched_select_parts: Vec<String> = vec!["l.__left_row AS __left_row".to_string()];
    for (original, alias) in right_aliases.as_slice() {
        matched_select_parts.push(format!(
            "r.{} AS {}",
            quote_ident(original.as_str()),
            quote_ident(alias.as_str())
        ));
    }
    matched_select_parts.push(format!("{overlap_expr} AS __score"));
    matched_select_parts.push(format!(
        "ROW_NUMBER() OVER (PARTITION BY l.__left_row ORDER BY {}) AS __rn",
        order_exprs.join(", ")
    ));

    let mut output_parts: Vec<String> = Vec::new();
    for column in left_keep.as_slice() {
        output_parts.push(format!(
            "l.{} AS {}",
            quote_ident(column.as_str()),
            quote_ident(column.as_str())
        ));
    }
    for (_, alias) in right_aliases.as_slice() {
        output_parts.push(format!(
            "b.{} AS {}",
            quote_ident(alias.as_str()),
            quote_ident(alias.as_str())
        ));
    }
    if config.emit_match_meta {
        let mode_lit = quote_lit(config.mode.trim().to_ascii_uppercase().as_str());
        let kind_expr = if join_mode == "left" {
            format!(
                "CASE WHEN b.__left_row IS NULL THEN 'NO_MATCH' ELSE {mode_lit} END AS {}",
                quote_ident(config.match_kind_col.as_str())
            )
        } else {
            format!(
                "{mode_lit} AS {}",
                quote_ident(config.match_kind_col.as_str())
            )
        };
        output_parts.push(kind_expr);
        let score_expr = if join_mode == "left" {
            format!(
                "CASE WHEN b.__left_row IS NULL THEN CAST(NULL AS DOUBLE) ELSE b.__score END AS {}",
                quote_ident(config.match_score_col.as_str())
            )
        } else {
            format!("b.__score AS {}", quote_ident(config.match_score_col.as_str()))
        };
        output_parts.push(score_expr);
    }

    Ok(format!(
        "WITH left_aug AS ( \\
            SELECT l.*, ROW_NUMBER() OVER () AS __left_row \\
            FROM __ca_left l \\
         ), \\
         matched AS ( \\
            SELECT {matched_select} \\
            FROM left_aug l \\
            JOIN __ca_right r \\
              ON {l_path} = {r_path} AND ({condition}) \\
         ), \\
         best AS ( \\
            SELECT * FROM matched WHERE __rn = 1 \\
         ) \\
         SELECT {outputs} \\
         FROM left_aug l \\
         {join_keyword} best b \\
           ON l.__left_row = b.__left_row",
        matched_select = matched_select_parts.join(", "),
        condition = mode_condition(config),
        outputs = output_parts.join(", "),
    ))
}

async fn materialize_interval_align(
    left_schema: SchemaRef,
    left_batches: Vec<RecordBatch>,
    right_schema: SchemaRef,
    right_batches: Vec<RecordBatch>,
    config: IntervalAlignProviderConfig,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let sql = build_interval_align_sql(&config, left_schema.as_ref(), right_schema.as_ref())?;
    let ctx = SessionContext::new();
    let left_mem = MemTable::try_new(left_schema, vec![left_batches])?;
    let right_mem = MemTable::try_new(right_schema, vec![right_batches])?;
    ctx.register_table("__ca_left", Arc::new(left_mem))?;
    ctx.register_table("__ca_right", Arc::new(right_mem))?;
    let df = ctx.sql(sql.as_str()).await?;
    let schema = df.schema().as_arrow().clone().into();
    let batches = df.collect().await?;
    Ok((schema, batches))
}

pub async fn build_interval_align_provider(
    left_schema: SchemaRef,
    left_batches: Vec<RecordBatch>,
    right_schema: SchemaRef,
    right_batches: Vec<RecordBatch>,
    config: IntervalAlignProviderConfig,
) -> Result<Arc<dyn TableProvider>> {
    let (schema, batches) = materialize_interval_align(
        left_schema,
        left_batches,
        right_schema,
        right_batches,
        config.clone(),
    )
    .await?;
    let mem = Arc::new(MemTable::try_new(schema, vec![batches])?);
    Ok(Arc::new(IntervalAlignProvider::new(mem, config)))
}

pub async fn execute_interval_align(
    left_schema: SchemaRef,
    left_batches: Vec<RecordBatch>,
    right_schema: SchemaRef,
    right_batches: Vec<RecordBatch>,
    config: IntervalAlignProviderConfig,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let provider = build_interval_align_provider(
        left_schema,
        left_batches,
        right_schema,
        right_batches,
        config,
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table("__ca_interval_align", provider)?;
    let df = ctx.sql("SELECT * FROM __ca_interval_align").await?;
    let schema = df.schema().as_arrow().clone().into();
    let batches = df.collect().await?;
    Ok((schema, batches))
}
