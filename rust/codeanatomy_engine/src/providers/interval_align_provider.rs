//! Interval-align provider implementation.

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Column;
use datafusion::datasource::{MemTable, TableType};
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{
    dml::InsertOp, Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown,
};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{cast, col, greatest, least, lit, when, JoinType};
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
    output_schema: SchemaRef,
    logical_plan: LogicalPlan,
}

impl IntervalAlignProvider {
    pub fn try_new(
        left_schema: SchemaRef,
        left_batches: Vec<RecordBatch>,
        right_schema: SchemaRef,
        right_batches: Vec<RecordBatch>,
        config: IntervalAlignProviderConfig,
    ) -> Result<Self> {
        let output_schema =
            build_output_schema(&config, left_schema.as_ref(), right_schema.as_ref())?;
        let (left_aug_schema, left_aug_batches) =
            augment_left_batches_with_row_id(&left_schema, left_batches.as_slice())?;
        let logical_plan = build_interval_align_logical_plan(
            &config,
            left_schema.as_ref(),
            left_aug_schema,
            left_aug_batches.as_slice(),
            right_schema.as_ref(),
            right_batches.as_slice(),
        )?;
        Ok(Self {
            output_schema,
            logical_plan,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for IntervalAlignProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        Some(Cow::Borrowed(&self.logical_plan))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut plan = LogicalPlanBuilder::from(self.logical_plan.clone());

        if let Some(filter_expr) = filters.iter().cloned().reduce(|acc, new| acc.and(new)) {
            plan = plan.filter(filter_expr)?;
        }

        if let Some(indices) = projection {
            let current_projection = (0..plan.schema().fields().len()).collect::<Vec<usize>>();
            if indices != &current_projection {
                let projected: Vec<Expr> = indices
                    .iter()
                    .map(|index| Expr::Column(Column::from(plan.schema().qualified_field(*index))))
                    .collect();
                plan = plan.project(projected)?;
            }
        }

        if let Some(row_limit) = limit {
            plan = plan.limit(0, Some(row_limit))?;
        }

        state.create_physical_plan(&plan.build()?).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| classify_interval_align_filter(expr))
            .collect())
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "IntervalAlignProvider is read-only".to_string(),
        ))
    }
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

struct IntervalAlignContext {
    left_available: Vec<String>,
    right_available: Vec<String>,
    left_keep: Vec<String>,
    right_aliases: Vec<(String, String)>,
    join_mode: String,
}

impl IntervalAlignContext {
    fn prepare(
        config: &IntervalAlignProviderConfig,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<Self> {
        let left_available = schema_columns(left_schema);
        let right_available = schema_columns(right_schema);
        if left_available.is_empty() || right_available.is_empty() {
            return Err(DataFusionError::Plan(
                "interval_align_table requires non-empty left and right schemas".to_string(),
            ));
        }
        validate_required_columns(
            config,
            left_available.as_slice(),
            right_available.as_slice(),
        )?;

        let left_keep = select_columns(config.select_left.as_slice(), left_available.as_slice());
        let right_keep = select_columns(config.select_right.as_slice(), right_available.as_slice());
        let right_aliases = resolve_right_aliases(
            right_keep.as_slice(),
            left_keep.as_slice(),
            config.right_suffix.as_str(),
            config.match_kind_col.as_str(),
            config.match_score_col.as_str(),
        );
        Ok(Self {
            left_available,
            right_available,
            left_keep,
            right_aliases,
            join_mode: config.how.trim().to_ascii_lowercase(),
        })
    }
}

fn qualified(alias: &str, column: &str) -> String {
    format!("{alias}.{column}")
}

fn mode_condition_expr(config: &IntervalAlignProviderConfig) -> Expr {
    let l_start = col(qualified("l", config.left_start_col.as_str()));
    let l_end = col(qualified("l", config.left_end_col.as_str()));
    let r_start = col(qualified("r", config.right_start_col.as_str()));
    let r_end = col(qualified("r", config.right_end_col.as_str()));
    match config.mode.trim().to_ascii_uppercase().as_str() {
        "EXACT" => r_start.eq(l_start).and(r_end.eq(l_end)),
        "OVERLAP_MAX" => r_start.lt(l_end).and(r_end.gt(l_start)),
        _ => r_start.lt_eq(l_start).and(r_end.gt_eq(l_end)),
    }
}

fn overlap_score_expr(config: &IntervalAlignProviderConfig) -> Result<Expr> {
    let l_start = cast(
        col(qualified("l", config.left_start_col.as_str())),
        DataType::Int64,
    );
    let l_end = cast(
        col(qualified("l", config.left_end_col.as_str())),
        DataType::Int64,
    );
    let r_start = cast(
        col(qualified("r", config.right_start_col.as_str())),
        DataType::Int64,
    );
    let r_end = cast(
        col(qualified("r", config.right_end_col.as_str())),
        DataType::Int64,
    );
    let min_end = least(vec![l_end.clone(), r_end.clone()]);
    let max_start = greatest(vec![l_start.clone(), r_start.clone()]);
    let mut score_builder = when(
        min_end.clone().gt(max_start.clone()),
        cast(min_end - max_start, DataType::Float64),
    );
    score_builder.otherwise(lit(0.0_f64))
}

fn classify_interval_align_filter(expr: &Expr) -> TableProviderFilterPushDown {
    let text = expr.to_string().to_ascii_lowercase();
    if text.contains("random(")
        || text.contains("now(")
        || text.contains("current_timestamp")
        || text.contains("__left_row")
        || text.contains("__rn")
    {
        TableProviderFilterPushDown::Unsupported
    } else if text.contains("match_score") || text.contains("match_kind") || text.contains("__r") {
        TableProviderFilterPushDown::Inexact
    } else {
        TableProviderFilterPushDown::Exact
    }
}

fn schema_columns(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

fn validate_required_columns(
    config: &IntervalAlignProviderConfig,
    left_columns: &[String],
    right_columns: &[String],
) -> Result<()> {
    let left_set: HashSet<&str> = left_columns.iter().map(String::as_str).collect();
    let right_set: HashSet<&str> = right_columns.iter().map(String::as_str).collect();

    for required in [
        config.left_path_col.as_str(),
        config.left_start_col.as_str(),
        config.left_end_col.as_str(),
    ] {
        if !left_set.contains(required) {
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
        if !right_set.contains(required) {
            return Err(DataFusionError::Plan(format!(
                "Right input is missing required column: {required}"
            )));
        }
    }
    Ok(())
}

fn build_output_schema(
    config: &IntervalAlignProviderConfig,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<SchemaRef> {
    let prepared = IntervalAlignContext::prepare(config, left_schema, right_schema)?;

    if prepared.join_mode != "inner" && prepared.join_mode != "left" {
        return Err(DataFusionError::Plan(format!(
            "interval_align_table unsupported how mode: {}",
            config.how
        )));
    }
    let right_nullable = prepared.join_mode == "left";
    let mut fields: Vec<Arc<Field>> = Vec::new();

    for column in prepared.left_keep {
        let field = left_schema.field_with_name(column.as_str())?;
        fields.push(Arc::new(field.as_ref().clone()));
    }
    for (original, alias) in prepared.right_aliases {
        let field = right_schema.field_with_name(original.as_str())?;
        let renamed = Field::new(
            alias.as_str(),
            field.data_type().clone(),
            field.is_nullable() || right_nullable,
        );
        fields.push(Arc::new(renamed));
    }
    if config.emit_match_meta {
        fields.push(Arc::new(Field::new(
            config.match_kind_col.as_str(),
            arrow::datatypes::DataType::Utf8,
            right_nullable,
        )));
        fields.push(Arc::new(Field::new(
            config.match_score_col.as_str(),
            arrow::datatypes::DataType::Float64,
            right_nullable,
        )));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn augment_left_batches_with_row_id(
    left_schema: &SchemaRef,
    left_batches: &[RecordBatch],
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let mut fields = left_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new("__left_row", DataType::Int64, false)));
    let augmented_schema = Arc::new(Schema::new(fields));

    let mut next_row_id: i64 = 1;
    let mut augmented_batches: Vec<RecordBatch> = Vec::with_capacity(left_batches.len());
    for batch in left_batches {
        let row_count = i64::try_from(batch.num_rows()).map_err(|_| {
            DataFusionError::Plan("interval_align row count exceeded i64 range".to_string())
        })?;
        let end_row_id = next_row_id
            .checked_add(row_count)
            .ok_or_else(|| DataFusionError::Plan("interval_align row id overflow".to_string()))?;
        let row_ids: Int64Array = (next_row_id..end_row_id).collect();
        next_row_id = end_row_id;

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(row_ids));
        augmented_batches.push(RecordBatch::try_new(augmented_schema.clone(), columns)?);
    }

    Ok((augmented_schema, augmented_batches))
}

fn build_interval_align_logical_plan(
    config: &IntervalAlignProviderConfig,
    left_schema: &Schema,
    left_aug_schema: SchemaRef,
    left_aug_batches: &[RecordBatch],
    right_schema: &Schema,
    right_batches: &[RecordBatch],
) -> Result<LogicalPlan> {
    let prepared = IntervalAlignContext::prepare(config, left_schema, right_schema)?;
    if prepared.join_mode != "inner" && prepared.join_mode != "left" {
        return Err(DataFusionError::Plan(format!(
            "interval_align_table unsupported how mode: {}",
            config.how
        )));
    }

    let state = SessionStateBuilder::new().with_default_features().build();
    let ctx = SessionContext::new_with_state(state);

    let left_provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
        left_aug_schema,
        vec![left_aug_batches.to_vec()],
    )?);
    let right_provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
        Arc::new(right_schema.clone()),
        vec![right_batches.to_vec()],
    )?);

    let left_for_match = ctx.read_table(left_provider.clone())?.alias("l")?;
    let right_for_match = ctx.read_table(right_provider.clone())?.alias("r")?;
    let match_predicates = vec![
        col(qualified("l", config.left_path_col.as_str()))
            .eq(col(qualified("r", config.right_path_col.as_str()))),
        mode_condition_expr(config),
    ];
    let matched = left_for_match.join_on(right_for_match, JoinType::Inner, match_predicates)?;

    let scored = matched.with_column("__score", overlap_score_expr(config)?)?;

    let left_available_set: HashSet<&str> =
        prepared.left_available.iter().map(String::as_str).collect();
    let right_available_set: HashSet<&str> = prepared
        .right_available
        .iter()
        .map(String::as_str)
        .collect();
    let mut sort_exprs = vec![
        col("l.__left_row").sort(true, false),
        col("__score").sort(false, false),
    ];
    for tie_breaker in config.tie_breakers.as_slice() {
        let tie_column = tie_breaker.column.trim();
        if tie_column.is_empty() {
            continue;
        }
        let asc = !tie_breaker.order.trim().eq_ignore_ascii_case("descending");
        if right_available_set.contains(tie_column) {
            sort_exprs.push(col(qualified("r", tie_column)).sort(asc, false));
        } else if left_available_set.contains(tie_column) {
            sort_exprs.push(col(qualified("l", tie_column)).sort(asc, false));
        }
    }
    sort_exprs.push(col(qualified("r", config.right_start_col.as_str())).sort(true, false));
    sort_exprs.push(col(qualified("r", config.right_end_col.as_str())).sort(true, false));

    let mut best_select_exprs = vec![col("l.__left_row").alias("__left_row"), col("__score")];
    for (original, alias) in prepared.right_aliases.as_slice() {
        best_select_exprs.push(col(qualified("r", original.as_str())).alias(alias.as_str()));
    }
    let best_matches = scored
        .distinct_on(
            vec![col("l.__left_row")],
            best_select_exprs,
            Some(sort_exprs),
        )?
        .alias("b")?;

    let left_for_output = ctx.read_table(left_provider)?.alias("l")?;
    let join_type = if prepared.join_mode == "left" {
        JoinType::Left
    } else {
        JoinType::Inner
    };
    let joined = left_for_output.join(
        best_matches,
        join_type,
        &["__left_row"],
        &["__left_row"],
        None,
    )?;

    let mut output_exprs: Vec<Expr> = Vec::new();
    for column in prepared.left_keep.as_slice() {
        output_exprs.push(col(qualified("l", column.as_str())).alias(column.as_str()));
    }
    for (_, alias) in prepared.right_aliases.as_slice() {
        output_exprs.push(col(qualified("b", alias.as_str())).alias(alias.as_str()));
    }
    if config.emit_match_meta {
        let mode_label = config.mode.trim().to_ascii_uppercase();
        let match_kind_expr = if prepared.join_mode == "left" {
            let mut builder = when(col("b.__left_row").is_null(), lit("NO_MATCH"));
            builder.otherwise(lit(mode_label))?
        } else {
            lit(mode_label)
        };
        output_exprs.push(match_kind_expr.alias(config.match_kind_col.as_str()));
        output_exprs.push(col("b.__score").alias(config.match_score_col.as_str()));
    }

    Ok(joined.select(output_exprs)?.logical_plan().clone())
}

pub async fn build_interval_align_provider(
    left_schema: SchemaRef,
    left_batches: Vec<RecordBatch>,
    right_schema: SchemaRef,
    right_batches: Vec<RecordBatch>,
    config: IntervalAlignProviderConfig,
) -> Result<Arc<dyn TableProvider>> {
    let provider = IntervalAlignProvider::try_new(
        left_schema,
        left_batches,
        right_schema,
        right_batches,
        config,
    )?;
    Ok(Arc::new(provider))
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
    let state = SessionStateBuilder::new().with_default_features().build();
    let execution_plan = provider.scan(&state, None, &[], None).await?;
    let task_ctx = Arc::new(datafusion::execution::TaskContext::from(&state));
    let schema = provider.schema();
    let batches = collect(execution_plan, task_ctx).await?;
    Ok((schema, batches))
}
