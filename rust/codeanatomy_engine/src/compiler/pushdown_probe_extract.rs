//! Derive provider pushdown probe predicates from filter transforms.
//!
//! This module expands probe extraction beyond direct `Filter(input)`:
//! - `Filter(Project(input))`
//! - `Filter(Normalize(input))`
//! - `Filter(Aggregate(input))`
//! - conservative join-side extraction for `Filter(Relate(...))`

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use datafusion::prelude::{Expr, SessionContext};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};

use crate::executor::warnings::{RunWarning, WarningCode, WarningStage};
use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::relations::{ViewDefinition, ViewTransform};

enum ResolvedFilterSource {
    Input(String),
    Relate {
        left_input: Option<String>,
        right_input: Option<String>,
        left_source: String,
        right_source: String,
    },
    Unsupported(String),
}

fn view_index(spec: &SemanticExecutionSpec) -> HashMap<&str, &ViewDefinition> {
    spec.view_definitions
        .iter()
        .map(|view| (view.name.as_str(), view))
        .collect()
}

fn resolve_single_input(
    source: &str,
    input_names: &HashSet<&str>,
    view_by_name: &HashMap<&str, &ViewDefinition>,
    visiting: &mut HashSet<String>,
) -> Option<String> {
    if input_names.contains(source) {
        return Some(source.to_string());
    }
    let view = view_by_name.get(source)?;
    if !visiting.insert(view.name.clone()) {
        return None;
    }
    let resolved = match &view.transform {
        ViewTransform::Project { source, .. }
        | ViewTransform::Filter { source, .. }
        | ViewTransform::Aggregate { source, .. }
        | ViewTransform::Normalize { source, .. } => {
            resolve_single_input(source, input_names, view_by_name, visiting)
        }
        _ => None,
    };
    visiting.remove(&view.name);
    resolved
}

fn resolve_filter_source(
    source: &str,
    input_names: &HashSet<&str>,
    view_by_name: &HashMap<&str, &ViewDefinition>,
    visiting: &mut HashSet<String>,
) -> ResolvedFilterSource {
    if input_names.contains(source) {
        return ResolvedFilterSource::Input(source.to_string());
    }
    let Some(view) = view_by_name.get(source) else {
        return ResolvedFilterSource::Unsupported(format!(
            "source '{source}' is not an input relation or view"
        ));
    };
    if !visiting.insert(view.name.clone()) {
        return ResolvedFilterSource::Unsupported(format!(
            "cycle detected while resolving source chain at '{source}'"
        ));
    }
    let resolved = match &view.transform {
        ViewTransform::Project { source, .. }
        | ViewTransform::Filter { source, .. }
        | ViewTransform::Aggregate { source, .. }
        | ViewTransform::Normalize { source, .. } => {
            resolve_filter_source(source, input_names, view_by_name, visiting)
        }
        ViewTransform::Relate { left, right, .. } => ResolvedFilterSource::Relate {
            left_input: resolve_single_input(left, input_names, view_by_name, &mut HashSet::new()),
            right_input: resolve_single_input(
                right,
                input_names,
                view_by_name,
                &mut HashSet::new(),
            ),
            left_source: left.clone(),
            right_source: right.clone(),
        },
        other => ResolvedFilterSource::Unsupported(format!(
            "unsupported source transform for pushdown probing: {:?}",
            std::mem::discriminant(other)
        )),
    };
    visiting.remove(&view.name);
    resolved
}

fn warning(message: impl Into<String>) -> RunWarning {
    RunWarning::new(
        WarningCode::CompliancePushdownProbeSkipped,
        WarningStage::Compliance,
        message,
    )
}

fn collect_expr_columns(expr: &Expr) -> BTreeSet<String> {
    let mut columns = BTreeSet::new();
    let _ = expr.apply(|node| {
        if let Expr::Column(col) = node {
            columns.insert(col.name.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    columns
}

async fn schema_columns(ctx: &SessionContext, source: &str) -> Option<BTreeSet<String>> {
    let df = ctx.table(source).await.ok()?;
    Some(
        df.schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect(),
    )
}

/// Derive per-input filter probe expressions for provider pushdown validation.
pub async fn extract_input_filter_predicates(
    ctx: &SessionContext,
    spec: &SemanticExecutionSpec,
) -> (BTreeMap<String, Vec<Expr>>, Vec<RunWarning>) {
    let input_names: HashSet<&str> = spec
        .input_relations
        .iter()
        .map(|relation| relation.logical_name.as_str())
        .collect();
    let view_by_name = view_index(spec);

    let mut predicates: BTreeMap<String, Vec<Expr>> = BTreeMap::new();
    let mut warnings = Vec::new();

    for view in &spec.view_definitions {
        let ViewTransform::Filter { source, predicate } = &view.transform else {
            continue;
        };

        let resolved = resolve_filter_source(source, &input_names, &view_by_name, &mut HashSet::new());
        match resolved {
            ResolvedFilterSource::Input(input_name) => {
                let source_df = match ctx.table(&input_name).await {
                    Ok(df) => df,
                    Err(err) => {
                        warnings.push(
                            warning(format!(
                                "Pushdown probe skipped for '{input_name}': failed to resolve source table ({err})"
                            ))
                            .with_context("source", input_name.clone())
                            .with_context("view", view.name.clone()),
                        );
                        continue;
                    }
                };
                match ctx.parse_sql_expr(predicate, source_df.schema()) {
                    Ok(expr) => {
                        predicates.entry(input_name).or_default().push(expr);
                    }
                    Err(err) => warnings.push(
                        warning(format!(
                            "Pushdown probe skipped for '{source}': failed to parse predicate '{predicate}' ({err})"
                        ))
                        .with_context("source", source.clone())
                        .with_context("view", view.name.clone()),
                    ),
                }
            }
            ResolvedFilterSource::Relate {
                left_input,
                right_input,
                left_source,
                right_source,
            } => {
                let joined_df = match ctx.table(source).await {
                    Ok(df) => df,
                    Err(err) => {
                        warnings.push(
                            warning(format!(
                                "Pushdown probe skipped for relate source '{source}': failed to resolve relation ({err})"
                            ))
                            .with_context("source", source.clone())
                            .with_context("view", view.name.clone()),
                        );
                        continue;
                    }
                };
                let expr = match ctx.parse_sql_expr(predicate, joined_df.schema()) {
                    Ok(expr) => expr,
                    Err(err) => {
                        warnings.push(
                            warning(format!(
                                "Pushdown probe skipped for relate source '{source}': failed to parse predicate '{predicate}' ({err})"
                            ))
                            .with_context("source", source.clone())
                            .with_context("view", view.name.clone()),
                        );
                        continue;
                    }
                };

                let referenced = collect_expr_columns(&expr);
                if referenced.is_empty() {
                    warnings.push(
                        warning(format!(
                            "Pushdown probe skipped for relate source '{source}': predicate '{predicate}' did not reference columns"
                        ))
                        .with_context("source", source.clone())
                        .with_context("view", view.name.clone()),
                    );
                    continue;
                }

                let left_columns = schema_columns(ctx, &left_source).await.unwrap_or_default();
                let right_columns = schema_columns(ctx, &right_source).await.unwrap_or_default();

                let all_left = referenced.iter().all(|col| left_columns.contains(col));
                let all_right = referenced.iter().all(|col| right_columns.contains(col));
                let any_left = referenced.iter().any(|col| left_columns.contains(col));
                let any_right = referenced.iter().any(|col| right_columns.contains(col));

                let target_input = if all_left && !any_right {
                    left_input
                } else if all_right && !any_left {
                    right_input
                } else {
                    None
                };

                let Some(target_input) = target_input else {
                    warnings.push(
                        warning(format!(
                            "Pushdown probe skipped for relate source '{source}': predicate '{predicate}' spans both sides or could not be resolved to one input"
                        ))
                        .with_context("source", source.clone())
                        .with_context("view", view.name.clone()),
                    );
                    continue;
                };
                predicates.entry(target_input).or_default().push(expr);
            }
            ResolvedFilterSource::Unsupported(reason) => {
                warnings.push(
                    warning(format!(
                        "Pushdown probe skipped for '{source}' in view '{}': {reason}",
                        view.name
                    ))
                    .with_context("source", source.clone())
                    .with_context("view", view.name.clone()),
                );
            }
        }
    }

    (predicates, warnings)
}
