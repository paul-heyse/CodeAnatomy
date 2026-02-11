//! Semantic validation pass for `SemanticExecutionSpec`.
//!
//! Extends structural graph validation with transform-level and contract-level
//! checks before logical compilation starts.

use std::collections::{BTreeMap, HashMap, HashSet};

use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};

use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::relations::ViewTransform;

#[derive(Debug, Clone)]
pub struct SemanticValidationResult {
    pub errors: Vec<SemanticValidationError>,
    pub warnings: Vec<SemanticValidationWarning>,
}

#[derive(Debug, Clone)]
pub enum SemanticValidationError {
    UnsupportedTransformComposition {
        view_name: String,
        detail: String,
    },
    UnresolvedColumnReference {
        view_name: String,
        column: String,
        context: String,
    },
    JoinKeyIncompatibility {
        view_name: String,
        left_key: String,
        right_key: String,
        left_type: String,
        right_type: String,
    },
    OutputContractViolation {
        output_name: String,
        expected_columns: Vec<String>,
        actual_columns: Vec<String>,
    },
    AggregationInvariantViolation {
        view_name: String,
        detail: String,
    },
}

#[derive(Debug, Clone)]
pub enum SemanticValidationWarning {
    ImplicitTypeCoercion {
        view_name: String,
        column: String,
        from_type: String,
        to_type: String,
    },
    BroadProjection {
        view_name: String,
        column_count: usize,
    },
}

impl SemanticValidationResult {
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

pub async fn validate_semantics(
    spec: &SemanticExecutionSpec,
    ctx: &SessionContext,
) -> Result<SemanticValidationResult> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let mut schema_by_name: HashMap<&str, BTreeMap<String, String>> = HashMap::new();
    for relation in &spec.input_relations {
        let df = ctx.table(&relation.logical_name).await.map_err(|err| {
            DataFusionError::Plan(format!(
                "Input relation '{}' is not registered for semantic validation: {err}",
                relation.logical_name
            ))
        })?;
        let mut columns = BTreeMap::new();
        for field in df.schema().fields() {
            columns.insert(field.name().to_string(), format!("{:?}", field.data_type()));
        }
        schema_by_name.insert(relation.logical_name.as_str(), columns);
    }
    for view in &spec.view_definitions {
        schema_by_name.insert(view.name.as_str(), view.output_schema.columns.clone());
    }

    for view in &spec.view_definitions {
        match &view.transform {
            ViewTransform::Union { sources, .. } => {
                if sources.len() < 2 {
                    errors.push(SemanticValidationError::UnsupportedTransformComposition {
                        view_name: view.name.clone(),
                        detail: "Union requires at least two sources".to_string(),
                    });
                }
            }
            ViewTransform::Relate {
                left,
                right,
                join_keys,
                ..
            } => {
                if join_keys.is_empty() {
                    errors.push(SemanticValidationError::UnsupportedTransformComposition {
                        view_name: view.name.clone(),
                        detail: "Relate requires at least one join key pair".to_string(),
                    });
                }
                let left_schema = schema_by_name.get(left.as_str());
                let right_schema = schema_by_name.get(right.as_str());
                if let (Some(left_schema), Some(right_schema)) = (left_schema, right_schema) {
                    for pair in join_keys {
                        let left_ty = left_schema.get(&pair.left_key);
                        let right_ty = right_schema.get(&pair.right_key);
                        match (left_ty, right_ty) {
                            (Some(l), Some(r)) if l == r => {}
                            (Some(l), Some(r)) if is_numeric(l) && is_numeric(r) => {
                                warnings.push(SemanticValidationWarning::ImplicitTypeCoercion {
                                    view_name: view.name.clone(),
                                    column: pair.left_key.clone(),
                                    from_type: l.clone(),
                                    to_type: r.clone(),
                                });
                            }
                            (Some(l), Some(r)) => {
                                errors.push(SemanticValidationError::JoinKeyIncompatibility {
                                    view_name: view.name.clone(),
                                    left_key: pair.left_key.clone(),
                                    right_key: pair.right_key.clone(),
                                    left_type: l.clone(),
                                    right_type: r.clone(),
                                });
                            }
                            (None, _) => {
                                errors.push(SemanticValidationError::UnresolvedColumnReference {
                                    view_name: view.name.clone(),
                                    column: pair.left_key.clone(),
                                    context: format!("Join left source '{}' missing key", left),
                                });
                            }
                            (_, None) => {
                                errors.push(SemanticValidationError::UnresolvedColumnReference {
                                    view_name: view.name.clone(),
                                    column: pair.right_key.clone(),
                                    context: format!("Join right source '{}' missing key", right),
                                });
                            }
                        }
                    }
                }
            }
            ViewTransform::Project { source, columns } => {
                if let Some(source_schema) = schema_by_name.get(source.as_str()) {
                    for column in columns {
                        if !source_schema.contains_key(column) {
                            errors.push(SemanticValidationError::UnresolvedColumnReference {
                                view_name: view.name.clone(),
                                column: column.clone(),
                                context: format!(
                                    "Project references unknown column in source '{}'",
                                    source
                                ),
                            });
                        }
                    }
                    if columns.len() > 64 {
                        warnings.push(SemanticValidationWarning::BroadProjection {
                            view_name: view.name.clone(),
                            column_count: columns.len(),
                        });
                    }
                }
            }
            ViewTransform::Filter { source, predicate } => {
                if let Some(source_schema) = schema_by_name.get(source.as_str()) {
                    let needs_fallback_column_check = match ctx.table(source).await {
                        Ok(source_df) => {
                            if let Err(err) = ctx.parse_sql_expr(predicate, source_df.schema()) {
                                errors.push(
                                    SemanticValidationError::UnresolvedColumnReference {
                                        view_name: view.name.clone(),
                                        column: predicate.clone(),
                                        context: format!(
                                            "Filter predicate failed schema-aware parsing for source '{}': {err}",
                                            source
                                        ),
                                    },
                                );
                            }
                            false
                        }
                        Err(_) => true,
                    };

                    if needs_fallback_column_check {
                        for token in referenced_identifiers(predicate) {
                            if !source_schema.contains_key(&token) {
                                errors.push(
                                    SemanticValidationError::UnresolvedColumnReference {
                                        view_name: view.name.clone(),
                                        column: token,
                                        context: format!(
                                            "Filter predicate references unknown column in source '{}'",
                                            source
                                        ),
                                    },
                                );
                            }
                        }
                    }
                }
            }
            ViewTransform::Aggregate {
                source,
                group_by,
                aggregations,
            } => {
                if group_by.is_empty() && aggregations.is_empty() {
                    errors.push(SemanticValidationError::AggregationInvariantViolation {
                        view_name: view.name.clone(),
                        detail: "Aggregate requires group_by or aggregations".to_string(),
                    });
                }
                if let Some(source_schema) = schema_by_name.get(source.as_str()) {
                    for key in group_by {
                        if !source_schema.contains_key(key) {
                            errors.push(SemanticValidationError::UnresolvedColumnReference {
                                view_name: view.name.clone(),
                                column: key.clone(),
                                context: format!(
                                    "Aggregate group_by references unknown column in source '{}'",
                                    source
                                ),
                            });
                        }
                    }
                    for agg in aggregations {
                        if !source_schema.contains_key(&agg.column) {
                            errors.push(SemanticValidationError::UnresolvedColumnReference {
                                view_name: view.name.clone(),
                                column: agg.column.clone(),
                                context: format!(
                                    "Aggregate expression references unknown column in source '{}'",
                                    source
                                ),
                            });
                        }
                    }
                }
            }
            ViewTransform::Normalize {
                source,
                id_columns,
                span_columns,
                text_columns,
            } => {
                if let Some(source_schema) = schema_by_name.get(source.as_str()) {
                    for column in id_columns.iter().chain(text_columns.iter()) {
                        if !source_schema.contains_key(column) {
                            errors.push(SemanticValidationError::UnresolvedColumnReference {
                                view_name: view.name.clone(),
                                column: column.clone(),
                                context: format!(
                                    "Normalize references unknown column in source '{}'",
                                    source
                                ),
                            });
                        }
                    }
                    if let Some((start, end)) = span_columns {
                        for column in [start, end] {
                            if !source_schema.contains_key(column) {
                                errors.push(SemanticValidationError::UnresolvedColumnReference {
                                    view_name: view.name.clone(),
                                    column: column.clone(),
                                    context: format!(
                                        "Normalize span column missing in source '{}'",
                                        source
                                    ),
                                });
                            }
                        }
                    }
                }
            }
            ViewTransform::IncrementalCdf { .. }
            | ViewTransform::Metadata { .. }
            | ViewTransform::FileManifest { .. }
            | ViewTransform::CpgEmit { .. } => {}
        }
    }

    for output in &spec.output_targets {
        let Some(schema) = schema_by_name.get(output.source_view.as_str()) else {
            continue;
        };
        if output.columns.is_empty() {
            continue;
        }
        let actual_columns: Vec<String> = schema.keys().cloned().collect();
        let expected_missing: Vec<String> = output
            .columns
            .iter()
            .filter(|column| !schema.contains_key(column.as_str()))
            .cloned()
            .collect();
        if !expected_missing.is_empty() {
            errors.push(SemanticValidationError::OutputContractViolation {
                output_name: output.table_name.clone(),
                expected_columns: expected_missing,
                actual_columns,
            });
        }
    }

    Ok(SemanticValidationResult { errors, warnings })
}

fn is_numeric(ty: &str) -> bool {
    ty.contains("Int") || ty.contains("UInt") || ty.contains("Float") || ty.contains("Decimal")
}

fn referenced_identifiers(predicate: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut token = String::new();
    let mut in_string = false;
    let keywords: HashSet<&str> = HashSet::from([
        "and", "or", "not", "is", "null", "true", "false", "in", "like", "between", "case",
        "when", "then", "else", "end", "as", "cast", "extract", "from", "distinct",
    ]);

    for ch in predicate.chars() {
        if ch == '\'' {
            in_string = !in_string;
            if !token.is_empty() {
                maybe_push_identifier(&mut out, &token, &keywords);
                token.clear();
            }
            continue;
        }
        if in_string {
            continue;
        }
        if ch == '_' || ch.is_ascii_alphanumeric() {
            token.push(ch);
        } else if !token.is_empty() {
            maybe_push_identifier(&mut out, &token, &keywords);
            token.clear();
        }
    }
    if !token.is_empty() {
        maybe_push_identifier(&mut out, &token, &keywords);
    }
    out.sort();
    out.dedup();
    out
}

fn maybe_push_identifier(out: &mut Vec<String>, token: &str, keywords: &HashSet<&str>) {
    if token.chars().all(|ch| ch.is_ascii_digit()) {
        return;
    }
    let lowercase = token.to_ascii_lowercase();
    if keywords.contains(lowercase.as_str()) {
        return;
    }
    out.push(token.to_string());
}
