//! WS4: Central plan combiner — walks view graph topologically, builds single LogicalPlan DAG.
//!
//! Orchestrates:
//! - Graph validation (WS4.5)
//! - Topological sort with cycle detection
//! - View compilation via transform-specific builders
//! - Cost-aware cache boundary insertion
//! - Output DataFrame construction

use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result};
use std::collections::{HashMap, VecDeque};
use std::env;

use crate::compiler::cache_boundaries;
use crate::compiler::graph_validator;
use crate::compiler::inline_policy::{compute_inline_policy, InlineDecision};
use crate::compiler::join_builder;
use crate::compiler::udtf_builder;
use crate::compiler::union_builder;
use crate::compiler::view_builder;
use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::outputs::OutputTarget;
use crate::spec::relations::{ViewDefinition, ViewTransform};

/// Plan validation result from EXPLAIN analysis.
#[derive(Debug)]
pub struct PlanValidation {
    pub unoptimized_plan: String,
    pub physical_plan: String,
    pub explain_verbose: String,
}

/// Central plan compiler for semantic execution specs.
pub struct SemanticPlanCompiler<'a> {
    ctx: &'a SessionContext,
    spec: &'a SemanticExecutionSpec,
}

impl<'a> SemanticPlanCompiler<'a> {
    /// Create a new plan compiler for the given context and spec.
    pub fn new(ctx: &'a SessionContext, spec: &'a SemanticExecutionSpec) -> Self {
        Self { ctx, spec }
    }

    /// Compile full spec into output DataFrames.
    ///
    /// Returns Vec<(OutputTarget, DataFrame)> ready for materialization.
    ///
    /// Process:
    /// 1. Validate graph structure (WS4.5)
    /// 2. Topological sort (view-to-view edges only)
    /// 3. Compile each view and register as lazy view
    /// 4. Insert cost-aware cache boundaries
    /// 5. Build output DataFrames
    pub async fn compile(&self) -> Result<Vec<(OutputTarget, DataFrame)>> {
        // 1. Validate graph
        graph_validator::validate_graph(self.spec)?;
        let parameter_values = self.collect_parameter_values()?;

        // 2. Topological sort
        let ordered = self.topological_sort()?;

        // 2.5. Compute inline policy for cross-view optimization
        let ref_counts = self.compute_ref_counts();
        let output_views: Vec<String> = self
            .spec
            .output_targets
            .iter()
            .map(|t| t.source_view.clone())
            .collect();
        let inline_policy =
            compute_inline_policy(&self.spec.view_definitions, &output_views, &ref_counts);
        let mut inline_cache: HashMap<String, DataFrame> = HashMap::new();

        // 3. Compile and register each view (respecting inline policy)
        for view_def in &ordered {
            let df = self.compile_view(view_def, &parameter_values).await?;
            match inline_policy.get(&view_def.name) {
                Some(InlineDecision::Inline) => {
                    inline_cache.insert(view_def.name.clone(), df);
                }
                _ => {
                    let view = df.into_view();
                    self.ctx.register_table(&view_def.name, view)?;
                }
            }
        }

        // 4. Insert cache boundaries
        cache_boundaries::insert_cache_boundaries(self.ctx, self.spec).await?;

        // 5. Build output DataFrames
        let mut outputs = Vec::new();
        for target in &self.spec.output_targets {
            let df = self.ctx.table(&target.source_view).await?;
            let projected = if target.columns.is_empty() {
                df
            } else {
                df.select(target.columns.iter().map(|c| col(c)).collect::<Vec<_>>())?
            };
            outputs.push((target.clone(), projected));
        }

        Ok(outputs)
    }

    /// Topological sort of view definitions using Kahn's algorithm.
    ///
    /// Only counts view-to-view edges (input relations are pre-registered).
    /// Deterministic tie-breaking via sorted queue.
    /// Explicit cycle detection: if ordered.len() != view_count, fails with diagnostic.
    fn topological_sort(&self) -> Result<Vec<ViewDefinition>> {
        let view_count = self.spec.view_definitions.len();

        // Build dependency graph (view name → dependencies)
        let mut graph: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut in_degree: HashMap<&str, usize> = HashMap::new();

        for view in &self.spec.view_definitions {
            graph.insert(&view.name, view.view_dependencies.iter().map(|s| s.as_str()).collect());
            in_degree.insert(&view.name, view.view_dependencies.len());
        }

        // Initialize queue with zero in-degree nodes (sorted for determinism)
        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(name, _)| *name)
            .collect();

        // Sort for deterministic tie-breaking
        let mut queue_vec: Vec<&str> = queue.into_iter().collect();
        queue_vec.sort();
        queue = queue_vec.into_iter().collect();

        // Kahn's algorithm
        let mut ordered = Vec::new();

        while let Some(node) = queue.pop_front() {
            ordered.push(node);

            // Find all nodes that depend on this one
            for view in &self.spec.view_definitions {
                if view.view_dependencies.iter().any(|dep| dep == node) {
                    let deg = in_degree.get_mut(view.name.as_str()).unwrap();
                    *deg -= 1;

                    if *deg == 0 {
                        queue.push_back(&view.name);

                        // Re-sort queue for determinism
                        let mut queue_vec: Vec<&str> = queue.into_iter().collect();
                        queue_vec.sort();
                        queue = queue_vec.into_iter().collect();
                    }
                }
            }
        }

        // Cycle detection
        if ordered.len() != view_count {
            let unresolved: Vec<&str> = in_degree
                .iter()
                .filter(|(_, &deg)| deg > 0)
                .map(|(name, _)| *name)
                .collect();

            return Err(DataFusionError::Plan(format!(
                "Cycle detected in view dependencies. Unresolvable views: {:?}",
                unresolved
            )));
        }

        // Map back to ViewDefinitions
        let name_to_view: HashMap<&str, &ViewDefinition> = self
            .spec
            .view_definitions
            .iter()
            .map(|v| (v.name.as_str(), v))
            .collect();

        Ok(ordered.iter().map(|name| (*name_to_view.get(name).unwrap()).clone()).collect())
    }

    /// Compile a single view definition into a DataFrame.
    ///
    /// Dispatches to transform-specific builders based on ViewTransform variant.
    async fn compile_view(
        &self,
        view_def: &ViewDefinition,
        parameter_values: &HashMap<String, String>,
    ) -> Result<DataFrame> {
        match &view_def.transform {
            ViewTransform::Normalize {
                source,
                id_columns,
                span_columns,
                text_columns,
            } => {
                let source = self
                    .resolve_source_with_templates(source, parameter_values)
                    .await?;
                view_builder::build_normalize(
                    self.ctx,
                    &view_def.name,
                    &source,
                    id_columns,
                    span_columns,
                    text_columns,
                )
                .await
            }

            ViewTransform::Relate {
                left,
                right,
                join_type,
                join_keys,
            } => {
                let left = self
                    .resolve_source_with_templates(left, parameter_values)
                    .await?;
                let right = self
                    .resolve_source_with_templates(right, parameter_values)
                    .await?;
                join_builder::build_join(self.ctx, &left, &right, join_type, join_keys).await
            }

            ViewTransform::Union {
                sources,
                discriminator_column,
                distinct,
            } => {
                let mut resolved_sources = Vec::with_capacity(sources.len());
                for source in sources {
                    resolved_sources.push(
                        self.resolve_source_with_templates(source, parameter_values)
                            .await?,
                    );
                }
                union_builder::build_union(
                    self.ctx,
                    resolved_sources.as_slice(),
                    discriminator_column,
                    *distinct,
                )
                .await
            }

            ViewTransform::Project { source, columns } => {
                let source = self
                    .resolve_source_with_templates(source, parameter_values)
                    .await?;
                view_builder::build_project(self.ctx, &source, columns).await
            }

            ViewTransform::Filter { source, predicate } => {
                let source = self
                    .resolve_source_with_templates(source, parameter_values)
                    .await?;
                view_builder::build_filter(self.ctx, &source, predicate).await
            }

            ViewTransform::Aggregate {
                source,
                group_by,
                aggregations,
            } => {
                let source = self
                    .resolve_source_with_templates(source, parameter_values)
                    .await?;
                view_builder::build_aggregate(self.ctx, &source, group_by, aggregations).await
            }

            ViewTransform::IncrementalCdf {
                source,
                starting_version,
                ending_version,
                starting_timestamp,
                ending_timestamp,
            } => {
                udtf_builder::build_incremental_cdf(
                    self.ctx,
                    source,
                    *starting_version,
                    *ending_version,
                    starting_timestamp.as_deref(),
                    ending_timestamp.as_deref(),
                )
                .await
            }

            ViewTransform::Metadata { source } => {
                udtf_builder::build_metadata(self.ctx, source).await
            }

            ViewTransform::FileManifest { source } => {
                udtf_builder::build_file_manifest(self.ctx, source).await
            }
        }
    }

    fn collect_parameter_values(&self) -> Result<HashMap<String, String>> {
        let mut expected_names: HashMap<String, String> = HashMap::new();
        for template in &self.spec.parameter_templates {
            expected_names.insert(template.name.to_ascii_uppercase(), template.name.clone());
        }

        let mut values = HashMap::new();
        for (key, value) in env::vars() {
            let Some(suffix) = key.strip_prefix("CODEANATOMY_PARAM_") else {
                continue;
            };
            let lookup = suffix.to_ascii_uppercase();
            let Some(template_name) = expected_names.get(&lookup) else {
                return Err(DataFusionError::Plan(format!(
                    "Unknown parameter template env var '{key}'"
                )));
            };
            values.insert(template_name.clone(), value);
        }
        let missing = self
            .spec
            .parameter_templates
            .iter()
            .filter(|template| !values.contains_key(&template.name))
            .map(|template| template.name.clone())
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Missing required parameter template values: {}. Set env vars CODEANATOMY_PARAM_<NAME>.",
                missing.join(", ")
            )));
        }
        Ok(values)
    }

    async fn resolve_source_with_templates(
        &self,
        source: &str,
        parameter_values: &HashMap<String, String>,
    ) -> Result<String> {
        let mut current_source = source.to_string();
        let mut templates = self
            .spec
            .parameter_templates
            .iter()
            .filter(|template| template.base_table == source)
            .collect::<Vec<_>>();
        templates.sort_by(|left, right| left.name.cmp(&right.name));

        for template in templates {
            let Some(value) = parameter_values.get(&template.name) else {
                continue;
            };
            let df = self.ctx.table(&current_source).await?;
            let schema = df.schema();
            let has_filter_column = schema
                .fields()
                .iter()
                .any(|field| field.name() == template.filter_column.as_str());
            if !has_filter_column {
                let available_columns = schema
                    .fields()
                    .iter()
                    .map(|field| field.name().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(DataFusionError::Plan(format!(
                    "Parameter template '{}' references unknown filter column '{}' in '{}'. Available columns: [{}]",
                    template.name, template.filter_column, current_source, available_columns
                )));
            }
            let parameter_literal =
                Self::render_parameter_literal(value, &template.parameter_type)?;
            let filter_sql = format!("{} = {}", template.filter_column, parameter_literal);
            let filter_expr = self.ctx.parse_sql_expr(&filter_sql, &schema)?;
            let filtered = df.filter(filter_expr)?;
            let filtered_view_name = format!("__param_{}_{}", source, template.name);
            self.ctx
                .register_table(&filtered_view_name, filtered.into_view())?;
            current_source = filtered_view_name;
        }
        Ok(current_source)
    }

    fn render_parameter_literal(raw: &str, parameter_type: &str) -> Result<String> {
        let normalized = parameter_type.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "string" | "str" | "utf8" => {
                let escaped = raw.replace('\'', "''");
                Ok(format!("'{escaped}'"))
            }
            "int" | "integer" | "long" => raw
                .parse::<i128>()
                .map(|_| raw.to_string())
                .map_err(|_| {
                    DataFusionError::Plan(format!(
                        "Invalid integer parameter value '{}' for type '{}'",
                        raw, parameter_type
                    ))
                }),
            "float" | "double" | "number" | "decimal" => raw
                .parse::<f64>()
                .map(|_| raw.to_string())
                .map_err(|_| {
                    DataFusionError::Plan(format!(
                        "Invalid numeric parameter value '{}' for type '{}'",
                        raw, parameter_type
                    ))
                }),
            "bool" | "boolean" => {
                let lowered = raw.trim().to_ascii_lowercase();
                if lowered == "true" || lowered == "1" {
                    Ok("true".to_string())
                } else if lowered == "false" || lowered == "0" {
                    Ok("false".to_string())
                } else {
                    Err(DataFusionError::Plan(format!(
                        "Invalid boolean parameter value '{}' for type '{}'",
                        raw, parameter_type
                    )))
                }
            }
            other => Err(DataFusionError::Plan(format!(
                "Unsupported parameter type '{}' for runtime template substitution",
                other
            ))),
        }
    }

    /// Compute reference counts (fanout) for each view by name.
    ///
    /// Counts how many times each view is referenced as a dependency by other views.
    /// Borrows from `self.spec` and returns owned `HashMap` with borrowed `&str` keys
    /// tied to the spec's lifetime.
    fn compute_ref_counts(&self) -> HashMap<&'a str, usize> {
        let mut ref_counts: HashMap<&str, usize> = HashMap::new();

        // Initialize all views with zero refs
        for view in &self.spec.view_definitions {
            ref_counts.insert(&view.name, 0);
        }

        // Count references from view dependencies
        for view in &self.spec.view_definitions {
            for dep in &view.view_dependencies {
                *ref_counts.entry(dep.as_str()).or_insert(0) += 1;
            }
        }

        // Count references from output targets
        for target in &self.spec.output_targets {
            *ref_counts.entry(target.source_view.as_str()).or_insert(0) += 1;
        }

        ref_counts
    }

    /// Resolve a source name, checking the inline cache before falling back
    /// to a registered table in the SessionContext.
    ///
    /// This allows inlined views to be consumed by downstream views without
    /// requiring named registration.
    #[allow(dead_code)]
    async fn resolve_source(
        &self,
        source_name: &str,
        inline_cache: &HashMap<String, DataFrame>,
    ) -> Result<DataFrame> {
        if let Some(df) = inline_cache.get(source_name) {
            Ok(df.clone())
        } else {
            self.ctx.table(source_name).await
        }
    }

    /// Validate a DataFrame's plan via EXPLAIN.
    ///
    /// Returns:
    /// - P0 (unoptimized logical plan) via df.logical_plan()
    /// - P2 (physical plan) via df.create_physical_plan().await?
    /// - EXPLAIN VERBOSE output via df.explain(true, false)?.collect().await?
    pub async fn validate_plan(df: &DataFrame) -> Result<PlanValidation> {
        // P0: Unoptimized logical plan
        let unoptimized_plan = format!("{:?}", df.logical_plan());

        // P2: Physical plan
        let physical_plan_obj = df.clone().create_physical_plan().await?;
        let physical_plan = format!("{:?}", physical_plan_obj);

        // EXPLAIN VERBOSE
        let explain_df = df.clone().explain(true, false)?;
        let explain_batches = explain_df.collect().await?;
        let explain_verbose = format!("{:?}", explain_batches);

        Ok(PlanValidation {
            unoptimized_plan,
            physical_plan,
            explain_verbose,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::execution_spec::SemanticExecutionSpec;
    use crate::spec::join_graph::JoinGraph;
    use crate::spec::outputs::{MaterializationMode, OutputTarget};
    use crate::spec::relations::{InputRelation, SchemaContract, ViewDefinition, ViewTransform};
    use crate::spec::rule_intents::RulepackProfile;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::datasource::MemTable;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn minimal_schema() -> SchemaContract {
        let mut schema = BTreeMap::new();
        schema.insert("id".to_string(), "Int64".to_string());
        SchemaContract { columns: schema }
    }

    async fn setup_test_context() -> SessionContext {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("input_table", Arc::new(table))
            .unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_topological_sort_empty() {
        let spec = SemanticExecutionSpec::new(
            1,
            vec![],
            vec![],
            JoinGraph::default(),
            vec![],
            vec![],
            RulepackProfile::Default,
            vec![],
        );

        let ctx = SessionContext::new();
        let compiler = SemanticPlanCompiler::new(&ctx, &spec);

        let result = compiler.topological_sort().unwrap();
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_topological_sort_single_view() {
        let views = vec![ViewDefinition {
            name: "view1".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: minimal_schema(),
        }];

        let spec = SemanticExecutionSpec::new(
            1,
            vec![],
            views,
            JoinGraph::default(),
            vec![],
            vec![],
            RulepackProfile::Default,
            vec![],
        );

        let ctx = SessionContext::new();
        let compiler = SemanticPlanCompiler::new(&ctx, &spec);

        let result = compiler.topological_sort().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "view1");
    }

    #[tokio::test]
    async fn test_topological_sort_chain() {
        let views = vec![
            ViewDefinition {
                name: "view1".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "view2".to_string(),
                view_kind: "filter".to_string(),
                view_dependencies: vec!["view1".to_string()],
                transform: ViewTransform::Filter {
                    source: "view1".to_string(),
                    predicate: "id > 0".to_string(),
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "view3".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec!["view2".to_string()],
                transform: ViewTransform::Project {
                    source: "view2".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
        ];

        let spec = SemanticExecutionSpec::new(
            1,
            vec![],
            views,
            JoinGraph::default(),
            vec![],
            vec![],
            RulepackProfile::Default,
            vec![],
        );

        let ctx = SessionContext::new();
        let compiler = SemanticPlanCompiler::new(&ctx, &spec);

        let result = compiler.topological_sort().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].name, "view1");
        assert_eq!(result[1].name, "view2");
        assert_eq!(result[2].name, "view3");
    }

    #[tokio::test]
    async fn test_topological_sort_diamond() {
        let views = vec![
            ViewDefinition {
                name: "base".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec![],
                transform: ViewTransform::Project {
                    source: "input".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "left".to_string(),
                view_kind: "filter".to_string(),
                view_dependencies: vec!["base".to_string()],
                transform: ViewTransform::Filter {
                    source: "base".to_string(),
                    predicate: "id > 0".to_string(),
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "right".to_string(),
                view_kind: "filter".to_string(),
                view_dependencies: vec!["base".to_string()],
                transform: ViewTransform::Filter {
                    source: "base".to_string(),
                    predicate: "id < 100".to_string(),
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "merged".to_string(),
                view_kind: "union".to_string(),
                view_dependencies: vec!["left".to_string(), "right".to_string()],
                transform: ViewTransform::Union {
                    sources: vec!["left".to_string(), "right".to_string()],
                    discriminator_column: None,
                    distinct: false,
                },
                output_schema: minimal_schema(),
            },
        ];

        let spec = SemanticExecutionSpec::new(
            1,
            vec![],
            views,
            JoinGraph::default(),
            vec![],
            vec![],
            RulepackProfile::Default,
            vec![],
        );

        let ctx = SessionContext::new();
        let compiler = SemanticPlanCompiler::new(&ctx, &spec);

        let result = compiler.topological_sort().unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].name, "base");
        // left and right can be in any order (deterministic but either is valid)
        assert!(result[1].name == "left" || result[1].name == "right");
        assert!(result[2].name == "left" || result[2].name == "right");
        assert_eq!(result[3].name, "merged");
    }

    #[tokio::test]
    async fn test_topological_sort_cycle_detection() {
        let views = vec![
            ViewDefinition {
                name: "view1".to_string(),
                view_kind: "project".to_string(),
                view_dependencies: vec!["view2".to_string()],
                transform: ViewTransform::Project {
                    source: "view2".to_string(),
                    columns: vec!["id".to_string()],
                },
                output_schema: minimal_schema(),
            },
            ViewDefinition {
                name: "view2".to_string(),
                view_kind: "filter".to_string(),
                view_dependencies: vec!["view1".to_string()],
                transform: ViewTransform::Filter {
                    source: "view1".to_string(),
                    predicate: "id > 0".to_string(),
                },
                output_schema: minimal_schema(),
            },
        ];

        let spec = SemanticExecutionSpec::new(
            1,
            vec![],
            views,
            JoinGraph::default(),
            vec![],
            vec![],
            RulepackProfile::Default,
            vec![],
        );

        let ctx = SessionContext::new();
        let compiler = SemanticPlanCompiler::new(&ctx, &spec);

        let result = compiler.topological_sort();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cycle detected"));
    }

    #[tokio::test]
    async fn test_compile_simple_spec() {
        let ctx = setup_test_context().await;

        let inputs = vec![InputRelation {
            logical_name: "input_table".to_string(),
            delta_location: "/path/to/delta".to_string(),
            requires_lineage: false,
            version_pin: None,
        }];

        let views = vec![ViewDefinition {
            name: "view1".to_string(),
            view_kind: "project".to_string(),
            view_dependencies: vec![],
            transform: ViewTransform::Project {
                source: "input_table".to_string(),
                columns: vec!["id".to_string()],
            },
            output_schema: minimal_schema(),
        }];

        let outputs = vec![OutputTarget {
            table_name: "output1".to_string(),
            delta_location: None,
            source_view: "view1".to_string(),
            columns: vec!["id".to_string()],
            materialization_mode: MaterializationMode::Overwrite,
            partition_by: vec![],
            write_metadata: std::collections::BTreeMap::new(),
            max_commit_retries: None,
        }];

        let spec = SemanticExecutionSpec::new(
            1,
            inputs,
            views,
            JoinGraph::default(),
            outputs,
            vec![],
            RulepackProfile::Default,
            vec![],
        );

        let compiler = SemanticPlanCompiler::new(&ctx, &spec);
        let result = compiler.compile().await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.table_name, "output1");
    }
}
