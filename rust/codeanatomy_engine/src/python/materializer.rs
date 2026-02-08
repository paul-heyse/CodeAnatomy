use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::compiler::plan_bundle::{build_plan_bundle_artifact, capture_plan_bundle_runtime, PlanBundleArtifact};
use crate::compiler::plan_compiler::SemanticPlanCompiler;
use crate::compliance::capture::{capture_explain_verbose, ComplianceCapture, RetentionPolicy, RulepackSnapshot};
use crate::executor::metrics_collector::{collect_plan_metrics, CollectedMetrics};
use crate::executor::maintenance::execute_maintenance;
use crate::executor::runner::execute_and_materialize;
use crate::executor::result::{RunResult, TuningHint};
use crate::providers::registration::register_extraction_inputs;
use crate::rules::rulepack::RulepackFactory;
use crate::session::envelope::SessionEnvelope;
use crate::spec::runtime::RuntimeTunerMode;
use crate::tuner::adaptive::{AdaptiveTuner, ExecutionMetrics, TunerConfig};

use super::compiler::CompiledPlan;
use super::result::PyRunResult;
use super::session::SessionFactory as PySessionFactory;

/// CPG materializer that executes compiled plans and writes Delta tables.
///
/// Orchestrates:
/// 1. Session creation from factory + spec rules
/// 2. Input relation registration (Delta providers)
/// 3. Logical plan compilation via SemanticPlanCompiler
/// 4. Execution and Delta materialization
#[pyclass]
pub struct CpgMaterializer {
    runtime: Arc<Runtime>,
}

fn parse_env_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn resolve_compliance_enabled(default: bool) -> bool {
    std::env::var("CODEANATOMY_ENGINE_COMPLIANCE_CAPTURE")
        .ok()
        .as_deref()
        .and_then(parse_env_bool)
        .unwrap_or(default)
}

fn resolve_tuner_mode(default: RuntimeTunerMode) -> RuntimeTunerMode {
    let Some(raw) = std::env::var("CODEANATOMY_ENGINE_TUNER_MODE").ok() else {
        return default;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" | "0" | "false" => RuntimeTunerMode::Off,
        "observe" => RuntimeTunerMode::Observe,
        "apply" | "on" | "1" | "true" => RuntimeTunerMode::Apply,
        _ => default,
    }
}

#[pymethods]
impl CpgMaterializer {
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {e}")))?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    /// Execute a compiled plan and materialize CPG outputs to Delta tables.
    ///
    /// Args:
    ///     session_factory: Configured session factory (from SessionFactory.new or .from_class)
    ///     compiled_plan: Validated execution plan (from SemanticPlanCompiler.compile)
    ///
    /// Returns:
    ///     RunResult with materialization outcomes and determinism contract
    ///
    /// Raises:
    ///     RuntimeError: If execution or materialization fails
    fn execute(
        &self,
        session_factory: &PySessionFactory,
        compiled_plan: &CompiledPlan,
    ) -> PyResult<PyRunResult> {
        let runtime = self.runtime.clone();

        runtime.block_on(async {
            let start_time = chrono::Utc::now();

            // Parse spec from compiled plan
            let spec = compiled_plan
                .parse_spec()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to parse spec: {e}")))?;

            // Get environment profile for rule compilation
            let env_profile = session_factory.get_profile();

            // Compile rule intents into ruleset using RulepackFactory
            let ruleset = RulepackFactory::build_ruleset(
                &spec.rulepack_profile,
                &spec.rule_intents,
                &env_profile,
            );

            // WS-P9: Build session with ruleset, using profiled builder when available.
            let (ctx, envelope) = if let Some(profile) = &spec.runtime_profile {
                let ctx = session_factory
                    .inner()
                    .build_session_from_profile(
                        profile,
                        &ruleset,
                        spec.runtime.enable_function_factory,
                        spec.runtime.enable_domain_planner,
                    )
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to build profiled session: {e}")))?;
                // build_session_from_profile returns only SessionContext;
                // build the envelope separately.
                let envelope = SessionEnvelope::capture(
                    &ctx,
                    compiled_plan.spec_hash(),
                    ruleset.fingerprint,
                    profile.memory_pool_bytes as u64,
                    true, // FairSpillPool always enables spilling
                )
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to capture envelope: {e}")))?;
                (ctx, envelope)
            } else {
                session_factory
                    .inner()
                    .build_session(&ruleset, compiled_plan.spec_hash())
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to build session: {e}")))?
            };

            // Register input relations as Delta providers
            register_extraction_inputs(&ctx, &spec.input_relations)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to register inputs: {e}")))?;

            // Compile logical plan DAG
            let compiler = SemanticPlanCompiler::new(&ctx, &spec);
            let output_plans = compiler
                .compile()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to compile plans: {e}")))?;
            let runtime_config = &spec.runtime;
            let compliance_enabled =
                resolve_compliance_enabled(runtime_config.compliance_capture);
            let tuner_mode = resolve_tuner_mode(runtime_config.tuner_mode);
            let mut compliance_capture = if compliance_enabled {
                let mut capture = ComplianceCapture::new(RetentionPolicy::Short);
                capture.capture_rulepack(RulepackSnapshot {
                    profile: format!("{:?}", spec.rulepack_profile),
                    analyzer_rules: ruleset
                        .analyzer_rules
                        .iter()
                        .map(|rule| rule.name().to_string())
                        .collect(),
                    optimizer_rules: ruleset
                        .optimizer_rules
                        .iter()
                        .map(|rule| rule.name().to_string())
                        .collect(),
                    physical_rules: ruleset
                        .physical_rules
                        .iter()
                        .map(|rule| rule.name().to_string())
                        .collect(),
                    fingerprint: ruleset.fingerprint,
                });
                let mut config_snapshot = BTreeMap::new();
                config_snapshot.insert(
                    "compliance_capture".to_string(),
                    compliance_enabled.to_string(),
                );
                config_snapshot.insert(
                    "tuner_mode".to_string(),
                    format!("{tuner_mode:?}"),
                );
                capture.capture_config(config_snapshot);
                Some(capture)
            } else {
                None
            };
            if let Some(capture) = compliance_capture.as_mut() {
                for (target, df) in &output_plans {
                    let explain_lines = capture_explain_verbose(df, &target.table_name)
                        .await
                        .unwrap_or_default();
                    if !explain_lines.is_empty() {
                        capture.record_explain(&target.table_name, explain_lines);
                    }
                }
            }

            // WS-P1: Capture plan bundles BEFORE execution consumes output_plans.
            let plan_bundles: Vec<PlanBundleArtifact> = if compliance_enabled {
                let mut bundles = Vec::new();
                for (_target, df) in &output_plans {
                    if let Ok(runtime_bundle) = capture_plan_bundle_runtime(&ctx, df).await {
                        if let Ok(artifact) = build_plan_bundle_artifact(
                            &ctx,
                            &runtime_bundle,
                            ruleset.fingerprint,
                            vec![],
                            spec.runtime.capture_substrait,
                            false,
                        )
                        .await
                        {
                            bundles.push(artifact);
                        }
                    }
                }
                bundles
            } else {
                vec![]
            };

            // WS-P7: Create physical plans for real metrics collection
            // BEFORE execution consumes the DataFrames.
            let mut physical_plans = Vec::new();
            for (_target, df) in &output_plans {
                if let Ok(plan) = df.clone().create_physical_plan().await {
                    physical_plans.push(plan);
                }
            }

            // Execute and materialize to Delta
            let materialization_results = execute_and_materialize(&ctx, output_plans, &spec.spec_hash, &envelope.envelope_hash)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Materialization failed: {e}")))?;
            let compliance_capture_json = compliance_capture
                .filter(|capture| !capture.is_empty())
                .and_then(|capture| capture.to_json().ok());

            // WS-P7: Collect real metrics from physical plans captured pre-execution.
            let mut collected = CollectedMetrics::default();
            for plan in &physical_plans {
                let plan_metrics = collect_plan_metrics(plan.as_ref());
                collected.output_rows += plan_metrics.output_rows;
                collected.spill_count += plan_metrics.spill_count;
                collected.spilled_bytes += plan_metrics.spilled_bytes;
                collected.elapsed_compute_nanos += plan_metrics.elapsed_compute_nanos;
                collected.peak_memory_bytes = collected.peak_memory_bytes.max(plan_metrics.peak_memory_bytes);
                collected.scan_selectivity = if plan_metrics.scan_selectivity > 0.0 {
                    plan_metrics.scan_selectivity
                } else {
                    collected.scan_selectivity
                };
                collected.partition_count += plan_metrics.partition_count;
                collected.operator_metrics.extend(plan_metrics.operator_metrics);
            }

            // WS-P11: Execute post-materialization maintenance if configured.
            let maintenance_reports = if let Some(schedule) = &spec.maintenance {
                let output_locations: Vec<(String, String)> = materialization_results
                    .iter()
                    .map(|r| {
                        let location = spec
                            .output_targets
                            .iter()
                            .find(|t| t.table_name == r.table_name)
                            .and_then(|t| t.delta_location.clone())
                            .unwrap_or_else(|| r.table_name.clone());
                        (r.table_name.clone(), location)
                    })
                    .collect();
                execute_maintenance(&ctx, schedule, &output_locations)
                    .await
                    .unwrap_or_default()
            } else {
                vec![]
            };

            let mut tuner_hints: Vec<TuningHint> = Vec::new();
            if tuner_mode != RuntimeTunerMode::Off {
                let initial_config = TunerConfig {
                    target_partitions: env_profile.target_partitions,
                    batch_size: env_profile.batch_size,
                    repartition_joins: true,
                    repartition_aggregations: true,
                };
                let mut tuner = match tuner_mode {
                    RuntimeTunerMode::Observe => {
                        AdaptiveTuner::observe_only(initial_config.clone())
                    }
                    RuntimeTunerMode::Apply => AdaptiveTuner::new(initial_config.clone()),
                    RuntimeTunerMode::Off => {
                        // Guarded above; keep deterministic branch coverage.
                        AdaptiveTuner::observe_only(initial_config.clone())
                    }
                };
                let elapsed_ms = (chrono::Utc::now() - start_time)
                    .num_milliseconds()
                    .max(0) as u64;
                let rows_processed = materialization_results
                    .iter()
                    .map(|outcome| outcome.rows_written)
                    .sum();
                // WS-P7: Use real metrics from physical plan tree when available,
                // falling back to environment defaults for zero values.
                let metrics = ExecutionMetrics {
                    elapsed_ms,
                    spill_count: collected.spill_count,
                    scan_selectivity: if collected.scan_selectivity > 0.0 {
                        collected.scan_selectivity
                    } else {
                        1.0
                    },
                    peak_memory_bytes: if collected.peak_memory_bytes > 0 {
                        collected.peak_memory_bytes
                    } else {
                        env_profile.memory_pool_bytes
                    },
                    rows_processed,
                };
                if let Some(next) = tuner.observe(&metrics) {
                    tuner_hints.push(TuningHint {
                        target_partitions: next.target_partitions,
                        batch_size: next.batch_size,
                        repartition_joins: next.repartition_joins,
                        repartition_aggregations: next.repartition_aggregations,
                    });
                }
            }

            // Build RunResult with determinism contract
            let run_result = RunResult {
                outputs: materialization_results,
                spec_hash: compiled_plan.spec_hash(),
                envelope_hash: envelope.envelope_hash,
                rulepack_fingerprint: ruleset.fingerprint,
                compliance_capture_json,
                tuner_hints,
                plan_bundles,
                collected_metrics: Some(collected),
                maintenance_reports,
                started_at: start_time,
                completed_at: chrono::Utc::now(),
            };

            Ok(PyRunResult::from_run_result(&run_result))
        })
    }
}

impl CpgMaterializer {
    /// Get a reference to the internal runtime for advanced use cases.
    #[allow(dead_code)]
    pub(crate) fn runtime(&self) -> &Runtime {
        &self.runtime
    }
}
