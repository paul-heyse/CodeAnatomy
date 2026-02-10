use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

use codeanatomy_engine::compiler::plan_compiler::SemanticPlanCompiler;
use codeanatomy_engine::compiler::pushdown_probe_extract::{
    extract_input_filter_predicates, verify_pushdown_contracts,
};
use codeanatomy_engine::compliance::capture::{capture_explain_verbose, ComplianceCapture, RetentionPolicy, RulepackSnapshot};
use codeanatomy_engine::executor::orchestration::prepare_execution_context;
use codeanatomy_engine::executor::pipeline;
use codeanatomy_engine::executor::result::TuningHint;
use codeanatomy_engine::executor::warnings::{RunWarning, WarningCode, WarningStage};
#[cfg(feature = "tracing")]
use codeanatomy_engine::executor::warnings::warning_counts_by_code;
use codeanatomy_engine::providers::registration::probe_provider_pushdown;
use codeanatomy_engine::providers::pushdown_contract::PushdownEnforcementMode as ContractEnforcementMode;
use codeanatomy_engine::rules::rulepack::RulepackFactory;
use codeanatomy_engine::spec::runtime::PushdownEnforcementMode;
use codeanatomy_engine::spec::runtime::RuntimeTunerMode;
use codeanatomy_engine::stability::optimizer_lab::run_lab_from_ruleset;
use codeanatomy_engine::tuner::adaptive::{AdaptiveTuner, ExecutionMetrics, TunerConfig};
#[cfg(feature = "tracing")]
use codeanatomy_engine::executor::tracing as engine_tracing;

use super::compiler::CompiledPlan;
use super::result::PyRunResult;
use super::session::SessionFactory as PySessionFactory;

/// CPG materializer that executes compiled plans and writes Delta tables.
///
/// Orchestrates the Python-specific execution lifecycle:
/// 1. Session creation from factory + spec rules
/// 2. Input relation registration (Delta providers)
/// 3. Envelope capture (post-registration, Scope 11 ordering)
/// 4. Compliance capture (EXPLAIN VERBOSE, rulepack snapshot) -- Python-specific
/// 5. Delegates core compile-materialize-metrics to `execute_pipeline`
/// 6. Tuner logic -- Python-specific
/// 7. Maps result to `PyRunResult`
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
    /// Handles Python-specific concerns (session construction, input registration,
    /// envelope capture, compliance capture, tuner logic) and delegates the core
    /// compile-materialize-metrics sequence to [`pipeline::execute_pipeline`].
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
            let tracing_config = spec.runtime.effective_tracing();
            #[cfg(feature = "tracing")]
            engine_tracing::init_otel_tracing(&tracing_config)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to initialize tracing: {e}")))?;

            #[cfg(feature = "tracing")]
            let execution_span = {
                let profile_name = spec
                    .runtime_profile
                    .as_ref()
                    .map(|profile| profile.profile_name.as_str())
                    .unwrap_or("environment");
                let span_info = engine_tracing::ExecutionSpanInfo::without_envelope(
                    &compiled_plan.spec_hash(),
                    &ruleset.fingerprint,
                    profile_name,
                );
                engine_tracing::execution_span(&span_info, &tracing_config)
            };
            #[cfg(feature = "tracing")]
            let _execution_span_guard = execution_span.enter();

            // ---------------------------------------------------------------
            // Shared pre-pipeline orchestration (native/Python parity)
            // ---------------------------------------------------------------
            let prepared = prepare_execution_context(
                session_factory.inner(),
                &spec,
                &ruleset,
                Some(&tracing_config),
            )
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to prepare execution context: {e}")))?;

            #[cfg(feature = "tracing")]
            {
                let mut locations: Vec<String> = spec
                    .input_relations
                    .iter()
                    .map(|relation| relation.delta_location.clone())
                    .collect();
                locations.extend(
                    spec.output_targets
                        .iter()
                        .filter_map(|target| target.delta_location.clone()),
                );
                engine_tracing::register_instrumented_stores_for_locations(
                    &prepared.ctx,
                    &tracing_config,
                    &locations,
                )
                .map_err(|e| {
                    PyRuntimeError::new_err(format!(
                        "Failed to register instrumented object stores: {e}"
                    ))
                })?;
            }

            #[cfg(feature = "tracing")]
            engine_tracing::record_envelope_hash(&execution_span, &prepared.envelope.envelope_hash);

            // ---------------------------------------------------------------
            // Python-specific: Compliance capture (EXPLAIN VERBOSE, rulepack snapshot)
            // ---------------------------------------------------------------
            let runtime_config = &spec.runtime;
            let compliance_enabled =
                resolve_compliance_enabled(runtime_config.compliance_capture);
            let tuner_mode = resolve_tuner_mode(runtime_config.tuner_mode);

            // Collect non-fatal warnings from Python-specific best-effort operations.
            let mut py_warnings: Vec<RunWarning> = Vec::new();

            let compliance_capture_json = if compliance_enabled {
                // Compile plans for EXPLAIN VERBOSE capture. The pipeline will
                // recompile independently; this double-compile only occurs when
                // compliance capture is explicitly enabled and is acceptable
                // because compilation is cheap relative to materialization.
                let compliance_compiler = SemanticPlanCompiler::new(&prepared.ctx, &spec);
                let compliance_plans = compliance_compiler
                    .compile()
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to compile plans for compliance: {e}")))?;

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
                config_snapshot.insert(
                    "capture_optimizer_lab".to_string(),
                    runtime_config.capture_optimizer_lab.to_string(),
                );
                capture.capture_config(config_snapshot);

                for (target, df) in &compliance_plans {
                    match capture_explain_verbose(df, &target.table_name).await {
                        Ok(explain_lines) => {
                            if !explain_lines.is_empty() {
                                capture.record_explain(&target.table_name, explain_lines);
                            }
                        }
                        Err(err) => py_warnings.push(
                            RunWarning::new(
                                WarningCode::ComplianceExplainCaptureFailed,
                                WarningStage::Compliance,
                                format!(
                                    "Compliance EXPLAIN VERBOSE capture failed for '{}': {err}",
                                    target.table_name
                                ),
                            )
                            .with_context("table_name", target.table_name.clone()),
                        ),
                    }
                }

                if runtime_config.capture_optimizer_lab {
                    let state = prepared.ctx.state();
                    let config = state.config();
                    let config_options = config.options();
                    let max_passes = config_options.optimizer.max_passes;
                    let skip_failed_rules = config_options.optimizer.skip_failed_rules;
                    for (target, df) in &compliance_plans {
                        match run_lab_from_ruleset(
                            df.logical_plan().clone(),
                            &ruleset,
                            max_passes,
                            skip_failed_rules,
                        ) {
                            Ok(lab_result) => {
                                let lab_name = format!("{}::optimizer_lab", target.table_name);
                                capture.record_lab_steps(&lab_name, lab_result.steps);
                            }
                            Err(err) => py_warnings.push(
                                RunWarning::new(
                                    WarningCode::ComplianceOptimizerLabFailed,
                                    WarningStage::Compliance,
                                    format!(
                                        "Optimizer lab capture failed for '{}': {err}",
                                        target.table_name
                                    ),
                                )
                                .with_context("table_name", target.table_name.clone()),
                            ),
                        }
                    }
                }

                let (pushdown_predicates, pushdown_warnings) =
                    extract_input_filter_predicates(&prepared.ctx, &spec).await;
                py_warnings.extend(pushdown_warnings);
                let mut pushdown_probes_by_table = BTreeMap::new();
                for (table_name, filters) in pushdown_predicates {
                    if filters.is_empty() {
                        continue;
                    }
                    match probe_provider_pushdown(&prepared.ctx, &table_name, &filters).await {
                        Ok(probe) => {
                            capture.record_pushdown_probe(&table_name, probe.clone());
                            pushdown_probes_by_table.insert(table_name, probe);
                        }
                        Err(err) => py_warnings.push(
                            RunWarning::new(
                                WarningCode::CompliancePushdownProbeFailed,
                                WarningStage::Compliance,
                                format!("Pushdown probe failed for '{table_name}': {err}"),
                            )
                            .with_context("table_name", table_name.clone()),
                        ),
                    }
                }
                if !pushdown_probes_by_table.is_empty() {
                    let enforcement_mode = match runtime_config.pushdown_enforcement_mode {
                        PushdownEnforcementMode::Warn => ContractEnforcementMode::Warn,
                        PushdownEnforcementMode::Strict => ContractEnforcementMode::Strict,
                        PushdownEnforcementMode::Disabled => ContractEnforcementMode::Disabled,
                    };
                    for (target, df) in &compliance_plans {
                        let optimized_plan = match prepared.ctx.state().optimize(df.logical_plan()) {
                            Ok(plan) => plan,
                            Err(err) => {
                                py_warnings.push(
                                    RunWarning::new(
                                        WarningCode::CompliancePushdownProbeFailed,
                                        WarningStage::Compliance,
                                        format!(
                                            "Pushdown contract optimization failed for '{}': {err}",
                                            target.table_name
                                        ),
                                    )
                                    .with_context("table_name", target.table_name.clone()),
                                );
                                continue;
                            }
                        };
                        let report = verify_pushdown_contracts(
                            &optimized_plan,
                            &pushdown_probes_by_table,
                            enforcement_mode.clone(),
                        );
                        capture.record_pushdown_contract_report(&target.table_name, report.clone());
                        if enforcement_mode == ContractEnforcementMode::Strict
                            && !report.violations.is_empty()
                        {
                            return Err(PyRuntimeError::new_err(format!(
                                "Pushdown contract violation in strict mode for '{}'",
                                target.table_name
                            )));
                        }
                        if enforcement_mode == ContractEnforcementMode::Warn {
                            for violation in report.violations {
                                py_warnings.push(
                                    RunWarning::new(
                                        WarningCode::PushdownContractViolation,
                                        WarningStage::Compliance,
                                        format!(
                                            "Pushdown contract violation on '{}': {}",
                                            violation.table_name, violation.predicate_text
                                        ),
                                    )
                                    .with_context("table_name", violation.table_name)
                                    .with_context("predicate", violation.predicate_text),
                                );
                            }
                        }
                    }
                }

                // Serialize to JSON with warning on failure.
                if !capture.is_empty() {
                    match capture.to_json() {
                        Ok(json) => Some(json),
                        Err(e) => {
                            py_warnings.push(RunWarning::new(
                                WarningCode::ComplianceSerializationFailed,
                                WarningStage::Compliance,
                                format!("Compliance capture JSON serialization failed: {e}"),
                            ));
                            None
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            };

            // ---------------------------------------------------------------
            // Delegate to unified pipeline orchestrator
            // ---------------------------------------------------------------
            let outcome = pipeline::execute_pipeline(
                &prepared.ctx,
                &spec,
                prepared.envelope.envelope_hash,
                ruleset.fingerprint,
                prepared.provider_identities.clone(),
                prepared.envelope.planning_surface_hash,
                prepared.preflight_warnings.clone(),
            )
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Pipeline execution failed: {e}")))?;

            let collected = outcome.collected_metrics;
            let mut run_result = outcome.run_result;

            // ---------------------------------------------------------------
            // Python-specific: Enrich RunResult with compliance and tuner data
            // ---------------------------------------------------------------
            run_result.compliance_capture_json = compliance_capture_json;

            // Merge Python-specific warnings into the pipeline's warnings.
            if !py_warnings.is_empty() {
                run_result.warnings.extend(py_warnings);
            }

            // ---------------------------------------------------------------
            // Python-specific: Tuner logic
            // ---------------------------------------------------------------
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
                let rows_processed = run_result
                    .outputs
                    .iter()
                    .map(|outcome| outcome.rows_written)
                    .sum();
                // Use real metrics from the pipeline when available,
                // falling back to environment defaults for zero values.
                let metrics = ExecutionMetrics {
                    elapsed_ms,
                    spill_count: collected.spill_count.min(u64::from(u32::MAX)) as u32,
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
                    run_result.tuner_hints.push(TuningHint {
                        target_partitions: next.target_partitions,
                        batch_size: next.batch_size,
                        repartition_joins: next.repartition_joins,
                        repartition_aggregations: next.repartition_aggregations,
                    });
                }
            }

            // ---------------------------------------------------------------
            // Tracing flush and result mapping
            // ---------------------------------------------------------------
            #[cfg(feature = "tracing")]
            if tracing_config.enabled {
                let warning_counts = warning_counts_by_code(&run_result.warnings);
                engine_tracing::record_warning_summary(
                    &execution_span,
                    run_result.warnings.len() as u64,
                    &warning_counts,
                );
            }

            #[cfg(feature = "tracing")]
            if tracing_config.enabled {
                engine_tracing::flush_otel_tracing().map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to flush tracing provider: {e}"))
                })?;
            }

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
