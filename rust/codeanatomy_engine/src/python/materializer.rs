use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::compiler::plan_compiler::SemanticPlanCompiler;
use crate::compliance::capture::{capture_explain_verbose, ComplianceCapture, RetentionPolicy, RulepackSnapshot};
use crate::executor::runner::execute_and_materialize;
use crate::executor::result::{RunResult, TuningHint};
use crate::providers::registration::register_extraction_inputs;
use crate::rules::rulepack::RulepackFactory;
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

            // Build session with ruleset and spec hash
            let (ctx, envelope) = session_factory
                .inner()
                .build_session(&ruleset, compiled_plan.spec_hash())
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to build session: {e}")))?;

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

            // Execute and materialize to Delta
            let materialization_results = execute_and_materialize(&ctx, output_plans)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Materialization failed: {e}")))?;
            let compliance_capture_json = compliance_capture
                .filter(|capture| !capture.is_empty())
                .and_then(|capture| capture.to_json().ok());

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
                let metrics = ExecutionMetrics {
                    elapsed_ms,
                    spill_count: 0,
                    scan_selectivity: 1.0,
                    peak_memory_bytes: env_profile.memory_pool_bytes,
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
