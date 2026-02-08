use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::compiler::plan_compiler::SemanticPlanCompiler;
use crate::executor::runner::execute_and_materialize;
use crate::executor::result::RunResult;
use crate::providers::registration::register_extraction_inputs;
use crate::rules::rulepack::RulepackFactory;

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

            // Execute and materialize to Delta
            let materialization_results = execute_and_materialize(&ctx, output_plans)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Materialization failed: {e}")))?;

            // Build RunResult with determinism contract
            let run_result = RunResult {
                outputs: materialization_results,
                spec_hash: compiled_plan.spec_hash(),
                envelope_hash: envelope.envelope_hash,
                rulepack_fingerprint: ruleset.fingerprint,
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
