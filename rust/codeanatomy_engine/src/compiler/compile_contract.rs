//! Compile-only contract surface for deterministic planning metadata.
//!
//! This module provides a compile boundary that returns plan artifacts,
//! scheduling/cost metadata, and warnings without materializing outputs.

use std::collections::BTreeMap;

use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};
#[cfg(feature = "tracing")]
use tracing::instrument;

use crate::compiler::compile_phases::{
    build_task_schedule_phase, compile_artifacts_phase, pushdown_probe_phase, ArtifactPhaseContext,
};
use crate::compiler::cost_model::StatsQuality;
use crate::compiler::plan_bundle::{PlanBundleArtifact, ProviderIdentity};
use crate::compiler::plan_compiler::SemanticPlanCompiler;
use crate::compiler::scheduling::TaskSchedule;
use crate::executor::orchestration::prepare_execution_context;
use crate::executor::warnings::RunWarning;
use crate::rules::registry::CpgRuleSet;
use crate::session::factory::SessionFactory;
use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::runtime::TracingConfig;

/// Compile contract request.
pub struct CompileRequest<'a> {
    pub session_factory: &'a SessionFactory,
    pub spec: &'a SemanticExecutionSpec,
    pub ruleset: &'a CpgRuleSet,
    pub tracing_config: Option<&'a TracingConfig>,
}

/// Per-output compile artifact entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompilePlanArtifact {
    pub table_name: String,
    pub artifact: PlanBundleArtifact,
}

/// Compile-only response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileResponse {
    pub spec_hash: [u8; 32],
    pub rulepack_fingerprint: [u8; 32],
    pub envelope_hash: [u8; 32],
    pub planning_surface_hash: [u8; 32],
    pub provider_identity_hash: [u8; 32],
    pub provider_identities: Vec<ProviderIdentity>,
    pub task_schedule: Option<TaskSchedule>,
    pub stats_quality: Option<StatsQuality>,
    pub task_costs: BTreeMap<String, f64>,
    pub bottom_level_costs: BTreeMap<String, f64>,
    pub slack_by_task: BTreeMap<String, f64>,
    pub dependency_map: BTreeMap<String, Vec<String>>,
    pub plan_artifacts: Vec<CompilePlanArtifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<RunWarning>,
}

/// Compile a request to deterministic planning artifacts without materialization.
#[cfg_attr(feature = "tracing", instrument(skip(request)))]
pub async fn compile_request(request: CompileRequest<'_>) -> Result<CompileResponse> {
    let CompileRequest {
        session_factory,
        spec,
        ruleset,
        tracing_config,
    } = request;

    let prepared =
        prepare_execution_context(session_factory, spec, ruleset, tracing_config).await?;
    let mut warnings = prepared.preflight_warnings;

    let compiler = SemanticPlanCompiler::new(&prepared.ctx, spec);
    let compilation = compiler.compile_with_warnings().await?;
    warnings.extend(compilation.warnings);
    let output_plans = compilation.outputs;

    let scheduling = build_task_schedule_phase(spec);
    warnings.extend(scheduling.warnings.clone());

    let pushdown = pushdown_probe_phase(&prepared.ctx, spec).await;
    warnings.extend(pushdown.warnings.clone());

    let artifacts = compile_artifacts_phase(
        &prepared.ctx,
        spec,
        ruleset,
        &output_plans,
        &pushdown.pushdown_probe_map,
        &ArtifactPhaseContext {
            stats_quality: scheduling.stats_quality,
            provider_identities: prepared.provider_identities.clone(),
            planning_surface_hash: prepared.envelope.planning_surface_hash,
        },
    )
    .await;
    warnings.extend(artifacts.warnings);

    Ok(CompileResponse {
        spec_hash: spec.spec_hash,
        rulepack_fingerprint: ruleset.fingerprint,
        envelope_hash: prepared.envelope.envelope_hash,
        planning_surface_hash: prepared.envelope.planning_surface_hash,
        provider_identity_hash: prepared.envelope.provider_identity_hash,
        provider_identities: prepared.provider_identities,
        task_schedule: scheduling.task_schedule,
        stats_quality: scheduling.stats_quality,
        task_costs: scheduling.task_costs,
        bottom_level_costs: scheduling.bottom_level_costs,
        slack_by_task: scheduling.slack_by_task,
        dependency_map: scheduling.dependency_map,
        plan_artifacts: artifacts.plan_artifacts,
        warnings,
    })
}

/// Build compile response directly from runtime plans and labels.
pub fn compile_response_to_json(response: &CompileResponse) -> Result<String> {
    serde_json::to_string_pretty(response)
        .map_err(|err| DataFusionError::Plan(format!("Failed to serialize CompileResponse: {err}")))
}
