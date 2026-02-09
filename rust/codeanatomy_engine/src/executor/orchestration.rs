//! Shared pre-pipeline orchestration for native and Python entrypoints.

use datafusion::execution::context::SessionContext;
use datafusion_common::Result;

use crate::compiler::plan_bundle::ProviderIdentity;
use crate::providers::registration::register_extraction_inputs;
use crate::rules::registry::CpgRuleSet;
use crate::session::envelope::SessionEnvelope;
use crate::session::factory::{SessionBuildOverrides, SessionFactory};
use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::runtime::TracingConfig;

/// Canonical pre-pipeline context shared by all execution entrypoints.
pub struct PreparedExecutionContext {
    pub ctx: SessionContext,
    pub envelope: SessionEnvelope,
    pub provider_identities: Vec<ProviderIdentity>,
    pub preflight_warnings: Vec<String>,
}

/// Build session state, register inputs, and capture the envelope.
pub async fn prepare_execution_context(
    session_factory: &SessionFactory,
    spec: &SemanticExecutionSpec,
    ruleset: &CpgRuleSet,
    tracing_config: Option<&TracingConfig>,
) -> Result<PreparedExecutionContext> {
    let overrides = SessionBuildOverrides {
        enable_function_factory: spec.runtime.enable_function_factory,
        enable_domain_planner: spec.runtime.enable_domain_planner,
        enable_delta_codec: spec.runtime.capture_delta_codec,
    };

    let mut state = if let Some(profile) = &spec.runtime_profile {
        session_factory
            .build_session_state_from_profile_with_overrides(
                profile,
                ruleset,
                spec.spec_hash,
                tracing_config,
                overrides,
            )
            .await?
    } else {
        session_factory
            .build_session_state_with_overrides(ruleset, spec.spec_hash, tracing_config, overrides)
            .await?
    };

    let registrations = register_extraction_inputs(&state.ctx, &spec.input_relations).await?;
    let mut provider_identities: Vec<ProviderIdentity> = registrations
        .iter()
        .map(|r| ProviderIdentity {
            table_name: r.name.clone(),
            identity_hash: r.provider_identity,
        })
        .collect();
    provider_identities.sort_by(|a, b| a.table_name.cmp(&b.table_name));

    let provider_identity_hash_input: Vec<(String, [u8; 32])> = provider_identities
        .iter()
        .map(|identity| (identity.table_name.clone(), identity.identity_hash))
        .collect();
    let provider_identity_hash =
        SessionEnvelope::hash_provider_identities(&provider_identity_hash_input);

    let envelope = SessionEnvelope::capture(
        &state.ctx,
        spec.spec_hash,
        ruleset.fingerprint,
        state.memory_pool_bytes,
        true,
        state.planning_surface_hash,
        provider_identity_hash,
    )
    .await?;

    Ok(PreparedExecutionContext {
        ctx: state.ctx,
        envelope,
        provider_identities,
        preflight_warnings: std::mem::take(&mut state.build_warnings),
    })
}
