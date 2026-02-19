//! Shared pre-pipeline orchestration for native and Python entrypoints.

use datafusion::execution::context::SessionContext;
use datafusion_common::Result;

use crate::compiler::graph_validator;
use crate::compiler::plan_bundle::{DeltaProviderCompatibility, ProviderIdentity};
use crate::executor::warnings::RunWarning;
use crate::providers::registration::register_extraction_inputs;
use crate::rules::overlay::build_overlaid_ruleset;
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
    pub preflight_warnings: Vec<RunWarning>,
}

/// Build session state, register inputs, and capture the envelope.
pub async fn prepare_execution_context(
    session_factory: &SessionFactory,
    spec: &SemanticExecutionSpec,
    ruleset: &CpgRuleSet,
    tracing_config: Option<&TracingConfig>,
) -> Result<PreparedExecutionContext> {
    let overlaid_ruleset = spec.rule_overlay.as_ref().map(|overlay| {
        build_overlaid_ruleset(
            &spec.rulepack_profile,
            &spec.rule_intents,
            overlay,
            session_factory.profile(),
        )
    });
    let effective_ruleset = overlaid_ruleset.as_ref().unwrap_or(ruleset);

    let overrides = SessionBuildOverrides {
        enable_function_factory: spec.runtime.enable_function_factory,
        enable_domain_planner: spec.runtime.enable_domain_planner,
        enable_relation_planner: spec.runtime.enable_relation_planner,
        enable_delta_codec: spec.runtime.capture_delta_codec,
        extension_governance_policy: spec.runtime.extension_governance_mode,
    };

    let mut state = if let Some(profile) = &spec.runtime_profile {
        session_factory
            .build_session_state_from_profile_with_overrides(
                profile,
                effective_ruleset,
                spec.spec_hash,
                tracing_config,
                overrides,
            )
            .await?
    } else {
        session_factory
            .build_session_state_with_overrides(
                effective_ruleset,
                spec.spec_hash,
                tracing_config,
                overrides,
            )
            .await?
    };

    let registrations = register_extraction_inputs(&state.ctx, &spec.input_relations).await?;
    for warning in graph_validator::validate_delta_compatibility(spec, &registrations)? {
        state
            .build_warnings
            .push(crate::executor::warnings::RunWarning::new(
                crate::executor::warnings::WarningCode::DeltaCompatibilityDrift,
                crate::executor::warnings::WarningStage::Preflight,
                warning,
            ));
    }
    let mut provider_identities: Vec<ProviderIdentity> = registrations
        .iter()
        .map(|r| ProviderIdentity {
            table_name: r.name.clone(),
            identity_hash: r.provider_identity,
            delta_compatibility: Some(DeltaProviderCompatibility {
                min_reader_version: r.compatibility.min_reader_version,
                min_writer_version: r.compatibility.min_writer_version,
                reader_features: r.compatibility.reader_features.clone(),
                writer_features: r.compatibility.writer_features.clone(),
                column_mapping_mode: r.compatibility.column_mapping_mode.clone(),
                partition_columns: r.compatibility.partition_columns.clone(),
            }),
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
        effective_ruleset.fingerprint,
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
