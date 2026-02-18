//! Session factory for deterministic SessionContext construction.
//!
//! Uses SessionStateBuilder for builder-first session creation.
//! Both profile and non-profile session-state builders route through a
//! shared planning-surface path — see `planning_surface.rs`.
//!
//! The only post-build mutation is `install_rewrites()` for function
//! rewrites that lack a builder API in DataFusion 51.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_common::Result;

use crate::compiler::plan_codec;
use crate::executor::tracing as engine_tracing;
use crate::executor::warnings::RunWarning;
use crate::rules::registry::CpgRuleSet;
use crate::session::capture::{planning_config_snapshot, GovernancePolicy};
use crate::spec::runtime::TracingConfig;

use super::format_policy::{build_table_options, default_file_formats, FormatPolicySpec};
use super::planning_manifest::{manifest_from_surface_with_context, PlanningSurfaceManifest};
use super::planning_surface::{
    apply_to_builder, install_rewrites, PlanningSurfaceSpec, TableFactoryEntry,
};
use super::profile_coverage::reserved_profile_warnings;
use super::profiles::EnvironmentProfile;
use super::runtime_profiles::RuntimeProfileSpec;

/// Factory for building deterministic DataFusion sessions.
///
/// Constructs SessionContext via SessionStateBuilder with all configuration,
/// rules, and UDFs registered upfront. Post-build mutation is limited to
/// function rewrites via `install_rewrites()`.
pub struct SessionFactory {
    profile: EnvironmentProfile,
}

/// Canonical pre-registration session build output.
pub struct SessionBuildState {
    pub ctx: SessionContext,
    pub memory_pool_bytes: u64,
    pub planning_surface_manifest: PlanningSurfaceManifest,
    pub planning_surface_hash: [u8; 32],
    pub build_warnings: Vec<RunWarning>,
}

/// Optional session build behavior overrides used by execution entrypoints.
#[derive(Debug, Clone, Copy, Default)]
pub struct SessionBuildOverrides {
    pub enable_function_factory: bool,
    pub enable_domain_planner: bool,
    pub enable_delta_codec: bool,
    pub extension_governance_policy: GovernancePolicy,
}

impl SessionFactory {
    /// Creates a new SessionFactory with the given environment profile.
    ///
    /// # Arguments
    ///
    /// * `profile` - Environment-specific configuration parameters
    ///
    /// # Returns
    ///
    /// New SessionFactory instance
    pub fn new(profile: EnvironmentProfile) -> Self {
        Self { profile }
    }

    /// Returns the immutable environment profile used by this factory.
    pub fn profile(&self) -> &EnvironmentProfile {
        &self.profile
    }

    /// Build deterministic session state (without input registration).
    pub async fn build_session_state(
        &self,
        ruleset: &CpgRuleSet,
        spec_hash: [u8; 32],
        tracing_config: Option<&TracingConfig>,
    ) -> Result<SessionBuildState> {
        self.build_session_state_with_overrides(
            ruleset,
            spec_hash,
            tracing_config,
            SessionBuildOverrides::default(),
        )
        .await
    }

    /// Build deterministic session state with explicit planning/runtime overrides.
    pub async fn build_session_state_with_overrides(
        &self,
        ruleset: &CpgRuleSet,
        spec_hash: [u8; 32],
        tracing_config: Option<&TracingConfig>,
        overrides: SessionBuildOverrides,
    ) -> Result<SessionBuildState> {
        let memory_pool = Arc::new(FairSpillPool::new(
            self.profile.memory_pool_bytes.try_into().unwrap(),
        ));
        let disk_manager_builder =
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory);
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_pool(memory_pool)
            .with_disk_manager_builder(disk_manager_builder)
            .build_arc()?;

        let mut config = SessionConfig::new()
            .with_default_catalog_and_schema("codeanatomy", "public")
            .with_information_schema(true)
            .with_target_partitions(self.profile.target_partitions as usize)
            .with_batch_size(self.profile.batch_size as usize)
            .with_repartition_joins(true)
            .with_repartition_aggregations(true)
            .with_repartition_windows(true)
            .with_parquet_pruning(true);
        let config_opts = config.options_mut();
        config_opts.execution.coalesce_batches = true;
        config_opts.execution.collect_statistics = true;
        config_opts.execution.parquet.pushdown_filters = true;
        config_opts.execution.parquet.enable_page_index = true;
        config_opts.execution.enable_recursive_ctes = true;
        config_opts.optimizer.filter_null_join_keys = true;
        config_opts.optimizer.skip_failed_rules = false;
        config_opts.optimizer.max_passes = 3;
        config_opts.optimizer.enable_dynamic_filter_pushdown = true;
        config_opts.optimizer.enable_topk_dynamic_filter_pushdown = true;
        config_opts.optimizer.enable_sort_pushdown = true;
        config_opts.optimizer.allow_symmetric_joins_without_pruning = true;
        config_opts.execution.planning_concurrency = self.profile.target_partitions as usize;
        config_opts.sql_parser.enable_ident_normalization = false;
        config_opts.explain.show_statistics = true;
        config_opts.explain.show_schema = true;

        self.build_session_state_internal(
            format!("{:?}", self.profile.class),
            self.profile.memory_pool_bytes,
            config,
            runtime,
            ruleset,
            spec_hash,
            tracing_config,
            overrides,
            Vec::new(),
        )
        .await
    }

    /// Build deterministic session state from a runtime profile.
    pub async fn build_session_state_from_profile(
        &self,
        profile: &RuntimeProfileSpec,
        ruleset: &CpgRuleSet,
        enable_function_factory: bool,
        enable_domain_planner: bool,
        spec_hash: [u8; 32],
        tracing_config: Option<&TracingConfig>,
    ) -> Result<SessionBuildState> {
        self.build_session_state_from_profile_with_overrides(
            profile,
            ruleset,
            spec_hash,
            tracing_config,
            SessionBuildOverrides {
                enable_function_factory,
                enable_domain_planner,
                ..SessionBuildOverrides::default()
            },
        )
        .await
    }

    /// Build deterministic session state from a runtime profile with explicit overrides.
    pub async fn build_session_state_from_profile_with_overrides(
        &self,
        profile: &RuntimeProfileSpec,
        ruleset: &CpgRuleSet,
        spec_hash: [u8; 32],
        tracing_config: Option<&TracingConfig>,
        overrides: SessionBuildOverrides,
    ) -> Result<SessionBuildState> {
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_pool(Arc::new(FairSpillPool::new(profile.memory_pool_bytes)))
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
            )
            .with_max_temp_directory_size(profile.max_temp_directory_bytes as u64)
            .build_arc()?;

        let mut config = SessionConfig::new()
            .with_default_catalog_and_schema("codeanatomy", "public")
            .with_information_schema(true)
            .with_target_partitions(profile.target_partitions)
            .with_batch_size(profile.batch_size)
            .with_repartition_joins(profile.repartition_joins)
            .with_repartition_aggregations(profile.repartition_aggregations)
            .with_repartition_windows(profile.repartition_windows)
            .with_repartition_sorts(profile.repartition_sorts)
            .with_repartition_file_scans(profile.repartition_file_scans)
            .with_repartition_file_min_size(profile.repartition_file_min_size)
            .with_parquet_pruning(profile.parquet_pruning);

        let opts = config.options_mut();
        opts.execution.coalesce_batches = true;
        opts.execution.collect_statistics = profile.collect_statistics;
        opts.execution.parquet.pushdown_filters = profile.pushdown_filters;
        opts.execution.parquet.enable_page_index = profile.enable_page_index;
        opts.execution.parquet.metadata_size_hint = Some(profile.metadata_size_hint);
        opts.execution.enable_recursive_ctes = profile.enable_recursive_ctes;
        opts.execution.planning_concurrency = profile.planning_concurrency;
        opts.optimizer.max_passes = profile.optimizer_max_passes;
        opts.optimizer.skip_failed_rules = profile.skip_failed_rules;
        opts.optimizer.filter_null_join_keys = profile.filter_null_join_keys;
        opts.optimizer.enable_dynamic_filter_pushdown = profile.enable_dynamic_filter_pushdown;
        opts.optimizer.enable_topk_dynamic_filter_pushdown =
            profile.enable_topk_dynamic_filter_pushdown;
        opts.optimizer.enable_sort_pushdown = profile.enable_sort_pushdown;
        opts.optimizer.allow_symmetric_joins_without_pruning =
            profile.allow_symmetric_joins_without_pruning;
        opts.sql_parser.enable_ident_normalization = profile.enable_ident_normalization;
        opts.explain.show_statistics = profile.show_statistics;
        opts.explain.show_schema = profile.show_schema;

        self.build_session_state_internal(
            profile.profile_name.clone(),
            profile.memory_pool_bytes as u64,
            config,
            runtime,
            ruleset,
            spec_hash,
            tracing_config,
            overrides,
            reserved_profile_warnings(profile),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_session_state_internal(
        &self,
        profile_name: String,
        memory_pool_bytes: u64,
        config: SessionConfig,
        runtime: Arc<RuntimeEnv>,
        ruleset: &CpgRuleSet,
        spec_hash: [u8; 32],
        tracing_config: Option<&TracingConfig>,
        overrides: SessionBuildOverrides,
        mut build_warnings: Vec<RunWarning>,
    ) -> Result<SessionBuildState> {
        let tracing_config = tracing_config.cloned().unwrap_or_default();
        engine_tracing::init_otel_tracing(&tracing_config)?;
        let trace_ctx = engine_tracing::TraceRuleContext::from_hashes(
            &spec_hash,
            &ruleset.fingerprint(),
            &profile_name,
            &tracing_config,
        );
        let physical_rules = engine_tracing::append_execution_instrumentation_rule(
            ruleset.physical_rules().to_vec(),
            &tracing_config,
            &trace_ctx,
        );

        let planning_surface = Self::build_planning_surface(&config, overrides)?;

        let builder = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_analyzer_rules(ruleset.analyzer_rules().to_vec())
            .with_optimizer_rules(ruleset.optimizer_rules().to_vec())
            .with_physical_optimizer_rules(physical_rules);
        let builder = apply_to_builder(builder, &planning_surface);
        let state = builder.build();
        let state = engine_tracing::instrument_session_state(state, &tracing_config, &trace_ctx);
        let ctx = SessionContext::new_with_state(state);

        datafusion_ext::udf_registry::register_all(&ctx)?;
        if planning_surface.delta_codec_enabled {
            plan_codec::install_delta_codecs(&ctx);
        }
        engine_tracing::register_instrumented_file_store(&ctx, &tracing_config)?;
        install_rewrites(&ctx, &planning_surface.function_rewrites)?;

        enforce_extension_governance(&planning_surface, &mut build_warnings)?;
        let planning_surface_manifest =
            manifest_from_surface_with_context(&planning_surface, &ctx).await?;
        let planning_surface_hash = planning_surface_manifest.hash();
        Ok(SessionBuildState {
            ctx,
            memory_pool_bytes,
            planning_surface_manifest,
            planning_surface_hash,
            build_warnings,
        })
    }

    fn build_planning_surface(
        config: &SessionConfig,
        overrides: SessionBuildOverrides,
    ) -> Result<PlanningSurfaceSpec> {
        let options = config.options();
        let format_policy = FormatPolicySpec {
            parquet_pushdown_filters: options.execution.parquet.pushdown_filters,
            parquet_enable_page_index: options.execution.parquet.enable_page_index,
            csv_delimiter: None,
        };

        let mut planning_surface = PlanningSurfaceSpec {
            enable_default_features: true,
            file_formats: default_file_formats(),
            table_options: Some(build_table_options(config, &format_policy)?),
            delta_codec_enabled: overrides.enable_delta_codec,
            planning_config_keys: planning_config_snapshot(config),
            extension_policy: overrides.extension_governance_policy,
            ..PlanningSurfaceSpec::default()
        };

        planning_surface.table_factories.push((
            "delta".to_string(),
            Arc::new(deltalake::delta_datafusion::DeltaTableFactory {}),
        ));

        #[cfg(feature = "delta-planner")]
        {
            use deltalake::delta_datafusion::planner::DeltaPlanner;
            planning_surface.query_planner = Some(DeltaPlanner::new());
        }

        if overrides.enable_function_factory {
            planning_surface.function_factory = Some(Arc::new(
                datafusion_ext::sql_macro_factory::SqlMacroFunctionFactory,
            ));
        }
        if overrides.enable_domain_planner {
            planning_surface.expr_planners = datafusion_ext::domain_expr_planners();
            planning_surface.function_rewrites = datafusion_ext::domain_function_rewrites();
        }

        planning_surface.table_factory_allowlist = planning_surface
            .table_factories
            .iter()
            .map(|(_name, factory)| TableFactoryEntry {
                factory_type: std::any::type_name_of_val(factory.as_ref()).to_string(),
                identity_hash: *blake3::hash(
                    std::any::type_name_of_val(factory.as_ref()).as_bytes(),
                )
                .as_bytes(),
            })
            .collect();

        Ok(planning_surface)
    }
}

fn enforce_extension_governance(
    surface: &PlanningSurfaceSpec,
    warnings: &mut Vec<RunWarning>,
) -> Result<()> {
    match surface.extension_policy {
        GovernancePolicy::Permissive => {}
        GovernancePolicy::WarnOnUnregistered | GovernancePolicy::StrictAllowlist => {
            let allowlist: BTreeMap<&str, [u8; 32]> = surface
                .table_factory_allowlist
                .iter()
                .map(|entry| (entry.factory_type.as_str(), entry.identity_hash))
                .collect();
            for (_name, factory) in &surface.table_factories {
                let factory_type = std::any::type_name_of_val(factory.as_ref());
                let identity = *blake3::hash(factory_type.as_bytes()).as_bytes();
                let allowed = allowlist
                    .get(factory_type)
                    .map(|hash| *hash == identity)
                    .unwrap_or(false);
                if !allowed {
                    if matches!(surface.extension_policy, GovernancePolicy::StrictAllowlist) {
                        return Err(datafusion_common::DataFusionError::Plan(format!(
                            "Table factory '{factory_type}' is not present in strict allowlist"
                        )));
                    }
                    warnings.push(
                        RunWarning::new(
                            crate::executor::warnings::WarningCode::ReservedProfileKnobIgnored,
                            crate::executor::warnings::WarningStage::Preflight,
                            format!(
                                "Table factory '{factory_type}' is not present in planning-surface allowlist"
                            ),
                        )
                        .with_context(
                            "extension_policy",
                            surface.extension_policy.as_str(),
                        ),
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::registry::CpgRuleSet;
    use crate::session::envelope::SessionEnvelope;
    use crate::session::profiles::{EnvironmentClass, EnvironmentProfile};

    #[tokio::test]
    async fn test_session_factory_builds_context() {
        let profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
        let factory = SessionFactory::new(profile);

        // Create empty ruleset for testing
        let ruleset = CpgRuleSet {
            analyzer_rules: vec![],
            optimizer_rules: vec![],
            physical_rules: vec![],
            fingerprint: [0u8; 32],
        };

        let state = factory
            .build_session_state(&ruleset, [0u8; 32], None)
            .await
            .unwrap();
        let envelope = SessionEnvelope::capture(
            &state.ctx,
            [0u8; 32],
            ruleset.fingerprint,
            state.memory_pool_bytes,
            true,
            state.planning_surface_hash,
            SessionEnvelope::hash_provider_identities(&[]),
        )
        .await
        .unwrap();

        // Verify session context is usable
        let sql_result = state.ctx.sql("SELECT 1 as test").await;
        assert!(sql_result.is_ok());

        // Verify envelope captures correct metadata
        assert_eq!(envelope.datafusion_version, datafusion::DATAFUSION_VERSION);
        assert_eq!(envelope.codeanatomy_version, env!("CARGO_PKG_VERSION"));
        assert!(!envelope.registered_functions.is_empty());
    }

    #[tokio::test]
    async fn test_session_factory_different_profiles() {
        let small_profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
        let large_profile = EnvironmentProfile::from_class(EnvironmentClass::Large);

        let small_factory = SessionFactory::new(small_profile.clone());
        let large_factory = SessionFactory::new(large_profile.clone());

        let ruleset = CpgRuleSet {
            analyzer_rules: vec![],
            optimizer_rules: vec![],
            physical_rules: vec![],
            fingerprint: [0u8; 32],
        };

        let small_state = small_factory
            .build_session_state(&ruleset, [0u8; 32], None)
            .await
            .unwrap();
        let small_envelope = SessionEnvelope::capture(
            &small_state.ctx,
            [0u8; 32],
            ruleset.fingerprint,
            small_state.memory_pool_bytes,
            true,
            small_state.planning_surface_hash,
            SessionEnvelope::hash_provider_identities(&[]),
        )
        .await
        .unwrap();
        let large_state = large_factory
            .build_session_state(&ruleset, [0u8; 32], None)
            .await
            .unwrap();
        let large_envelope = SessionEnvelope::capture(
            &large_state.ctx,
            [0u8; 32],
            ruleset.fingerprint,
            large_state.memory_pool_bytes,
            true,
            large_state.planning_surface_hash,
            SessionEnvelope::hash_provider_identities(&[]),
        )
        .await
        .unwrap();

        // Verify different configurations
        assert_eq!(small_envelope.target_partitions, 4);
        assert_eq!(large_envelope.target_partitions, 16);
        assert_eq!(small_envelope.batch_size, 4096);
        assert_eq!(large_envelope.batch_size, 16384);

        // Verify different envelope hashes (due to different configs)
        assert_ne!(small_envelope.envelope_hash, large_envelope.envelope_hash);
    }
}
