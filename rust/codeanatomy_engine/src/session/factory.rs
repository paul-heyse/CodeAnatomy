//! Session factory for deterministic SessionContext construction.
//!
//! Uses SessionStateBuilder for builder-first session creation with no
//! post-build mutation.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_common::Result;

use super::envelope::SessionEnvelope;
use super::profiles::EnvironmentProfile;
use crate::rules::registry::CpgRuleSet;

/// Factory for building deterministic DataFusion sessions.
///
/// Constructs SessionContext via SessionStateBuilder with all configuration,
/// rules, and UDFs registered upfront. No post-build mutation allowed.
pub struct SessionFactory {
    profile: EnvironmentProfile,
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

    /// Builds a deterministic SessionContext with the given ruleset.
    ///
    /// Construction order:
    /// 1. Build RuntimeEnv with memory pool and disk manager
    /// 2. Build SessionConfig with typed configuration
    /// 3. Build SessionState via SessionStateBuilder with rules
    /// 4. Create SessionContext from state
    /// 5. Register all UDFs via datafusion_ext
    /// 6. Capture SessionEnvelope
    ///
    /// # Arguments
    ///
    /// * `ruleset` - Immutable rule set for execution policy
    /// * `spec_hash` - BLAKE3 hash of the execution spec
    ///
    /// # Returns
    ///
    /// Tuple of (SessionContext, SessionEnvelope)
    pub async fn build_session(
        &self,
        ruleset: &CpgRuleSet,
        spec_hash: [u8; 32],
    ) -> Result<(SessionContext, SessionEnvelope)> {
        // Build RuntimeEnv with memory pool and disk manager
        let memory_pool = Arc::new(FairSpillPool::new(
            self.profile.memory_pool_bytes.try_into().unwrap(),
        ));
        let disk_manager_builder =
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory);

        let runtime = RuntimeEnvBuilder::default()
            .with_memory_pool(memory_pool)
            .with_disk_manager_builder(disk_manager_builder)
            .build_arc()?;

        // Build SessionConfig with typed configuration
        let mut config = SessionConfig::new()
            .with_default_catalog_and_schema("codeanatomy", "public")
            .with_information_schema(true)
            .with_target_partitions(self.profile.target_partitions as usize)
            .with_batch_size(self.profile.batch_size as usize)
            .with_repartition_joins(true)
            .with_repartition_aggregations(true)
            .with_repartition_windows(true)
            .with_parquet_pruning(true);

        // Typed config mutation via options_mut()
        let config_opts = config.options_mut();
        config_opts.execution.coalesce_batches = true;
        config_opts.execution.collect_statistics = true;
        config_opts.execution.parquet.pushdown_filters = true;
        config_opts.execution.enable_recursive_ctes = true;

        // Optimizer options
        config_opts.optimizer.filter_null_join_keys = true;
        config_opts.optimizer.skip_failed_rules = false;
        config_opts.optimizer.max_passes = 3;
        config_opts.optimizer.enable_dynamic_filter_pushdown = true;
        config_opts.optimizer.enable_topk_dynamic_filter_pushdown = true;

        // Parallel planning for UNION children (materially reduces compile time)
        config_opts.execution.planning_concurrency = self.profile.target_partitions as usize;

        // Canonical lowercase, no normalization surprises
        config_opts.sql_parser.enable_ident_normalization = false;

        // Explain options for debugging
        config_opts.explain.show_statistics = true;
        config_opts.explain.show_schema = true;

        // Build SessionState via SessionStateBuilder
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_analyzer_rules(ruleset.analyzer_rules.clone())
            .with_optimizer_rules(ruleset.optimizer_rules.clone())
            .with_physical_optimizer_rules(ruleset.physical_rules.clone())
            .build();

        // Create SessionContext from state
        let ctx = SessionContext::new_with_state(state);

        // Register all UDFs from datafusion_ext
        datafusion_ext::udf_registry::register_all(&ctx)?;

        // Capture SessionEnvelope
        let envelope = SessionEnvelope::capture(
            &ctx,
            spec_hash,
            ruleset.fingerprint,
            self.profile.memory_pool_bytes,
            true, // spill_enabled (FairSpillPool always enables spilling)
        )
        .await?;

        Ok((ctx, envelope))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::registry::CpgRuleSet;
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

        let result = factory.build_session(&ruleset, [0u8; 32]).await;
        assert!(result.is_ok());

        let (ctx, envelope) = result.unwrap();

        // Verify session context is usable
        let sql_result = ctx.sql("SELECT 1 as test").await;
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

        let (_small_ctx, small_envelope) = small_factory
            .build_session(&ruleset, [0u8; 32])
            .await
            .unwrap();
        let (_large_ctx, large_envelope) = large_factory
            .build_session(&ruleset, [0u8; 32])
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
