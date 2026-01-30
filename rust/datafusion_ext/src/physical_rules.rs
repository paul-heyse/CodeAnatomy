use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::{ConfigExtension, ConfigOptions};
use datafusion_common::Result;

const PREFIX: &str = "codeanatomy_physical";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeAnatomyPhysicalConfig {
    pub enabled: bool,
    pub coalesce_partitions: bool,
    pub coalesce_batches: bool,
}

impl Default for CodeAnatomyPhysicalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            coalesce_partitions: true,
            coalesce_batches: true,
        }
    }
}

impl CodeAnatomyPhysicalConfig {
    pub fn from_config(config: &ConfigOptions) -> Self {
        config
            .extensions
            .get::<CodeAnatomyPhysicalConfig>()
            .cloned()
            .unwrap_or_default()
    }
}

crate::impl_extension_options!(
    CodeAnatomyPhysicalConfig,
    prefix = PREFIX,
    unknown_key = "Unknown CodeAnatomy physical config key: {key}",
    fields = [
        (
            enabled,
            bool,
            "Enable CodeAnatomy physical rulepack."
        ),
        (
            coalesce_partitions,
            bool,
            "Coalesce partitions in the physical plan."
        ),
        (
            coalesce_batches,
            bool,
            "Coalesce batches in the physical plan."
        ),
    ]
);

impl ConfigExtension for CodeAnatomyPhysicalConfig {
    const PREFIX: &'static str = PREFIX;
}

pub fn ensure_physical_config(options: &mut ConfigOptions) -> &mut CodeAnatomyPhysicalConfig {
    if options.extensions.get::<CodeAnatomyPhysicalConfig>().is_none() {
        options.extensions.insert(CodeAnatomyPhysicalConfig::default());
    }
    options
        .extensions
        .get_mut::<CodeAnatomyPhysicalConfig>()
        .expect("CodeAnatomyPhysicalConfig should be installed")
}

#[derive(Debug, Default)]
pub struct CodeAnatomyPhysicalRule;

impl PhysicalOptimizerRule for CodeAnatomyPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let policy = CodeAnatomyPhysicalConfig::from_config(config);
        if !policy.enabled {
            return Ok(plan);
        }
        let mut optimized = plan;
        if policy.coalesce_partitions {
            optimized = Arc::new(CoalescePartitionsExec::new(optimized));
        }
        if policy.coalesce_batches {
            optimized = CoalesceBatches::new().optimize(optimized, config)?;
        }
        Ok(optimized)
    }

    fn name(&self) -> &str {
        "codeanatomy_physical_rule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

pub fn install_physical_rules(ctx: &SessionContext) -> Result<()> {
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    let _ = ensure_physical_config(config.options_mut());
    let builder = SessionStateBuilder::from(state.clone())
        .with_physical_optimizer_rule(Arc::new(CodeAnatomyPhysicalRule::default()));
    *state = builder.build();
    Ok(())
}
