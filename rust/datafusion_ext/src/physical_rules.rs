use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::{ConfigExtension, ConfigOptions};
use datafusion_common::{DataFusionError, Result};

const PREFIX: &str = "codeanatomy_physical";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeAnatomyPhysicalConfig {
    pub enabled: bool,
    pub coalesce_partitions: bool,
}

impl Default for CodeAnatomyPhysicalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            coalesce_partitions: true,
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
        (enabled, bool, "Enable CodeAnatomy physical rulepack."),
        (
            coalesce_partitions,
            bool,
            "Coalesce partitions in the physical plan."
        ),
    ]
);

impl ConfigExtension for CodeAnatomyPhysicalConfig {
    const PREFIX: &'static str = PREFIX;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CodeAnatomyPhysicalRuleInstalled {
    installed: bool,
}

impl Default for CodeAnatomyPhysicalRuleInstalled {
    fn default() -> Self {
        Self { installed: true }
    }
}

crate::impl_extension_options!(
    CodeAnatomyPhysicalRuleInstalled,
    prefix = "codeanatomy_physical_rule_installed",
    unknown_key = "Unknown physical rule install marker key: {key}",
    fields = [(
        installed,
        bool,
        "Marker extension that indicates CodeAnatomyPhysicalRule was installed."
    ),]
);

impl ConfigExtension for CodeAnatomyPhysicalRuleInstalled {
    const PREFIX: &'static str = "codeanatomy_physical_rule_installed";
}

pub fn ensure_physical_config(
    options: &mut ConfigOptions,
) -> Result<&mut CodeAnatomyPhysicalConfig> {
    if options
        .extensions
        .get::<CodeAnatomyPhysicalConfig>()
        .is_none()
    {
        options
            .extensions
            .insert(CodeAnatomyPhysicalConfig::default());
    }
    options
        .extensions
        .get_mut::<CodeAnatomyPhysicalConfig>()
        .ok_or_else(|| {
            DataFusionError::Internal("CodeAnatomyPhysicalConfig should be installed".to_string())
        })
}

#[derive(Debug, Default)]
pub struct CodeAnatomyPhysicalRule;

impl PhysicalOptimizerRule for CodeAnatomyPhysicalRule {
    #[tracing::instrument(level = "debug", skip_all, fields(rule = "CodeAnatomyPhysicalRule"))]
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let policy = CodeAnatomyPhysicalConfig::from_config(config);
        if !policy.enabled {
            return Ok(plan);
        }
        if policy.coalesce_partitions
            && (config.optimizer.enable_dynamic_filter_pushdown
                || config.optimizer.enable_sort_pushdown)
        {
            return Ok(plan);
        }
        if policy.coalesce_partitions {
            return Ok(Arc::new(CoalescePartitionsExec::new(plan)));
        }
        Ok(plan)
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
    let options = config.options_mut();
    let _ = ensure_physical_config(options)?;
    if options
        .extensions
        .get::<CodeAnatomyPhysicalRuleInstalled>()
        .is_some()
    {
        return Ok(());
    }
    options
        .extensions
        .insert(CodeAnatomyPhysicalRuleInstalled::default());
    let builder = SessionStateBuilder::from(state.clone())
        .with_physical_optimizer_rule(Arc::new(CodeAnatomyPhysicalRule));
    *state = builder.build();
    Ok(())
}
