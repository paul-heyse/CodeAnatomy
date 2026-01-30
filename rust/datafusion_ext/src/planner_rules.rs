use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion_common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::LogicalPlan;

const PREFIX: &str = "codeanatomy_policy";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeAnatomyPolicyConfig {
    pub allow_ddl: bool,
    pub allow_dml: bool,
    pub allow_statements: bool,
}

impl Default for CodeAnatomyPolicyConfig {
    fn default() -> Self {
        Self {
            allow_ddl: true,
            allow_dml: true,
            allow_statements: true,
        }
    }
}

impl CodeAnatomyPolicyConfig {
    pub fn from_config(config: &ConfigOptions) -> Self {
        config
            .extensions
            .get::<CodeAnatomyPolicyConfig>()
            .cloned()
            .unwrap_or_default()
    }
}

impl ExtensionOptions for CodeAnatomyPolicyConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        match key {
            "allow_ddl" => {
                self.allow_ddl = parse_bool(value, key)?;
            }
            "allow_dml" => {
                self.allow_dml = parse_bool(value, key)?;
            }
            "allow_statements" => {
                self.allow_statements = parse_bool(value, key)?;
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Unknown CodeAnatomy policy config key: {key}"
                )))
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            ConfigEntry {
                key: format!("{PREFIX}.allow_ddl"),
                value: Some(self.allow_ddl.to_string()),
                description: "Allow DDL logical plans (CREATE/DROP/etc).",
            },
            ConfigEntry {
                key: format!("{PREFIX}.allow_dml"),
                value: Some(self.allow_dml.to_string()),
                description: "Allow DML logical plans (INSERT/UPDATE/DELETE).",
            },
            ConfigEntry {
                key: format!("{PREFIX}.allow_statements"),
                value: Some(self.allow_statements.to_string()),
                description: "Allow DataFusion Statement logical plans.",
            },
        ]
    }
}

impl ConfigExtension for CodeAnatomyPolicyConfig {
    const PREFIX: &'static str = PREFIX;
}

#[derive(Debug, Default)]
pub struct CodeAnatomyPolicyRule;

impl AnalyzerRule for CodeAnatomyPolicyRule {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let policy = CodeAnatomyPolicyConfig::from_config(config);
        validate_plan_policy(&plan, &policy)?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "codeanatomy_policy_rule"
    }
}

pub fn ensure_policy_config(options: &mut ConfigOptions) -> &mut CodeAnatomyPolicyConfig {
    if options.extensions.get::<CodeAnatomyPolicyConfig>().is_none() {
        options.extensions.insert(CodeAnatomyPolicyConfig::default());
    }
    options
        .extensions
        .get_mut::<CodeAnatomyPolicyConfig>()
        .expect("CodeAnatomyPolicyConfig should be installed")
}

pub fn install_policy_rules(ctx: &SessionContext) -> Result<()> {
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    state.add_analyzer_rule(Arc::new(CodeAnatomyPolicyRule::default()));
    Ok(())
}

fn validate_plan_policy(plan: &LogicalPlan, policy: &CodeAnatomyPolicyConfig) -> Result<()> {
    match plan {
        LogicalPlan::Ddl(_) if !policy.allow_ddl => {
            return Err(DataFusionError::Plan(
                "DDL statements are disabled by policy.".to_string(),
            ));
        }
        LogicalPlan::Dml(_) if !policy.allow_dml => {
            return Err(DataFusionError::Plan(
                "DML statements are disabled by policy.".to_string(),
            ));
        }
        LogicalPlan::Copy(_) if !policy.allow_dml => {
            return Err(DataFusionError::Plan(
                "COPY operations are disabled by policy.".to_string(),
            ));
        }
        LogicalPlan::Statement(_) if !policy.allow_statements => {
            return Err(DataFusionError::Plan(
                "Statement execution is disabled by policy.".to_string(),
            ));
        }
        _ => {}
    }
    for child in plan.inputs() {
        validate_plan_policy(child, policy)?;
    }
    Ok(())
}

fn parse_bool(value: &str, key: &str) -> Result<bool> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid boolean for {key}: {value}"
        ))),
    }
}
