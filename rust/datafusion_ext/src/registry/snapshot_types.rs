//! Snapshot type authority.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::function_types::FunctionKind;
use crate::udf_config::UdfConfigValue;

#[derive(Debug, Clone, Serialize)]
pub struct RegistrySnapshot {
    pub(crate) version: u32,
    pub(crate) scalar: Vec<String>,
    pub(crate) aggregate: Vec<String>,
    pub(crate) window: Vec<String>,
    pub(crate) table: Vec<String>,
    pub(crate) aliases: BTreeMap<String, Vec<String>>,
    pub(crate) parameter_names: BTreeMap<String, Vec<String>>,
    pub(crate) volatility: BTreeMap<String, String>,
    pub(crate) rewrite_tags: BTreeMap<String, Vec<String>>,
    pub(crate) simplify: BTreeMap<String, bool>,
    pub(crate) coerce_types: BTreeMap<String, bool>,
    pub(crate) short_circuits: BTreeMap<String, bool>,
    pub(crate) signature_inputs: BTreeMap<String, Vec<Vec<String>>>,
    pub(crate) return_types: BTreeMap<String, Vec<String>>,
    pub(crate) config_defaults: BTreeMap<String, BTreeMap<String, UdfConfigValue>>,
    pub(crate) custom_udfs: Vec<String>,
}

impl RegistrySnapshot {
    pub const CURRENT_VERSION: u32 = 1;

    pub fn new() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            scalar: Vec::new(),
            aggregate: Vec::new(),
            window: Vec::new(),
            table: Vec::new(),
            aliases: BTreeMap::new(),
            parameter_names: BTreeMap::new(),
            volatility: BTreeMap::new(),
            rewrite_tags: BTreeMap::new(),
            simplify: BTreeMap::new(),
            coerce_types: BTreeMap::new(),
            short_circuits: BTreeMap::new(),
            signature_inputs: BTreeMap::new(),
            return_types: BTreeMap::new(),
            config_defaults: BTreeMap::new(),
            custom_udfs: Vec::new(),
        }
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn scalar(&self) -> &[String] {
        &self.scalar
    }

    pub fn aggregate(&self) -> &[String] {
        &self.aggregate
    }

    pub fn window(&self) -> &[String] {
        &self.window
    }

    pub fn table(&self) -> &[String] {
        &self.table
    }

    pub fn aliases(&self) -> &BTreeMap<String, Vec<String>> {
        &self.aliases
    }

    pub fn parameter_names(&self) -> &BTreeMap<String, Vec<String>> {
        &self.parameter_names
    }

    pub fn volatility(&self) -> &BTreeMap<String, String> {
        &self.volatility
    }

    pub fn rewrite_tags(&self) -> &BTreeMap<String, Vec<String>> {
        &self.rewrite_tags
    }

    pub fn simplify(&self) -> &BTreeMap<String, bool> {
        &self.simplify
    }

    pub fn coerce_types(&self) -> &BTreeMap<String, bool> {
        &self.coerce_types
    }

    pub fn short_circuits(&self) -> &BTreeMap<String, bool> {
        &self.short_circuits
    }

    pub fn signature_inputs(&self) -> &BTreeMap<String, Vec<Vec<String>>> {
        &self.signature_inputs
    }

    pub fn return_types(&self) -> &BTreeMap<String, Vec<String>> {
        &self.return_types
    }

    pub fn config_defaults(&self) -> &BTreeMap<String, BTreeMap<String, UdfConfigValue>> {
        &self.config_defaults
    }

    pub fn custom_udfs(&self) -> &[String] {
        &self.custom_udfs
    }
}

impl Default for RegistrySnapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// Capability inventory for a single registered function, covering all
/// optimizer-visible hooks across scalar, aggregate, and window function kinds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionHookCapabilities {
    pub name: String,
    pub kind: FunctionKind,
    pub has_simplify: bool,
    pub has_coerce_types: bool,
    pub has_short_circuits: bool,
    pub has_propagate_constraints: bool,
    pub has_groups_accumulator: bool,
    pub has_retract_batch: bool,
    pub has_reverse_expr: bool,
    pub has_sort_options: bool,
}
