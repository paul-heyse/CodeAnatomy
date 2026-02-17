//! Snapshot type authority.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::function_types::FunctionKind;
use crate::udf_config::UdfConfigValue;

#[derive(Debug, Clone, Serialize)]
pub struct RegistrySnapshot {
    pub version: u32,
    pub scalar: Vec<String>,
    pub aggregate: Vec<String>,
    pub window: Vec<String>,
    pub table: Vec<String>,
    pub aliases: BTreeMap<String, Vec<String>>,
    pub parameter_names: BTreeMap<String, Vec<String>>,
    pub volatility: BTreeMap<String, String>,
    pub rewrite_tags: BTreeMap<String, Vec<String>>,
    pub simplify: BTreeMap<String, bool>,
    pub coerce_types: BTreeMap<String, bool>,
    pub short_circuits: BTreeMap<String, bool>,
    pub signature_inputs: BTreeMap<String, Vec<Vec<String>>>,
    pub return_types: BTreeMap<String, Vec<String>>,
    pub config_defaults: BTreeMap<String, BTreeMap<String, UdfConfigValue>>,
    pub custom_udfs: Vec<String>,
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
