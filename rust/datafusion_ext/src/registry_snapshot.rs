use std::collections::BTreeMap;

use datafusion::execution::session_state::SessionState;
use datafusion_expr::{Signature, Volatility};

pub struct RegistrySnapshot {
    pub scalar: Vec<String>,
    pub aggregate: Vec<String>,
    pub window: Vec<String>,
    pub table: Vec<String>,
    pub aliases: BTreeMap<String, Vec<String>>,
    pub parameter_names: BTreeMap<String, Vec<String>>,
    pub volatility: BTreeMap<String, String>,
}

impl RegistrySnapshot {
    pub fn new() -> Self {
        Self {
            scalar: Vec::new(),
            aggregate: Vec::new(),
            window: Vec::new(),
            table: Vec::new(),
            aliases: BTreeMap::new(),
            parameter_names: BTreeMap::new(),
            volatility: BTreeMap::new(),
        }
    }
}

pub fn registry_snapshot(state: &SessionState) -> RegistrySnapshot {
    let mut snapshot = RegistrySnapshot::new();
    record_scalar_udfs(state, &mut snapshot);
    record_aggregate_udfs(state, &mut snapshot);
    record_window_udfs(state, &mut snapshot);
    record_table_functions(state, &mut snapshot);
    snapshot.scalar.sort();
    snapshot.aggregate.sort();
    snapshot.window.sort();
    snapshot.table.sort();
    snapshot
}

fn record_scalar_udfs(state: &SessionState, snapshot: &mut RegistrySnapshot) {
    for (name, udf) in state.scalar_functions() {
        snapshot.scalar.push(name.clone());
        record_aliases(name, udf.aliases(), &mut snapshot.aliases);
        record_signature(name, udf.signature(), &mut snapshot.parameter_names, &mut snapshot.volatility);
    }
}

fn record_aggregate_udfs(state: &SessionState, snapshot: &mut RegistrySnapshot) {
    for (name, udaf) in state.aggregate_functions() {
        snapshot.aggregate.push(name.clone());
        record_aliases(name, udaf.aliases(), &mut snapshot.aliases);
        record_signature(
            name,
            udaf.signature(),
            &mut snapshot.parameter_names,
            &mut snapshot.volatility,
        );
    }
}

fn record_window_udfs(state: &SessionState, snapshot: &mut RegistrySnapshot) {
    for (name, udwf) in state.window_functions() {
        snapshot.window.push(name.clone());
        record_aliases(name, udwf.aliases(), &mut snapshot.aliases);
        record_signature(
            name,
            udwf.signature(),
            &mut snapshot.parameter_names,
            &mut snapshot.volatility,
        );
    }
}

fn record_table_functions(state: &SessionState, snapshot: &mut RegistrySnapshot) {
    for (name, _udtf) in state.table_functions() {
        snapshot.table.push(name.clone());
    }
}

fn record_aliases(name: &str, aliases: &[String], target: &mut BTreeMap<String, Vec<String>>) {
    if aliases.is_empty() {
        return;
    }
    target.insert(name.to_string(), aliases.to_vec());
}

fn record_signature(
    name: &str,
    signature: &Signature,
    params_target: &mut BTreeMap<String, Vec<String>>,
    volatility_target: &mut BTreeMap<String, String>,
) {
    if let Some(names) = signature.parameter_names.clone() {
        if !names.is_empty() {
            params_target.insert(name.to_string(), names);
        }
    }
    volatility_target.insert(name.to_string(), volatility_label(signature.volatility));
}

fn volatility_label(value: Volatility) -> String {
    match value {
        Volatility::Immutable => "immutable".to_string(),
        Volatility::Stable => "stable".to_string(),
        Volatility::Volatile => "volatile".to_string(),
    }
}
