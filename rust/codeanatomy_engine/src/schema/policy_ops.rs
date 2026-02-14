//! JSON policy/spec helpers for Python boundary adapters.

use serde_json::{Map, Value};

fn as_object(value: &Value) -> Option<&Map<String, Value>> {
    value.as_object()
}

fn merge_json(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base_obj), Value::Object(overlay_obj)) => {
            for (key, value) in overlay_obj {
                match base_obj.get_mut(&key) {
                    Some(existing) => merge_json(existing, value),
                    None => {
                        base_obj.insert(key, value);
                    }
                }
            }
        }
        (base_slot, overlay_value) => {
            *base_slot = overlay_value;
        }
    }
}

pub fn dataset_name(spec: &Value) -> Option<String> {
    as_object(spec)
        .and_then(|obj| obj.get("name"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

pub fn dataset_schema(spec: &Value) -> Value {
    as_object(spec)
        .and_then(|obj| obj.get("table_spec"))
        .cloned()
        .unwrap_or(Value::Null)
}

pub fn dataset_policy(spec: &Value) -> Value {
    as_object(spec)
        .and_then(|obj| obj.get("policies"))
        .cloned()
        .unwrap_or(Value::Null)
}

pub fn dataset_contract(spec: &Value) -> Value {
    as_object(spec)
        .and_then(|obj| obj.get("contract_spec"))
        .cloned()
        .unwrap_or(Value::Null)
}

pub fn apply_scan_policy(scan_policy: &Value, defaults: &Value) -> Value {
    let mut merged = defaults.clone();
    merge_json(&mut merged, scan_policy.clone());
    merged
}

pub fn apply_delta_scan_policy(delta_scan_policy: &Value, defaults: &Value) -> Value {
    let mut merged = defaults.clone();
    merge_json(&mut merged, delta_scan_policy.clone());
    merged
}
