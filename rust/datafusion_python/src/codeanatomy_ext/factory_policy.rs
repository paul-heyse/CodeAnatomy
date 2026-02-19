//! Function-factory policy derivation bridge surface.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

use super::helpers::json_to_py;

fn json_string_list(value: Option<&JsonValue>) -> Vec<String> {
    let Some(JsonValue::Array(values)) = value else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(JsonValue::as_str)
        .map(ToString::to_string)
        .collect()
}

fn json_object(value: Option<&JsonValue>) -> Option<&JsonMap<String, JsonValue>> {
    match value {
        Some(JsonValue::Object(entries)) => Some(entries),
        _ => None,
    }
}

fn dtype_for_param(
    signature_inputs: Option<&JsonMap<String, JsonValue>>,
    name: &str,
    index: usize,
) -> String {
    let Some(entries) = signature_inputs else {
        return "Utf8".to_string();
    };
    let Some(JsonValue::Array(signatures)) = entries.get(name) else {
        return "Utf8".to_string();
    };
    let Some(JsonValue::Array(first_signature)) = signatures.first() else {
        return "Utf8".to_string();
    };
    first_signature
        .get(index)
        .and_then(JsonValue::as_str)
        .map_or_else(|| "Utf8".to_string(), ToString::to_string)
}

#[pyfunction]
pub(crate) fn derive_function_factory_policy(
    py: Python<'_>,
    snapshot: &Bound<'_, PyAny>,
    allow_async: bool,
) -> PyResult<Py<PyAny>> {
    let json_module = py.import("json")?;
    let dumped = json_module.call_method1("dumps", (snapshot,))?;
    let snapshot_json: String = dumped.extract()?;
    let parsed: JsonValue = serde_json::from_str(&snapshot_json).map_err(|err| {
        PyValueError::new_err(format!(
            "Invalid snapshot payload for FunctionFactory policy: {err}"
        ))
    })?;
    let snapshot_obj = parsed.as_object().ok_or_else(|| {
        PyValueError::new_err("FunctionFactory policy derivation requires a mapping snapshot.")
    })?;

    let scalar_names = json_string_list(snapshot_obj.get("scalar"));
    let parameter_names = json_object(snapshot_obj.get("parameter_names"));
    let signature_inputs = json_object(snapshot_obj.get("signature_inputs"));
    let return_types = json_object(snapshot_obj.get("return_types"));
    let volatility = json_object(snapshot_obj.get("volatility"));

    let primitives = scalar_names
        .iter()
        .map(|name| {
            let params = json_string_list(parameter_names.and_then(|entries| entries.get(name)));
            let params_payload = params
                .iter()
                .enumerate()
                .map(|(index, param_name)| {
                    json!({
                        "name": param_name,
                        "dtype": dtype_for_param(signature_inputs, name, index),
                    })
                })
                .collect::<Vec<_>>();
            let return_type = return_types
                .and_then(|entries| entries.get(name))
                .and_then(|value| value.as_array())
                .and_then(|rows| rows.first())
                .and_then(JsonValue::as_str)
                .map_or_else(|| "Utf8".to_string(), ToString::to_string);
            let volatility_value = volatility
                .and_then(|entries| entries.get(name))
                .and_then(JsonValue::as_str)
                .map_or_else(|| "stable".to_string(), ToString::to_string);
            json!({
                "name": name,
                "params": params_payload,
                "return_type": return_type,
                "volatility": volatility_value,
                "description": JsonValue::Null,
                "supports_named_args": !params.is_empty(),
            })
        })
        .collect::<Vec<_>>();

    let domain_operator_hooks = json_string_list(snapshot_obj.get("domain_operator_hooks"));
    let payload = json!({
        "primitives": primitives,
        "prefer_named_arguments": parameter_names.map_or(false, |entries| !entries.is_empty()),
        "allow_async": allow_async,
        "domain_operator_hooks": domain_operator_hooks,
    });
    json_to_py(py, &payload)
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(derive_function_factory_policy, module)?)?;
    Ok(())
}
