use pyo3::prelude::*;
use serde_json::Value;

use crate::errors::engine_execution_error;

#[pyclass]
pub struct SchemaRuntime;

fn parse_json_payload(payload: &str, field: &str) -> PyResult<Value> {
    serde_json::from_str(payload).map_err(|err| {
        engine_execution_error(
            "validation",
            "INVALID_SCHEMA_RUNTIME_JSON",
            format!("Invalid JSON payload for {field}: {err}"),
            Some(serde_json::json!({"field": field, "error": err.to_string()})),
        )
    })
}

fn encode_json_payload(payload: &Value, field: &str) -> PyResult<String> {
    serde_json::to_string(payload).map_err(|err| {
        engine_execution_error(
            "runtime",
            "SCHEMA_RUNTIME_SERIALIZE_FAILED",
            format!("Failed to serialize {field}: {err}"),
            Some(serde_json::json!({"field": field, "error": err.to_string()})),
        )
    })
}

#[pymethods]
impl SchemaRuntime {
    #[new]
    fn new() -> Self {
        Self
    }

    fn dataset_name(&self, spec_json: &str) -> PyResult<String> {
        let spec = parse_json_payload(spec_json, "spec_json")?;
        codeanatomy_engine::schema::dataset_name(&spec).ok_or_else(|| {
            engine_execution_error(
                "validation",
                "SCHEMA_RUNTIME_DATASET_NAME_MISSING",
                "Dataset spec payload is missing 'name'",
                None,
            )
        })
    }

    fn dataset_schema_json(&self, spec_json: &str) -> PyResult<String> {
        let spec = parse_json_payload(spec_json, "spec_json")?;
        let payload = codeanatomy_engine::schema::dataset_schema(&spec);
        encode_json_payload(&payload, "dataset_schema_json")
    }

    fn dataset_policy_json(&self, spec_json: &str) -> PyResult<String> {
        let spec = parse_json_payload(spec_json, "spec_json")?;
        let payload = codeanatomy_engine::schema::dataset_policy(&spec);
        encode_json_payload(&payload, "dataset_policy_json")
    }

    fn dataset_contract_json(&self, spec_json: &str) -> PyResult<String> {
        let spec = parse_json_payload(spec_json, "spec_json")?;
        let payload = codeanatomy_engine::schema::dataset_contract(&spec);
        encode_json_payload(&payload, "dataset_contract_json")
    }

    fn apply_scan_policy_json(
        &self,
        scan_policy_json: &str,
        defaults_json: &str,
    ) -> PyResult<String> {
        let scan_policy = parse_json_payload(scan_policy_json, "scan_policy_json")?;
        let defaults = parse_json_payload(defaults_json, "defaults_json")?;
        let merged = codeanatomy_engine::schema::apply_scan_policy(&scan_policy, &defaults);
        encode_json_payload(&merged, "apply_scan_policy_json")
    }

    fn apply_delta_scan_policy_json(
        &self,
        delta_scan_json: &str,
        defaults_json: &str,
    ) -> PyResult<String> {
        let delta_scan = parse_json_payload(delta_scan_json, "delta_scan_json")?;
        let defaults = parse_json_payload(defaults_json, "defaults_json")?;
        let merged = codeanatomy_engine::schema::apply_delta_scan_policy(&delta_scan, &defaults);
        encode_json_payload(&merged, "apply_delta_scan_policy_json")
    }

    fn __repr__(&self) -> String {
        "SchemaRuntime()".to_string()
    }
}
