//! WS9: PyO3 Bindings — thin facade over Rust engine.
//!
//! Python owns CLI/API ergonomics and spec submission.
//! Rust owns everything else.

use pyo3::prelude::*;
use serde_json::{json, Value};
use std::path::Path;

pub mod compiler;
pub mod errors;
pub mod materializer;
pub mod result;
pub mod schema;
pub mod session;

fn infer_output_root(outputs: &Value) -> Option<String> {
    let entries = outputs.as_array()?;
    for entry in entries {
        let Some(location) = entry.get("delta_location").and_then(Value::as_str) else {
            continue;
        };
        let Some(parent) = Path::new(location).parent() else {
            continue;
        };
        return Some(parent.to_string_lossy().to_string());
    }
    None
}

#[pyfunction]
fn run_build(py: Python<'_>, request_json: &str) -> PyResult<Py<pyo3::types::PyAny>> {
    let request: Value = serde_json::from_str(request_json).map_err(|err| {
        errors::engine_execution_error(
            "validation",
            "INVALID_RUN_BUILD_REQUEST",
            format!("Invalid run_build request JSON: {err}"),
            Some(json!({"error": err.to_string()})),
        )
    })?;
    let spec_json = request
        .get("spec_json")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            errors::engine_execution_error(
                "validation",
                "RUN_BUILD_REQUEST_MISSING_SPEC_JSON",
                "run_build request must include non-empty 'spec_json'",
                None,
            )
        })?;
    let engine_profile = request
        .get("engine_profile")
        .and_then(Value::as_str)
        .unwrap_or("medium");
    let runtime_overrides = request.get("runtime").cloned();
    let orchestration = request
        .get("orchestration")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let effective_spec_json = if let Some(runtime_value) = runtime_overrides {
        let mut spec_value: Value = serde_json::from_str(spec_json).map_err(|err| {
            errors::engine_execution_error(
                "validation",
                "INVALID_SPEC_JSON",
                format!("Invalid spec_json for run_build: {err}"),
                Some(json!({"error": err.to_string()})),
            )
        })?;
        if let Some(spec_obj) = spec_value.as_object_mut() {
            spec_obj.insert("runtime".to_string(), runtime_value);
        }
        serde_json::to_string(&spec_value).map_err(|err| {
            errors::engine_execution_error(
                "validation",
                "INVALID_SPEC_JSON",
                format!("Failed to serialize effective spec_json for run_build: {err}"),
                Some(json!({"error": err.to_string()})),
            )
        })?
    } else {
        spec_json.to_string()
    };

    let session_factory = session::SessionFactory::from_class_name(engine_profile)?;
    let compiler = compiler::SemanticPlanCompiler::new_internal()?;
    let compiled_plan = compiler.compile_internal(&effective_spec_json)?;
    let materializer = materializer::CpgMaterializer::new_internal()?;
    let run_result = materializer.execute_internal(&session_factory, &compiled_plan)?;

    let run_result_value: Value = serde_json::from_str(run_result.json_str()).map_err(|err| {
        errors::engine_execution_error(
            "runtime",
            "RUN_BUILD_RESULT_PARSE_FAILED",
            format!("Failed to parse run_result payload: {err}"),
            Some(json!({"error": err.to_string()})),
        )
    })?;
    let outputs = run_result_value
        .get("outputs")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let warnings = run_result_value
        .get("warnings")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let output_root = infer_output_root(&outputs);
    let include_manifest = orchestration
        .get("include_manifest")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let include_run_bundle = orchestration
        .get("include_run_bundle")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let emit_auxiliary_outputs = orchestration
        .get("emit_auxiliary_outputs")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let manifest_path = if include_manifest {
        output_root
            .as_ref()
            .map(|root| format!("{root}/run_manifest"))
            .map(Value::String)
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let run_bundle_dir = if include_run_bundle {
        output_root
            .as_ref()
            .map(|root| format!("{root}/run_bundle"))
            .map(Value::String)
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let mut auxiliary_outputs = serde_json::Map::new();
    if emit_auxiliary_outputs {
        let normalize_path = output_root
            .as_ref()
            .map(|root| format!("{root}/normalize_outputs"));
        auxiliary_outputs.insert(
            "normalize_outputs_delta".to_string(),
            normalize_path.map_or(Value::Null, Value::String),
        );
        auxiliary_outputs.insert(
            "extract_error_artifacts_delta".to_string(),
            Value::Null,
        );
        let manifest_aux = output_root
            .as_ref()
            .filter(|_| include_manifest)
            .map(|root| format!("{root}/run_manifest"));
        auxiliary_outputs.insert(
            "run_manifest_delta".to_string(),
            manifest_aux.map_or(Value::Null, Value::String),
        );
    }
    let diagnostics = json!({
        "events": [],
        "artifacts": [],
    });
    let artifacts = json!({
        "manifest_path": manifest_path,
        "run_bundle_dir": run_bundle_dir,
        "auxiliary_outputs": Value::Object(auxiliary_outputs),
        "include_manifest": include_manifest,
        "include_run_bundle": include_run_bundle,
        "emit_auxiliary_outputs": emit_auxiliary_outputs,
    });
    let response = json!({
        "contract_version": 3,
        "run_result": run_result_value,
        "outputs": outputs,
        "warnings": warnings,
        "diagnostics": diagnostics,
        "artifacts": artifacts,
    });
    result::json_value_to_py(py, &response)
}

/// Python module entry point for codeanatomy_engine.
///
/// Exposes:
/// - SessionFactory: session configuration and creation
/// - SemanticPlanCompiler: compile execution specs to plans
/// - CpgMaterializer: execute plans and materialize results
/// - RunResult: execution outcome with determinism contract
#[pymodule]
fn codeanatomy_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "EngineExecutionError",
        m.py().get_type::<errors::EngineExecutionError>(),
    )?;
    m.add_class::<session::SessionFactory>()?;
    m.add_class::<compiler::SemanticPlanCompiler>()?;
    m.add_class::<compiler::CompiledPlan>()?;
    m.add_class::<materializer::CpgMaterializer>()?;
    m.add_class::<result::PyRunResult>()?;
    m.add_class::<schema::SchemaRuntime>()?;
    m.add_function(wrap_pyfunction!(run_build, m)?)?;
    Ok(())
}
