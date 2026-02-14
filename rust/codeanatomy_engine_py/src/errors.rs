use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use serde_json::Value;

create_exception!(codeanatomy_engine, EngineExecutionError, PyException);

/// Build a typed engine execution error with stage/code/details attributes.
pub fn engine_execution_error(
    stage: &str,
    code: &str,
    message: impl Into<String>,
    details: Option<Value>,
) -> PyErr {
    let message = message.into();
    Python::attach(|py| {
        let error_type = py.get_type::<EngineExecutionError>();
        let instance = match error_type.call1((message.clone(),)) {
            Ok(obj) => obj,
            Err(_) => return PyException::new_err(message.clone()),
        };
        let _ = instance.setattr("stage", stage);
        let _ = instance.setattr("code", code);
        let _ = instance.setattr("message", message.clone());
        let _ = match details {
            Some(value) => {
                let details_obj = serde_json::to_string(&value).ok().and_then(|serialized| {
                    py.import("json")
                        .ok()
                        .and_then(|json| json.call_method1("loads", (serialized,)).ok())
                });
                match details_obj {
                    Some(obj) => instance.setattr("details", obj),
                    None => instance.setattr("details", py.None()),
                }
            }
            None => instance.setattr("details", py.None()),
        };
        PyErr::from_value(instance.into_any())
    })
}
