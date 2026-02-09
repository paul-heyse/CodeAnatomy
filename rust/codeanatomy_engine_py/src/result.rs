use pyo3::prelude::*;

#[pyclass(name = "RunResult")]
#[derive(Clone)]
pub struct PyRunResult {
    inner_json: String,
}

#[pymethods]
impl PyRunResult {
    /// Serialize result to JSON string.
    fn to_json(&self) -> &str {
        &self.inner_json
    }

    /// Convert to Python dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyAny>> {
        let json_module = py.import("json")?;
        let result = json_module.call_method1("loads", (&self.inner_json,))?;
        Ok(result.into())
    }

    fn __repr__(&self) -> String {
        format!("RunResult({})", &self.inner_json[..self.inner_json.len().min(100)])
    }
}

impl PyRunResult {
    pub fn from_run_result(result: &codeanatomy_engine::executor::result::RunResult) -> Self {
        Self {
            inner_json: result.to_json().unwrap_or_else(|_| "{}".to_string()),
        }
    }
}
