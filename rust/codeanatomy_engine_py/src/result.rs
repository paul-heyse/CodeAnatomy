use pyo3::prelude::*;
use pyo3::types::PyList;
use serde_json::Value;

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

    /// Convert to typed Python payload.
    fn to_payload(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyAny>> {
        let value: Value = serde_json::from_str(&self.inner_json)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
        json_value_to_py(py, &value)
    }

    /// Convert to Python dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyAny>> {
        self.to_payload(py)
    }

    /// Return the optional task schedule payload as a Python dict.
    fn task_schedule(&self, py: Python<'_>) -> PyResult<Option<Py<pyo3::types::PyAny>>> {
        let value: Value = serde_json::from_str(&self.inner_json)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
        let Some(schedule) = value.get("task_schedule") else {
            return Ok(None);
        };
        if schedule.is_null() {
            return Ok(None);
        }
        let json_module = py.import("json")?;
        let dumped = serde_json::to_string(schedule)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
        let dict = json_module.call_method1("loads", (dumped,))?;
        Ok(Some(dict.unbind()))
    }

    /// Return the serialized plan bundles as Python dict objects.
    fn plan_bundles(&self, py: Python<'_>) -> PyResult<Vec<Py<pyo3::types::PyAny>>> {
        let value: Value = serde_json::from_str(&self.inner_json)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
        let Some(plan_bundles) = value.get("plan_bundles") else {
            return Ok(Vec::new());
        };
        let Some(items) = plan_bundles.as_array() else {
            return Ok(Vec::new());
        };
        let json_module = py.import("json")?;
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let dumped = serde_json::to_string(item)
                .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
            let as_dict = json_module.call_method1("loads", (dumped,))?;
            out.push(as_dict.unbind());
        }
        Ok(out)
    }

    /// Return the count of serialized plan bundles.
    fn plan_bundle_count(&self) -> PyResult<usize> {
        let value: Value = serde_json::from_str(&self.inner_json)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
        let count = value
            .get("plan_bundles")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        Ok(count)
    }

    /// Return the task schedule critical path if present.
    fn critical_path(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let value: Value = serde_json::from_str(&self.inner_json)
            .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
        let Some(schedule) = value.get("task_schedule") else {
            return Ok(Vec::new());
        };
        let Some(path) = schedule.get("critical_path") else {
            return Ok(Vec::new());
        };
        let list = path
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| entry.as_str().map(ToString::to_string))
            .collect::<Vec<_>>();
        let _ = PyList::new(py, &list)?;
        Ok(list)
    }

    fn __repr__(&self) -> String {
        format!(
            "RunResult({})",
            &self.inner_json[..self.inner_json.len().min(100)]
        )
    }
}

pub(crate) fn json_value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<pyo3::types::PyAny>> {
    let json_module = py.import("json")?;
    let dumped = serde_json::to_string(value)
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))?;
    let parsed = json_module.call_method1("loads", (dumped,))?;
    Ok(parsed.unbind())
}

impl PyRunResult {
    pub fn from_run_result(result: &codeanatomy_engine::executor::result::RunResult) -> Self {
        Self {
            inner_json: result.to_json().unwrap_or_else(|_| "{}".to_string()),
        }
    }

    pub(crate) fn json_str(&self) -> &str {
        &self.inner_json
    }
}
