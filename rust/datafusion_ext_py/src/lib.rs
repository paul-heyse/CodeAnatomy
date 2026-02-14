use pyo3::prelude::*;
use pyo3::types::PyDict;

pub use datafusion_python::errors::{to_datafusion_err, ExtError, ExtResult};

#[pymodule]
fn datafusion_ext(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Mirror the canonical datafusion._internal surface so SessionContext
    // objects from the datafusion wheel remain ABI-compatible when invoked
    // through the datafusion_ext namespace.
    let internal = py.import("datafusion._internal")?;
    let dict = internal.dict();
    for (key, value) in dict.iter() {
        let name: String = key.extract()?;
        if name.starts_with('_') {
            continue;
        }
        module.add(name.as_str(), value)?;
    }
    module.add("IS_STUB", false)?;
    module.add("__all__", collect_public_names(py, &dict)?)?;
    Ok(())
}

fn collect_public_names(py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<Vec<String>> {
    let mut names: Vec<String> = Vec::new();
    for (key, _value) in dict.iter() {
        let name: String = key.extract()?;
        if name.starts_with('_') {
            continue;
        }
        names.push(name);
    }
    names.sort_unstable();
    names.dedup();
    let _ = py;
    Ok(names)
}
