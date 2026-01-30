use pyo3::prelude::*;

pub use datafusion_python::errors::{to_datafusion_err, ExtError, ExtResult};

#[pymodule]
fn datafusion_ext(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    datafusion_python::codeanatomy_ext::init_module(py, module)
}
