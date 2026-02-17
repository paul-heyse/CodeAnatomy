use pyo3::prelude::*;

#[test]
fn py_value_err_maps_to_python_value_error() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let err = datafusion_python::errors::py_value_err("bad value");
        let value_error = py
            .import("builtins")
            .expect("import builtins")
            .getattr("ValueError")
            .expect("get ValueError");
        assert!(err.is_instance(py, &value_error).expect("instance check"));
        assert!(err.to_string().contains("bad value"));
    });
}
