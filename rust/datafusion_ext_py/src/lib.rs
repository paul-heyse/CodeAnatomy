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
    module.add("__all__", collect_public_names_from_dict(py, &dict)?)?;
    Ok(())
}

fn collect_public_names(names: &[String]) -> Vec<String> {
    let mut out = names
        .iter()
        .filter(|name| !name.starts_with('_'))
        .cloned()
        .collect::<Vec<_>>();
    out.sort_unstable();
    out.dedup();
    out
}

fn collect_public_names_from_dict(py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<Vec<String>> {
    let mut names: Vec<String> = Vec::new();
    for (key, _value) in dict.iter() {
        let name: String = key.extract()?;
        names.push(name);
    }
    let _ = py;
    Ok(collect_public_names(&names))
}

#[cfg(test)]
mod tests {
    use super::collect_public_names;

    #[test]
    fn collect_public_names_excludes_dunder() {
        let names = vec![
            "__module__".to_string(),
            "my_function".to_string(),
            "_private".to_string(),
            "another_func".to_string(),
        ];
        let public = collect_public_names(&names);
        assert!(!public.contains(&"__module__".to_string()));
        assert!(public.contains(&"my_function".to_string()));
        assert!(public.contains(&"another_func".to_string()));
    }

    #[test]
    fn collect_public_names_is_sorted() {
        let names = vec!["z_func".to_string(), "a_func".to_string(), "m_func".to_string()];
        let public = collect_public_names(&names);
        let mut sorted = public.clone();
        sorted.sort();
        assert_eq!(public, sorted);
    }
}
