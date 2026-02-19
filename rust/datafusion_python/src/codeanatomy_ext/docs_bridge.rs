//! UDF documentation bridge surface.

use datafusion_expr::Documentation;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::udf_docs;

use super::helpers::extract_session_ctx;

#[pyfunction]
pub(crate) fn udf_docs_snapshot(py: Python<'_>, ctx: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let payload = PyDict::new(py);
    let add_doc = |name: &str, doc: &Documentation| -> PyResult<()> {
        let entry = PyDict::new(py);
        entry.set_item("description", doc.description.clone())?;
        entry.set_item("syntax", doc.syntax_example.clone())?;
        entry.set_item("section", doc.doc_section.label)?;
        if let Some(example) = &doc.sql_example {
            entry.set_item("sql_example", example.clone())?;
        } else {
            entry.set_item("sql_example", py.None())?;
        }
        if let Some(arguments) = &doc.arguments {
            entry.set_item("arguments", PyList::new(py, arguments.clone())?)?;
        } else {
            entry.set_item("arguments", PyList::empty(py))?;
        }
        if let Some(alternatives) = &doc.alternative_syntax {
            entry.set_item("alternative_syntax", PyList::new(py, alternatives.clone())?)?;
        } else {
            entry.set_item("alternative_syntax", PyList::empty(py))?;
        }
        if let Some(related) = &doc.related_udfs {
            entry.set_item("related_udfs", PyList::new(py, related.clone())?)?;
        } else {
            entry.set_item("related_udfs", PyList::empty(py))?;
        }
        payload.set_item(name, entry)?;
        Ok(())
    };

    let state = extract_session_ctx(ctx)?.state();
    let docs = udf_docs::registry_docs(&state);
    for (name, doc) in docs {
        add_doc(name.as_str(), doc)?;
    }
    Ok(payload.into())
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(udf_docs_snapshot, module)?)?;
    Ok(())
}
