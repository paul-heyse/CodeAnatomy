//! Rust-pivot bridge entrypoints for scheduling, interval alignment, and tree-sitter extraction.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use codeanatomy_engine::compiler::scheduling::{
    derive_cache_policies as derive_cache_policies_native, CachePolicyRequest,
};
use codeanatomy_engine::providers::{execute_interval_align, IntervalAlignProviderConfig};
use codeanatomy_engine::python::tree_sitter_extractor::extract_tree_sitter_batch as extract_ts_native;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList};
use serde::Deserialize;

use super::helpers::{parse_python_payload, runtime};

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct CachePolicyGraphPayload {
    out_degree: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct CachePolicyPayload {
    graph: CachePolicyGraphPayload,
    outputs: Vec<String>,
    cache_overrides: BTreeMap<String, String>,
    workload_class: Option<String>,
}

fn batches_from_pyarrow(value: &Bound<'_, PyAny>) -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
    if let Ok(stream_reader) = ArrowArrayStreamReader::from_pyarrow_bound(value) {
        let schema = stream_reader.schema();
        let batches = stream_reader
            .collect::<Result<Vec<RecordBatch>, arrow::error::ArrowError>>()
            .map_err(|err| PyValueError::new_err(format!("Invalid Arrow stream input: {err}")))?;
        return Ok((schema, batches));
    }
    if let Ok(batch) = RecordBatch::from_pyarrow_bound(value) {
        return Ok((batch.schema(), vec![batch]));
    }
    Err(PyValueError::new_err(
        "Expected Arrow table/record-batch/stream compatible object.",
    ))
}

fn pyarrow_table_from_batches(
    py: Python<'_>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> PyResult<Py<PyAny>> {
    let mut py_batches: Vec<Py<PyAny>> = Vec::with_capacity(batches.len());
    for batch in batches {
        py_batches.push(batch.to_pyarrow(py)?.unbind());
    }
    let batch_list = PyList::new(py, py_batches)?;
    let schema_py = schema.as_ref().to_pyarrow(py)?;
    let table = py
        .import("pyarrow")?
        .getattr("Table")?
        .call_method1("from_batches", (batch_list, schema_py))?;
    Ok(table.unbind())
}

#[pyfunction]
pub(crate) fn derive_cache_policies(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let parsed: CachePolicyPayload = parse_python_payload(py, payload, "cache policy request")?;
    let request = CachePolicyRequest {
        out_degree: parsed.graph.out_degree,
        outputs: parsed.outputs.into_iter().collect::<BTreeSet<_>>(),
        cache_overrides: parsed.cache_overrides,
        workload_class: parsed.workload_class,
    };
    let decisions = derive_cache_policies_native(&request);
    let out = PyDict::new(py);
    for decision in decisions {
        out.set_item(decision.view_name, decision.policy)?;
    }
    Ok(out.into())
}

#[pyfunction]
#[pyo3(signature = (source, file_path, payload=None))]
pub(crate) fn extract_tree_sitter_batch(
    py: Python<'_>,
    source: String,
    file_path: String,
    payload: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let _ = payload;
    let batch = extract_ts_native(source.as_str(), file_path.as_str())
        .map_err(|err| PyRuntimeError::new_err(format!("tree-sitter extraction failed: {err}")))?;
    Ok(batch.to_pyarrow(py)?.unbind())
}

#[pyfunction]
#[pyo3(signature = (left, right, payload=None))]
pub(crate) fn interval_align_table(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    payload: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let config = if let Some(value) = payload {
        parse_python_payload::<IntervalAlignProviderConfig>(py, value, "interval align payload")?
    } else {
        IntervalAlignProviderConfig::default()
    };
    let (left_schema, left_batches) = batches_from_pyarrow(left)?;
    let (right_schema, right_batches) = batches_from_pyarrow(right)?;
    let runtime = runtime()?;
    let (schema, batches) = runtime.block_on(async {
        execute_interval_align(
            left_schema,
            left_batches,
            right_schema,
            right_batches,
            config,
        )
        .await
        .map_err(|err| PyRuntimeError::new_err(format!("interval align failed: {err}")))
    })?;
    pyarrow_table_from_batches(py, schema, batches)
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(derive_cache_policies, module)?)?;
    module.add_function(wrap_pyfunction!(extract_tree_sitter_batch, module)?)?;
    module.add_function(wrap_pyfunction!(interval_align_table, module)?)?;
    Ok(())
}
