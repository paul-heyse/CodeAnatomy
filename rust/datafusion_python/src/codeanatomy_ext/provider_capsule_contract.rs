//! Session-aware provider capsule contract helpers.

use std::ffi::CString;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::execution::TaskContextProvider;
use datafusion::execution::context::SessionContext;
use datafusion_ffi::table_provider::FFI_TableProvider;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

fn datafusion_table_provider_capsule_name() -> PyResult<CString> {
    CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))
}

fn provider_capsule_with_session(
    py: Python<'_>,
    session: &SessionContext,
    provider: Arc<dyn TableProvider>,
) -> PyResult<Py<PyAny>> {
    let task_ctx_provider: Arc<dyn TaskContextProvider> = Arc::new(session.state());
    let ffi_provider = FFI_TableProvider::new(
        provider,
        true,
        None,
        &task_ctx_provider,
        None,
    );
    let capsule = PyCapsule::new(py, ffi_provider, Some(datafusion_table_provider_capsule_name()?))?;
    Ok(capsule.unbind().into())
}

/// Build a provider capsule tied to the caller-provided session context.
pub(crate) fn provider_capsule_from_session(
    py: Python<'_>,
    session: &SessionContext,
    provider: Arc<dyn TableProvider>,
) -> PyResult<Py<PyAny>> {
    provider_capsule_with_session(py, session, provider)
}
