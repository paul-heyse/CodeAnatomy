//! Shared helpers for the CodeAnatomy Python extension bridge.

use datafusion::execution::context::SessionContext;
use pyo3::prelude::*;

use crate::context::PySessionContext;

/// Extract the underlying DataFusion `SessionContext` from a Python object.
pub(crate) fn extract_session_ctx(ctx: &Bound<'_, PyAny>) -> PyResult<SessionContext> {
    if let Ok(session) = ctx.extract::<Bound<'_, PySessionContext>>() {
        return Ok(session.borrow().ctx().clone());
    }
    let inner = ctx.getattr("ctx")?;
    let session: Bound<'_, PySessionContext> = inner.extract()?;
    Ok(session.borrow().ctx().clone())
}
