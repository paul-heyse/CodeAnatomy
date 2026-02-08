//! WS9: PyO3 Bindings — thin facade over Rust engine.
//!
//! Python owns CLI/API ergonomics and spec submission.
//! Rust owns everything else.

use pyo3::prelude::*;

pub mod compiler;
pub mod materializer;
pub mod result;
pub mod session;

/// Python module entry point for codeanatomy_engine.
///
/// Exposes:
/// - SessionFactory: session configuration and creation
/// - SemanticPlanCompiler: compile execution specs to plans
/// - CpgMaterializer: execute plans and materialize results
/// - RunResult: execution outcome with determinism contract
#[pymodule]
fn codeanatomy_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<session::SessionFactory>()?;
    m.add_class::<compiler::SemanticPlanCompiler>()?;
    m.add_class::<compiler::CompiledPlan>()?;
    m.add_class::<materializer::CpgMaterializer>()?;
    m.add_class::<result::PyRunResult>()?;
    Ok(())
}
