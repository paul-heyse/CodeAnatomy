use pyo3::prelude::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::spec::execution_spec::SemanticExecutionSpec;

/// Semantic plan compiler that validates and prepares execution specs.
///
/// This is a thin Python wrapper that validates specs and prepares them
/// for execution. The actual DataFusion compilation happens in the
/// CpgMaterializer during execution.
#[pyclass]
pub struct SemanticPlanCompiler {
    #[allow(dead_code)]
    runtime: Arc<Runtime>,
}

#[pymethods]
impl SemanticPlanCompiler {
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {e}")))?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    /// Compile a spec JSON into a validated, executable plan.
    ///
    /// This performs structural validation and hash computation but does not
    /// create a SessionContext (that happens during execution).
    ///
    /// Args:
    ///     spec_json: JSON-serialized SemanticExecutionSpec
    ///
    /// Returns:
    ///     CompiledPlan with validated spec and computed hash
    ///
    /// Raises:
    ///     ValueError: If JSON is malformed or spec is invalid
    fn compile(&self, spec_json: &str) -> PyResult<CompiledPlan> {
        // Parse and validate spec structure with field-path diagnostics.
        let mut deserializer = serde_json::Deserializer::from_str(spec_json);
        let mut spec: SemanticExecutionSpec = serde_path_to_error::deserialize(&mut deserializer)
            .map_err(|err| {
                let path = err.path().to_string();
                let inner = err.into_inner();
                if path.is_empty() {
                    PyValueError::new_err(format!("Invalid spec JSON: {inner}"))
                } else {
                    PyValueError::new_err(format!("Invalid spec JSON at '{path}': {inner}"))
                }
            })?;

        // Compute canonical hash
        spec.spec_hash = crate::spec::hashing::hash_spec(&spec);

        // Validate basic structure
        if spec.view_definitions.is_empty() {
            return Err(PyValueError::new_err("Spec must have at least one view definition"));
        }
        if spec.output_targets.is_empty() {
            return Err(PyValueError::new_err("Spec must have at least one output target"));
        }

        Ok(CompiledPlan {
            spec_json: spec_json.to_string(),
            spec_hash: spec.spec_hash,
        })
    }
}

/// Compiled execution plan ready for materialization.
///
/// Contains validated spec JSON and computed hash. The actual DataFusion
/// compilation happens during execution in CpgMaterializer.
#[pyclass]
pub struct CompiledPlan {
    spec_json: String,
    spec_hash: [u8; 32],
}

#[pymethods]
impl CompiledPlan {
    /// Get the spec JSON.
    fn spec_json(&self) -> &str {
        &self.spec_json
    }

    /// Get the spec hash as hex string.
    fn spec_hash_hex(&self) -> String {
        hex::encode(self.spec_hash)
    }

    /// Get the spec hash as bytes.
    fn spec_hash_bytes(&self) -> Vec<u8> {
        self.spec_hash.to_vec()
    }

    fn __repr__(&self) -> String {
        format!("CompiledPlan(spec_hash={})", hex::encode(self.spec_hash))
    }
}

impl CompiledPlan {
    /// Internal: parse the spec from stored JSON.
    pub(crate) fn parse_spec(&self) -> Result<SemanticExecutionSpec, serde_json::Error> {
        let mut spec: SemanticExecutionSpec = serde_json::from_str(&self.spec_json)?;
        spec.spec_hash = self.spec_hash;
        Ok(spec)
    }

    /// Internal: get the spec hash.
    pub(crate) fn spec_hash(&self) -> [u8; 32] {
        self.spec_hash
    }
}
