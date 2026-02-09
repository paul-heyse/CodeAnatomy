use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;

use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};

/// Factory for creating DataFusion sessions with environment-specific configuration.
///
/// Supports both JSON-based custom profiles and predefined environment classes.
/// The factory stores configuration but creates sessions on-demand during execution.
#[pyclass]
pub struct SessionFactory {
    inner: codeanatomy_engine::session::factory::SessionFactory,
}

#[pymethods]
impl SessionFactory {
    /// Create a new SessionFactory from a JSON-serialized EnvironmentProfile.
    ///
    /// Args:
    ///     profile_json: JSON string containing profile configuration
    ///         (target_partitions, batch_size, memory_pool_bytes, etc.)
    ///
    /// Returns:
    ///     New SessionFactory instance
    ///
    /// Raises:
    ///     ValueError: If JSON is malformed or missing required fields
    ///
    /// Example:
    ///     >>> profile_json = '{"target_partitions": 8, "batch_size": 8192, ...}'
    ///     >>> factory = SessionFactory(profile_json)
    #[new]
    fn new(profile_json: &str) -> PyResult<Self> {
        let profile: EnvironmentProfile = serde_json::from_str(profile_json)
            .map_err(|e| PyValueError::new_err(format!("Invalid profile JSON: {e}")))?;
        Ok(Self {
            inner: codeanatomy_engine::session::factory::SessionFactory::new(profile),
        })
    }

    /// Create a SessionFactory from a predefined environment class.
    ///
    /// Args:
    ///     environment_class: One of "small", "medium", or "large"
    ///         - small: 4 partitions, 4096 batch size, 512MB memory
    ///         - medium: 8 partitions, 8192 batch size, 2GB memory
    ///         - large: 16 partitions, 16384 batch size, 8GB memory
    ///
    /// Returns:
    ///     New SessionFactory instance
    ///
    /// Raises:
    ///     ValueError: If environment_class is not recognized
    ///
    /// Example:
    ///     >>> factory = SessionFactory.from_class("medium")
    #[staticmethod]
    fn from_class(environment_class: &str) -> PyResult<Self> {
        let class = match environment_class.to_lowercase().as_str() {
            "small" => EnvironmentClass::Small,
            "medium" => EnvironmentClass::Medium,
            "large" => EnvironmentClass::Large,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unknown environment class '{other}' (expected small, medium, or large)"
                )))
            }
        };
        let profile = EnvironmentProfile::from_class(class);
        Ok(Self {
            inner: codeanatomy_engine::session::factory::SessionFactory::new(profile),
        })
    }

    /// Get the profile configuration as JSON string.
    fn profile_json(&self) -> PyResult<String> {
        serde_json::to_string(self.inner.profile())
            .map_err(|e| PyValueError::new_err(format!("Failed to encode profile: {e}")))
    }

    fn __repr__(&self) -> String {
        "SessionFactory(...)".to_string()
    }
}

impl SessionFactory {
    /// Internal accessor for the Rust SessionFactory.
    pub(crate) fn inner(&self) -> &codeanatomy_engine::session::factory::SessionFactory {
        &self.inner
    }

    /// Internal accessor for the environment profile.
    pub(crate) fn get_profile(&self) -> EnvironmentProfile {
        self.inner.profile().clone()
    }
}
