//! Environment profiles for session configuration.
//!
//! Defines tuning parameters for Small/Medium/Large execution contexts.

use serde::{Deserialize, Serialize};

/// Environment classification based on codebase size and complexity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentClass {
    /// Small codebase: < 50 files, < 10K LOC
    Small,
    /// Medium codebase: 50-500 files, 10K-100K LOC
    Medium,
    /// Large codebase: > 500 files, > 100K LOC
    Large,
}

/// Environment-specific configuration parameters.
///
/// Controls DataFusion session tuning based on expected workload size.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentProfile {
    /// Environment classification
    pub class: EnvironmentClass,
    /// Number of target partitions for parallel execution
    pub target_partitions: u32,
    /// Batch size for Arrow record batches
    pub batch_size: u32,
    /// Memory pool size in bytes for execution
    pub memory_pool_bytes: u64,
}

impl EnvironmentProfile {
    /// Creates a profile from an environment class with default tuning parameters.
    ///
    /// # Arguments
    ///
    /// * `class` - Environment classification
    ///
    /// # Returns
    ///
    /// Profile with class-appropriate tuning parameters
    pub fn from_class(class: EnvironmentClass) -> Self {
        match class {
            EnvironmentClass::Small => Self {
                class,
                target_partitions: 4,
                batch_size: 4096,
                memory_pool_bytes: 512 * 1024 * 1024, // 512 MB
            },
            EnvironmentClass::Medium => Self {
                class,
                target_partitions: 8,
                batch_size: 8192,
                memory_pool_bytes: 2 * 1024 * 1024 * 1024, // 2 GB
            },
            EnvironmentClass::Large => Self {
                class,
                target_partitions: 16,
                batch_size: 16384,
                memory_pool_bytes: 8 * 1024 * 1024 * 1024, // 8 GB
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_profile() {
        let profile = EnvironmentProfile::from_class(EnvironmentClass::Small);
        assert_eq!(profile.class, EnvironmentClass::Small);
        assert_eq!(profile.target_partitions, 4);
        assert_eq!(profile.batch_size, 4096);
        assert_eq!(profile.memory_pool_bytes, 512 * 1024 * 1024);
    }

    #[test]
    fn test_medium_profile() {
        let profile = EnvironmentProfile::from_class(EnvironmentClass::Medium);
        assert_eq!(profile.class, EnvironmentClass::Medium);
        assert_eq!(profile.target_partitions, 8);
        assert_eq!(profile.batch_size, 8192);
        assert_eq!(profile.memory_pool_bytes, 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_large_profile() {
        let profile = EnvironmentProfile::from_class(EnvironmentClass::Large);
        assert_eq!(profile.class, EnvironmentClass::Large);
        assert_eq!(profile.target_partitions, 16);
        assert_eq!(profile.batch_size, 16384);
        assert_eq!(profile.memory_pool_bytes, 8 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_profile_serialization() {
        let profile = EnvironmentProfile::from_class(EnvironmentClass::Medium);
        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: EnvironmentProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.class, profile.class);
        assert_eq!(deserialized.target_partitions, profile.target_partitions);
    }
}
