//! Result envelope types for execution outcomes.
//!
//! Two-layer determinism contract:
//! - spec_hash: execution spec identity
//! - envelope_hash: input data identity

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Compact result envelope containing both determinism layers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub outputs: Vec<MaterializationResult>,
    pub spec_hash: [u8; 32],
    pub envelope_hash: [u8; 32],
    pub rulepack_fingerprint: [u8; 32],
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

/// Per-table materialization outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationResult {
    pub table_name: String,
    pub rows_written: u64,
    pub partition_count: u32,
}

/// Two-layer determinism contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismContract {
    pub spec_hash: [u8; 32],
    pub envelope_hash: [u8; 32],
}

impl DeterminismContract {
    /// A replay is valid iff both spec_hash AND envelope_hash match.
    pub fn is_replay_valid(&self, original: &DeterminismContract) -> bool {
        self.spec_hash == original.spec_hash && self.envelope_hash == original.envelope_hash
    }
}

impl RunResult {
    /// Create a new RunResult builder.
    pub fn builder() -> RunResultBuilder {
        RunResultBuilder::default()
    }

    /// Get the determinism contract for this run.
    pub fn determinism_contract(&self) -> DeterminismContract {
        DeterminismContract {
            spec_hash: self.spec_hash,
            envelope_hash: self.envelope_hash,
        }
    }

    /// Serialize to JSON for Python consumption.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[derive(Debug, Default)]
pub struct RunResultBuilder {
    outputs: Vec<MaterializationResult>,
    spec_hash: [u8; 32],
    envelope_hash: [u8; 32],
    rulepack_fingerprint: [u8; 32],
    started_at: Option<DateTime<Utc>>,
}

impl RunResultBuilder {
    pub fn with_spec_hash(mut self, hash: [u8; 32]) -> Self {
        self.spec_hash = hash;
        self
    }

    pub fn with_envelope_hash(mut self, hash: [u8; 32]) -> Self {
        self.envelope_hash = hash;
        self
    }

    pub fn with_rulepack_fingerprint(mut self, fp: [u8; 32]) -> Self {
        self.rulepack_fingerprint = fp;
        self
    }

    pub fn started_now(mut self) -> Self {
        self.started_at = Some(Utc::now());
        self
    }

    pub fn add_output(mut self, output: MaterializationResult) -> Self {
        self.outputs.push(output);
        self
    }

    pub fn build(self) -> RunResult {
        RunResult {
            outputs: self.outputs,
            spec_hash: self.spec_hash,
            envelope_hash: self.envelope_hash,
            rulepack_fingerprint: self.rulepack_fingerprint,
            started_at: self.started_at.unwrap_or_else(Utc::now),
            completed_at: Utc::now(),
        }
    }
}
