//! WS-P2: Rust-native Substrait round-trip support.
//!
//! Provides Substrait serialization for logical plans as a **portability artifact**.
//! This is explicitly not a deterministic replay contract. Determinism is governed by:
//! 1. spec hash
//! 2. session envelope/runtime profile hash
//! 3. rulepack fingerprint
//! 4. provider identity fingerprints
//!
//! All functions are feature-gated behind `#[cfg(feature = "substrait")]`.
//! When the feature is disabled, stub implementations return
//! `DataFusionError::NotImplemented`.

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result};

// ---------------------------------------------------------------------------
// Feature-gated: real implementations
// ---------------------------------------------------------------------------

/// Encode an optimized logical plan to Substrait bytes.
///
/// Substrait encoding is logical-only: the encoded plan captures the
/// computation graph but not physical execution decisions. This makes it
/// suitable for:
/// - Portability to external tooling
/// - Logical-plan comparison support
/// - Optional compliance artifacts
///
/// Note: Execution always remains SessionContext-bound. Substrait plans
/// are never executed directly.
#[cfg(feature = "substrait")]
pub fn encode_substrait(ctx: &SessionContext, plan: &LogicalPlan) -> Result<Vec<u8>> {
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    let state = ctx.state();
    let substrait_plan = to_substrait_plan(plan, &state)?;
    let mut bytes = Vec::new();
    substrait_plan
        .encode(&mut bytes)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(bytes)
}

/// Encode an optimized logical plan to Substrait bytes.
///
/// Stub implementation when the `substrait` feature is not enabled.
/// Returns `DataFusionError::NotImplemented`.
#[cfg(not(feature = "substrait"))]
pub fn encode_substrait(_ctx: &SessionContext, _plan: &LogicalPlan) -> Result<Vec<u8>> {
    Err(DataFusionError::NotImplemented(
        "substrait feature not enabled".into(),
    ))
}

/// Decode Substrait bytes back to a LogicalPlan.
///
/// The decoded plan is bound to the provided SessionContext, which must
/// have compatible catalog/schema registrations.
#[cfg(feature = "substrait")]
pub async fn decode_substrait(ctx: &SessionContext, bytes: &[u8]) -> Result<LogicalPlan> {
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use prost::Message;

    let substrait_plan = substrait::proto::Plan::decode(bytes)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let state = ctx.state();
    let plan = from_substrait_plan(&state, &substrait_plan).await?;
    Ok(plan)
}

/// Decode Substrait bytes back to a LogicalPlan.
///
/// Stub implementation when the `substrait` feature is not enabled.
/// Returns `DataFusionError::NotImplemented`.
#[cfg(not(feature = "substrait"))]
pub async fn decode_substrait(_ctx: &SessionContext, _bytes: &[u8]) -> Result<LogicalPlan> {
    Err(DataFusionError::NotImplemented(
        "substrait feature not enabled".into(),
    ))
}

/// Compute a portability hash from Substrait bytes.
///
/// Hash equality is a useful signal, but not a standalone determinism
/// contract. The canonical determinism identity is:
/// (spec_hash, envelope_hash, rulepack_fingerprint, provider_identities).
pub fn substrait_plan_hash(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

// ---------------------------------------------------------------------------
// Convenience wrapper for plan_bundle integration
// ---------------------------------------------------------------------------

/// Try to encode a plan to Substrait, returning Ok(Some(bytes)) on success
/// or Ok(None) when the feature is disabled. Propagates real encoding errors.
///
/// This is the integration point used by `build_plan_bundle_artifact`.
pub fn try_substrait_encode(ctx: &SessionContext, plan: &LogicalPlan) -> Result<Vec<u8>> {
    encode_substrait(ctx, plan)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substrait_plan_hash_deterministic() {
        let data = b"test substrait bytes";
        let h1 = substrait_plan_hash(data);
        let h2 = substrait_plan_hash(data);
        assert_eq!(h1, h2);
        assert_ne!(h1, [0u8; 32]);
    }

    #[test]
    fn test_substrait_plan_hash_differs_for_different_input() {
        let h1 = substrait_plan_hash(b"plan_a");
        let h2 = substrait_plan_hash(b"plan_b");
        assert_ne!(h1, h2);
    }

    /// When the substrait feature is disabled, encode should return NotImplemented.
    #[cfg(not(feature = "substrait"))]
    #[test]
    fn test_encode_substrait_not_implemented() {
        let ctx = SessionContext::new();
        // We need a LogicalPlan, but since encode should fail immediately
        // when the feature is disabled, we can use any plan.
        // Create a minimal plan via SQL.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            ctx.sql("SELECT 1").await.unwrap();
            let df = ctx.sql("SELECT 1").await.unwrap();
            let plan = df.logical_plan().clone();
            encode_substrait(&ctx, &plan)
        });
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not enabled") || err_msg.contains("NotImplemented"),
            "Expected NotImplemented error, got: {err_msg}"
        );
    }

    /// When the substrait feature is disabled, decode should return NotImplemented.
    #[cfg(not(feature = "substrait"))]
    #[tokio::test]
    async fn test_decode_substrait_not_implemented() {
        let ctx = SessionContext::new();
        let result = decode_substrait(&ctx, b"fake bytes").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not enabled") || err_msg.contains("NotImplemented"),
            "Expected NotImplemented error, got: {err_msg}"
        );
    }
}
