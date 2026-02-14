//! Optional Delta codec seams for plan serialization.
//!
//! Provides two feature-gated capabilities:
//!
//! 1. **`delta-planner`** (marker only): Enables the Delta query planner
//!    path in session factory construction. No additional code here;
//!    the planner is wired in `session/factory.rs`.
//!
//! 2. **`delta-codec`**: Provides extension-node-aware serialization for
//!    Delta-specific logical and physical plan nodes. This uses
//!    `DeltaLogicalCodec` and `DeltaPhysicalCodec` from the `deltalake`
//!    crate with `datafusion-proto` serialization APIs.
//!
//! Both features are disabled by default. When disabled, stub functions
//! return `DataFusionError::NotImplemented`.
//!
//! # Operational Guidance
//!
//! - **Default (off):** No Delta-specific plan encoding. Standard DataFusion
//!   plan serialization works but cannot round-trip Delta extension nodes.
//! - **Advanced (on):** Enable `delta-codec` for deployments that serialize
//!   plans containing DeltaScan or other Delta-specific plan nodes.

use std::sync::Arc;

use datafusion_common::Result;

// ---------------------------------------------------------------------------
// Feature-gated: real implementations
// ---------------------------------------------------------------------------

/// Install Delta logical/physical plan codecs as session config extensions.
///
/// This registers `DeltaLogicalCodec` and `DeltaPhysicalCodec` on the
/// session's `ConfigOptions` extensions, enabling DataFusion's plan
/// serialization infrastructure to handle Delta-specific plan nodes.
///
/// Requires the `delta-codec` feature.
#[cfg(feature = "delta-codec")]
pub fn install_delta_codecs(ctx: &datafusion::execution::context::SessionContext) {
    use deltalake::delta_datafusion::{DeltaLogicalCodec, DeltaPhysicalCodec};

    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    config.set_extension(Arc::new(DeltaLogicalCodec {}));
    config.set_extension(Arc::new(DeltaPhysicalCodec {}));
}

/// Install Delta plan codecs as session config extensions.
///
/// Stub implementation when the `delta-codec` feature is not enabled.
/// This is a no-op.
#[cfg(not(feature = "delta-codec"))]
pub fn install_delta_codecs(_ctx: &datafusion::execution::context::SessionContext) {
    // No-op: Delta codec types not available without feature gate.
}

/// Encode a logical plan and physical plan to bytes using Delta extension codecs.
///
/// Uses `DeltaLogicalCodec` and `DeltaPhysicalCodec` for extension-node-aware
/// serialization. This enables round-tripping plans that contain Delta-specific
/// nodes (e.g., `DeltaScan`).
///
/// Returns a tuple of (logical_bytes, physical_bytes).
///
/// Requires the `delta-codec` feature.
#[cfg(feature = "delta-codec")]
pub fn encode_with_delta_codecs(
    logical: &datafusion::logical_expr::LogicalPlan,
    physical: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Result<(Vec<u8>, Vec<u8>)> {
    use datafusion_proto::bytes::{
        logical_plan_to_bytes_with_extension_codec, physical_plan_to_bytes_with_extension_codec,
    };
    use deltalake::delta_datafusion::{DeltaLogicalCodec, DeltaPhysicalCodec};

    let logical_bytes = logical_plan_to_bytes_with_extension_codec(logical, &DeltaLogicalCodec {})?;
    let physical_bytes =
        physical_plan_to_bytes_with_extension_codec(physical, &DeltaPhysicalCodec {})?;
    Ok((logical_bytes.to_vec(), physical_bytes.to_vec()))
}

/// Encode a logical and physical plan to bytes using Delta extension codecs.
///
/// Stub implementation when the `delta-codec` feature is not enabled.
/// Returns `DataFusionError::NotImplemented`.
#[cfg(not(feature = "delta-codec"))]
pub fn encode_with_delta_codecs(
    _logical: &datafusion::logical_expr::LogicalPlan,
    _physical: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Result<(Vec<u8>, Vec<u8>)> {
    Err(datafusion_common::DataFusionError::NotImplemented(
        "delta-codec feature not enabled".into(),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    /// When the delta-codec feature is disabled, encode should return NotImplemented.
    #[cfg(not(feature = "delta-codec"))]
    #[test]
    fn test_encode_not_implemented_without_feature() {
        use super::encode_with_delta_codecs;
        use datafusion::prelude::*;

        let ctx = SessionContext::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let df = ctx.sql("SELECT 1 AS x").await.unwrap();
            let plan = df.logical_plan().clone();
            let physical = df.create_physical_plan().await.unwrap();
            encode_with_delta_codecs(&plan, physical)
        });
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not enabled") || err_msg.contains("NotImplemented"),
            "Expected NotImplemented error, got: {err_msg}"
        );
    }

    /// install_delta_codecs is a no-op without the feature gate.
    #[cfg(not(feature = "delta-codec"))]
    #[test]
    fn test_install_codecs_noop_without_feature() {
        use super::install_delta_codecs;

        let ctx = datafusion::execution::context::SessionContext::new();
        // Should not panic or error
        install_delta_codecs(&ctx);
    }
}
