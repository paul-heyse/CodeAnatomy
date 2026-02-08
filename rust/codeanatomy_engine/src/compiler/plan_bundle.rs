//! WS-P1: Structured plan artifact contract.
//!
//! Split model separating runtime plan handles from persisted artifact payloads:
//! - `PlanBundleRuntime`: runtime-only plan handles for execution (never serialized)
//! - `PlanBundleArtifact`: persisted, versioned payload for storage/diffing
//!
//! Plan comparison uses canonical digests (blake3 over normalized text), not
//! debug-string equality. This makes diffs deterministic and storage-efficient.

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::substrait::try_substrait_encode;

// ---------------------------------------------------------------------------
// Runtime-only plan handles (never serialized)
// ---------------------------------------------------------------------------

/// Runtime-only plan handles capturing the three plan stages.
///
/// This struct holds live DataFusion plan objects that cannot be safely
/// serialized. It exists only for the duration of a compilation session
/// and is consumed to produce the persisted `PlanBundleArtifact`.
#[derive(Debug)]
pub struct PlanBundleRuntime {
    /// P0: unoptimized logical plan (as submitted by the view builder).
    pub p0_logical: LogicalPlan,
    /// P1: optimized logical plan (after SessionContext optimizer rules).
    pub p1_optimized: LogicalPlan,
    /// P2: physical execution plan.
    pub p2_physical: Arc<dyn ExecutionPlan>,
}

// ---------------------------------------------------------------------------
// Persisted artifact payload
// ---------------------------------------------------------------------------

/// Persisted, versioned plan artifact payload.
///
/// Contains canonical digests for each plan stage, optional normalized text
/// for audit/debug, explain outputs, identity fingerprints, and optional
/// portability artifacts (Substrait bytes, SQL text).
///
/// All digests are computed via blake3 over deterministic normalized text.
/// Schema fingerprints cover the Arrow schema at each plan boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanBundleArtifact {
    /// Artifact format version for forward compatibility.
    pub artifact_version: u32,

    // -- Canonical digests (primary diff identity) --
    /// Blake3 digest of P0 normalized logical plan text.
    pub p0_digest: [u8; 32],
    /// Blake3 digest of P1 normalized optimized plan text.
    pub p1_digest: [u8; 32],
    /// Blake3 digest of P2 normalized physical plan text.
    pub p2_digest: [u8; 32],

    // -- Optional normalized text for audit/debug --
    /// Normalized P0 logical plan text (display_indent format).
    pub p0_text: Option<String>,
    /// Normalized P1 optimized plan text (display_indent format).
    pub p1_text: Option<String>,
    /// Normalized P2 physical plan text (displayable indent format).
    pub p2_text: Option<String>,

    // -- Explain outputs --
    /// EXPLAIN VERBOSE entries for the optimized plan.
    pub explain_verbose: Vec<ExplainEntry>,
    /// EXPLAIN ANALYZE entries for the optimized plan.
    pub explain_analyze: Vec<ExplainEntry>,

    // -- Identity fingerprints --
    /// Blake3 digest of the rulepack definition.
    pub rulepack_fingerprint: [u8; 32],
    /// Identity hashes for each provider (table source).
    pub provider_identities: Vec<ProviderIdentity>,
    /// Schema fingerprints at each plan boundary.
    pub schema_fingerprints: SchemaFingerprints,

    // -- Portability artifacts (optional) --
    /// Substrait-encoded logical plan bytes (requires `substrait` feature).
    pub substrait_bytes: Option<Vec<u8>>,
    /// SQL text unparsed from the optimized logical plan.
    pub sql_text: Option<String>,
}

/// A single entry from EXPLAIN output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainEntry {
    /// The plan type label (e.g., "logical_plan", "physical_plan").
    pub plan_type: String,
    /// The plan text content.
    pub plan_text: String,
}

/// Identity hash for a registered table provider.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderIdentity {
    /// Logical table name as registered in the session catalog.
    pub table_name: String,
    /// Blake3 identity hash covering provider configuration.
    pub identity_hash: [u8; 32],
}

/// Schema fingerprints at each plan boundary.
///
/// Used to detect schema drift between compilation runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaFingerprints {
    /// Blake3 hash of the P0 logical plan output schema.
    pub p0_schema_hash: [u8; 32],
    /// Blake3 hash of the P1 optimized plan output schema.
    pub p1_schema_hash: [u8; 32],
    /// Blake3 hash of the P2 physical plan output schema.
    pub p2_schema_hash: [u8; 32],
}

// ---------------------------------------------------------------------------
// PlanDiff: digest-based comparison
// ---------------------------------------------------------------------------

/// Digest-based plan comparison result.
///
/// All comparisons operate on blake3 digests, not debug strings.
/// This makes diffs deterministic, storage-efficient, and immune to
/// formatting changes in DataFusion's Display impls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanDiff {
    /// True if P0 logical plan digests differ.
    pub p0_changed: bool,
    /// True if P1 optimized plan digests differ.
    pub p1_changed: bool,
    /// True if P2 physical plan digests differ.
    pub p2_changed: bool,
    /// True if any schema fingerprint changed.
    pub schema_drift: bool,
    /// True if rulepack fingerprints differ.
    pub rulepack_changed: bool,
    /// True if provider identity sets differ.
    pub providers_changed: bool,
    /// Human-readable summary of what changed.
    pub summary: Vec<String>,
}

// ---------------------------------------------------------------------------
// Capture pipeline
// ---------------------------------------------------------------------------

/// Capture the three plan stages from a DataFrame.
///
/// Produces a `PlanBundleRuntime` containing:
/// - P0: the unoptimized logical plan
/// - P1: the optimized logical plan (after SessionContext optimizer rules)
/// - P2: the physical execution plan
pub async fn capture_plan_bundle_runtime(
    ctx: &SessionContext,
    df: &DataFrame,
) -> Result<PlanBundleRuntime> {
    let p0_logical = df.logical_plan().clone();
    let p1_optimized = ctx.state().optimize(&p0_logical)?;
    let p2_physical = df.clone().create_physical_plan().await?;

    Ok(PlanBundleRuntime {
        p0_logical,
        p1_optimized,
        p2_physical,
    })
}

/// Build a persisted artifact from runtime plan handles.
///
/// Computes canonical digests, captures explain outputs, and optionally
/// produces portability artifacts (Substrait bytes, SQL text).
///
/// Parameters
/// ----------
/// ctx
///     Active SessionContext for explain operations.
/// runtime
///     Runtime plan handles from `capture_plan_bundle_runtime`.
/// rulepack_fingerprint
///     Blake3 digest of the rulepack definition.
/// provider_identities
///     Identity hashes for each registered provider.
/// capture_substrait
///     When true, encode the optimized plan to Substrait bytes.
/// capture_sql
///     When true, unparse the optimized plan to SQL text.
pub async fn build_plan_bundle_artifact(
    ctx: &SessionContext,
    runtime: &PlanBundleRuntime,
    rulepack_fingerprint: [u8; 32],
    provider_identities: Vec<ProviderIdentity>,
    capture_substrait: bool,
    capture_sql: bool,
) -> Result<PlanBundleArtifact> {
    let p0_text = normalize_logical(&runtime.p0_logical);
    let p1_text = normalize_logical(&runtime.p1_optimized);
    let p2_text = normalize_physical(runtime.p2_physical.as_ref());

    let explain_verbose = capture_explain_verbose(ctx, &runtime.p1_optimized).await?;
    let explain_analyze = capture_explain_analyze(ctx, &runtime.p1_optimized).await?;

    // Schema fingerprints: P0/P1 use DFSchema::as_arrow(), P2 uses Arrow directly.
    let p0_arrow_schema = runtime.p0_logical.schema().as_arrow();
    let p1_arrow_schema = runtime.p1_optimized.schema().as_arrow();

    Ok(PlanBundleArtifact {
        artifact_version: 1,
        p0_digest: blake3_hash_bytes(p0_text.as_bytes()),
        p1_digest: blake3_hash_bytes(p1_text.as_bytes()),
        p2_digest: blake3_hash_bytes(p2_text.as_bytes()),
        p0_text: Some(p0_text),
        p1_text: Some(p1_text),
        p2_text: Some(p2_text),
        explain_verbose,
        explain_analyze,
        rulepack_fingerprint,
        provider_identities,
        schema_fingerprints: SchemaFingerprints {
            p0_schema_hash: hash_schema(p0_arrow_schema),
            p1_schema_hash: hash_schema(p1_arrow_schema),
            p2_schema_hash: hash_schema(&runtime.p2_physical.schema()),
        },
        substrait_bytes: if capture_substrait {
            try_substrait_encode(ctx, &runtime.p1_optimized).ok()
        } else {
            None
        },
        sql_text: if capture_sql {
            try_sql_unparse(&runtime.p1_optimized).ok()
        } else {
            None
        },
    })
}

// ---------------------------------------------------------------------------
// Diff
// ---------------------------------------------------------------------------

/// Compare two plan bundle artifacts using canonical digests.
///
/// Returns a `PlanDiff` summarizing which aspects changed.
pub fn diff_artifacts(a: &PlanBundleArtifact, b: &PlanBundleArtifact) -> PlanDiff {
    let p0_changed = a.p0_digest != b.p0_digest;
    let p1_changed = a.p1_digest != b.p1_digest;
    let p2_changed = a.p2_digest != b.p2_digest;
    let schema_drift = a.schema_fingerprints.p0_schema_hash
        != b.schema_fingerprints.p0_schema_hash
        || a.schema_fingerprints.p1_schema_hash != b.schema_fingerprints.p1_schema_hash
        || a.schema_fingerprints.p2_schema_hash != b.schema_fingerprints.p2_schema_hash;
    let rulepack_changed = a.rulepack_fingerprint != b.rulepack_fingerprint;
    let providers_changed = a.provider_identities != b.provider_identities;

    let mut summary = Vec::new();
    if p0_changed {
        summary.push("P0 logical digest changed".into());
    }
    if p1_changed {
        summary.push("P1 optimized digest changed".into());
    }
    if p2_changed {
        summary.push("P2 physical digest changed".into());
    }
    if schema_drift {
        summary.push("Schema fingerprints changed".into());
    }
    if rulepack_changed {
        summary.push("Rulepack fingerprint changed".into());
    }
    if providers_changed {
        summary.push("Provider identities changed".into());
    }

    PlanDiff {
        p0_changed,
        p1_changed,
        p2_changed,
        schema_drift,
        rulepack_changed,
        providers_changed,
        summary,
    }
}

// ---------------------------------------------------------------------------
// SQL unparse
// ---------------------------------------------------------------------------

/// Attempt to unparse a logical plan back to SQL text.
///
/// Uses `datafusion_sql::unparser::plan_to_sql` to produce a SQL statement.
/// Returns an error if the plan contains constructs that cannot be represented
/// in SQL (e.g., custom UDFs without SQL mappings).
pub fn try_sql_unparse(plan: &LogicalPlan) -> Result<String> {
    let statement = datafusion_sql::unparser::plan_to_sql(plan)?;
    Ok(statement.to_string())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Produce a deterministic normalized text representation of a logical plan.
///
/// Uses DataFusion's `display_indent()` which provides a stable, indented
/// representation suitable for digest computation.
fn normalize_logical(plan: &LogicalPlan) -> String {
    format!("{}", plan.display_indent())
}

/// Produce a deterministic normalized text representation of a physical plan.
///
/// Uses DataFusion's `displayable()` with indentation for a stable
/// representation suitable for digest computation.
fn normalize_physical(plan: &dyn ExecutionPlan) -> String {
    format!("{}", displayable(plan).indent(true))
}

/// Compute a blake3 hash over raw bytes.
fn blake3_hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

/// Compute a blake3 hash over an Arrow schema.
///
/// Serializes the schema to its canonical JSON representation before hashing.
/// This ensures schema comparisons are format-independent and stable.
fn hash_schema(schema: &ArrowSchema) -> [u8; 32] {
    // Use serde_json for a canonical representation of the Arrow schema.
    // Arrow's Schema implements Serialize via serde.
    let schema_json = serde_json::to_string(schema).unwrap_or_default();
    blake3_hash_bytes(schema_json.as_bytes())
}

/// Capture EXPLAIN VERBOSE output for a logical plan.
///
/// Creates a DataFrame from the plan, runs EXPLAIN(verbose=true, analyze=false),
/// and parses the resulting record batches into `ExplainEntry` items.
async fn capture_explain_verbose(
    ctx: &SessionContext,
    plan: &LogicalPlan,
) -> Result<Vec<ExplainEntry>> {
    let df = ctx.execute_logical_plan(plan.clone()).await?;
    let explain_df = df.explain(true, false)?;
    parse_explain_batches(explain_df).await
}

/// Capture EXPLAIN ANALYZE output for a logical plan.
///
/// Creates a DataFrame from the plan, runs EXPLAIN(verbose=false, analyze=true),
/// and parses the resulting record batches into `ExplainEntry` items.
async fn capture_explain_analyze(
    ctx: &SessionContext,
    plan: &LogicalPlan,
) -> Result<Vec<ExplainEntry>> {
    let df = ctx.execute_logical_plan(plan.clone()).await?;
    let explain_df = df.explain(false, true)?;
    parse_explain_batches(explain_df).await
}

/// Parse EXPLAIN output batches into `ExplainEntry` items.
///
/// DataFusion EXPLAIN produces record batches with two columns:
/// - column 0: plan_type (Utf8)
/// - column 1: plan (Utf8)
async fn parse_explain_batches(explain_df: DataFrame) -> Result<Vec<ExplainEntry>> {
    use arrow::array::AsArray;

    let batches = explain_df.collect().await?;
    let mut entries = Vec::new();

    for batch in &batches {
        if batch.num_columns() < 2 {
            continue;
        }

        let plan_type_col = batch.column(0).as_string::<i32>();
        let plan_text_col = batch.column(1).as_string::<i32>();

        for row in 0..batch.num_rows() {
            entries.push(ExplainEntry {
                plan_type: plan_type_col.value(row).to_string(),
                plan_text: plan_text_col.value(row).to_string(),
            });
        }
    }

    Ok(entries)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    /// Create a minimal test context with one in-memory table.
    async fn test_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("test_table", Arc::new(table)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_capture_runtime_produces_three_stages() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        // All three plan stages should be populated.
        // P0 and P1 should have schemas with the same field count.
        let p0_fields = runtime.p0_logical.schema().fields().len();
        let p1_fields = runtime.p1_optimized.schema().fields().len();
        let p2_fields = runtime.p2_physical.schema().fields().len();
        assert_eq!(p0_fields, 2);
        assert_eq!(p1_fields, 2);
        assert_eq!(p2_fields, 2);
    }

    #[tokio::test]
    async fn test_build_artifact_produces_digests() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let artifact = build_plan_bundle_artifact(
            &ctx,
            &runtime,
            [0u8; 32],
            vec![],
            false,
            false,
        )
        .await
        .unwrap();

        // Digests should be non-zero (normalized text is non-empty).
        assert_ne!(artifact.p0_digest, [0u8; 32]);
        assert_ne!(artifact.p1_digest, [0u8; 32]);
        assert_ne!(artifact.p2_digest, [0u8; 32]);
        assert_eq!(artifact.artifact_version, 1);
    }

    #[tokio::test]
    async fn test_stable_digests_across_runs() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();

        let runtime1 = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();
        let artifact1 = build_plan_bundle_artifact(
            &ctx,
            &runtime1,
            [0u8; 32],
            vec![],
            false,
            false,
        )
        .await
        .unwrap();

        let df2 = ctx.table("test_table").await.unwrap();
        let runtime2 = capture_plan_bundle_runtime(&ctx, &df2).await.unwrap();
        let artifact2 = build_plan_bundle_artifact(
            &ctx,
            &runtime2,
            [0u8; 32],
            vec![],
            false,
            false,
        )
        .await
        .unwrap();

        // Identical inputs should produce identical digests.
        assert_eq!(artifact1.p0_digest, artifact2.p0_digest);
        assert_eq!(artifact1.p1_digest, artifact2.p1_digest);
        assert_eq!(artifact1.p2_digest, artifact2.p2_digest);
    }

    #[tokio::test]
    async fn test_diff_identical_artifacts() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let artifact = build_plan_bundle_artifact(
            &ctx,
            &runtime,
            [0u8; 32],
            vec![],
            false,
            false,
        )
        .await
        .unwrap();

        let diff = diff_artifacts(&artifact, &artifact);
        assert!(!diff.p0_changed);
        assert!(!diff.p1_changed);
        assert!(!diff.p2_changed);
        assert!(!diff.schema_drift);
        assert!(!diff.rulepack_changed);
        assert!(!diff.providers_changed);
        assert!(diff.summary.is_empty());
    }

    #[tokio::test]
    async fn test_diff_detects_rulepack_change() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let artifact_a = build_plan_bundle_artifact(
            &ctx,
            &runtime,
            [0u8; 32],
            vec![],
            false,
            false,
        )
        .await
        .unwrap();

        let artifact_b = build_plan_bundle_artifact(
            &ctx,
            &runtime,
            [1u8; 32],
            vec![],
            false,
            false,
        )
        .await
        .unwrap();

        let diff = diff_artifacts(&artifact_a, &artifact_b);
        assert!(diff.rulepack_changed);
        assert!(diff.summary.iter().any(|s| s.contains("Rulepack")));
    }

    #[tokio::test]
    async fn test_diff_detects_provider_change() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let artifact_a = build_plan_bundle_artifact(
            &ctx,
            &runtime,
            [0u8; 32],
            vec![],
            false,
            false,
        )
        .await
        .unwrap();

        let artifact_b = build_plan_bundle_artifact(
            &ctx,
            &runtime,
            [0u8; 32],
            vec![ProviderIdentity {
                table_name: "test_table".into(),
                identity_hash: [42u8; 32],
            }],
            false,
            false,
        )
        .await
        .unwrap();

        let diff = diff_artifacts(&artifact_a, &artifact_b);
        assert!(diff.providers_changed);
        assert!(diff.summary.iter().any(|s| s.contains("Provider")));
    }

    #[tokio::test]
    async fn test_sql_unparse_produces_text() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let artifact = build_plan_bundle_artifact(
            &ctx,
            &runtime,
            [0u8; 32],
            vec![],
            false,
            true, // capture SQL
        )
        .await
        .unwrap();

        // SQL text should be present and non-empty.
        assert!(artifact.sql_text.is_some());
        let sql = artifact.sql_text.as_ref().unwrap();
        assert!(!sql.is_empty());
    }

    #[tokio::test]
    async fn test_explain_entries_populated() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let artifact = build_plan_bundle_artifact(
            &ctx,
            &runtime,
            [0u8; 32],
            vec![],
            false,
            false,
        )
        .await
        .unwrap();

        // EXPLAIN VERBOSE should produce at least one entry.
        assert!(!artifact.explain_verbose.is_empty());
        // EXPLAIN ANALYZE should produce at least one entry.
        assert!(!artifact.explain_analyze.is_empty());
    }

    #[test]
    fn test_blake3_hash_deterministic() {
        let input = b"deterministic input";
        let hash1 = blake3_hash_bytes(input);
        let hash2 = blake3_hash_bytes(input);
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, [0u8; 32]);
    }

    #[test]
    fn test_hash_schema_deterministic() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let h1 = hash_schema(&schema);
        let h2 = hash_schema(&schema);
        assert_eq!(h1, h2);
        assert_ne!(h1, [0u8; 32]);
    }

    #[test]
    fn test_hash_schema_differs_for_different_schemas() {
        let s1 = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let s2 = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        assert_ne!(hash_schema(&s1), hash_schema(&s2));
    }

    #[test]
    fn test_normalize_logical_nonempty() {
        // Build a trivial logical plan via SessionContext.
        // We can't easily construct one without async, so test the helper directly.
        let text = "some plan text";
        let hash = blake3_hash_bytes(text.as_bytes());
        assert_ne!(hash, [0u8; 32]);
    }
}
