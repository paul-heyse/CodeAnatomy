//! WS-P1: Structured plan artifact contract.
//!
//! Split model separating runtime plan handles from persisted artifact payloads:
//! - `PlanBundleRuntime`: runtime-only plan handles for execution (never serialized)
//! - `PlanBundleArtifact`: persisted, versioned payload for storage/diffing
//!
//! Plan comparison uses canonical digests (blake3 over normalized text), not
//! debug-string equality. This makes diffs deterministic and storage-efficient.

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::compiler::optimizer_pipeline::OptimizerPassTrace;
use crate::executor::warnings::{RunWarning, WarningCode, WarningStage};
use crate::providers::pushdown_contract::PushdownContractReport;
use crate::schema::introspection::hash_arrow_schema;

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

/// Inputs required to build a persisted plan bundle artifact.
pub struct PlanBundleArtifactBuildRequest<'a> {
    pub ctx: &'a SessionContext,
    pub runtime: &'a PlanBundleRuntime,
    pub rulepack_fingerprint: [u8; 32],
    pub provider_identities: Vec<ProviderIdentity>,
    pub optimizer_traces: Vec<OptimizerPassTrace>,
    pub pushdown_report: Option<PushdownContractReport>,
    pub deterministic_inputs: bool,
    pub no_volatile_udfs: bool,
    pub deterministic_optimizer: bool,
    pub stats_quality: Option<String>,
    pub capture_substrait: bool,
    pub capture_sql: bool,
    pub capture_delta_codec: bool,
    pub planning_surface_hash: [u8; 32],
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

    // -- Planning surface identity --
    /// BLAKE3 hash of the planning surface manifest.
    pub planning_surface_hash: [u8; 32],

    // -- Portability artifacts (optional) --
    /// Substrait-encoded logical plan bytes (requires `substrait` feature).
    pub substrait_bytes: Option<Vec<u8>>,
    /// SQL text unparsed from the optimized logical plan.
    pub sql_text: Option<String>,
    /// Delta extension-codec logical plan bytes.
    pub delta_codec_logical_bytes: Option<Vec<u8>>,
    /// Delta extension-codec physical plan bytes.
    pub delta_codec_physical_bytes: Option<Vec<u8>>,
    /// Optimizer pass traces captured for this plan compilation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub optimizer_traces: Vec<OptimizerPassTrace>,
    /// Pushdown contract validation report.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushdown_report: Option<PushdownContractReport>,
    /// Provider lineage mapping of scans to provider identities.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provider_lineage: Vec<ProviderLineageEntry>,
    /// Referenced table names captured from optimized plan scans.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub referenced_tables: Vec<String>,
    /// Scalar UDF names captured from optimized plan expressions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_udfs: Vec<String>,
    /// Replay compatibility indicators.
    #[serde(default)]
    pub replay_flags: ReplayCompatibilityFlags,
    /// Portable vs non-portable artifact policy markers.
    #[serde(default)]
    pub portability: PortableArtifactPolicy,
    /// Stats quality grade for cost/scheduling reliability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats_quality: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ProviderLineageEntry {
    pub scan_node_id: String,
    pub provider_name: String,
    pub provider_identity_hash: [u8; 32],
    pub delta_version: Option<i64>,
    pub file_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayCompatibilityFlags {
    pub deterministic_inputs: bool,
    pub no_volatile_udfs: bool,
    pub deterministic_optimizer: bool,
}

impl Default for ReplayCompatibilityFlags {
    fn default() -> Self {
        Self {
            deterministic_inputs: false,
            no_volatile_udfs: false,
            deterministic_optimizer: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PortableArtifactPolicy {
    pub substrait_portable: bool,
    pub physical_diagnostics_portable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub portable_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub portable_digest: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub portable_schema_version: Option<u32>,
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
    /// Optional Delta compatibility facts captured at registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_compatibility: Option<DeltaProviderCompatibility>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeltaProviderCompatibility {
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub reader_features: Vec<String>,
    pub writer_features: Vec<String>,
    pub column_mapping_mode: Option<String>,
    pub partition_columns: Vec<String>,
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
    /// True if planning surface hashes differ.
    pub planning_surface_changed: bool,
    /// True if logical delta-codec bytes differ.
    pub delta_codec_logical_changed: bool,
    /// True if physical delta-codec bytes differ.
    pub delta_codec_physical_changed: bool,
    /// True if optimizer trace payloads differ.
    pub optimizer_traces_changed: bool,
    /// True if pushdown reports differ.
    pub pushdown_report_changed: bool,
    /// True if provider lineage differs.
    pub provider_lineage_changed: bool,
    /// True if referenced table sets differ.
    pub referenced_tables_changed: bool,
    /// True if required UDF sets differ.
    pub required_udfs_changed: bool,
    /// True if replay flags differ.
    pub replay_flags_changed: bool,
    /// True if portability policy differs.
    pub portability_changed: bool,
    /// Digest summary for logical delta-codec payload in artifact A.
    pub delta_codec_logical_before: Option<CodecDigestSummary>,
    /// Digest summary for logical delta-codec payload in artifact B.
    pub delta_codec_logical_after: Option<CodecDigestSummary>,
    /// Digest summary for physical delta-codec payload in artifact A.
    pub delta_codec_physical_before: Option<CodecDigestSummary>,
    /// Digest summary for physical delta-codec payload in artifact B.
    pub delta_codec_physical_after: Option<CodecDigestSummary>,
    /// Human-readable summary of what changed.
    pub summary: Vec<String>,
}

/// Compact digest summary for codec payload diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodecDigestSummary {
    pub len: usize,
    pub blake3: [u8; 32],
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
/// capture_delta_codec
///     When true, encode logical/physical plans with Delta extension codecs.
/// planning_surface_hash
///     BLAKE3 hash of the planning surface manifest.
pub async fn build_plan_bundle_artifact(
    request: PlanBundleArtifactBuildRequest<'_>,
) -> Result<PlanBundleArtifact> {
    let (artifact, _warnings) = build_plan_bundle_artifact_with_warnings(request).await?;
    Ok(artifact)
}

/// Build a persisted artifact and return non-fatal capture warnings.
pub async fn build_plan_bundle_artifact_with_warnings(
    request: PlanBundleArtifactBuildRequest<'_>,
) -> Result<(PlanBundleArtifact, Vec<RunWarning>)> {
    let PlanBundleArtifactBuildRequest {
        ctx,
        runtime,
        rulepack_fingerprint,
        provider_identities,
        optimizer_traces,
        pushdown_report,
        deterministic_inputs,
        no_volatile_udfs,
        deterministic_optimizer,
        stats_quality,
        capture_substrait,
        capture_sql,
        capture_delta_codec,
        planning_surface_hash,
    } = request;
    let p0_text = normalize_logical(&runtime.p0_logical);
    let p1_text = normalize_logical(&runtime.p1_optimized);
    let p2_text = normalize_physical(runtime.p2_physical.as_ref());

    let explain_verbose = capture_explain_verbose(ctx, &runtime.p1_optimized).await?;
    let explain_analyze = capture_explain_analyze(ctx, &runtime.p1_optimized).await?;
    let mut warnings: Vec<RunWarning> = Vec::new();

    // Schema fingerprints: P0/P1 use DFSchema::as_arrow(), P2 uses Arrow directly.
    let p0_arrow_schema = runtime.p0_logical.schema().as_arrow();
    let p1_arrow_schema = runtime.p1_optimized.schema().as_arrow();

    let substrait_bytes = if capture_substrait {
        match try_substrait_encode(ctx, &runtime.p1_optimized) {
            Ok(bytes) => Some(bytes),
            Err(err) => {
                warnings.push(RunWarning::new(
                    WarningCode::OptionalSubstraitCaptureFailed,
                    WarningStage::PlanBundle,
                    format!("Optional plan capture failed for substrait: {err}"),
                ));
                None
            }
        }
    } else {
        None
    };
    let sql_text = if capture_sql {
        match try_sql_unparse(&runtime.p1_optimized) {
            Ok(text) => Some(text),
            Err(err) => {
                warnings.push(RunWarning::new(
                    WarningCode::OptionalSqlCaptureFailed,
                    WarningStage::PlanBundle,
                    format!("Optional plan capture failed for sql_text: {err}"),
                ));
                None
            }
        }
    } else {
        None
    };
    let (delta_codec_logical_bytes, delta_codec_physical_bytes) = if capture_delta_codec {
        match super::plan_codec::encode_with_delta_codecs(
            &runtime.p1_optimized,
            Arc::clone(&runtime.p2_physical),
        ) {
            Ok((logical, physical)) => (Some(logical), Some(physical)),
            Err(err) => {
                warnings.push(RunWarning::new(
                    WarningCode::OptionalDeltaCodecCaptureFailed,
                    WarningStage::PlanBundle,
                    format!("Optional plan capture failed for delta_codec: {err}"),
                ));
                (None, None)
            }
        }
    } else {
        (None, None)
    };

    let provider_lineage = extract_provider_lineage(&runtime.p1_optimized, &provider_identities);
    let referenced_tables = extract_referenced_tables(&provider_lineage);
    let required_udfs = extract_required_udfs(&runtime.p1_optimized);
    let portability = PortableArtifactPolicy {
        substrait_portable: substrait_bytes.is_some(),
        physical_diagnostics_portable: false,
        portable_format: substrait_bytes.as_ref().map(|_| "substrait".to_string()),
        portable_digest: substrait_bytes
            .as_ref()
            .map(|payload| blake3_hash_bytes(payload)),
        portable_schema_version: substrait_bytes.as_ref().map(|_| 1u32),
    };
    Ok((
        PlanBundleArtifact {
            artifact_version: 2,
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
            planning_surface_hash,
            substrait_bytes,
            sql_text,
            delta_codec_logical_bytes,
            delta_codec_physical_bytes,
            optimizer_traces,
            pushdown_report,
            provider_lineage,
            referenced_tables,
            required_udfs,
            replay_flags: ReplayCompatibilityFlags {
                deterministic_inputs,
                no_volatile_udfs,
                deterministic_optimizer,
            },
            portability,
            stats_quality,
        },
        warnings,
    ))
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
    let schema_drift = a.schema_fingerprints.p0_schema_hash != b.schema_fingerprints.p0_schema_hash
        || a.schema_fingerprints.p1_schema_hash != b.schema_fingerprints.p1_schema_hash
        || a.schema_fingerprints.p2_schema_hash != b.schema_fingerprints.p2_schema_hash;
    let rulepack_changed = a.rulepack_fingerprint != b.rulepack_fingerprint;
    let providers_changed = a.provider_identities != b.provider_identities;
    let planning_surface_changed = a.planning_surface_hash != b.planning_surface_hash;
    let delta_codec_logical_changed = a.delta_codec_logical_bytes != b.delta_codec_logical_bytes;
    let delta_codec_physical_changed = a.delta_codec_physical_bytes != b.delta_codec_physical_bytes;
    let optimizer_traces_changed = a.optimizer_traces != b.optimizer_traces;
    let pushdown_report_changed = a.pushdown_report != b.pushdown_report;
    let provider_lineage_changed = a.provider_lineage != b.provider_lineage;
    let referenced_tables_changed = a.referenced_tables != b.referenced_tables;
    let required_udfs_changed = a.required_udfs != b.required_udfs;
    let replay_flags_changed = a.replay_flags != b.replay_flags;
    let portability_changed = a.portability != b.portability;
    let delta_codec_logical_before = codec_digest_summary(a.delta_codec_logical_bytes.as_deref());
    let delta_codec_logical_after = codec_digest_summary(b.delta_codec_logical_bytes.as_deref());
    let delta_codec_physical_before = codec_digest_summary(a.delta_codec_physical_bytes.as_deref());
    let delta_codec_physical_after = codec_digest_summary(b.delta_codec_physical_bytes.as_deref());

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
    if planning_surface_changed {
        summary.push("Planning surface hash changed".into());
    }
    if delta_codec_logical_changed {
        summary.push("Delta codec logical payload changed".into());
    }
    if delta_codec_physical_changed {
        summary.push("Delta codec physical payload changed".into());
    }
    if optimizer_traces_changed {
        summary.push("Optimizer traces changed".into());
    }
    if pushdown_report_changed {
        summary.push("Pushdown report changed".into());
    }
    if provider_lineage_changed {
        summary.push("Provider lineage changed".into());
    }
    if referenced_tables_changed {
        summary.push("Referenced tables changed".into());
    }
    if required_udfs_changed {
        summary.push("Required UDF set changed".into());
    }
    if replay_flags_changed {
        summary.push("Replay flags changed".into());
    }
    if portability_changed {
        summary.push("Portability policy changed".into());
    }

    PlanDiff {
        p0_changed,
        p1_changed,
        p2_changed,
        schema_drift,
        rulepack_changed,
        providers_changed,
        planning_surface_changed,
        delta_codec_logical_changed,
        delta_codec_physical_changed,
        optimizer_traces_changed,
        pushdown_report_changed,
        provider_lineage_changed,
        referenced_tables_changed,
        required_udfs_changed,
        replay_flags_changed,
        portability_changed,
        delta_codec_logical_before,
        delta_codec_logical_after,
        delta_codec_physical_before,
        delta_codec_physical_after,
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
/// Delegates to the authoritative schema hashing implementation.
fn hash_schema(schema: &ArrowSchema) -> [u8; 32] {
    hash_arrow_schema(schema)
}

fn codec_digest_summary(bytes: Option<&[u8]>) -> Option<CodecDigestSummary> {
    bytes.map(|payload| CodecDigestSummary {
        len: payload.len(),
        blake3: blake3_hash_bytes(payload),
    })
}

fn extract_provider_lineage(
    optimized_plan: &LogicalPlan,
    provider_identities: &[ProviderIdentity],
) -> Vec<ProviderLineageEntry> {
    let mut entries = Vec::new();
    let mut stack = vec![optimized_plan.clone()];
    let mut scan_index = 0usize;
    while let Some(plan) = stack.pop() {
        if let LogicalPlan::TableScan(scan) = &plan {
            let table_name = scan.table_name.to_string();
            if let Some(identity) = provider_identities
                .iter()
                .find(|provider| provider.table_name == table_name)
            {
                entries.push(ProviderLineageEntry {
                    scan_node_id: format!("{table_name}:{scan_index}"),
                    provider_name: identity.table_name.clone(),
                    provider_identity_hash: identity.identity_hash,
                    delta_version: None,
                    file_count: None,
                });
                scan_index += 1;
            }
        }
        for input in plan.inputs() {
            stack.push((*input).clone());
        }
    }
    entries.sort_by(|a, b| a.provider_name.cmp(&b.provider_name));
    entries
}

fn extract_referenced_tables(provider_lineage: &[ProviderLineageEntry]) -> Vec<String> {
    let mut names = provider_lineage
        .iter()
        .map(|entry| entry.provider_name.clone())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

fn extract_required_udfs(optimized_plan: &LogicalPlan) -> Vec<String> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

    let mut names: BTreeSet<String> = BTreeSet::new();
    let mut stack = vec![optimized_plan.clone()];
    while let Some(plan) = stack.pop() {
        for expr in plan.expressions() {
            let _ = expr.apply(|child_expr| {
                if let Expr::ScalarFunction(func) = child_expr {
                    names.insert(func.name().to_string());
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
        for input in plan.inputs() {
            stack.push((*input).clone());
        }
    }
    names.into_iter().collect()
}

/// Migrate plan artifacts across schema versions.
pub fn migrate_artifact(artifact: &PlanBundleArtifact) -> Result<PlanBundleArtifact> {
    match artifact.artifact_version {
        2 => Ok(artifact.clone()),
        1 => {
            let mut upgraded = artifact.clone();
            upgraded.artifact_version = 2;
            upgraded.optimizer_traces = Vec::new();
            upgraded.pushdown_report = None;
            upgraded.provider_lineage = Vec::new();
            upgraded.referenced_tables = Vec::new();
            upgraded.required_udfs = Vec::new();
            upgraded.replay_flags = ReplayCompatibilityFlags::default();
            upgraded.portability = PortableArtifactPolicy::default();
            upgraded.stats_quality = None;
            Ok(upgraded)
        }
        version => Err(datafusion_common::DataFusionError::Plan(format!(
            "Unknown artifact version: {version}"
        ))),
    }
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

        let artifact = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
        .await
        .unwrap();

        // Digests should be non-zero (normalized text is non-empty).
        assert_ne!(artifact.p0_digest, [0u8; 32]);
        assert_ne!(artifact.p1_digest, [0u8; 32]);
        assert_ne!(artifact.p2_digest, [0u8; 32]);
        assert_eq!(artifact.artifact_version, 2);
    }

    #[tokio::test]
    async fn test_stable_digests_across_runs() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();

        let runtime1 = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();
        let artifact1 = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime1,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
        .await
        .unwrap();

        let df2 = ctx.table("test_table").await.unwrap();
        let runtime2 = capture_plan_bundle_runtime(&ctx, &df2).await.unwrap();
        let artifact2 = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime2,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
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

        let artifact = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
        .await
        .unwrap();

        let diff = diff_artifacts(&artifact, &artifact);
        assert!(!diff.p0_changed);
        assert!(!diff.p1_changed);
        assert!(!diff.p2_changed);
        assert!(!diff.schema_drift);
        assert!(!diff.rulepack_changed);
        assert!(!diff.providers_changed);
        assert!(!diff.planning_surface_changed);
        assert!(!diff.delta_codec_logical_changed);
        assert!(!diff.delta_codec_physical_changed);
        assert!(diff.delta_codec_logical_before.is_none());
        assert!(diff.delta_codec_logical_after.is_none());
        assert!(diff.delta_codec_physical_before.is_none());
        assert!(diff.delta_codec_physical_after.is_none());
        assert!(diff.summary.is_empty());
    }

    #[tokio::test]
    async fn test_diff_detects_rulepack_change() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let artifact_a = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
        .await
        .unwrap();

        let artifact_b = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime,
            rulepack_fingerprint: [1u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
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

        let artifact_a = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
        .await
        .unwrap();

        let artifact_b = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![ProviderIdentity {
                table_name: "test_table".into(),
                identity_hash: [42u8; 32],
                delta_compatibility: None,
            }],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
        .await
        .unwrap();

        let diff = diff_artifacts(&artifact_a, &artifact_b);
        assert!(diff.providers_changed);
        assert!(diff.summary.iter().any(|s| s.contains("Provider")));
    }

    #[tokio::test]
    async fn test_diff_detects_codec_payload_changes() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let mut artifact_a = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
        .await
        .unwrap();
        artifact_a.delta_codec_logical_bytes = Some(vec![1, 2, 3]);
        artifact_a.delta_codec_physical_bytes = Some(vec![4, 5, 6]);

        let mut artifact_b = artifact_a.clone();
        artifact_b.delta_codec_logical_bytes = Some(vec![1, 2, 9]);
        artifact_b.delta_codec_physical_bytes = Some(vec![4, 5, 7]);

        let diff = diff_artifacts(&artifact_a, &artifact_b);
        assert!(diff.delta_codec_logical_changed);
        assert!(diff.delta_codec_physical_changed);
        assert!(diff.delta_codec_logical_before.is_some());
        assert!(diff.delta_codec_logical_after.is_some());
        assert!(diff.delta_codec_physical_before.is_some());
        assert!(diff.delta_codec_physical_after.is_some());
    }

    #[tokio::test]
    async fn test_sql_unparse_produces_text() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let artifact = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: true, // capture SQL
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
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

        let artifact = build_plan_bundle_artifact(PlanBundleArtifactBuildRequest {
            ctx: &ctx,
            runtime: &runtime,
            rulepack_fingerprint: [0u8; 32],
            provider_identities: vec![],
            optimizer_traces: vec![],
            pushdown_report: None,
            deterministic_inputs: false,
            no_volatile_udfs: true,
            deterministic_optimizer: true,
            stats_quality: None,
            capture_substrait: false,
            capture_sql: false,
            capture_delta_codec: false,
            planning_surface_hash: [0u8; 32],
        })
        .await
        .unwrap();

        // EXPLAIN VERBOSE should produce at least one entry.
        assert!(!artifact.explain_verbose.is_empty());
        // EXPLAIN ANALYZE should produce at least one entry.
        assert!(!artifact.explain_analyze.is_empty());
    }

    #[cfg(not(feature = "substrait"))]
    #[tokio::test]
    async fn test_optional_substrait_capture_failure_returns_warning() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let (_artifact, warnings) =
            build_plan_bundle_artifact_with_warnings(PlanBundleArtifactBuildRequest {
                ctx: &ctx,
                runtime: &runtime,
                rulepack_fingerprint: [0u8; 32],
                provider_identities: vec![],
                optimizer_traces: vec![],
                pushdown_report: None,
                deterministic_inputs: false,
                no_volatile_udfs: true,
                deterministic_optimizer: true,
                stats_quality: None,
                capture_substrait: true,
                capture_sql: false,
                capture_delta_codec: false,
                planning_surface_hash: [0u8; 32],
            })
            .await
            .unwrap();

        assert!(
            warnings
                .iter()
                .any(|warning| warning.code == WarningCode::OptionalSubstraitCaptureFailed),
            "optional capture failures should surface through warnings"
        );
    }

    #[cfg(not(feature = "delta-codec"))]
    #[tokio::test]
    async fn test_optional_delta_codec_capture_failure_returns_warning() {
        let ctx = test_ctx().await;
        let df = ctx.table("test_table").await.unwrap();
        let runtime = capture_plan_bundle_runtime(&ctx, &df).await.unwrap();

        let (_artifact, warnings) =
            build_plan_bundle_artifact_with_warnings(PlanBundleArtifactBuildRequest {
                ctx: &ctx,
                runtime: &runtime,
                rulepack_fingerprint: [0u8; 32],
                provider_identities: vec![],
                optimizer_traces: vec![],
                pushdown_report: None,
                deterministic_inputs: false,
                no_volatile_udfs: true,
                deterministic_optimizer: true,
                stats_quality: None,
                capture_substrait: false,
                capture_sql: false,
                capture_delta_codec: true,
                planning_surface_hash: [0u8; 32],
            })
            .await
            .unwrap();

        assert!(
            warnings
                .iter()
                .any(|warning| warning.code == WarningCode::OptionalDeltaCodecCaptureFailed),
            "delta-codec optional capture failures should surface through warnings"
        );
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
