# Blank-Page Implementation Plan v2: DataFusion + DeltaLake Set-and-Forget Semantic Execution

Date: 2026-02-08

Design basis: `blank_page_datafusion_deltalake_set_and_forget_semantic_execution_v1_2026-02-07.md`

Integrates review feedback from: `blank_page_implementation_plan_v1_best_in_class_recommendations_2026-02-08.md`

---

## 0) Design Principles (from v1 Blank-Page)

1. **Rust-first hot path** — Python is a thin submission shell; planning, optimization, and execution live in Rust.
2. **One SessionContext per run** — All plan fragments built in a single universe. No cross-context plan merges.
3. **One LogicalPlan DAG** — Compose at the logical level only. Never concatenate explain text or physical plans.
4. **Rules-as-policy** — All enforcement and optimization policy encoded as AnalyzerRules, OptimizerRules, and PhysicalOptimizerRules registered into the DataFusion pipeline.
5. **Delta-native providers only** — No Arrow Dataset fallback. `DeltaTableProvider` is the mandatory scan path.
6. **Set-and-forget optimization** — Rely on engine-native optimizers. Bounded auto-tuning adjusts a narrow set of knobs.
7. **Deterministic-by-construction** — Session envelope (versions + config + catalog + functions + rules) is fingerprinted. Same inputs = same outputs.
8. **No bespoke services** — No custom policy engine, protocol validator, or artifact warehouse. Engine-native contracts replace them.
9. **Builder-first session construction** — Build `SessionState` via `SessionStateBuilder` with explicit analyzer/optimizer/physical rules before first query. No ad-hoc post-build mutation.
10. **Two-layer determinism** — Layer A: spec determinism (`spec_hash`). Layer B: execution environment determinism (`envelope_hash`). Replay requires both hashes unchanged.

---

## 1) Existing Rust Infrastructure (What We Have and Reuse)

The new engine crate builds alongside and reuses our existing hardened Rust infrastructure. The review explicitly recommends reusing existing Delta and rule implementations rather than parallel reimplementation.

```
rust/                                       # existing workspace (resolver = "2")
├── Cargo.toml                              # 6 member crates
├── datafusion_ext/                         # ~30 scalar UDFs, Delta control plane, rules
│   └── src/
│       ├── udf/                            # hash, span, string, collection, struct, position, metadata, cdf
│       ├── udf_registry.rs                 # macro-driven registry: scalar_udf_specs()
│       ├── delta_control_plane.rs          # load_delta_table, delta_provider, scan_config_from_session
│       │                                   # ↑ REUSE: table loading, EagerSnapshot extraction, scan config
│       ├── delta_mutations.rs              # insert, update, delete, merge
│       ├── delta_maintenance.rs            # vacuum, optimize, checkpoint
│       ├── planner_rules.rs               # CodeAnatomyPolicyRule (AnalyzerRule)
│       │                                   # ↑ REUSE: existing analyzer rule infrastructure
│       ├── physical_rules.rs              # CodeAnatomyPhysicalRule (PhysicalOptimizerRule)
│       │                                   # ↑ REUSE: SessionStateBuilder pattern for rule installation
│       └── function_factory.rs            # SQL macro factory
├── datafusion_ext_py/                      # PyO3 cdylib wrapping datafusion_ext
├── datafusion_python/                      # Forked Apache DataFusion Python (v51.0.0)
│   └── src/codeanatomy_ext.rs             # Plugin loading, provider factory, UDF registration
├── df_plugin_api/                          # ABI-stable plugin contract (abi_stable)
├── df_plugin_codeanatomy/                  # Plugin: Delta/CDF providers + UDF bundle
└── df_plugin_host/                         # Plugin runtime loader
```

**Pinned versions:** DataFusion 51.0.0, Arrow 57.1.0, DeltaLake 0.30.1, PyO3 0.26.

**Proven patterns we reuse (from existing workspace):**

1. **Delta table loading** — `delta_control_plane.rs:272`: `table.snapshot()?.snapshot().clone()` for `EagerSnapshot`
2. **Delta provider construction** — `delta_control_plane.rs:277`: `DeltaTableProvider::try_new(eager_snapshot, log_store, scan_config)`
3. **Scan config from session** — `delta_control_plane.rs:275`: `scan_config_from_session(&session_state, Some(&eager_snapshot), overrides)`
4. **Physical rule installation via SessionStateBuilder** — `physical_rules.rs:121-123`: `SessionStateBuilder::from(state.clone()).with_physical_optimizer_rule(Arc::new(...))`
5. **Protocol gating** — `delta_control_plane.rs:269-271`: Delta protocol version checks before provider construction

---

## 2) New File Structure

### 2.1 New Rust Crate: `codeanatomy_engine`

```
rust/
├── codeanatomy_engine/
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs                          # #[pymodule] entry + public API
│   │   │
│   │   ├── compat/                         # WS0: API Compatibility Harness
│   │   │   ├── mod.rs
│   │   │   └── smoke.rs                    # Compile-checked API conformance tests
│   │   │
│   │   ├── spec/                           # WS1: SemanticExecutionSpec
│   │   │   ├── mod.rs
│   │   │   ├── execution_spec.rs           # SemanticExecutionSpec root struct
│   │   │   ├── relations.rs               # InputRelation, ViewDefinition, ViewTransform
│   │   │   ├── join_graph.rs              # JoinGraph, JoinEdge, JoinConstraint
│   │   │   ├── outputs.rs                 # OutputTarget, MaterializationMode
│   │   │   ├── rule_intents.rs            # RuleIntent, RulepackProfile
│   │   │   └── hashing.rs                 # Canonical BLAKE3 spec hashing (with key sorting)
│   │   │
│   │   ├── session/                        # WS2: Session Factory
│   │   │   ├── mod.rs
│   │   │   ├── factory.rs                 # build_session() → (SessionContext, SessionEnvelope)
│   │   │   ├── envelope.rs                # SessionEnvelope capture + fingerprint
│   │   │   └── profiles.rs               # EnvironmentClass::Small/Medium/Large
│   │   │
│   │   ├── rules/                          # WS2.5 + WS5: Rulepack Lifecycle + Compiler
│   │   │   ├── mod.rs
│   │   │   ├── registry.rs                # CpgRuleSet: ordered rule container + fingerprint
│   │   │   ├── rulepack.rs                # RulepackProfile → immutable RuleSet mapping
│   │   │   ├── intent_compiler.rs         # RuleIntent → concrete rule instances
│   │   │   ├── analyzer.rs                # Semantic integrity AnalyzerRules
│   │   │   ├── optimizer.rs               # Logical OptimizerRules
│   │   │   └── physical.rs                # PhysicalOptimizerRules
│   │   │
│   │   ├── providers/                      # WS3: Delta Provider Manager
│   │   │   ├── mod.rs
│   │   │   ├── registration.rs            # register_extraction_inputs() via existing control plane
│   │   │   ├── snapshot.rs                # Latest vs version-pinned snapshot resolution
│   │   │   └── scan_config.rs             # Standardized DeltaScanConfig
│   │   │
│   │   ├── compiler/                       # WS4: Global Plan Combiner
│   │   │   ├── mod.rs
│   │   │   ├── plan_compiler.rs           # SemanticPlanCompiler: spec → combined LogicalPlan DAG
│   │   │   ├── view_builder.rs            # Per-view DataFrame construction
│   │   │   ├── join_builder.rs            # Join graph → join plan nodes (equality + inequality)
│   │   │   ├── union_builder.rs           # union_by_name multi-source composition
│   │   │   ├── cache_boundaries.rs        # Cost-aware cache boundary insertion
│   │   │   └── graph_validator.rs         # WS4.5: DAG acyclicity + dependency validation
│   │   │
│   │   ├── executor/                       # WS6: Execution Engine
│   │   │   ├── mod.rs
│   │   │   ├── runner.rs                  # execute_and_materialize() stream-first
│   │   │   ├── delta_writer.rs            # Delta insert_into + schema enforcement
│   │   │   └── result.rs                  # RunResult envelope
│   │   │
│   │   ├── stability/                      # WS6.5: Join/Union Stability Harness
│   │   │   ├── mod.rs
│   │   │   └── harness.rs                 # Join/union correctness + perf tests
│   │   │
│   │   ├── tuner/                          # WS7: Adaptive Tuner
│   │   │   ├── mod.rs
│   │   │   └── adaptive.rs                # Bounded auto-tuning + rollback + guardrails
│   │   │
│   │   ├── schema/                         # WS8: Schema Runtime Utilities
│   │   │   ├── mod.rs
│   │   │   └── introspection.rs           # info-schema helpers, schema diff, cast injection
│   │   │
│   │   ├── python/                         # WS9: PyO3 Bindings (thin facade)
│   │   │   ├── mod.rs
│   │   │   ├── session.rs                 # #[pyclass] SessionFactory
│   │   │   ├── compiler.rs                # #[pyclass] SemanticPlanCompiler
│   │   │   ├── materializer.rs            # #[pyclass] CpgMaterializer
│   │   │   └── result.rs                  # #[pyclass] RunResult
│   │   │
│   │   └── compliance/                     # WS10: Optional Compliance Profile Module
│   │       ├── mod.rs
│   │       └── capture.rs                 # EXPLAIN VERBOSE capture, rule-impact digest, retention
│   │
│   └── tests/
│       ├── compat_smoke.rs                 # WS0 compile-checked conformance
│       ├── session_determinism.rs
│       ├── provider_registration.rs
│       ├── plan_compilation.rs
│       ├── graph_validation.rs             # WS4.5 cycle/dependency checks
│       ├── rule_registration.rs
│       ├── join_union_stability.rs         # WS6.5 stability harness
│       └── end_to_end.rs
```

### 2.2 New Python Modules

```
src/
├── engine/
│   ├── __init__.py
│   ├── facade.py               # execute_cpg_build(): thin Python entry point
│   ├── spec_builder.py         # Build SemanticExecutionSpec from Python semantic IR
│   └── profile.py              # EnvironmentProfile detection
```

### 2.3 New Test Files

```
tests/
├── unit/
│   └── engine/
│       ├── test_spec_builder.py
│       └── test_facade.py
├── integration/
│   └── test_rust_engine_e2e.py
```

---

## 3) WS0: API Compatibility Harness (New)

Before any WS1+ implementation, gate all plan snippets through a compile-checked API conformance workspace. This prevents the class of API-mismatch bugs identified in the P0 review.

### 3.1 Purpose

Every code pattern used in this plan must compile against our pinned DataFusion 51.0.0 / DeltaLake 0.30.1. WS0 is a set of compilation-only tests that validate API surfaces before they are wired into production modules.

### 3.2 Smoke Tests

```rust
// rust/codeanatomy_engine/tests/compat_smoke.rs

//! WS0: Compile-checked API conformance against pinned DF 51 / DL 0.30.1.
//! These tests verify that the API surfaces we depend on actually exist
//! and have the expected signatures. If any test fails to compile,
//! the API has drifted and snippets must be updated.

use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::session_state::SessionStateBuilder;
use deltalake::DeltaTable;
use deltalake::delta_datafusion::{DeltaTableProvider, DeltaScanConfig};

/// Verify RuntimeEnvBuilder API (P0 correction #1).
/// Uses with_memory_pool + FairSpillPool, NOT with_fair_spill_pool.
#[tokio::test]
async fn test_runtime_env_builder_api() {
    let pool = FairSpillPool::new(512 * 1024 * 1024);
    let runtime = RuntimeEnvBuilder::default()
        .with_memory_pool(Arc::new(pool))
        .with_disk_manager_builder(
            DiskManagerBuilder::new().with_mode(DiskManagerMode::OsTmpDirectory)
        )
        .build_arc()
        .unwrap();

    assert_eq!(runtime.memory_pool.reserved(), 0);
}

/// Verify SessionConfig API (P0 correction #2).
/// Uses set_bool/set_str, NOT set(&str).
#[test]
fn test_session_config_api() {
    let config = SessionConfig::new()
        .with_default_catalog_and_schema("codeanatomy", "public")
        .with_information_schema(true)
        .with_target_partitions(8)
        .with_batch_size(8192)
        .with_repartition_joins(true)
        .with_repartition_aggregations(true)
        .with_repartition_windows(true)
        .with_parquet_pruning(true);

    // Set string config values via options_mut (P0 correction #2)
    let mut config = config;
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().optimizer.enable_dynamic_filter_pushdown = true;
    config.options_mut().optimizer.enable_topk_dynamic_filter_pushdown = true;
    config.options_mut().execution.enable_recursive_ctes = true;
    config.options_mut().sql_parser.enable_ident_normalization = false;

    assert_eq!(config.target_partitions(), 8);
}

/// Verify version capture APIs (P0 correction #3).
#[test]
fn test_version_capture_api() {
    // DataFusion version: module-level constant
    let df_version: &str = datafusion::DATAFUSION_VERSION;
    assert!(!df_version.is_empty());

    // DeltaLake version: crate function
    let dl_version: &str = deltalake::crate_version();
    assert!(!dl_version.is_empty());

    // Engine version: Cargo pkg version
    let engine_version: &str = env!("CARGO_PKG_VERSION");
    assert!(!engine_version.is_empty());
}

/// Verify DataFrame::into_view() + register_table pattern (P0 correction #5).
/// NOT register_view (which is Python-only).
#[tokio::test]
async fn test_view_registration_api() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT) AS VALUES (1), (2), (3)")
        .await
        .unwrap();

    let df = ctx.table("t").await.unwrap();

    // Correct Rust DF51 pattern: into_view() + register_table()
    let view = df.into_view();
    ctx.register_table("v", view).unwrap();

    let result = ctx.table("v").await.unwrap().collect().await.unwrap();
    assert!(!result.is_empty());
}

/// Verify DataFrame::create_physical_plan (P0 correction #6).
/// NOT execution_plan().
#[tokio::test]
async fn test_physical_plan_api() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT) AS VALUES (1)")
        .await
        .unwrap();

    let df = ctx.table("t").await.unwrap();

    // Correct Rust DF51 pattern
    let physical = df.create_physical_plan().await.unwrap();
    assert!(physical.output_partitioning().partition_count() > 0);
}

/// Verify write_table semantics (P0 correction #7).
/// write_table().await? returns Vec<RecordBatch> directly; no .collect().
#[tokio::test]
async fn test_write_table_api() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE src (a INT) AS VALUES (1), (2)")
        .await
        .unwrap();
    ctx.sql("CREATE TABLE dst (a INT) AS VALUES (0)")
        .await
        .unwrap();

    let df = ctx.table("src").await.unwrap();

    // write_table returns Vec<RecordBatch> directly
    let _result = df
        .write_table("dst", DataFrameWriteOptions::new())
        .await
        .unwrap();
    // No .collect() chained — write_table is the terminal operation
}

/// Verify SessionStateBuilder for rule registration (P0 correction #8).
/// NOT SessionContext::remove_optimizer_rule or SessionState::add_physical_optimizer_rule.
#[test]
fn test_rule_registration_via_builder() {
    use datafusion::physical_optimizer::PhysicalOptimizerRule;

    // Correct DF51 pattern: build SessionState with rules via builder
    let ctx = SessionContext::new();
    let state_ref = ctx.state_ref();
    let state = state_ref.read();

    // Rules are added via SessionStateBuilder, not post-build mutation
    let _builder = SessionStateBuilder::from(state.clone());
    // .with_optimizer_rules(vec![...])
    // .with_physical_optimizer_rules(vec![...])
    // .with_analyzer_rules(vec![...])
    // .build()
}

/// Verify DeltaTableProvider construction (P0 correction #4).
/// Uses EagerSnapshot from table.snapshot()?.snapshot().clone()
/// Matches existing pattern in datafusion_ext/src/delta_control_plane.rs:272
#[tokio::test]
async fn test_delta_provider_api() {
    // This test validates the type signatures compile correctly.
    // Actual Delta table loading uses existing load_delta_table() from control plane.
    //
    // Pattern:
    //   let table = load_delta_table(...).await?;
    //   let eager_snapshot = table.snapshot()?.snapshot().clone();
    //   let log_store = table.log_store();
    //   let scan_config = DeltaScanConfig::default();
    //   let provider = DeltaTableProvider::try_new(eager_snapshot, log_store, scan_config)?;
}
```

### 3.3 Implementation Checklist

- [ ] Create `codeanatomy_engine` crate skeleton with Cargo.toml
- [ ] Add WS0 compatibility smoke tests covering all 8 API surfaces
- [ ] Verify all tests compile and pass against DF 51.0.0 / DL 0.30.1
- [ ] Gate: no WS1+ code lands until WS0 passes `cargo test`

---

## 4) WS1: SemanticExecutionSpec Contract

The spec is the immutable contract between Python (semantic model) and Rust (execution engine). Python builds it; Rust consumes it. Nothing else crosses the boundary.

### 4.1 Rust Structs

```rust
// rust/codeanatomy_engine/src/spec/execution_spec.rs

use serde::{Deserialize, Serialize};

/// The complete execution contract: everything Rust needs to build + execute.
/// Versioned for backward-compatible parsing. Unknown fields rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticExecutionSpec {
    /// Schema version for evolution
    pub version: u32,

    /// Canonical input tables (extraction outputs as Delta locations)
    pub input_relations: Vec<InputRelation>,

    /// Semantic view definitions (the computation graph)
    pub view_definitions: Vec<ViewDefinition>,

    /// Join graph with inferred key constraints from semantic model
    pub join_graph: JoinGraph,

    /// Output target contract (CPG tables to materialize)
    pub output_targets: Vec<OutputTarget>,

    /// Rule intent declarations from semantic model
    pub rule_intents: Vec<RuleIntent>,

    /// Rulepack activation profile
    pub rulepack_profile: RulepackProfile,

    /// Parameter template definitions (for stable plan shapes)
    pub parameter_templates: Vec<ParameterTemplate>,

    /// Canonical spec identity hash (computed, not user-supplied)
    #[serde(skip)]
    pub spec_hash: [u8; 32],
}
```

```rust
// rust/codeanatomy_engine/src/spec/relations.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputRelation {
    /// Logical name for SQL/DataFrame reference
    pub logical_name: String,
    /// Delta table location (local path or object store URI)
    pub delta_location: String,
    /// Whether to enable file_column_name for provenance tracking
    pub requires_lineage: bool,
    /// Explicit version pin (None = latest snapshot)
    pub version_pin: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// Unique view name (canonical lowercase)
    pub name: String,
    /// View classification (normalize, relate, union, project, etc.)
    pub view_kind: String,
    /// Named dependencies: OTHER VIEW names only (not input relations).
    /// Input relations are referenced within the transform, not here.
    /// This distinction is critical for correct topological sorting.
    pub view_dependencies: Vec<String>,
    /// The transformation to apply
    pub transform: ViewTransform,
    /// Optional output schema contract
    pub output_schema: Option<SchemaContract>,
}

/// Each variant maps to a specific DataFrame construction pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ViewTransform {
    /// Normalize extraction input (derive IDs, spans, text normalization)
    Normalize {
        source: String,
        id_columns: Vec<String>,
        span_columns: Option<(String, String)>,
        text_columns: Vec<String>,
    },
    /// Join two relations
    Relate {
        left: String,
        right: String,
        join_type: JoinType,
        join_keys: Vec<JoinKeyPair>,
    },
    /// Union compatible schemas (drift-safe via union_by_name)
    Union {
        sources: Vec<String>,
        discriminator_column: Option<String>,
        /// true => UNION DISTINCT (dedupe), false => UNION ALL
        distinct: bool,
    },
    /// Project to output schema
    Project {
        source: String,
        columns: Vec<String>,
    },
    /// Filter rows
    Filter {
        source: String,
        predicate: String, // SQL expression string parsed by DataFusion
    },
    /// Aggregate
    Aggregate {
        source: String,
        group_by: Vec<String>,
        aggregations: Vec<AggregationExpr>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinKeyPair {
    pub left_key: String,
    pub right_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Inner, Left, Right, Full, Semi, Anti,
}
```

```rust
// rust/codeanatomy_engine/src/spec/outputs.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaterializationMode {
    Append,
    Overwrite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputTarget {
    pub table_name: String,
    pub source_view: String,
    pub columns: Vec<String>,
    pub materialization_mode: MaterializationMode,
}
```

```rust
// rust/codeanatomy_engine/src/spec/rule_intents.rs

/// Semantic constraint translated into a planning-visible rule intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleIntent {
    pub name: String,
    pub class: RuleClass,
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleClass {
    SemanticIntegrity,
    DeltaScanAware,
    CostShape,
    Safety,
}

/// Rulepack activation profile (v1 section 5.4, 17.4)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RulepackProfile {
    Default,
    LowLatency,
    Replay,
    Strict,
}
```

### 4.2 Canonical Hashing (P0 Correction #10)

`rmp_serde::to_vec_named` does not by itself guarantee canonical sorted-key encoding across arbitrary map-bearing payloads. We implement explicit canonicalization before hashing.

```rust
// rust/codeanatomy_engine/src/spec/hashing.rs

use blake3::Hasher;
use serde::Serialize;
use std::collections::BTreeMap;

/// Deterministic canonical hash of the spec.
///
/// Canonicalization contract:
/// 1. Sort all map keys lexicographically (BTreeMap guarantees this)
/// 2. Normalize numeric forms (no float-int ambiguity in spec; all fields are typed)
/// 3. Normalize string forms (canonical lowercase for identifiers)
/// 4. Serialize the canonical form, then hash
///
/// This is NOT just "serialize and hash" — we explicitly build a canonical
/// representation first to ensure cross-platform, cross-version stability.
pub fn hash_spec(spec: &SemanticExecutionSpec) -> [u8; 32] {
    let canonical = build_canonical_form(spec);
    let mut hasher = Hasher::new();
    hasher.update(&canonical);
    *hasher.finalize().as_bytes()
}

/// Build a canonical byte representation of the spec.
///
/// Strategy:
/// 1. Convert spec -> serde_json::Value
/// 2. Recursively convert to CanonicalValue with explicit BTreeMap ordering
/// 3. Serialize CanonicalValue -> bytes
///
/// This avoids relying on serde_json::Map internals for ordering behavior.
fn build_canonical_form(spec: &SemanticExecutionSpec) -> Vec<u8> {
    let value = serde_json::to_value(spec)
        .expect("spec must serialize to JSON");
    let canonical = canonicalize_value(value);
    serde_json::to_vec(&canonical)
        .expect("canonical value must serialize")
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum CanonicalValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<CanonicalValue>),
    Object(BTreeMap<String, CanonicalValue>),
}

/// Recursively ensure all map keys are sorted and values normalized.
fn canonicalize_value(value: serde_json::Value) -> CanonicalValue {
    match value {
        serde_json::Value::Null => CanonicalValue::Null,
        serde_json::Value::Bool(v) => CanonicalValue::Bool(v),
        serde_json::Value::Number(v) => CanonicalValue::Number(v),
        serde_json::Value::String(v) => CanonicalValue::String(v),
        serde_json::Value::Array(arr) => {
            CanonicalValue::Array(arr.into_iter().map(canonicalize_value).collect())
        }
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<String, CanonicalValue> = map
                .into_iter()
                .map(|(k, v)| (k, canonicalize_value(v)))
                .collect();
            CanonicalValue::Object(sorted)
        }
    }
}
```

### 4.3 Python Mirror (msgspec)

```python
# src/engine/spec_builder.py

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Literal

import msgspec

if TYPE_CHECKING:
    from semantics.ir import SemanticIR


class InputRelation(msgspec.Struct, frozen=True):
    logical_name: str
    delta_location: str
    requires_lineage: bool = False
    version_pin: int | None = None


class ViewTransform(msgspec.Struct, frozen=True):
    kind: str
    params: dict[str, object]


class ViewDefinition(msgspec.Struct, frozen=True):
    name: str
    view_kind: str
    view_dependencies: tuple[str, ...]  # view-to-view only, not input relations
    transform: ViewTransform
    output_schema: dict[str, str] | None = None


class RuleIntent(msgspec.Struct, frozen=True):
    name: str
    rule_class: str
    params: dict[str, object]


class OutputTarget(msgspec.Struct, frozen=True):
    table_name: str
    source_view: str
    columns: tuple[str, ...]
    materialization_mode: Literal["append", "overwrite"] = "overwrite"


class SemanticExecutionSpec(msgspec.Struct, frozen=True):
    version: int
    input_relations: tuple[InputRelation, ...]
    view_definitions: tuple[ViewDefinition, ...]
    join_graph: dict[str, object]
    output_targets: tuple[OutputTarget, ...]
    rule_intents: tuple[RuleIntent, ...]
    rulepack_profile: str
    parameter_templates: tuple[dict[str, object], ...] = ()
    spec_hash: bytes = b""


def build_spec_from_ir(
    ir: SemanticIR,
    input_locations: dict[str, str],
    output_targets: list[str],
    rulepack_profile: str = "default",
) -> SemanticExecutionSpec:
    """Build SemanticExecutionSpec from the Python semantic IR.

    Parameters
    ----------
    ir
        The compiled semantic IR from the upstream compiler.
    input_locations
        Map of logical name -> Delta location for extraction outputs.
    output_targets
        List of CPG output table names to materialize.
    rulepack_profile
        One of "default", "low_latency", "replay", "strict".

    Returns
    -------
    SemanticExecutionSpec
        Complete execution contract ready for Rust engine.
    """
    ...
```

### 4.4 Two-Layer Determinism Contract (P1 Improvement #5)

```rust
/// Replay requires both hashes unchanged:
///
/// Layer A: spec_hash — identifies WHAT is computed
///   Covers: input relations, view definitions, join graph, output targets,
///           rule intents, rulepack profile, parameter templates.
///
/// Layer B: envelope_hash — identifies the ENVIRONMENT in which it's computed
///   Covers: DataFusion version, DeltaLake version, engine version,
///           SessionConfig snapshot, function registry, rulepack fingerprint,
///           table registrations with schema hashes.
///
/// A replay is valid iff both spec_hash AND envelope_hash match the original run.
pub struct DeterminismContract {
    pub spec_hash: [u8; 32],
    pub envelope_hash: [u8; 32],
}

impl DeterminismContract {
    pub fn is_replay_valid(&self, original: &DeterminismContract) -> bool {
        self.spec_hash == original.spec_hash
            && self.envelope_hash == original.envelope_hash
    }
}
```

### 4.5 Implementation Checklist

- [ ] Define `SemanticExecutionSpec` Rust struct with `serde(deny_unknown_fields)`
- [ ] Define sub-structs: `InputRelation`, `ViewDefinition` (with `view_dependencies` for view-to-view only), `ViewTransform` (6 variants), `JoinGraph`, `JoinEdge`, `OutputTarget`, `RuleIntent`, `ParameterTemplate`
- [ ] Implement canonical BLAKE3 hashing with explicit key-sorted JSON canonicalization (not raw msgpack)
- [ ] Add schema version field + strict unknown-field rejection
- [ ] Add `RulepackProfile` enum (Default, LowLatency, Replay, Strict)
- [ ] Implement `DeterminismContract` with two-layer hash model
- [ ] Create Python msgspec mirror structs in `src/engine/spec_builder.py`
- [ ] Implement `build_spec_from_ir()` bridging existing `SemanticIR` → spec
- [ ] Unit tests: round-trip serde (Rust→bytes→Rust), hash stability across runs
- [ ] Unit tests: canonical hash determinism — same spec different field order → same hash
- [ ] Unit tests: unknown field rejection, version mismatch handling
- [ ] Integration test: existing semantic model → spec → deterministic hash

---

## 5) WS2: Rust Session Factory (Builder-First)

The session factory produces a deterministic `SessionContext` using `SessionStateBuilder` for full deterministic rule ordering. All config knobs explicit, all functions registered in deterministic order, all rules compiled as an immutable rulepack.

### 5.1 Key Architectural Elements

```rust
// rust/codeanatomy_engine/src/session/factory.rs

use datafusion::prelude::*;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::session_state::SessionStateBuilder;
use std::sync::Arc;

use crate::session::envelope::SessionEnvelope;
use crate::session::profiles::EnvironmentProfile;
use crate::rules::registry::CpgRuleSet;

pub struct SessionFactory {
    profile: EnvironmentProfile,
}

impl SessionFactory {
    pub fn new(profile: EnvironmentProfile) -> Self {
        Self { profile }
    }

    /// Build a deterministic SessionContext.
    ///
    /// Uses SessionStateBuilder (P0 correction #8 / P1 improvement #2):
    /// - Build complete SessionState with explicit rules BEFORE first query
    /// - No ad-hoc post-build mutation
    /// - Profile switch = new SessionState, not in-place rule deletion
    pub async fn build_session(
        &self,
        rule_set: &CpgRuleSet,
    ) -> Result<(SessionContext, SessionEnvelope)> {
        // 1. Build runtime environment with explicit memory/spill config
        //    P0 correction #1: use with_memory_pool(FairSpillPool), not with_fair_spill_pool
        let pool = FairSpillPool::new(self.profile.memory_pool_bytes as usize);
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_pool(Arc::new(pool))
            .with_disk_manager_builder(
                DiskManagerBuilder::new().with_mode(DiskManagerMode::OsTmpDirectory)
            )
            .build_arc()?;

        // 2. Build session config — every knob set, nothing left to defaults
        //    P0 correction #2: use options_mut() for typed config, not .set(&str)
        let mut config = SessionConfig::new()
            .with_default_catalog_and_schema("codeanatomy", "public")
            .with_information_schema(true)
            .with_target_partitions(self.profile.target_partitions as usize)
            .with_batch_size(self.profile.batch_size as usize)
            .with_repartition_joins(true)
            .with_repartition_aggregations(true)
            .with_repartition_windows(true)
            .with_parquet_pruning(true);

        // Typed config mutation (P0 correction #2)
        config.options_mut().execution.parquet.pushdown_filters = true;
        config.options_mut().optimizer.enable_dynamic_filter_pushdown = true;
        config.options_mut().optimizer.enable_topk_dynamic_filter_pushdown = true;
        config.options_mut().execution.enable_recursive_ctes = true;
        // Canonical lowercase, no normalization surprises (v1 section 5.3)
        config.options_mut().sql_parser.enable_ident_normalization = false;
        // Parallel planning for UNION children (best-in-class improvement)
        config.options_mut().execution.planning_concurrency =
            self.profile.target_partitions as usize;

        // 3. Build SessionState via SessionStateBuilder with ALL rules pre-registered
        //    P0 correction #8: use SessionStateBuilder, not post-build mutation
        //    P1 improvement #2: builder-first for full deterministic rule ordering
        //
        //    This is the core "rules-as-policy" contract (v1 section 7.4):
        //    rules are part of the session identity, not runtime additions.
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            // Register analyzer rules (semantic integrity + safety)
            .with_analyzer_rules(rule_set.analyzer_rules.clone())
            // Register optimizer rules (delta-scan-aware + custom)
            .with_optimizer_rules(rule_set.optimizer_rules.clone())
            // Register physical optimizer rules (cost shape + repartition)
            .with_physical_optimizer_rules(rule_set.physical_rules.clone())
            .build();

        // 4. Create context from pre-built state
        let ctx = SessionContext::new_with_state(state);

        // 5. Register existing datafusion_ext UDF/UDAF/UDWF/UDFT in deterministic order
        datafusion_ext::udf_registry::register_all(&ctx)?;
        // Optional: register engine-local UDFs when introduced.
        // register_engine_local_udfs(&ctx)?;

        // 6. Capture session envelope for fingerprinting
        let envelope = SessionEnvelope::capture(&ctx, &self.profile).await?;

        Ok((ctx, envelope))
    }
}
```

### 5.2 Session Envelope (P0 Correction #3)

```rust
// rust/codeanatomy_engine/src/session/envelope.rs

use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use crate::providers::registration::TableRegistration;

/// Complete deterministic snapshot of the session environment.
/// Two runs with the same envelope hash must produce the same plan artifacts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEnvelope {
    /// Engine version pins (v1 section 5.1)
    /// P0 correction #3: correct version capture APIs
    pub datafusion_version: String,
    pub delta_rs_version: String,
    pub codeanatomy_version: String,

    /// Full SessionConfig snapshot (sorted keys for determinism)
    pub config_snapshot: BTreeMap<String, String>,

    /// Runtime parameters
    pub target_partitions: u32,
    pub batch_size: u32,
    pub memory_pool_bytes: u64,
    pub spill_enabled: bool,

    /// Catalog/table registrations (sorted by name)
    /// Includes per-table version + schema hash for replay validation.
    pub table_registrations: Vec<TableRegistration>,

    /// Registered UDF/UDAF/UDWF names (sorted)
    pub registered_functions: Vec<String>,

    /// Semantic spec hash (Layer A of determinism contract)
    pub spec_hash: [u8; 32],

    /// Rulepack fingerprint (rule IDs + versions + order)
    pub rulepack_fingerprint: [u8; 32],

    /// Composite envelope hash (Layer B of determinism contract)
    pub envelope_hash: [u8; 32],
}

impl SessionEnvelope {
    pub async fn capture(
        ctx: &SessionContext,
        profile: &EnvironmentProfile,
    ) -> Result<Self> {
        // Snapshot config via information_schema.df_settings
        let settings_df = ctx
            .sql("SELECT name, value FROM information_schema.df_settings ORDER BY name")
            .await?;
        let batches = settings_df.collect().await?;
        let config_snapshot = batches_to_sorted_map(&batches);

        // Snapshot registered functions via SHOW FUNCTIONS
        let fn_df = ctx.sql("SHOW FUNCTIONS").await?;
        let fn_batches = fn_df.collect().await?;
        let mut registered_functions = batches_to_string_list(&fn_batches);
        registered_functions.sort();

        // Compute composite hash over all fields
        let envelope_hash = compute_envelope_hash(
            &config_snapshot,
            &registered_functions,
            profile,
        );

        Ok(Self {
            // P0 correction #3: correct version capture
            datafusion_version: datafusion::DATAFUSION_VERSION.to_string(),
            delta_rs_version: deltalake::crate_version(),
            codeanatomy_version: env!("CARGO_PKG_VERSION").to_string(),
            config_snapshot,
            target_partitions: profile.target_partitions,
            batch_size: profile.batch_size,
            memory_pool_bytes: profile.memory_pool_bytes,
            spill_enabled: true, // always with DiskManagerMode::OsTmpDirectory
            table_registrations: Vec::new(), // populated after provider registration
            registered_functions,
            spec_hash: [0u8; 32],            // populated after spec binding
            rulepack_fingerprint: [0u8; 32], // populated after rule registration
            envelope_hash,
        })
    }
}
```

### 5.3 Environment Profiles

```rust
// rust/codeanatomy_engine/src/session/profiles.rs

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentClass {
    Small,   // < 50 files, < 10K LOC
    Medium,  // 50-500 files, 10K-100K LOC
    Large,   // > 500 files, > 100K LOC
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentProfile {
    pub class: EnvironmentClass,
    pub target_partitions: u32,
    pub batch_size: u32,
    pub memory_pool_bytes: u64,
}

impl EnvironmentProfile {
    pub fn from_class(class: EnvironmentClass) -> Self {
        match class {
            EnvironmentClass::Small => Self {
                class,
                target_partitions: 4,
                batch_size: 4096,
                memory_pool_bytes: 512 * 1024 * 1024,
            },
            EnvironmentClass::Medium => Self {
                class,
                target_partitions: 8,
                batch_size: 8192,
                memory_pool_bytes: 2 * 1024 * 1024 * 1024,
            },
            EnvironmentClass::Large => Self {
                class,
                target_partitions: 16,
                batch_size: 16384,
                memory_pool_bytes: 8 * 1024 * 1024 * 1024,
            },
        }
    }
}
```

### 5.4 Implementation Checklist

- [ ] Create `rust/codeanatomy_engine/Cargo.toml` depending on workspace `datafusion_ext`, `datafusion 51`, `deltalake 0.30.1`, `arrow 57.1`, `pyo3 0.26`, `blake3`, `tokio`
- [ ] Add `codeanatomy_engine` to workspace `Cargo.toml` members
- [ ] Implement `EnvironmentProfile` with Small/Medium/Large class detection
- [ ] Implement `SessionFactory::build_session()` using `SessionStateBuilder` (not post-build mutation)
- [ ] Use `options_mut()` for typed config (not `set(&str)`)
- [ ] Use `FairSpillPool::new()` + `with_memory_pool()` (not `with_fair_spill_pool`)
- [ ] Use `with_disk_manager_builder(DiskManagerBuilder::new().with_mode(DiskManagerMode::OsTmpDirectory))` (not deprecated `with_disk_manager`)
- [ ] Use `datafusion::DATAFUSION_VERSION` and `deltalake::crate_version()` (not `env!()` macros)
- [ ] Set `planning_concurrency` for parallel UNION child planning
- [ ] Register existing UDFs via `datafusion_ext::udf_registry::register_all(&ctx)`
- [ ] Add engine-local UDF registry only when concrete engine UDFs exist
- [ ] Implement `SessionEnvelope::capture()` via `information_schema.df_settings` + `SHOW FUNCTIONS`
- [ ] Implement canonical `envelope_hash` using BLAKE3 over sorted fields
- [ ] Test: same profile → same `envelope_hash` across runs
- [ ] Test: `information_schema.df_settings` snapshot matches expected config knobs

---

## 6) WS2.5: Rulepack Lifecycle (New)

Define an immutable `RulepackProfile → RuleSet` mapping. No in-place rule deletions; profile switch means new `SessionState`.

### 6.1 Immutable Profile Mapping

```rust
// rust/codeanatomy_engine/src/rules/rulepack.rs

use std::sync::Arc;
use crate::rules::registry::CpgRuleSet;
use crate::spec::rule_intents::RulepackProfile;

/// Immutable mapping from RulepackProfile to a concrete RuleSet.
///
/// Key design decision (P1 improvement / P0 correction #8):
/// - NO in-place rule deletions (no remove_optimizer_rule)
/// - NO post-build mutation (no add_physical_optimizer_rule)
/// - Profile switch = build new SessionState via SessionStateBuilder
///
/// This aligns with DataFusion 51's actual API: SessionStateBuilder
/// provides with_optimizer_rules() and with_physical_optimizer_rules()
/// for explicit, up-front rule registration.
pub struct RulepackFactory;

impl RulepackFactory {
    /// Build the CpgRuleSet for a given profile from spec intents.
    ///
    /// Each profile yields a complete, immutable rule set.
    /// The set identity (fingerprint) is part of the session envelope.
    pub fn build_ruleset(
        profile: &RulepackProfile,
        intents: &[RuleIntent],
        env_profile: &EnvironmentProfile,
    ) -> CpgRuleSet {
        let mut analyzer_rules = Vec::new();
        let mut optimizer_rules = Vec::new();
        let mut physical_rules = Vec::new();

        // Compile all intents to concrete rules
        for intent in intents {
            match intent.class {
                RuleClass::SemanticIntegrity => {
                    if let Some(rule) = build_integrity_rule(intent) {
                        analyzer_rules.push(rule);
                    }
                }
                RuleClass::DeltaScanAware => {
                    if let Some(rule) = build_delta_scan_rule(intent) {
                        optimizer_rules.push(rule);
                    }
                }
                RuleClass::CostShape => {
                    if let Some(rule) = build_cost_shape_rule(intent, env_profile) {
                        physical_rules.push(rule);
                    }
                }
                RuleClass::Safety => {
                    if let Some(rule) = build_safety_rule(intent) {
                        analyzer_rules.push(rule);
                    }
                }
            }
        }

        // Apply profile-specific filtering BEFORE building the set.
        // No removals after construction — profiles are additive/subtractive
        // at construction time, not mutation time.
        match profile {
            RulepackProfile::LowLatency => {
                // Remove non-correctness optimization rules at build time
                optimizer_rules.retain(|r| !is_non_correctness_optimization(r.name()));
            }
            RulepackProfile::Strict => {
                // Add extra safety enforcement rules
                analyzer_rules.push(build_strict_safety_rule());
            }
            _ => {} // Default, Replay: full standard set
        }

        let fingerprint = compute_ruleset_fingerprint(
            &analyzer_rules, &optimizer_rules, &physical_rules,
        );

        CpgRuleSet { analyzer_rules, optimizer_rules, physical_rules, fingerprint }
    }
}
```

### 6.2 Rule Registry (Corrected)

```rust
// rust/codeanatomy_engine/src/rules/registry.rs

use std::sync::Arc;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_optimizer::PhysicalOptimizerRule;

/// Ordered container for all CPG plan rules.
/// Registration order is deterministic: tier → declared order within tier.
/// The set is fingerprinted for reproducibility verification.
///
/// IMPORTANT: This struct is IMMUTABLE after construction.
/// Rules are consumed by SessionStateBuilder at session construction time.
/// There is no register_all() that mutates a live SessionContext.
pub struct CpgRuleSet {
    pub analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    pub optimizer_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    pub physical_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    pub fingerprint: [u8; 32],
}
```

### 6.3 Implementation Checklist

- [ ] Implement `RulepackFactory::build_ruleset()` with profile-specific rule selection
- [ ] Implement `CpgRuleSet` as immutable struct consumed by `SessionStateBuilder`
- [ ] Implement `compute_ruleset_fingerprint()` using BLAKE3 over rule names + order
- [ ] Keep rulepack identity in envelope hash
- [ ] Test: same profile + same intents → same fingerprint across runs
- [ ] Test: LowLatency profile excludes expected rules at build time
- [ ] Test: profile switch produces entirely new CpgRuleSet (no mutation)

---

## 7) WS3: Delta Provider Manager

Always register native Delta providers. No Arrow Dataset fallback path. **Reuses existing `datafusion_ext::delta_control_plane`** for table loading and provider construction (P1 improvement #1).

### 7.1 Key Architectural Elements

```rust
// rust/codeanatomy_engine/src/providers/registration.rs

use std::collections::HashMap;
use std::sync::Arc;
use datafusion_ext::delta_control_plane::{delta_provider_from_session, DeltaScanOverrides};

use crate::spec::relations::InputRelation;

/// Registration record for the session envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRegistration {
    pub name: String,
    pub delta_version: i64,
    pub schema_hash: [u8; 32],
}

/// Register all extraction inputs as native Delta providers.
///
/// P1 improvement #1: Reuses existing delta_control_plane infrastructure
/// instead of reimplementing Delta table loading.
///
/// P0 correction #4: Uses existing load_delta_table() which correctly
/// extracts EagerSnapshot via table.snapshot()?.snapshot().clone()
/// (NOT DeltaTable::try_from_uri which doesn't exist in DL 0.30).
pub async fn register_extraction_inputs(
    ctx: &SessionContext,
    inputs: &[InputRelation],
) -> Result<Vec<TableRegistration>> {
    let mut registrations = Vec::new();

    // Sort for deterministic registration order (v1 section 5.2)
    let mut sorted = inputs.to_vec();
    sorted.sort_by_key(|i| i.logical_name.clone());

    for input in &sorted {
        // Reuse existing control-plane loader (P1 improvement #1)
        // This is the proven pattern from datafusion_ext/src/delta_control_plane.rs:272
        let (provider, snapshot, _scan_config, _add_payloads, _predicate_error) =
            delta_provider_from_session(
                ctx,
                &input.delta_location,
                Option::<HashMap<String, String>>::None,
                input.version_pin, // None = latest, Some(v) = version-pinned
                None,              // no timestamp pin
                None,              // no predicate pushdown at registration
                build_scan_overrides(input.requires_lineage),
                None,              // no protocol gate override
            )
            .await?;

        let schema_hash = hash_arrow_schema(provider.schema().as_ref());

        ctx.register_table(&input.logical_name, Arc::new(provider))?;

        registrations.push(TableRegistration {
            name: input.logical_name.clone(),
            delta_version: snapshot.version,
            schema_hash,
        });
    }

    Ok(registrations)
}

/// Build scan config overrides for lineage tracking.
fn build_scan_overrides(requires_lineage: bool) -> DeltaScanOverrides {
    if requires_lineage {
        DeltaScanOverrides {
            file_column_name: Some("__source_file".to_string()),
            ..Default::default()
        }
    } else {
        DeltaScanOverrides::default()
    }
}
```

### 7.2 Implementation Checklist

- [ ] Implement `register_extraction_inputs()` delegating to existing `delta_control_plane::delta_provider_from_session()`
- [ ] Use existing `scan_config_from_session()` for standardized scan config
- [ ] Support both latest and version-pinned snapshot modes
- [ ] Enforce replay profile pinning: every `InputRelation` must have `version_pin` in replay mode
- [ ] Apply profile-based `DeltaFeatureGate` defaults (strict/replay fail-fast on unsupported features)
- [ ] Implement `hash_arrow_schema()` for schema drift detection
- [ ] Enforce no Arrow Dataset fallback (hard error if location is not Delta)
- [ ] Test: two-tier pruning verified via `df.explain(true, true)?` showing Parquet pushdown
- [ ] Test: version-pinned registration produces same `schema_hash` and `delta_version`
- [ ] Test: `file_column_name` appears in scan output when `requires_lineage = true`

---

## 8) WS4: Global Plan Combiner (Core Design)

This is the central workstream. The plan combiner walks the view graph topologically and builds a single LogicalPlan DAG. Plan construction is the highest-leverage design element.

### 8.1 Design: One LogicalPlan DAG

The v1 design (section 7.1) is explicit: **never concatenate explain text or physical plans. Compose at the LogicalPlan level only.**

Plan combination uses three DataFusion surfaces:

1. **DataFrame API chaining** — For linked transformations (A feeds B). DataFusion produces one logical plan DAG and optimizes it as a unit. Projection/filter pushdown, join reordering, etc. all work across the entire graph.

2. **Relational operators (join/union)** — For combining independent subplans. `join()` for key-based (→ HashJoinExec), `join_on()` for predicate-based (inequality/span containment → NestedLoopJoinExec), `union_by_name()` for schema-drift-safe unions (Rust-only API).

3. **Views as named subplans** — `df.into_view()` + `ctx.register_table(name, view)` registers a DataFrame as a lazy view (P0 correction #5). Downstream views reference it by name. Views are re-planned each use unless explicitly `cache()`d.

The compiler NEVER:
- Concatenates physical plans
- Builds plans across different SessionContexts
- Uses SQL string concatenation as a plan combination method

### 8.2 Plan Compiler Implementation

```rust
// rust/codeanatomy_engine/src/compiler/plan_compiler.rs

use datafusion::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::relations::{ViewDefinition, ViewTransform};
use crate::spec::outputs::OutputTarget;

/// The SemanticPlanCompiler converts a SemanticExecutionSpec into a set of
/// output DataFrames, each representing a CPG output table.
///
/// The compiler walks the view dependency graph topologically and builds
/// one LogicalPlan DAG inside a single SessionContext (v1 One-Context Rule).
pub struct SemanticPlanCompiler<'a> {
    ctx: &'a SessionContext,
    spec: &'a SemanticExecutionSpec,
}

impl<'a> SemanticPlanCompiler<'a> {
    pub fn new(ctx: &'a SessionContext, spec: &'a SemanticExecutionSpec) -> Self {
        Self { ctx, spec }
    }

    /// Compile the full semantic spec into output DataFrames.
    ///
    /// Returns: Vec<(OutputTarget, DataFrame)> ready for materialization.
    ///
    /// Algorithm (v1 section 19):
    /// 1. Validate the view graph (acyclicity, unresolved deps) — WS4.5
    /// 2. Topologically sort view definitions by view-to-view dependency graph
    /// 3. For each view: compile to DataFrame, register as lazy view
    /// 4. Insert cost-aware cache boundaries for shared subplans
    /// 5. Build output DataFrames for each materialization target
    pub async fn compile(&self) -> Result<Vec<(OutputTarget, DataFrame)>> {
        // Step 1: Validate the view graph (WS4.5)
        self.validate_graph()?;

        // Step 2: Topological sort of view definitions
        //         P0 correction #9: only view-to-view edges, explicit cycle check
        let ordered = self.topological_sort()?;

        // Step 3: Compile each view and register as a lazy view.
        //
        // Core "plan combination" pattern: each view is a DataFrame (wrapper
        // over a LogicalPlan). When a downstream view references an upstream
        // view by name via ctx.table(), DataFusion expands the view's
        // LogicalPlan as a subplan in the downstream plan's DAG.
        //
        // Result: one interconnected LogicalPlan DAG for the entire computation.
        for view_def in &ordered {
            let df = self.compile_view(view_def).await?;

            // P0 correction #5: Rust DF51 uses into_view() + register_table(),
            // NOT register_view() (which is Python-only).
            //
            // into_view() creates a ViewTable: a lazy view that re-plans
            // using the caller's SessionState at scan time.
            // Ref: docs.rs/datafusion DataFrame::into_view()
            let view = df.into_view();
            self.ctx.register_table(&view_def.name, view)?;
        }

        // Step 4: Insert cost-aware cache boundaries (P1 improvement #3)
        self.insert_cache_boundaries().await?;

        // Step 5: Build output DataFrames for each materialization target
        let mut outputs = Vec::new();
        for target in &self.spec.output_targets {
            let df = self.ctx.table(&target.source_view).await?;
            let projected = df.select(
                target.columns.iter()
                    .map(|c| col(c))
                    .collect::<Vec<_>>()
            )?;
            outputs.push((target.clone(), projected));
        }

        Ok(outputs)
    }

    /// Topologically sort view definitions by view-to-view dependency graph.
    ///
    /// P0 correction #9: CRITICAL changes from v1:
    /// 1. Only view-to-view edges counted in indegree (input relations are pre-available roots)
    /// 2. Explicit cycle detection: if ordered.len() != view_count, fail fast with diagnostics
    /// 3. Deterministic tie-breaking via sorted queue
    fn topological_sort(&self) -> Result<Vec<&ViewDefinition>> {
        let view_names: HashSet<&str> = self.spec.view_definitions
            .iter()
            .map(|v| v.name.as_str())
            .collect();

        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

        // Build dependency graph — ONLY view-to-view edges
        for view in &self.spec.view_definitions {
            in_degree.entry(&view.name).or_insert(0);
            for dep in &view.view_dependencies {
                // Only count dependencies that are OTHER VIEWS, not input relations
                if view_names.contains(dep.as_str()) {
                    dependents.entry(dep.as_str()).or_default().push(&view.name);
                    *in_degree.entry(&view.name).or_insert(0) += 1;
                }
                // Input relations are already registered as tables — they're always available
            }
        }

        // Kahn's algorithm with deterministic tie-breaking
        let mut queue: VecDeque<&str> = {
            let mut starts: Vec<&str> = in_degree.iter()
                .filter(|(_, &deg)| deg == 0)
                .map(|(&name, _)| name)
                .collect();
            starts.sort(); // deterministic tie-breaking
            starts.into_iter().collect()
        };

        let mut ordered = Vec::new();
        let view_map: HashMap<&str, &ViewDefinition> = self.spec.view_definitions
            .iter()
            .map(|v| (v.name.as_str(), v))
            .collect();

        while let Some(name) = queue.pop_front() {
            if let Some(view) = view_map.get(name) {
                ordered.push(*view);
            }
            if let Some(deps) = dependents.get(name) {
                let mut newly_ready = Vec::new();
                for dep in deps {
                    let deg = in_degree.get_mut(dep).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        newly_ready.push(*dep);
                    }
                }
                newly_ready.sort(); // deterministic tie-breaking
                queue.extend(newly_ready);
            }
        }

        // P0 correction #9: explicit cycle detection
        if ordered.len() != self.spec.view_definitions.len() {
            let processed: HashSet<&str> = ordered.iter().map(|v| v.name.as_str()).collect();
            let mut cyclic: Vec<&str> = view_names.difference(&processed).copied().collect();
            cyclic.sort_unstable();
            return Err(DataFusionError::Plan(
                format!("Cycle detected in view dependency graph. Unresolvable views: {:?}", cyclic)
            ));
        }

        Ok(ordered)
    }

    /// Compile a single view definition to a DataFrame.
    async fn compile_view(&self, def: &ViewDefinition) -> Result<DataFrame> {
        match &def.transform {
            ViewTransform::Normalize { source, id_columns, span_columns, text_columns } => {
                self.build_normalize(source, id_columns, span_columns, text_columns).await
            }
            ViewTransform::Relate { left, right, join_type, join_keys } => {
                self.build_relate(left, right, join_type, join_keys).await
            }
            ViewTransform::Union { sources, discriminator_column, distinct } => {
                self.build_union(sources, discriminator_column, *distinct).await
            }
            ViewTransform::Project { source, columns } => {
                self.build_project(source, columns).await
            }
            ViewTransform::Filter { source, predicate } => {
                self.build_filter(source, predicate).await
            }
            ViewTransform::Aggregate { source, group_by, aggregations } => {
                self.build_aggregate(source, group_by, aggregations).await
            }
        }
    }
}
```

### 8.3 WS4.5: Plan Graph Validation (New)

```rust
// rust/codeanatomy_engine/src/compiler/graph_validator.rs

use std::collections::HashSet;

impl<'a> SemanticPlanCompiler<'a> {
    /// Validate the view dependency graph before compilation.
    ///
    /// Checks (WS4.5):
    /// 1. DAG acyclicity (caught by topological sort, but fail-fast here with diagnostics)
    /// 2. All view dependencies resolve to either a view or a registered input relation
    /// 3. Output schema contracts are satisfiable (columns exist in upstream schemas)
    /// 4. No duplicate view names
    /// 5. Stable ordering: deterministic compilation regardless of spec field order
    pub fn validate_graph(&self) -> Result<()> {
        let view_names: HashSet<&str> = self.spec.view_definitions
            .iter()
            .map(|v| v.name.as_str())
            .collect();

        let input_names: HashSet<&str> = self.spec.input_relations
            .iter()
            .map(|i| i.logical_name.as_str())
            .collect();

        // Check for duplicate view names
        if view_names.len() != self.spec.view_definitions.len() {
            return Err(DataFusionError::Plan(
                "Duplicate view names in spec".into()
            ));
        }

        // Check all dependencies resolve
        for view in &self.spec.view_definitions {
            // Check view_dependencies (view-to-view edges)
            for dep in &view.view_dependencies {
                if !view_names.contains(dep.as_str()) {
                    return Err(DataFusionError::Plan(
                        format!("View '{}' depends on unknown view '{}'", view.name, dep)
                    ));
                }
            }

            // Check source references in transforms resolve to views or inputs
            for source in extract_sources(&view.transform) {
                if !view_names.contains(source.as_str())
                    && !input_names.contains(source.as_str())
                {
                    return Err(DataFusionError::Plan(
                        format!(
                            "View '{}' references unknown source '{}' \
                             (not a view or input relation)",
                            view.name, source
                        )
                    ));
                }
            }
        }

        Ok(())
    }
}

/// Extract all source table/view names referenced in a ViewTransform.
fn extract_sources(transform: &ViewTransform) -> Vec<&str> {
    match transform {
        ViewTransform::Normalize { source, .. } => vec![source],
        ViewTransform::Relate { left, right, .. } => vec![left, right],
        ViewTransform::Union { sources, .. } => sources.iter().map(|s| s.as_str()).collect(),
        ViewTransform::Project { source, .. } => vec![source],
        ViewTransform::Filter { source, .. } => vec![source],
        ViewTransform::Aggregate { source, .. } => vec![source],
    }
}
```

### 8.4 View Builder Implementations

```rust
// rust/codeanatomy_engine/src/compiler/view_builder.rs

use datafusion::prelude::*;

impl<'a> SemanticPlanCompiler<'a> {
    /// Build a normalization view.
    pub async fn build_normalize(
        &self,
        source: &str,
        id_columns: &[String],
        span_columns: &Option<(String, String)>,
        text_columns: &[String],
    ) -> Result<DataFrame> {
        let df = self.ctx.table(source).await?;

        // Derive stable entity ID from id_columns using stable_hash_id UDF
        // (already implemented in datafusion_ext/src/udf/hash.rs)
        let mut df = df.with_column(
            "entity_id",
            call_fn("stable_hash_id", id_columns.iter().map(|c| col(c)).collect())?,
        )?;

        // Derive canonical byte span struct if span columns specified
        if let Some((bstart, bend)) = span_columns {
            df = df.with_column(
                "span",
                call_fn("span_make", vec![col(bstart), col(bend)])?,
            )?;
        }

        // Normalize text columns using utf8_normalize UDF
        for text_col in text_columns {
            df = df.with_column(
                &format!("{text_col}_normalized"),
                call_fn("utf8_normalize", vec![col(text_col)])?,
            )?;
        }

        Ok(df)
    }

    /// Build a filter view.
    pub async fn build_filter(&self, source: &str, predicate: &str) -> Result<DataFrame> {
        let df = self.ctx.table(source).await?;
        let expr = self.ctx.parse_sql_expr(predicate, df.schema())?;
        df.filter(expr)
    }

    /// Build a project view (output schema enforcement).
    pub async fn build_project(&self, source: &str, columns: &[String]) -> Result<DataFrame> {
        let df = self.ctx.table(source).await?;
        df.select(columns.iter().map(|c| col(c)).collect::<Vec<_>>())
    }

    /// Build an aggregate view.
    pub async fn build_aggregate(
        &self,
        source: &str,
        group_by: &[String],
        aggregations: &[AggregationExpr],
    ) -> Result<DataFrame> {
        let df = self.ctx.table(source).await?;
        let group_exprs: Vec<Expr> = group_by.iter().map(|c| col(c)).collect();
        let agg_exprs: Vec<Expr> = aggregations.iter()
            .map(|a| build_agg_expr(a))
            .collect::<Result<Vec<_>>>()?;
        df.aggregate(group_exprs, agg_exprs)
    }
}
```

### 8.5 Join Builder (Equality + Inequality)

```rust
// rust/codeanatomy_engine/src/compiler/join_builder.rs

use datafusion::prelude::*;
use crate::spec::relations::{JoinKeyPair, JoinType as SpecJoinType};

impl<'a> SemanticPlanCompiler<'a> {
    /// Build a relate (join) view from the semantic spec.
    ///
    /// Two join strategies:
    /// 1. Equality join via DataFrame::join() → HashJoinExec (fast, hash-based)
    /// 2. Predicate join via DataFrame::join_on() → NestedLoopJoinExec/SortMergeJoinExec
    ///    Used for byte-span containment: outer.bstart <= inner.bstart AND inner.bend <= outer.bend
    ///
    /// Best-in-class improvement: we also set dynamic filter pushdown knobs
    /// via session config to enable hash_join_inlist_pushdown for build-side
    /// value pruning on equality joins.
    pub async fn build_relate(
        &self,
        left_name: &str,
        right_name: &str,
        join_type: &SpecJoinType,
        join_keys: &[JoinKeyPair],
    ) -> Result<DataFrame> {
        let left = self.ctx.table(left_name).await?;
        let right = self.ctx.table(right_name).await?;

        let df_join_type = match join_type {
            SpecJoinType::Inner => JoinType::Inner,
            SpecJoinType::Left => JoinType::Left,
            SpecJoinType::Right => JoinType::Right,
            SpecJoinType::Full => JoinType::Full,
            SpecJoinType::Semi => JoinType::LeftSemi,
            SpecJoinType::Anti => JoinType::LeftAnti,
        };

        // Classify join keys: equality vs inequality (span containment)
        let (equality_keys, inequality_keys): (Vec<_>, Vec<_>) = join_keys
            .iter()
            .partition(|k| !is_span_containment_key(k));

        if inequality_keys.is_empty() {
            // Pure equality join — DataFrame::join() (key-based)
            // Produces HashJoinExec at the physical level.
            let left_cols: Vec<&str> = equality_keys.iter()
                .map(|k| k.left_key.as_str()).collect();
            let right_cols: Vec<&str> = equality_keys.iter()
                .map(|k| k.right_key.as_str()).collect();

            left.join(right, df_join_type, &left_cols, &right_cols, None)
        } else {
            // Inequality join — DataFrame::join_on() with predicate expressions
            // Produces NestedLoopJoinExec at physical level.
            let mut predicates: Vec<Expr> = Vec::new();

            for key in &equality_keys {
                predicates.push(
                    col(format!("{left_name}.{}", key.left_key))
                        .eq(col(format!("{right_name}.{}", key.right_key)))
                );
            }

            for key in &inequality_keys {
                let (outer_start, outer_end, inner_start, inner_end) =
                    parse_span_containment_keys(key);
                predicates.push(
                    col(format!("{left_name}.{outer_start}"))
                        .lt_eq(col(format!("{right_name}.{inner_start}")))
                );
                predicates.push(
                    col(format!("{right_name}.{inner_end}"))
                        .lt_eq(col(format!("{left_name}.{outer_end}")))
                );
            }

            left.join_on(right, df_join_type, predicates)
        }
    }
}
```

### 8.6 Union Builder (Schema-Drift-Safe)

```rust
// rust/codeanatomy_engine/src/compiler/union_builder.rs

use datafusion::prelude::*;

impl<'a> SemanticPlanCompiler<'a> {
    /// Build a union view from multiple sources.
    ///
    /// Uses Rust-only DataFrame::union_by_name() for schema-drift safety.
    /// Aligns columns by name, fills missing columns with NULL.
    ///
    /// Best-in-class improvement: also supports union_by_name_distinct
    /// for cases where deduplication is needed. The variant selection
    /// is driven by the spec (distinct flag in Union transform).
    pub async fn build_union(
        &self,
        sources: &[String],
        discriminator_column: &Option<String>,
        distinct: bool,
    ) -> Result<DataFrame> {
        if sources.is_empty() {
            return Err(DataFusionError::Plan("Union requires at least one source".into()));
        }

        let first = self.ctx.table(&sources[0]).await?;
        let mut result = if let Some(disc_col) = discriminator_column {
            first.with_column(disc_col, lit(&sources[0]))?
        } else {
            first
        };

        for source_name in &sources[1..] {
            let mut next = self.ctx.table(source_name).await?;
            if let Some(disc_col) = discriminator_column {
                next = next.with_column(disc_col, lit(source_name))?;
            }
            // Rust-only API: aligns by column name, fills missing with NULL.
            // Handles schema drift safely across evolving extraction outputs.
            result = if distinct {
                result.union_by_name_distinct(next)?
            } else {
                result.union_by_name(next)?
            };
        }

        Ok(result)
    }
}
```

### 8.7 Cost-Aware Cache Boundary Insertion (P1 Improvement #3)

```rust
// rust/codeanatomy_engine/src/compiler/cache_boundaries.rs

use std::collections::HashMap;

/// Cache decision inputs (P1 improvement #3):
/// Replace fixed "≥3 consumers" heuristic with cost-aware boundary insertion.
#[derive(Debug)]
struct CacheCandidateMetrics {
    /// Number of downstream consumers
    fanout: usize,
    /// Whether the view involves joins (higher recomputation cost)
    involves_join: bool,
    /// Whether the view involves aggregations (higher recomputation cost)
    involves_aggregation: bool,
}

impl<'a> SemanticPlanCompiler<'a> {
    /// Insert cost-aware cache boundaries for shared subplans.
    ///
    /// P1 improvement #3: Replace fixed ≥3 threshold with cost-aware insertion.
    ///
    /// Cache decision considers:
    /// - fanout count (how many downstream views consume this)
    /// - transform complexity (join/aggregate have higher recomputation cost)
    /// - Delta materialization for medium/large intermediates (P1 improvement #4)
    ///
    /// Threshold logic:
    /// - fanout ≥ 3: always cache (high reuse benefit)
    /// - fanout == 2 AND (join OR aggregate): cache (expensive to recompute)
    /// - fanout == 2 AND simple (filter/project): skip (cheap to recompute)
    /// - fanout == 1: never cache
    pub async fn insert_cache_boundaries(&self) -> Result<()> {
        // Count downstream references for each view
        let mut ref_counts: HashMap<&str, usize> = HashMap::new();
        for view in &self.spec.view_definitions {
            for dep in &view.view_dependencies {
                *ref_counts.entry(dep.as_str()).or_default() += 1;
            }
        }

        // Classify view transforms for cost estimation
        let view_transform_map: HashMap<&str, &ViewTransform> = self.spec.view_definitions
            .iter()
            .map(|v| (v.name.as_str(), &v.transform))
            .collect();

        for (view_name, &fanout) in &ref_counts {
            let should_cache = match fanout {
                0 | 1 => false,
                2 => {
                    // Only cache fanout-2 if the transform is expensive
                    if let Some(transform) = view_transform_map.get(view_name) {
                        is_expensive_transform(transform)
                    } else {
                        false
                    }
                }
                _ => true, // fanout ≥ 3: always cache
            };

            if should_cache {
                let df = self.ctx.table(view_name).await?;
                // cache() is eager: executes the plan and stores results in memory.
                // After this, the view name resolves to a MemTable scan.
                // cache AFTER filter/projection to reduce cached data volume.
                let cached = df.cache().await?;
                // Re-register as the cached version
                // P0 correction #5: into_view() + register_table()
                // Replace existing registration deterministically.
                let _ = self.ctx.deregister_table(view_name)?;
                let view = cached.into_view();
                self.ctx.register_table(view_name, view)?;
            }
        }

        Ok(())
    }
}

/// Classify whether a ViewTransform is expensive to recompute.
fn is_expensive_transform(transform: &ViewTransform) -> bool {
    matches!(
        transform,
        ViewTransform::Relate { .. } | ViewTransform::Aggregate { .. }
    )
}
```

### 8.8 Parameterized Plan Templates (P1 Improvement #6)

```rust
// rust/codeanatomy_engine/src/compiler/plan_compiler.rs (continued)

impl<'a> SemanticPlanCompiler<'a> {
    /// Build a parameterized plan template for stable plan shapes (v1 section 7.2).
    ///
    /// P1 improvement #6: Standardize when to use SQL prepared forms
    /// vs DataFrame with_param_values:
    ///
    /// - SQL PREPARE/EXECUTE: when the template is expressed as SQL text
    ///   and reused across multiple scalar rebinds
    /// - DataFrame placeholder() + with_param_values: when the template is
    ///   built programmatically and the plan shape must be stable
    ///
    /// Plan shape is stable: optimizer has already optimized the parameterized shape.
    /// Only the parameter value changes per execution.
    pub async fn build_parameterized_template(
        &self,
        base_table: &str,
        filter_column: &str,
    ) -> Result<DataFrame> {
        self.ctx.table(base_table).await?
            .filter(col(filter_column).eq(placeholder("$1")))
    }

    /// Execute a parameterized template with a specific value.
    pub async fn execute_template(
        template: &DataFrame,
        value: ScalarValue,
    ) -> Result<Vec<RecordBatch>> {
        template.clone()
            .with_param_values(ParamValues::List(vec![value]))?
            .collect()
            .await
    }
}
```

### 8.9 Plan Verification via EXPLAIN (P0 Correction #6)

```rust
// rust/codeanatomy_engine/src/compiler/plan_compiler.rs (continued)

impl<'a> SemanticPlanCompiler<'a> {
    /// Validate the compiled plan DAG before execution.
    ///
    /// Uses EXPLAIN to verify plan structure:
    /// - P0 (unoptimized logical): validates binding, type coercion, operator placement
    /// - P1 (optimized logical): validates pushdowns, simplifications
    /// - P2 (physical): validates operator selection, parallelism topology
    ///
    /// P0 correction #6: use df.create_physical_plan().await? (NOT execution_plan())
    pub async fn validate_plan(&self, df: &DataFrame) -> Result<PlanValidation> {
        // P0: unoptimized logical plan
        let p0 = df.logical_plan();

        // P1: optimized logical plan
        let p1 = df.optimized_logical_plan();

        // P2: physical plan — correct DF51 Rust API
        let p2 = df.create_physical_plan().await?;

        // EXPLAIN VERBOSE for rule-by-rule traceability
        let explain = df.explain(true, false)?.collect().await?;

        Ok(PlanValidation {
            p0_schema: p0.schema().clone(),
            p1_schema: p1.schema().clone(),
            p2_partition_count: p2.output_partitioning().partition_count(),
            explain_rows: explain,
        })
    }
}
```

### 8.10 Implementation Checklist

- [ ] Implement `validate_graph()` with cycle detection, dependency resolution, duplicate check (WS4.5)
- [ ] Implement topological sort with view-to-view-only edges and explicit cycle diagnostics (P0 #9)
- [ ] Implement `compile_view()` dispatcher for each `ViewTransform` variant
- [ ] Implement normalize view builder using existing UDFs from `datafusion_ext`
- [ ] Implement relate (join) builder with two strategies:
  - [ ] Equality join via `DataFrame::join()` (key-based, produces HashJoinExec)
  - [ ] Inequality join via `DataFrame::join_on()` (span containment predicates)
- [ ] Implement union builder via `DataFrame::union_by_name()` / `union_by_name_distinct()` driven by `ViewTransform::Union.distinct`
- [ ] Implement project builder (output schema enforcement via `select()`)
- [ ] Implement filter builder (SQL expression parsing via `ctx.parse_sql_expr()`)
- [ ] Implement aggregate builder
- [ ] Implement cost-aware cache boundary insertion (P1 #3) replacing fixed ≥3 threshold
- [ ] Implement parameterized plan template support (`placeholder()` + `with_param_values()`)
- [ ] Use `df.into_view()` + `ctx.register_table()` for view registration (P0 #5)
- [ ] Use `df.create_physical_plan().await?` for physical plan retrieval (P0 #6)
- [ ] Test: full semantic spec compiles to connected LogicalPlan DAG
- [ ] Test: all view transform variants produce valid DataFrames
- [ ] Test: cycle detection fails fast with descriptive error
- [ ] Test: unresolved dependencies fail fast with descriptive error
- [ ] Test: join keys produce correct physical join operators (verify via EXPLAIN)
- [ ] Test: `union_by_name` handles schema drift (missing columns → NULL)
- [ ] Test: cost-aware cache reduces scan count for shared expensive subplans
- [ ] Test: parameterized templates preserve plan shape across different values

---

## 9) WS5: Rule Intent Compiler and Deterministic Rulepack Fingerprinting

Policy is encoded as DataFusion planner rules. The intent compiler translates semantic-model `RuleIntent` declarations into concrete rule instances.

### 9.1 Representative Rule: OptimizerRule using TreeNode API

```rust
// rust/codeanatomy_engine/src/rules/optimizer.rs

use datafusion::logical_expr::{LogicalPlan, OptimizerRule, OptimizerConfig};
use datafusion::common::tree_node::Transformed;

/// Rewrite redundant span containment predicates into UDF calls.
///
/// Detects:   a.bstart <= b.bstart AND b.bend <= a.bend
/// Rewrites:  byte_span_contains(a.bstart, a.bend, b.bstart, b.bend)
///
/// Uses DataFusion's TreeNode API for plan rewriting:
/// - transform_down: visit from root toward leaves
/// - Transformed::yes/no: signal whether the node was modified
///
/// Visible in EXPLAIN VERBOSE output as "span_containment_rewrite".
#[derive(Debug)]
pub struct SpanContainmentRewriteRule;

impl OptimizerRule for SpanContainmentRewriteRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|node| {
            match node {
                LogicalPlan::Filter(filter) => {
                    if let Some(rewritten) = try_rewrite_span_containment(&filter.predicate) {
                        let new_filter = Filter::try_new(rewritten, filter.input.clone())?;
                        Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
                    } else {
                        Ok(Transformed::no(LogicalPlan::Filter(filter)))
                    }
                }
                other => Ok(Transformed::no(other)),
            }
        })
    }

    fn name(&self) -> &str { "span_containment_rewrite" }
}
```

### 9.2 Representative Rule: PhysicalOptimizerRule (Best-in-Class)

```rust
// rust/codeanatomy_engine/src/rules/physical.rs

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;

/// CPG-specific physical optimization rule.
///
/// Extends existing CodeAnatomyPhysicalRule from datafusion_ext
/// with additional CPG-specific optimizations.
///
/// Best-in-class improvement: uses the `schema_check()` method to
/// enable DataFusion's built-in schema validation after rewrite,
/// catching bugs where a physical rewrite silently changes output schema.
#[derive(Debug, Default)]
pub struct CpgPhysicalRule {
    /// Whether to enable batch coalescing after selective filters
    pub coalesce_after_filter: bool,
    /// Memory budget hint for hash join build sides
    pub hash_join_memory_hint: Option<usize>,
}

impl PhysicalOptimizerRule for CpgPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Delegate to existing CodeAnatomyPhysicalRule for base optimizations,
        // then apply CPG-specific rewrites
        let base_optimized = datafusion_ext::physical_rules::CodeAnatomyPhysicalRule::default()
            .optimize(plan, config)?;

        // Additional CPG-specific physical optimizations here
        Ok(base_optimized)
    }

    fn name(&self) -> &str { "cpg_physical_rule" }

    /// Enable schema validation after rewrite (best-in-class)
    fn schema_check(&self) -> bool { true }
}
```

### 9.3 Implementation Checklist

- [ ] Implement intent compiler mapping RuleIntents → concrete rule instances
- [ ] Implement semantic integrity AnalyzerRules (required columns, schema contracts)
- [ ] Implement Delta-scan-aware OptimizerRules (preserve pushdown opportunities)
- [ ] Implement cost-shape PhysicalOptimizerRules (repartition strategy, memory budget)
- [ ] Implement safety AnalyzerRules (reject non-deterministic constructs)
- [ ] Integrate with existing `datafusion_ext::planner_rules::CodeAnatomyPolicyRule`
- [ ] Integrate with existing `datafusion_ext::physical_rules::CodeAnatomyPhysicalRule`
- [ ] Use TreeNode API (transform_down/transform_up) for plan rewriting
- [ ] Use `schema_check() -> true` for post-rewrite schema validation
- [ ] Test: `EXPLAIN VERBOSE` shows all custom rules by name in output
- [ ] Test: same spec + same profile → same rulepack fingerprint
- [ ] Test: rule registration order is deterministic across runs

---

## 10) WS6: Execution Engine (P0 Correction #7)

Stream-first execution + CPG materialization via Delta write path.

### 10.1 Key Architectural Elements

```rust
// rust/codeanatomy_engine/src/executor/runner.rs

use datafusion::prelude::*;
use crate::spec::outputs::{OutputTarget, MaterializationMode};

/// Execute compiled output plans and materialize results to Delta tables.
///
/// Stream-first: uses execute_stream_partitioned for memory-efficient
/// streaming execution. Results are written to Delta via insert_into
/// (DeltaTableProvider supports Append and Overwrite).
///
/// P0 correction #7: write_table().await? returns Vec<RecordBatch> directly.
/// Do NOT chain .collect().await? after write_table — that's double-await.
pub async fn execute_and_materialize(
    ctx: &SessionContext,
    output_plans: Vec<(OutputTarget, DataFrame)>,
) -> Result<Vec<MaterializationResult>> {
    let mut results = Vec::new();

    for (target, df) in output_plans {
        // Get partition count from physical plan for diagnostics
        // P0 correction #6: use create_physical_plan(), NOT execution_plan()
        let plan = df.create_physical_plan().await?;
        let partition_count = plan.output_partitioning().partition_count();
        let insert_op = match target.materialization_mode {
            MaterializationMode::Append => InsertOp::Append,
            MaterializationMode::Overwrite => InsertOp::Overwrite,
        };

        // P0 correction #7: write_table returns Vec<RecordBatch> directly.
        // This is the terminal operation — no .collect() afterward.
        let write_result = df.clone()
            .write_table(
                &target.table_name,
                DataFrameWriteOptions::new()
                    .with_insert_op(insert_op),
            )
            .await?;
        // write_result IS Vec<RecordBatch> — already collected

        results.push(MaterializationResult {
            table_name: target.table_name,
            rows_written: extract_row_count(&write_result),
            partition_count: partition_count as u32,
        });
    }

    Ok(results)
}
```

```rust
// rust/codeanatomy_engine/src/executor/result.rs

/// Compact result envelope (v1 section 19, step 10).
///
/// Contains both layers of the determinism contract:
/// - spec_hash (Layer A): what was computed
/// - envelope_hash (Layer B): in what environment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub outputs: Vec<MaterializationResult>,
    pub spec_hash: [u8; 32],
    pub envelope_hash: [u8; 32],
    pub rulepack_fingerprint: [u8; 32],
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationResult {
    pub table_name: String,
    pub rows_written: u64,
    pub partition_count: u32,
}
```

### 10.2 Implementation Checklist

- [ ] Implement `execute_and_materialize()` using correct `write_table()` semantics (no double-await)
- [ ] Use `create_physical_plan()` not `execution_plan()` for plan inspection
- [ ] Map `OutputTarget.materialization_mode` to `InsertOp::{Append, Overwrite}`
- [ ] Implement Delta output table creation (`open_or_create_delta_table()`)
- [ ] Implement schema enforcement on write path (reject schema mismatches)
- [ ] Implement `RunResult` envelope assembly with both determinism layers
- [ ] Test: end-to-end extraction Delta → compilation → execution → CPG Delta output
- [ ] Test: schema enforcement rejects mismatched writes
- [ ] Test: `RunResult` contains valid spec_hash + envelope_hash + rulepack_fingerprint

---

## 11) WS6.5: Join/Union Stability Harness (New)

Targeted performance and correctness tests for the most complex plan combination patterns.

### 11.1 Test Categories

```rust
// rust/codeanatomy_engine/tests/join_union_stability.rs

/// WS6.5: Join/Union Stability Harness
///
/// Validates:
/// 1. Inequality joins (span containment) vs equality joins produce correct results
/// 2. union_by_name handles schema drift (missing columns → NULL, extra columns → included)
/// 3. Repartition/union optimizer toggles produce correct results under all combinations
/// 4. Large fan-in unions (>10 sources) don't explode partition count
/// 5. Mixed equality + inequality join predicates produce correct results

#[tokio::test]
async fn test_span_containment_join_correctness() {
    // Verify: outer.bstart <= inner.bstart AND inner.bend <= outer.bend
    // produces correct containment semantics (inner fully within outer)
}

#[tokio::test]
async fn test_equality_join_produces_hash_join_exec() {
    // Verify via EXPLAIN: equality-only joins produce HashJoinExec
}

#[tokio::test]
async fn test_inequality_join_produces_nested_loop() {
    // Verify via EXPLAIN: inequality joins produce NestedLoopJoinExec
}

#[tokio::test]
async fn test_union_by_name_schema_drift() {
    // Source A: {id, name, score}
    // Source B: {id, name, extra_col}
    // Result: {id, name, score, extra_col} with NULLs filled
}

#[tokio::test]
async fn test_large_fanin_union_partition_count() {
    // 15 sources → verify partition count stays bounded
    // (InterleaveExec vs UnionExec behavior)
}

#[tokio::test]
async fn test_repartition_toggle_stability() {
    // Run with repartition_joins=true/false
    // Verify results are identical (correctness), only perf differs
}
```

### 11.2 Implementation Checklist

- [ ] Implement span containment join correctness tests
- [ ] Implement join operator verification via EXPLAIN
- [ ] Implement union_by_name schema drift tests
- [ ] Implement large fan-in union partition count bounds tests
- [ ] Implement repartition toggle stability tests
- [ ] Implement mixed equality+inequality join predicate tests

---

## 12) WS7: Adaptive Tuner (Tightened Guardrails)

Bounded auto-tuning with narrow scope. P1/P2 scope addition: explicit rollback policy, never mutate correctness-affecting options, tune only bounded execution knobs.

### 12.1 Key Architectural Elements

```rust
// rust/codeanatomy_engine/src/tuner/adaptive.rs

/// Bounded auto-tuner (v1 section 9.3-9.4).
///
/// P1/P2 tightened guardrails:
/// - Explicit rollback policy on regressions
/// - NEVER mutate correctness-affecting options in auto-tuner
/// - Tune ONLY bounded execution knobs (listed below)
///
/// Tunable knobs (bounded):
/// - target_partitions (within ±25% of profile default)
/// - batch_size (within 1024..65536)
/// - repartition toggles (joins, aggregations)
///
/// Non-tunable (correctness-affecting, never touched):
/// - optimizer rule set
/// - analyzer rules
/// - pushdown filters
/// - ident normalization
/// - parquet pruning
/// - enable_recursive_ctes
#[derive(Debug, Clone)]
pub struct AdaptiveTuner {
    mode: TunerMode,
    current_config: TunerConfig,
    stable_config: TunerConfig,
    observation_count: u32,
    stability_window: u32,
    /// Lower bound for target_partitions (never go below)
    partitions_floor: u32,
    /// Upper bound for target_partitions (never go above)
    partitions_ceiling: u32,
}

impl AdaptiveTuner {
    pub fn new(initial_config: TunerConfig, profile: &EnvironmentProfile) -> Self {
        let base = initial_config.target_partitions;
        Self {
            mode: TunerMode::ObserveOnly,
            current_config: initial_config.clone(),
            stable_config: initial_config,
            observation_count: 0,
            stability_window: 5,
            partitions_floor: (base * 3 / 4).max(2),
            partitions_ceiling: base * 5 / 4,
        }
    }

    /// Observe execution metrics and optionally propose adjusted config.
    ///
    /// Guardrail: any proposed change that regresses performance by >2x
    /// is automatically rolled back to the last stable config.
    pub fn observe(&mut self, metrics: &ExecutionMetrics) -> Option<TunerConfig> {
        self.observation_count += 1;

        if self.observation_count < self.stability_window {
            return None;
        }
        self.mode = TunerMode::BoundedAdapt;

        // Regression check FIRST: rollback if worse
        if self.is_regression(metrics) {
            let rollback = self.stable_config.clone();
            self.current_config = rollback.clone();
            return Some(rollback);
        }

        let mut proposed = self.current_config.clone();

        // Bounded adjustment: reduce parallelism on spill pressure
        if metrics.spill_count > 0 && proposed.target_partitions > self.partitions_floor {
            proposed.target_partitions =
                (proposed.target_partitions * 3 / 4).max(self.partitions_floor);
        }

        // Bounded adjustment: reduce batch size on very selective scans
        if metrics.scan_selectivity < 0.1 {
            proposed.batch_size = (proposed.batch_size / 2).max(1024);
        }

        // Accept adjustment, promote current to stable
        self.stable_config = self.current_config.clone();
        self.current_config = proposed.clone();
        Some(proposed)
    }
}
```

### 12.2 Implementation Checklist

- [ ] Implement `AdaptiveTuner` with observe-only initial mode
- [ ] Implement bounded `target_partitions` adjustment (within floor..ceiling, never >±25%)
- [ ] Implement bounded `batch_size` adjustment (1024..65536)
- [ ] Implement explicit regression detection and automatic rollback
- [ ] Enforce: never mutate correctness-affecting options (optimizer rules, pushdown, etc.)
- [ ] Test: tuner stays in observe-only for first N runs
- [ ] Test: bounded adjustments never exceed floor/ceiling limits
- [ ] Test: regression triggers rollback to stable config
- [ ] Test: correctness-affecting knobs are never modified

---

## 13) WS9: Python API Shell (Tightened Boundary)

Thin Python facade. Python owns CLI/API ergonomics and semantic spec submission. **Rust owns everything else** (P1/P2 tightened: keep Python API to submit spec, run, and read compact result envelope. Keep planner/optimizer/rule machinery Rust-only).

### 13.1 PyO3 Bindings (Rust Side)

```rust
// rust/codeanatomy_engine/src/python/session.rs

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use crate::session::profiles::{EnvironmentClass, EnvironmentProfile};

#[pyclass]
pub struct SessionFactory {
    inner: crate::session::factory::SessionFactory,
}

#[pymethods]
impl SessionFactory {
    #[new]
    fn new(profile_json: &str) -> PyResult<Self> {
        let profile: EnvironmentProfile = serde_json::from_str(profile_json)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self {
            inner: crate::session::factory::SessionFactory::new(profile),
        })
    }

    #[staticmethod]
    fn from_class(environment_class: &str) -> PyResult<Self> {
        let class = match environment_class {
            "small" => EnvironmentClass::Small,
            "medium" => EnvironmentClass::Medium,
            "large" => EnvironmentClass::Large,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unknown environment_class '{other}' (expected small|medium|large)"
                )))
            }
        };
        let profile = EnvironmentProfile::from_class(class);
        Ok(Self {
            inner: crate::session::factory::SessionFactory::new(profile),
        })
    }
}
```

### 13.2 Python Facade

```python
# src/engine/facade.py

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from engine.spec_builder import SemanticExecutionSpec


def execute_cpg_build(
    extraction_inputs: dict[str, str],
    semantic_spec: SemanticExecutionSpec,
    environment_class: str = "medium",
) -> dict[str, object]:
    """Single-call entry point from Python to Rust engine.

    This is the thin Python shell (v1 section 4.2).
    Python submits spec + inputs; Rust does all planning, optimization,
    and execution; Python reads the compact result envelope.

    Parameters
    ----------
    extraction_inputs
        Map of logical_name -> Delta location for extraction outputs.
    semantic_spec
        The compiled semantic execution spec.
    environment_class
        One of "small", "medium", "large".

    Returns
    -------
    dict
        RunResult with outputs, metrics, and fingerprints.
    """
    from codeanatomy_engine import (
        SessionFactory,
        SemanticPlanCompiler,
        CpgMaterializer,
    )

    spec_json = msgspec.json.encode(semantic_spec).decode()

    factory = SessionFactory.from_class(environment_class)
    session = factory.build_session(spec_json)
    session.register_inputs(extraction_inputs)

    compiler = SemanticPlanCompiler(session)
    compiled = compiler.compile(spec_json)

    materializer = CpgMaterializer(session)
    result = materializer.execute(compiled)

    return msgspec.json.decode(result.to_json())
```

### 13.3 Implementation Checklist

- [ ] Implement `#[pymodule]` entry point in `lib.rs`
- [ ] Implement `#[pyclass] SessionFactory` with `new(profile_json)` and `from_class(environment_class)`
- [ ] Implement `#[pyclass] SemanticPlanCompiler` wrapping Rust compiler
- [ ] Implement `#[pyclass] CpgMaterializer` wrapping Rust executor
- [ ] Implement `#[pyclass] RunResult` with `.to_json()` / `.to_dict()`
- [ ] Implement `execute_cpg_build()` thin facade in Python
- [ ] Configure maturin build with `abi3-py313` for stable ABI
- [ ] Add `codeanatomy_engine` build step to `scripts/rebuild_rust_artifacts.sh`
- [ ] Test: full pipeline callable from Python via `uv run`
- [ ] Test: `RunResult` serializable and inspectable from Python

---

## 14) WS10: Optional Compliance Profile Modules (P1 Improvement #7)

Observability/compliance as optional profile modules, not core runtime obligations. Core profile: max throughput. Strict/replay profile: include explain/rule traces and extended snapshots.

### 14.1 Implementation Checklist

- [ ] Implement compliance as a Cargo feature flag (`features = ["compliance"]`)
- [ ] Core profile (default): zero overhead, no trace capture
- [ ] Strict/Replay profile: `EXPLAIN VERBOSE` capture, per-rule impact digest
- [ ] Implement effective SessionConfig snapshot serialization
- [ ] Implement effective rulepack snapshot (rule IDs, order, activation profile)
- [ ] Implement compact run envelope serialization (commit summary: table/version/rows/files)
- [ ] Implement retention controls (short default, long for regulatory)
- [ ] Test: compliance disabled has zero runtime overhead (no allocation, no I/O)
- [ ] Test: when enabled, all registered rules appear in trace output by name

---

## 15) Cargo.toml for `codeanatomy_engine`

```toml
[package]
name = "codeanatomy-engine"
version = "0.1.0"
edition = "2021"

[lib]
name = "codeanatomy_engine"
crate-type = ["cdylib", "rlib"]

[dependencies]
# Workspace dependencies (pinned versions)
datafusion = { workspace = true, features = ["sql"] }
datafusion-common = { workspace = true }
datafusion-expr = { workspace = true }
datafusion-execution = { workspace = true }
arrow = { workspace = true }
pyo3 = { workspace = true, features = ["extension-module"] }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true }

# Local workspace crate — existing UDFs, Delta control plane, rules
datafusion_ext = { path = "../datafusion_ext" }

# Engine-specific dependencies
deltalake = { version = "0.30.1", features = ["datafusion"] }
datafusion-proto = "51.0.0"
datafusion-sql = { version = "51.0.0", features = ["unparser"] }
blake3 = "1"
uuid = { version = "1", features = ["v4"] }
chrono = { version = "0.4", features = ["serde"] }

[dev-dependencies]
tempfile = "3"

[features]
default = []
compliance = [] # Enable compliance/audit profile modules
```

---

## 16) Delivery Phases with Exit Criteria (Revised Order)

### Phase 1: API Conformance + Deterministic Core (WS0 + WS1 + WS2/WS2.5)

**Target:** Compile-checked APIs, deterministic session, immutable rulepack lifecycle.

**Exit criteria:**
- [ ] All WS0 smoke tests compile and pass against DF 51.0.0 / DL 0.30.1
- [ ] No usage of unsupported APIs (`register_view`, `execution_plan`, `remove_optimizer_rule`, `add_physical_optimizer_rule`, invalid `RuntimeEnvBuilder` methods, `set(&str)`)
- [ ] `SemanticExecutionSpec` round-trips through serde with canonical BLAKE3 hash (sorted-key JSON, not raw msgpack)
- [ ] Two-layer determinism contract implemented (spec_hash + envelope_hash)
- [ ] `SessionContext` construction is deterministic (same inputs = same `envelope_hash`)
- [ ] Rulepack profiles produce immutable RuleSets via SessionStateBuilder
- [ ] `cargo test` passes for compat, spec, session, and rulepack modules

### Phase 2: Semantic Compilation (WS3 + WS4/WS4.5 + WS5)

**Target:** Full semantic spec → single LogicalPlan DAG → rule-applied plan.

**Exit criteria:**
- [ ] All extraction outputs register as native Delta providers via existing control plane (no reimplementation)
- [ ] Existing ~30 UDFs from `datafusion_ext` registered and callable
- [ ] Two-tier pruning verified via `EXPLAIN` (Delta file skip + Parquet row-group)
- [ ] Complete semantic spec compiles to connected LogicalPlan DAG
- [ ] All view transform variants produce valid plans
- [ ] Topological sort uses view-to-view-only edges with explicit cycle detection
- [ ] Graph validation catches unresolved dependencies and duplicate view names
- [ ] Equality and inequality joins both work (verified via EXPLAIN showing correct operators)
- [ ] `union_by_name` handles schema drift correctly
- [ ] Cost-aware cache boundaries reduce scan count for shared expensive subplans
- [ ] Rules registered and visible in `EXPLAIN VERBOSE` with per-rule effects

### Phase 3: End-to-End Execution (WS6 + WS6.5 + WS9)

**Target:** Extraction Delta → compilation → execution → CPG Delta.

**Exit criteria:**
- [ ] Full pipeline callable from Python via `execute_cpg_build()`
- [ ] CPG Delta outputs written with correct schema via `insert_into`
- [ ] `write_table` used with correct semantics (no double-await)
- [ ] `RunResult` includes both spec_hash + envelope_hash
- [ ] Join/union stability harness passes (inequality joins, schema drift, repartition toggles)
- [ ] Deterministic replay validated with `(spec_hash, envelope_hash)` pair
- [ ] Maturin wheel builds and installs for development (`maturin develop`)

### Phase 4: Optimization + Governance (WS7 + WS8 + WS10)

**Target:** Auto-tuning with guardrails, schema utilities, optional compliance.

**Exit criteria:**
- [ ] Adaptive tuner adjusts partitions/batch size within bounded limits
- [ ] Tuner never mutates correctness-affecting options
- [ ] Regression triggers automatic rollback to stable config
- [ ] Schema diff utility detects additive evolution
- [ ] Compliance pack is zero-overhead when disabled
- [ ] Throughput and memory behavior validated on representative multi-branch joins/unions
- [ ] Core runtime succeeds without custom policy/protocol/artifact services (v1 acceptance criterion 7)

---

## 17) Deterministic Execution Algorithm (Reference — Updated)

```
 1. Python: Build SemanticExecutionSpec from semantic IR + input locations
    → src/engine/spec_builder.py: build_spec_from_ir()
    → Compute spec_hash via canonical JSON + BLAKE3 (Layer A)

 2. Python: Call execute_cpg_build(inputs, spec, profile)
    → src/engine/facade.py

 3. Rust: RulepackFactory::build_ruleset(profile, intents, env_profile)
    → codeanatomy_engine::rules::rulepack
    → Produces: immutable CpgRuleSet (fingerprinted)

 4. Rust: SessionFactory::build_session(profile, rule_set)
    → codeanatomy_engine::session::factory
    → Uses SessionStateBuilder with pre-registered rules
    → Produces: SessionContext + SessionEnvelope (Layer B)

 5. Rust: register_extraction_inputs(ctx, spec.input_relations)
    → codeanatomy_engine::providers::registration
    → Delegates to existing delta_control_plane::delta_provider_from_session()
    → Produces: Vec<TableRegistration> (Delta-native only)

 6. Rust: SemanticPlanCompiler::compile(ctx, spec)
    → codeanatomy_engine::compiler::plan_compiler
    → validate_graph() → topological_sort() → compile views
    → into_view() + register_table() → cost-aware cache boundaries
    → Produces: Vec<(OutputTarget, DataFrame)>

 7. Rust: validate_plan(df) [optional, compliance profile only]
    → Capture P0/P1/P2 + EXPLAIN VERBOSE for diagnostics

 8. Rust: execute_and_materialize(ctx, output_plans)
    → codeanatomy_engine::executor::runner
    → write_table().await? (terminal, no double-await)
    → Produces: Vec<MaterializationResult>

 9. Rust: Assemble RunResult
    → spec_hash + envelope_hash + rulepack_fingerprint + outputs + timing

10. Python: Return RunResult to caller
```

---

## 18) Definition of Done (Updated)

1. All plan snippets compile against pinned DataFusion 51 / DeltaLake 0.30 (WS0 gate).
2. No usage of unsupported APIs (`register_view`, `execution_plan`, `remove_optimizer_rule`, `add_physical_optimizer_rule`, invalid `RuntimeEnvBuilder` methods, `set(&str)`, `env!("DATAFUSION_VERSION")`).
3. Deterministic replay validated with `(spec_hash, envelope_hash)` pair (two-layer contract).
4. Delta provider path is native-only and reuses existing control-plane implementation.
5. Topological compiler fails fast on cycles and unresolved view dependencies.
6. Throughput and memory behavior are validated on representative multi-branch joins/unions (WS6.5 stability harness).

---

## 19) Best-in-Class Improvements Summary

Beyond integrating the review feedback, this v2 plan adds the following improvements derived from the DataFusion/DeltaLake Rust reference materials:

1. **`planning_concurrency` config** — Set `execution.planning_concurrency` equal to `target_partitions` to enable parallel planning of UNION children, which materially reduces compile time for our many-input union graphs.

2. **`InterleaveExec` awareness** — When combining many union branches with compatible hash-partitioning, DataFusion can use `InterleaveExec` instead of `UnionExec` for better downstream locality. The `prefer_existing_union` config knob controls this. We set it explicitly.

3. **`schema_check()` on PhysicalOptimizerRules** — Our custom physical optimizer rules return `schema_check() -> true`, enabling DataFusion's built-in schema validation after physical rewrites. This catches bugs where a physical rewrite silently changes output schema.

4. **`DeltaScanConfig::new_from_session()`** — Instead of building DeltaScanConfig manually, we use the session-derived constructor that inherits all relevant config knobs automatically. This is exactly what our existing `scan_config_from_session()` does.

5. **`union_by_name_distinct`** — In addition to `union_by_name`, we expose the distinct variant for cases where deduplication across union branches is needed. This is a Rust-only API not available in Python bindings.

6. **Canonical JSON for spec hashing** — Instead of relying on msgpack (which doesn't guarantee key ordering), we canonicalize into an explicit recursive `CanonicalValue` representation backed by `BTreeMap` for map ordering, then serialize and hash.

7. **`datafusion-proto` for plan serialization** — When compliance/replay profiles need to persist plan artifacts, we use `logical_plan_to_bytes()` / `logical_plan_from_bytes()` for version-locked plan persistence rather than custom serialization.

8. **ScalarUDFImpl advanced hooks** — Our existing UDFs in `datafusion_ext` can be enhanced with `return_field_from_args` (for metadata-aware return types), `simplify` (for constant-folding optimization hooks), and `short_circuits` (for conditional argument evaluation). These are opt-in per-UDF.

9. **Dynamic filter pushdown knobs** — We explicitly set `enable_dynamic_filter_pushdown` and `enable_topk_dynamic_filter_pushdown` in session config to enable runtime filter propagation from build-side hash join values, improving scan pruning for large join graphs.

10. **`plan_to_sql` / Unparser** — For compliance profiles that need human-readable plan summaries, DataFusion's `plan_to_sql()` can convert our compiled LogicalPlan back to SQL for audit logs. This requires the `sql` + `unparser` crate features.

---

## 20) Migration Path

### Step 1: Build alongside (Phase 1-2)
- New Rust engine builds in `rust/codeanatomy_engine/`
- Existing Python pipeline continues operating unchanged
- No code removal during this phase

### Step 2: Shadow execution (Phase 3)
- New engine executes full pipeline in shadow mode alongside existing pipeline
- Compare CPG outputs between old and new for parity validation

### Step 3: Cutover (Phase 3 exit)
- Switch `build_cpg()` in `src/semantics/pipeline.py` to call Rust engine
- Python-side orchestration becomes pass-through

### Step 4: Cleanup (Phase 4)
- Remove: Hamilton pipeline, rustworkx scheduling, policy compiler, compiled policy
- Remove: `_default_semantic_cache_policy()` (replaced by cost-aware caching)
- Auto-tuning and compliance pack become the new operational layer
