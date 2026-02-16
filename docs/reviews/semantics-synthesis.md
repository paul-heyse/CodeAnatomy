# Synthesis: src/semantics/ Design Review

**Date:** 2026-02-16
**Scope:** Full `src/semantics/` module (~23,400 LOC, 75 files)
**Reviews synthesized:** 5 parallel subsystem reviews

## Individual Reviews

| Review | File | LOC | Avg Score |
|--------|------|-----|-----------|
| Core Pipeline & IR | [semantics-core-pipeline.md](semantics-core-pipeline.md) | ~6,800 | ~1.9/3 |
| Types, Schema & Joins | [semantics-types-joins.md](semantics-types-joins.md) | ~2,500 | ~2.3/3 |
| Catalog, Entity & Registry | [semantics-catalog-entity.md](semantics-catalog-entity.md) | ~4,000 | ~2.0/3 |
| Incremental / CDF | [semantics-incremental.md](semantics-incremental.md) | ~3,900 | ~1.7/3 |
| Quality, Normalization & Infra | [semantics-infra-quality.md](semantics-infra-quality.md) | ~6,000 | ~2.0/3 |

---

## Cross-Cutting Themes (Appearing in 3+ Reviews)

### 1. DRY Violations Are Systemic (5/5 reviews, scored 0-1/3)

DRY is the lowest-scoring principle across the entire module. Every subsystem surfaced knowledge duplication:

| Subsystem | DRY Score | Key Duplication |
|-----------|-----------|-----------------|
| Incremental | 0/3 | Duplicate `CdfReadResult` class + `read_cdf_changes` function in two files |
| Core Pipeline | 1/3 | `build_cpg` and `build_cpg_from_inferred_deps` duplicate compile-resolution |
| Types/Joins | 1/3 | Parallel `ColumnType`/`SemanticType` classification systems |
| Catalog | 1/3 | `DatasetRegistrySpec` duplicates all 20 fields of `SemanticDatasetRow` |
| Infrastructure | 1/3 | `_table_exists` in 3 files; `_byte_offset` in 2 files; line-index join in 2 files |

**Root cause:** Organic growth where new code paths were added alongside existing ones rather than refactoring to share common logic.

### 2. God Modules Compress Multiple Concerns (4/5 reviews, scored 1/3 on SRP)

Four subsystems identified files with 3-6 distinct responsibilities:

| File | LOC | Responsibilities |
|------|-----|------------------|
| `pipeline.py` | 2,256 | View orchestration, CDF resolution, output materialization, diagnostics, builders, cache policy |
| `compiler.py` | 1,818 | 10 semantic rules + quality-relationship compilation (564 LOC) + table caching |
| `diagnostics.py` | 773 | Quality metrics, coverage reporting, ambiguity analysis, issue batching, schema anomalies |
| `cdf_runtime.py` | 500 | CDF read, DataFusion registration, telemetry, cursor management, fallback orchestration |
| `analysis_builders.py` | 740 | 14 builders across 6+ analytic domains |

**Root cause:** These files were natural landing zones for related but distinct features, and no fission trigger was identified early.

### 3. SEMANTIC_MODEL Global Singleton Coupling (3/5 reviews)

The `SEMANTIC_MODEL` module-level singleton from `semantics.registry` is directly accessed by `pipeline.py`, `ir_pipeline.py`, and downstream modules, creating:
- Implicit coupling to global mutable state
- Inability to test IR pipeline phases independently
- Hidden composition that bypasses explicit parameter passing

`compile_semantics(model)` already accepts the model as a parameter -- the other 3 IR functions (`optimize_semantics`, `emit_semantics`, `infer_semantics`) should follow this pattern.

### 4. Deep Chain Access / Law of Demeter Violations (3/5 reviews)

Multiple subsystems reach 3-5 levels deep into `DataFusionRuntimeProfile`:

```
runtime.profile.policies.scan_policy           # incremental
runtime.profile.data_sources.cdf_cursor_store  # pipeline
runtime.profile.delta_ops.delta_service()      # incremental
runtime.profile.diagnostics.diagnostics_sink   # incremental
request.runtime_profile.data_sources.semantic_output.cache_overrides  # pipeline
```

**Fix:** Add facade methods on `IncrementalRuntime` and `DataFusionRuntimeProfile` that expose commonly-needed values directly.

### 5. Silent Error Handling / Missing Observability (3/5 reviews)

Multiple locations swallow exceptions without logging:

| Location | Pattern |
|----------|---------|
| `analysis_builders.py:275,536` | `except (RuntimeError, KeyError, ValueError): pass` |
| `cdf_cursors.py:173-176` | Silent `None` return on `DecodeError`/`OSError` |
| `cdf_runtime.py` fallback chains | 3-layer try/except without logging |

The incremental subsystem has zero `logging` calls across 18 modules and no OpenTelemetry spans, unlike the main semantic pipeline.

---

## Principle Heatmap (Aggregated Across All 5 Reviews)

| # | Principle | Avg | Min | Worst Subsystem |
|---|-----------|-----|-----|-----------------|
| 7 | **DRY** | **0.8** | **0** | Incremental (duplicate types) |
| 3 | **SRP** | **1.2** | 1 | All except Types/Joins |
| 2 | **Separation of Concerns** | **1.2** | 1 | Pipeline, Incremental, Infra |
| 11 | **CQS** | **1.6** | 1 | Core Pipeline (compiler.get) |
| 5 | **Dependency Direction** | **1.8** | 1 | Incremental (datafusion→semantics) |
| 6 | **Ports & Adapters** | **1.8** | 1 | Incremental (no Delta port) |
| 1 | **Information Hiding** | **1.8** | 1 | Core Pipeline (_cpg_view_specs) |
| 12 | **DI + Composition** | **1.8** | 1 | Incremental (no injection seams in cdf_runtime) |
| 14 | **Law of Demeter** | **1.8** | 1 | Incremental (5-level chains) |
| 16 | **Functional Core** | **1.8** | 1 | Incremental (IO interspersed in domain) |
| 20 | **YAGNI** | **1.8** | 1 | Infra (metrics.py/stats unused) |
| 21 | **Least Astonishment** | **1.6** | 1 | Pipeline + Incremental |
| 22 | **Public Contracts** | **1.8** | 1 | Core Pipeline (__all__ gaps) |
| 24 | **Observability** | **2.0** | 1 | Catalog (zero logging) |
| 23 | **Testability** | **2.2** | 2 | Catalog/Incremental (cache isolation) |
| 10 | **Illegal States** | **2.0** | 2 | All reasonable |
| 8 | **Design by Contract** | **2.0** | 2 | All reasonable |
| 9 | **Parse, Don't Validate** | **2.0** | 2 | All reasonable |
| 4 | **Cohesion/Coupling** | **2.0** | 2 | All reasonable |
| 15 | **Tell, Don't Ask** | **2.0** | 2 | All reasonable |
| 19 | **KISS** | **2.0** | 2 | All reasonable |
| 13 | **Composition over Inheritance** | **3.0** | 3 | None (perfect across all) |
| 17 | **Idempotency** | **2.8** | 2 | All strong |
| 18 | **Determinism** | **2.8** | 2 | All strong |

**Strengths:** Composition over inheritance (3.0), determinism/idempotency (2.8), and observability in the core pipeline (3.0) are excellent.

**Weaknesses:** DRY (0.8), SRP (1.2), and Separation of Concerns (1.2) need the most attention.

---

## Prioritized Action Plan

### Tier 1: Quick Wins (small effort, immediate impact)

| # | Action | Principles | Files | Effort |
|---|--------|-----------|-------|--------|
| 1 | Rename `_cpg_view_specs` to `cpg_view_specs`, add to `__all__` | P1, P21, P22 | pipeline.py, registry_specs.py | small |
| 2 | Extract `_TABLE_TYPE_GRAIN_MAP` constant in `catalog/tags.py` | P7 | tags.py | small |
| 3 | Add `logger.warning()` to silent exception handlers in `analysis_builders.py` | P24 | analysis_builders.py | small |
| 4 | Standardize `_table_exists` on `table_names_snapshot` | P7, P6 | diagnostics.py, signals.py | small |
| 5 | Rename `SemanticCompiler.get()` to `get_or_register()` | P11, P21 | compiler.py | small |
| 6 | Add `__post_init__` to `JoinStrategy` (non-empty keys, confidence range) | P8, P10 | strategies.py | small |
| 7 | Add `__post_init__` to `SemanticIncrementalConfig` (enabled+state_dir) | P8, P10 | config.py | small |
| 8 | Add `require_unambiguous_spans()` to `SemanticSchema` | P14, P15 | schema.py, compiler.py | small |
| 9 | Remove dead `_builder_for_artifact_spec` | P20 | pipeline.py | small |
| 10 | Add `logger.warning()` for cursor decode failures and CDF fallbacks | P24 | cdf_cursors.py, cdf_runtime.py | small |

### Tier 2: Medium Refactors (medium effort, high impact)

| # | Action | Principles | Scope |
|---|--------|-----------|-------|
| 11 | **Thread `model` parameter through all 4 IR pipeline functions** | P12, P23 | ir_pipeline.py |
| 12 | **Consolidate duplicate `CdfReadResult` types** | P7, P21 | cdf_reader.py, cdf_runtime.py, __init__.py |
| 13 | **Eliminate duplicate compile-resolution in `build_cpg_from_inferred_deps`** | P7 | pipeline.py |
| 14 | **Add `IncrementalRuntime` facade methods** for deep profile access | P14, P1 | runtime.py + 8 call sites |
| 15 | **Extract shared normalization helpers** (_byte_offset + line-index join) | P7 | span_normalize.py, scip_normalize.py |
| 16 | **Extract code-unit join helper** in analysis_builders.py | P7 | analysis_builders.py |
| 17 | **Consolidate `DatasetRegistrySpec` into `SemanticDatasetRow`** | P7, P3 | 6 files in catalog + ir_pipeline.py |
| 18 | **Move `CdfCursorStore` type to shared location** | P5 | Fixes datafusion→semantics dependency inversion |
| 19 | **Audit/remove unused `metrics.py` and `stats/collector.py`** | P20 | ~755 LOC |

### Tier 3: Structural Decomposition (large effort, long-term health)

| # | Action | Principles | Scope |
|---|--------|-----------|-------|
| 20 | **Extract CDF resolution from `pipeline.py`** (~130 LOC) | P2, P3 | New semantics/cdf_resolution.py |
| 21 | **Extract output materialization from `pipeline.py`** (~215 LOC) | P2, P3 | New semantics/output_materialization.py |
| 22 | **Extract diagnostics emission from `pipeline.py`** (~200 LOC) | P2, P3 | New semantics/diagnostics_emission.py |
| 23 | **Split `diagnostics.py` into package** (5 modules) | P2, P3 | diagnostics/ package |
| 24 | **Extract quality-relationship compilation from `compiler.py`** (~564 LOC) | P3 | New semantics/quality_compiler.py |
| 25 | **Unify `ColumnType`/`SemanticType` classification** | P7 | column_types.py, types/core.py |
| 26 | **Introduce `DeltaCdfPort` protocol** | P6, P12 | Incremental subsystem |
| 27 | **Split `analysis_builders.py` by domain** | P3 | 4+ new builder modules |

---

## Summary Statistics

- **Total findings across all reviews:** ~90
- **Quick wins (Tier 1):** 10 items, all small effort
- **Medium refactors (Tier 2):** 9 items
- **Structural decomposition (Tier 3):** 8 items
- **Principles scoring 3/3 everywhere:** Composition over inheritance, near-perfect idempotency and determinism
- **Principle needing most attention:** DRY (avg 0.8/3)
- **Single highest-impact fix:** Consolidate duplicate `CdfReadResult` types (#12) -- eliminates the most confusing name collision
- **Single highest-ROI fix:** Thread `model` parameter through IR pipeline (#11) -- unlocks independent testing of all 4 phases
