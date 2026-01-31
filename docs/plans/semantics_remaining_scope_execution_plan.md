# Semantics Integration: Remaining Scope Execution Plan

**Created:** 2026-01-31  
**Status:** Planning  
**Scope:** Complete the remaining items from `semantics_integration_wiring_plan.md` with targeted, testable implementation steps.

---

## Executive Summary

This plan captures the remaining work required to finish the semantic integration program. The scope items below are the ones still incomplete after the current implementation. Each item includes:

- Representative code snippets (architectural patterns + expected APIs)
- Target file list
- Deprecation/deletion scope after completion
- Implementation checklist

The plan is ordered by dependency and risk:

1) Information‑schema‑driven validation  
2) Runtime cache policy & cache diagnostics  
3) Plan‑fingerprint CI gating  
4) Delta‑first materialization + schema enforcement  
5) CDF incremental merge hardening  
6) Observability/diagnostics artifacts (lineage + cache)  
7) Legacy module removal + alias consolidation

---

## Scope 1: Information‑Schema‑Driven Validation

**Goal:** Replace hardcoded schema checks with catalog‑driven validation using `information_schema`.

### Representative Code Snippets

```python
# src/semantics/validation/catalog_validation.py (NEW)
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class TableColumnSpec:
    table_name: str
    required_columns: tuple[str, ...]


def information_schema_tables(ctx: SessionContext) -> set[str]:
    rows = ctx.table("information_schema.tables").select("table_name").collect()
    return {row[0] for row in rows}


def information_schema_columns(ctx: SessionContext, table_name: str) -> set[str]:
    cols = (
        ctx.table("information_schema.columns")
        .filter(f"table_name = '{table_name}'")
        .select("column_name")
        .collect()
    )
    return {row[0] for row in cols}


def validate_required_columns(ctx: SessionContext, spec: TableColumnSpec) -> None:
    available = information_schema_columns(ctx, spec.table_name)
    missing = [col for col in spec.required_columns if col not in available]
    if missing:
        msg = f"Missing columns for {spec.table_name!r}: {missing!r}"
        raise ValueError(msg)
```

```python
# src/datafusion_engine/views/registry_specs.py (hook)
from semantics.validation.catalog_validation import TableColumnSpec, validate_required_columns

def _validate_semantic_inputs(ctx: SessionContext) -> None:
    required = (
        TableColumnSpec("cst_refs", ("file_id", "path", "span_start", "span_end", "symbol")),
        TableColumnSpec("scip_occurrences_norm_v1", ("file_id", "bstart", "bend", "symbol")),
    )
    for spec in required:
        validate_required_columns(ctx, spec)
```

### Target Files
- `src/semantics/validation/catalog_validation.py` (NEW)
- `src/semantics/pipeline.py` (call validation before building semantic views)
- `src/datafusion_engine/views/registry_specs.py` (pre‑registration validation)

### Deprecation / Deletion
After completion:
- Remove ad‑hoc schema checks that hardcode column names in:
  - `src/datafusion_engine/views/registry.py`
  - `src/semantics/scip_normalize.py` (if duplication exists)

### Implementation Checklist
- [ ] Create `catalog_validation.py` with information_schema queries
- [ ] Wire validation into semantic view registration
- [ ] Add explicit error artifact for schema mismatches
- [ ] Remove redundant hard‑coded schema checks

---

## Scope 2: Runtime Cache Policy + Cache Diagnostics

**Goal:** Make cache configuration explicit and emit cache diagnostics artifacts per run.

### Representative Code Snippets

```python
# src/datafusion_engine/session/cache_policy.py (NEW)
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CachePolicyConfig:
    listing_cache_size: int
    metadata_cache_size: int
    stats_cache_size: int


DEFAULT_CACHE_POLICY = CachePolicyConfig(
    listing_cache_size=10_000,
    metadata_cache_size=10_000,
    stats_cache_size=10_000,
)
```

```python
# src/datafusion_engine/session/runtime.py (hook)
def apply_cache_policy(ctx: SessionContext, policy: CachePolicyConfig) -> None:
    ctx.set_config_option("datafusion.execution.listing_cache_size", str(policy.listing_cache_size))
    ctx.set_config_option("datafusion.execution.metadata_cache_size", str(policy.metadata_cache_size))
    ctx.set_config_option("datafusion.execution.stats_cache_size", str(policy.stats_cache_size))
```

```python
# src/datafusion_engine/views/graph.py (artifact)
record_artifact(runtime_profile, "cache_policy_v1", {
    "listing_cache_size": policy.listing_cache_size,
    "metadata_cache_size": policy.metadata_cache_size,
    "stats_cache_size": policy.stats_cache_size,
})
```

### Target Files
- `src/datafusion_engine/session/cache_policy.py` (NEW)
- `src/datafusion_engine/session/runtime.py` (apply policy)
- `src/datafusion_engine/views/graph.py` (artifact emission)

### Deprecation / Deletion
- None; add new infrastructure, no removal required.

### Implementation Checklist
- [ ] Define `CachePolicyConfig` + defaults
- [ ] Apply cache policy in runtime profile creation
- [ ] Emit `cache_policy_v1` artifact per run
- [ ] Add docs reference in `docs/architecture/semantic_pipeline_graph.md`

---

## Scope 3: Plan‑Fingerprint CI Gating

**Goal:** Use stored plan fingerprints to detect regressions and gate CI.

### Representative Code Snippets

```python
# tools/plan_fingerprint_gate.py (NEW)
from __future__ import annotations

from pathlib import Path
import json


def load_fingerprints(path: Path) -> dict[str, dict[str, str]]:
    return json.loads(path.read_text())


def diff_fingerprints(before: dict[str, dict[str, str]], after: dict[str, dict[str, str]]) -> list[str]:
    changed: list[str] = []
    for view_name, payload in after.items():
        before_payload = before.get(view_name)
        if before_payload is None:
            changed.append(view_name)
            continue
        if before_payload.get("logical_plan_hash") != payload.get("logical_plan_hash"):
            changed.append(view_name)
    return changed
```

```python
# scripts/ci/check_semantic_plan_fingerprints.sh (NEW)
uv run python tools/plan_fingerprint_gate.py \
  --before artifacts/semantic_plan_fingerprints.before.json \
  --after artifacts/semantic_plan_fingerprints.after.json
```

### Target Files
- `tools/plan_fingerprint_gate.py` (NEW)
- `scripts/ci/check_semantic_plan_fingerprints.sh` (NEW)
- CI config (where semantic plan artifacts are produced/consumed)

### Deprecation / Deletion
- None.

### Implementation Checklist
- [ ] Implement diff tool with CLI and deterministic output
- [ ] Capture baseline fingerprint artifact per branch
- [ ] Fail CI on unexpected fingerprint deltas
- [ ] Add documentation in `docs/plans/semantics_integration_wiring_plan.md`

---

## Scope 4: Delta‑First Materialization + Schema Enforcement

**Goal:** Ensure semantic outputs are stored as Delta tables with explicit schema evolution policy and schema hash.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/schema_guard.py (NEW)
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SchemaEvolutionPolicy:
    mode: str  # "strict" | "additive"


def enforce_schema_policy(expected_hash: str, current_hash: str, policy: SchemaEvolutionPolicy) -> None:
    if policy.mode == "strict" and expected_hash != current_hash:
        msg = "Schema hash mismatch for Delta write."
        raise ValueError(msg)
```

```python
# src/semantics/pipeline.py (hook)
def _materialize_semantic_view(
    ctx: SessionContext,
    *,
    view_name: str,
    df: DataFrame,
    delta_location: str,
    schema_policy: SchemaEvolutionPolicy,
) -> None:
    fingerprint = compute_plan_fingerprint(df, view_name=view_name, ctx=ctx)
    enforce_schema_policy(
        expected_hash=fingerprint.schema_hash or "",
        current_hash=fingerprint.schema_hash or "",
        policy=schema_policy,
    )
    df.write().format("delta").mode("overwrite").save(delta_location)
```

### Target Files
- `src/datafusion_engine/delta/schema_guard.py` (NEW)
- `src/semantics/pipeline.py` (optional materialization step)
- `src/datafusion_engine/session/runtime.py` (delta provider config)

### Deprecation / Deletion
- Remove any Arrow dataset fallback paths once Delta is enforced.

### Implementation Checklist
- [ ] Add schema guard + policy config
- [ ] Materialize semantic outputs to Delta where configured
- [ ] Store schema hash artifacts per output
- [ ] Update documentation with schema evolution guarantees

---

## Scope 5: CDF Incremental Merge Hardening

**Goal:** Replace simplified CDF join path with merge semantics and deterministic output.

### Representative Code Snippets

```python
# src/semantics/incremental/cdf_joins.py (extend)
def merge_incremental_results(
    ctx: SessionContext,
    *,
    incremental_df: DataFrame,
    base_table: str,
    key_columns: tuple[str, ...],
) -> DataFrame:
    base_df = ctx.table(base_table)
    # Replace existing keys with incremental results.
    # (Implementation uses anti-join + union to avoid duplicates.)
    anti = base_df.join(incremental_df, key_columns, how="anti")
    return anti.union(incremental_df)
```

```python
# src/semantics/compiler.py (hook)
if options.use_cdf and incremental_join_enabled(...):
    joined = build_incremental_join(...)
    joined = merge_incremental_results(
        self.ctx,
        incremental_df=joined,
        base_table=spec.output_name,
        key_columns=key_columns,
    )
```

### Target Files
- `src/semantics/incremental/cdf_joins.py`
- `src/semantics/compiler.py`
- `src/semantics/pipeline.py` (optional CDF materialization path)

### Deprecation / Deletion
- None; extend incremental framework.

### Implementation Checklist
- [ ] Add merge strategy and deterministic key handling
- [ ] Wire merge into `SemanticCompiler.relate()` when `use_cdf=True`
- [ ] Add tests for incremental correctness
- [ ] Document fallback behavior when base table missing

---

## Scope 6: Observability + Diagnostics Artifacts

**Goal:** Emit lineage, cache, and semantic diagnostics artifacts for full traceability.

### Representative Code Snippets

```python
# src/datafusion_engine/views/graph.py (artifact)
record_artifact(runtime_profile, "semantic_lineage_v1", {
    "views": {
        node.name: {
            "deps": list(node.deps),
            "required_udfs": list(node.required_udfs),
        }
        for node in nodes
    },
})
```

```python
# src/datafusion_engine/views/registry_specs.py (artifact)
record_artifact(runtime_profile, "semantic_cache_policy_v1", {
    "views": {name: node.cache_policy for name, node in nodes_by_name.items()},
})
```

### Target Files
- `src/datafusion_engine/views/graph.py`
- `src/datafusion_engine/views/registry_specs.py`

### Deprecation / Deletion
- None.

### Implementation Checklist
- [ ] Emit lineage artifact (`semantic_lineage_v1`)
- [ ] Emit cache policy artifact (`semantic_cache_policy_v1`)
- [ ] Add doc references to artifacts

---

## Scope 7: Legacy Module Removal + Alias Consolidation

**Goal:** Fully remove deprecated legacy relationship builders and centralize alias mapping.

### Representative Code Snippets

```python
# src/datafusion_engine/views/registry_specs.py
def _resolve_alias(name: str) -> str:
    from semantics.naming import SEMANTIC_OUTPUT_ALIASES
    return SEMANTIC_OUTPUT_ALIASES.get(name, name)
```

```python
# Remove files after migration:
#   src/relspec/relationship_datafusion.py
#   src/cpg/relationship_builder.py
#   src/cpg/relationship_specs.py
```

### Target Files
- `src/datafusion_engine/views/registry_specs.py` (alias consolidation)
- `src/relspec/relationship_datafusion.py` (delete)
- `src/cpg/relationship_builder.py` (delete)
- `src/cpg/relationship_specs.py` (delete)
- `src/cpg/relationship_contracts.py` (migrate off legacy import)

### Deprecation / Deletion
- Delete legacy modules after all imports are removed and tests pass.

### Implementation Checklist
- [ ] Remove all imports of legacy modules
- [ ] Replace alias logic with `SEMANTIC_OUTPUT_ALIASES` mapping
- [ ] Delete legacy modules and update documentation
- [ ] Run integration tests to confirm no regressions

---

## Suggested Execution Order

1) Scope 1 (information_schema validation)  
2) Scope 2 (cache policy + diagnostics)  
3) Scope 6 (observability artifacts)  
4) Scope 5 (CDF merge hardening)  
5) Scope 4 (Delta‑first materialization)  
6) Scope 3 (fingerprint gating)  
7) Scope 7 (legacy removal + alias consolidation)

---

## Notes

- Keep semantic view registration in `src/datafusion_engine/views/registry_specs.py` as the single authority.
- Avoid pushing legacy schema concerns into `SemanticCompiler`; prefer adapters in registry layer.
- When deprecating/removing modules, update docs and tests in lock‑step.
