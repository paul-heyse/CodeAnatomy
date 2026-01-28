# ArrowDSL Streamline & Legacy Decommission Plan

## Goal
Minimize legacy surfaces in `src/arrowdsl` while preserving the core runtime/schema utilities that are still foundational. This plan decommissions spec/IPC/registry/finalize layers that are now better served by `datafusion_engine`, `ibis_engine`, `storage`, and `sqlglot_tools`, and streamlines the remaining ArrowDSL core to align with the go‑forward architecture.

## Guiding Principles
- Keep **core** runtime/context, schema metadata/encoding, and Arrow interop in one place (ArrowDSL core).
- Move **engine‑specific** concerns (kernels, execution/finalize, SQL policy, IO/storage) into engine/storage packages.
- Prefer **SQLGlot** as the expression/diagnostics IR; use Ibis/DataFusion for execution.
- Remove unused legacy layers (spec tables, ExprIR, custom IPC spec IO) once call sites are migrated.

---

## Scope 1 — Kernel Registry: migrate ArrowDSL kernel registry into datafusion_engine
**Objective:** Treat kernel metadata as engine‑owned, not ArrowDSL‑owned.

**Status:** Completed.

**Representative pattern**
```python
# datafusion_engine/kernel_registry.py (new home)
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal


class KernelLane(StrEnum):
    BUILTIN = "builtin"
    DF_UDF = "df_udf"


@dataclass(frozen=True)
class KernelDef:
    name: str
    lane: KernelLane
    volatility: Literal["stable", "volatile", "immutable"] = "stable"
    requires_ordering: bool = False
    impl_name: str = ""
```

**Targets**
- `src/arrowdsl/kernel/registry.py` (decommission)
- `src/datafusion_engine/kernel_registry.py` (new canonical location)
- Update imports in:
  - `src/datafusion_engine/kernel_registry.py`
  - `src/relspec/pipeline_policy.py`
  - any other callers referencing `arrowdsl.kernel.registry`

**Implementation checklist**
- Create new kernel registry module in `datafusion_engine`.
- Update imports at call sites.
- Remove ArrowDSL kernel registry module after migration.

**Completed work**
- Moved `KernelLane`, `KernelDef`, and registry constants into `src/datafusion_engine/kernel_registry.py`.
- Updated import sites (e.g., `src/relspec/pipeline_policy.py`).
- Deleted `src/arrowdsl/kernel/registry.py` and `src/arrowdsl/kernel/__init__.py`.

---

## Scope 2 — Replace ArrowDSL spec tables + ExprIR with SQLGlot‑based spec serialization
**Objective:** Decommission `arrowdsl.spec.*` by representing predicates/derived fields/specs as SQLGlot AST or SQL text, compiled via `sqlglot_tools` and executed through Ibis/DataFusion.

**Status:** Completed.

**Representative pattern**
```python
# schema_spec/specs.py (or new schema_spec/sqlglot_spec.py)
from sqlglot import parse_one
from sqlglot_tools.diagnostics import DiagnosticsSink
from sqlglot_tools.optimizer import optimize_expression


def compile_predicate(sql: str, *, dialect: str, sink: DiagnosticsSink) -> str:
    expr = parse_one(sql, read=dialect)
    optimized = optimize_expression(expr, dialect=dialect, sink=sink)
    return optimized.sql(dialect=dialect)
```

**Targets**
- Decommission:
  - `src/arrowdsl/spec/expr_ir.py`
  - `src/arrowdsl/spec/literals.py`
  - `src/arrowdsl/spec/scalar_union.py`
  - `src/arrowdsl/spec/io.py`
  - `src/arrowdsl/spec/infra.py`
  - `src/arrowdsl/spec/tables/*`
- Update call sites:
  - `src/schema_spec/system.py`
  - `src/schema_spec/specs.py`
  - `src/normalize/evidence_specs.py`
  - `src/normalize/dataset_builders.py`
  - `src/datafusion_engine/extract_builders.py`
  - `src/ibis_engine/query_compiler.py`
  - `src/relspec/model.py`

**Implementation checklist**
- Define a SQLGlot‑based spec payload (SQL text + dialect + optional transform metadata).
- Implement compile/validate helpers using `sqlglot_tools` diagnostics policy (aligned with datafusion policy).
- Migrate spec table serialization to msgspec/JSON (or reuse storage/Delta metadata).
- Replace ExprIR usage with SQLGlot payload parsing/compilation.
- Delete ArrowDSL spec modules after call sites are migrated.

**Completed work**
- Introduced SQLGlot‑backed expression spec at `src/sqlglot_tools/expr_spec.py`.
- Repointed all ExprIR usage sites to `sqlglot_tools.expr_spec.ExprIR`.
- Added `src/schema_spec/registration.py` (dataset registration) and `src/schema_spec/literals.py` (scalar literals).
- Removed all ArrowDSL spec modules:
  - `src/arrowdsl/spec/expr_ir.py`
  - `src/arrowdsl/spec/literals.py`
  - `src/arrowdsl/spec/scalar_union.py`
  - `src/arrowdsl/spec/io.py`
  - `src/arrowdsl/spec/infra.py`
  - `src/arrowdsl/spec/tables/*`
  - `src/arrowdsl/spec/__init__.py`
- Updated schema_spec exports to remove spec table adapters and deprecated helpers.
- Migrated predicate/derived fields to SQL‑text `SqlExprSpec` payloads.
- Updated query compilation to SQL‑first predicates/derived expressions.

---

## Scope 3 — Consolidate IPC + payload hashing into storage/engine
**Objective:** Remove ArrowDSL IPC helpers and unify IPC/hashing with storage utilities.

**Status:** Completed.

**Representative pattern**
```python
# storage/io.py (canonical IPC utilities)
def payload_hash(payload: Mapping[str, object], schema: pa.Schema) -> str:
    table = pa.Table.from_pylist([dict(payload)], schema=schema)
    return hashlib.sha256(ipc_bytes(table)).hexdigest()
```

**Targets**
- Decommission:
  - `src/arrowdsl/io/ipc.py`
  - `src/arrowdsl/spec/io.py` (if not already removed)
- Update imports in:
  - `src/engine/runtime_profile.py`
  - `src/schema_spec/specs.py`
  - `src/sqlglot_tools/optimizer.py`
  - `src/datafusion_engine/runtime.py`
  - `src/ibis_engine/param_tables.py`
  - `src/relspec/rustworkx_graph.py`
  - `src/normalize/*` and `src/incremental/*` where `payload_hash` or IPC helpers are used

**Implementation checklist**
- Move IPC read/write + hash helpers to `src/storage/io.py`.
- Keep Arrow interop shims in `arrowdsl.core.interop` (readers/tables).
- Update all imports to new storage IO module.
- Delete ArrowDSL IPC modules after migration.

**Completed work**
- Added `src/storage/ipc.py` with IPC + payload hashing API.
- Updated all imports to use `storage.ipc_utils`.
- Deleted `src/arrowdsl/io/ipc.py` and `src/arrowdsl/io/__init__.py`.

---

## Scope 4 — Move finalize gate into datafusion_engine + ibis execution patterns
**Objective:** Relocate `arrowdsl.finalize` to engine‑owned execution, using Ibis plans and DataFusion kernels without ArrowDSL‑specific entry points.

**Status:** Completed.

**Representative pattern**
```python
# datafusion_engine/finalize.py (new home)
def finalize_table(
    table: TableLike,
    *,
    contract: ContractSpec,
    ctx: ExecutionContext,
    options: FinalizeOptions | None = None,
) -> FinalizeResult:
    # apply schema policy + invariants via DataFusion kernels
    ...
```

**Targets**
- Decommission:
  - `src/arrowdsl/finalize/finalize.py`
  - `src/arrowdsl/finalize/__init__.py`
- Update imports in:
  - `src/normalize/runner.py`
  - `src/extract/schema_ops.py`
  - `src/relspec/contract_catalog.py`

**Implementation checklist**
- Create `datafusion_engine/finalize.py` or `engine/finalize.py` as canonical home.
- Update `normalize`/`extract` to call new finalize entry point.
- Ensure finalize uses `ExecutionContext` + `DataFusionRuntimeProfile`.
- Delete ArrowDSL finalize package.

**Completed work**
- Moved finalize implementation to `src/datafusion_engine/finalize.py`.
- Updated imports in `normalize`, `extract`, `relspec`, and `cpg`.
- Deleted `src/arrowdsl/finalize/*`.

---

## Scope 5 — ArrowDSL core streamlining (retain but trim)
**Objective:** Keep only core runtime/schema/interop utilities; drop legacy, low‑value modules that can move or be retired.

**Status:** Completed.

**Core to keep**
- `src/arrowdsl/core/execution_context.py`
- `src/arrowdsl/core/runtime_profiles.py`
- `src/arrowdsl/core/interop.py`
- `src/arrowdsl/core/ordering.py`
- `src/arrowdsl/core/plan_ops.py`
- `src/arrowdsl/schema/*` (encoding, metadata, validation, serialization)

**Candidates to retire or re‑home**
- `src/arrowdsl/core/metrics.py` → move to `obs`
- `src/arrowdsl/core/scan_telemetry.py` → move to `obs`
- `src/arrowdsl/core/joins.py` → migrate to `datafusion_engine.kernels` or `ibis_engine`
- `src/arrowdsl/core/validity.py` → keep only if used broadly; otherwise move/retire
- `src/arrowdsl/core/ids.py`, `src/arrowdsl/core/array_iter.py` → keep only if used across packages

**Implementation checklist**
- Audit usage for each candidate module.
- For each used module: move to `datafusion_engine`/`engine` with updated imports.
- Remove unused modules outright after import cleanup.

**Completed work**
- Moved join helpers to `src/normalize/join_utils.py` and updated callers.
- Removed `src/arrowdsl/core/joins.py`.
- Removed unused `src/arrowdsl/core/validity.py`.
- Added union type constants to `src/arrowdsl/schema/union_codec.py` to replace spec‑package dependency.
- Moved metrics + scan telemetry into `src/obs` and updated imports.

---

## Decommission List (after plan is complete)
**Files**
- ✅ `src/arrowdsl/kernel/registry.py`
- ✅ `src/arrowdsl/spec/expr_ir.py`
- ✅ `src/arrowdsl/spec/literals.py`
- ✅ `src/arrowdsl/spec/scalar_union.py`
- ✅ `src/arrowdsl/spec/io.py`
- ✅ `src/arrowdsl/spec/infra.py`
- ✅ `src/arrowdsl/spec/tables/*`
- ✅ `src/arrowdsl/io/ipc.py`
- ✅ `src/arrowdsl/finalize/*`
- ✅ `src/arrowdsl/core/joins.py`
- ✅ `src/arrowdsl/core/validity.py`
- (optional, based on audit) `src/arrowdsl/core/metrics.py`, `src/arrowdsl/core/scan_telemetry.py`

**Functions to remove/migrate**
- Kernel registry functions: `kernel_def`, `KERNEL_REGISTRY` (to datafusion_engine)
- ExprIR encode/decode/compile APIs (superseded by SQLGlot)
- IPC helpers: `ipc_bytes`, `ipc_hash`, `payload_hash`, `write_table_ipc_file`, etc. (move to storage)
- Finalize public API: `finalize`, `FinalizeContext`, `FinalizeOptions`, `FinalizeResult` (move to engine)

---

## Sequencing Recommendation
1. **Scope 1** (kernel registry) — low risk, minimal dependency fallout.
2. **Scope 3** (IPC/hashing) — broad but mechanical import updates.
3. **Scope 4** (finalize) — engine‑level alignment.
4. **Scope 2** (spec/ExprIR) — largest change; do after SQLGlot policy alignment is ready.
5. **Scope 5** (core trim) — audit‑driven cleanup after migrations.

---

## Success Criteria
- No direct imports from `arrowdsl.spec.*`, `arrowdsl.io.ipc`, or `arrowdsl.finalize`.
- SQLGlot policy used for diagnostics and compile paths where ExprIR existed.
- Kernel metadata lives in `datafusion_engine`.
- ArrowDSL left as a minimal core runtime/schema/interop utility library.

---

## Remaining Scope (Explicit TODOs)
- ✅ All remaining items completed (SQL‑text payloads migrated, spec serialization added, and core relocations finished).
