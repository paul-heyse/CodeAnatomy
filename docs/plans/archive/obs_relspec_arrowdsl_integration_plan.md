## Obs + Relspec ArrowDSL Integration Plan

### Goals
- Replace bespoke Python row loops in relspec with shared ArrowDSL compute and Acero plan-lane primitives.
- Keep relationship pipelines stream-friendly by preferring plan-lane execution where possible.
- Centralize schema fingerprint/serialization in ArrowDSL and reuse across obs modules.
- Maintain strict typing and Ruff compliance with no suppressions.

### Constraints
- Preserve current output schemas and contracts unless explicitly redefined in scope items.
- Keep plan-lane work inside Acero and kernel-lane work inside `arrowdsl.compute` helpers.
- Avoid introducing new non-Arrow runtime dependencies.
- Maintain deterministic behavior (ordering and tie-breakers) when a rule requires it.

---

### Scope 1: Interval alignment via ArrowDSL compute primitives
**Description**
Replace the bespoke Python loop interval alignment in `relspec.compiler` with a kernel-lane
implementation that uses `arrowdsl.compute.kernels` + Arrow compute. The new kernel should:
- join on path (or other configured join keys),
- compute interval match predicates with `pc` expressions,
- score candidates using a computed span length,
- select best matches using `dedupe_keep_best_by_score`, and
- optionally emit left-only rows when `how="left"`.

**Code pattern**
```python
# src/arrowdsl/compute/kernels.py

def interval_align_table(
    left: TableLike,
    right: TableLike,
    *,
    cfg: IntervalAlignConfig,
) -> TableLike:
    left = set_or_append_column(
        left,
        "__left_id",
        pc.arange(0, left.num_rows, 1, type=pa.int64()),
    )
    right = set_or_append_column(
        right,
        "__right_id",
        pc.arange(0, right.num_rows, 1, type=pa.int64()),
    )

    joined = apply_join(
        left,
        right,
        spec=JoinSpec(
            join_type="inner",
            left_keys=(cfg.left_path_col,),
            right_keys=(cfg.right_path_col,),
            left_output=tuple(left.column_names),
            right_output=tuple(right.column_names),
        ),
        use_threads=True,
    )

    match_mask = _interval_match_expr(
        cfg,
        left_start=pc.field(cfg.left_start_col),
        left_end=pc.field(cfg.left_end_col),
        right_start=pc.field(cfg.right_start_col),
        right_end=pc.field(cfg.right_end_col),
    )
    matched = joined.filter(match_mask)

    span_len = pc.subtract(
        pc.cast(pc.field(cfg.right_end_col), pa.int64()),
        pc.cast(pc.field(cfg.right_start_col), pa.int64()),
    )
    scored = matched.append_column(cfg.match_score_col, pc.multiply(span_len, pa.scalar(-1)))

    best = dedupe_keep_best_by_score(
        scored,
        keys=("__left_id",),
        score_col=cfg.match_score_col,
        score_order="ascending",
        tie_breakers=cfg.tie_breakers,
    )

    if cfg.how == "left":
        return _emit_left_only(best, left=left, cfg=cfg)
    return best


# src/relspec/compiler.py

def _interval_align(left: TableLike, right: TableLike, cfg: IntervalAlignConfig) -> TableLike:
    return interval_align_table(left, right, cfg=cfg)
```

**Target files**
- Update: `src/arrowdsl/compute/kernels.py`
- Update: `src/relspec/compiler.py`
- Update: `src/relspec/model.py` (if helper config/types are added)

**Implementation checklist**
- [x] Add `interval_align_table(...)` to `arrowdsl.compute.kernels` using Arrow compute + join helpers.
- [x] Replace the Python-loop interval alignment in `relspec.compiler` with the new kernel.
- [x] Preserve `match_kind`/`match_score` semantics and tie-breaker behavior.
- [x] Ensure `how="left"` returns left-only rows with appropriate null-filled right columns.

**Status**
Completed in `src/arrowdsl/compute/kernels.py` and `src/relspec/compiler.py`.

---

### Scope 2: Winner-select rules backed by ArrowDSL dedupe kernels
**Description**
Implement `RuleKind.WINNER_SELECT` using shared dedupe kernels rather than passthrough logic.
Introduce a `WinnerSelectConfig` that maps to a `DedupeSpec` (KEEP_BEST_BY_SCORE) and use
`apply_dedupe` for deterministic selection.

**Code pattern**
```python
# src/relspec/model.py
class WinnerSelectConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    keys: tuple[str, ...] = ()
    score_col: str = "score"
    score_order: Literal["ascending", "descending"] = "descending"
    tie_breakers: tuple[SortKey, ...] = ()


# src/relspec/compiler.py
if rule.kind == RuleKind.WINNER_SELECT:
    cfg = rule.winner_select
    if cfg is None:
        raise ValueError("WINNER_SELECT rules require winner_select config.")

    def _exec(ctx: ExecutionContext, resolver: PlanResolver) -> TableLike:
        table = resolver.resolve(rule.inputs[0], ctx=ctx).to_table(ctx=ctx)
        spec = DedupeSpec(
            keys=cfg.keys,
            strategy="KEEP_BEST_BY_SCORE",
            tie_breakers=(SortKey(cfg.score_col, cfg.score_order), *cfg.tie_breakers),
        )
        return apply_dedupe(table, spec=spec, _ctx=ctx)
```

**Target files**
- Update: `src/relspec/model.py`
- Update: `src/relspec/compiler.py`

**Implementation checklist**
- [x] Add `WinnerSelectConfig` and wire it into `RelationshipRule`.
- [x] Compile WINNER_SELECT rules to a kernel-lane dedupe using `apply_dedupe`.
- [x] Preserve deterministic tie-breaker behavior with `SortKey` ordering.

**Status**
Completed in `src/relspec/model.py`, `src/relspec/compiler.py`, and `src/relspec/__init__.py`.

---

### Scope 3: Plan-lane union with schema alignment
**Description**
Keep UNION_ALL outputs in Acero whenever possible by aligning each plan to a unified schema
and unioning without materializing intermediate tables. Use `projection_for_schema` and
`SchemaEvolutionSpec.unify_schema_from_schemas` to align columns and types.

**Code pattern**
```python
# src/relspec/compiler.py

def _align_plan(plan: Plan, *, schema: SchemaLike, ctx: ExecutionContext) -> Plan:
    exprs, names = projection_for_schema(
        schema,
        available=plan.schema(ctx=ctx).names,
        safe_cast=ctx.safe_cast,
    )
    return plan.project(exprs, names, ctx=ctx)


def _union_all_plans_aligned(plans: Sequence[Plan], *, ctx: ExecutionContext) -> Plan:
    schema = SchemaEvolutionSpec().unify_schema_from_schemas(
        [plan.schema(ctx=ctx) for plan in plans]
    )
    aligned = [_align_plan(plan, schema=schema, ctx=ctx) for plan in plans]
    return union_all_plans(aligned, label="relspec_union")
```

**Target files**
- Update: `src/relspec/compiler.py`
- Update: `src/arrowdsl/schema/schema.py` (if new alignment helpers are centralized)

**Implementation checklist**
- [x] Add plan-lane schema alignment helper using `projection_for_schema`.
- [x] Update UNION_ALL compilation to use aligned plan unions when all inputs are Acero plans.
- [x] Retain kernel-lane fallback when post-kernels require materialization.

**Status**
Completed in `src/relspec/compiler.py`.

---

### Scope 4: Shared schema utilities for obs stats and repro
**Description**
Centralize schema fingerprinting and schema serialization in ArrowDSL and reuse in `obs.stats`
and `obs.repro`. This eliminates duplicated logic and keeps schema metadata consistent across
manifests and run bundles.

**Code pattern**
```python
# src/arrowdsl/schema/schema.py

def schema_to_dict(schema: SchemaLike) -> dict[str, object]:
    return {
        "fields": [
            {"name": f.name, "type": str(f.type), "nullable": bool(f.nullable)}
            for f in schema
        ]
    }


def schema_fingerprint(schema: SchemaLike) -> str:
    payload = json.dumps(schema_to_dict(schema), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


# src/obs/stats.py
from arrowdsl.schema.schema import schema_fingerprint, schema_to_dict

# src/obs/repro.py
from arrowdsl.schema.schema import schema_fingerprint, schema_to_dict
```

**Target files**
- Update: `src/arrowdsl/schema/schema.py`
- Update: `src/obs/stats.py`
- Update: `src/obs/repro.py`
- Update: `src/obs/manifest.py` (to use shared helpers where needed)

**Implementation checklist**
- [x] Add `schema_to_dict` and `schema_fingerprint` helpers in ArrowDSL.
- [x] Replace local schema serialization in `obs.stats` and `obs.repro`.
- [x] Ensure manifest output stays stable after refactor (same field ordering and values).

**Status**
Completed in `src/arrowdsl/schema/schema.py`, `src/obs/stats.py`, and `src/obs/repro.py`.
