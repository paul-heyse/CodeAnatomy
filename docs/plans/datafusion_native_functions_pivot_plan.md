# DataFusion Native Functions Pivot Plan

## Purpose
Pivot ExprIR function support to DataFusion built-ins and DataFusion UDFs only, remove
Ibis UDF fallback paths for the aligned functions, and make registry/coverage reports
reflect DataFusion-native availability. No compatibility shim or fallback behavior
will remain for the targeted functions.

## Objectives
- DataFusion is the only execution lane for the aligned ExprIR call set.
- Registry and coverage signals report DataFusion (not Ibis) for these functions.
- Ibis UDF fallback implementations (pyarrow/python) are removed.
- Function usage continues to compile through Ibis and SQLGlot without behavioral
  change, but any non-DataFusion execution attempts fail fast.

## In scope
- ExprIR calls used by rules today: `coalesce`, `concat`, `bit_wise_and`, `equal`,
  `not_equal`, `fill_null`, `if_else`, `invert`, `is_null`, `stringify`,
  `stable_hash128`.
- DataFusion UDFs already registered: `stable_hash64`, `stable_hash128`,
  `position_encoding_norm`, `col_to_byte`, `normalize_span`.
- Registry/coverage alignment for DataFusion built-ins and SQL expressions.

## Out of scope
- No new Ibis or SQLGlot features beyond the existing compilation surface.
- No compatibility shim for Ibis/pyarrow/python lanes for the targeted functions.

## Scope items

### Scope 1: DataFusion builtin and SQL-expression registry support
Goal: make registry/coverage indicate DataFusion-native support for the aligned
ExprIR calls, without relying on Ibis UDF specs.

Representative code patterns:
```python
DATAFUSION_BUILTIN_SPECS: tuple[FunctionSpec, ...] = (
    FunctionSpec(
        func_id="coalesce",
        engine_name="coalesce",
        kind="scalar",
        input_types=(pa.null(), pa.null()),
        return_type=pa.null(),
        lanes=("df_rust",),
    ),
    FunctionSpec(
        func_id="concat",
        engine_name="concat",
        kind="scalar",
        input_types=(pa.string(), pa.string()),
        return_type=pa.string(),
        lanes=("df_rust",),
    ),
)

DATAFUSION_SQL_EXPRESSIONS: Mapping[str, str] = {
    "bit_wise_and": "AND",
    "equal": "=",
    "not_equal": "!=",
    "fill_null": "COALESCE(value, fill_value)",
    "if_else": "CASE WHEN",
    "invert": "NOT",
    "is_null": "IS NULL",
    "stringify": "CAST(... AS STRING)",
}
```

Target files:
- `src/engine/function_registry.py`
- `src/relspec/rules/coverage.py`
- (new) `src/datafusion_engine/builtin_registry.py`

Implementation checklist:
- [ ] Add a DataFusion builtin registry list for the aligned built-in functions.
- [ ] Add a SQL-expression registry map for non-function SQL operators.
- [ ] Merge builtin specs into `build_function_registry(...)` with lane `df_rust`.
- [ ] Update coverage reporting to treat SQL-expression entries as DataFusion-supported.
- [ ] Confirm registry payloads show DataFusion lanes and no Ibis lanes for these names.

### Scope 2: ExprIR mapping patterns for DataFusion-native expressions
Goal: ensure ExprIR compilation stays in Ibis expressions that translate directly to
DataFusion SQL, without Ibis UDFs.

Representative code patterns:
```python
def _if_else_expr(cond: Value, true_value: Value, false_value: Value) -> Value:
    return ibis.ifelse(cond, true_value, false_value)

def _stringify_expr(value: Value) -> StringValue:
    return value.cast("string")

def _concat_expr(*values: Value) -> Value:
    if all(value.type().is_string() for value in values):
        result = cast("StringValue", values[0])
        for part in values[1:]:
            result = result.concat(cast("StringValue", part))
        return result
    if all(value.type().is_array() for value in values):
        result = cast("ArrayValue", values[0])
        for part in values[1:]:
            result = result.concat(cast("ArrayValue", part))
        return result
    raise ValueError("concat requires all string or all array inputs.")
```

Target files:
- `src/ibis_engine/expr_compiler.py`
- `src/ibis_engine/hashing.py`
- `src/datafusion_engine/df_builder.py`

Implementation checklist:
- [ ] Keep ExprIR call handlers that compile to DataFusion-friendly SQL patterns.
- [ ] Confirm `concat` string vs array dispatch stays type-directed.
- [ ] Ensure SQLGlot -> DataFusion translation keeps these operators as native SQL.

### Scope 3: Remove Ibis UDF specs for DataFusion built-ins
Goal: decommission Ibis UDF specs and definitions that duplicate DataFusion built-ins
or SQL expressions.

Representative code patterns:
```python
IBIS_UDF_SPECS: tuple[IbisUdfSpec, ...] = (
    # Keep only DataFusion UDF stubs and rule primitives.
    IbisUdfSpec(func_id="stable_hash64", ...),
    IbisUdfSpec(func_id="stable_hash128", ...),
    IbisUdfSpec(func_id="position_encoding_norm", ...),
    IbisUdfSpec(func_id="col_to_byte", ...),
    IbisUdfSpec(func_id="cpg_score", ...),
)
```

Target files:
- `src/ibis_engine/builtin_udfs.py`
- `src/engine/function_registry.py`
- `src/relspec/rules/coverage.py`

Implementation checklist:
- [ ] Delete UDF definitions for `bit_wise_and`, `coalesce`, `concat`, `if_else`,
      `invert`, `is_null`, `stringify`.
- [ ] Remove their `IbisUdfSpec` entries from `IBIS_UDF_SPECS`.
- [ ] Verify no remaining imports or references to the removed symbols.

### Scope 4: DataFusion-only UDFs (drop Ibis pyarrow/python fallbacks)
Goal: keep DataFusion UDF registration as the only execution surface for
`stable_hash64`, `stable_hash128`, `position_encoding_norm`, `col_to_byte`,
`normalize_span`.

Representative code patterns:
```python
DATAFUSION_UDF_SPECS: tuple[DataFusionUdfSpec, ...] = (
    DataFusionUdfSpec(
        func_id="stable_hash128",
        engine_name="stable_hash128",
        kind="scalar",
        input_types=(pa.string(),),
        return_type=pa.string(),
        udf_tier="pyarrow",
    ),
)

def register_datafusion_udfs(ctx: SessionContext) -> None:
    for name, udf in datafusion_scalar_udf_map().items():
        ctx.register_udf(udf)
```

Target files:
- `src/ibis_engine/builtin_udfs.py`
- `src/datafusion_engine/udf_registry.py`
- `src/datafusion_engine/runtime.py`

Implementation checklist:
- [ ] Remove `*_pyarrow` and `*_python` Ibis UDF implementations for these functions.
- [ ] Keep minimal Ibis builtin stubs only where needed to compile ExprIR to SQL.
- [ ] Ensure DataFusion UDFs are registered in every execution context.

### Scope 5: Diagnostics and coverage outputs
Goal: align diagnostics with DataFusion-native execution and remove Ibis fallback
signals for the targeted functions.

Representative code patterns:
```python
def _function_coverage(..., datafusion_builtins: set[str], sql_exprs: set[str]) -> ...:
    datafusion_supported = name in datafusion_builtins or name in sql_exprs
    registry_supported = registry_spec is not None or datafusion_supported
```

Target files:
- `src/relspec/rules/coverage.py`
- `docs/plans/datafusion_displacement_mapping.md`
- `docs/plans/datafusion_displacement_mapping.json`

Implementation checklist:
- [ ] Update coverage fields to represent DataFusion-native support explicitly.
- [ ] Regenerate the displacement mapping artifacts from the updated registry.
- [ ] Confirm no fallbacks are attributed to Ibis lanes for these functions.

## Legacy code to decommission and delete
- `src/ibis_engine/builtin_udfs.py`: UDF definitions and spec entries for
  `bit_wise_and`, `coalesce`, `concat`, `if_else`, `invert`, `is_null`, `stringify`.
- `src/ibis_engine/builtin_udfs.py`: `*_pyarrow` and `*_python` UDF implementations for
  `stable_hash64`, `stable_hash128`, `position_encoding_norm`, `col_to_byte`.
- `src/ibis_engine/builtin_udfs.py`: Ibis UDF lane metadata (`ibis_pyarrow`,
  `ibis_python`) for the functions listed above.
- Any imports, exports, or tests that reference the removed symbols.

## Follow-up actions
- Regenerate coverage and mapping artifacts to prove DataFusion-native support.
- Update `docs/python_library_reference/datafusion_builtin_and_udf.md` if any API
  names or usage patterns change.
- Run quality gates after implementation:
  - `uv run ruff check --fix`
  - `uv run pyrefly check`
  - `uv run pyright --warnings --pythonversion=3.13`
