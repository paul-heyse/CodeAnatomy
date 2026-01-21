# Bytecode Best-in-Class Extraction Plan (Design-Phase)

## Goals
- Capture Python 3.13 bytecode facts with deterministic normalization.
- Emit richer instruction metadata (specialization, caches, line mapping, stack effects).
- Improve CFG and add minimal DFG scaffolding for later CPG wiring.
- Keep DataFusion schemas/view specs fully aligned and validated via information_schema.
- Maintain performance with caching and predictable runtime metadata.

## Non-goals
- Adding symtable extraction or crosswalk logic.
- Replacing AST/CST extraction or CPG binding layers.
- Enabling adaptive disassembly by default (keep deterministic output).

## Constraints
- Keep `bytecode_files_v1` backward-compatible (new fields nullable; no breaking renames).
- All validation should route through DataFusion `information_schema` and `arrow_typeof`.
- Use Python stdlib `dis` surfaces (no private CPython imports).

---

## Scope 1: Bytecode schema expansion (v1) inside bytecode_files_v1
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/extract_templates.py`

### Code pattern
```python
BYTECODE_CACHE_ENTRY_T = pa.struct(
    [
        ("name", pa.string()),
        ("size", pa.int32()),
        ("data_hex", pa.string()),
    ]
)

BYTECODE_LINE_T = pa.struct(
    [
        ("offset", pa.int32()),
        ("line1", pa.int32()),
        ("line0", pa.int32()),
        ("attrs", ATTRS_T),
    ]
)

BYTECODE_INSTR_T = pa.struct(
    [
        ("offset", pa.int32()),
        ("start_offset", pa.int32()),
        ("end_offset", pa.int32()),
        ("opname", pa.string()),
        ("baseopname", pa.string()),
        ("opcode", pa.int32()),
        ("baseopcode", pa.int32()),
        ("arg", pa.int32()),
        ("oparg", pa.int32()),
        ("argval_kind", pa.string()),
        ("argval_int", pa.int64()),
        ("argval_str", pa.string()),
        ("line_number", pa.int32()),
        ("label", pa.int32()),
        ("cache_info", pa.list_(BYTECODE_CACHE_ENTRY_T)),
        ("span", SPAN_T),
        ("attrs", ATTRS_T),
    ]
)
```

### Implementation checklist
- [ ] Extend `BYTECODE_INSTR_T` with normalization fields (`baseopname`, `baseopcode`,
  `line_number`, `label`, `cache_info`, offset range fields).
- [ ] Add `BYTECODE_LINE_T` and store it as a `line_table` list inside
  `BYTECODE_CODE_OBJ_T` (no new root dataset).
- [ ] Add code-unit metadata fields as nullable columns or `attrs` (e.g., `co_qualname`,
  `co_filename`, decoded `co_flags` booleans).
- [ ] Update `SCHEMA_REGISTRY` and `NESTED_DATASET_INDEX` for new
  `code_objects.*` paths (derived views only).
- [ ] Keep extract outputs unchanged at the root (`bytecode_files_v1` only).

---

## Scope 2: Instruction extraction normalization (specialization, caches, argval)
Status: Planned

### Target file list
- `src/extract/bytecode_extract.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```python
baseopname = getattr(ins, "baseopname", ins.opname)
baseopcode = getattr(ins, "baseopcode", ins.opcode)
line_number = getattr(ins, "line_number", None)
label = getattr(ins, "label", None)
cache_rows = _cache_info_rows(getattr(ins, "cache_info", None))

argval_kind, argval_int, argval_str = _argval_fields(ins)

ins_rows.append(
    {
        **identity,
        **_code_unit_key_columns(unit_ctx),
        "instr_index": int(idx),
        "offset": int(ins.offset),
        "start_offset": int(getattr(ins, "start_offset", ins.offset)),
        "end_offset": int(getattr(ins, "end_offset", ins.offset)),
        "opname": ins.opname,
        "baseopname": baseopname,
        "opcode": int(ins.opcode),
        "baseopcode": int(baseopcode),
        "arg": int(ins.arg) if ins.arg is not None else None,
        "oparg": int(getattr(ins, "oparg", ins.arg)) if ins.arg is not None else None,
        "argval_kind": argval_kind,
        "argval_int": argval_int,
        "argval_str": argval_str,
        "line_number": int(line_number) if line_number is not None else None,
        "label": int(label) if label is not None else None,
        "cache_info": cache_rows,
    }
)
```

### Implementation checklist
- [ ] Add `_cache_info_rows` to normalize `(name, size, data)` to stable rows.
- [ ] Add `_argval_fields` to provide typed `argval` fields and a `argval_kind` label
  using `dis.hasconst/hasname/haslocal/hasfree/hascompare`.
- [ ] Populate `baseopname/baseopcode` for deterministic opcode semantics.
- [ ] Persist `line_number` and `label` (3.13) when available.
- [ ] Update `bytecode_instructions_sql` to project the new fields.

---

## Scope 3: Line table + code-object metadata (embedded)
Status: Planned

### Target file list
- `src/extract/bytecode_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```python
def _append_line_table_rows(
    unit_ctx: CodeUnitContext,
    line_rows: list[Row],
) -> None:
    identity = unit_ctx.file_ctx.identity_row()
    for offset, lineno in dis.findlinestarts(unit_ctx.code):
        line_rows.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "offset": int(offset),
                "line1": int(lineno) if lineno is not None else None,
                "line0": int(lineno) - 1 if lineno is not None else None,
            }
        )
```

### Implementation checklist
- [ ] Add a `line_table` list inside `BYTECODE_CODE_OBJ_T` and a corresponding
  derived view (offset → line mapping).
- [ ] Add code-object metadata fields (`co_qualname`, `co_filename`, decoded CO_* flags).
- [ ] Store compiler metadata in file/code attrs (python version, magic number,
  optimize, dont_inherit, adaptive flag).
- [ ] Update `bytecode_code_units_sql` to include new metadata fields.

---

## Scope 4: CFG correctness + jump taxonomy
Status: Planned

### Target file list
- `src/extract/bytecode_extract.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```python
has_jump = ins.opcode in dis.hasjump
jump_target = getattr(ins, "jump_target", None)

if has_jump and jump_target is not None:
    is_unconditional = ins.opname.startswith("JUMP") and "IF" not in ins.opname
    edge_kind = "jump" if is_unconditional else "branch"
```

### Implementation checklist
- [ ] Prefer `dis.hasjump` and `Instruction.jump_target` over opname heuristics.
- [ ] Keep exception edges keyed by entry index; include `depth/lasti` in edge attrs.
- [ ] Ensure block boundaries include `dis.findlabels` plus exception table targets.
- [ ] Update CFG edge attrs to include `jump_kind` and `jump_target_label` (if present).

---

## Scope 5: Stack-effect DFG scaffolding (minimal def/use, embedded)
Status: Planned

### Target file list
- `src/extract/bytecode_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/extract_templates.py`

### Code pattern
```python
stack_depth_before = len(stack)
delta = dis.stack_effect(ins.opcode, ins.arg or 0, jump=None)
stack_depth_after = stack_depth_before + delta

for _ in range(max(0, -delta)):
    if stack:
        producer = stack.pop()
        dfg_edges.append(
            {
                **identity,
                **_code_unit_key_columns(unit_ctx),
                "src_instr_index": producer.instr_index,
                "dst_instr_index": int(idx),
                "kind": "USE_STACK",
            }
        )

if delta > 0:
    for _ in range(delta):
        stack.append(ValueRef(instr_index=int(idx)))
```

### Implementation checklist
- [ ] Add a `dfg_edges` list inside `BYTECODE_CODE_OBJ_T` and schema.
- [ ] Record `stack_depth_before/after` on instruction attrs for quick checks.
- [ ] Emit minimal stack-use edges (`USE_STACK`) and optional store/load edges.
- [ ] Add a DataFusion view for DFG edges keyed by `code_id` and `instr_id`.
- [ ] Add a validation rule to assert stack depth never goes negative.

---

## Scope 6: DataFusion validation + mapping workflow
Status: Planned

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/schema_introspection.py`

### Code pattern
```sql
SELECT
  arrow_typeof(instructions) AS instr_type,
  arrow_typeof(blocks) AS block_type,
  arrow_typeof(cfg_edges) AS edge_type
FROM bytecode_files_v1
LIMIT 1;
```

### Implementation checklist
- [ ] Extend bytecode fragment views to project new schema fields, line table, and DFG edges.
- [ ] Add a `validate_bytecode_views` routine that checks `arrow_typeof` for new
  `code_objects.*` fields.
- [ ] Record schema snapshot deltas after registration (information_schema tables/columns).
- [ ] Add optional `map_entries`/`unnest` helpers for cache-info and consts expansion.
- [ ] Update view registry snapshots in runtime to include bytecode views.

---

## Scope 7: Performance, determinism, and diagnostics
Status: Planned

### Target file list
- `src/extract/bytecode_extract.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
cache_key = (
    file_ctx.file_id,
    file_ctx.file_sha256,
    options.optimize,
    options.dont_inherit,
    options.adaptive,
)
cached = _BYTECODE_CACHE.get(cache_key)
if cached is None:
    compiled = compile(text, file_ctx.path, "exec", dont_inherit=options.dont_inherit,
                       optimize=options.optimize)
    cached = _compile_to_units(compiled, options)
    _BYTECODE_CACHE[cache_key] = cached
```

### Implementation checklist
- [ ] Add an in-memory cache for compiled code objects + disassembly results.
- [ ] Gate parallel extraction by file count and memory budget.
- [ ] Emit structured error rows for compile, disassembly, and analysis failures.
- [ ] Persist `determinism` attributes (adaptive flag, Python version) for audits.

---

## Rollout order (recommended)
1. Schema expansion + extractor updates (Scopes 1–2).
2. Line table + code object metadata (Scope 3).
3. CFG correctness (Scope 4).
4. Stack-effect DFG scaffolding (Scope 5).
5. DataFusion validation + view updates (Scope 6).
6. Performance/diagnostics hardening (Scope 7).
