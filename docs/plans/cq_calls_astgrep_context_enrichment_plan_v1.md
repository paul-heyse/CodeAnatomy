# CQ Calls: ast-grep-py + Context Enrichment Plan (v1)

> **Status:** Draft (requested)
> **Author:** Codex
> **Created:** 2026-02-03
> **Context:** Align `cq calls` with ast-grep-py-first scanning, on-demand AST/symtable/bytecode enrichment, rpygrep-based context windows, and zero caching.

---

## Executive Summary

This plan rebuilds `cq calls` around **ast-grep-py as the fast, structural search layer**, then performs **targeted enrichment only on matched call sites**. It removes **all caching** and avoids **upfront whole-repo indexing**, while adding **symtable/bytecode confidence signals** and **rpygrep-derived context windows** that capture function boundaries around each call.

Key upgrades:
- ast-grep-py is the authoritative callsite enumerator.
- AST + symtable + bytecode analysis runs **only on files that contain call matches**.
- Context windows are computed using **rpygrep-assisted def discovery** and **indentation boundaries**, not full-repo indexing.
- All DiskCache and cache CLI/config plumbing is removed.

---

## Goals

- Make `cq calls` fast and correct using **ast-grep-py** as the base enumerator.
- Provide confidence indicators using **symtable + bytecode** for each callsite.
- Provide **context windows** per callsite (module + start/end lines of the containing function block), derived via **rpygrep** and file text.
- Remove **all caching** and avoid **upfront index builds**; only analyze files returned by ast-grep-py or rpygrep.

## Non-Goals

- Rewriting `cq q` or other macros beyond required shared utilities.
- Introducing new caches or background indexes.
- Adding cross-language support.

---

## Scope Item 1: Fix ast-grep-py Invocation Semantics

**Objective:** Ensure `ast-grep-py` is called with correct config shape (full config dict vs kwargs), per `docs/python_library_reference/ast-grep-py.md`.

**Representative snippet:**

```python
config = rule.config
if _is_full_config(config):
    matches = node.find_all(config)
else:
    matches = node.find_all(**config)
```

**Target files:**
- `tools/cq/astgrep/sgpy_scanner.py`
- `tools/cq/astgrep/rules_py.py` (if any rule shapes need normalization)

**Deprecate/delete after completion:**
- Any implicit assumption that `config.get("rule")` is safe to pass as kwargs.

**Implementation checklist:**
- [ ] Add `_is_full_config()` helper to detect `rule`/`constraints`/`utils`/`transform` keys.
- [ ] Route full configs to `node.find_all(config)`.
- [ ] Route simple rule kwargs to `node.find_all(**config)`.
- [ ] Add unit coverage for config routing behavior.

---

## Scope Item 2: ast-grep-py First, rpygrep Fallback Only

**Objective:** Make `cq calls` depend on ast-grep-py records for callsites; use rpygrep only if ast-grep is unavailable or yields no records due to toolchain issues.

**Representative snippet:**

```python
call_records = sg_scan(
    paths=scan_files,
    record_types={"call"},
    root=root,
)
if not call_records and fallback_allowed:
    candidates = find_call_candidates(root, function_name)
```

**Target files:**
- `tools/cq/macros/calls.py`
- `tools/cq/query/sg_parser.py`

**Deprecate/delete after completion:**
- Default reliance on rpygrep for callsite discovery.

**Implementation checklist:**
- [ ] Ensure `cmd_calls` uses ast-grep results as the primary path.
- [ ] Preserve rpygrep fallback behind explicit failure conditions.
- [ ] Report scan_method consistently (`ast-grep` / `rpygrep`).

---

## Scope Item 3: On-Demand AST + Symtable + Bytecode Enrichment

**Objective:** Enrich each callsite with symtable and bytecode signals, computed only for files that actually contain call matches.

**Representative snippet:**

```python
symtable_map = analyze_symtable(source, rel_path)
bytecode_info = analyze_bytecode(code_obj)
callsite.details["symtable"] = {
    "is_closure": scope.has_free_vars,
    "free_vars": scope.free_vars,
}
callsite.details["bytecode"] = {
    "load_globals": info.load_globals,
    "load_attrs": info.load_attrs,
}
```

**Target files:**
- `tools/cq/macros/calls.py`
- `tools/cq/query/enrichment.py`
- `tools/cq/introspection/symtable_extract.py`

**Deprecate/delete after completion:**
- Any reliance on repo-wide `DefIndex.build(...)` for enrichment-only purposes.

**Implementation checklist:**
- [ ] Parse AST only for files with matching call records.
- [ ] Use `SymtableEnricher` or `analyze_symtable` on those files only.
- [ ] Compute bytecode info for the containing function (best-effort).
- [ ] Add confidence adjustments based on hazards + symtable/bytecode signals.

---

## Scope Item 4: rpygrep-Based Context Windows

**Objective:** Provide a context window for each callsite: module path + function block boundaries (start/end lines), derived using rpygrep + indentation, not whole-repo AST indexing.

**Representative snippet:**

```python
# 1) rpygrep all def/async def lines in the file
# 2) choose nearest preceding def whose indent <= call indent
# 3) compute end line as next def at same/less indent - 1
context = {
    "module": rel_path,
    "start_line": def_line,
    "end_line": end_line,
}
```

**Target files:**
- `tools/cq/macros/calls.py`
- `tools/cq/search/adapter.py` (if new helper for def-line discovery)

**Deprecate/delete after completion:**
- Callsite output that lacks module context and enclosing function bounds.

**Implementation checklist:**
- [ ] Add rpygrep helper to locate `def`/`async def` lines per file.
- [ ] Compute containing function block by indent-based boundary detection.
- [ ] Add `context_window` (module, start/end lines) to callsite details.
- [ ] Optionally include a small snippet or preview for the context window.

---

## Scope Item 5: No Caching Anywhere in CQ

**Objective:** Remove all cache code and CLI/config plumbing so CQ runs are deterministic and uncached.

**Representative snippet:**

```python
# Remove cache modules and CLI flags
# Remove cache wiring in executor/commands/bundles
```

**Target files:**
- `tools/cq/cache/*`
- `tools/cq/index/diskcache_*`
- `tools/cq/cli_app/commands/admin.py`
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/report.py`
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/config.py`
- `tools/cq/cli_app/config_types.py`
- `tools/cq/cli_app/params.py`
- `tools/cq/core/bundles.py`
- `tools/cq/query/executor.py`

**Deprecate/delete after completion:**
- `IndexCache`, `QueryCache`, `diskcache_profile`, cache CLI commands, and config options.

**Implementation checklist:**
- [ ] Remove cache modules and references across CQ.
- [ ] Remove cache CLI flags and config fields.
- [ ] Update docs to omit caching references.
- [ ] Ensure `rg "cache" tools/cq` is empty aside from unrelated uses.

---

## Scope Item 6: Calls Output Schema + Evidence Enhancements

**Objective:** Extend `calls` output to include context windows and enrichment signals; keep output stable via msgspec.

**Representative snippet:**

```python
Finding(
    category="call",
    message=f"{function_name}({site.arg_preview})",
    anchor=Anchor(file=site.file, line=site.line, col=site.col),
    details={
        "context_window": {
            "module": site.file,
            "start_line": start_line,
            "end_line": end_line,
        },
        "symtable": symtable_info,
        "bytecode": bytecode_info,
    },
)
```

**Target files:**
- `tools/cq/macros/calls.py`
- `tools/cq/core/schema.py` (if additional typing is needed)

**Deprecate/delete after completion:**
- Callsite outputs without context window and confidence signals.

**Implementation checklist:**
- [ ] Add new fields to callsite details (context_window, symtable, bytecode).
- [ ] Surface enrichment stats in summary and sections.
- [ ] Ensure msgspec compatibility for new fields.

---

## Validation Plan (Real Searches)

> All validation commands must run with `uv run` (no direct `python`).

1) **ast-grep-py call discovery**
```bash
uv run python -m tools.cq.cli calls cmd_calls
```

2) **High-signal call target** (known function in repo)
```bash
uv run python -m tools.cq.cli calls build_graph_product
```

3) **Context window correctness** (inspect sample output)
```bash
uv run python -m tools.cq.cli calls resolve_call_targets
```

4) **Confirm no cache wiring**
```bash
rg "cache|IndexCache|QueryCache|diskcache" tools/cq
```

---

## Dependencies / Assumptions

- `ast-grep-py` is installed and available in the toolchain.
- `rpygrep` is available and behaves deterministically on the repo.
- Context window computation uses indentation heuristics rather than AST boundaries.

---

## Risks and Mitigations

- **Context window accuracy:** indentation-based bounds can be fooled by edge cases (e.g., decorators, multi-line defs). Mitigate with conservative heuristics and fall back to `<module>` context.
- **Performance on large repos:** avoid whole-repo AST passes; parse only files with matched call records.
- **Confidence signals:** symtable/bytecode failures should be best-effort, never fatal.

---

## Out-of-Scope

- Updating other macros to use the same context window strategy.
- Cross-language callsite extraction.

