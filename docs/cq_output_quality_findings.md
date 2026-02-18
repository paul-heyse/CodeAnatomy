# CQ Output Quality Assessment Findings

**Date:** 2026-02-18
**Scope:** search, q, calls, impact, sig-impact commands (markdown format)
**Method:** Static analysis of rendering pipeline + 19 empirical test queries

---

## Executive Summary

CQ outputs are **3-10x too verbose** for LLM agent consumption. A search for `build_graph_product` produces 1,201 lines (44KB) when an agent needs ~50-100 lines. The primary causes:

1. **No source code snippets** - The most valuable data for agents (actual code) is disabled by default
2. **Triple redundancy** - Callers, locations, and categories each shown 3 separate times
3. **Tool internals exposed** - Confidence, degradation, budget, enrichment telemetry waste ~25% of tokens
4. **Code Facts bloat** - Qualified Names #2-#8, Binding Candidates #1-#8, Incremental Enrichment metrics
5. **Calls dumps entire modules** - "Parent Scopes" in neighborhood preview outputs 700+ lines of raw source

## Empirical Results

| Query | Lines | Bytes | Signal | Key Issue |
|-------|-------|-------|--------|-----------|
| S1: search build_graph_product | 1,201 | 44KB | ~30% | Code Facts bloat, no snippets |
| S2: search config | 2,493 | 117KB | ~15% | Object over-splitting, massive |
| S3: search compile --in semantics | 1,738 | 62KB | ~20% | Scoped but still huge |
| S4: search register_udf --lang rust | 366 | 14KB | ~40% | Reasonable for Rust |
| S5: search regex pattern | 1,284 | 51KB | ~25% | Pattern matches verbose |
| S6: search TODO --include-strings | 717 | 27KB | ~20% | Non-code section works |
| S7: search nonexistent | 35 | 1.4KB | ~10% | 35 lines for "not found" |
| S8: search DataFrame | 2,788 | 121KB | ~15% | Cross-language explosion |
| Q1: entity build_cpg | 179 | 11KB | ~40% | Reasonable |
| Q2: entity expand=callers | 206 | 12KB | ~40% | Reasonable |
| Q3: pattern getattr | 298 | 13KB | ~45% | Good structural results |
| Q4: closure scope | 1,753 | 62KB | ~20% | 53 closures = massive |
| Q5: import Path | 258 | 15KB | ~35% | Import listings ok |
| Q6: mermaid visualization | 270 | 14KB | ~60% | Mermaid output is clean |
| Q7: rust pattern | 65 | 5KB | ~50% | Clean |
| C1: calls build_graph_product | 1,554 | 84KB | ~15% | Dumps entire module source |
| C2: calls SemanticCompiler.compile | 873 | 46KB | ~20% | Still verbose |
| C3: impact --param repo_root | 122 | 5KB | ~30% | Compact but Code Facts repeated |
| C4: sig-impact | 384 | 15KB | ~30% | Ok sites verbose |

**Median output: 366 lines. Mean: 820 lines. Target: 50-150 lines for typical queries.**

---

## Findings (Ranked by Impact)

### F1: Source Snippets Disabled for ALL Commands [CRITICAL]

**Evidence:** `report.py:629`: `show_context = result.run.macro != "search"` plus double-gate at `report.py:173-178` requiring `CQ_RENDER_CONTEXT_SNIPPETS=1` env var.

**Impact:** The data is already computed and attached to findings but never rendered. An LLM agent needs to see actual code at each match location. Without snippets, every search result forces a separate `Read` tool call.

**Fix:** Enable snippets by default for the primary target definition. Show 5-10 lines of source for the top definition match.

### F2: Code Facts "Qualified Name #2-#8" and "Binding Candidates #1-#8" Bloat [HIGH]

**Evidence:** S1 output lines 33-50 show 8 qualified names and 8 binding candidates for a single function. These are every symbol referenced within the function body, not the function's own identity.

**Impact:** ~40-60 tokens per enriched finding wasted on internal symbol tables. An agent doesn't need to know that `build_graph_product` references `_resolve_work_dir`, `emit_diagnostics_event`, `str`, `type`, `_resolve_run_id`, `start_build_heartbeat`, `reset_run_id` as qualified names.

**Fix:** Limit to the primary qualified name (index 0 only). Drop "Binding Candidates" entirely or limit to 2.

### F3: Calls Neighborhood Preview Dumps Entire Module Source [HIGH]

**Evidence:** C1 output lines 38-242+: "Parent Scopes" renders the entire `product_build.py` module (700+ lines) as the parent scope of `build_graph_product`.

**Impact:** 700 lines of raw source in a calls result that should show 6 call sites.

**Fix:** Truncate parent scope to the function's own docstring/signature, not the entire containing module.

### F4: Insight Card Low-Value Lines [HIGH]

**Evidence:** Every output starts with:
- `Degradation: semantic=skipped, scan=ok, scope_filter=none (not_attempted_by_design; tree_sitter_neighborhood_disabled_by_default)` (~20 tokens)
- `Budget: top_candidates=1, preview_per_slice=5, semantic_targets=1` (~15 tokens)
- `Artifact Refs: diagnostics=cache://... | telemetry=cache://...` (~30 tokens, two 64-char hashes)

**Impact:** ~65 tokens per output with zero agent utility. These are CQ developer diagnostics.

**Fix:** Move to `--verbose` only or a collapsed diagnostics section.

### F5: "Incremental Enrichment" Code Facts Cluster [HIGH]

**Evidence:** S1 lines 56-61, C3 lines 39-44, repeated for every enriched finding:
```
- Incremental Enrichment
  - Mode: full
  - Sym Scope Tables: 37
  - Binding Resolve Status: missing
  - CFG Edges: 217
  - Anchor Def/Use: 0
```

**Impact:** ~35 tokens per enriched finding. "CFG Edges: 217" and "Sym Scope Tables: 37" are never actionable for an agent.

**Fix:** Remove this cluster entirely from default output.

### F6: "Enrichment Language: python" on Every Code Facts Block [MEDIUM]

**Evidence:** Appears at the bottom of every Code Facts section (e.g., S1 line 62, C3 line 45, C4 line 49).

**Impact:** ~8 tokens per finding, shown even when the language is already obvious from the file extension.

**Fix:** Remove. Language is already in the Insight Card and Code Overview.

### F7: Callers/Callees Shown 3x [MEDIUM]

**Evidence:**
1. Insight Card: `callers=6, callees=25`
2. Insight Card Risk: `callers=6, callees=25, hazards=0, forwarding=0`
3. Neighborhood Preview section: full caller/callee listings

The Risk line is the main duplication - same counts in both neighborhood and risk.

**Fix:** Remove caller/callee counts from Risk line (they're already in Neighborhood line above).

### F8: Impact Reports Entire Code Facts Block for Each Caller [MEDIUM]

**Evidence:** C3 output shows identical Code Facts blocks (lines 15-45 and 49-81) for the same function appearing as both "target" and "caller".

**Impact:** The exact same enrichment data (37 sym scope tables, 217 CFG edges, etc.) rendered twice for the same function.

**Fix:** Deduplicate Code Facts when target and caller are the same entity.

### F9: Summary JSON Blob [MEDIUM]

**Evidence:** C3 lines 98-119: A single-line JSON blob with ~40 fields including `rg_pcre2_available`, `scan_method`, `enrichment_telemetry`, etc.

**Impact:** ~100-400 tokens of mostly-redundant telemetry data.

**Fix:** Remove from default output. Keep in `--verbose` or `--format json`.

### F10: No-Results Output Still 35 Lines [LOW]

**Evidence:** S7 output: 35 lines including full Insight Card, Code Overview, Key Findings for "xyznonexistent_symbol_12345".

**Impact:** A no-result output should be 3-5 lines: "No matches found for `xyznonexistent_symbol_12345`"

**Fix:** Short-circuit rendering when match count is 0.

### F11: Section Ordering Missing for q/impact/sig-impact [LOW]

**Evidence:** `_SECTION_ORDER_MAP` at `report.py:63-84` only covers "search" and "calls".

**Fix:** Add orderings for remaining command types.

---

## Recommended Changes (Priority Order)

1. **Suppress low-value Insight Card lines** (Degradation, Budget, Artifact Refs) - immediate token savings
2. **Suppress Incremental Enrichment Code Facts cluster** - biggest per-finding savings
3. **Limit Qualified Names to primary only, drop Binding Candidates** - second biggest per-finding savings
4. **Remove Enrichment Language line from Code Facts** - easy win
5. **Remove Summary JSON section from markdown output** - medium savings
6. **Remove callers/callees from Risk counters line** - dedup
7. **Fix Calls Parent Scopes to show only function context, not entire module** - C1 specific
8. **Shorten no-results output** - edge case but clean
9. **Enable source snippets for primary target definition** - biggest info gain (separate change)
10. **Add section ordering for q/impact/sig-impact** - output consistency
