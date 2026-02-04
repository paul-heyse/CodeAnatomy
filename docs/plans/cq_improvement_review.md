
According to a document from **February 4, 2026**, your **cq** tool already has a very strong “LLM-friendly” foundation: a layered architecture (CLI → context → commands/macros → optional index → core result/rendering) and a **structured Markdown result** format intended to be injected directly into agent context. It also explicitly positions `calls` as **on-demand (no full index build)** while still providing context enrichment. 

Below is what I would change to make it *best-in-class* for agents **without pre-indexing/caching the repo**, and to specifically displace “plain ripgrep” for general search with something agents naturally adopt.

---

## What’s already working (and why agents gravitate to it)

### 1) `calls` is “immediately useful”

It gives an agent exactly what they need before modifying a function: where it’s used, how it’s called, and **a context window/snippet** to understand the call site quickly.

That’s why it gets used.

### 2) `q entity=...` is “simple enough”

Agents can use `entity=function name=...` to locate definitions and move on.

### 3) You already have the primitives to beat grep

The docs and tool design lean into semantic/structural analysis instead of raw text search, and `q` explicitly describes an (intended) “ripgrep prefilter → ast-grep scan → build findings” pipeline.

---

## Why “general search” hasn’t been displaced yet

In practice, agents use ripgrep because:

1. **It’s zero-friction**: `rg foo` is always the fastest path to “show me occurrences”.
2. **They don’t want to design a query** (pattern DSL, entity type selection, scoping) when they’re still orienting.
3. Even if `q pattern=...` can do it structurally, pattern queries are still “planning overhead”.

So the winning move is: **give them an “rg-like” command that returns higher-signal results by default**, and only *optionally* expose the deeper knobs.

---

## The single biggest improvement: add a “Smart Search” that is strictly better than rg

### Goal

Replace 80% of `rg` usage with a single cq entrypoint that:

* behaves like grep at the interface level (low effort)
* returns **semantic context** (high signal)
* requires **no upfront index**
* scales by using ripgrep as a *candidate generator*, then enriches results on-demand

### Recommended UX (keep command set small)

You have two good options:

#### Option A (cleanest): add one command: `/cq search <query>`

* Agents will learn exactly one new thing.
* It cleanly “replaces grep”.

#### Option B (even lower friction): make `/cq q "<anything-without-=>"` fall back to Smart Search

* If the `q` parser doesn’t see `key=value`, interpret the whole string as a search query.
* So agents can do: `/cq q repo_root` and get “better grep”.

If you want to keep the visible surface area minimal, I’d do **Option B + also expose `/cq search` as an alias**.

---

## What “Smart Search” should return to beat ripgrep

### 1) Always return *grouped* results, not a flat stream

A flat list is what grep does; cq should do better.

For example, group by:

* **Containing function/method** (default for Python files)
* fallback: **file** for non-Python files

This makes results immediately actionable for an agent (“oh, it appears in these 3 functions”).

### 2) Always include “context that answers: what is this usage?”

For each match cluster, include:

* the containing def signature (or at least `file:line` + `def name(...)`)
* a snippet of the containing function (you already do this for `calls`)
* the exact matched line(s) highlighted or at least clearly indicated

### 3) Classify match *kind* (this is where you surpass grep)

For Python files, classify each match into buckets like:

* **definition** (`def foo`, `class Foo`)
* **call** (`foo(...)`)
* **import** (`from x import foo`, `import foo`)
* **attribute access** (`obj.foo`)
* **string/comment/docstring** occurrence

Then either:

* show only “code” occurrences by default (definition/call/import/attr), and
* tuck “string/comment” into a collapsed/secondary section (or show counts + “rerun with --include-strings”).

Agents overwhelmingly want “real code references”, not docstrings and comments, unless they explicitly ask.

### 4) Rank results by “likely relevance”

Even without indexing, you can rank well with heuristics:

* code > tests > docs (but don’t hide tests; just rank lower)
* definitions > callsites > attributes > strings/comments
* shorter path depth (e.g., `src/...` often more central)
* exact word-boundary matches over substrings

### 5) Add a “next-step suggestions” footer

This is an adoption hack.

If the query looks like a symbol name, add:

* “If you’re planning to change this function: run `/cq calls <symbol>`”
* “To find definitions: `/cq q "entity=function name=<symbol>"`”
* “To see callers transitively: `/cq q "entity=function name=<symbol> expand=callers(depth=2)"`”

This directly aligns with your AGENTS.md “use skills before guessing” contract.

---

## How to implement Smart Search without indexing

### Phase 1: ripgrep candidate generation (fast)

Use your existing rpygrep wrapper (`search_content`) to get `(file, line, text)` tuples.

Enhancements over today:

* add `--context N` (before/after) by *reading the file once* and slicing lines
* cap per-file matches and total matches (you already have SearchLimits)
* collect per-file match counts for summaries

### Phase 2: on-demand enrichment per matched file (only when useful)

For Python files only, enrich each matched line with:

* containing function/class span
* signature line (or `def/class` header)
* “kind classification” (call/import/def/etc.)

You can do this without indexing by:

* parsing only the matched files (often tens, not thousands)
* caching parse results **in-memory for the duration of the command**

### Phase 3: render as CqResult using your existing report pipeline

You already have a great output pipeline: put match groups into `Section`s and set `context_snippet/context_window` so Markdown rendering “just works.”

---

## Second biggest improvement: make `q` feel fast by aggressively prefiltering with rg

Your documentation already states an intended step: “Optional ripgrep prefilter narrows files by name pattern.”

To make `q entity=function name=Foo` beat “rg Foo”, you want:

* If `name=` is present (exact or regex), generate a **cheap ripgrep prefilter** that finds candidate files.

  * For functions: `def\s+NAME\s*\(` (as docs describe)
  * For classes: `class\s+NAME\s*[:\(]`
  * For imports: `import NAME|from .* import .*NAME`

Then run ast-grep scanning only on those candidates.

This one change tends to:

* make entity queries “feel instant”
* reduce the psychological pull of raw rg
* reduce compute spent parsing files that clearly can’t match

---

## Third improvement: make include/exclude affect scanning, not just post-filtering

Right now, CLI `--include/--exclude` is documented and used for filtering outputs, but for agent ergonomics the real win is:

* if the agent says “search in src/”, don’t even scan `tests/`

So:

* plumb include/exclude into macros and into `q`’s scan scope
* keep the post-filter too, but treat it as a final cleanup stage, not the main mechanism

This also reduces timeouts and makes results tighter, which improves agent trust.

---

## Fourth improvement: unify “lookup a symbol” into one default workflow

Agents don’t want to decide between:

* “entity query?”
* “calls macro?”
* “imports macro?”
* “impact?”

So give them one “starter” operation.

### Best pattern:

When Smart Search input looks like a symbol (identifier or dotted name), produce a **multi-panel result**:

1. **Definitions** (top 1–5)
2. **Imports** (top N)
3. **Callsites** (top N, possibly just counts + a few representatives)
4. “Next steps” suggesting `/cq calls` or `/cq impact`

This displaces a *lot* of ad-hoc grep use and reduces “agent planning overhead” to near-zero.

---

## Fifth improvement: make `calls` even more “refactor-ready” with better call resolution confidence

Your docs pitch “intelligent call resolution,” and the `calls` macro already captures hazards and context windows.

To push it into “best-in-class” territory without indexing:

### Add lightweight, per-file name binding

For each candidate file (already being parsed), resolve whether `foo(...)` refers to the intended target by:

* checking local defs
* checking `from x import foo as bar` aliasing
* checking `import module as m` then `m.foo(...)` patterns
* (optionally) symtable signals for locals vs globals

Then label callsites with:

* `resolved_high`, `resolved_med`, `unresolved`
* and show a summary histogram by confidence

This reduces false positives and increases agent willingness to trust `/cq calls` output as “the ground truth”.

---

## Sixth improvement: “progressive disclosure” output for high-recall commands

Agents stop reading if outputs are huge.

So default behaviors should be:

* show **top clusters** + **representative examples**
* show counts for the rest
* always save full JSON artifact for retrieval (you already do artifacts)

Example:

* “128 matches in 34 files”
* “Top 10 contexts shown”
* “Artifact contains full list”

This keeps context injection high-signal.

---

## Small, high-leverage UX improvements (these matter for LLM adoption)

### 1) Add “did you mean?” suggestions

If `/cq calls Foo.bar` finds nothing:

* suggest possible matches found by fuzzy search (case differences, underscores, partial matches)

### 2) Add “Explain interpretation” in Smart Search

Show:

* detected mode: identifier vs literal vs regex
* word boundary behavior
* file globs used
* whether strings/comments were included

### 3) Make `q` easier for simple cases

Even if you keep the query-string DSL, consider supporting equivalent flags:

* `/cq q --entity function --name build_graph_product --in src/`
  This reduces quoting errors and mental overhead.

---

## Putting it together: a practical “best-in-class” roadmap

If you want the shortest path to big adoption gains:

### Step 1 (highest ROI)

Implement **Smart Search** (either `/cq search` or `q` fallback).

### Step 2

Add **rg-prefilter for `q entity=... name=...`**, so entity queries feel instant and “grep-like.”

### Step 3

Make **include/exclude affect scan scope** in macros (especially `calls`) so agents don’t have to think about performance tuning.

### Step 4

Improve `calls` with **lightweight per-file binding confidence**, so it becomes *more authoritative than grep*, not just “nicer formatting.”

---

## Why this stays within your “no pre-index/cache” constraint

Everything above works by:

* using rg as a *candidate generator*
* parsing/enriching only the small subset of files that actually matched
* optionally caching parse results **in-memory for one run** (not a repo index)
* saving artifacts as outputs (which you already do), not as a required precomputed index

This preserves your core philosophy while making the tool feel like “grep, but smarter.”

---

If you want, I can also propose a concrete Markdown layout for Smart Search output that matches your existing `CqResult` conventions (Summary → Key Findings → Sections → Evidence) so it blends seamlessly with the rest of cq’s outputs and maximizes the chance agents will actually read it.

According to a document from **February 4, 2026**, every cq command returns a **CqResult** with **summary → key_findings → sections → evidence** (plus artifacts), and your Markdown renderer is already optimized around that exact ordering. 

Below is a **concrete “Smart Search” Markdown layout** that fits that structure exactly and is designed so agents can skim in ~10 seconds, then dive only if needed.

````md
# cq search

## Summary
- **query**: "<original user input>"
- **mode**: "smart" | "literal" | "regex" | "symbol"
- **pattern**: "<actual rg/sg pattern used>"          # if different from query
- **scope**: "<root-relative dir or 'repo'>"
- **include**: "<glob(s) or '~regex'>"
- **exclude**: "<glob(s) or '~regex'>"
- **file_globs**: ["*.py", "*.pyi"]                   # keep short; avoid big JSON blobs
- **files_scanned**: <N>
- **files_with_matches**: <N>
- **total_matches**: <N>
- **top_files**: "path/a.py (12), path/b.py (7), ..." # render as a single string for readability
- **caps_hit**: "none" | "max_files" | "max_total_matches" | "timeout"
- **tldr**: "<one sentence: what the matches are mostly about + where to look first>"

## Key Findings
- [impact:high] [conf:high] Best starting point: <why this file/symbol is the ‘center’> (`path/to/file.py:123`)
  Context (lines 110-140):
  ```python
  # <short snippet showing the match in its natural semantic unit>
  # <include a caret/comment marker on the matching line if helpful>
````

* [impact:med] [conf:high] Likely definition(s) of `<symbol>` found: `<qualified name>` (`path/to/defs.py:45`)
  Context (lines 40-60):

  ```python
  def <symbol>(...):
      ...
  ```
* [impact:med] [conf:med] Most common usage pattern: `<pattern summary>` (`path/to/usage.py:88`)
  Context (lines 80-95):

  ```python
  # example usage (most representative call-site / import / constant)
  ```
* [impact:low] [conf:high] Note: matches are concentrated in tests/docs; consider excluding `<dir>` (`tests/test_x.py:12`)

### Top Matches (ranked)

* [impact:high] [conf:high] <rank #1: concise match label> (`path/to/file.py:123`)
  Context (lines 110-140):

  ```python
  ...
  ```
* [impact:high] [conf:med] <rank #2: concise match label> (`path/to/file.py:210`)
  Context (lines 205-220):

  ```python
  ...
  ```
* [impact:med] [conf:med] <rank #3 ...> (`path/to/file.py:77`)
  Context (lines 70-90):

  ```python
  ...
  ```

### Matches by Kind

* [impact:med] [conf:high] Definitions: <N> (functions/classes/vars)
* [impact:med] [conf:high] Call sites: <N>
* [impact:low] [conf:high] Imports/exports: <N>
* [impact:low] [conf:med] String/comment/config mentions: <N>
* [impact:low] [conf:med] Tests-only matches: <N>

### Definitions

* [impact:high] [conf:high] function: `<qualified name>` (`path/to/file.py:45`)
  Context (lines 40-65):

  ```python
  def <name>(...):
      ...
  ```
* [impact:med] [conf:high] class: `<name>` (`path/to/file.py:120`)
  Context (lines 118-155):

  ```python
  class <name>:
      ...
  ```

### Usages

* [impact:med] [conf:high] called from `<containing function>` (`path/to/file.py:88`)
  Context (lines 80-95):

  ```python
  ...
  <name>(...)
  ...
  ```
* [impact:low] [conf:med] imported as `<alias>` (`path/to/file.py:12`)
  Context (lines 1-20):

  ```python
  from ... import <name> as <alias>
  ```

### Suggested Followups

* [impact:med] [conf:high] If you’re refactoring `<symbol>`, run: `/cq calls <symbol>`
* [impact:med] [conf:high] If you’re changing a parameter, run: `/cq impact <symbol> --param <p>`
* [impact:low] [conf:high] If you need structural search, run: `/cq q "pattern='<ast-grep pattern>'"`

## Evidence

* [impact:low] [conf:low] raw match: `<line preview>` (`path/to/file.py:301`)
* [impact:low] [conf:low] raw match: `<line preview>` (`path/to/other.py:19`)
* [impact:low] [conf:low] raw match: `<line preview>` (`path/to/third.py:77`)
* *... and <N> more evidence items*

## Artifacts

* `.cq/artifacts/search_<run_id>.json` (json)

---

*Completed in <ms>ms | Schema: <schema>*

```

### Why this layout tends to “get read”
- **Summary has a real TL;DR** and the “where should I look first?” answer (`top_files` + `tldr`).
- **Key Findings stays tiny** (aim for 3–6 bullets max) and uses **context snippets** so agents don’t bounce out to open files.
- **Sections are predictable** (Top Matches → Kinds → Definitions/Usages → Followups), so agents learn the muscle memory quickly.
- **Evidence is explicitly “raw”** (lower confidence), which prevents agents from treating grep-lines as ground truth. The schema’s separation of `sections` vs `evidence` is exactly meant for that kind of trust layering. :contentReference[oaicite:1]{index=1}

If you want, I can also propose the **exact `summary` keys + section titles** you should standardize on (so outputs are consistent across repos and agents learn them fast), but the block above is ready to drop into your implementation as-is.
```

Below is a **drop-in “contract”** for Smart Search that (a) matches cq’s existing **Summary → Key Findings → Sections → Evidence** shape and (b) makes outputs predictable so agents learn the layout once and then skim on autopilot. This aligns with how other cq macros already describe their output sections and with cq’s emphasis on context enrichment.

---

## Standard `summary` keys for Smart Search

### Summary keys must be

* **snake_case**
* **stable + ordered** (always emit in the same order; dict insertion order makes this easy)
* **present even when zero/empty** (agents learn where to look; avoids “did it forget to compute X?”)

### Canonical `result.summary` (ordered)

1. **`query`** *(str)*
   The exact user query string as provided (not “normalized”).

2. **`mode`** *(str enum)*
   One of:

   * `literal` (plain text)
   * `regex` (rg-style regex)
   * `identifier` (identifier/symbol-ish query; enables def/use classification)
   * `structural` (ast-grep-style pattern; optional if you support it)

3. **`file_globs`** *(list[str])*
   Effective file globs searched (e.g., `["*.py", "*.pyi"]`). If you also support language sets, put them here as globs rather than adding a separate `language` key (works across repos/languages).

4. **`include`** *(list[str])*
   Effective include patterns (from global `--include` + any command-local include). cq already standardizes `--include/--exclude` as global filters, so surfacing the resolved values in summary makes Smart Search feel “native.”

5. **`exclude`** *(list[str])*
   Effective exclude patterns.

6. **`context_lines`** *(dict)*
   Always emit as:

   * `{"before": int, "after": int}`
     (Even if you also emit richer “containing function” context later—this communicates the *baseline* context window policy.)

7. **`limit`** *(int)*
   Max returned “Top Matches” findings (post-ranking). (This should correspond to what the user thinks of as “how many results did I ask for?”)

8. **`scanned_files`** *(int)*
   How many files you actually scanned in the final search stage (post-include/exclude and any prefiltering decisions).

9. **`matched_files`** *(int)*
   Files that contain ≥ 1 match.

10. **`total_matches`** *(int)*
    Total raw match occurrences found (pre-ranking / pre-limit).

11. **`returned_matches`** *(int)*
    How many matches you actually surfaced (typically `min(total_matches, limit)` but not always if you group/merge).

12. **`scan_method`** *(str enum)*
    One of:

* `rpygrep` (your current rg wrapper)
* `ast_grep`
* `hybrid` (if you do a two-stage rg → AST validate/enrich)

(This mirrors how other macros record their scan method / evidence kind and helps agents calibrate trust quickly.)

### Optional-but-standard keys (only include when non-default)

13. **`ranker`** *(str)*
    E.g., `tfidf_v1`, `path_boost_v1`, `recency_v1`. Only emit if you truly rank results; omit if you just return rg order.

14. **`truncated`** *(bool)*
    `true` if you hit timeouts, max-files, max-matches, or otherwise didn’t fully exhaust the search space.

15. **`timed_out`** *(bool)*
    `true` if any search stage timed out (even if you still have partial results).

16. **`notes`** *(list[str])*
    Short operator notes that agents actually read, e.g.:

* `"regex mode forced case_sensitive=true"`
* `"structural validation skipped for 412 matches (limit reached)"`

> If you only adopt *one* optional key: adopt `truncated`. It prevents silent failure modes.

---

## Standard section titles for Smart Search

These are the exact **`Section.title`** strings I’d standardize on. Keep them **Title Case**, no punctuation, and keep the set small so agents remember it.

### Always present (when there is ≥ 1 match)

1. **`Top Matches`**
   The ranked, most useful match sites with the best context (your “smart” value-add).

2. **`Uses by Kind`**
   Aggregated context classification: call, import, attribute access, type annotation, docstring/comment, string literal, etc.
   (This is the “how is it used?” replacement for what agents currently do manually with repeated greps.)

3. **`Hot Files`**
   File-level clustering: `path → match_count`, plus the top 1–3 representative hits per hot file.

4. **`Suggested Follow-ups`**
   Concrete next commands in cq terms (e.g., “run `/cq calls X`”, “run `/cq q entity=function name=~…`”), so agents don’t have to invent their own investigation plan. This mirrors the spirit of cq being a “high-signal” guided tool rather than raw grep.

### Conditionally present

5. **`Likely Definitions`** *(only if mode is `identifier` and you found plausible defs)*
   Function/class defs, constants, imports/aliases that define the symbol. (This should be *small* and high-confidence.)

6. **`Limitations`** *(only if `truncated` or `timed_out` or you skipped enrichment stages)*
   Bullet findings explaining what you didn’t do (and why), so agents don’t misinterpret silence as absence.

### Evidence section usage (already standard in cq)

You don’t control the heading (it’s always **`## Evidence`**), but you *do* control what goes there. The most consistent convention is:

* Put **raw match artifacts / low-level proofs** in Evidence (e.g., “rg hit at file:line with minimal snippet”), while keeping “Top Matches” more curated and enriched. This matches how cq macros keep “Evidence” as supporting material.

---

## Copy/paste template (keys + section titles in one place)

```python
result.summary = {
    "query": query,
    "mode": mode,  # literal|regex|identifier|structural
    "file_globs": file_globs,
    "include": include,
    "exclude": exclude,
    "context_lines": {"before": before, "after": after},
    "limit": limit,
    "scanned_files": scanned_files,
    "matched_files": matched_files,
    "total_matches": total_matches,
    "returned_matches": returned_matches,
    "scan_method": scan_method,  # rpygrep|ast_grep|hybrid
    # Optional keys (only when non-default):
    # "ranker": ranker,
    # "truncated": truncated,
    # "timed_out": timed_out,
    # "notes": notes,
}

sections = [
    Section(title="Top Matches", findings=top_matches),
    Section(title="Uses by Kind", findings=uses_by_kind),
    Section(title="Hot Files", findings=hot_files),
    Section(title="Suggested Follow-ups", findings=followups),
    # Conditional:
    # Section(title="Likely Definitions", findings=definitions),
    # Section(title="Limitations", findings=limitations),
]
result.sections = sections
```

If you want, I can also propose the **exact Finding.category strings** to standardize inside those sections (so downstream tooling and agent prompting can key off categories consistently), but the above gives you the “learn-once, skim-fast” contract you asked for.

According to a document from **February 4, 2026**, your `cq` output schema already uses `Finding.category` values like **`import` / `from_import`** (for `entity=import`) and **`definition`** (for `entity=function/class`), and it recognizes **`callsite`** as an entity type. So the “best-in-class” move is: **reuse those exact strings wherever possible** (so agents learn once), and then add a *small* set of new categories that cover the remaining “search” surface area (text/path/docstrings/comments), plus graph/hazard add-ons.

Below is the **exact category registry** I’d standardize on for the Smart Search output contract (and it won’t fight your existing conventions).

---

## Finding.category registry to standardize on

### 1) Meta categories (Key Findings + operational notes)

Use these sparingly; they exist to help agents “decide what to do next” quickly.

* `summary`
* `disambiguation`
* `next_step`
* `no_results`
* `truncated`

**Rule:** don’t use `info`/`warning`/`error` as `category`—those belong in `Finding.severity` (and you already have `--severity` filtering).

---

### 2) Code-entity categories (these should feel identical to `/cq q …`)

These are the categories that should dominate Smart Search output when you can structurally classify matches.

* `definition`
  *Reuse exactly; aligns with `entity=function/class` output*

  * Put the specific kind in `details.kind` (`function`, `class`, `method`, etc.) like you already do.

* `callsite`
  *Reuse exactly; aligns with the `callsite` entity type*

* `import`
  *Reuse exactly*

* `from_import`
  *Reuse exactly*

* `pattern_match`
  (For “structural search” style results; this mirrors your existing naming pattern and matches the mental model of “AST-aware hit”.)

* `reference`
  (Name/attribute reference that is **not** a call. This is the missing middle between `callsite` and `text_match`.)

* `assignment`
  (Write-sites: `x = …`, `obj.attr = …`, `x += …`, destructuring assigns, etc.)

* `annotation`
  (Type-hint / signature annotation hits: `x: T`, `def f(x: T) -> U`, `TypeAlias`, etc.)

---

### 3) “Search” categories (fills the gap vs ripgrep)

These are what make Smart Search a *strict upgrade* over raw `rg`, while staying on-demand (no pre-index).

* `text_match`
  (Fallback when you have a content hit but you *cannot* or *did not* classify it structurally.)

* `path_match`
  (When the query matched the **file path** / module path / filename rather than content.)

* `docstring_match`
  (Hit is inside a docstring. This directly addresses the “grep finds mentions that aren’t code usage” problem.)

* `comment_match`
  (Hit is inside a comment.)

* `string_match`
  (Hit is inside a non-docstring string literal; e.g., SQL fragments, templates, CLI strings.)

**Practical note:** these three (`docstring_match`, `comment_match`, `string_match`) are extremely agent-friendly because they instantly communicate “likely not a runtime reference” without reading the snippet.

---

### 4) Graph/relationship categories (only if Smart Search includes “related usage”)

If your Smart Search includes optional “who calls this / what does this call” expansions, standardize these:

* `caller`
* `callee`

(These align naturally with your graph expansion model that already names `callers`/`callees` as traversal operators.)

---

### 5) Hazard category (optional, but consistent)

If Smart Search adds a “Hazards nearby” section, don’t explode the category space—keep it simple:

* `hazard`

Put the hazard type/pattern in details (e.g., `details.data["pattern"] = "dynamic_dispatch"`), since hazards already have stable pattern names like `dynamic_dispatch` and `forwarding` in your docs.

---

## Section → allowed categories mapping

This is the “don’t make agents learn more than they need” part: each section should only emit a small subset of categories.

* **Key Findings**

  * `summary`, `disambiguation`, `next_step`, `no_results`, `truncated`
  * (Optionally: include a *single* `definition` or `callsite` finding as “top hit” if you want.)

* **Primary Matches**

  * `definition`, `callsite`, `import`, `from_import`, `reference`, `assignment`, `annotation`, `pattern_match`
  * (Fallback: `text_match`)

* **Non-code Mentions**

  * `docstring_match`, `comment_match`, `string_match`

* **Path Hits**

  * `path_match`

* **Related Call Graph** (if enabled)

  * `caller`, `callee`

* **Hazards** (if enabled)

  * `hazard` (with hazard pattern info in details)

* **Evidence**

  * (I’d standardize all evidence items to `evidence` *or* keep evidence as separate `CqResult.evidence` entries but still use one of the meta categories.)
  * If you want to be strict: **Evidence list always uses `evidence`** and `details.kind` differentiates sub-kinds.

---

## Drop-in “registry” block you can literally paste into docs

```text
Finding.category (Smart Search contract)

Meta:
- summary
- disambiguation
- next_step
- no_results
- truncated

Code entities:
- definition
- callsite
- import
- from_import
- pattern_match
- reference
- assignment
- annotation

Search hits:
- text_match
- path_match
- docstring_match
- comment_match
- string_match

Relationships (optional):
- caller
- callee

Hazards (optional):
- hazard
```

If you want, next step I can propose the **exact `details.kind` strings + `details.data` keys** that pair with each of these categories (so “category tells you what it is” and “details tells you what to do with it”), but the above is the tight, learn-once registry that downstream tooling and agent prompting can key off reliably.

Below is a **drop-in “details registry”** you can standardize for Smart Search so:

* **`Finding.category`** stays a *small, learn-once* set (“what it is”)
* **`Finding.details.kind`** is a *stable discriminator* (“how to interpret + what to do next”)
* **`Finding.details.data`** uses **predictable keys** so agents (and any downstream tooling) can skim + act without extra planning.

I’m assuming your Smart Search categories are the ones we standardized around the output sections (Primary/Defs/Calls/Refs/Imports/Matches/etc.). If you ended up with slightly different names, you can keep the **kind + data contracts** below and just remap the category strings.

---

# Smart Search v1: `details.kind` + `details.data` registry

## Global conventions (apply to *all* Smart Search findings)

### A) Optional universal keys (high leverage, low noise)

These keys are allowed in **any** finding’s `details.data`:

* `rank`: `int` — 1 = best within its section; stable ordering hint.
* `score`: `float` — retrieval score (your search rank), *not* cq impact/confidence.
* `tags`: `list[str]` — tiny labels like `["tests"]`, `["generated"]`, `["public_api"]`.
* `snippet`: `str` — multi-line excerpt intended for humans (can mirror `context_snippet`).
* `snippet_lang`: `str` — `"python" | "toml" | "md" | "json" | "bash" | ...`
  *(If you later update the renderer to not hardcode ```python, this becomes instantly valuable.)*
* `context_window`: `{ "start_line": int, "end_line": int }` — matches your existing convention.
* `context_snippet`: `str` — matches your existing convention (so markdown renderer auto-expands it).
* `actions`: `list[ActionHint]` — copy/paste next steps (schema below).

### B) `actions` object schema (copy/paste friendly + machine-parseable)

```json
{
  "kind": "cq.calls | cq.impact | cq.q | open.file | open.symbol | search.refine",
  "title": "Short label shown to agent",
  "cmd": "/cq calls foo",
  "why": "One sentence: what you learn / why it matters",
  "priority": 1
}
```

Notes:

* Keep `priority` as **1–5** (1 = “do this next”).
* Keep `cmd` as **exactly runnable** in the agent environment.

---

## Category → `details.kind` + `details.data` contract

### 1) `Finding.category = "primary"`

Use for the single “best answer” item you want agents to read first (often goes in **Key Findings**).

* **`details.kind`**: `"smart_search.primary"`
* **`details.data` keys**

  * **required**

    * `primary_type`: `"symbol" | "file" | "match"`
    * `why`: `str` (short, decisive rationale)
  * **optional**

    * `symbol`: `str` (if `primary_type="symbol"`)
    * `symbol_kind`: `"function" | "class" | "method" | "module" | "constant" | "variable"`
    * `path`: `str` (if `primary_type="file"` and/or anchor not present)
    * `query_normalized`: `str` (what Smart Search actually ran)
    * `rank`, `score`, `tags`, `actions`
    * `context_window`, `context_snippet`, `snippet`, `snippet_lang`

**Intent:** “Here’s the best place to start; here’s exactly why.”

---

### 2) `Finding.category = "definition"`

Symbol definitions (functions/classes/methods/constants).

* **`details.kind`**: `"symbol.definition"`
* **`details.data` keys**

  * **required**

    * `symbol`: `str` (short name)
    * `symbol_kind`: `"function" | "class" | "method" | "constant" | "variable" | "module"`
    * `qualname`: `str` (best-effort fully-qualified name, e.g. `pkg.mod.Class.method`)
  * **optional**

    * `signature`: `str` (for callables)
    * `module`: `str` (e.g. `pkg.mod`)
    * `visibility`: `"public" | "internal" | "private"`
    * `doc_summary`: `str` (first line of docstring / comment)
    * `decorators`: `list[str]`
    * `bases`: `list[str]` (for classes)
    * `exports`: `list[str]` (if you detect re-export patterns)
    * `rank`, `score`, `tags`, `actions`
    * `context_window`, `context_snippet`, `snippet`, `snippet_lang`

**Best default actions** (put in `actions`):

* `cq.calls` for functions/methods
* `cq.q` for `entity=class` / `entity=function` neighbors

---

### 3) `Finding.category = "callsite"`

Concrete call sites (or best-effort candidates) where something is invoked.

* **`details.kind`**: `"usage.callsite"`
* **`details.data` keys**

  * **required**

    * `callee`: `str` (name as written or resolved best-effort)
    * `caller`: `str` (enclosing function/method name or `"<module>"`)
    * `call_text`: `str` (single-line pretty preview, e.g. `foo(a, b=1)`)
  * **optional**

    * `call_kind`: `"direct" | "method" | "attribute" | "dynamic" | "unknown"`
    * `args_count`: `int`
    * `kwargs`: `list[str]`
    * `has_star_args`: `bool`
    * `has_star_kwargs`: `bool`
    * `binding_confidence`: `"high" | "med" | "low"` (how sure you are it’s a real callsite)
    * `hazards`: `list[str]` (e.g. `["forwarding", "dynamic_dispatch"]`)
    * `call_id`: `str` (stable id if you already generate one)
    * `rank`, `score`, `tags`, `actions`
    * `context_window`, `context_snippet`, `snippet`, `snippet_lang`

**Best default actions:**

* `open.file` (jump to callsite context)
* `cq.q` for the caller definition (`entity=function name=<caller>`)

---

### 4) `Finding.category = "reference"`

Non-call references (attribute access, constant usage, type usage, etc.).

* **`details.kind`**: `"usage.reference"`
* **`details.data` keys**

  * **required**

    * `symbol`: `str` (what is being referenced)
    * `ref_text`: `str` (the fragment/line snippet that contains the reference)
  * **optional**

    * `ref_kind`: `"name" | "attribute" | "type" | "string" | "comment" | "unknown"`
    * `enclosing`: `str` (function/class name or `"<module>"`)
    * `rank`, `score`, `tags`, `actions`
    * `context_window`, `context_snippet`, `snippet`, `snippet_lang`

**Best default actions:**

* `open.file`
* if it looks like a type/annotation hotspot: `search.refine` suggestion to focus on `->` / `:` patterns (repo-dependent)

---

### 5) `Finding.category = "import"`

Import statements relevant to the query (useful for “where does this come from?”).

* **`details.kind`**: `"usage.import"`
* **`details.data` keys**

  * **required**

    * `import_kind`: `"import" | "from_import"`
    * `module`: `str` (e.g. `pathlib` or `pkg.subpkg`)
  * **optional**

    * `name`: `str` (imported symbol for `from ... import name`)
    * `alias`: `str` (if `as X`)
    * `import_text`: `str` (single-line normalized import statement)
    * `rank`, `score`, `tags`, `actions`
    * `context_window`, `context_snippet`, `snippet`, `snippet_lang`

**Best default actions:**

* `cq.imports` (if you expose module-focused import analysis)
* `open.file` (imports are usually near the top; context helps)

---

### 6) `Finding.category = "text_match"`

Plain-text/regex matches (your “better than ripgrep” result type).

* **`details.kind`**: `"match.text"`
* **`details.data` keys**

  * **required**

    * `pattern`: `str` (the regex/literal you searched)
    * `matched_line`: `str` (the full matched line)
  * **optional**

    * `match_spans`: `list[{ "start_col": int, "end_col": int }]` (highlighting)
    * `match_count_in_file`: `int`
    * `is_probably_definition`: `bool` (heuristic)
    * `is_probably_callsite`: `bool` (heuristic)
    * `rank`, `score`, `tags`, `actions`
    * `context_window`, `context_snippet`, `snippet`, `snippet_lang`

**Best default actions:**

* If heuristics say “probable definition”: `cq.q entity=function name=...`
* If heuristics say “probable callsite”: `cq.calls <symbol>` or refine query to `pattern='symbol($$$)'`

---

### 7) `Finding.category = "pattern_match"`

Structural matches (ast-grep style) returned by Smart Search.

* **`details.kind`**: `"match.structural"`
* **`details.data` keys**

  * **required**

    * `pattern`: `str` (the structural pattern)
    * `matched_text`: `str` (short excerpt of the matched node)
  * **optional**

    * `rule_id`: `str`
    * `captures`: `dict[str, str]` (e.g., `{ "$X": "foo", "$Y": "bar" }` rendered as text)
    * `match_kind`: `"astgrep" | "treesitter" | "other"`
    * `rank`, `score`, `tags`, `actions`
    * `context_window`, `context_snippet`, `snippet`, `snippet_lang`

**Best default actions:**

* `open.file`
* “refine pattern” suggestion action (`search.refine`) if captures show a narrowable metavariable

---

### 8) `Finding.category = "file"`

File-level hits (filename/path match, or file surfaced as “highly relevant”).

* **`details.kind`**: `"match.file"`
* **`details.data` keys**

  * **required**

    * `path`: `str` (repo-relative is best)
    * `why`: `str` (why this file is relevant)
  * **optional**

    * `file_role`: `"source" | "test" | "doc" | "config" | "script" | "generated" | "unknown"`
    * `top_symbols`: `list[{ "symbol": str, "symbol_kind": str, "line": int }]` (lightweight scan)
    * `rank`, `score`, `tags`, `actions`

**Best default actions:**

* `open.file`
* optionally: `search.refine` to restrict to this file / directory

---

### 9) `Finding.category = "related"`

“Related” entities (neighbors) surfaced by Smart Search: same module, same prefix, frequently co-occurring, etc.

* **`details.kind`**: `"smart_search.related"`
* **`details.data` keys**

  * **required**

    * `relation`: `"same_module" | "co_occurs" | "name_similarity" | "calls" | "imports" | "tests_cover" | "doc_mentions"`
    * `target_type`: `"symbol" | "file"`
    * `target`: `str` (symbol qualname or path)
  * **optional**

    * `why`: `str`
    * `rank`, `score`, `tags`, `actions`
    * for symbol targets:

      * `symbol_kind`, `qualname`

**Best default actions:**

* `open.symbol` or `open.file` depending on target type

---

### 10) `Finding.category = "hint"`

Search refinement / routing hints (keep these rare; they’re “tool UX”).

* **`details.kind`**: `"smart_search.hint"`
* **`details.data` keys**

  * **required**

    * `hint_kind`: `"refine_query" | "narrow_scope" | "broaden_scope" | "switch_mode"`
    * `why`: `str`
  * **optional**

    * `suggested_queries`: `list[str]` (exact strings to rerun)
    * `suggested_globs`: `list[str]`
    * `suggested_patterns`: `list[str]` (structural patterns)
    * `rank`, `score`, `tags`, `actions`

**Best default actions:**

* One `search.refine` action with an exact next query to run

---

### 11) `Finding.category = "hazard"`

Use when Smart Search detects “this will be tricky” situations: dynamic dispatch, heavy forwarding, generated code hotspots, etc.

* **`details.kind`**: `"smart_search.hazard"`
* **`details.data` keys**

  * **required**

    * `hazard_kind`: `"dynamic_dispatch" | "forwarding" | "metaprogramming" | "generated_code" | "reflection" | "unknown"`
    * `why`: `str`
  * **optional**

    * `evidence`: `list[str]` (tiny bullets, not huge dumps)
    * `rank`, `score`, `tags`, `actions`
    * `context_window`, `context_snippet`, `snippet`, `snippet_lang`

**Best default actions:**

* `cq.q pattern='getattr($X, $Y)'` / or your relevant structural pattern queries
* `cq.calls <fn>` if the hazard is around call indirection

---

# Tiny “golden rule” to keep this best-in-class

If you do **only one thing**: ensure every `definition`, `callsite`, `text_match`, and `pattern_match` finding includes:

* `context_window`
* `context_snippet`
* **and at least 1 `actions[]` item** (copy/paste next command)

That’s the minimum that reliably displaces “agent runs rg 8 times and scrolls”.

---
