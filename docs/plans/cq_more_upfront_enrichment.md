Here’s what I’d embed, based on how `cq` is currently shaped:

* **`search`** already does rich per-hit enrichment (AST + symtable + Python/Rust enrichment + Pyrefly prefetch) but the *most actionable “what is this symbol + what’s around it?”* is still mostly implicit / buried.
* **`q entity=…`** has *cheap access* to repo-wide structural relationships because the executor already builds `def_records`, `call_records`, interval indexes, and `calls_by_def` during the scan.
* **`calls`** is already “high signal” for refactors, but it’s missing the *one screen* “target card + neighborhood” context that prevents agents from needing a separate `nb` call.

So the move is: treat **search/calls/entity as your three “front doors”**, and have them emit a **small, standardized “Target Card + Neighborhood Preview + Risk Flags”** block automatically (budgeted), with deeper payloads pushed to artifacts.

---

## High-value “insight packs” to embed (pulled from the rest of cq)

These are the best adds from *all* other query types/macros (neighborhood, impact, sig-impact, scopes, bytecode-surface, imports, exceptions, side-effects), distilled into what an agent actually uses:

### 1) Target Card (identity + grounding)

**What it answers:** “What is this thing, where is it, and which candidate did you pick?”

* kind (function/class/method/module-ish), file:line:col
* best signature/type contract you can get (Pyrefly if available; otherwise structural signature)
* qualified name / enclosing class
* resolution confidence + ambiguity (multiple defs with same name; symbol-only fallback)
  **Why it matters:** prevents wasted follow-up calls just to “locate the real definition”.

### 2) Neighborhood Preview (the `nb` core, but compact)

**What it answers:** “What touches it and what does it touch?”
Budgeted to **top_k preview + totals** (same progressive disclosure model as SNB):

* callers (total + top examples)
* callees (total + top examples)
* references (total + top examples)
* implementations / overrides / base classes (when available)
* enclosing context + parents/children/siblings (structural containment)
  **Why it matters:** this is the “so what” that changes how an agent edits code.

### 3) Change Risk Flags (cheap, but extremely high leverage)

**What it answers:** “If I change this, what’s likely to break?”
A small set of boolean/enum flags + counts (no giant lists):

* call-surface size (incoming callers count; number of callsites; files-with-calls)
* *shape variance* (how many distinct arg-shapes; any `*args/**kwargs` forwarding)
* dynamic hazards (eval/exec/getattr/importlib.import_module/etc. — you already have a starter list in `calls`)
* closure capture / globals usage / heavy attribute usage (symtable/bytecode surface signals)
  **Why it matters:** agents pick safer edit strategies when they see risk early.

### 4) Dependency Context (imports + importers)

**What it answers:** “How does it enter/leave modules?”

* import alias chain / resolved import path (Pyrefly already gives this for many anchors)
* imports in target file (cheap from scan records)
* *reverse* importers (who imports this module/symbol) — even an approximate list is valuable
  **Why it matters:** refactors often fail due to import-level coupling, not callsites.

### 5) Reliability/Safety Glance (exceptions + side-effects)

**What it answers:** “Am I touching scary runtime edges?”

* exceptions raised/caught in target (counts + top types)
* import-time side effects (boolean + top files) if target is module-level relevant
  **Why it matters:** helps agents avoid “harmless” changes that alter runtime behavior.

### 6) Provenance + Degradation (confidence you can trust)

**What it answers:** “How much of this is real vs best-effort?”

* evidence_kind / confidence bucket
* LSP capability gates + degrade events (the SNB model is great here)
* fallback used (rg-only, partial scan, limited file set)

---

## What to embed into each of the three front-door commands

### A) `search` (default discovery loop)

**Trigger:** only when query mode is identifier-ish *and* you have at least one “definition” match with decent confidence.

**Embed:**

1. **Target Candidates (top 3)**

   * show the top definition matches (ranked) as *Target Cards*
   * if ambiguous (multiple defs), be explicit (“picked #1; #2/#3 also plausible”)

2. **Neighborhood Preview for the #1 candidate**

   * **best source available**:

     * if Pyrefly payload exists for that anchor → use it for call graph + implementations + references
     * also add **structural containment** (parents/children/siblings) from lightweight scan bounded to the candidate file (very cheap)
   * keep it *tiny*: totals + 5–8 preview nodes per slice

3. **Risk Flags**

   * incoming/outgoing totals (Pyrefly overview already computes these)
   * if the best candidate has many callers or high variance, emit “refactor risk: high”

**Output structure recommendation (search):**

* **Key Findings**

  1. `Target (best guess)` card
  2. `Neighborhood (preview)` one-liner summary (callers/callees/refs/impl counts)
  3. `Risk flags` one-liner
* **Sections (stable order)**

  1. Target Candidates (3)
  2. Neighborhood Preview (callers/callees/refs/impl/type hierarchy + enclosing context)
  3. Existing “Top Contexts / Definitions / Imports / Callsites …” (keep as evidence-y)

This keeps search as “one call to get oriented”.

---

### B) `calls` (the refactor front door)

`calls` already does heavy lifting (arg shapes, kwargs, contexts, hazards, callsites). What it’s missing is: “ok, but what is the callee’s world?”

**Embed:**

1. **Target Card**

   * definition location + signature (you already compute signature cheaply)
   * if definition is ambiguous (multiple defs), show top candidates (same as search)

2. **Neighborhood Preview for the target definition**

   * callers/callees totals (from the call census + optional Pyrefly)
   * *especially* “callees” of the target function (what it depends on) — agents routinely need this before edits

3. **Risk Flags (promote what you already have)**

   * distinct arg-shapes count
   * forwarding count
   * hazard summary (counts per hazard)
   * “top calling contexts” already exists → keep, but add a single headline line

**Output structure recommendation (calls):**

* **Key Findings**

  1. Target Card (signature + file)
  2. “Call surface” headline: `N calls / M files / K arg-shapes / forwarding=X`
  3. “Neighborhood” headline: `callees=Y (top: …)` (even if you only compute from the definition body)
* **Sections**

  1. Target / Neighborhood Preview
  2. Argument Shape Histogram (existing)
  3. Keyword Usage (existing)
  4. Hazards (existing)
  5. Call Sites (existing; this stays the big evidence sink)

---

### C) `q entity=…` (the semantic selection door)

This one is the easiest to improve because the entity executor already has the scan state needed to compute structural relationships cheaply.

**Embed (default, without requiring `fields=` or `expand=`):**

* For `entity=function|class|method`:

  1. Target Cards for each result (up to a cap, say 10)
  2. **Per-result mini-neighborhood** (budgeted):

     * calls_within (already computed)
     * callers/callees counts (already computable from `call_records` + `calls_by_def`)
     * enclosing class / parents / children / siblings (cheap via file interval index)
  3. Risk flags (closure/globals/bytecode surface) **only for top N results** to avoid explosion

* For `entity=import`:

  * show resolved import path / alias chain (Pyrefly when available, else structural)
  * add “importers” preview (reverse edges) when possible

**Output structure recommendation (entity):**

* **Key Findings**

  * If 1 result: show full Target Card + Neighborhood Preview
  * If many results: show “Top 3 most relevant” cards (by calls_within, file role, etc.)
* **Sections**

  1. Results (Target Cards list)
  2. Neighborhood Preview (only for top 1–3 unless user asks)

---

## How to structure the output so agents actually use it

### 1) Make a *single* “Target + Neighborhood + Risk” block always appear near the top

Agents are pattern-matching. If every command prints the same 3-part block early, they’ll absorb it without learning new commands.

### 2) Budget everything; push the rest to artifacts

Suggested defaults:

* max target cards: **3** for search/calls; **10** for entity
* neighborhood top_k per slice: **5–8**
* only run heavier enrichments (exceptions/side-effects/bytecode-surface style) for **the single chosen target**, not for every hit

### 3) Use a stable machine-readable shape *inside* `summary` and/or a single evidence finding

Even if the agent reads markdown, you want deterministic keys for downstream parsing. I’d add something like:

* `summary["auto_insights"] = { primary_target: {...}, candidates: [...], neighborhood: {...}, risk: {...}, provenance: {...} }`

…and keep it consistent across search/calls/entity.

### 4) Keep the “neighborhood” model: totals + preview + (optional) artifact pointer

SNB already proved this pattern works. Reuse it everywhere:

* `total`
* `preview[]`
* `artifact_path` (only when overflow)

---

## The highest ROI “embed first” shortlist

If you only do a first pass, do these in order:

1. **Target Card** (for search/calls/entity)
2. **Neighborhood Preview: callers + callees + references totals + top examples**
3. **Risk Flags: arg-shape variance + forwarding + dynamic hazard counts**
4. **Dependency Context: import alias chain + imports in target file**

Everything else (impact, sig-impact, side-effects, exceptions) can follow once the “front door” outputs are reliably consumed.


