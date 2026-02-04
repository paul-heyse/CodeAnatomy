I dug through what’s currently under `src/semantics/` (types → schema inference → join helpers → `SemanticCompiler` rules → the example `pipeline.py`). The overall direction is strong: you’re collapsing a big surface area of bespoke view-builders/specs into a small set of composable, DataFusion-native transformations, and you’re leaning on plan compilation + lineage for dependency inference (which is exactly where DataFusion is a force multiplier).

That said, there are a few **implementation traps** in the current draft that you’ll want to address *before* you wire this into the rest of the system, because they can silently produce **ID collisions**, **wrong joins**, or **non-reproducible caching**.

---

## 1) Biggest gap: “first matching column wins” breaks on multi-span + multi-id tables

Right now, `SemanticSchema` chooses:

* the first `*_id` (excluding `file_id`) as “the entity id”
* the first `*_bstart`/`*_bend` as “the span”

That’s fine for simple tables like `cst_refs` that have exactly one meaningful span (`bstart/bend`) and one meaningful id (`ref_id`). But a bunch of your extractor tables have **multiple spans** and/or **foreign-key ids**, and the current heuristic picks the *wrong* one.

### Concrete examples from your repo schemas

* **`cst_defs`** has:

  * `container_def_bstart/bend`
  * `def_bstart/bend`
  * `name_bstart/bend`

  With today’s logic, `span_start_name()` will pick **`container_def_bstart`** (because it appears first in the schema), so `normalize(prefix="def")` will generate `def_id` from the *container span*, not the def span.

  That’s a collision factory: many defs share the same container span (all methods inside a class, etc.).

* **`cst_imports`** has:

  * `stmt_bstart/bend`
  * `alias_bstart/bend`

  Today it will pick **`stmt_bstart/bend`**, so multiple imported names in a single statement can collapse into the same `import_id`.

* **Tables with a foreign-key `*_id` before the “real” id you’ll add**

  * e.g. docstring/decorator-ish patterns: `owner_def_id` appears in the schema *before* whatever you’d compute as `docstring_id`/`decorator_id`.
  * With the current `entity_id_name()` implementation, the “entity id” becomes `owner_def_id` (wrong), and downstream `relate()` will treat the owner id as the entity id.

This isn’t a small edge case—it’s common across the CST-derived tables.

### What to do instead

You’ll need *either* better heuristics *or* a tiny amount of declarative configuration. Given your stated goal (“few static inputs and declarations”), I’d recommend leaning into a **declarative per-table semantic spec**:

#### Minimal spec you’ll want per table

* **primary_id**: which column is “the entity id”
* **primary_span**: which `(bstart, bend)` pair is the entity’s span
* (optionally) **join_span**: if you want to join on a different span than the one used for IDs (sometimes useful)
* (optionally) **id_parts**: if the ID can’t be uniquely determined by span alone (rare if span is correct, but you may want it for safety)

Even if you keep regex inference as the fallback, you want an “escape hatch” where the inference is ambiguous.

#### If you want “mostly inferred” but safer heuristics

A good default priority order for “primary span” is:

1. exact `bstart/bend`
2. exact `{prefix}_bstart/{prefix}_bend` (where `prefix` is the entity prefix you pass to `normalize`)
3. exact `name_bstart/name_bend` (often best for symbol-like entities)
4. if exactly **one** candidate pair exists, use it
5. otherwise: raise with a message listing candidates (don’t guess)

Same for entity id:

1. exact `{prefix}_id` if present (this also fixes “foreign key id picked first”)
2. exact `entity_id` if you standardize on it
3. if exactly one non-file `_id` exists, use it
4. else: raise

To make that work cleanly, the schema methods need to accept the `prefix` you already have at call sites (`normalize(prefix=...)`), rather than being prefix-agnostic.

---

## 2) SCIP occurrences aren’t byte spans (your current `relate()` can’t work on them yet)

Your `SemanticCompiler.relate()` assumes evidence tables have `bstart/bend`-style byte offsets (because it builds `span_make(bstart, bend)` and then uses `span_overlaps` / `span_contains`).

But in your existing schema registry, **`scip_occurrences` is line/char based** (`start_line`, `start_char`, `end_line`, `end_char`, …) and does not expose `bstart/bend`.

So as written, any `relate(..., "scip_occurrences", join_type="span_overlap")` will fail unless you already have a normalized view/table that adds byte offsets.

### What you’re missing

A **normalization step** for SCIP evidence into the same span space as your CST evidence.

Options:

* Add a `scip_occurrences_norm` view that produces `bstart/bend` (and ideally preserves the raw `(line,char)` too).
* Or extend the semantic system to support `(start_line,start_char,end_line,end_char)` as a first-class “span type” with its own overlap/contains UDFs. (This is more work; your Rust span UDFs currently compare `bstart/bend`.)

Also: SCIP’s `col_unit` can be UTF-16 or bytes depending on position encoding. Even after you compute offsets, you’ll want to ensure both sides are in the **same unit** before overlap comparisons.

---

## 3) `union_with_discriminator()` will almost certainly explode on heterogeneous node tables

In `semantics/pipeline.py`, `cpg_nodes` is built by unioning `cst_refs_norm`, `cst_defs_norm`, `cst_imports_norm`, `cst_callsites_norm`.

Those normalized tables still have their original payload columns, which differ widely. DataFusion unions generally require compatible schemas. Even if DataFusion allows it by position, you won’t get a coherent “node” output.

### Fix

Before union, project into a **canonical node schema**, e.g.:

* `entity_id` (string)
* `node_kind` (discriminator)
* `path`
* `bstart`
* `bend`
* maybe `file_id`
* maybe `attrs` (map) if you want to keep extras in a single column

This is also consistent with what your legacy CPG builders already do: they emit standardized `cpg_nodes_v1` columns rather than unioning raw extractor shapes.

---

## 4) `plan_fingerprint = hash(str(plan))` is a real caching bug

In `build_cpg_from_inferred_deps`, you compute:

```py
plan_fingerprint = hash(str(plan))
```

Python’s `hash()` is **intentionally randomized per process** (unless `PYTHONHASHSEED` is fixed), so this value is **not stable across runs**. That will break any cache keying / incremental reasoning you build on top.

### Fix

Use a stable hash:

* `sha256(str(plan).encode()).hexdigest()`
* or (better) reuse your existing plan fingerprint machinery (`incremental/plan_fingerprints.py` + `DataFusionPlanBundle`), since your legacy infra already treats plan fingerprinting as a first-class concern.

---

## 5) Column name ambiguity after joins (likely to bite as you add more sources)

Your join helpers do:

* join on `path`
* then reference columns via `col("bstart")`, `col("bend")`, etc.

As soon as both sides have a same-named column that is **not** the join key, you risk:

* ambiguous column resolution, or
* silently picking the wrong side, depending on how DataFusion resolves unqualified column names.

Right now, CST tables vary (e.g. `call_bstart`, `def_bstart`, etc.), SCIP is line/char, so you may have avoided collisions accidentally. But once you normalize SCIP to `bstart/bend`, this becomes very real.

### Fix

Either:

* rename columns before joining (prefix left/right), then reference `l_bstart` vs `r_bstart`, or
* use qualified column references if DataFusion exposes that in the Python API you’re on, or
* project down to only the fields needed for the join + output prior to joining (often the cleanest).

---

## 6) Text normalization: be careful about casefolding and “in-place” mutation

`normalize_text()` currently overwrites every inferred TEXT column with `utf8_normalize(...)`. In your Rust defaults, `utf8_normalize_casefold` defaults to **true** (lowercasing).

That may be intentional for search/indexing, but it’s risky if you ever need:

* case-sensitive identifiers (Python is case-sensitive),
* faithful reproduction of names,
* or stable linkage to external symbol formats.

If you keep it, I’d strongly consider:

* writing normalized text into `*_norm` columns instead of overwriting (so you don’t lose original),
* or only normalizing columns explicitly marked as “free text” vs “identifier”.

---

## 7) Wiring recommendation: don’t bypass your existing view-graph + contract + artifact stack

Even though `src/semantics` is “new world”, your repo already has heavy-duty mechanisms that you probably still want:

* view registration with explicit deps and topo sort (`datafusion_engine/views/graph.py`)
* schema contracts and validation
* UDF snapshot validation / required UDF enforcement
* plan bundles + lineage extraction from bundles

Right now `semantics/pipeline.py` registers views directly on `SessionContext` and then re-implements a thin dependency inference. That’s fine as a demo, but if the goal is a robust replacement, I’d aim for:

**SemanticCompiler → emits `ViewNode`s** (name, deps, builder, required_udfs, cache_policy, plan_bundle)
then you call your existing `register_view_graph(...)`.

That way, the semantics layer stays small and declarative, and your existing “production hardening” stays intact.

---

# High-priority changes I’d make before deleting legacy code

## P0 (must fix)

1. **Primary span + primary entity id selection**

   * add declarative per-table overrides or prefix-aware selection
   * don’t “first match wins” on `_id` and `_bstart`

2. **Introduce SCIP span normalization**

   * get `scip_occurrences_norm` into byte offsets (or implement alternate span UDFs)

3. **Make `cpg_nodes`/`cpg_edges` unions canonical**

   * union standardized projections, not heterogeneous raw tables

4. **Replace `hash(str(plan))`**

   * stable hash or reuse plan bundle fingerprinting

## P1 (strongly recommended)

5. Make joins robust against duplicate column names (prefix/alias/projection)
6. Don’t overwrite identifier-like text in place unless you’re sure you want casefolding
7. Emit `ViewNode`s and use `register_view_graph` so caching/contracts/artifacts stay consistent

---

## Bottom line

`src/semantics` is a very promising direction, but **as written it’s still missing the one piece you can’t “infer away” safely**:

> A way to declare (or deterministically select) the *primary* id/span in tables that contain multiple ids/spans, plus a normalization bridge for SCIP’s line/char coordinates.

Once you add those, the rest of the approach (generic rules + DataFusion plans + inferred deps) will get you the streamlined, robust core you’re aiming for—and you’ll be able to retire large parts of the legacy view/spec plumbing without losing correctness or reproducibility.

If you want, I can propose a concrete `SemanticTableSpec` shape (Python dataclass or YAML) that plugs into your current `SemanticCompiler` API with minimal churn, and show exactly how it would resolve the CST tables that have `container_*` spans and `owner_*_id` foreign keys.

Below is a concrete `SemanticTableSpec` shape that solves the two hard cases you called out:

1. **tables with multiple “span-ish” columns** (e.g., `cst_defs` has `container_def_bstart`, `def_bstart`, `name_bstart`, …)
2. **tables where the first `*_id` is actually a foreign key** (e.g., `cst_docstrings.owner_def_id`), so naïve “first `_id` wins” inference is wrong.

The key idea is: **make “primary evidence span” explicit** (what defines *this* row’s identity + join span), and treat all other spans as **named reference spans** that can be turned into **FK IDs**.

---

## 1) A concrete `SemanticTableSpec` (Python dataclasses)

This plugs into your current `SemanticCompiler` model with minimal churn if you add **one new method** like `normalize_from_spec(spec)` (doesn’t require rewriting `relate()`), and optionally add a tiny improvement: prefer canonical `bstart/bend/entity_id` columns when present.

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Sequence

SpanUnit = Literal["byte"]  # you can extend later

@dataclass(frozen=True, slots=True)
class SpanBinding:
    """Bind a semantic span role to concrete source columns."""
    start_col: str
    end_col: str
    unit: SpanUnit = "byte"

    # Optional canonical aliases that the compiler will create
    # (these are what SemanticSchema should prefer for joins/projection)
    canonical_start: str = "bstart"
    canonical_end: str = "bend"
    canonical_span: str = "span"


@dataclass(frozen=True, slots=True)
class IdDerivation:
    """Derive an ID from (path, start, end) with a stable namespace."""
    out_col: str                 # e.g. "def_id", "docstring_id"
    namespace: str               # e.g. "cst_def", "cst_docstring"
    path_col: str = "path"
    start_col: str = "bstart"
    end_col: str = "bend"

    # If True, emit NULL when any required part is NULL.
    # (important for owner/container relationships where owner span may be NULL)
    null_if_any_null: bool = True

    # Optional canonical alias (lets you have def_id *and* entity_id)
    canonical_entity_id: str | None = "entity_id"


@dataclass(frozen=True, slots=True)
class ForeignKeyDerivation:
    """Derive a foreign-key ID to another entity namespace using a reference span."""
    out_col: str                 # e.g. "container_def_id", "owner_def_id"
    target_namespace: str        # e.g. "cst_def"
    path_col: str = "path"
    start_col: str = ""
    end_col: str = ""
    null_if_any_null: bool = True

    # Optional guard column(s) (e.g. container_def_kind NULL => container_def_id NULL)
    guard_null_if: Sequence[str] = ()


@dataclass(frozen=True, slots=True)
class SemanticTableSpec:
    table: str                   # DataFusion table/view name (e.g. "cst_defs")
    path_col: str = "path"

    # The span that defines *this row’s* evidence identity and joins
    primary_span: SpanBinding = field(default_factory=lambda: SpanBinding("bstart", "bend"))

    # How to derive the row’s entity ID (and canonical entity_id alias)
    entity_id: IdDerivation = field(default_factory=lambda: IdDerivation(
        out_col="entity_id",
        namespace="entity",
    ))

    # Optional derived foreign keys (owner/container/etc.)
    foreign_keys: tuple[ForeignKeyDerivation, ...] = ()

    # Optional: explicitly list text cols for normalize_text (docstring doesn’t match your current TEXT regex)
    text_cols: tuple[str, ...] = ()
```

### Minimal-churn compiler hook

Add one helper in `SemanticCompiler` (conceptually):

* Creates canonical `bstart/bend/span` from the *spec’s primary span*, **even if the table has many other `_bstart/_bend` columns**.
* Derives `def_id`, `docstring_id`, etc. using a **namespace** independent of the output column name.
* Derives FK IDs (container/owner) using the same namespace as the target table so they **resolve**.

---

## 2) YAML shape (same semantics)

If you want this as static config (fits your “few static inputs” direction), here’s a YAML equivalent that round-trips cleanly to the dataclasses above:

```yaml
tables:
  cst_defs:
    path_col: path
    primary_span:
      start_col: def_bstart
      end_col: def_bend
      canonical_start: bstart
      canonical_end: bend
      canonical_span: span
    entity_id:
      out_col: def_id
      namespace: cst_def
      path_col: path
      start_col: def_bstart
      end_col: def_bend
      canonical_entity_id: entity_id
    foreign_keys:
      - out_col: container_def_id
        target_namespace: cst_def
        path_col: path
        start_col: container_def_bstart
        end_col: container_def_bend
        guard_null_if: [container_def_kind]   # if kind is null => fk is null

  cst_docstrings:
    path_col: path
    primary_span:
      start_col: bstart
      end_col: bend
    entity_id:
      out_col: docstring_id
      namespace: cst_docstring
      path_col: path
      start_col: bstart
      end_col: bend
      canonical_entity_id: entity_id
    foreign_keys:
      - out_col: owner_def_id
        target_namespace: cst_def
        path_col: path
        start_col: owner_def_bstart
        end_col: owner_def_bend
    text_cols: [docstring]

  cst_decorators:
    path_col: path
    primary_span:
      start_col: bstart
      end_col: bend
    entity_id:
      out_col: decorator_id
      namespace: cst_decorator
      path_col: path
      start_col: bstart
      end_col: bend
      canonical_entity_id: entity_id
    foreign_keys:
      - out_col: owner_def_id
        target_namespace: cst_def
        path_col: path
        start_col: owner_def_bstart
        end_col: owner_def_bend
    text_cols: [decorator_text]
```

---

## 3) “Exactly how” this resolves `container_*` spans and `owner_*_id` FKs

### A) `cst_defs`: container span ⇒ `container_def_id` that resolves to another `cst_def.def_id`

**Problem today (without a spec):**
`SemanticSchema.from_df()` picks the *first* `_bstart/_bend` columns it sees. In `cst_defs`, that’s typically `container_def_bstart/container_def_bend`, which makes:

* the **entity’s own span wrong** (your “def” becomes identified by its container span)
* joins to SCIP occurrences wrong (containment/overlap is evaluated against the container)

**With the spec above:**

* Primary span is explicitly `def_bstart/def_bend`.
* The compiler creates canonical `bstart = def_bstart`, `bend = def_bend`, and `span = span_make(def_bstart, def_bend)`.

ID derivations become:

* **Definition identity**

  * `def_id = stable_id_parts("cst_def", path, def_bstart, def_bend)`
* **Container foreign key**

  * `container_def_id = NULL if container_def_kind is NULL OR container_def_bstart/bend is NULL`
  * else `stable_id_parts("cst_def", path, container_def_bstart, container_def_bend)`

✅ Why it resolves:

* The owning container definition row has `def_bstart/def_bend` equal to the child row’s `container_def_bstart/container_def_bend`.
* Both IDs are derived with **the same namespace** (`"cst_def"`) and the same parts structure (`path + span`).
* Therefore:

[
\texttt{container_def_id(child)} \equiv \texttt{def_id(parent)}
]

### B) `cst_docstrings` (and `cst_decorators`): owner span ⇒ `owner_def_id` FK that resolves to `cst_defs.def_id`

**Problem today (without a spec):**

* `owner_def_id` is the first `*_id` column, so naïve inference treats it as the **docstring entity ID**.
* `owner_def_bstart/bend` likely appear before `bstart/bend`, so naïve inference also picks the **owner span** as the row span.

**With the spec above:**

* Primary span for docstring/decorator is explicitly the row’s own `bstart/bend`.
* The compiler generates:

  * `docstring_id = stable_id_parts("cst_docstring", path, bstart, bend)`
  * `decorator_id = stable_id_parts("cst_decorator", path, bstart, bend)`
* And FK derivation:

  * `owner_def_id = NULL if owner_def_bstart/bend is NULL`
  * else `stable_id_parts("cst_def", path, owner_def_bstart, owner_def_bend)`

✅ Why it resolves:

* The owning definition row has `def_bstart/def_bend` equal to `owner_def_bstart/owner_def_bend`.
* Both use namespace `"cst_def"`, so:

[
\texttt{owner_def_id(docstring)} \equiv \texttt{def_id(owning\ def)}
]

---

## 4) What I’d implement in `SemanticCompiler.normalize_from_spec(spec)` (conceptual)

This is the exact DataFusion expression shape you want for these columns (pseudo-code but very close to your current style):

```python
from datafusion import col, lit, functions as f
from datafusion_engine.udf.expr import udf_expr

def _nullable_stable_id(ns: str, path_c: str, start_c: str, end_c: str, guard_cols=()):
    cond = col(start_c).is_null() | col(end_c).is_null()
    for g in guard_cols:
        cond = cond | col(g).is_null()
    return f.when(cond, lit(None)).otherwise(
        udf_expr("stable_id_parts", ns, col(path_c), col(start_c), col(end_c))
    )

def normalize_from_spec(ctx, spec: SemanticTableSpec):
    df = ctx.table(spec.table)

    # Canonical primary span columns for joins/projection
    df = (
        df
        .with_column(spec.primary_span.canonical_start, col(spec.primary_span.start_col))
        .with_column(spec.primary_span.canonical_end, col(spec.primary_span.end_col))
        .with_column(
            spec.primary_span.canonical_span,
            udf_expr(
                "span_make",
                col(spec.primary_span.canonical_start),
                col(spec.primary_span.canonical_end),
            ),
        )
    )

    # Entity ID (plus canonical entity_id alias)
    ent_expr = _nullable_stable_id(
        spec.entity_id.namespace,
        spec.entity_id.path_col,
        spec.entity_id.start_col,
        spec.entity_id.end_col,
    )
    df = df.with_column(spec.entity_id.out_col, ent_expr)
    if spec.entity_id.canonical_entity_id:
        df = df.with_column(spec.entity_id.canonical_entity_id, col(spec.entity_id.out_col))

    # Foreign keys
    for fk in spec.foreign_keys:
        df = df.with_column(
            fk.out_col,
            _nullable_stable_id(
                fk.target_namespace,
                fk.path_col,
                fk.start_col,
                fk.end_col,
                guard_cols=fk.guard_null_if,
            ),
        )

    return df
```

This is exactly the logic you need to make:

* `cst_defs.container_def_id` resolve to another `cst_defs.def_id`
* `cst_docstrings.owner_def_id` / `cst_decorators.owner_def_id` resolve to `cst_defs.def_id`

…and also to prevent primary-span ambiguity from silently corrupting your joins.

---

## 5) One small but important follow-up (to keep churn low)

If you *don’t* want to thread spec overrides through `SemanticSchema`, the simplest global tweak is:

* **Teach `SemanticSchema` to prefer canonical columns** when present:

  * prefer `"bstart"` over any `*_bstart`
  * prefer `"bend"` over any `*_bend`
  * prefer `"entity_id"` over any `*_id`

That single preference rule lets:

* `relate()` keep working without any per-table hacks
* complex CST tables carry extra spans/FKs safely
* spec normalization “pins” the intended semantics via canonical columns

---

If you want, I can also sketch the *two* small edges you’d likely want to emit once these IDs are correct:

* `cst_def__contains__cst_def` using `(container_def_id -> def_id)`
* `cst_def__owns__cst_docstring` / `cst_def__owns__cst_decorator` using `(owner_def_id -> docstring_id/decorator_id)`

…but the spec above is the missing piece that makes those edges **deterministic and referentially consistent**.



# add on review for added reliability #

Yes — a *lot* of the “extra” fields you’ve captured in `registry.py` are relevant to ambiguity resolution and **join quality**, but the key is to treat them as **evidence & diagnostics**, not as “more equality columns to join on”.

A clean way to think about it:

* **Hard keys (identity)**: what *must* match for a join to be valid.
* **Soft evidence (ranking/selection)**: what helps you choose the *best* match when there are multiple candidates.
* **Quality signals (confidence modifiers)**: what tells you to *downweight* (or even suppress) matches because the upstream evidence is shaky.

That separation keeps your semantics layer robust even as schemas evolve.

---

## 1) What you already have in `registry.py` that *directly* improves join quality

### A) File identity = your most important hard key

Across your extract families you consistently carry **`path`, `file_id`, `file_sha256`** (and often `repo`). For example, `LIBCST_FILES_SCHEMA` includes `repo`, `path`, `file_id`, `file_sha256` and then the nested lists (`defs`, `callsites`, etc.). Likewise, AST files carry the same identity fields.

**How to leverage for join quality:**

* Use `(repo, file_id)` or `(file_id)` as your **primary** file join key if `file_id` is stable in your pipeline.
* Use `file_sha256` as a **join validity check** / anti-footgun:

  * If two sources disagree on `file_sha256` for the “same” `(repo, path, file_id)`, that’s almost always “different file content” → your match confidence should drop sharply or be treated as invalid.

This is one of the highest ROI pieces of “extra” metadata.

---

### B) Span coordinate metadata = the #1 cause of false ambiguity if you ignore it

Your schemas explicitly track span semantics: `line_base`, `col_unit`, `end_exclusive` are embedded as schema metadata (AST) and are also present in some tables/records (e.g., SCIP occurrences).

You even have symtable spans explicitly set to `utf32` via `span_metadata(col_unit="utf32")`.

**Why this matters for joins:**
Most of your “best” cross-source joins are **interval/overlap joins** (span alignment). If one source uses UTF-32 columns and another uses byte offsets, or one is end-inclusive vs end-exclusive, you’ll create phantom overlaps/misses → which looks like “ambiguity” but is really a coordinate bug.

**Actionable recommendation:**
Create a canonical “span normalization” step per evidence source that outputs:

* `bstart`, `bend` in **byte offsets**
* and a `span_key` you can compare deterministically

…and always base interval joins on the normalized byte spans.

This is explicitly called out in your relationship guide’s ambiguity taxonomy (span ambiguity and coordinate mismatch).

---

### C) Structural “container / owner” fields are *excellent* ambiguity reducers

Your CST extract is already shaped to make ownership joins cheap and reliable.

Examples:

* `CST_DEF_T` includes `def_id` (nullable), plus **`container_def_*` spans** and the definition spans (`def_bstart/def_bend`, `name_bstart/name_bend`) and qualified naming (`qnames`, `def_fqns`).
* `CST_DOCSTRING_T` includes `owner_def_id` (nullable) and **`owner_def_bstart/owner_def_bend`**.
* `CST_DECORATOR_T` similarly includes `owner_def_id` and owner spans.

**How to use it:**

* Prefer joining via `owner_def_id -> def_id` when present (strong ID-based match).
* Fallback to span containment or exact span equality within the file when IDs are missing:

  * `(file_id, owner_def_bstart, owner_def_bend)` ↔ `(file_id, def_bstart, def_bend)`
* Use `container_def_*` spans as a *secondary* check when multiple defs share identical spans in odd cases (generated code, macros, etc.).

This is exactly the kind of “extra data” that turns ambiguous joins into deterministic joins.

---

### D) SCIP fields are “superior evidence” for symbol identity — but only after span normalization

Your `SCIP_OCCURRENCES_SCHEMA` includes:

* `symbol` + `symbol_roles`
* syntax info (`syntax_kind`, `syntax_kind_name`)
* range data both raw and normalized (`start_line/start_char/end_line/end_char`, etc.)
* and flags like `is_definition`, `is_import`, `is_write`, `is_read`, `is_generated`, `is_test`

This aligns with the relationship guide’s “superior evidence” ordering where **SCIP symbol identity** is preferred when aligned successfully.

**How to use it for join quality:**

* Treat `symbol` + `symbol_roles` as the *authoritative semantic payload* once you have reliable span alignment.
* Use the role bits and syntax kind as **ranking features** when multiple occurrences overlap the same CST span.

Also: your `SCIP_METADATA_SCHEMA` includes `project_root` (plus tool + project identifiers). That is extremely useful for path normalization if any extractor emits paths relative to different roots.

---

### E) You already have semantic-type validation infrastructure — that’s a big deal for “mapping/view creation among variables”

`validate_semantic_types()` walks tables/columns and checks expected semantic types derived from `semantic_type_for_field_name(...)`.

**How this helps your “inferential mapping” question:**
If your semantic compiler is trying to infer “these two datasets should join on X”, semantic types are the right mechanism to reduce guesswork:

* “file identity” columns join with “file identity”
* “byte span” columns join with “byte span”
* etc.

This is exactly the kind of metadata that should guide *automatic wiring* without relying on naming conventions alone.

---

## 2) How to incorporate “extra fields” without making joins brittle

### Principle: Don’t add more hard join conditions to “force” correctness

That often *reduces* robustness:

* tool versions change
* text fields differ
* optional fields appear/disappear
* encodings differ

Instead:

1. **Generate candidates** with a small set of hard constraints
2. **Score/rank candidates** using richer evidence
3. **Either keep ambiguity** or select a winner deterministically
4. **Emit a join-quality contract** in the output

Your relationship guide is explicit that ambiguous relationships should include:

* `confidence` (0..1)
* optional `score`
* `ambiguity_group_id`
* `origin/provider/rule_name` for explainability

…and that winner selection must be deterministic with explicit sort keys (row order isn’t guaranteed).

So: the “extra data” belongs primarily in (2) and (4).

---

## 3) Concrete examples of “extra metadata → better join quality”

### Example A: CST name refs ↔ SCIP occurrences (span-aligned symbol resolution)

Your relationship guide already describes this pattern:

* Join type: interval-align join within a file (candidate if occurrence span intersects/contained by name_ref span)
* Ranking: contained first, smallest span, role match, deterministic tie-break by `(occ_bstart, occ_bend, symbol)`

**What extra SCIP fields buy you:**

* `symbol_roles` lets you match CST context (load/store/import) → better ranking
* `is_generated` / `is_test` lets you downweight questionable or non-source-of-truth matches (quality signal)
* `syntax_kind` can be used as a tie-breaker when overlap patterns are identical

**Bottom line:** Keep all that metadata available, but use it in scoring/confidence, not as equality join keys.

---

### Example B: CST docstrings/decorators/type_exprs ↔ CST defs (ownership joins)

Because `owner_def_id` is nullable in docstrings/decorators and `def_id` is nullable in defs, you *must* have a robust fallback.

That’s why the owner spans (`owner_def_bstart/owner_def_bend`) and container spans (`container_def_*`) are so valuable: they allow stable fallbacks without needing an ID to exist.

This is a perfect case where “extra columns” are not bloat — they are your ambiguity killer.

---

### Example C: Call resolution quality (callee shape + qname candidates + signature text)

Your `CST_CALLSITE_T` includes:

* `callee_shape`, `callee_text`, `arg_count`
* `callee_dotted`, `callee_qnames`, `callee_fqns`
* plus an `inferred_type` hint

And SCIP symbol info can carry signature text and kind/name fields (e.g., `signature_text`, `kind_name`).

Those are great *soft* features to rank call resolution candidates:

* does the candidate symbol look callable?
* does the signature “fit” the observed arg counts?
* do qname/fqn candidates match the symbol’s package/module?

Again: don’t hard-join on these; score with them.

---

## 4) So should you keep “every available output” in the schema?

Keeping everything in the *base* extract tables is defensible **if** you also do this:

* Build **normalized “core” views** that project only what each relationship needs (identity + normalized span + a small set of evidence features).
* Keep the richer payload available for debugging/explanations, but avoid dragging it through every join plan.

This matches your broader architecture goal: static declarations + compiler selects just what’s needed.

---

## 5) A practical recipe for “join quality” in your semantic compiler

When you define a relationship/mapping, make the compiler produce:

1. **Candidate dataset** (many rows per left entity)

* hard constraints: `file_id` match, span overlap, etc.

2. **Scored dataset**

* `score`: computed from features (containment, span_len, role match, sha match, diagnostics presence…)

3. **Selected or ambiguity-preserving output**

* either:

  * keep all candidates and group by `ambiguity_group_id`, *or*
  * select top-1 per group after explicit sort/tie-break

4. **Diagnostics output**

* coverage stats, ambiguity rates, mismatched `file_sha256`, parse error presence, etc.

Your relationship guide already “wants” this shape: represent ambiguity explicitly + deterministic winner selection.

---

## Direct answer to your question (“should we leverage the additional data for join quality?”)

**Yes — absolutely — but mostly as scoring/validation signals, not as additional hard join keys.**

If you only pick a handful of “highest signal” extras to actively use, I’d prioritize:

1. **`file_sha256`** as a join validity signal (prevents silent bad joins)
2. **Span metadata / coordinate normalization** (`line_base`, `col_unit`, `end_exclusive`) to eliminate fake ambiguity
3. **SCIP `symbol` + `symbol_roles`** as the semantic payload + role compatibility scoring
4. **CST owner/container spans + nullable IDs** for ownership fallback joins
5. **Semantic type metadata** to drive automatic mapping decisions safely

Everything else can remain available as “explainability payload” unless/until it proves predictive for scoring.

---

If you want, I can take your earlier `SemanticTableSpec` shape and show a minimal extension that lets you declare (per table/relationship) **hard keys vs soft evidence vs quality signals**, and then map those directly into DataFusion expressions for candidate generation + ranking (aligning with the `confidence/score/ambiguity_group_id` contract in your relationship guide).

Yes — a lot of the “extra” fields in `registry.py` *can* be relevant for resolving ambiguity and measuring “quality of joins,” but only if you separate them into three roles and wire them in deterministically:

* **Hard keys**: *must* match (or you don’t even generate a candidate).
* **Soft evidence**: helps **rank** candidates *within an ambiguity group*.
* **Quality signals**: penalize/boost confidence based on **provider reliability** (parse errors, coordinate encoding mismatches, timeouts, etc.).

That separation maps cleanly onto your relationship contract (`confidence`, optional `score`, `ambiguity_group_id`, plus `origin/provider/rule_name`). Your relationship guide explicitly calls out representing ambiguity with these fields and (importantly) needing deterministic ordering when selecting a “best” row.

Below is the “minimal extension” I’d make to your earlier `SemanticTableSpec`/relationship shape so you can **declare** those three categories and then **compile them into DataFusion expressions** for candidate generation + ranking.

---

## 1) Minimal spec extension: add `signals` (hard / evidence / quality) + deterministic ranking

### Python dataclasses (minimal churn)

This is intentionally additive: if your compiler ignores `signals`, nothing breaks.

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Sequence, Optional

# Use SQL-expression strings to keep the spec serializable (YAML-friendly),
# while still compiling into DataFusion expression trees (per your guide's “expression compilation” stance).

ExprStr = str

@dataclass(frozen=True)
class HardPredicate:
    """
    A boolean predicate that must be true to consider a pair a valid candidate.
    Evaluated during join (ON) and/or post-join filter.
    """
    expr: ExprStr


@dataclass(frozen=True)
class Feature:
    """
    A numeric feature used for scoring.
    - evidence: helps pick best among plausible matches
    - quality: boosts/penalizes confidence/score based on provider health/metadata
    """
    name: str
    expr: ExprStr                 # yields numeric (or boolean castable to numeric)
    weight: float
    kind: Literal["evidence", "quality"] = "evidence"


@dataclass(frozen=True)
class SignalsSpec:
    hard: Sequence[HardPredicate] = ()
    features: Sequence[Feature] = ()

    # Optional: base priority lets you encode “evidence tiers”
    # (e.g., SCIP > symtable > bytecode > heuristics) as a big constant in score.
    base_score: float = 0.0
    base_confidence: float = 0.5


@dataclass(frozen=True)
class RankSpec:
    """
    How to group ambiguous candidates and deterministically choose winners.
    """
    # Key used for window partitioning (often the left entity id).
    ambiguity_key_expr: ExprStr

    # How to compute ambiguity_group_id column (defaults to stable_hash64(key)).
    ambiguity_group_id_expr: Optional[ExprStr] = None

    # Deterministic ordering — critical because join output row order is not guaranteed.
    order_by: Sequence[str] = ("score DESC",)

    # "all" keeps all candidates; "best" filters to top-k by order_by.
    keep: Literal["all", "best"] = "all"
    top_k: int = 1


@dataclass(frozen=True)
class RelationshipSpec:
    name: str
    left_view: str
    right_view: str

    # Equi-join keys are your main “hard keys” (fast & selective).
    left_on: Sequence[str] = ()
    right_on: Sequence[str] = ()
    how: Literal["inner", "left", "right", "full"] = "inner"

    # Additional “hard” predicates (span overlaps, containment, etc.)
    signals: SignalsSpec = field(default_factory=SignalsSpec)

    # Required relationship-guide contract fields
    origin: str = "semantic_compiler"
    provider: str = "unknown_provider"
    rule_name: Optional[str] = None  # default to spec.name

    # Ranking + ambiguity grouping
    rank: Optional[RankSpec] = None

    # Mapping into final output columns
    select_exprs: Sequence[ExprStr] = ()
```

Key idea: you generate candidates by **(1)** equi-join keys, **(2)** apply `signals.hard` filters, **(3)** compute feature columns, **(4)** compute `score`/`confidence`/`ambiguity_group_id`, and **(5)** optionally apply deterministic winner selection via `row_number()`.

Your engine already expects functions like `stable_hash64` and `row_number` to exist. 

---

### YAML representation (same model, just serializable)

```yaml
relationships:
  - name: rel_cst_docstring_owner
    left_view: cst_docstrings_norm
    right_view: cst_defs_norm
    how: inner
    left_on: [file_id]
    right_on: [file_id]

    provider: libcst
    origin: semantic_compiler
    rule_name: rel_cst_docstring_owner

    signals:
      base_score: 1000
      base_confidence: 0.98
      hard:
        - expr: "owner_def_id IS NOT NULL AND owner_def_id = def_id"
      features:
        - name: exact_owner_id_match
          kind: evidence
          weight: 100
          expr: "CASE WHEN owner_def_id = def_id THEN 1 ELSE 0 END"
        - name: owner_kind_matches
          kind: evidence
          weight: 5
          expr: "CASE WHEN owner_kind = def_kind THEN 1 ELSE 0 END"

    rank:
      ambiguity_key_expr: "def_id"
      ambiguity_group_id_expr: "stable_hash64(def_id)"
      order_by:
        - "score DESC"
        - "bend - bstart ASC"
        - "bstart ASC"
      keep: best
      top_k: 1

    select_exprs:
      - "def_id as entity_id"
      - "docstring as docstring"
      - "confidence"
      - "score"
      - "ambiguity_group_id"
      - "origin"
      - "provider"
      - "rule_name"
```

---

## 2) Compiling the spec into DataFusion DataFrame expressions

DataFusion’s Python DataFrame API supports **expression joins** (including non-equality predicates) via `join_on`, plus `select_exprs` (SQL-expression strings) for computed columns.

### Generic compilation pattern (pseudo-code)

```python
def compile_relationship(ctx, spec: RelationshipSpec):
    l = ctx.table(spec.left_view)
    r = ctx.table(spec.right_view)

    # 1) Start with equi-join keys if provided (fast, selective)
    if spec.left_on and spec.right_on:
        df = l.join(r, join_keys=(list(spec.left_on), list(spec.right_on)), how=spec.how)
    else:
        # fallback: cross join + filter (avoid unless you must)
        df = l.cross_join(r)

    # 2) Apply hard predicates (span overlaps, owner-id match, etc.)
    for hp in spec.signals.hard:
        df = df.filter(hp.expr)

    # 3) Add feature columns
    feature_cols = []
    for f in spec.signals.features:
        feature_cols.append(f"{f.expr} AS feat__{f.kind}__{f.name}")
    if feature_cols:
        df = df.select_exprs("*", *feature_cols)

    # 4) Score and confidence
    # score = base_score + sum(weight * feature)
    score_terms = [str(spec.signals.base_score)]
    for f in spec.signals.features:
        score_terms.append(f"({f.weight}) * feat__{f.kind}__{f.name}")
    score_expr = " + ".join(score_terms) + " AS score"

    # confidence = clamp01(base_confidence + small_scaled_score - quality_penalties)
    # Keep it simple + monotone; you can refine later.
    conf_expr = (
        f"LEAST(1.0, GREATEST(0.0, "
        f"{spec.signals.base_confidence} + (score / 10000.0)"
        f")) AS confidence"
    )

    rule_name = spec.rule_name or spec.name
    df = df.select_exprs(
        *spec.select_exprs,
        score_expr,
        conf_expr,
        f"'{spec.origin}' AS origin",
        f"'{spec.provider}' AS provider",
        f"'{rule_name}' AS rule_name",
    )

    # 5) Ambiguity grouping + deterministic winner selection
    if spec.rank:
        group_key = spec.rank.ambiguity_key_expr
        group_id = spec.rank.ambiguity_group_id_expr or f"stable_hash64({group_key})"
        df = df.select_exprs(
            "*",
            f"{group_id} AS ambiguity_group_id",
            "row_number() OVER ("
            f"PARTITION BY {group_key} "
            f"ORDER BY {', '.join(spec.rank.order_by)}"
            ") AS _rn",
        )
        if spec.rank.keep == "best":
            df = df.filter(f"_rn <= {spec.rank.top_k}").drop("_rn")

    return df
```

The critical bit is the `row_number() OVER (PARTITION BY ... ORDER BY ...)` with explicit, deterministic tie-breakers — your guide calls out that you can’t rely on join output order.

---

## 3) Concrete example: resolving `owner_*_id` + `owner_*_bstart/bend` (CST docstrings/decorators)

From your CST extraction schema:

* `cst_docstrings` carries `owner_def_id` (nullable) plus `owner_def_bstart/owner_def_bend`, and the docstring’s own `(bstart,bend)`. 
* `cst_decorators` is similar (nullable `owner_def_id`, plus owner span and decorator span/text). 
* `cst_defs` (not shown in that snippet, but present in your CST view set) is the natural “owner” table.

### Why this is a perfect “hard-vs-soft” split

* **Hard key (best)**: `owner_def_id = def_id` when `owner_def_id IS NOT NULL`.
* **Fallback candidate gen**: match by **file identity + owner span** (because span match can collide in weird parses, but still useful).
* **Quality signal**: presence of parse errors in that file, weird coordinate encoding, etc. (see next section).

### Two-rule approach (keeps logic simple and deterministic)

#### Rule A: owner_def_id join (high confidence)

```yaml
- name: rel_cst_docstring_owner_by_id
  left_view: cst_docstrings_norm
  right_view: cst_defs_norm
  left_on: [file_id]
  right_on: [file_id]
  how: inner
  provider: libcst
  origin: semantic_compiler

  signals:
    base_score: 1000
    base_confidence: 0.98
    hard:
      - expr: "owner_def_id IS NOT NULL AND owner_def_id = def_id"
    features:
      - name: owner_kind_matches
        kind: evidence
        weight: 5
        expr: "CASE WHEN owner_kind = def_kind THEN 1 ELSE 0 END"

  rank:
    ambiguity_key_expr: "def_id"
    order_by: ["score DESC", "bstart ASC", "bend ASC"]
    keep: best
    top_k: 1

  select_exprs:
    - "def_id AS src"
    - "stable_hash64(def_id) AS dst"        # or a docstring entity id if you model it
    - "'has_docstring' AS kind"
    - "docstring AS payload"
```

#### Rule B: owner span join fallback (lower confidence)

```yaml
- name: rel_cst_docstring_owner_by_span
  left_view: cst_docstrings_norm
  right_view: cst_defs_norm
  left_on: [file_id]
  right_on: [file_id]
  how: inner
  provider: libcst
  origin: semantic_compiler

  signals:
    base_score: 500
    base_confidence: 0.75
    hard:
      - expr: "owner_def_id IS NULL"
      - expr: "owner_def_bstart = def_bstart AND owner_def_bend = def_bend"
    features:
      - name: owner_kind_matches
        kind: evidence
        weight: 5
        expr: "CASE WHEN owner_kind = def_kind THEN 1 ELSE 0 END"

  rank:
    ambiguity_key_expr: "def_id"
    order_by: ["score DESC", "bstart ASC", "bend ASC"]
    keep: best
    top_k: 1

  select_exprs:
    - "def_id AS src"
    - "stable_hash64(def_id) AS dst"
    - "'has_docstring' AS kind"
    - "docstring AS payload"
```

This is deterministic, explainable, and matches your schema realities (nullable foreign keys + spans). 

---

## 4) Concrete example: CST name refs ↔ SCIP occurrences (candidate generation + ranking)

You already have rich CST ref data per row:

* `cst_refs` includes `ref_id` (nullable), `ref_text`, `expr_ctx`, `scope_type`, `parent_kind`, `inferred_type`, and `(bstart,bend)` byte offsets. 

SCIP occurrences carry:

* `symbol`, `symbol_roles`, and coordinate encoding fields (`line_base`, `col_unit`, `end_exclusive`), plus start/end line/char. 

And your relationship guide explicitly warns that span/coordinate ambiguity exists and mismatches can yield false overlaps unless you normalize and/or penalize.

### Practical pattern

1. **Normalize SCIP coordinates to byte spans** (a table/view step). Your engine already requires `col_to_byte`, which strongly suggests you intended this conversion. 
2. Candidate generation:

   * hard: same `path` or `file_id` (whichever you standardize)
   * hard: span overlap in byte space
3. Ranking features:

   * prefer contained overlaps (occurrence span inside ref span)
   * prefer minimal span difference
   * match read/write semantics:

     * CST `expr_ctx` vs SCIP `is_read`/`is_write` (SCIP includes booleans). 

### Example RelationshipSpec (assuming you have `scip_occurrences_norm` with byte spans)

```yaml
- name: rel_cst_ref_to_scip_symbol
  left_view: cst_refs_norm
  right_view: scip_occurrences_norm
  left_on: [path]     # or file_id if you have it for SCIP; pick ONE canonical file identity
  right_on: [path]
  how: inner
  provider: scip
  origin: semantic_compiler

  signals:
    base_score: 2000
    base_confidence: 0.95
    hard:
      - expr: "cst_bstart < scip_bend AND scip_bstart < cst_bend"   # overlap

    features:
      - name: contained
        kind: evidence
        weight: 50
        expr: "CASE WHEN scip_bstart >= cst_bstart AND scip_bend <= cst_bend THEN 1 ELSE 0 END"

      - name: span_len_delta_penalty
        kind: evidence
        weight: -0.01
        expr: "ABS((scip_bend - scip_bstart) - (cst_bend - cst_bstart))"

      - name: ctx_matches_read
        kind: evidence
        weight: 10
        expr: "CASE WHEN (cst_expr_ctx = 'Load' AND scip_is_read) THEN 1 ELSE 0 END"

      - name: ctx_matches_write
        kind: evidence
        weight: 10
        expr: "CASE WHEN (cst_expr_ctx = 'Store' AND scip_is_write) THEN 1 ELSE 0 END"

      - name: coord_encoding_penalty
        kind: quality
        weight: -100
        expr: "CASE WHEN (scip_col_unit IS NULL OR scip_line_base IS NULL) THEN 1 ELSE 0 END"

  rank:
    ambiguity_key_expr: "cst_ref_id"                    # group by the ref we’re trying to resolve
    ambiguity_group_id_expr: "stable_hash64(cst_ref_id)"
    order_by:
      - "score DESC"
      - "scip_bstart ASC"
      - "scip_bend ASC"
      - "scip_symbol ASC"
    keep: all   # or best if you want only the top edge per ref

  select_exprs:
    - "cst_ref_id as src"
    - "stable_hash64(scip_symbol) as dst"               # or your canonical symbol entity id
    - "'refers_to' as kind"
```

This uses your contract: it emits `score`, `confidence`, `ambiguity_group_id`, and groups candidates by the “thing being resolved.”

---

## 5) Where the “extra” registry fields become genuinely useful (quality signals)

You’ve captured a lot that’s *not* join keys, but *is* join-quality evidence:

### File/provider health signals (strongly recommended)

* **LibCST parse errors**: `cst_parse_errors` has `error_type`, `message`, plus coordinate metadata (`line_base`, `col_unit`, `end_exclusive`). Use this as a penalty for relationships derived from CST in that file. 
* **LibCST parse manifest**: `parser_backend`, `parsed_python_version`, `libcst_version` can help detect “mixed environments” and cluster issues. 
* **Tree-sitter stats**: `error_count`, `parse_timed_out`, `match_limit_exceeded` are excellent “do-not-trust-this-file-too-much” signals. 
* **SCIP diagnostics**: presence/severity counts can downweight SCIP-based edges in files with indexing problems. 
* **Coordinate encoding metadata**: SCIP’s `line_base`, `col_unit`, `end_exclusive` are exactly the kind of stuff that can explain overlap ambiguity if you’re converting to byte offsets. 

This aligns with your relationship guide’s warning about coordinate ambiguity and mismatches.

### Implementing file-level quality as a reusable view

Create a single `file_quality` view and join it into any cross-provider relationship:

```sql
-- Pseudocode SQL; translate to DataFusion DataFrame if you prefer
WITH
cst_err AS (
  SELECT file_id, CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS has_cst_parse_errors
  FROM cst_parse_errors
  GROUP BY file_id
),
ts AS (
  SELECT file_id,
         CASE WHEN stats.parse_timed_out THEN 1 ELSE 0 END AS ts_timed_out,
         stats.error_count AS ts_error_count,
         CASE WHEN stats.match_limit_exceeded THEN 1 ELSE 0 END AS ts_match_limit_exceeded
  FROM ts_stats
),
scip_diag AS (
  SELECT document_id AS file_id, CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS has_scip_diagnostics
  FROM scip_diagnostics
  GROUP BY document_id
)
SELECT
  f.file_id,
  COALESCE(cst_err.has_cst_parse_errors, 0) AS has_cst_parse_errors,
  COALESCE(ts.ts_timed_out, 0) AS ts_timed_out,
  COALESCE(ts.ts_error_count, 0) AS ts_error_count,
  COALESCE(ts.ts_match_limit_exceeded, 0) AS ts_match_limit_exceeded,
  COALESCE(scip_diag.has_scip_diagnostics, 0) AS has_scip_diagnostics
FROM file_index f
LEFT JOIN cst_err ON f.file_id = cst_err.file_id
LEFT JOIN ts ON f.file_id = ts.file_id
LEFT JOIN scip_diag ON f.file_id = scip_diag.file_id
```

Then in relationship scoring, add a `quality` feature like:

```yaml
- name: file_health_penalty
  kind: quality
  weight: -200
  expr: "has_cst_parse_errors + ts_timed_out + ts_match_limit_exceeded + has_scip_diagnostics"
```

---

## 6) Encoding “evidence tiers” directly in the spec

Your relationship guide explicitly recommends a precedence order (SCIP strongest, then symtable, then bytecode, etc.).

You can implement that with **no fancy modeling**:

* Each rule gets a big `base_score` constant (e.g., SCIP edges start at 2000, symtable 1500, bytecode 1200, heuristics 500).
* Your “union of rules” produces a superset.
* A final deterministic “keep best per ambiguity key” picks the winner.

That also matches the guide’s A1 strategy (generate candidates → rank → pick best deterministically).

---

## 7) The one thing I’d be careful about

Don’t let “extra fields” silently become *implicit keys*.

A safe rule of thumb:

* If a field is an **identifier** (`*_id`, `symbol`, stable hashes): treat as **hard key** only when you *know* it’s canonical for the entity.
* If a field is **textual / contextual** (`ref_text`, `callee_text`, `scope_type`, `parser_backend`): treat as **soft evidence**.
* If a field is **health/diagnostic** (`parse_errors`, `error_count`, `timed_out`, `col_unit`): treat as **quality signal** that scales confidence, not as a join gate (unless you intentionally want to drop those files).

---

### If you want one extra (tiny) enhancement

Because your registry already encodes semantic expectations and function requirements (including `stable_hash64`, `row_number`, and coordinate conversion hooks like `col_to_byte`) , you can make your compiler *safer* by:

* requiring any relationship with span logic to declare which coordinate space it’s using (byte vs line/char),
* and auto-adding a **quality penalty** if the required coordinate metadata is missing (SCIP’s `col_unit`, `line_base`, etc.). 

That keeps ambiguity handling robust without turning schema richness into complexity.

---

If you tell me the *exact names* of the normalized views you plan to expose in `src/semantics` (e.g., whether you already have `cst_defs_norm`, `scip_occurrences_norm` with byte spans, and what your canonical file identity column is for SCIP), I can tailor the two concrete YAML rule examples above into drop-in specs that match your actual view names and column prefixes—without changing the underlying model.
