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
from datafusion_engine.udf.shims import stable_id_parts, span_make

def _nullable_stable_id(ns: str, path_c: str, start_c: str, end_c: str, guard_cols=()):
    cond = col(start_c).is_null() | col(end_c).is_null()
    for g in guard_cols:
        cond = cond | col(g).is_null()
    return f.when(cond, lit(None)).otherwise(
        stable_id_parts(ns, col(path_c), col(start_c), col(end_c))
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
            span_make(col(spec.primary_span.canonical_start), col(spec.primary_span.canonical_end)),
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
