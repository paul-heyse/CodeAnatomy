

Below is a **deep dive for Topic #1 (“decorators / function modifier surface area”)** in the same “full surface area + minimum sufficient implementation detail + holistic examples” style as your reference doc. 
(Excludes OpenLineage.)

---

# 1) Decorators / function modifier surface area

## Mental model for this whole section

These modifiers fall into three “systems” patterns:

1. **Dependency rewiring** (compile-time graph edits): `@inject`, `@resolve_from_config`, `@parameterize_frame`
2. **Graph-local pipelines** (pre/post transforms as explicit nodes): pipe family (`pipe_input`, `pipe_output`, `step(...).when(...)`, naming, namespacing, `on_input`, `on_output`), and `@mutate`
3. **Tabular/column subDAGs** (map-like column transforms inside one DF node): `@with_columns`

---

## 1.1 `@inject` (single-shot parameter substitution)

### What you get (full surface area)

`@inject(**key_mapping)` replaces one or more function parameters with **dependency specs**, and is described as “like `@parameterize` but with only one parameterization whose output name is the function name.” ([Hamilton][1])

Core dependency spec primitives (shared with parameterization):

* `source("node_name")` → pull from an upstream node ([Hamilton][2])
* `value(obj)` → literal injection ([Hamilton][2])
* `group(...)` → pack multiple deps into a single list argument (supports positional + kw grouping) ([Hamilton][2])

Important limitation: undefined behavior if you use the **same function parameter multiple times as an injection** (e.g., inject the same param as two lists, list+dict, etc.). ([Hamilton][1])

### Why it matters (systems use cases)

* **Feature-set assembly without wrapper nodes**: pick which nodes feed an aggregator without redefining the aggregator or creating many near-duplicate nodes.
* **Clean “registry → injection” bridge**: convert a *catalog* (list of node names) into actual DAG wiring.
* **Decouple “what to compute” from “how to combine”**: keeps combination logic stable while wiring is configurable.

### Configuration / mechanics

1. Your function’s **signature** declares “shape” (e.g., `list[pd.Series]`, `dict[str, Any]`, etc.).
2. `@inject(...)` replaces selected parameters’ upstream providers with specs (`source/value/group`), effectively rewriting dependencies at DAG build time. ([Hamilton][1])
3. If you need a “dynamic” injection that depends on config, pair `@inject` with `@resolve_from_config` (covered in §1.6). ([hamilton.staged.apache.org][3])

### Minimal patterns

#### Pattern A — “bundle a few dependencies into one list input”

```python
from __future__ import annotations
from hamilton.function_modifiers import inject, group, source, value

@inject(nums=group(source("a"), value(10), source("b"), value(2)))
def sum_nums(nums: list[int]) -> int:
    return sum(nums)
```

This is the canonical “inject + group/source/value” composition. ([Hamilton][2])

#### Pattern B — “inject a dict-shaped registry”

```python
from hamilton.function_modifiers import inject, source

@inject(features={"loc": source("loc"), "cyclo": source("cyclomatic_complexity")})
def risk_score(features: dict[str, float]) -> float:
    return 0.2 * features["loc"] + 0.8 * features["cyclo"]
```

### Footguns

* **Hidden coupling**: `inject` rewires dependencies “out of band” relative to the function signature—great for modularity, but you must keep naming conventions stable.
* **Don’t double-inject the same parameter**: explicitly called out as undefined behavior. ([Hamilton][1])

---

## 1.2 `@parameterize_frame` (experimental “parameterize + extract spec from a DataFrame”)

### What you get (full surface area)

`parameterize_frame` is an **EXPERIMENTAL** decorator that instantiates a “parameterize_extract” decorator using a DataFrame-shaped spec; the docs explicitly warn the API may change. ([Hamilton][4])

The parameterization DataFrame has a specific shape:

* Columns are a **2-level index**:

  * level 0: parameter names
  * level 1: injection type: `"out" | "value" | "source"` ([Hamilton][4])
* Each row corresponds to one output configuration. ([Hamilton][4])
* Current ergonomic constraint: your function must accept the “column names” and output a DataFrame with those names (docs suggest this may change). ([Hamilton][4])

Docs also note it “currently works for series” with plans to extend. ([Hamilton][4])

### Why it matters

This is a “DAG shape from data” tool:

* Build many similar nodes from a **tabular catalog** (rules table, config table, experiment matrix).
* Keep *parameterization spec* out of code (but still versionable), while the transform stays in code.

### Configuration / mechanics

1. Build a spec DataFrame with MultiIndex columns (param name + injection type). ([Hamilton][4])
2. Decorate a function that returns a DataFrame; the decorator expands nodes based on the spec. ([Hamilton][4])

### Minimal example (close to docs, but “systemified”)

```python
from __future__ import annotations

import pandas as pd
from hamilton.experimental.decorators.parameterize_frame import parameterize_frame

# Imagine this table is produced from a config registry (YAML/DB) at import time.
spec = pd.DataFrame(
    [
        # output1,        output2,        input1,        input2,        weight
        ["risk_v1",       "risk_v2",       "loc",         "cyclo",       0.2],
        ["risk_v1_alt",   "risk_v2_alt",   "loc_alt",     "cyclo_alt",   0.5],
    ],
    columns=[
        ["out1", "out2", "loc_col", "cyclo_col", "w"],
        ["out",  "out",  "source",  "source",    "value"],
    ],
)

@parameterize_frame(spec)
def compute_risks(loc_col: pd.Series, cyclo_col: pd.Series, w: float) -> pd.DataFrame:
    # Your output df column names must match the "out" column entries from spec. :contentReference[oaicite:17]{index=17}
    return pd.DataFrame({
        "risk_v1": loc_col * w,
        "risk_v2": cyclo_col * (1 - w),
    })
```

### Footguns / when to avoid

* Because it’s explicitly experimental, use it when the *benefit of data-driven expansion* outweighs the *API-stability risk*. ([Hamilton][4])
* Watch naming collisions: this will mass-create outputs; pair with naming conventions + tags.

---

## 1.3 `@with_columns` (DataFrame column subDAG “map ops” executor)

### What you get (full surface area)

`with_columns` exists as plugin implementations for multiple dataframe backends:

* pandas: `hamilton.plugins.h_pandas.with_columns(...)` ([Hamilton][5])
* polars eager: `hamilton.plugins.h_polars.with_columns(...)` ([Hamilton][5])
* polars lazy: `hamilton.plugins.h_polars_lazyframe.with_columns(...)` with lazy typing (`pl.Expr`) ([Hamilton][5])
* spark: includes `mode: str = 'append'`; described as “linearizing” map DAG into chained `.withColumn` ops to avoid extract+join inefficiency. ([Hamilton][5])

Common constructor parameters (shared across backends) include:

* `*load_from`: functions/modules that define the column subDAG ([Hamilton][5])
* `select`: which computed “columns” to append to the dataframe ([Hamilton][5])
* `namespace`: avoid node-name clashes and allow reuse ([Hamilton][5])
* `columns_to_pass` vs `on_input` behavior:

  * `columns_to_pass` is used to decide which upstream inputs are taken from the dataframe; if empty, it may assume all deps are from the dataframe; cannot be used with `on_input`. ([Hamilton][5])
  * if `on_input` is provided, you’re responsible for extracting columns yourself. ([Hamilton][5])

### Why it matters

This is the “best-of-both-worlds” pattern:

* Write column logic as small pure functions (unit-testable, reusable).
* Execute them efficiently inside the dataframe engine, preserving a DAG of column lineage.

### Minimal example (polars eager vs lazy)

```python
from __future__ import annotations
import polars as pl
from hamilton.plugins.h_polars import with_columns

# column ops
def a_plus_b(a: pl.Series, b: pl.Series) -> pl.Series:
    return a + b

def a_b_average(a: pl.Series, b: pl.Series) -> pl.Series:
    return (a + b) / 2

@with_columns(
    a_plus_b,
    a_b_average,
    columns_to_pass=["a", "b"],
    select=["a_plus_b", "a_b_average"],
    namespace="feat",
)
def enriched(df: pl.DataFrame) -> pl.DataFrame:
    return df
```

The docs show the same conceptual pattern for eager, and for lazy mode you switch to `pl.Expr` ops and `pl.LazyFrame`. ([Hamilton][5])

### “Overwrite columns” nuance

The docs explicitly note cases where selected ops can overwrite columns (example given for polars lazy). ([Hamilton][5])
In practice: treat `select` as the “projection contract” for what gets appended/replaced; keep it stable and version it.

### Footguns

* `columns_to_pass` and `on_input` are mutually constraining; decide early whether you want Hamilton to auto-extract or you want explicit extraction. ([Hamilton][5])
* Always namespace reusable subDAGs to prevent global name collisions. ([Hamilton][5])

---

## 1.4 Pipe family “power knobs” (conditional steps, stable naming, target selection)

### What you get (full surface area)

#### Core pieces

* `pipe_input(*steps, namespace=..., on_input=...)`
* `pipe_output(*steps, namespace=..., on_output=...)`
* `step(fn, **bound_args)` where bound args may be literal or upstream:

  * `value(...)` and `source(...)` are supported inside step arg binding. ([hamilton.staged.apache.org][6])

The docs also note:

* `pipe` is deprecated in 2.0.0; prefer `pipe_input`. ([hamilton.staged.apache.org][6])
* `collapse` exists but is “not currently supported.” ([hamilton.staged.apache.org][6])
* Functions used in `step(...)` must be “kwarg-friendly” (no positional-only args, no `*args`, no `**kwargs`). ([hamilton.staged.apache.org][6])

#### Step-level config gating

Each `step(...)` supports `when/when_not/when_in/when_not_in` to include/exclude that step based on config; described as equivalent to `@config.when`, but applied *inside the pipeline*. ([hamilton.staged.apache.org][6])

#### Stable naming + namespacing

* `step(...).named("stable_name", namespace=...)` creates intermediate nodes you can reference later; docs warn auto-generated names aren’t guaranteed stable if you need to refer to them. ([hamilton.staged.apache.org][6])
* Namespacing options:

  * per-step namespace can be a string or `...` (“share the pipe namespace”) ([hamilton.staged.apache.org][6])
  * `pipe_input(namespace=...)` can set a common namespace for steps ([hamilton.staged.apache.org][6])

#### Selecting which function input is piped

`pipe_input(on_input=...)` chooses which parameter is transformed; default is “first parameter.” ([hamilton.staged.apache.org][6])

#### Selecting which output is piped (multi-output nodes)

`pipe_output` supports:

* per-step targeting: `step(B).on_output("field_1")` ([hamilton.staged.apache.org][6])
* global targeting: `pipe_output(..., on_output=["field_1","field_2"])` (docs include a warning about mixing global + step-level targeting). ([hamilton.staged.apache.org][6])

### Why it matters

Pipe family is how you:

* Make “preprocessing / postprocessing” **first-class DAG structure** instead of hidden in function bodies.
* Create reusable transformation chains without node redefinition.
* Express *conditional DAG shape* (config-driven) at the step level.

### Minimal example (showing the power knobs together)

```python
from __future__ import annotations
from hamilton.function_modifiers import pipe_input, step, source, value

def _drop_bad_rows(df):
    return df[df["loc"].notna()]

def _clip_loc(df, max_loc: int):
    df = df.copy()
    df["loc"] = df["loc"].clip(upper=max_loc)
    return df

@pipe_input(
    step(_drop_bad_rows).when(clean_mode="strict"),
    step(_clip_loc, max_loc=value(2000)).named("loc_clipped", namespace="prep"),
    on_input="df",           # pipe over "df", not the first param by accident :contentReference[oaicite:43]{index=43}
    namespace="prep",
)
def downstream(df):
    return df
```

Key ideas are all documented: step-level `when` for DAG shape ([hamilton.staged.apache.org][6]), stable naming ([hamilton.staged.apache.org][6]), and `on_input` selection ([hamilton.staged.apache.org][6]).

---

## 1.5 `@mutate` (post-process *other* nodes without touching them)

### What you get (full surface area)

`mutate` is described as closely related to `pipe_output`, enabling transformations on a node output **without touching the node**, by selecting target functions. ([hamilton.staged.apache.org][6])

Constraints + guidance:

* Target functions must currently be in the **same module** (docs suggest reaching out if you need cross-module). ([hamilton.staged.apache.org][6])
* Suggested convention: name mutator functions with a leading underscore so they only show in transform pipelines. ([hamilton.staged.apache.org][6])

Documented use cases include: pre-cleaning, feature engineering transforms, and experimentation via selectively toggling transforms. ([hamilton.staged.apache.org][6])

Additional dependency binding works like pipe family:

* The mutator’s first argument is the target’s output.
* Extra args can be wired via `step(...)`/`value(...)`. ([hamilton.staged.apache.org][6])

Per-target specialization:

* Use `apply_to(...)` to give different injected args per target. ([hamilton.staged.apache.org][6])

Multi-output targeting:

* Use `.on_output("field_1")` against targets that are “expanded” by extractors. ([hamilton.staged.apache.org][6])

### Why it matters

`mutate` is the cleanest “cross-cutting normalization layer” you can add after the fact:

* “Everything we load gets standardized”
* “All risk scores get clipped”
* “Apply transformation X only to subset of outputs”

…without forking original nodes.

### Minimal example

```python
from __future__ import annotations
from hamilton.function_modifiers import mutate, apply_to, value

def raw_scores() -> dict[str, float]:
    return {"a": 3.0, "b": 999.0}

def score_a(raw_scores: dict[str, float]) -> float:
    return raw_scores["a"]

def score_b(raw_scores: dict[str, float]) -> float:
    return raw_scores["b"]

@mutate(
    apply_to(score_a, max_value=value(10.0)),
    apply_to(score_b, max_value=value(100.0)),
)
def _clip(score: float, max_value: float) -> float:
    return min(score, max_value)
```

This is the “apply_to per target” pattern. ([hamilton.staged.apache.org][6])

---

## 1.6 `resolve_from_config` (compile-time decorator synthesis)

### What you get (full surface area)

`resolve` delays decorator evaluation until configuration is available; it’s explicitly labeled a power-user feature requiring `hamilton.enable_power_user_mode=True`. ([hamilton.staged.apache.org][3])
The docs also recommend isolating these functions in their own module for readability and so you can build other DAGs without enabling power-user mode. ([hamilton.staged.apache.org][3])

`resolve_from_config` is a convenience subclass that resolves at config-available time (driver instantiation / compile time). ([hamilton.staged.apache.org][3])

The contract:

* `decorate_with=callable(...) -> decorator`
* callable args correspond to config keys (by name), and returns a *real* Hamilton decorator (e.g., `parameterize_sources`, `inject`, even plugin decorators). ([hamilton.staged.apache.org][3])

### Why it matters

This is how you build “best in class” *design-time variability*:

* “Same codebase, different semantic layer”
* “Select features / transforms / materializers by environment”
* “Versioned DAGs” with minimal branching

### Minimal example (docs pattern, generalized)

```python
from __future__ import annotations
import pandas as pd
from hamilton.function_modifiers import resolve_from_config, parameterize_sources

@resolve_from_config(
    decorate_with=lambda s1_pairs, s2_pairs: parameterize_sources(
        sum_1={"s1": s1_pairs[0], "s2": s2_pairs[1]},
        sum_2={"s1": s2_pairs[1], "s2": s2_pairs[2]},
    )
)
def summation(s1: pd.Series, s2: pd.Series) -> pd.Series:
    return s1 + s2
```

This is the documented pattern: config-driven callable returns a decorator that expands/rewires the node at compile time. ([hamilton.staged.apache.org][3])

### Power move: `resolve_from_config` + `inject`

Use config to decide “which nodes feed this aggregator,” then return an `inject(...)` decorator (often via `group(source(...), ...)`).

---

## 1.7 `@tag` advanced targeting + `tag_outputs` per-output tagging

### What you get (full surface area)

#### `@tag` details

* Attaches metadata (key/value tag pairs) to nodes. ([Hamilton][7])
* You can namespace tag keys (dots) only via `@tag(**{"namespace.key": "value"})`. ([Hamilton][7])
* `target_` controls *which produced nodes* get the tags (critical when a decorator expands multiple nodes):

  * `None`: tag “final” nodes produced by this decorator expansion
  * `...`: tag all nodes produced
  * `Collection[str]` or `str`: tag specific node(s) ([Hamilton][7])
* There is a `bypass_reserved_namespaces_` knob to skip reserved namespace checks. ([Hamilton][7])

#### Querying tags

The tag page shows a simple pattern: use `Driver.list_available_variables()` and filter on `o.tags`. ([Hamilton][7])
The Driver reference also supports a `tag_filter` argument with semantics for exact match, multi-values, “tag exists”, and AND queries. ([Hamilton][8])

#### `tag_outputs`

`tag_outputs(**tag_mapping)` assigns tags per output name, explicitly “akin to applying `@tag` to individual outputs produced by the function,” and it does **not validate** spelling because it accepts a superset of nodes. ([Hamilton][7])
Docs show a typical pairing with extractors: tag outputs + `extract_columns(...)`. ([Hamilton][7])

### Why it matters (in your “semantic layer” world)

Tags are your best mechanism to make the DAG:

* self-describing (entity/grain/join keys)
* queryable (“show me all semantic outputs”)
* versionable (semantic_id, schema_version, etc.)

…and to let build tools automatically compile registries from DAG metadata.

---

# Holistic example (combining multiple features)

Below is a single coherent pattern that uses:

* `pipe_input` step gating (`when`) + naming + namespace
* `with_columns` for column subDAG
* `extract_columns` + `tag_outputs` for semantic metadata
* `mutate(...).on_output(...)` for cross-cutting normalization
* `resolve_from_config` to choose the feature set at compile time
* `inject + group(source(...))` to wire an aggregator from a config-defined list

> This is intentionally “systems-shaped” rather than minimal: it shows how these modifiers become *design primitives*.

## `feature_ops.py` (column subDAG)

```python
from __future__ import annotations
import pandas as pd

def loc_bucket(loc: pd.Series) -> pd.Series:
    return pd.cut(loc, bins=[0, 50, 200, 1000, 10_000], include_lowest=True)

def is_public(symbol: pd.Series) -> pd.Series:
    return ~symbol.str.startswith("_")

def base_risk(loc: pd.Series, cyclo: pd.Series) -> pd.Series:
    return 0.3 * loc + 0.7 * cyclo
```

## `pipeline.py` (DAG assembly)

```python
from __future__ import annotations

import pandas as pd
from hamilton.function_modifiers import (
    pipe_input, pipe_output, step, source, value,
    extract_columns, tag, tag_outputs,
    mutate, apply_to,
    resolve_from_config, inject, group,
)
from hamilton.plugins.h_pandas import with_columns

import feature_ops

def raw_functions_df() -> pd.DataFrame:
    # columns: symbol, loc, cyclo, module, ...
    ...

def _drop_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(subset=["loc", "cyclo"])

def _clip_loc(df: pd.DataFrame, max_loc: int) -> pd.DataFrame:
    df = df.copy()
    df["loc"] = df["loc"].clip(upper=max_loc)
    return df

@pipe_input(
    step(_drop_bad_rows).when(clean_mode="strict"),
    step(_clip_loc, max_loc=value(3000)).named("loc_clipped", namespace="prep"),
    on_input="df",
    namespace="prep",
)
def cleaned_functions_df(df: pd.DataFrame = source("raw_functions_df")) -> pd.DataFrame:
    return df

# --- compile-time feature selection via resolve_from_config ---
@resolve_from_config(
    decorate_with=lambda selected_cols: with_columns(
        feature_ops,
        columns_to_pass=["symbol", "loc", "cyclo"],
        select=selected_cols,      # config controls which features append
        namespace="feat",
    )
)
@tag(data_product="codeintel", entity="function", grain="per_function")
def functions_enriched_df(cleaned_functions_df: pd.DataFrame) -> pd.DataFrame:
    return cleaned_functions_df

# Split out semantic columns & tag them individually
@tag_outputs(
    base_risk={"semantic_id": "base_risk", "dtype": "float", "kind": "metric"},
    loc_bucket={"semantic_id": "loc_bucket", "dtype": "category", "kind": "feature"},
    is_public={"semantic_id": "is_public", "dtype": "bool", "kind": "feature"},
)
@extract_columns("base_risk", "loc_bucket", "is_public")
def semantic_columns(functions_enriched_df: pd.DataFrame) -> pd.DataFrame:
    return functions_enriched_df

# Cross-cutting postprocess: clip risk outputs, but only for the extracted "base_risk" output
@mutate(
    apply_to(semantic_columns).on_output("base_risk"),
    max_value=value(10_000.0),
)
def _clip_risk(base_risk: pd.Series, max_value: float) -> pd.Series:
    return base_risk.clip(upper=max_value)

# Config-driven aggregator wiring via resolve_from_config + inject + group(source(...))
@resolve_from_config(
    decorate_with=lambda risk_inputs: inject(
        cols=group(*[source(name) for name in risk_inputs])
    )
)
def combined_risk(cols: list[pd.Series]) -> pd.Series:
    # example: sum a variable set of series
    out = cols[0].copy()
    for s in cols[1:]:
        out = out + s
    return out
```

### How to run/configure (the “minimum sufficient” bits)

* `pipe_input` step gating uses `step(...).when(...)` to shape the DAG like `@config.when`. ([hamilton.staged.apache.org][6])
* `with_columns` runs a subDAG of map ops “inside” the dataframe node; `columns_to_pass` determines which dependencies come from the df. ([Hamilton][5])
* `resolve_from_config` requires `hamilton.enable_power_user_mode=True` and is resolved at compile time. ([hamilton.staged.apache.org][3]) ([hamilton.staged.apache.org][3])
* `inject + group(source(...))` is the standard way to build “list inputs” from named upstream nodes. ([Hamilton][2])
* `mutate(...).on_output(...)` targets a single output of an extracted node. ([hamilton.staged.apache.org][6])
* You can compile a semantic registry by scanning tags; `Driver.list_available_variables(tag_filter=...)` supports “tag exists”, multi-values, and AND. ([Hamilton][8])

---

If you want next, I can do the same “feature-complete + minimum sufficient” expansion for **Topic #2 (“Driver/Builder execution + composition knobs”)**, and I’ll thread these decorators into an opinionated “semantic-layer conventions” pattern (tag taxonomy + registry compilation + stable naming rules) so you can lock design choices early.

[1]: https://hamilton.apache.org/reference/decorators/inject/ "inject - Hamilton"
[2]: https://hamilton.apache.org/reference/decorators/parameterize/ "parameterize - Hamilton"
[3]: https://hamilton.staged.apache.org/reference/decorators/resolve/ "resolve - Hamilton"
[4]: https://hamilton.apache.org/reference/decorators/parameterize_frame/ "parameterize_frame - Hamilton"
[5]: https://hamilton.apache.org/reference/decorators/with_columns/ "with_columns - Hamilton"
[6]: https://hamilton.staged.apache.org/reference/decorators/pipe/ "pipe family - Hamilton"
[7]: https://hamilton.apache.org/reference/decorators/tag/ "tag* - Hamilton"
[8]: https://hamilton.apache.org/reference/drivers/Driver/ "Builder - Hamilton"



# 2) Driver/Builder execution + composition knobs

## 2.0 System model: “build-time DAG” vs “run-time execution”

Hamilton has two *orthogonal* control planes:

* **Build-time (DAG construction):** which modules/nodes exist, which config-gated variants are selected, how duplicates resolve, what instrumentation/adapters are attached. `driver.Builder()` is the canonical assembly surface. ([Hamilton][1])
* **Run-time (execution):** `Driver.execute(final_vars, inputs=..., overrides=...)` for value injection and “what-if/testing” without rebuilding. ([Hamilton][2])

**Rule of thumb:** if you want to change *DAG shape*, rebuild the driver; if you want to change *inputs/values*, use `inputs` or `overrides`. The docs explicitly note config can’t be changed after the driver is created—you rebuild with new config values. ([Hamilton][1])

---

## 2.1 Graph composition: modules, conflicts, and controlled override layers

### 2.1.1 `Builder.with_modules(*modules)`

Adds one or more Python modules to crawl for Hamilton nodes; multiple modules assemble into one dataflow. ([Hamilton][1]) ([Hamilton][2])

**Design leverage:** organize modules by dependency footprint (e.g., “heavy ML deps” isolated) and by semantic domain (features/training/eval) for reuse across projects. ([Hamilton][1])

### 2.1.2 Duplicate node names + `Builder.allow_module_overrides()`

By default, same-named functions across modules are an error at build time (“cannot have two nodes with the same name”). ([Hamilton][1])
`allow_module_overrides()` changes semantics so later modules win: “later module overrides previous one(s)” and module order matters. ([Hamilton][2]) ([Hamilton][1])

**Best-in-class pattern (recommended):**

* Treat the module list as a **layer stack**:

  1. `core_*` (canonical definitions)
  2. `plugins_*` (optional feature nodes)
  3. `overrides_*` (environment overrides/hotfixes)
* Use `allow_module_overrides()` only when you *intend* to allow “late binding” overrides.

### 2.1.3 `Builder.copy()`

Creates a copy of the builder state, but the docs caution the copy “holds reference of Builder attributes.” ([Hamilton][2])

**Use case:** “variant drivers” (dev/prod, different caches, different adapters) without rewriting assembly code—just be mindful of shared references if you mutate nested objects.

---

## 2.2 Configuration: build-time selection & precedence

### 2.2.1 `Builder.with_config(config: dict)`

Adds config used during DAG construction; can be called multiple times and later calls take precedence. ([Hamilton][2])

This config is what powers decorators like `@config.when(...)` / `@config.when_not(...)` (and power-user `@resolve`/`resolve_from_config` patterns from Topic #1). ([Hamilton][1])

### 2.2.2 Immutable after build

Once the `Driver` is created, you can’t change build-time config; you rebuild a new driver with new config values. ([Hamilton][1])

**System implication:** for services, do one of:

* Build a small **DriverFactory** that caches “drivers by config fingerprint”.
* Or build per-request drivers only when config space is small and build time is acceptable.

---

## 2.3 Execution backends: adapters vs executors vs lifecycle adapters

Hamilton gives you three levers that people often conflate:

### 2.3.1 Graph adapter (`Builder.with_adapter(...)`)

Sets the execution adapter (a `HamiltonGraphAdapter`). ([Hamilton][2])
This is the “submit each node to an execution system” path (threadpool/ray/dask/etc.). Dynamic DAG docs call this “Using an Adapter” and emphasize you “don’t need to touch your Hamilton functions.” ([Hamilton][3])

Example: `FutureAdapter` delegates node execution to a threadpool and avoids serialization concerns; it’s best for I/O-heavy graphs, limited for CPU-heavy work, and it explicitly doesn’t support `Parallelizable/Collect`. ([Hamilton][4])

### 2.3.2 Lifecycle adapters / hooks (`Builder.with_adapters(...)`)

Attaches lifecycle hooks/validators/result builders/etc. via the lifecycle adapter set (the signature enumerates many hook types). ([Hamilton][2])

**Use case:** instrumentation, type checking, task hooks (dynamic execution), progress bars, slack notifiers, graceful error adapters, etc. (all non-OpenLineage).

### 2.3.3 Graph executors (Default vs Task-based)

* **DefaultGraphExecutor** does DFS execution in-memory and cannot handle `Parallelizable/Collect`. ([Hamilton][2])
* **TaskBasedGraphExecutor** is used for dynamic execution; it groups nodes into tasks and runs them task-by-task (supports `Parallelizable/Collect`). ([Hamilton][2])

---

## 2.4 Dynamic execution: `Parallelizable[]/Collect[]` + task orchestration knobs

### 2.4.1 Enable it: `Builder.enable_dynamic_execution(allow_experimental_mode=True)`

`enable_dynamic_execution()` enables `Parallelizable[]` and grouped task execution/parallel execution. ([Hamilton][2])
The dynamic execution guide says you currently need `allow_experimental_mode=True` to toggle the V2 executor. ([Hamilton][3])

### 2.4.2 Pick how tasks run: `with_local_executor(...)` / `with_remote_executor(...)`

Builder exposes:

* `with_local_executor(local_executor: TaskExecutor)` ([Hamilton][2])
* `with_remote_executor(remote_executor: TaskExecutor)` ([Hamilton][2])

Dynamic execution docs show a concrete pattern using executors from `hamilton.execution.executors` (local synchronous + remote multiprocessing). ([Hamilton][3])

### 2.4.3 Task grouping & routing: `with_grouping_strategy(...)` and `with_execution_manager(...)`

* `with_grouping_strategy(grouping_strategy: GroupingStrategy)` controls how nodes are grouped into tasks. ([Hamilton][2])
* `with_execution_manager(execution_manager: ExecutionManager)` controls assigning executors to node groups; it cannot be used if local/remote executors are set. ([Hamilton][2])

### 2.4.4 Known caveats (important to design around)

The dynamic execution guide explicitly calls out:

1. no nested `Parallelizable/Collect` blocks,
2. multiprocessing serialization uses default pickle (can break),
3. only one `Collect[]` input per function. ([Hamilton][3])

It also shows a “fix” approach using `@resolve` + `@inject` + `group(source(...))` to avoid multiple collects by selecting what to collect at DAG construction time. ([Hamilton][3])

### 2.4.5 Task-level observability hooks (dynamic execution only)

If you want “best in class” introspection/telemetry around task expansion/submission:

* **TaskSubmissionHook**: `pre_task_submission(...)` receives `run_id`, `task_id`, `nodes`, `inputs`, `overrides`, and `spawning_task_id`; only useful in dynamic execution. ([Hamilton][5])
* **TaskGroupingHook**: `post_task_expand(...)` and `post_task_group(...)` to observe expansion + grouping; only useful in dynamic execution. ([Hamilton][6])

You attach these via `Builder.with_adapters(...)`. ([Hamilton][2])

---

## 2.5 Caching: builder-level policy, custom stores, and production-safe patterns

### 2.5.1 Enable + configure: `Builder.with_cache(...)`

The builder-level API is the main “control surface”:

* sets cache path + stores
* per-node behaviors: `default`, `recompute`, `ignore`, `disable` (each can be `True` for all or a list of node names)
* defaults: `default_behavior`, plus loader/saver defaults
* `log_to_file` enables JSONL logs under the metadata store directory ([Hamilton][2]) ([Hamilton][2])

### 2.5.2 Architecture (why it scales)

Caching is explicitly built from:

* a **cache adapter** (decides hit vs execute),
* a **metadata store** (tracks cache keys/data versions/runs),
* a **result store** (maps `data_version -> result`). ([Hamilton][7])

The docs also warn cache keys can be unstable across Python/Hamilton versions and upgrades may require a new empty cache for reliable behavior. ([Hamilton][7])

### 2.5.3 Store interfaces (plug-in point for “real systems”)

If you want caching to be an architectural component, the `MetadataStore` / `ResultStore` APIs are the interface boundary (run IDs, get_run metadata, etc.). ([Hamilton Apache Incubator][8]) ([Hamilton Apache Incubator][8])

### 2.5.4 Separation of metadata vs results + inspection

The caching concept docs spell out:

* metadata store is lightweight “execution facts”
* result store can grow large; default pickling; other formats possible
* you can pass stores directly to `.with_cache(metadata_store=..., result_store=...)` (then `path` is ignored). ([Hamilton][9])

You can also inspect via `Driver.cache` and query stores directly. ([Hamilton][9])

### 2.5.5 In-memory caching (dev notebooks / interactive)

The caching docs show in-memory metadata/results stores and persistence back to disk. ([Hamilton][9])
The Stores reference provides `InMemoryMetadataStore.load_from(...)` and `.persist_to(...)` patterns. ([Hamilton Apache Incubator][8])

---

## 2.6 Materializers + graph-level overrides (data I/O as first-class nodes)

### 2.6.1 `Builder.with_materializers(...)`

Adds DataLoader/DataSaver (“materializer”) nodes that show up in visualization and can be executed by name via `Driver.execute()`. ([Hamilton][2]) ([Hamilton][1])

### 2.6.2 `Driver.materialize(...)` and “from_ extractor behaves like overrides”

The driver docs explicitly describe that a `from_...` extractor can replace nodes inside a DAG—“effectively functions as overrides” but computed dynamically rather than statically passed. ([Hamilton][2])

### 2.6.3 Validate without running side effects

`Driver.validate_materialization(...)` is effectively a dry-run of `.materialize()` and raises if issues are detected. ([Hamilton][2])

---

## 2.7 Driver inspection, validation, navigation, and reproducibility

### 2.7.1 `Driver.execute(..., inputs=..., overrides=...)`

* `inputs`: runtime inputs
* `overrides`: substitute node values (great for tests, partial recompute, “inject fixtures”) ([Hamilton][2])

### 2.7.2 Prefer `execute()` over `raw_execute()`

`raw_execute()` exists but docs say “don’t use this entry point” and recommend `.execute()` (and note `base.DictResult()` is the default execute return when building via `driver.Builder()`). ([Hamilton][2])

### 2.7.3 Validate without executing

* `validate_execution(...)` raises if execution issues can be detected. ([Hamilton][2])
* `validate_inputs(...)` checks runtime inputs don’t clash with config and that required inputs are provided. ([Hamilton][2])
* `has_cycles(...)` checks for cycles. ([Hamilton][2])

### 2.7.4 Graph export for snapshots

`export_execution(...)` creates a JSON representation of the execution graph given final vars/inputs/overrides. ([Hamilton][2])

### 2.7.5 Introspection: tags are a first-class query surface

`list_available_variables(tag_filter=...)` returns nodes with name/tags/type/is_external_input. ([Hamilton][2])
The `tag_filter` supports: exact match, value-in-list, tag existence (`None`), and AND across tags (see examples). ([Hamilton][2])

### 2.7.6 Visualization + navigation

* `display_all_functions`, `display_downstream_of`, `display_upstream_of` for DOT/graphviz visualization. ([Hamilton][2]) ([Hamilton][2])
* `visualize_execution(...)` has a notable limitation: overrides are “not handled at this time.” ([Hamilton][2])
* graph navigation helpers: `what_is_upstream_of`, `what_is_downstream_of`, `what_is_the_path_between`. ([Hamilton][2])

### 2.7.7 Telemetry hooks (built-in capture points)

`capture_constructor_telemetry(...)` and `capture_execute_telemetry(...)` exist as part of the driver surface. ([Hamilton][2])

---

## 2.8 AsyncDriver: correct service integration shape

### 2.8.1 When to use

AsyncDriver is explicitly intended for async contexts (e.g., FastAPI). ([Hamilton][10])

### 2.8.2 Initialization & limitations

* If you use hooks/adapters, you need to call `ainit()` (backwards-compat path), and the docs recommend using `hamilton.async_driver.Builder` instead of instantiating directly. ([Hamilton][10])
* It “only (currently) work[s] properly with asynchronous lifecycle hooks” and doesn’t support methods or validators; synchronous hooks may behave strangely. ([Hamilton][10])
* The async builder is “more limited” and (per docs) does not support dynamic execution or materializers “for now.” ([Hamilton][10])

---

# 2.9 Opinionated semantic-layer conventions (tag taxonomy + registry compilation + naming rules)

This is the “lock design choices early” pattern that turns Hamilton into a **semantic-contract compiler**.

## 2.9.1 Tag taxonomy (minimal sufficient set)

Use tags as a *semantic ABI* (agents/tools read tags; code computes nodes). A compact schema that scales:

### Core identity

* `layer`: `"semantic"` | `"docs"` | `"raw"` | `"intermediate"`
* `semantic_id`: stable identifier used by clients/agents (do **not** overload node name)
* `entity`: `"function"` | `"module"` | `"class"` | `"repo"` | …
* `grain`: `"per_function"` | `"per_module"` | …

### Join & keying

* `entity_keys`: e.g. `"repo,commit,goid_h128"` (or a JSON array string)
* `join_keys`: `"repo,commit,module"` (keys expected present in DF outputs)

### Type & shape

* `dtype`: `"int64"` | `"float"` | `"str"` | `"bool"` | `"json"` | …
* `schema_ref`: `"semantic.function_risk_v1"` (logical schema name)
* `version`: semantic schema version (e.g., `"1"`)

### Operational metadata (optional but high leverage)

* `owner`: team/service name
* `stability`: `"experimental"` | `"stable"`
* `pii`: `"none"` | `"possible"` | `"yes"`

## 2.9.2 Stable naming rules (so graphs don’t rot)

1. **Node names = implementation names** (function names) and are allowed to change; `semantic_id` is the *stable public key*.
2. When you need “override behavior,” prefer a dedicated override module + `allow_module_overrides()` so the override boundary is explicit and deterministic (later modules win). ([Hamilton][2])
3. For any generated/intermediate nodes (pipes/subdags/extracts), force stable naming (Topic #1’s `named(...)`, namespaces) and keep those names out of the public registry unless they’re semantic outputs.

## 2.9.3 Registry compilation via `list_available_variables(tag_filter=...)`

The driver can return nodes with tags and types, and tag filters support “tag exists” queries (`None`) and AND across tags. ([Hamilton][2]) ([Hamilton][2])

So your registry builder becomes a pure function:

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)
class SemanticSpec:
    node_name: str          # Hamilton node name (execution key)
    semantic_id: str        # stable public name
    entity: str
    grain: str
    dtype: str | None
    schema_ref: str | None
    tags: dict[str, Any]

def compile_semantic_registry(dr) -> dict[str, SemanticSpec]:
    # Require layer=semantic AND semantic_id exists
    nodes = dr.list_available_variables(tag_filter={"layer": "semantic", "semantic_id": None})
    out: dict[str, SemanticSpec] = {}
    for n in nodes:
        tags = dict(n.tags)
        sid = tags["semantic_id"]
        out[sid] = SemanticSpec(
            node_name=n.name,
            semantic_id=sid,
            entity=tags.get("entity", "unknown"),
            grain=tags.get("grain", "unknown"),
            dtype=tags.get("dtype"),
            schema_ref=tags.get("schema_ref"),
            tags=tags,
        )
    return out
```

## 2.9.4 Snapshot discipline (CI-friendly)

Two “cheap and strong” invariants:

* **Registry snapshot:** `compile_semantic_registry(dr)` JSON snapshot in CI (diff on PR).
* **Graph snapshot:** `export_execution(...)` JSON snapshot for your canonical semantic outputs. ([Hamilton][2])

This prevents accidental semantic drift even when the internal DAG evolves.

---

# Holistic example: “Semantic Driver Factory” (modules + config + caching + dynamic execution + registry)

### `semantic_driver_factory.py`

```python
from __future__ import annotations

import os
from typing import Any

from hamilton import driver
from hamilton.execution import executors

def build_driver(
    *,
    modules: list[Any],
    config: dict[str, Any],
    enable_dynamic: bool,
    cache_path: str | None,
    cache_opt_in: bool = True,
):
    b = driver.Builder().with_modules(*modules)

    # Config is build-time; later calls override earlier values. :contentReference[oaicite:66]{index=66}
    b = b.with_config(config)

    # Optional: dynamic execution (Parallelizable/Collect) requires allow_experimental_mode=True. :contentReference[oaicite:67]{index=67}
    if enable_dynamic:
        b = (
            b.enable_dynamic_execution(allow_experimental_mode=True)
             .with_local_executor(executors.SynchronousLocalTaskExecutor())
             .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=5))
        )

    # Caching: configure opt-in by default_behavior="disable" (cache only what you specify). :contentReference[oaicite:68]{index=68}
    if cache_path:
        if cache_opt_in:
            b = b.with_cache(path=cache_path, default_behavior="disable", log_to_file=True)
        else:
            b = b.with_cache(path=cache_path, log_to_file=True)

    return b.build()  # Builder statement order doesn’t matter as long as build is last. :contentReference[oaicite:69]{index=69}

def default_runtime_config() -> dict[str, Any]:
    return {
        "env": os.getenv("APP_ENV", "dev"),
        # include your config.when keys, resolve_from_config keys, etc.
    }
```

### “Semantic node tagging” (sketch; ties to Topic #1)

```python
from hamilton.function_modifiers import tag

@tag(
    layer="semantic",
    semantic_id="function.risk_score.v1",
    entity="function",
    grain="per_function",
    entity_keys="repo,commit,goid_h128",
    join_keys="repo,commit,goid_h128",
    dtype="float",
    schema_ref="semantic.function_risk_v1",
    version="1",
)
def risk_score_v1(...):
    ...
```

### “Registry compiler”

Use the `compile_semantic_registry(dr)` function above; it relies on `list_available_variables(tag_filter=...)` semantics. ([Hamilton][2])

---

If you want, I can now apply the same treatment to **Topic #3 (“Caching internals + stores + structured logs + operational patterns”)** or **Topic #4 (“UI/telemetry SDK mechanics”)**, but the above should fully cover the *Builder/Driver execution surface area* plus a concrete “semantic layer conventions” design you can codify immediately.

[1]: https://hamilton.apache.org/concepts/builder/ "Builder - Hamilton"
[2]: https://hamilton.apache.org/reference/drivers/Driver/ "Builder - Hamilton"
[3]: https://hamilton.apache.org/concepts/parallel-task/ "Dynamic DAGs/Parallel Execution - Hamilton"
[4]: https://hamilton.apache.org/reference/graph-adapters/ThreadPoolFutureAdapter/ "h_threadpool.FutureAdapter - Hamilton"
[5]: https://hamilton.apache.org/reference/lifecycle-hooks/TaskSubmissionHook/ "lifecycle.api.TaskSubmissionHook - Hamilton"
[6]: https://hamilton.apache.org/reference/lifecycle-hooks/TaskGroupingHook/ "lifecycle.api.TaskGroupingHook - Hamilton"
[7]: https://hamilton.dagworks.io/en/latest/concepts/caching/?utm_source=chatgpt.com "Caching - Hamilton"
[8]: https://hamilton.incubator.apache.org/content/reference/caching/stores/?utm_source=chatgpt.com "Stores - Hamilton"
[9]: https://hamilton.apache.org/concepts/caching/?utm_source=chatgpt.com "Caching - Hamilton"
[10]: https://hamilton.apache.org/reference/drivers/AsyncDriver/ "AsyncDriver - Hamilton"
