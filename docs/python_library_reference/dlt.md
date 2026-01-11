## Foundational bundle deep dive: Pipeline + Source + Resource + resource transforms (dlt)

This chapter is the “mental model + API surface” you’ll keep returning to. Everything else in dlt (schema/contracts/state/destinations/observability) is downstream of these objects.

> The docs excerpts below are from dlt Docs “latest” (showing **1.20.0** at time of browsing). ([dltHub][1])

---

# 1) The 4 primitives and how they compose

### **Pipeline**

A pipeline is the orchestration wrapper that accepts sources/resources (or plain iterables), runs **extract → normalize → load**, and writes to a **destination** + **dataset**. ([dltHub][2])

### **Source**

A source is a *logical grouping* of resources—typically the endpoints of one API or one logical system. It’s a function decorated with `@dlt.source` that returns one or more resources (and optionally carries schema hints). ([dltHub][3])

### **Resource**

A resource is an (optionally async) function that yields data items. It is the unit that maps most directly to “tables at the destination,” and it’s the main point where you specify **write policy**, **hints**, and **in-flight transforms**. ([dltHub][4])

### **Transforms**

There are *two* transform planes:

1. **In-resource “item pipe” transforms**: `resource.add_map`, `add_filter`, `add_yield_map`, `add_metrics`, `add_limit`—these are per-item pipeline steps attached to a resource. ([dltHub][4])
2. **Transformer resources**: `@dlt.transformer` resources that take upstream items (`data_from=...`) and optionally run in parallel or in deferred pools; they compose via the `|` pipe operator. ([dltHub][4])

---

# 2) Pipeline in depth

## 2.1 Create a pipeline: identity, destination, dataset

`dlt.pipeline(...)` is where you set the durable identity and where the run artifacts live.

Key args (conceptually):

* `pipeline_name`: used to identify and restore pipeline state/schemas across runs; if not provided, dlt derives it from the executing module filename. ([dltHub][2])
* `destination`: where data loads (DuckDB, BigQuery, etc.). ([dltHub][2])
* `dataset_name`: destination grouping (database schema / dataset / folder depending on destination). ([dltHub][2])

## 2.2 `pipeline.run(...)`: what it accepts + what it does

`pipeline.run(data, ...)` is the “do the thing” API. The pipeline can accept:

* dlt **sources** or **resources**
* generator functions / iterators
* lists / general iterables ([dltHub][2])

**High-level semantics**:

* `run` extracts from `data`, infers schema, normalizes into load packages (e.g., jsonl or parquet) and then loads into the destination. ([dltHub][5])
* The method also supports per-run overrides like `table_name`, `write_disposition`, `primary_key`, `columns`, `schema`, `schema_contract`, etc. ([dltHub][5])
* dlt explicitly mentions it supports common “non-JSON-native” Python values such as `bytes`, `datetime`, `decimal`, `uuid` in documents you load. ([dltHub][5])

### `write_disposition` at the pipeline level

Pipeline-level `write_disposition` sets the default write behavior for the run:

* `append`: always add rows
* `replace`: replace existing data
* `merge`: dedupe/merge based on `primary_key` and `merge_key` hints ([dltHub][2])

In practice: you usually specify disposition at the **resource** (table) level, and only use `pipeline.run(write_disposition=...)` as an override.

## 2.3 Pending work + destination sync (the “it’s not just a function call” part)

The API reference is explicit that `pipeline.run` is stateful and “resumable”:

* Before doing new work, `run` calls `sync_destination` to synchronize state/schemas with the destination (this can be disabled via a `restore_from_destination` configuration option). ([dltHub][5])
* Then it checks if previous data is fully processed; if not, it will **normalize/load pending items and exit** (instead of mixing old + new in one logical run). ([dltHub][5])
* `sync_destination` may restore state/schemas from the destination when the destination has a higher state version, potentially deleting local working directory contents; and a special case is described where local state exists but the destination dataset does not, causing local wipe. ([dltHub][5])

This is one of the key reasons dlt pipelines feel “production-y” even when run from a script.

## 2.4 Pipeline working directory: reproducibility and “where the truth lives”

Each pipeline stores extracted files, load packages, inferred schemas, execution traces, and pipeline state under a working folder (default: `~/.dlt/pipelines/<pipeline_name>`). ([dltHub][2])

Important implication:

* Same `pipeline_name` ⇒ same working directory and shared state across scripts unless you isolate it. ([dltHub][2])
* Use `pipelines_dir` to create separate “environments” (dev/stage/prod) for the *same* logical pipeline name. ([dltHub][2])
* You can attach to an existing working folder using `dlt.attach`. ([dltHub][2])
* `dev_mode=True` resets state and adds a datetime suffix to dataset names for experimentation. ([dltHub][2])

## 2.5 Refresh/reset semantics (pipeline-level “start over” controls)

There are two layers:

* The **docs** describe refresh as resetting state and dropping/truncating tables for selected sources/resources; and that destination modifications only happen if extract+normalize succeed (otherwise modifications are discarded). ([dltHub][2])
* The **API reference** lists `refresh` modes such as `drop_sources`, `drop_resources`, `drop_data`. ([dltHub][5])

This becomes crucial once you’re doing incremental/stateful pipelines.

## 2.6 Active pipeline context: why `pipeline.activate()` exists

The active pipeline concept matters because it:

* provides **state** to sources/resources evaluated outside `pipeline.run` and `pipeline.extract`
* adds the active pipeline name as an outer section for config/secret lookup (example in docs: `chess_pipeline.destination.bigquery.credentials` checked before `destination.bigquery.credentials`)
* provides destination capabilities context (naming convention, max identifier length, etc.)
* only one pipeline is active at a time; creating/attaching a pipeline and calling `run/load/extract` auto-activates it ([dltHub][5])

If you’ve ever tried to iterate a resource manually “outside” of a pipeline and wondered why config/state isn’t there—this is the reason.

## 2.7 Introspection surfaces (core ones)

From the API reference, pipeline exposes:

* `has_data`, `has_pending_data` to gate operations
* `state` dictionary
* `last_trace` to inspect the most recent run trace
* `list_extracted_load_packages`, `list_normalized_load_packages` for job/package lifecycle ([dltHub][5])

---

# 3) Source in depth

## 3.1 What a source “is”

A source is a `@dlt.source` function returning resources; typically one module per source system (auth/pagination/customization live there). ([dltHub][3])

A source can optionally define schema hints (e.g., max table nesting). ([dltHub][3])

## 3.2 Selection and access patterns

The docs show the practical API:

* `source.resources` is a dict-like view over all resources
* `source.resources.selected` shows selected resources
* `source.with_resources("a","b")` loads only a subset (convenience method)
* resources are accessible as attributes (`source.companies`)
* each resource has a `.selected` flag you can toggle ([dltHub][3])

Under the hood, DltSource:

* is iterable (you can iterate over all items)
* provides `selected_resources`
* can `discover_schema(...)` for selected resources
* can `with_resources(...)` which returns a clone with only the specified resources selected ([dltHub][6])

## 3.3 Dynamic resource creation inside a source

You don’t have to predeclare every resource as a top-level function: the docs show you can create resources dynamically using `dlt.resource` as a function (e.g., loop endpoints and build resources). ([dltHub][3])

This is a foundational “metaprogramming” move that lets you build modular connectors.

## 3.4 `add_limit` and `parallelize` at the source level

DltSource supports:

* `add_limit(...)` to limit items for **all selected resources** in the source (excluding transformers), useful for sampling/testing; note that it counts yields/pages by default (not rows), and does not limit transformers. ([dltHub][6])
* `parallelize()` to mark all eligible resources in a source to run in parallel (skips unsupported resources). ([dltHub][6])

Also: `source.run` exists as a convenience that runs on the currently active pipeline (or creates a default one). ([dltHub][6])

## 3.5 Source cloning/renaming for config separation

The “Source” docs show a pattern where you clone a source with a new `name`/`section` to create multiple instances and force config/credentials lookup into a custom section. ([dltHub][3])

This is a core trick for multi-tenant pipelines or “same connector, different accounts”.

## 3.6 Source-level nesting controls (quick but important)

Both the API reference and docs show `max_table_nesting` as a hint controlling how deeply nested structures are broken into nested tables vs stored as JSON/structs. ([dltHub][6])

---

# 4) Resource in depth

## 4.1 Resource basics

A resource is an (optionally async) function yielding data; you create it via `@dlt.resource`. Common decorator args:

* `name` / `table_name` (table generated at destination)
* `write_disposition` (`append`, `replace`, `merge`) ([dltHub][4])

The object you get back is a `DltResource`, which:

* implements `Iterable`
* wraps a **data pipe** plus adjustable table schema/hints ([dltHub][7])

## 4.2 Resource ≠ just a function: it’s an object with a “pipe”

Key capabilities from the API reference:

* resources can be iterated (`__iter__`) and will yield items “in the same order as in Pipeline class” with a read-only state from the active pipeline. ([dltHub][7])
* resource has `state` property (resource-scoped state from active pipeline; raises if no pipeline context). ([dltHub][7])
* resource supports piping with `|` via `__or__` and `__ror__` to connect resources and transformer functions. ([dltHub][7])

## 4.3 Parameterized resources: binding and lazy evaluation

Resources can take args like normal Python. dlt provides:

* `resource(*args, **kwargs)` via `__call__` to return a bound resource without evaluating generators/iterators
* `.bind(...)` to bind in place
* `.args_bound` and `.explicit_args` to introspect binding state ([dltHub][7])

Also, the docs call out that you can mark some arguments as configuration/credentials so dlt injects them automatically. ([dltHub][4])

## 4.4 Standalone vs “nested inside source”

The resource docs show an important ergonomics split:

* When a resource is defined top-level and accepts config values, you often must *call* it to get the actual resource instance before running it. ([dltHub][4])

This comes up a lot when you build generic resources like filesystem readers, database table extractors, etc.

## 4.5 Routing to many destination tables (dynamic table names)

Two key patterns shown in the docs:

1. `table_name` as a function of the data item (dynamic dispatch): `@dlt.resource(table_name=lambda event: event["type"])` ([dltHub][4])
2. Marking an individual item with a table name using `dlt.mark.with_table_name(item, name)`. ([dltHub][4])

Once you dispatch dynamically, the API reference provides `resource.select_tables(...)` to select only certain tables and filter out others (supports both the marker and function-based dynamic names). ([dltHub][7])

## 4.6 `max_table_nesting` at the resource level

Resource docs give a concrete meaning:

* limit how deep dlt generates nested tables and flattens dicts
* typical settings: `0` means no nested tables + no dict flattening (everything nested becomes JSON); `1` means only root-level nested tables and deeper nested becomes JSON
* can also be set after instantiation: `resource.max_table_nesting = 0` ([dltHub][4])

## 4.7 Parallel extraction: two different knobs

You’ll see both:

### A) Decorator hint `parallelized=True`

Resource docs show that if you mark multiple sync generator resources as `parallelized=True`, dlt will iterate them in parallel threads during extraction. ([dltHub][4])

The performance docs add detail: `parallelized=True` wraps generators to yield callables that are executed in a thread pool. ([dltHub][8])

### B) `resource.parallelize()` method

The API reference includes a `parallelize()` helper that wraps a resource to execute each item in a threadpool; requires generator/generator function/transformer function. ([dltHub][7])

(These feel similar, but operationally you’ll choose based on whether you want it declarative in the decorator or applied at composition time.)

---

# 5) Resource transforms in depth

## 5.1 In-resource “item pipe” transforms (add_map / add_filter / add_yield_map / add_metrics / add_limit)

### 5.1.1 `add_map`: per-item transform with explicit ordering

Docs define `add_map` as applying custom logic to each data item after extraction to transform/enrich/validate/clean/anonymize before loading. ([dltHub][9])

Signature highlights:

* `resource.add_map(item_map, insert_at=None)` ([dltHub][9])
* if the resource yields a list, dlt automatically enumerates the list and applies your function to each item. ([dltHub][9])
* `insert_at` lets you control ordering of pipe steps; docs give a mental model where yield is index 0, transforms occur at later indices, and incremental processing can occur “after” transforms—so `insert_at` exists to place your transform before incremental logic if needed. ([dltHub][9])

Best-practice note from docs: keep `add_map` stateless/lightweight; heavier per-record API calls should be moved into transformer resources or post-load steps. ([dltHub][9])

### 5.1.2 `add_yield_map`: one item → many items

The docs explain the conceptual difference: `add_map` returns exactly one item; `add_yield_map` yields zero or more items (useful for pivoting/flattening). ([dltHub][9])

API reference is explicit that `add_yield_map` receives single items (auto-enumerates lists) and may yield 0+ items. ([dltHub][7])

### 5.1.3 `add_filter`: keep/drop items

`add_filter` takes a predicate and keeps the item when it returns True; it also auto-enumerates lists yielded by the resource. ([dltHub][7])

### 5.1.4 `add_metrics`: measure without changing items

API reference: `add_metrics(metrics_f, insert_at=None)` where the metrics function receives `(items, meta, metrics_dict)` and updates metrics in-place; items pass through unchanged. ([dltHub][7])

### 5.1.5 `add_limit`: sampling/batching with real caveats

Two layers of details (docs + API reference):

* You can limit by **item count** and/or **time**, and optionally count rows instead of yields/pages (`count_rows=True`). ([dltHub][7])
* Default behavior counts “yields/batches/pages”, not rows; last page won’t be trimmed even in row-counting mode. ([dltHub][7])
* Transformers are **not** limited; they must fully process what they receive to avoid inconsistencies. ([dltHub][4])
* Async resources with a limit may occasionally produce one extra item; non-deterministic. ([dltHub][4])

In other words: `add_limit` is a dev/testing and batching tool, not a “precise row slicer”.

---

## 5.2 Transformer resources (`@dlt.transformer`) and piped composition (`|`)

### 5.2.1 What a transformer is

Resource docs describe the core usage: define a “parent” resource (e.g., list of users), then define a transformer that takes items from that resource (`data_from=users`) and yields derived/enriched items (e.g., user details). ([dltHub][4])

The key constraint called out: transformers must have at least one argument that receives the parent resource’s items. ([dltHub][4])

Transformers can:

* yield *or* return values
* decorate async functions and async generators ([dltHub][10])

### 5.2.2 Piping with `|`

This is not just “syntactic sugar”—it’s implemented as `__or__` / `__ror__` on `DltResource` to allow piping across resources and transform functions. ([dltHub][7])

The Pokemon example shows practical pipe composition:

* `pokemon_list | pokemon` (list → details)
* `pokemon_list | pokemon | species` (details → related endpoint) ([dltHub][11])

### 5.2.3 Parallel transformer execution: `parallelized=True` and `@dlt.defer`

The Pokemon example demonstrates two parallel patterns:

1. Mark a transformer with `parallelized=True` for parallel execution. ([dltHub][11])
2. Use `@dlt.defer` inside a transformer to execute a function in a thread pool while yielding results normally (dlt handles the parallelism). ([dltHub][11])

The performance docs also explain how parallelization works at a mechanism level: `parallelized=True` wraps resources to yield callables that are executed in a thread pool; non-generator transformers may be internally wrapped to yield once. ([dltHub][8])

### 5.2.4 Selection/deselection for “connector-only” resources

Pokemon example shows `selected=False` on the “list” resource because it’s just a driver for downstream transformers and shouldn’t land as a destination table. ([dltHub][11])

That pattern is extremely common (think “IDs table” that only exists to drive enrichment).

---

# 6) Golden templates (copy/paste)

## 6.1 Minimal pipeline: run a Python iterable

```python
import dlt

pipeline = dlt.pipeline(destination="duckdb", dataset_name="sequence")
info = pipeline.run([{"id": 1}, {"id": 2}, {"id": 3}], table_name="three")
print(info)
```

This mirrors the official docs pattern (iterable input + explicit table name). ([dltHub][2])

---

## 6.2 A “real” source with two resources + selection + a quick filter

```python
import dlt
import requests
from datetime import datetime, timedelta

@dlt.resource(write_disposition="append")
def companies(api_key=dlt.secrets.value):
    yield requests.get("https://api.example.com/companies", headers={"Authorization": api_key}).json()

@dlt.resource(write_disposition="append")
def deals(api_key=dlt.secrets.value):
    yield requests.get("https://api.example.com/deals", headers={"Authorization": api_key}).json()

@dlt.source
def example_source():
    return companies, deals

pipeline = dlt.pipeline(destination="duckdb", dataset_name="example")

src = example_source()
yesterday = (datetime.utcnow() - timedelta(days=1)).isoformat()
src.deals.add_filter(lambda d: d["created_at"] > yesterday)

# load only deals
pipeline.run(src.with_resources("deals"))
```

Selection patterns match the docs (resources dict, `.with_resources(...)`, `.selected` / `.add_filter(...)`). ([dltHub][3])

---

## 6.3 Enrichment via transformer (`data_from=`) instead of heavy `add_map`

```python
import dlt

@dlt.resource(write_disposition="replace")
def users():
    for u in _get_users():  # your API/pagination loop
        yield u

@dlt.transformer(data_from=users)
def user_details(user_item):
    yield _get_details(user_item["user_id"])  # call related endpoint(s)

pipeline = dlt.pipeline(destination="duckdb", dataset_name="app")
pipeline.run([users(), user_details])
```

This is the canonical “list endpoint → details endpoint” transformer pattern. ([dltHub][4])

---

## 6.4 Explicit transform ordering with `insert_at`

```python
import dlt
import hashlib

@dlt.resource
def users():
    yield {"id": 1, "email": "a@example.com"}

def mask_email(r):
    r["email"] = hashlib.sha256(r["email"].encode()).hexdigest()
    return r

def add_flag(r):
    r["masked"] = True
    return r

x = users().add_map(add_flag).add_map(mask_email, insert_at=1)
pipeline = dlt.pipeline(destination="duckdb", dataset_name="app")
pipeline.run(x, table_name="users")
```

`insert_at` is explicitly documented as the control for pipe order (especially relative to incremental). ([dltHub][9])

---

## 6.5 Parallel enrichment with `|` + `@dlt.defer`

This is structurally the same as the official Pokemon example (adapted naming):

```python
import dlt
from dlt.sources.helpers import requests

@dlt.source(max_table_nesting=2)
def src(base_url: str):
    @dlt.resource(write_disposition="replace", selected=False)
    def list_items():
        yield requests.get(base_url + "/items").json()["results"]

    @dlt.transformer
    def details(items):
        @dlt.defer
        def _fetch_one(i):
            return requests.get(i["url"]).json()

        for i in items:
            yield _fetch_one(i)

    return (list_items | details)
```

This mirrors how the docs connect transformers with `|` and use `@dlt.defer` for thread-pool parallelism. ([dltHub][11])

---

# 7) Practical watchouts you want “in your bones”

* **Pipeline name collisions are real**: same `pipeline_name` ⇒ shared working directory (`~/.dlt/pipelines/<pipeline_name>`) and shared state across scripts unless you isolate via `pipelines_dir`. ([dltHub][2])
* **Transform ordering matters**: if you care about running a transform before incremental filtering/processing, use `insert_at`. ([dltHub][9])
* **Don’t do per-record network calls in `add_map`** unless you understand the performance profile; docs explicitly recommend using transformer resources or post-load steps for heavier operations. ([dltHub][9])
* **`add_limit` is not a precise row slice** and does not limit transformers; async resources may yield an extra item occasionally. ([dltHub][4])
* **State/config access requires pipeline context**: resource/source `.state` depends on an active pipeline context (or it raises). ([dltHub][5])

---

If you want the next deep dive to continue “foundations-first,” the natural continuation is:

**(A) schema & hints mechanics (columns/primary_key/merge_key/nested_hints/max_table_nesting), then (B) state & incrementals**, because both are configured primarily at the **source/resource** layer and strongly affect how transforms behave.

[1]: https://dlthub.com/docs/intro "Introduction | dlt Docs"
[2]: https://dlthub.com/docs/general-usage/pipeline "Pipeline | dlt Docs"
[3]: https://dlthub.com/docs/general-usage/source "Source | dlt Docs"
[4]: https://dlthub.com/docs/general-usage/resource "Resource | dlt Docs"
[5]: https://dlthub.com/docs/api_reference/dlt/pipeline "dlt.pipeline.pipeline | dlt Docs"
[6]: https://dlthub.com/docs/api_reference/dlt/extract/source "dlt.extract.source | dlt Docs"
[7]: https://dlthub.com/docs/api_reference/dlt/extract/resource "dlt.extract.resource | dlt Docs"
[8]: https://dlthub.com/docs/reference/performance "Optimizing dlt | dlt Docs"
[9]: https://dlthub.com/docs/dlt-ecosystem/transformations/add-map "Transform data with `add_map` | dlt Docs"
[10]: https://dlthub.com/docs/general-usage/resource?utm_source=chatgpt.com "Resource | dlt Docs"
[11]: https://dlthub.com/docs/examples/transformers "Pokemon details in parallel using transformers | dlt Docs"

Below is the next “foundations-first” deep dive in two parts:

* **(A) Schema & hints mechanics**: `columns`, `primary_key`, `merge_key`, `nested_hints`, `max_table_nesting` (+ a few closely-coupled knobs like `root_key`, identifier normalization, and apply_hints).
* **(B) State & incrementals**: the pipeline/resource state dictionary, `dlt.sources.incremental(...)` (cursor-based incremental), plus the key parameters that make incremental *correct*.

---

# (A) Schema & hints mechanics

## A1) What dlt means by “schema”

dlt’s **data normalizer** turns Python dict/list structures into a **relational structure** (tables + columns) and records those definitions into a `Schema` object. ([dltHub][1])

Two critical implications:

1. **Schema comes from data**, but you can **steer** it with hints (table hints + column hints). ([dltHub][2])
2. Many hints are applied *after* identifiers are normalized (snake_case by default, destination-dependent). ([dltHub][3])

---

## A2) Identifier normalization and “which names do I use in hints?”

This trips people up more than anything.

* In **transformers / mapping / filtering**, you see **original source keys** (pre-normalization). ([dltHub][3])
* For `primary_key` and incremental cursor paths, you should use **source identifiers**, because incremental inspects the raw items. ([dltHub][3])
* For other hints (`columns`, `merge_key`), dlt can normalize hints alongside data, so you can often use either form—just be consistent. ([dltHub][3])
* The `Schema` object you inspect (`pipeline.default_schema`, `source.discover_schema`) contains **destination-normalized identifiers**. ([dltHub][3])

Practical rule of thumb:

* **Anything that needs to “look into the item” (cursor paths, PK used for dedupe during filtering)** → use source field names.
* **Anything that’s purely “table contract metadata” (types, partitioning, clustering, merge keys)** → you can use either, but I recommend you standardize on destination-normalized names when you start writing schema files/rules.

---

## A3) Column schemas with `columns=...`

You have three main ways to control columns:

### 1) Inline column dict (most common)

The `@dlt.resource(..., columns=...)` argument accepts a typed dict of column schemas, where you can set:

* `data_type`
* `precision`, `scale`
* `timezone`
* `nullable`
  …and more. ([dltHub][2])

Example (force a JSON column so it does **not** become nested tables):

```python
import dlt

@dlt.resource(name="user", columns={"tags": {"data_type": "json"}})
def users():
    yield {"id": 1, "tags": [{"k": "a"}, {"k": "b"}]}
```

This is explicitly documented as the way to keep a list column as JSON/struct instead of normalizing to a separate nested table. ([dltHub][2])

### 2) Pydantic model as schema (nice for validation + type inference)

You can pass a Pydantic model to `columns=...` and dlt infers column types from Pydantic fields. It also notes:

* `Optional[...]` → nullable
* `Union[...]` → coerces to the first non-None type
* `list/dict/nested model` → defaults to `json` (so it stays structured, not nested tables) ([dltHub][2])

There’s also a documented `dlt_config` option (`skip_nested_types`) to alter how nested types are handled. ([dltHub][2])

### 3) Schema-level rules (power-user)

If you want global behavior (“every `*_timestamp` is a timestamp”, “every `id` is a PK”), schema settings include:

* **Column hint rules** (regex-able) ([dltHub][1])
* **Preferred data types** rules ([dltHub][1])

These matter once you’re building connectors meant to run against many similarly-shaped endpoints.

---

## A4) `primary_key` vs `merge_key`: what they *do* in dlt

At the resource decorator level, dlt lets you set:

* `primary_key`
* `merge_key`
  (and notes compound keys are allowed). ([dltHub][2])

In schema terms, `primary_key` and `merge_key` are just column hints (metadata). ([dltHub][1])
But operationally they control two big behaviors:

### 1) Merge loading requires a key

To do merge loading you set `write_disposition="merge"` and provide a `primary_key` or `merge_key`. ([dltHub][4])

Pipeline-level `write_disposition="merge"` (or resource-level) is defined as “deduplicate + merge based on PK / merge key hints”, and can be configured with a strategy dict (e.g. SCD2). ([dltHub][5])

### 2) Incremental dedupe on overlapping cursor values uses `primary_key`

When using `dlt.sources.incremental`, if the resource has a `primary_key`, dlt uses it to deduplicate overlapping items with the same cursor value. ([dltHub][6])

### Bonus: tie-breakers and deletes are column hints too

When you’re in merge mode, column hints can define:

* `dedup_sort` (choose “latest” record in duplicates) ([dltHub][4])
* `hard_delete` (delete semantics) ([dltHub][4])

These are “schema & hints” topics because they’re expressed as column hints inside `columns={...}`.

---

## A5) Nested tables, nested references, and `nested_hints`

dlt creates **nested tables** when it finds **lists of objects** in your data. ([dltHub][2])
Those nested tables carry special reference columns (row key, parent key, list index) like `_dlt_id`, `_dlt_parent_id`, `_dlt_list_idx`. ([dltHub][7])

### `nested_hints` lets you control schema *for nested tables*

The resource docs show:

* `nested_hints={ "purchases": dlt.mark.make_nested_hints(...) }` to apply column types + schema contract to the nested table created from `purchases`. ([dltHub][2])
* deeper nesting uses a tuple path: `("purchases", "coupons")` → affects `customers__purchases__coupons` ([dltHub][2])
* you must specify **all parent path elements**, even if empty; missing path elements are not auto-added. ([dltHub][2])

### What you can (and shouldn’t) do with nested_hints

dlt calls out some caveats:

* `nested_hints` is mainly for **column hints + schema contracts** on nested tables (same idea as root tables). ([dltHub][2])
* Setting `write_disposition` on nested tables “works” but can lead to weird outcomes (e.g., nested table `replace` while root is `append`). ([dltHub][2])
* Setting `primary_key` / `merge_key` on a nested table effectively turns it into a “regular” table with its own load semantics and enables custom relationships using natural keys. ([dltHub][8])

The schema docs show a full worked example of converting `customers__purchases` into a top-level-like table by pushing down `customer_id`, declaring a compound PK, adding references, and using merge disposition. ([dltHub][1])

---

## A6) `max_table_nesting`: controlling how much normalization happens

This is your main “prevent schema explosion” knob.

Source-level docs explicitly define:

* `max_table_nesting=0`: no nested tables; no dict flattening; nested data becomes JSON ([dltHub][9])
* `max_table_nesting=1`: only one level of nested tables from roots; deeper stays JSON ([dltHub][9])

You can set it on:

* `@dlt.source(max_table_nesting=...)` ([dltHub][9])
* `@dlt.resource(max_table_nesting=...)` or `resource.max_table_nesting = ...` ([dltHub][9])

Important gotcha: source-level `max_table_nesting` doesn’t automatically apply if you directly pluck a resource instance; the docs recommend `source.with_resources(...)` or setting it on the resource. ([dltHub][9])

### Column-level override: “don’t normalize this one”

If one column should remain nested JSON no matter what, set that column’s `data_type` to `json` via `apply_hints` or `@dlt.resource(columns=...)`. ([dltHub][10])

---

## A7) Root key propagation (`root_key`) and merge with nested tables

When you use merge on data with nested tables, dlt needs a nested-to-root reference (“root key”) that skips intermediate parents. Merge loading docs say merge requires propagating the root table row key into nested tables, and that it is automatically propagated for tables with merge disposition (enabled there because it costs storage). ([dltHub][4])

The source API also exposes a `root_key` convenience property described as propagating `_dlt_id` from the root table to nested tables. ([dltHub][9])

Operational consequence: enabling root key on an **existing** table set requires dropping/recreating those tables because `_dlt_root_id` can’t be added to tables that already have data. ([dltHub][4])

---

## A8) `apply_hints`: the “I need to set hints programmatically” tool

The schema docs say: when resources are generated dynamically or you need to apply hints across many collections/tables, `apply_hints` is the tool. ([dltHub][1])

Two patterns you’ll use constantly:

### 1) Apply the same hint across many resources

```python
# e.g., mark a column as json across many collections/resources
for r in source_data.resources.values():
    r.apply_hints(columns={"column_name": {"data_type": "json"}})
```

Documented as a canonical use. ([dltHub][1])

### 2) Rename tables to avoid collisions / add prefixes

The SQL database verified-source docs show iterating resources and calling `apply_hints(table_name=...)` to prefix table names. ([dltHub][11])

---

# (B) State & incrementals

## B1) Pipeline state: what it is and where it lives

dlt state is a Python dictionary “alongside your data” that you read/write inside resources, and it persists across runs. ([dltHub][12])

Key properties:

* Must be **JSON-serializable**, though dlt also supports dumping/restoring some common types (e.g. `DateTime`, `Decimal`, `bytes`, `UUID`). ([dltHub][12])
* State is stored locally in the pipeline working dir and can be synced/restored from the destination. ([dltHub][12])
* Remote state is stored in `_dlt_pipeline_state` in the destination and can be synced via `dlt pipeline sync`. ([dltHub][12])
* You can disable restore-from-destination if your working dir is persistent (`restore_from_destination=false`). ([dltHub][12])

## B2) Resource-scoped vs source-scoped state (and the “don’t do this too much” warning)

* `dlt.current.resource_state()` is **resource-scoped** (private to that resource). ([dltHub][12])
* There’s also `dlt.current.source_state()` which is shared across resources for a source—but the API reference explicitly says “please avoid using source scoped state” and prefer resource state. ([dltHub][13])

Also: if you call `resource_state()` from async/deferred/threaded contexts, you may need to pass the resource name explicitly (dlt notes rare cases where it can’t resolve it due to threading). ([dltHub][13])

---

## B3) Cursor-based incremental with `dlt.sources.incremental(...)`

Cursor-based incremental is the standard: you pass a “last seen” timestamp/ID to the upstream system, fetch only new/updated items, and store the new cursor for the next run. dlt explicitly says it handles cursor aggregation (max/min), dedupe, and state management once you’ve chosen the cursor field and how to apply it to requests. ([dltHub][14])

### The Incremental object (what it *actually* is)

API reference: `Incremental` “adds incremental extraction for a resource by storing a cursor value in persistent state.” ([dltHub][6])

Usage shapes:

* As a default argument / annotated argument in a resource function ([dltHub][14])
* As a transform step you can attach to a resource (`resource.add_step(...)`) ([dltHub][6])
* Or you can read an incremental cursor from existing state (`from_existing_state`) ([dltHub][6])

### Most important Incremental parameters (and what they imply)

From the API reference (these drive correctness):

* `cursor_path`: field name or JSON path to the cursor in the raw item; uses source names (pre-normalization). ([dltHub][6])
* `initial_value`: used when no state exists (first run). ([dltHub][6])
* `last_value_func`: how dlt picks the new cursor (`max` by default, but can be custom). ([dltHub][6])
* `primary_key`: optional override PK for dedupe; can be compound (tuple); empty tuple disables unique checks. ([dltHub][6])
* `end_value`: backfill range; **stateless** behavior where `initial_value` supersedes stored state. ([dltHub][6])
* `row_order`: `asc`/`desc` lets dlt stop early by closing the generator when out-of-range. ([dltHub][6])
* `allow_external_schedulers`: let Airflow provide the window; in that mode state is not used. ([dltHub][6])
* `on_cursor_value_missing`: what happens if the cursor field is missing/None (raise/include/exclude). ([dltHub][6])
* `lag`: moving attribution window (seconds for datetime cursors). ([dltHub][6])
* `range_start`: `open` vs `closed`; open excludes items equal to last_value and disables dedupe (an optimization when you know no overlap). ([dltHub][6])

---

## B4) Advanced incremental patterns you’ll actually use

### Pattern 1: “Stateful” entities → incremental extract + merge load

Use when rows can change (profiles, tickets, accounts):

* incremental cursor on `updated_at`
* `write_disposition="merge"`
* set `primary_key` (and optionally `dedup_sort` if you can receive duplicates / out-of-order rows) ([dltHub][4])

```python
import dlt

@dlt.resource(primary_key="id", write_disposition="merge")
def users(updated_at=dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")):
    yield from api.fetch_users(updated_since=updated_at.start_value)
```

### Pattern 2: Backfill a range with `end_value`, then “turn on” stateful incremental

Docs show you can specify `end_value` for backfill and (with the right `row_order`) dlt can stop fetching once out of range. ([dltHub][14])

Key mental model: backfill mode is a **stateless range loader**. ([dltHub][6])

### Pattern 3: Split massive incrementals into chunks

Cursor-based incremental docs show combining incremental with `add_limit` to produce sequential “chunks,” and explicitly warns you need deterministic order or you’ll miss or duplicate items. ([dltHub][14])

If you know you’ll never see overlap between chunks, set `range_start="open"` to disable dedupe cost. ([dltHub][14])

### Pattern 4: “My API doesn’t return the cursor field”

FAQ suggests two approaches:

* use `dlt.sources.incremental` with a custom cursor (see incremental cursor docs)
* or manage custom pagination checkpoints directly in `resource_state()`—with a caution that overusing state (e.g., per-user substreams) gets unwieldy. ([dltHub][10])

---

## B5) Custom incremental via state (what `dlt.sources.incremental` is doing under the hood)

The advanced-state page gives the simplest mental model:

* read a checkpoint with `dlt.current.resource_state().setdefault(...)`
* yield data
* update the checkpoint
  …and the key property is that state is committed with the data load. ([dltHub][15])

```python
import dlt

@dlt.resource
def tweets():
    last_val = dlt.current.resource_state().setdefault("last_updated", None)
    data = api.fetch_tweets(start_from=last_val)
    yield data
    dlt.current.resource_state()["last_updated"] = data["last_timestamp"]
```

This is essentially the reference implementation of “custom incremental.” ([dltHub][15])

---

## B6) Full refresh and resets

Incremental loading docs note you can force a full refresh:

* for merge pipelines: data in destination is deleted and reloaded
* for `dlt.sources.incremental`: the incremental state resets to the initial value ([dltHub][16])

---

# Suggested next deep-dive step

If you want to keep the “foundations-first” sequence, the next best chapter is:

**Destinations + write dispositions + merge strategies (delete-insert / upsert / scd2) + staging datasets**, because schema/keys/incremental only become “real” when you understand how the loader uses them (including root_key propagation, staging schemas, and per-destination interpretation of hints). ([dltHub][4])

[1]: https://dlthub.com/docs/general-usage/schema "Schema | dlt Docs"
[2]: https://dlthub.com/docs/general-usage/resource "Resource | dlt Docs"
[3]: https://dlthub.com/docs/general-usage/naming-convention "Naming convention | dlt Docs"
[4]: https://dlthub.com/docs/general-usage/merge-loading "Merge loading | dlt Docs"
[5]: https://dlthub.com/docs/api_reference/dlt/pipeline "dlt.pipeline.pipeline | dlt Docs"
[6]: https://dlthub.com/docs/api_reference/dlt/extract/incremental/__init__ "dlt.extract.incremental | dlt Docs"
[7]: https://dlthub.com/docs/general-usage/destination-tables "Destination tables & lineage | dlt Docs"
[8]: https://dlthub.com/docs/general-usage/resource?utm_source=chatgpt.com "Resource | dlt Docs"
[9]: https://dlthub.com/docs/general-usage/source "Source | dlt Docs"
[10]: https://dlthub.com/docs/reference/frequently-asked-questions "Frequently asked questions | dlt Docs"
[11]: https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database/configuration "Configuration | dlt Docs"
[12]: https://dlthub.com/docs/general-usage/state "State | dlt Docs"
[13]: https://dlthub.com/docs/api_reference/dlt/extract/state "dlt.extract.state | dlt Docs"
[14]: https://dlthub.com/docs/general-usage/incremental/cursor "Cursor-based incremental loading | dlt Docs"
[15]: https://dlthub.com/docs/general-usage/incremental/advanced-state "Advanced state management for incremental loading | dlt Docs"
[16]: https://dlthub.com/docs/general-usage/incremental-loading "Incremental loading | dlt Docs"

# Destinations + write dispositions + merge strategies + staging (dlt)

This is the chapter where dlt’s “it just works” becomes concrete: **destinations** define what loading *can* do, **write dispositions** define what loading *should* do, and **staging** is how dlt makes the hard modes (merge + transactional replace) correct and usually atomic.

The references below reflect dlt Docs **1.20.0 (latest)** at time of browsing. ([dltHub][1])

---

## 1) Destination model: what a destination actually provides

At runtime, a destination is not just “a connection string.” It’s a bundle of **capabilities + loaders + schema/migration logic** that dlt uses to decide:

* **Which loader file format** to produce (insert-values vs Parquet/CSV/JSONL, local vs staged). ([dltHub][2])
* **Whether transactions exist** and how atomicity can be achieved. ([dltHub][2])
* **How table/column hints map to DDL** (indexes, clustering, partitioning, etc.). ([dltHub][2])
* **Whether “merge” can be done as delete-insert vs upsert vs scd2**, and which are supported per backend. ([dltHub][1])

The destination docs explicitly frame destinations as “a location where dlt maintains schema and loads data,” and recommend declaring destination at pipeline creation so extract/normalize can produce compatible load packages and schemas. ([dltHub][3])

---

## 2) Two different “staging” concepts (don’t conflate them)

dlt uses “staging” to mean **two separate mechanisms**: ([dltHub][4])

### A) Staging dataset (database schema / dataset)

Used for **merge** and **transactional replace**. It lives **alongside** the final dataset and usually holds tables named like your final tables. ([dltHub][4])

### B) Staging storage (remote bucket / filesystem stage)

Used to copy **loader files** close to the warehouse and then run **COPY-from-bucket** jobs. This is destination chaining: `staging='filesystem'` + `destination='redshift'` (etc.). ([dltHub][4])

They solve different problems:

* **Staging dataset**: correctness + atomicity for table mutations (merge / replace).
* **Staging storage**: reliability/perf of file loading to warehouses (avoid pushing large local files directly).

---

## 3) Write dispositions: the contract for how tables change

dlt defines 3 dispositions for writing to tables:

* **append**: add rows
* **replace**: full refresh (rebuild table contents)
* **merge**: update existing rows (and optionally delete/retire) based on keys ([dltHub][5])

Under the hood, the critical thing is: dispositions determine **whether the destination dataset must be modified in a potentially destructive way**, and therefore whether **staging dataset + table-chain atomicity** is required. ([dltHub][4])

---

# 4) Replace (full loading) deep dive: strategies + tradeoffs

`write_disposition="replace"` is “full load,” but the *mechanics* depend on a **replace strategy**. dlt has three. ([dltHub][6])

## 4.1 Strategy 1 — `truncate-and-insert` (default, fastest, risky)

* Truncates destination tables at the **beginning** of the load.
* Inserts new data “consecutively” and **not within one transaction**.
* Downsides: **downtime** (tables empty during load) and **partial refresh risk** if failure happens mid-run. ([dltHub][6])

This is why dlt explicitly warns that replace with default strategy can irreversibly modify data (truncate happens before load completes). ([dltHub][7])

## 4.2 Strategy 2 — `insert-from-staging` (slowest, consistent, “zero downtime”)

* Loads all new data into **staging dataset tables**.
* Then truncates + inserts into final tables **in a single transaction**.
* Keeps nested + root tables consistent. ([dltHub][6])

Use this when you need correctness and atomic cutover more than speed.

## 4.3 Strategy 3 — `staging-optimized` (fast *if your destination supports it*, but drops tables)

Same “staging dataset” approach, but with destination-specific optimizations (often faster/cheaper) **at the cost of dropping/recreating destination tables**, which can drop downstream views/constraints. ([dltHub][6])

Destination behavior examples:

* **Postgres**: drop destination tables and replace with staging tables (fast, minimal data movement). ([dltHub][6])
* **BigQuery**: drop & recreate using BigQuery table cloning from staging tables. ([dltHub][6])
* **Snowflake**: drop & recreate using Snowflake clone command from staging. ([dltHub][6])

## 4.4 How to configure replace strategy

You set it in `config.toml`:

```toml
[destination]
replace_strategy = "staging-optimized"  # or "insert-from-staging" or "truncate-and-insert"
```

Default is `truncate-and-insert`. ([dltHub][6])

---

# 5) Merge deep dive: strategies, keys, and what staging is doing

To do merge loading you set `write_disposition="merge"` and provide a **`primary_key` or `merge_key`**. dlt then uses one of three merge strategies. ([dltHub][1])

## 5.1 Merge Strategy A — `delete-insert` (default)

This is the “universal backend” strategy: stage the new data, figure out what should change, then delete + insert in an atomic step.

dlt describes it as:

* Load new data into **staging dataset**
* **Deduplicate** staging data if `primary_key` is provided
* **Delete** from destination using `merge_key` and/or `primary_key`
* **Insert** the new records
* Do it in **one atomic transaction for a root and all nested tables** ([dltHub][1])

### Keys: `primary_key` vs `merge_key` in delete-insert

* `primary_key`: de-dupes *entities* (e.g. one row per `user_id`). ([dltHub][1])
* `merge_key`: de-dupes *batches/partitions* (e.g. one row per “day”). This enables “re-load the same day’s batch idempotently.” ([dltHub][1])

### Merge without keys (“staging append”)

If you use `merge` but **don’t specify keys**, dlt can fall back to append semantics (but still can insert from staging in a transaction for many destinations). dlt also calls this “staging append” in production guidance. ([dltHub][1])

---

## 5.2 Control and cost: how dlt deduplicates staging data

### “Which duplicate wins?” → `dedup_sort`

By default, `primary_key` dedupe is arbitrary. You can mark a column with `dedup_sort: "desc"` or `"asc"` so dlt sorts duplicates before keeping one (e.g., keep the “latest” record). ([dltHub][1])

```python
@dlt.resource(
    primary_key="id",
    write_disposition="merge",
    columns={"metadata_modified": {"dedup_sort": "desc"}}
)
def records():
    yield from ...
```

### “My staging input is already clean” → disable dedupe

You can declare the strategy as deduplicated to save database work/cost:

```python
@dlt.resource(
  primary_key="id",
  write_disposition={"disposition": "merge", "strategy": "delete-insert", "deduplicated": True},
)
def records():
    yield from ...
```

dlt explicitly calls this out. ([dltHub][1])

---

## 5.3 Deletes: `hard_delete` hint

dlt supports a “delete marker” column hint:

* For boolean columns: only `True` triggers delete.
* For other types: any non-`None` triggers delete.
* Deletes propagate to nested tables and rely on root key linkage. ([dltHub][1])

This matters because it turns merge into “CDC-like” semantics without needing a full CDC log stream.

---

# 6) Root key propagation: why merge + nested tables require extra columns

If a resource produces nested tables, merge requires that the root table’s `_dlt_id` (“row_key”) be propagated into nested tables as `_dlt_root_id` so deletes/merges can cascade correctly across the “table chain.” ([dltHub][1])

Important operational rule:

* If you enable root-key propagation on a pipeline that already loaded data, you must **drop and recreate** the affected tables because `_dlt_root_id` can’t be added to non-empty tables. ([dltHub][1])

dlt provides a worked example using `dlt pipeline ... drop <resource>` and then running `replace` once before `merge` to recreate tables with the required column. ([dltHub][1])

---

# 7) Merge Strategy B — SCD2 (history tracking with validity windows)

SCD2 in dlt is configured as:

```python
@dlt.resource(write_disposition={"disposition": "merge", "strategy": "scd2"})
def dim_customer():
    ...
```

dlt creates and maintains `_dlt_valid_from` and `_dlt_valid_to` validity windows, inserting new versions when records change and retiring old ones by setting `_dlt_valid_to`. ([dltHub][1])

### Key nuance: `_dlt_id` uniqueness changes under SCD2

dlt notes that `_dlt_id` is **not unique** in SCD2 tables due to delete/reinsert patterns, but `(_dlt_id, _dlt_valid_from)` is unique; nested tables remain unaffected. ([dltHub][1])

### Incremental SCD2: using `merge_key` to define “what counts as deleted”

dlt explains that `merge_key` in SCD2 can be used to control retirement of absent rows (the “tombstone semantics” for incremental extracts): ([dltHub][1])

* Set natural key as `merge_key` to **avoid retiring absent records** (treat incremental extracts as “updates only”). ([dltHub][1])
* Use a “partition” column as `merge_key` to retire absent rows only within partitions present in the extract. ([dltHub][1])

Also: if you previously set `merge_key` and want to resume retiring absent records, you must explicitly unset it (e.g., `columns={"customer_key": {"merge_key": False}}`)—omitting it is not enough. ([dltHub][1])

---

# 8) Merge Strategy C — `upsert` (native MERGE/UPDATE where available)

`upsert` is the “true upsert” strategy (update if exists, insert if not). dlt’s docs are explicit:

* Requires a **primary_key**
* Expects primary_key to be **unique** (dlt does not deduplicate staging data)
* Does **not** support `merge_key`
* Uses `MERGE` or `UPDATE` operations ([dltHub][1])

dlt also enumerates which destinations support it (as of 1.20.0): `athena`, `bigquery`, `databricks`, `mssql`, `postgres`, `snowflake`, plus `filesystem` with delta/iceberg formats (with limitations). ([dltHub][1])

**When to prefer upsert over delete-insert**

* You have a natural PK, updates are sparse, and backend supports efficient MERGE/UPDATE.
* You do not need batch-partition semantics (`merge_key`) or staging dedupe.

**When to prefer delete-insert**

* Your update model is “batches/partitions” (daily snapshots).
* You can’t rely on backend MERGE performance/features.
* You want dlt-managed dedupe via `primary_key` + `dedup_sort`. ([dltHub][1])

---

# 9) Staging dataset mechanics: naming, truncation, cleanup, and concurrency

## 9.1 Default naming and lifecycle

dlt creates a staging dataset when required by merge/replace. Default name is `<dataset_name>_staging`. It creates and migrates tables there like the main dataset. It truncates staging tables **at the beginning** of load for tables participating in that load. ([dltHub][4])

By default, dlt **does not truncate staging tables at the end** (leftover data can help debugging), but you can set:

```toml
[load]
truncate_staging_dataset=true
```

([dltHub][4])

## 9.2 Custom naming patterns (and why you should care)

You can configure the staging dataset naming layout:

```toml
[destination.postgres]
staging_dataset_name_layout="staging_%s"
```

Or force a static name via destination factory:

```python
dest_ = dlt.destinations.postgres(staging_dataset_name_layout="_dlt_staging")
```

dlt warns that the staging dataset name must differ from the final dataset name; otherwise it raises `ValueError` to prevent catastrophic truncation. ([dltHub][4])

## 9.3 Concurrency pitfalls

If you run multiple pipelines in parallel that write to the same destination dataset and use staging, you should:

* use unique staging dataset names per pipeline (`staging_dataset_name_layout`), and
* ensure staging storage paths are unique per pipeline if you also use staging storage buckets. ([dltHub][8])

---

# 10) Table-chain atomicity: how dlt decides when to “apply” merge/replace

This is the key internal mechanism to understand:

* Load jobs write data to staging tables.
* dlt groups root + nested tables into a **table chain**.
* When the chain is fully loaded, dlt runs SQL jobs that move/merge data from staging → destination. ([dltHub][2])

The “create new destination” guide notes the merge SQL requires:

* SELECT, INSERT, DELETE/TRUNCATE
* WINDOW functions for merge ([dltHub][2])

This is why dlt can implement merge consistently across many SQL destinations (even if they don’t support native MERGE).

---

# 11) Staging storage (bucket staging) deep dive: destination chaining + file truncation

Staging storage is configured by passing `staging='filesystem'` to your pipeline. The filesystem destination uploads files to remote storage, then the final destination runs COPY-from-remote jobs. ([dltHub][4])

### Current support

* Only **filesystem** can act as the staging destination.
* Destinations that can copy remote files include: Azure Synapse, Athena, BigQuery, Dremio, Redshift, Snowflake. ([dltHub][4])

### Loader format selection

dlt auto-selects an appropriate loader file format when staging is enabled; you can still explicitly request (e.g., Parquet). ([dltHub][4])

### File retention behavior

dlt does not delete loaded files from staging storage, but it **truncates previously loaded files** by default; you can disable truncation via destination config:

```toml
[destination.redshift]
truncate_tables_on_staging_destination_before_load=false
```

([dltHub][4])

---

# 12) Per-destination interpretation of hints and capabilities (what changes across backends)

## 12.1 Hints are portable; interpretation is destination-specific

dlt’s schema docs explicitly say each destination can interpret hints differently (example: `cluster` is Redshift distribution key, BigQuery cluster column; DuckDB/Postgres may ignore it). ([dltHub][9])

Concrete examples:

* **Redshift** supports `cluster` (DISTKEY) and `sort` (SORTKEY). ([dltHub][10])
* **DuckDB** can create unique indexes for `unique` hints but it’s disabled by default due to load slowdowns; also has timezone quirks for Parquet timestamps. ([dltHub][11])
* **BigQuery** has a “streaming insert” mode (append only) enabled via `additional_table_hints={"x-insert-api": "streaming"}`, and streaming inserts block updates/deletes for up to ~90 minutes. ([dltHub][12])

## 12.2 Capability tables exist per destination

Some destination pages include a capabilities table you can treat as the “truth” for supported merge/replace strategies.

Example: **Postgres** supports merge strategies `delete-insert`, `upsert`, `scd2` and replace strategies including `staging-optimized`. ([dltHub][13])

Example: **Filesystem destination** supports merge strategy `upsert` and replace strategies `truncate-and-insert` and `insert-from-staging`, and supports `delta` / `iceberg` table formats. ([dltHub][14])

## 12.3 Table formats: Delta/Iceberg come with special merge caveats

* **Iceberg**: upsert supported; warns about pyiceberg version constraints (chunking) and schema evolution limits under upsert with certain versions. ([dltHub][15])
* **Delta table format**: upsert supported but experimental for filesystem+delta; has limitations like no `hard_delete` support and no nested table deletions; offers knobs like `deltalake_streamed_exec`. ([dltHub][16])

These limitations matter a lot if you’re counting on `hard_delete` or nested-table correctness.

---

# 13) Operational guidance: how to pick a disposition + strategy

### Append

Use when:

* immutable event/fact data,
* you want the cheapest, simplest behavior,
* rollback is easiest via `_dlt_load_id` filtering. ([dltHub][7])

### Replace

Use when:

* the source is “a full snapshot every time.”
  Pick strategy:
* **truncate-and-insert** for speed and you can tolerate downtime/partial refresh risk. ([dltHub][6])
* **insert-from-staging** for consistent datasets / no downtime. ([dltHub][6])
* **staging-optimized** when destination supports it and you accept drop/recreate tradeoffs. ([dltHub][6])

### Merge (delete-insert)

Use when:

* updates arrive as “latest state” and you want one current record per entity, or
* you load in partitions/batches and want idempotency (via `merge_key`). ([dltHub][1])

### Merge (upsert)

Use when:

* backend supports efficient native upsert,
* you have strict PK uniqueness,
* you don’t need `merge_key` semantics. ([dltHub][1])

### Merge (scd2)

Use when:

* you want change history in dimension tables with validity windows. ([dltHub][1])

---

# 14) Practical “golden” configuration snippets

## 14.1 A safe production default for full refresh tables (transactional replace)

```toml
[destination]
replace_strategy = "insert-from-staging"

[load]
truncate_staging_dataset = false  # keep staging data for debugging; set true if you must
```

([dltHub][6])

## 14.2 Avoid staging dataset name collisions in parallel pipelines

```toml
[destination.postgres]
staging_dataset_name_layout="staging_%s"
```

Or per-pipeline destination factory:

```python
dest = dlt.destinations.postgres(staging_dataset_name_layout="staging_%s")
pipeline = dlt.pipeline(destination=dest, dataset_name="github_data")
```

([dltHub][4])

## 14.3 BigQuery: append + streaming inserts (but no updates/deletes)

```python
@dlt.resource(write_disposition="append")
def streamed():
    yield {"field1": 1, "field2": 2}

streamed.apply_hints(additional_table_hints={"x-insert-api": "streaming"})
```

Streaming insert constraints: append-only; update/delete restrictions for a period after insert. ([dltHub][12])

## 14.4 Warehouse loads via bucket staging

```python
pipeline = dlt.pipeline(
    pipeline_name="chess_pipeline",
    destination="redshift",
    staging="filesystem",
    dataset_name="player_data",
)
pipeline.run(chess_source(), loader_file_format="parquet")
```

([dltHub][4])

---

## Next deep-dive recommendation

The most valuable follow-on chapter after this is:

**Destination tables & metadata surfaces**: `_dlt_loads`, `_dlt_load_id`, `_dlt_id`, nested linkage columns, and “how to reason about correctness/rollback/lineage when loads fail,” because that’s how you operationalize append/merge/replace safely. ([dltHub][17])

[1]: https://dlthub.com/docs/general-usage/merge-loading "Merge loading | dlt Docs"
[2]: https://dlthub.com/docs/walkthroughs/create-new-destination "Create new destination | dlt Docs"
[3]: https://dlthub.com/docs/general-usage/destination "Destination | dlt Docs"
[4]: https://dlthub.com/docs/dlt-ecosystem/staging "Staging | dlt Docs"
[5]: https://dlthub.com/docs/general-usage/pipeline?utm_source=chatgpt.com "Pipeline | dlt Docs"
[6]: https://dlthub.com/docs/general-usage/full-loading "Full loading | dlt Docs"
[7]: https://dlthub.com/docs/running-in-production/running "Running | dlt Docs"
[8]: https://dlthub.com/docs/reference/performance "Optimizing dlt | dlt Docs"
[9]: https://dlthub.com/docs/general-usage/schema "Schema | dlt Docs"
[10]: https://dlthub.com/docs/dlt-ecosystem/destinations/redshift "Amazon Redshift | dlt Docs"
[11]: https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb "DuckDB | dlt Docs"
[12]: https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery "Google BigQuery | dlt Docs"
[13]: https://dlthub.com/docs/dlt-ecosystem/destinations/postgres "Postgres | dlt Docs"
[14]: https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem "Cloud storage and filesystem | dlt Docs"
[15]: https://dlthub.com/docs/dlt-ecosystem/destinations/iceberg "Iceberg | dlt Docs"
[16]: https://dlthub.com/docs/dlt-ecosystem/destinations/delta-iceberg "Delta | dlt Docs"
[17]: https://dlthub.com/docs/general-usage/destination-tables "Destination tables & lineage | dlt Docs"

# Destination tables & metadata surfaces in dlt

There are **two metadata layers** you should internalize:

1. **Row-level metadata columns** that dlt adds to your *data tables* (root + nested).
2. **Dataset-level internal tables** (prefixed `_dlt_`) that act like a **commit log + state log + schema log** for the pipeline. These are what let you reason about *completeness, rollback, and lineage*.

> In dlt Docs 1.20.0 these internal tables are *not shown* in `dlt pipeline show`, but exist in the destination schema and can be queried directly. ([dltHub][1])

---

## 1) Row-level metadata columns (what every destination table gets)

### 1.1 `_dlt_load_id`: “which load package wrote this row?”

Every row loaded by dlt includes a `_dlt_load_id` that points to the **load package** that produced it. dlt’s “destination tables” guide describes: each pipeline execution creates one or more load packages identified by a `load_id`, and that `load_id` is written into data tables as `_dlt_load_id`. ([dltHub][1])

Under the hood, `load_id` is generated as the **current unix timestamp converted to a string**; dlt requires that load IDs **increase over time** for a given schema+destination+dataset, and it uses the highest load id as “most up to date” when restoring state from the destination. ([dltHub][2])

**Operational meaning:** `_dlt_load_id` is the key you use to:

* filter out partial loads,
* delete/rollback a bad append load,
* group rows into “what happened together” during a run.

---

### 1.2 `_dlt_id`: stable row key for *linking* (and sometimes for dedupe)

dlt gives every table a `row_key` column (named `_dlt_id` by default) that uniquely identifies each row in that table. ([dltHub][3])

How `_dlt_id` is generated matters a lot: ([dltHub][3])

* **Root tables:** `_dlt_id` is a **random string**, *except* for **upsert** and **scd2** merge strategies, where it becomes a **deterministic hash** of `primary_key` (or a content hash if PK isn’t defined).
* **Nested tables:** `_dlt_id` is a **deterministic hash** of (`parent_key`, parent table name, `_dlt_list_idx`).

You can also **bring your own** `_dlt_id` (for root and nested) by including a `_dlt_id` field in your data; any type with an equality operator works. ([dltHub][3])

**Operational meaning:**

* In “plain append” pipelines, root `_dlt_id` is not stable across runs (random), so don’t treat it like a business key.
* In upsert/scd2, root `_dlt_id` is deterministic and behaves more like an entity identity derived from PK.

---

### 1.3 Nested linkage columns: `_dlt_parent_id` and `_dlt_list_idx`

When dlt normalizes lists of objects into nested tables (e.g., `users__pets`), it creates:

* `_dlt_parent_id`: the parent row key (`_dlt_id`) this nested row belongs to
* `_dlt_list_idx`: the 0-based position of the item in the original list ([dltHub][1])

This is the “nested reference” mechanism. dlt’s schema docs emphasize that `parent` + `row_key` + `parent_key` form a nested reference that is **extensively used** during loading, including **replace** and **merge** dispositions. ([dltHub][3])

**Practical join:**

```sql
-- root -> nested
select
  u.*,
  p.id as pet_id,
  p.name as pet_name,
  p.type as pet_type,
  p._dlt_list_idx
from mydata.users u
left join mydata.users__pets p
  on p._dlt_parent_id = u._dlt_id;
```

---

### 1.4 `_dlt_root_id`: required for merge across nested tables

For nested tables loaded with **merge**, dlt adds `_dlt_root_id` to reference the nested row directly back to the **root table**, skipping intermediate parents. ([dltHub][1])

dlt’s schema docs spell out why: merge requires an additional nested reference “nested → root,” created via a `root_key` hint (named `_dlt_root_id` by default). ([dltHub][3])

**Operational meaning:** If you plan to use merge on nested structures, `_dlt_root_id` is part of what makes “table-chain atomic merge” possible.

---

## 2) Internal tables (`_dlt_*`): commit log + state log + schema log

dlt automatically creates internal tables in the destination schema (prefixed `_dlt_`) to track pipeline runs, incremental checkpoints, and schema versions. ([dltHub][1])

### 2.1 `_dlt_loads`: the “commit log” for load packages

`_dlt_loads` is the authoritative record of which load packages are **complete** and what schema version they used.

The destination tables guide gives the schema and semantics: it records `load_id`, `schema_name`, `schema_version_hash`, `status`, and `inserted_at`. Only rows with `status = 0` are considered complete; other values represent incomplete/interrupted loads, and `status` can coordinate multi-step transformations. ([dltHub][1])

**Key invariant:**

> If a row exists in a data table with `_dlt_load_id = X`, but there is **no** `_dlt_loads` row for `load_id = X` with `status = 0`, then that data may be **partially loaded** and should be treated as “uncommitted.” ([dltHub][1])

Why this matters: many destinations can expose partially loaded data because they don’t support long-running/distributed transactions; dlt explicitly recommends filtering out rows whose load ids don’t exist in `_dlt_loads`. ([dltHub][1])

**Canonical “only committed data” view (per table):**

```sql
create view mydata.users__committed as
select u.*
from mydata.users u
join mydata._dlt_loads l
  on l.load_id = u._dlt_load_id
where l.status = 0;
```

You can apply the same pattern to nested tables.

---

### 2.2 `_dlt_pipeline_state`: incremental checkpoints + resumability

`_dlt_pipeline_state` stores the pipeline state snapshots that power incremental loading and resuming after failure. The docs describe columns including `version`, `engine_version`, `pipeline_name`, `state` (serialized dict), `created_at`, `version_hash`, plus `_dlt_load_id` (link to `_dlt_loads`) and `_dlt_id` (unique row id for the state table). ([dltHub][1])

The docs explicitly connect this table to “last-value incremental loading” and resuming from the correct checkpoint if a run fails/stops. ([dltHub][1])

**Operational meaning:**

* `_dlt_pipeline_state` is your forensic trail for “what cursor/state did dlt think it was at?”
* It is also why **load_id ordering** matters (state restore picks “highest load id” as most up to date). ([dltHub][2])

---

### 2.3 `_dlt_version`: schema version history

`_dlt_version` records schema versions whenever dlt evolves schema (new columns/tables, etc.). The destination tables guide lists columns like `version`, `engine_version`, `inserted_at`, `schema_name`, `version_hash`, and `schema` (full schema as JSON). ([dltHub][1])

**Operational meaning:**

* You can answer “what schema was used for load X?” via `schema_version_hash` in `_dlt_loads` and `version_hash` in `_dlt_version`. ([dltHub][1])
* This becomes critical when debugging type drift / variant columns / schema contract failures.

---

## 3) “What counts as complete?”: load packages, jobs, and failure semantics

dlt’s production “Running” guide provides the key behavioral guarantee:

* `load_info` (returned by `pipeline.run`) includes per-package job status, failures, and error messages. ([dltHub][4])
* If any job fails terminally, the load package is aborted, and **its load id is not added to `_dlt_loads`**; furthermore, “the dlt state, if present, will not be visible to dlt.” ([dltHub][4])

This is the precise reason `_dlt_loads` is the “commit table” and `_dlt_load_id` in your data tables is a “write attempt id.”

---

## 4) How to reason about correctness + rollback by write disposition

### 4.1 Append: simplest rollback story

If a load package fails, you can remove the partial/unprocessed data by deleting rows with that `_dlt_load_id`. dlt explicitly recommends this as the “append-only” mitigation. ([dltHub][4])

**Rollback snippet:**

```sql
-- delete uncommitted rows (example: users table)
delete from mydata.users
where _dlt_load_id not in (
  select load_id from mydata._dlt_loads where status = 0
);
```

(Prefer a “specific load_id” delete if you know which one was bad.)

---

### 4.2 Replace: the sharp edge is `truncate-and-insert`

dlt warns that **replace** with the default **truncate-and-insert** strategy truncates tables *before* loading and does not load within one transaction, so you can end up with partial refreshes or downtime. ([dltHub][5])

dlt says an incomplete replace load can be detected by checking whether `_dlt_loads` contains the relevant load id referenced by `_dlt_load_id` in replaced tables. ([dltHub][5])

**Operational guidance:**

* If you need “atomic replace,” use a staging-based replace strategy (insert-from-staging or staging-optimized) so the cutover happens only when all table-chain data has been fully loaded. ([dltHub][5])

---

### 4.3 Merge: staging dataset + table-chain atomic merge

For merge, dlt uses a **staging dataset** (default `<dataset_name>_staging`) and then loads staging → destination “in a single atomic transaction.” ([dltHub][1])

And critically: dlt states that merge “will happen only when all data for this table (and nested tables) got loaded.” ([dltHub][4])

**Rollback implication:**

* If a merge run fails before follow-up merge jobs apply, your destination tables may remain consistent, while staging contains partial data; a rerun typically reprocesses pending work (see next section). ([dltHub][4])

---

## 5) Lineage surfaces you should actually use in production

### 5.1 The “built-in lineage key”: `(pipeline_name, load_id)`

dlt’s destination tables guide calls out lineage value explicitly: using **pipeline name + load_id**, you can identify the source and time of data for troubleshooting or “data vault” style lineage. ([dltHub][1])

### 5.2 Persist “load info” and “trace” into your destination (high leverage)

dlt recommends saving:

* `load_info` into a table (example: `_load_info`)
* `pipeline.last_trace` into a table (example: `_trace`)

The trace includes timing and configuration/secret provenance (sensitive values removed when saved), and normalize info includes per-table row counts. ([dltHub][4])

Minimal pattern:

```python
load_info = pipeline.run(data)

# persist run artifacts for audit/debug
pipeline.run([load_info], table_name="_load_info")
pipeline.run([pipeline.last_trace], table_name="_trace")
```

([dltHub][4])

This becomes your “run log” *inside the warehouse*—extremely useful when correlating `_dlt_loads` with what actually happened.

### 5.3 Chaining transformations via `_dlt_loads.status`

dlt suggests using the `_dlt_loads.status` column to coordinate multi-step transformations: transform rows with status 0, then update status to 1, etc. ([dltHub][1])

---

## 6) The “safety harness” patterns you’ll use constantly

### Pattern A — Default all analytics to “committed views”

Create views (or dbt models) that automatically join to `_dlt_loads` with `status=0`. This prevents downstream consumers from ever seeing partially loaded data on destinations that don’t guarantee end-to-end transactional visibility. ([dltHub][1])

### Pattern B — Alert on anomalies per load

The destination tables guide notes you can test/alert per load (e.g., “no data”, “too much loaded”), and the production guide shows how to inspect package info and row counts from the trace. ([dltHub][1])

### Pattern C — Know what `pipeline.run` does on rerun

Before it extracts new data, `run` checks for pending work from previous runs; if there is pending data, it normalizes/loads it and exits. ([dltHub][4])
This is why the “commit log” concept (only completed load_ids in `_dlt_loads`) fits naturally with retry/resume.

---

## 7) Special case: loading into tables not created by dlt

If you load into a table that already exists and has data, dlt warns you may need to manually add the mandatory metadata columns (some DBs won’t let you add non-nullable columns to non-empty tables).

dlt lists the required columns you may need to pre-create:

* `_dlt_load_id`, `_dlt_id`
* and for nested tables also `_dlt_parent_id`, `_dlt_root_id` ([dltHub][1])

---

If you want the next deep dive after this, the natural continuation is:

**“Operational hardening”**: retries (tenacity helpers), failure classification (terminal vs transient), and a canonical “warehouse-side health dashboard” built on `_dlt_loads` + `_trace` row counts + staging dataset hygiene flags (truncate policies). ([dltHub][4])

[1]: https://dlthub.com/docs/general-usage/destination-tables "Destination tables & lineage | dlt Docs"
[2]: https://dlthub.com/docs/api_reference/dlt/common/storages/load_package "dlt.common.storages.load_package | dlt Docs"
[3]: https://dlthub.com/docs/general-usage/schema "Schema | dlt Docs"
[4]: https://dlthub.com/docs/running-in-production/running "Running | dlt Docs"
[5]: https://dlthub.com/docs/general-usage/full-loading "Full loading | dlt Docs"

## Operational hardening in dlt

There are **three retry layers** you want to design intentionally:

1. **Per-request (HTTP) resilience** inside sources
2. **Per-step (extract/normalize/load) retries** at the pipeline runner boundary
3. **Per-job (loader job) retries** inside destinations / custom destinations

Then you make it operational by **persisting run facts into the warehouse**: `_dlt_loads` (commit log) + `_trace`-derived metrics (row counts, timings) + a couple of **hygiene flags** so you can spot “silent” risk (staging growth, disk bloat, etc.).

---

# 1) Retries: the canonical stack

## 1.1 HTTP retries inside sources (don’t re-invent this)

dlt ships a **Requests wrapper** (`from dlt.sources.helpers import requests`) with automatic retries and configurable timeouts, recommended for API calls in sources. ([dltHub][1])

**Default behavior:**

* retries up to **5 times** with exponential delay (1s … 16s) ([dltHub][1])
* retries on **5xx** and **429** (honors `Retry-After` if present) ([dltHub][1])
* retries on connection/timeouts (unreachable server, dropped connection, request exceeds timeout) ([dltHub][1])

**Tune globally in `config.toml` (`[runtime]`):** `request_timeout`, `request_max_attempts`, `request_backoff_factor`, `request_max_retry_delay`. ([dltHub][1])

```toml
[runtime]
request_timeout = 60
request_max_attempts = 10
request_backoff_factor = 1.5
request_max_retry_delay = 30
```

**Tune locally per-client (status codes, exception classes, custom predicate):** ([dltHub][1])

```python
from dlt.sources.helpers import requests

http_client = requests.Client(
    status_codes=(429, 500, 502, 503),
    exceptions=(requests.ConnectionError, requests.ChunkedEncodingError),
)
```

Takeaway: **treat HTTP resilience as “inside the source”**, not as pipeline-step retries.

---

## 1.2 Pipeline-step retries (Tenacity + dlt’s retry helpers)

Key fact: **dlt does not retry pipeline steps by default**; step retries are intentionally left to **Tenacity + dlt helpers**. ([dltHub][2])

Also understand what you’re retrying: `pipeline.run` first syncs state/schema with the destination, then finalizes pending data (if any), then does extract→normalize→load. ([dltHub][2])
So “retrying run” may retry **sync** or **pending-data finalization**, not just the “load” you care about.

### The built-in Tenacity predicate: `retry_load`

`dlt.pipeline.helpers.retry_load()` is a Tenacity predicate that (by default) repeats the **load step** for exceptions that are **not terminal**; terminal exceptions (missing config, auth, terminal job failures, etc.) should not be retried. ([dltHub][3])

Minimal “retry load only” wrapper (pattern from docs): ([dltHub][2])

```python
from tenacity import Retrying, stop_after_attempt, wait_exponential, retry_if_exception
from dlt.pipeline.helpers import retry_load

def run_with_retries(pipeline, data):
    for attempt in Retrying(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1.5, min=4, max=10),
        retry=retry_if_exception(retry_load()),  # default: only retry "load" step, non-terminal only
        reraise=True,
    ):
        with attempt:
            return pipeline.run(data)
```

### When to retry `normalize` too

You typically add `normalize` retries only if:

* your normalize/load uses a destination connection that flakes, or
* your normalize step depends on transient externalities (rare).

But avoid retrying `extract` unless your extract is truly idempotent and your source-level HTTP retries aren’t sufficient.

---

## 1.3 Loader job retries and classification (inside destinations)

### The load-job state machine exists (and it matters)

A load job is **polled** by the loader; it starts “running” and ends in one of terminal states: `"retry"`, `"failed"`, or `"completed"`. When it reaches a terminal state, it’s discarded and not called again; the loader calls `exception()` to fetch error info for `"failed"`/`"retry"`. ([dltHub][4])
Jobs can also raise a terminal/transient load-client exception immediately in `__init__` to transition to `"failed"`/`"retry"`. ([dltHub][4])

### Custom destinations: built-in retry behavior

For `@dlt.destination`, destination calls are retried **5 times** before giving up (configurable via `load.raise_on_max_retries`). ([dltHub][5])
If your exception derives from `DestinationTerminalException`, the job is marked **failed** and **not retried**. ([dltHub][5])

**Design rule:** raise **terminal** exceptions for “won’t succeed without intervention” (bad credentials, invalid config, permanent schema/permission problems), and **transient** exceptions for flaky network/service conditions.

---

## 1.4 “I want this turnkey”: dltHub Runner

If you’re using `dlt pipeline <pipeline> run` in a project, dltHub’s Runner is used automatically; you can also call it directly. ([dltHub][6])

Runner gives you:

* configurable retry policy (fixed or exponential backoff) ([dltHub][6])
* `retry_pipeline_steps` to decide whether retries apply to `load` only vs `normalize+load` vs all steps ([dltHub][6])
* automatic trace storage (`store_trace_info`) after **each attempt**, and it won’t fail the main pipeline if trace loading fails (it keeps the trace file locally and logs warnings). ([dltHub][6])
* `run_from_clean_folder` to wipe the local pipeline working dir and restore state/schema from destination before running (useful in ephemeral runners). ([dltHub][6])

If you want “production default” without writing your own harness, Runner is the simplest path.

---

# 2) Failure classification: terminal vs transient (and where to look)

## 2.1 dlt encodes retryability in exception types

dlt provides mixins:

* `TerminalException`: unrecoverable (don’t retry)
* `TransientException`: retryable ([dltHub][7])

When a step fails, dlt raises `PipelineStepFailed`, and the underlying cause is typically in `__context__` (normal Python exception chaining). ([dltHub][2])

Docs give a simple terminal check: ([dltHub][2])

```python
from dlt.common.exceptions import TerminalException

def should_retry(ex: Exception) -> bool:
    if isinstance(ex, TerminalException) or (ex.__context__ and isinstance(ex.__context__, TerminalException)):
        return False
    return True
```

The docs also list typical terminal causes: missing config/secrets, most HTTP 40x errors, missing DB relations; destinations preserve their own terminal sets. ([dltHub][2])

## 2.2 “Failed jobs” are the sharpest edge

If any job fails terminally, it’s moved to `failed_jobs`. By default dlt raises and aborts the package with a terminal exception (`LoadClientJobFailed`); the package is “completed” but its `load_id` is **not** added to `_dlt_loads`, and pipeline state “will not be visible” to dlt. ([dltHub][2])
You can disable this behavior with:

```toml
[load]
raise_on_failed_jobs = false
```

…and then decide yourself whether to raise using `load_info.has_failed_jobs` / `load_info.raise_on_failed_jobs()`. ([dltHub][2])

Also: aborted packages **cannot be retried** (important for your retry policy). ([dltHub][8])

---

# 3) Hygiene knobs (disk + staging datasets + staging storage)

These don’t “fix correctness,” but they prevent slow, expensive operational failure modes.

## 3.1 Local load-package bloat

By default, dlt keeps loaded packages on disk for inspection; you can delete successfully completed jobs:

```toml
[load]
delete_completed_jobs = true
```

([dltHub][2])

Note: deletion is skipped if the package has failed jobs. ([dltHub][9])

## 3.2 Staging dataset bloat (merge/replace)

By default, dlt leaves data in the staging dataset used for merge/replace deduplication; you can clear it automatically:

```toml
[load]
truncate_staging_dataset = true
```

([dltHub][2])

FAQ note: truncating/dropping staging is safe, but it will be recreated and may incur cost/time. ([dltHub][10])

## 3.3 Staging storage bloat (bucket staging)

dlt doesn’t delete staged files after load, but it **truncates previously loaded files** by default; to keep full history, disable truncation: ([dltHub][11])

```toml
[destination.redshift]
truncate_tables_on_staging_destination_before_load = false
```

---

# 4) Canonical warehouse-side health dashboard

## 4.1 The minimum surfaces you want in-warehouse

### A) `_dlt_loads` (commit log)

Each load package gets a `load_id`, which is written to your data tables as `_dlt_load_id` and recorded in `_dlt_loads` with `status=0` when fully completed. ([dltHub][12])
`_dlt_loads` exists specifically so you can filter out partial loads on destinations without long transactions: rows whose `_dlt_load_id` doesn’t exist in `_dlt_loads` are “not yet completed.” ([dltHub][12])

### B) `_trace` tables (row counts + timings)

`pipeline.last_trace` contains timings and normalize row counts; you can store it with:

```python
pipeline.run([pipeline.last_trace], table_name="_trace")
```

([dltHub][2])

When you do that, dlt creates several trace-derived tables; row counts + `load_id` are available in `_trace__steps__extract_info__table_metrics`. ([dltHub][13])

### C) Optional: `_load_info`

`load_info` includes package/job states and failed job messages; it can be loaded to the destination as `_load_info`. ([dltHub][2])

> If you want traces stored *even on failed attempts*, use dltHub Runner’s `store_trace_info`; it writes trace after each successful or failed attempt via a separate trace pipeline, and trace-load failures don’t fail the main run. ([dltHub][6])

---

## 4.2 “Dashboard views” you can create in any SQL destination

Assume your dataset/schema is `mydata`.

### View 1 — committed loads

```sql
create view mydata.v_dlt_committed_loads as
select
  load_id,
  inserted_at,
  schema_name,
  schema_version_hash
from mydata._dlt_loads
where status = 0;
```

(Columns are defined in dlt docs.) ([dltHub][12])

### View 2 — detect “visible but uncommitted” rows (per table)

This is the most important safety check for append pipelines (and many destinations):

```sql
-- template: replace mydata.users with your table
select
  count(*) as uncommitted_rows
from mydata.users t
left join mydata._dlt_loads l
  on l.load_id = t._dlt_load_id
where l.load_id is null;
```

The “filter by `_dlt_loads` existence” rule is explicitly recommended by dlt. ([dltHub][12])

### View 3 — rows loaded per table per load_id (from trace)

First, inspect the trace metrics table once:

```sql
select * from mydata._trace__steps__extract_info__table_metrics limit 25;
```

dlt guarantees it contains per-table row counts and `load_id`. ([dltHub][13])

Then define a view using the discovered column names (you’ll usually see a table name + a row count metric + `load_id`):

```sql
create view mydata.v_rows_loaded as
select
  load_id,
  /* rename these after inspection */
  table_name,
  row_count
from mydata._trace__steps__extract_info__table_metrics;
```

Now you can build charts: **row_count over time** by table (the Monitoring guide explicitly suggests plotting these time series). ([dltHub][13])

### View 4 — schema churn (are we evolving unexpectedly?)

A simple “schema version changed recently” view:

```sql
create view mydata.v_schema_versions as
select
  inserted_at,
  schema_name,
  schema_version_hash
from mydata._dlt_loads
where status = 0;
```

Then monitor: “count(distinct schema_version_hash) per day” as a stability signal. (The schema hash is stored on `_dlt_loads`.) ([dltHub][12])

### View 5 — staging hygiene (DB staging dataset)

If you use merge/replace, you’ll also have `<dataset>_staging` by default. ([dltHub][12])
Two “hygiene modes”:

* if `truncate_staging_dataset=true`, staging should usually be near-empty after successful runs ([dltHub][2])
* if not, staging can grow; add a basic staging-size check (per table) based on your destination’s catalog/information_schema.

(Exact SQL differs per engine; the dashboard pattern is “staging row counts over time,” same as main tables.)

---

## 4.3 A practical “health model” (what to alert on vs monitor)

**Alert (page someone):**

* no committed load for pipeline in N hours (`max(inserted_at)` in `_dlt_loads`) ([dltHub][12])
* uncommitted rows detected in critical tables (View 2) ([dltHub][12])
* repeated terminal failures (from stored trace/load_info, or orchestrator logs) ([dltHub][2])

**Monitor (trend charts):**

* row counts per table per load (`_trace__...table_metrics`) ([dltHub][13])
* schema churn (distinct schema_version_hash) ([dltHub][12])
* runtime per step (from trace; `pipeline.last_trace` includes extract/normalize/load timings) ([dltHub][2])
* staging dataset size (if you don’t truncate) ([dltHub][2])

---

# 5) “Golden” production harness (retry + trace + hygiene)

This is the most robust “single-file” pattern if you’re not using the Runner.

```python
import dlt
from tenacity import Retrying, stop_after_attempt, wait_exponential, retry_if_exception
from dlt.pipeline.helpers import retry_load

def run_pipeline_once(pipeline: dlt.Pipeline, data):
    load_info = pipeline.run(data)

    # Persist run artifacts in-warehouse for monitoring/forensics
    pipeline.run([load_info], table_name="_load_info")           # optional but very useful
    pipeline.run([pipeline.last_trace], table_name="_trace")     # enables _trace__... metrics tables

    return load_info

def run_pipeline_with_retries(pipeline: dlt.Pipeline, data):
    # Retry only when dlt says it's retryable AND only for the load step by default
    for attempt in Retrying(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1.5, min=4, max=10),
        retry=retry_if_exception(retry_load()),
        reraise=True,
    ):
        with attempt:
            return run_pipeline_once(pipeline, data)

if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb", dataset_name="mydata")
    data = ...  # your source/resource
    run_pipeline_with_retries(pipeline, data)
```

Why this is “canonical”:

* step retries are explicit (dlt doesn’t do it automatically) ([dltHub][2])
* retries use dlt’s terminal/transient classification (via `retry_load`) ([dltHub][3])
* trace persistence creates `_trace__...table_metrics` for row-count monitoring by `load_id` ([dltHub][13])
* `_dlt_loads` remains the commit log you use to filter partial loads ([dltHub][12])

And for hygiene, add:

```toml
[load]
delete_completed_jobs = true
truncate_staging_dataset = true
```

([dltHub][2])

---

If you want the next deep dive after this: **Alerting** (Slack/webhooks) + **Tracing to Sentry** + **orchestrator integrations** (Airflow/GitHub Actions) and how to unify “orchestrator-level run state” with “warehouse-level truth” (`_dlt_loads` + trace metrics). ([dltHub][13])

[1]: https://dlthub.com/docs/general-usage/http/requests "Requests wrapper | dlt Docs"
[2]: https://dlthub.com/docs/running-in-production/running "Running | dlt Docs"
[3]: https://dlthub.com/docs/api_reference/dlt/pipeline/helpers "dlt.pipeline.helpers | dlt Docs"
[4]: https://dlthub.com/docs/api_reference/dlt/common/destination/client "dlt.common.destination.client | dlt Docs"
[5]: https://dlthub.com/docs/dlt-ecosystem/destinations/destination "Custom destination | dlt Docs"
[6]: https://dlthub.com/docs/hub/production/pipeline-runner "Pipeline runner | dlt Docs"
[7]: https://dlthub.com/docs/api_reference/dlt/common/exceptions "dlt.common.exceptions | dlt Docs"
[8]: https://dlthub.com/docs/walkthroughs/run-a-pipeline "Run a pipeline | dlt Docs"
[9]: https://dlthub.com/docs/api_reference/dlt/common/storages/load_storage "dlt.common.storages.load_storage | dlt Docs"
[10]: https://dlthub.com/docs/reference/frequently-asked-questions?utm_source=chatgpt.com "Frequently asked questions | dlt Docs"
[11]: https://dlthub.com/docs/dlt-ecosystem/staging "Staging | dlt Docs"
[12]: https://dlthub.com/docs/general-usage/destination-tables "Destination tables & lineage | dlt Docs"
[13]: https://dlthub.com/docs/running-in-production/monitoring "Monitoring | dlt Docs"

Yes — there are a handful of **dlt features we haven’t fully unpacked yet** that are *exactly* what you want for **maximal schema inference + change management**, and they map very cleanly onto **PyArrow / Pydantic / Pandera / Hamilton**:

* **Schema “settings” as an inference control plane**: ordered **type autodetectors** (`settings.detections`), global **column hint rules** (`settings.default_hints` + your own hint rules), and global **preferred types** (`settings.preferred_types`). These are applied to *newly inferred* structures and are matched after identifier normalization. ([dltHub][1])
* **Schema file workflows**: `import_schema_path`/`export_schema_path` (plus CLI tooling) and the semantics of “import schema becomes user-owned and overrides automatic updates.” ([dltHub][2])
* **“Attach schema to source” patterns**: auto-load `{source}.schema.yaml` next to the module, and runtime schema tweaks inside a source via `dlt.current.source_schema()`. ([dltHub][1])
* **Contracts + Pydantic integration semantics** (especially: what happens when you pass a model as `columns=` and how contract modes map to Pydantic behavior). ([dltHub][3])
* **Arrow-native ingress/egress** and the dataset access surface (`pipeline.dataset()` → Arrow tables / iterators). ([dltHub][4])
* **Hamilton↔dlt materializers** (`from_.dlt`, `to.dlt`) and Hamilton’s built-in Pandera/Pydantic validation hooks (`@check_output`). ([Hamilton][5])

Below is the deep dive on **Schema settings** + **Schema file workflows**, with the integrations woven in.

---

## A) Schema settings: dlt’s “inference + drift policy” layer

### A1) Where schema settings live and when they apply

A dlt schema file has a `settings` section that defines **global rules** affecting how tables/columns/types are inferred (e.g., “force timestamps,” “treat all `id` as PK,” “apply `partition` hint to `_timestamp$` columns”). ([dltHub][1])

Two key mechanics to internalize:

1. **Rules match after naming normalization** (usually snake_case, but destinations can override). ([dltHub][6])
2. These rules primarily affect **newly inferred** structure (new tables, new columns, new variant columns), i.e. *they are change-management controls*, not just “initial inference knobs.” ([dltHub][3])

---

### A2) Type autodetectors (`settings.detections`)

This is the “power inference” feature: you define an ordered list of detectors that attempt to infer a column’s type from values; they run **top-to-bottom**. dlt notes that an `iso_timestamp` detector (ISO8601 strings → timestamp) is enabled by default, and you can inspect available detectors in `detections.py`. ([dltHub][1])

**Schema YAML example (global inference behavior):**

```yaml
settings:
  detections:
    - timestamp
    - iso_timestamp
    - iso_date
    - large_integer
```

(Example structure is directly shown in the docs.) ([dltHub][1])

**Code-level control (when you want per-source tuning):**
You can add/remove detectors programmatically on a source’s schema, including inside the source function when you have runtime context. ([dltHub][1])

```python
@dlt.source
def my_source(...):
    schema = dlt.current.source_schema()
    schema.remove_type_detection("iso_timestamp")
    schema.add_type_detection("timestamp")
    ...
```

([dltHub][1])

**Integration implications**

* **PyArrow-first ingestion**: if you feed Arrow tables, the Arrow schema becomes the strongest “type truth” (dlt translates Arrow schema to destination schema and can bypass many JSON processing steps). So detectors matter most for your **messy JSON/raw dict** paths, and less for **Arrow paths**. ([dltHub][4])
* **Pydantic/Pandera**: detectors are what reduce “type entropy” upstream so your validators don’t spend their lives dealing with `str | int | None` chaos.

---

### A3) Column hint rules (`settings.default_hints` + your own hint rules)

dlt lets you define rules that apply **hints** to newly inferred columns. These rules match normalized column names and can be exact or regex (“SimpleRegex”). ([dltHub][1])

dlt also notes it **adopts hint rules from the json(relational) normalizer by default** so internal linkage columns get the right hints (`row_key`, `parent_key`, `root_key`, `unique`, `not_null`). ([dltHub][1])

Example (from docs):

```yaml
settings:
  default_hints:
    row_key:
      - _dlt_id
    parent_key:
      - _dlt_parent_id
    root_key:
      - _dlt_root_id
    unique:
      - _dlt_id
    not_null:
      - _dlt_id
      - _dlt_root_id
      - _dlt_parent_id
      - _dlt_list_idx
      - _dlt_load_id
```

([dltHub][1])

You can also add your own rules, e.g. “mark all columns ending `_timestamp` as partition columns” via regex: ([dltHub][1])

```yaml
settings:
  partition:
    - re:_timestamp$
```

…and in code you can merge hints using `source.schema.merge_hints(...)`. ([dltHub][1])

**Integration implications**

* This is the cleanest way to create *global* governance without writing repetitive `columns={...}` on every resource.
* It also gives you a single place to encode “semantic hints” you might want downstream (e.g., `x-pii: true`), which you can then use in Arrow/Hamilton transformations and “curated” table generation.

---

### A4) Preferred data types (`settings.preferred_types`)

Preferred types are rules that force the inferred type for newly created columns. Rules match **after normalization** and can be exact or regex. ([dltHub][1])

Docs example:

```yaml
settings:
  preferred_types:
    re:timestamp: timestamp
    inserted_at: timestamp
    created_at: timestamp
    updated_at: timestamp
```

([dltHub][1])

**Integration implications**

* Preferred types are your best “schema drift stabilizer” when sources are inconsistent (especially strings that look like timestamps).
* They also reduce the likelihood of variant columns being spawned due to “same column, different typing” across runs.

---

### A5) Introspection + automation hooks you can build on

These are extremely relevant if you want to *generate* Pydantic/Pandera contracts or produce Hamilton node schemas automatically:

* `resource.compute_table_schema(item=...)` evaluates hints (including dynamic hints) and returns the computed schema; `item` is specifically used to resolve table hints based on data. ([dltHub][7])
* `compute_nested_table_schemas(...)` does the same recursively for nested hints. ([dltHub][8])
* The schema docs show printing pretty YAML via `pipeline.default_schema.to_pretty_yaml()`. ([dltHub][1])

These are the primitives you’d use to build “contract generators” (e.g., emit a Pandera DataFrameModel from a dlt table schema).

---

## B) Schema file workflows: export/import + “apply changes” safely

### B1) The import/export mechanism (how you actually do schema change management)

dlt supports a deliberate workflow:

* Configure `export_schema_path` to write out schemas.
* Configure `import_schema_path` to read your edited schema on future runs. ([dltHub][2])

Example from docs:

```python
dlt.pipeline(
    import_schema_path="schemas/import",
    export_schema_path="schemas/export",
    pipeline_name="...",
    destination="duckdb",
    dataset_name="..."
)
```

([dltHub][2])

This creates a `schemas/import` and `schemas/export` folder structure. ([dltHub][2])

You can also set these in `config.toml`, and you can export in `yaml` (default), `json`, or `dbml` (export only; dbml import not implemented). ([dltHub][2])

---

### B2) The “import schema is user-owned” semantics (the crucial bit)

The adjust-a-schema guide spells out the intended mental model:

* When a schema is first created, it’s generated from global settings + resource hints.
* That initial schema is saved to the import folder if it doesn’t exist.
* Once a schema exists in the import folder, it is “writable by the user only.”
* Any changes you make in import are detected and propagated to the pipeline automatically on the next run.
* Import schema effectively “reverts” automatic updates coming from data. ([dltHub][2])

This is essentially “checked-in schema policy” for local, reproducible runs.

Under the hood, SchemaStorage links the runtime schema to an imported schema hash (`_imported_version_hash`) and uses that to detect import changes and overwrite storage schema. ([dltHub][9])

---

### B3) Applying schema edits to existing tables (where people get burned)

dlt is explicit:

* It **does not modify existing columns after table creation** (new columns can be added, but altering types or adding hints won’t automatically take effect).
* If you modify a YAML schema file, you must either delete the dataset, enable `dev_mode=True`, or use Pipeline **refresh** options to apply changes. ([dltHub][2])

This is *the* bridge to safe local migrations.

---

### B4) Refresh modes: your “local migration lever”

dlt’s pipeline refresh lets you reset state and drop/truncate tables for selected sources/resources, and dlt will only modify the destination if extract+normalize succeed; otherwise modifications are discarded. ([dltHub][10])

Modes you’ll actually use for schema-change application:

* `refresh="drop_data"`: truncate data and wipe resource state (schema is not modified). ([dltHub][11])
* `refresh="drop_sources"`: drop tables, wipe source+resource state, reset schema (and **erases schema history** for those sources). ([dltHub][10])

(There’s also `drop_resources` for dropping only selected resource tables/state.) ([dltHub][11])

**Practical local rule**

* If you changed “table structure” (types/hints that require DDL changes), treat it like a migration and use `drop_sources` for that source (or delete dataset / dev_mode depending on how disposable your local destination is). ([dltHub][2])

---

### B5) CLI tooling: validate/upgrade/convert schemas

For a schema-file-centric workflow, `dlt schema ...` is useful as a pre-commit check: it loads, validates, and can output formats (`json/yaml/dbml/dot/mermaid`). ([dltHub][12])

---

## How this all snaps into your stack

### 1) PyArrow as the interchange “truth format”

dlt supports loading Arrow Tables/RecordBatches (or Pandas DataFrames) directly; with Parquet-supporting destinations it can translate the Arrow schema, write Parquet, and bypass many JSON processing steps. ([dltHub][4])

Then, after loading, `pipeline.dataset()` can read back as Arrow tables, and dlt’s transformation docs explicitly show `.arrow()` and `iter_arrow()` patterns, including “iterate rather than fetch all at once.” ([dltHub][13])

**Why it matters for schema management**: Arrow schemas are strict, so once you move to Arrow-first “curated layers,” you can treat schema drift as an explicit event (update preferred_types/contracts + refresh/migrate), rather than a silent creep. ([dltHub][14])

---

### 2) Pydantic as row-level contract + dlt contract bridge

dlt’s schema contracts page states:

* Pydantic models can define table schemas and validate incoming data.
* Passing a model in the `columns` argument sets an implicit contract aligned with Pydantic defaults: tables evolve, extra fields ignored (discard_value), invalid data raises (data_type freeze). ([dltHub][3])
* Contract entities include `tables`, `columns`, and `data_type` (the latter triggers when a value can’t be coerced into the column’s type). ([dltHub][3])

This is the cleanest “ingest boundary” guardrail.

---

### 3) Pandera as table-level semantic validation (best placed inside Hamilton)

Pandera is ideal for constraints that dlt doesn’t try to encode as schema (ranges, uniqueness across columns, custom checks, etc.). Pandera docs also explicitly note DataFrameModel is compatible with Pydantic BaseModels. ([pandera.readthedocs.io][15])

Hamilton makes this particularly ergonomic:

* Hamilton has a Pandera plugin (`pip install sf-hamilton[pandera]`) and you can validate outputs with `@check_output(schema=...)`.
* It also supports Pydantic validation with `@check_output(model=...)` (strict mode). ([Hamilton][16])

So your “maximal” local posture becomes:

* dlt schema settings + contracts control **structural drift**
* Hamilton+Pandera/Pydantic checks control **semantic correctness**
* Arrow is the performance + type-fidelity medium between them

---

### 4) Hamilton ↔ dlt glue you don’t need to write

Hamilton’s dlt integration provides:

* `from_.dlt(...)` DataLoader to pull in-memory data from a `dlt.Resource`
* `to.dlt(...)` DataSaver to write Hamilton node results via a `dlt.Pipeline`, and you can pass pipeline.run behavior knobs (primary_key, incremental, loader format, etc.). ([Hamilton][5])

This is the “keep everything local, keep everything typed” endgame.

---

## If you want the next deep dive

Given your priorities, the next chapter that will pay off most is:

**Schema contracts in practice (with Pydantic + Arrow paths) + a concrete “two-layer” local design**:

* **raw landing** (high inference, permissive evolve, capture drift)
* **curated** (Arrow-first + contract freeze + Hamilton validations)
* explicit migration recipes using `refresh` and import-schema edits.

All the building blocks are now on the table: detectors + hint rules + preferred types + import/export semantics + refresh + validators + Arrow IO. ([dltHub][1])

[1]: https://dlthub.com/docs/general-usage/schema "Schema | dlt Docs"
[2]: https://dlthub.com/docs/walkthroughs/adjust-a-schema "Adjust a schema | dlt Docs"
[3]: https://dlthub.com/docs/general-usage/schema-contracts "Schema and data contracts | dlt Docs"
[4]: https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas "Arrow Table / Pandas | dlt Docs"
[5]: https://hamilton.apache.org/integrations/dlt/ "dlt - Hamilton"
[6]: https://dlthub.com/docs/general-usage/naming-convention "Naming convention | dlt Docs"
[7]: https://dlthub.com/docs/general-usage/resource "Resource | dlt Docs"
[8]: https://dlthub.com/docs/api_reference/dlt/extract/hints "dlt.extract.hints | dlt Docs"
[9]: https://dlthub.com/docs/api_reference/dlt/common/storages/schema_storage "dlt.common.storages.schema_storage | dlt Docs"
[10]: https://dlthub.com/docs/general-usage/pipeline "Pipeline | dlt Docs"
[11]: https://dlthub.com/docs/api_reference/dlt/pipeline?utm_source=chatgpt.com "dlt.pipeline.pipeline | dlt Docs"
[12]: https://dlthub.com/docs/reference/command-line-interface "Command Line Interface | dlt Docs"
[13]: https://dlthub.com/docs/general-usage/dataset-access/dataset "Access datasets in Python | dlt Docs"
[14]: https://dlthub.com/blog/how-dlt-uses-apache-arrow "How dlt uses Apache Arrow"
[15]: https://pandera.readthedocs.io/en/stable/pydantic_integration.html "Pydantic - pandera documentation"
[16]: https://hamilton.apache.org/concepts/function-modifiers/ "Function modifiers - Hamilton"

Below is the deep dive you asked for: **schema contracts in practice** (including **Pydantic + Arrow paths**) and a **concrete two-layer, local-first design** with **explicit migration recipes** that use **import-schema edits + refresh**.

---

# 1) Schema contracts: the real control points

dlt contracts target **three schema entities**:

* `tables` (new table created)
* `columns` (new column on an existing table)
* `data_type` (a value can’t be coerced to the existing column type → “type drift”) ([DLT Hub][1])

Contract modes are:

* `evolve` (allow)
* `freeze` (raise → nothing loads)
* `discard_row` (drop offending rows)
* `discard_value` (drop offending values, keep the row) ([DLT Hub][1])

Important nuances:

* Contracts are evaluated **after table/column names are normalized**. ([DLT Hub][1])
* A contract set on a **resource applies to root + nested tables** that resource creates. ([DLT Hub][1])
* `discard_row` is **table-level**: if a nested table row violates, dlt discards that nested row but still loads the parent root row. ([DLT Hub][1])
* Default `evolve` behavior: new tables and columns are accepted, and “bad type” values are routed into **variant columns** (e.g., `answer__v_text`). ([DLT Hub][1])

---

# 2) Contracts + Pydantic: “validate at extract boundary”

## 2.1 What you get when you pass a Pydantic model

dlt can use Pydantic models to **define table schemas** and **validate incoming data**, synthesizing models as needed to match your contract. ([DLT Hub][1])

If you pass a model in the `columns=` argument of `@dlt.resource`, dlt implicitly applies this contract:

```python
{
  "tables": "evolve",
  "columns": "discard_value",
  "data_type": "freeze",
}
```

Meaning: new tables allowed, extra fields ignored, invalid types raise. ([DLT Hub][1])

## 2.2 How contract modes map to Pydantic “extra” handling

dlt maps **column contract modes** to Pydantic’s “extra fields” behavior:

* `evolve` → allow
* `freeze` → forbid
* `discard_value` → ignore
* `discard_row` → forbid

…and it works bidirectionally (if your model config sets extra behavior, dlt reflects it into contract mode). ([DLT Hub][1])

## 2.3 What data_type mode means for Pydantic

For Pydantic validation inside dlt:

* `data_type=evolve` produces a lenient model that allows any type (can lead to variant columns later).
* `data_type=freeze` re-raises validation errors.
* `data_type=discard_row` removes non-validating items.
* `discard_value` for `data_type` isn’t supported (noted as potential future work for Pydantic v2 handling). ([DLT Hub][1])

## 2.4 Where validation happens (and a critical consequence)

Pydantic validation runs **on extracted data before names are normalized or nested tables are created**—so your model fields must match the **input** keys. ([DLT Hub][1])

Also: since validation happens before nesting, `discard_row` drops the **whole data item**, even if only a nested model field was problematic. ([DLT Hub][1])

## 2.5 Practical power tools for Pydantic↔dlt alignment

dlt exposes utilities to:

* Convert a Pydantic model to dlt `columns` dict (`pydantic_to_table_schema_columns`)
* Apply schema contract semantics to a model (`apply_schema_contract_to_model`)
* Validate/filter items in batch (`validate_and_filter_items`) ([DLT Hub][2])

### DltConfig: control how nested/complex fields become schema

You can attach `dlt_config` to a model to control schema generation. For example, `skip_nested_types=True` excludes `dict`, `list`, and `BaseModel` fields from the dlt schema. ([DLT Hub][2])

This is very useful in a curated layer where you want “structural simplicity” (e.g., keep nested payloads as JSON rather than relationalize further).

---

# 3) Contracts + Arrow/Pandas: “tabular contracts behave differently”

## 3.1 Arrow loads are a different pipeline path

dlt supports loading `pyarrow.Table`, `pyarrow.RecordBatch`, and `pandas.DataFrame` directly; on Parquet-native destinations (including DuckDB) it can translate the Arrow schema and write Parquet, bypassing many JSON pipeline steps for performance. ([DLT Hub][3])

**Big practical constraint:** the Arrow data must already be compatible with the destination types because dlt doesn’t do row-level conversion in that fast path. ([DLT Hub][3])

## 3.2 Contracts apply to Arrow/Pandas, but with special semantics

dlt explicitly states that all contract settings apply to Arrow tables and pandas frames as well, with key differences:

* `columns` mode can **modify tables/frames during extract** to avoid rewriting Parquet files.
* `data_type` changes in tables/frames are **not allowed** and produce a schema clash (no “variant column” evolution for Arrow’s strict types). ([DLT Hub][1])

And for Arrow/Pandas column modes specifically:

* `evolve`: allow new columns (table may be reordered to put them at the end)
* `discard_value`: delete the column
* `discard_row`: delete rows that have the column present, then delete the column
* `freeze`: raise on a new column ([DLT Hub][1])

## 3.3 Arrow lineage columns are opt-in

When loading Arrow tables, dlt does **not** add `_dlt_load_id` / `_dlt_id` by default for performance. You can enable them:

```toml
[normalize.parquet_normalizer]
add_dlt_load_id = true
add_dlt_id = true
```

…and dlt documents the overhead tradeoff (adding `_dlt_id` requires rereading and rewriting extracted files). ([DLT Hub][3])

---

# 4) DataValidationError: turning contract violations into actionable diffs

When a freeze-mode contract is violated, dlt raises `DataValidationError`, re-raised via `PipelineStepFailed`. dlt documents how to catch it differently depending on whether it happened in `extract` or `normalize`. ([DLT Hub][1])

The error includes: schema/table/column, which entity violated, the schema used (dlt schema or Pydantic model), the expanded contract, and the actual offending data item. ([DLT Hub][1])

This is the hook you use to implement “local-first schema migration prompts” (i.e., generate a PR snippet for your import schema or Pydantic model).

---

# 5) Two-layer local design: raw landing vs curated

This design assumes **local DuckDB** (works great with Arrow/Parquet), but the pattern holds for any destination.

## 5.1 Layer 1: RAW landing

**Goal:** accept messy input, infer aggressively, never block ingestion, and **capture drift** so you can promote it later.

### Recommended configuration

**Schema settings** should do the heavy lifting: detectors, preferred types, and hint rules (global inference controls). ([DLT Hub][4])

**Contract posture (raw):**

* `tables: evolve`
* `columns: evolve`
* `data_type: evolve`

Why: you want new tables/columns and type drift to land, with type drift going to **variant columns** by default. ([DLT Hub][1])

### Schema file strategy (raw)

Use export/import, but keep import minimal. The “adjust schema” workflow is:

* export schema shows what inference produced
* import schema contains only explicit declarations and acts as the user-owned override ([DLT Hub][5])

In raw, I usually do:

* export schema always on (for audit)
* import schema only for **global settings**, not for pinning every column

You can also bundle a schema next to the source module (`{source}.schema.yaml`) and dlt will auto-load it. ([DLT Hub][4])

### “Raw pipeline” skeleton

```python
import dlt

raw = dlt.pipeline(
    pipeline_name="my_source_raw",
    destination="duckdb",
    dataset_name="raw_my_source",
    import_schema_path="schemas/raw/import",
    export_schema_path="schemas/raw/export",
)

# permissive ingestion, drift lands as new cols/variant cols
raw.run(my_source(), schema_contract="evolve")
```

## 5.2 Layer 2: CURATED

**Goal:** enforce strong contracts, keep types stable, emit Arrow-first tables, validate with Hamilton + Pandera/Pydantic, and load curated tables with `merge` or `replace`.

### Curated contract posture (strict)

For curated tables you generally want:

* `tables: freeze` (no surprise tables)
* `columns: freeze` (no surprise columns)
* `data_type: freeze` (no surprise type drift)

This gives you “schema drift = explicit migration” behavior. ([DLT Hub][1])

### Curated should be Arrow-first

Read raw tables into Arrow and keep transformations tabular:

* `pipeline.dataset()` provides lazy relations and can materialize to Arrow via `.arrow()` (or chunked reads). ([DLT Hub][6])

### Hamilton as the “curation spine”

Hamilton’s dlt integration frames dlt as extract/load and Hamilton as transform. ([hamilton.apache.org][7])
Hamilton materialization (`from_` / `to`) is explicitly designed to reduce boilerplate and integrate with third-party libs including dlt. ([hamilton.staged.apache.org][8])

For validation: Hamilton’s `@check_output` supports Pandera and Pydantic validators directly. ([hamilton.staged.apache.org][9])

### “Curated pipeline” skeleton (Arrow + Hamilton)

```python
import dlt

raw = dlt.attach(pipeline_name="my_source_raw")
raw_ds = raw.dataset()

# pull raw as Arrow
raw_events = raw_ds.table("events").arrow()  # or chunk it

# run Hamilton transforms (you produce a pyarrow.Table output)
curated_table = build_curated_arrow_table(raw_events)

cur = dlt.pipeline(
    pipeline_name="my_source_curated",
    destination="duckdb",
    dataset_name="curated_my_source",
    import_schema_path="schemas/curated/import",
    export_schema_path="schemas/curated/export",
)

# strict: fail fast on schema drift
cur.run(curated_table, table_name="events_curated", schema_contract="freeze")
```

### Where Pydantic fits best in curated

Two strong patterns:

1. **Use Pydantic to define the curated schema** (`columns=Model`) and let dlt enforce it on ingestion into curated. ([DLT Hub][1])
2. **Use Hamilton checks** (`@check_output(model=...)`) to validate outputs before you even call dlt. ([hamilton.staged.apache.org][9])

I usually do both for maximal correctness: Hamilton validates “transform correctness”, dlt validates “load correctness”.

---

# 6) Explicit migration recipes (import-schema edits + refresh)

This is the part that makes “curated freeze” workable without pain.

## 6.1 The key rule: dlt won’t mutate existing columns automatically

dlt does not modify existing columns after creation; changes like type alterations or new hints won’t take effect unless you delete the dataset, run in `dev_mode=True`, or use refresh options. ([DLT Hub][5])

## 6.2 Workflow: evolve in raw → promote to curated via import schema / model

**Promote a new field from raw to curated:**

1. Raw ingests it (new column appears, or drift lands in variant columns). ([DLT Hub][1])
2. Decide how curated should represent it:

   * add it to the curated **Pydantic model**, or
   * copy the column from **export schema** to **import schema** and set `data_type`, `nullable`, hints, etc. ([DLT Hub][5])
3. Apply migration using refresh (next section).

## 6.3 Refresh modes: choose the lightest hammer that actually applies your change

Refresh options (conceptually):

* `drop_data`: truncate table data and wipe resource state, **schema unchanged** (good for “full reload from initial_value”). ([DLT Hub][10])
* `drop_resources`: drop selected tables and wipe resource state; **schema history erased** for affected sources. ([DLT Hub][10])
* `drop_sources`: drop all tables for sources being processed + wipe source/resource state + reset schema; dlt documents step-by-step including removing schema from `_dlt_version`. ([DLT Hub][10])

**Applying a schema edit (type change, new NOT NULL, new hints):**

* Use `refresh="drop_resources"` if you only changed one resource’s tables.
* Use `refresh="drop_sources"` if you changed schema-level rules or multiple tables and want a clean re-materialization. ([DLT Hub][10])

Example:

```python
cur.run(curated_table, table_name="events_curated", schema_contract="freeze", refresh="drop_resources")
```

## 6.4 A practical “freeze transition” pattern for curated

If you’re migrating a dataset that has some manual tables/columns or you need an initial inference pass, dlt notes that “new tables” are treated specially: for a single run it temporarily allows columns to evolve for new tables, then reverts to the configured mode for subsequent runs. It also defines what counts as “new” (nested tables, dynamic tables, incomplete columns, etc.). ([DLT Hub][1])

A robust pattern is:

1. Run curated once with `schema_contract={"tables":"evolve","columns":"evolve","data_type":"freeze"}` to establish baseline and prevent type drift.
2. Export schema, copy critical pieces into import schema / models.
3. Switch curated to full `freeze`.
4. If you change types/hints later, use `drop_resources` / `drop_sources` to apply. ([DLT Hub][1])

## 6.5 Migration observability: treat schema hashes as your “change log”

Each schema has a content-based `version_hash` and numeric version; the destination is migrated when its hash isn’t stored in dlt’s schema tracking table, and dlt uses this to detect synchronization. ([DLT Hub][4])

In practice, for local operations you can:

* diff `schemas/curated/export/*.schema.yaml` across runs
* query `_dlt_version` to see schema history and `_dlt_loads.schema_version_hash` to link loads to schema versions ([DLT Hub][11])

---

# 7) A “maximal” local posture for your specific stack

If you want the strongest possible setup with minimal ongoing friction:

### RAW layer

* Aggressive schema settings (detectors + preferred types + hint rules). ([DLT Hub][4])
* `schema_contract="evolve"` so ingestion never blocks and drift lands as columns/variant columns. ([DLT Hub][1])
* Export schema always; keep import schema minimal.

### CURATED layer

* Arrow-first outputs loaded via `pipeline.run(pyarrow.Table, ...)` (fast Parquet path on DuckDB). ([DLT Hub][3])
* Enable `_dlt_load_id` (and optionally `_dlt_id`) for lineage on Arrow loads if you care; otherwise keep it off for max throughput. ([DLT Hub][3])
* Define table schema via Pydantic (`columns=Model` + `DltConfig`) and/or import schema. ([DLT Hub][1])
* Hamilton validates with `@check_output(schema=...)` (Pandera) and/or `@check_output(model=...)` (Pydantic) before load. ([hamilton.staged.apache.org][9])
* `schema_contract="freeze"` and migrations are explicit via `refresh="drop_resources"`/`drop_sources`. ([DLT Hub][1])

---

If you want, the next “most useful” deep dive after this is to turn the above into a **full end-to-end reference implementation template**:

* a sample `schemas/raw/` and `schemas/curated/` layout (with example detectors/hints)
* a “drift report” query against `_dlt_version` + `_dlt_loads`
* Hamilton module skeleton that reads raw via `pipeline.dataset().table(...).arrow()`, validates with Pandera/Pydantic, and writes curated Arrow tables back through dlt. ([DLT Hub][6])

[1]: https://dlthub.com/docs/general-usage/schema-contracts "Schema and data contracts | dlt Docs"
[2]: https://dlthub.com/docs/api_reference/dlt/common/libs/pydantic "dlt.common.libs.pydantic | dlt Docs"
[3]: https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas "Arrow Table / Pandas | dlt Docs"
[4]: https://dlthub.com/docs/general-usage/schema "Schema | dlt Docs"
[5]: https://dlthub.com/docs/walkthroughs/adjust-a-schema "Adjust a schema | dlt Docs"
[6]: https://dlthub.com/docs/general-usage/dataset-access/dataset "Access datasets in Python | dlt Docs"
[7]: https://hamilton.apache.org/integrations/dlt/ "dlt - Hamilton"
[8]: https://hamilton.staged.apache.org/concepts/materialization/ "Materialization - Hamilton"
[9]: https://hamilton.staged.apache.org/reference/decorators/check_output/ "check_output* - Hamilton"
[10]: https://dlthub.com/docs/general-usage/pipeline "Pipeline | dlt Docs"
[11]: https://dlthub.com/docs/general-usage/destination-tables "Destination tables & lineage | dlt Docs"

Below is a **repo-style, end-to-end reference template** that implements the two-layer pattern:

* **RAW landing**: high inference + permissive evolve + drift capture
* **CURATED**: Arrow-first + contract freeze + Hamilton validations
* **Change management**: import-schema edits + `refresh` migrations
* **Monitoring**: a warehouse-side drift report using `_dlt_loads` + `_dlt_version`

This is written for a **local-only** setup (e.g., DuckDB destination), but the pattern is destination-agnostic.

---

# 0) Proposed project layout

```text
my_project/
  pyproject.toml
  README.md

  config/
    config.toml
    secrets.toml

  schemas/
    raw/
      import/
        my_source.schema.yaml
      export/                      # generated
    curated/
      import/
        curated.schema.yaml        # curated contract + settings
      export/                      # generated

  src/
    myproj/
      dlt_pipelines.py             # create/attach pipelines
      sources/
        my_source.py               # @dlt.source + resources
      hamilton/
        curated_nodes.py           # Hamilton nodes + validations
      run_raw.py                   # run raw landing
      run_curated.py               # run curated build
      sql/
        drift_report.sql           # the drift report queries
```

---

# 1) dlt config for local-only operation

## `config/config.toml`

This sets some “sane defaults” plus (optionally) enables `_dlt_load_id`/`_dlt_id` on Arrow/Parquet normalization for curated lineage. (Arrow metadata columns are opt-in and have performance tradeoffs.)

```toml
# config/config.toml

[load]
delete_completed_jobs = true

# If you use merge/replace with staging dataset and want it cleaned:
# truncate_staging_dataset = true

[normalize.parquet_normalizer]
# CURATED: turn these on if you want full lineage columns on Arrow/Parquet loads
add_dlt_load_id = true
add_dlt_id = true
```

## `config/secrets.toml`

If you’re local-only DuckDB, this may be empty.

---

# 2) Sample schemas layout (RAW + CURATED)

Two key points before the examples:

* The safest workflow is: **run once with export enabled**, then copy the generated schema to `import/` and edit. dlt’s import schema is “user-owned,” and changes there override future automatic updates.
* **Schema edits that require DDL changes** (type changes, certain hints) usually require a migration (`refresh` / drop tables) because dlt doesn’t automatically mutate existing columns.

---

## 2.1 RAW import schema: “high inference + drift capture”

### `schemas/raw/import/my_source.schema.yaml` (representative snippet)

This focuses on the **settings** section—the core of inference control (detections, preferred types, hint rules). dlt supports ordered type detections and rules that match after naming normalization.

```yaml
# schemas/raw/import/my_source.schema.yaml
# Tip: generate via export first, then copy to import and edit.

name: my_source
version: 1
engine_version: 11

settings:
  # Type inference detectors (run in order).
  detections:
    - iso_timestamp
    - iso_date

  # Force stable types for common drift columns (matches after normalization).
  preferred_types:
    re:(_at|_ts|_timestamp)$: timestamp
    re:^is_.*: bool
    re:.*_id$: text

  # Global hint rules (example: treat *_id as a candidate key).
  # (Exact hints supported vary by destination; use these primarily as policy markers.)
  primary_key:
    - re:.*_id$

  # You typically don't need to restate default_hints; dlt adopts normalizer defaults.
  # But you can pin them if you want "schema as explicit contract surface".
  default_hints:
    row_key: [_dlt_id]
    parent_key: [_dlt_parent_id]
    root_key: [_dlt_root_id]
    not_null: [_dlt_id, _dlt_load_id, _dlt_parent_id, _dlt_list_idx]
```

**RAW contract posture**
You’ll normally run raw with `schema_contract="evolve"` so new columns/tables and type drift land instead of blocking ingestion (type drift typically becomes variant columns).

---

## 2.2 CURATED import schema: “freeze + strong typing”

### `schemas/curated/import/curated.schema.yaml` (representative snippet)

Curated should be strict: the schema is *yours*, and drift becomes an explicit migration. Contracts apply to tables/columns/data_type.

```yaml
# schemas/curated/import/curated.schema.yaml
name: curated
version: 1
engine_version: 11

settings:
  # Curated inference should be minimal — you want Arrow/Pydantic to drive types.
  preferred_types:
    re:(_at|_ts|_timestamp)$: timestamp
    re:.*_id$: text

# You can also embed schema_contract per-table in the schema file (or pass it in pipeline.run).
# Many teams keep contract selection in code for clarity; either approach works.
```

In curated, you’ll run with `schema_contract="freeze"` (or a dict that freezes tables/columns/types).

---

# 3) Python: pipelines + sources

## 3.1 `src/myproj/dlt_pipelines.py`

This centralizes how you create/attach pipelines and ensures the import/export paths are consistent.

```python
# src/myproj/dlt_pipelines.py
from __future__ import annotations

from pathlib import Path
import dlt

ROOT = Path(__file__).resolve().parents[2]  # repo root (adjust if needed)

def raw_pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="my_source_raw",
        destination="duckdb",
        dataset_name="raw_my_source",
        import_schema_path=str(ROOT / "schemas/raw/import"),
        export_schema_path=str(ROOT / "schemas/raw/export"),
    )

def curated_pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="my_source_curated",
        destination="duckdb",
        dataset_name="curated_my_source",
        import_schema_path=str(ROOT / "schemas/curated/import"),
        export_schema_path=str(ROOT / "schemas/curated/export"),
    )

def attach_raw() -> dlt.Pipeline:
    return dlt.attach(pipeline_name="my_source_raw")

def attach_curated() -> dlt.Pipeline:
    return dlt.attach(pipeline_name="my_source_curated")
```

## 3.2 `src/myproj/sources/my_source.py` (minimal example source)

Keep RAW permissive; use resource hints sparingly—prefer schema settings for global inference policy.

```python
# src/myproj/sources/my_source.py
from __future__ import annotations

import dlt

@dlt.resource(name="events", write_disposition="append")
def events():
    # yield dicts (raw JSON-ish)
    yield {"event_id": "e1", "created_at": "2026-01-11T00:00:00Z", "payload": {"x": 1}}
    yield {"event_id": "e2", "created_at": "2026-01-11T01:00:00Z", "payload": {"x": 2}}

@dlt.source
def my_source():
    return events
```

---

# 4) Running RAW landing (and capturing drift)

## `src/myproj/run_raw.py`

```python
# src/myproj/run_raw.py
from __future__ import annotations

from myproj.dlt_pipelines import raw_pipeline
from myproj.sources.my_source import my_source

def main() -> None:
    p = raw_pipeline()

    # RAW: permissive
    info = p.run(
        my_source(),
        schema_contract="evolve",
    )
    print(info)

if __name__ == "__main__":
    main()
```

This gives you:

* raw tables + nested tables
* `_dlt_load_id` tagging on rows
* `_dlt_loads` commit-log rows (status=0 is “committed”)
* `_dlt_version` schema history and hashes you can diff in your drift report

---

# 5) Drift report SQL (_dlt_loads + _dlt_version)

Create `src/myproj/sql/drift_report.sql`.

These queries assume your dataset/schema is `raw_my_source` or `curated_my_source`. (Replace accordingly.)

## 5.1 “Schema hash changed since previous load?”

`_dlt_loads` contains `schema_version_hash`; `_dlt_version` contains the schema JSON keyed by `version_hash`.

```sql
-- src/myproj/sql/drift_report.sql
-- Replace schema name raw_my_source with your dataset schema.

with committed as (
  select
    load_id,
    inserted_at,
    schema_name,
    schema_version_hash
  from raw_my_source._dlt_loads
  where status = 0
),
joined as (
  select
    c.*,
    v.version as schema_version,
    v.inserted_at as schema_inserted_at,
    v.schema as schema_json
  from committed c
  left join raw_my_source._dlt_version v
    on v.schema_name = c.schema_name
   and v.version_hash = c.schema_version_hash
)
select
  load_id,
  inserted_at as load_inserted_at,
  schema_name,
  schema_version_hash,
  schema_version,
  schema_inserted_at,
  (schema_version_hash <> lag(schema_version_hash) over (
      partition by schema_name order by inserted_at
   )) as schema_changed_since_prev_load
from joined
order by inserted_at desc
limit 200;
```

## 5.2 “Schema churn summary”

```sql
select
  date_trunc('day', inserted_at) as day,
  schema_name,
  count(*) as committed_loads,
  count(distinct schema_version_hash) as distinct_schema_versions
from raw_my_source._dlt_loads
where status = 0
group by 1, 2
order by day desc, schema_name;
```

## 5.3 “Loads referencing unknown schema hash”

(Should be ~0; useful sanity check.)

```sql
select
  l.load_id,
  l.inserted_at,
  l.schema_name,
  l.schema_version_hash
from raw_my_source._dlt_loads l
left join raw_my_source._dlt_version v
  on v.schema_name = l.schema_name
 and v.version_hash = l.schema_version_hash
where l.status = 0
  and v.version_hash is null
order by l.inserted_at desc
limit 100;
```

---

# 6) Hamilton module skeleton: Arrow → validate → Arrow → curated dlt load

This skeleton uses:

* **dlt** to read raw tables as Arrow (`pipeline.dataset().table(...).arrow()`)
* **Hamilton** to structure transforms
* **Pandera** to validate the table (convert to pandas for validation, then convert back to Arrow)
* **Pydantic** for row-level constraints (optional, but included in a practical way: validate a *sample* or validate critical columns)
* **dlt** to load curated Arrow tables with `schema_contract="freeze"` and your chosen disposition

> Note: Pandera is primarily DataFrame-centric; validating Arrow directly is not as universal, so the common pattern is Arrow → pandas → validate → Arrow.

## 6.1 `src/myproj/hamilton/curated_nodes.py`

```python
# src/myproj/hamilton/curated_nodes.py
from __future__ import annotations

from typing import Iterable
import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd

import pandera as pa_checks
from pandera.typing import Series

from pydantic import BaseModel, Field, ValidationError

# Hamilton validation decorators
from hamilton.function_modifiers import check_output


# -------------------------
# Pydantic row model (optional)
# -------------------------

class EventRow(BaseModel):
    event_id: str = Field(min_length=1)
    created_at: str  # keep as str if RAW; curated may convert to datetime elsewhere

def _validate_rows_pydantic(df: pd.DataFrame, sample_n: int = 1000) -> None:
    # Local-only pragmatic approach: validate a sample so you catch gross drift
    # without paying per-row validation for huge tables.
    sample = df.head(sample_n)
    errors: list[str] = []
    for i, rec in enumerate(sample.to_dict(orient="records")):
        try:
            EventRow.model_validate(rec)
        except ValidationError as e:
            errors.append(f"row[{i}]: {e}")
            if len(errors) >= 10:
                break
    if errors:
        raise ValueError("Pydantic validation failed (sample):\n" + "\n".join(errors))


# -------------------------
# Pandera DataFrameModel (table semantics)
# -------------------------

class EventsCurated(pa_checks.DataFrameModel):
    event_id: Series[str] = pa_checks.Field(nullable=False)
    created_at: Series[pd.Timestamp] = pa_checks.Field(nullable=False)
    x: Series[int] = pa_checks.Field(nullable=False, ge=0)

    class Config:
        strict = True  # no extra columns


# -------------------------
# Nodes
# -------------------------

def raw_events_arrow(raw_dataset_schema: str = "raw_my_source") -> pa.Table:
    """
    Reads RAW dlt table as Arrow.
    This is intentionally not parameterizing pipeline_name here; you can pass it via config.
    """
    import dlt

    p = dlt.attach(pipeline_name="my_source_raw")
    ds = p.dataset()

    # If large, prefer iter_arrow() in a different node to stream chunks.
    return ds.table("events").arrow()


def curated_events_arrow(raw_events_arrow: pa.Table) -> pa.Table:
    """
    Example transform:
    - ensure event_id exists
    - parse created_at
    - extract payload.x into a top-level column x
    """
    t = raw_events_arrow

    # Extract nested payload.x if payload is a struct / JSON-ish column.
    # This is a placeholder — adjust to your actual RAW schema.
    cols = t.column_names

    # event_id passthrough
    event_id = t["event_id"]

    # created_at -> timestamp (Arrow)
    created_at = pc.strptime(t["created_at"], format="%Y-%m-%dT%H:%M:%SZ", unit="s")

    # payload.x example: if payload is struct, you can use struct_field
    # If payload is JSON text, parse_json + json_extract_scalar would be needed.
    if "payload" in cols and pa.types.is_struct(t.schema.field("payload").type):
        x = pc.struct_field(t["payload"], "x")
    else:
        # fallback: if you already landed x in a column, use it; else raise
        if "x" in cols:
            x = t["x"]
        else:
            raise ValueError("Expected payload struct with field 'x' or column 'x'")

    out = pa.table(
        {
            "event_id": event_id,
            "created_at": created_at,
            "x": x,
        }
    )
    return out


@check_output(schema=EventsCurated)
def curated_events_df(curated_events_arrow: pa.Table) -> pd.DataFrame:
    """
    Validate using Pandera (DataFrameModel).
    Convert Arrow -> pandas, validate, return validated DataFrame.
    """
    df = curated_events_arrow.to_pandas(types_mapper=None)
    # Pydantic sample validation as an extra guardrail (optional)
    _validate_rows_pydantic(df, sample_n=500)
    return df


def curated_events_validated_arrow(curated_events_df: pd.DataFrame) -> pa.Table:
    """
    Convert validated DF back to Arrow for dlt loading.
    """
    return pa.Table.from_pandas(curated_events_df, preserve_index=False)
```

Hamilton’s `@check_output` supports Pandera and Pydantic checks via the data-quality extensions.

---

## 6.2 `src/myproj/run_curated.py`: run Hamilton, then load via dlt

```python
# src/myproj/run_curated.py
from __future__ import annotations

from hamilton import driver
import dlt

from myproj.dlt_pipelines import curated_pipeline
from myproj.hamilton import curated_nodes


def main() -> None:
    # Build Hamilton DAG from module
    dr = driver.Driver({}, curated_nodes)

    # Execute to produce the curated Arrow table
    curated_arrow = dr.execute(["curated_events_validated_arrow"])["curated_events_validated_arrow"]

    # Load into curated dataset using dlt
    p = curated_pipeline()

    # CURATED: strict
    info = p.run(
        curated_arrow,
        table_name="events",
        schema_contract="freeze",
        # Choose your write policy:
        # write_disposition="replace",
        # or for incremental curated tables:
        # write_disposition={"disposition":"merge", "strategy":"upsert"},
        # primary_key="event_id",
    )
    print(info)


if __name__ == "__main__":
    main()
```

Notes:

* If you want merge/upsert in curated, set `primary_key` and `write_disposition={"disposition":"merge","strategy":"upsert"}` (where supported), or use `delete-insert` merge when you want dedupe semantics in staging.
* If you enable `_dlt_load_id` / `_dlt_id` for Arrow normalization, configure it in `normalize.parquet_normalizer` as shown earlier.

---

# 7) Migration recipes (import schema edits + refresh)

Here’s the practical loop you’ll use constantly in curated:

### Scenario: curated schema is `freeze`, and you add a new column `y`

1. Run curated build → it fails with a contract error (good).
2. Decide the change:

   * update Hamilton transform to produce `y`
   * update curated import schema (or your Pydantic model / Pandera model)
3. Apply DDL-affecting changes with `refresh` (because dlt doesn’t automatically alter existing columns).

Example (drop & rebuild curated table cleanly):

```python
p.run(
  curated_arrow,
  table_name="events",
  schema_contract="freeze",
  refresh="drop_resources",
)
```

Use `drop_sources` if you changed schema-wide settings affecting multiple tables.

---

# What you now have “end-to-end”

* **RAW**: drift-friendly ingestion with global inference policy (detections + preferred types + hint rules)
* **CURATED**: Arrow-first outputs, validated in Hamilton (Pandera/Pydantic), loaded with strict contracts
* **Change mgmt**: import/export schema workflow + explicit refresh migrations
* **Drift reporting**: `_dlt_loads` + `_dlt_version` queries you can run locally in DuckDB

If you want to push this to “maximal”: the next incremental improvement is to make the Hamilton DAG **chunked** (use `iter_arrow()` instead of `.arrow()` for large raw tables) and validate per-chunk, then load curated in chunks as well.
