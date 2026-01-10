

# 4) UI / telemetry SDK mechanics

## 4.0 Mental model

**Apache Hamilton UI = a self-hostable “observability + catalog + versioning” service** that becomes useful only when you attach a **tracking adapter** to your Driver. The adapter streams:

* **DAG structure + code provenance** (so the UI can version/browse the dataflow),
* **run telemetry + node-level timing/error attribution** (so you can compare runs and debug),
* **artifact metadata + data summaries/statistics** (so the catalog is searchable and executions are inspectable). ([Hamilton][1])

The UI itself explicitly targets four “first-class” surfaces: **run telemetry**, a **feature/artifact catalog**, a **DAG visualizer for lineage**, and a **project explorer with dataflow versioning**. ([Hamilton][1])

---

## 4.1 Running the UI: local vs deployed (and what changes)

### 4.1.1 Local mode (SQLite, single-machine)

Local mode is the fast way to get value during dev: install the extras and run the CLI:

```bash
pip install "sf-hamilton[ui,sdk]"
hamilton ui
# python -m hamilton.cli.__main__ ui  # on windows
```

This launches a browser at `localhost:8241`. The docs explicitly note local mode can handle small workflows, but recommend Postgres (deployed mode) for “full scalability and a multi-read/write db.” ([Hamilton Apache Incubator][2])

### 4.1.2 Docker/deployed mode (frontend + backend + Postgres)

Deployed mode is a docker-compose stack that starts **UI frontend**, **backend server**, and **Postgres**. Setup is:

```bash
git clone https://github.com/apache/hamilton
cd hamilton/ui
./run.sh
# UI available at http://localhost:8242
```

Docs call out:

* “Invalid HTTP_HOST” → set `HAMILTON_ALLOWED_HOSTS="*"` (or comma-separated hosts) for the backend container. ([Hamilton Apache Incubator][2])
* Building images locally: increase Docker memory to **10GB+**, then `./dev.sh --build` (or `./dev.sh` to mount local code). ([Hamilton Apache Incubator][2])
* Self-hosting behind a subpath: set `REACT_APP_HAMILTON_SUB_PATH=/hamilton` (must begin with `/`). ([Hamilton Apache Incubator][2])
* There’s also explicit mention you can run the UI on **Snowflake Container Services** (docs link out to a guide/example). ([Hamilton Apache Incubator][2])

### 4.1.3 Ports and why they matter to the tracker

The tracker defaults to sending telemetry to `localhost:8241/8242` and you can override both via `hamilton_api_url` and `hamilton_ui_url`. The docs explicitly note “if using docker the UI is on 8242.” ([Hamilton][3])

---

## 4.2 How “projects” and “DAG versions” are formed

### 4.2.1 Open-source UI flow (HamiltonTracker)

The UI workflow is:

1. start UI,
2. create/select a project in the UI,
3. attach `HamiltonTracker(project_id=..., username=..., dag_name=..., tags=...)` to your Driver,
4. run your DAG; logs include links back to the UI. ([Hamilton Apache Incubator][2])

### 4.2.2 Hosted UI flow (DAGWorksTracker) — same mental model, different auth

DAGWorks’ hosted docs make the “versioning semantics” explicit:

* On **Driver instantiation**, a new project “version” is created if code changed or you provide a new `dag_name`, and the DAG structure is saved.
* When the run is complete, run metadata and summaries are logged and appear in the UI. ([DAGWorks Documentation][4])

That’s useful even for self-hosted/open-source planning because it tells you the *intended boundary*: **driver build == “version check”**, **driver execute == “run record.”** ([DAGWorks Documentation][4])

---

## 4.3 What the UI is designed to show (feature-by-feature, with the mechanics behind each)

### 4.3.1 Telemetry: run history + comparisons + run data

The UI advertises “run tracking + telemetry” including history, comparisons, and “data for specific runs.” ([Hamilton][5])
Hamilton’s UI announcement blog further frames this as execution observability including profiling/traces and errors pinpointed at the function level. ([blog.dagworks.io][6])
The async-focused blog post calls out a **waterfall chart of node performance** and result inspection in the UI once you add the right adapter. ([blog.dagworks.io][7])

**Mechanics (design boundary):**

* You don’t “instrument nodes” manually; you attach a lifecycle adapter via `Builder.with_adapters(tracker)` and execute normally. ([Hamilton][3])
* Node attribution depends on stable node naming; intermediate nodes (pipes/extracts) will also appear unless you control naming/namespacing (Topic #1).

### 4.3.2 Feature / artifact catalog

The UI overview explicitly includes an “assets/features catalog” to view “functions, nodes, and assets across a history of runs.” ([Hamilton][5])
The UI announcement blog describes this as a combined transform + artifact catalog that auto-populates from runs and includes artifact metadata captured through “approved methods” (i.e., the Hamilton I/O/materialization patterns). ([blog.dagworks.io][6])

**Mechanics (how you make catalog entries “real”):**

* Prefer explicit loaders/savers/materializers (so artifacts are first-class, not incidental side effects).
* Use tags consistently so the catalog can be filtered by environment/team/version (see §4.5).

### 4.3.3 DAG visualizer / lineage and the code browser

The UI overview calls out:

* a DAG visualizer “for exploring and looking at your code, and determining lineage,”
* and a “browser” view to inspect dataflow structure and code. ([Hamilton][1])

**Mechanics:**

* The tracker must upload DAG structure + code context; that’s why “versioning” is tied to driver instantiation (you want the UI to see the graph before execution, not just the run after). ([DAGWorks Documentation][4])

### 4.3.4 Dataflow versioning

The UI overview includes “dataflow versioning” (select/compare versions). ([Hamilton][5])
Practically, version boundaries come from **(code changes OR `dag_name` changes)**, which is explicitly how the hosted tracker behaves; you should treat `dag_name` as a deliberate “semantic version key,” not a label. ([DAGWorks Documentation][4])

---

## 4.4 SDK wiring: HamiltonTracker (self-hosted UI)

### 4.4.1 Minimal integration (one adapter)

```python
from hamilton import driver
from hamilton_sdk import adapters

tracker = adapters.HamiltonTracker(
    project_id=PROJECT_ID,
    username="YOU@COMPANY.COM",          # same identity you entered in the UI
    dag_name="codeintel_semantic_v1",    # treat as “version key”
    tags={"environment": "DEV", "team": "CodeIntel", "version": "v1"},
)

dr = (
    driver.Builder()
    .with_modules(*your_modules)
    .with_config(your_config)
    .with_adapters(tracker)
    .build()
)

dr.execute([...])
```

This is the canonical pattern in the UI docs. ([Hamilton][3])

### 4.4.2 Pointing the tracker at a remote/self-hosted instance

```python
tracker = adapters.HamiltonTracker(
    project_id=PROJECT_ID,
    username="YOU@COMPANY.COM",
    dag_name="codeintel_semantic_v1",
    tags={"environment": "PROD", "team": "CodeIntel"},
    hamilton_api_url="http://YOUR_DOMAIN:8241",
    hamilton_ui_url="http://YOUR_DOMAIN:8242",  # docker UI port
)
```

The docs state you can override `hamilton_api_url` / `hamilton_ui_url`, and that defaults are localhost `8241/8242`. ([Hamilton][3])

---

## 4.5 Capture control: performance + data governance knobs (what to tune and how)

Hamilton documents a small but important set of capture controls in `hamilton_sdk.tracking.constants`, with three configuration planes:

* module defaults,
* config file (default `~/.hamilton.conf`),
* environment variables (prefixed with `HAMILTON_`),
* direct constant assignment (highest precedence). ([Hamilton][3])

### 4.5.1 The documented constants

* `CAPTURE_DATA_STATISTICS` (default True): whether to capture data insights/statistics
* `MAX_LIST_LENGTH_CAPTURE` (default 50): truncation for list capture
* `MAX_DICT_LENGTH_CAPTURE` (default 100): truncation for dict capture
* `DEFAULT_CONFIG_URI` (default `~/.hamilton.conf`) ([Hamilton][3])

### 4.5.2 Recommended “safe-by-default” profile (my strong suggestion)

For CodeIntel-style workloads (where objects can contain code, paths, secrets, etc.), I’d treat “data capture” as an explicit opt-in:

* Set `CAPTURE_DATA_STATISTICS=False` in production by environment variable.
* Keep list/dict max sizes small globally; allow local overrides in dev.

```bash
export HAMILTON_CAPTURE_DATA_STATISTICS=0
export HAMILTON_MAX_LIST_LENGTH_CAPTURE=20
export HAMILTON_MAX_DICT_LENGTH_CAPTURE=50
```

You can also do the same via `~/.hamilton.conf` under `[SDK_CONSTANTS]`. ([Hamilton][3])

*(This recommendation goes beyond the docs: the docs tell you how to configure; the “safe-by-default” policy is an operational stance that tends to prevent accidental leakage.)*

---

## 4.6 Async services: AsyncDriver + AsyncHamiltonTracker (FastAPI-grade integration)

Hamilton’s AsyncDriver docs include an example of using an async tracker adapter:

```python
from hamilton import async_driver
from hamilton_sdk import adapters

tracker_async = adapters.AsyncHamiltonTracker(
    project_id=1,
    username="elijah",
    dag_name="async_tracker",
)

dr = (
    await async_driver.Builder()
    .with_modules(async_module)
    .with_adapters(tracker_async)
    .build()
)
```

This is the documented “async builder + async tracker” pattern. ([Hamilton][8])

**Operational note:** the async builder is described as “more limited” (e.g., doesn’t support some features “for now”), so keep your service integration slim: one driver per worker, stable config surface, and avoid mixing in unsupported execution modes. ([Hamilton][8])

---

## 4.7 Hosted DAGWorks tracker: mechanics that matter even if you self-host

If you use DAGWorks hosted:

* You attach `adapters.DAGWorksTracker(project_id, api_key, username, dag_name, tags)` via `Builder.with_adapters(...)`. ([DAGWorks Documentation][9])
* Their docs call out that run tracking can have marginal overhead for *very short* executions due to connecting to the server, but for longer executions the impact “disappears.” ([DAGWorks Documentation][4])

Even if you’re self-hosting the open-source UI, that’s a good heuristic: **don’t over-telemetry micro-runs**; batch or raise the granularity where possible.

---

## 4.8 Holistic “best in class” example: production service + self-hosted UI + deterministic versioning + governance

This shows: docker-deployed UI, tracker endpoint override, strong `dag_name` semantics, tags aligned to your semantic layer, and capture governance.

### 4.8.1 Infra (UI stack)

* Deploy UI via docker-compose; fix host validation with `HAMILTON_ALLOWED_HOSTS`; set subpath if you reverse proxy. ([Hamilton Apache Incubator][2])

### 4.8.2 App (Driver factory)

```python
from __future__ import annotations

import os
from hamilton import driver
from hamilton_sdk import adapters

def build_codeintel_driver(*, modules: list[object], cfg: dict) -> driver.Driver:
    tracker = adapters.HamiltonTracker(
        project_id=int(os.environ["HAMILTON_PROJECT_ID"]),
        username=os.environ["HAMILTON_USERNAME"],
        # Make DAG versioning intentional:
        # - semantic layer version
        # - repo+commit fingerprint (or release)
        dag_name=f"codeintel::semantic_v1::{cfg['repo']}::{cfg['commit']}",
        tags={
            "environment": os.environ.get("APP_ENV", "prod"),
            "team": "CodeIntel",
            "repo": cfg["repo"],
            "commit": cfg["commit"],
            "semantic_version": "v1",
        },
        hamilton_api_url=os.environ.get("HAMILTON_API_URL", "http://ui-backend:8241"),
        hamilton_ui_url=os.environ.get("HAMILTON_UI_URL", "http://ui-frontend:8242"),
    )

    return (
        driver.Builder()
        .with_modules(*modules)
        .with_config(cfg)          # build-time config selection
        .with_adapters(tracker)    # telemetry + UI integration
        .build()
    )
```

### 4.8.3 Governance (capture tuning)

Use env vars / config file to clamp capture volume and disable stats in prod. The SDK supports env-var overrides with `HAMILTON_` prefix and documents precedence. ([Hamilton][3])

---

If you want, I can now “close the loop” by writing the **UI-facing semantic conventions** as a concrete spec (exact tag keys + required values + naming rules + a registry compiler), so the Hamilton UI’s catalog/versioning becomes the authoritative UI for your semantic layer — not just a nice-to-have telemetry screen.

[1]: https://hamilton.apache.org/hamilton-ui/ui/?utm_source=chatgpt.com "UI Overview - Hamilton"
[2]: https://hamilton.incubator.apache.org/hamilton-ui/ui/ "UI Overview - Hamilton"
[3]: https://hamilton.apache.org/concepts/ui/ "UI Overview - Hamilton"
[4]: https://docs.dagworks.io/capabilities?utm_source=chatgpt.com "Hosted Hamilton UI on DAGWorks Overview - DAGWorks Documentation"
[5]: https://hamilton.apache.org/hamilton-ui/ui/ "UI Overview - Hamilton"
[6]: https://blog.dagworks.io/p/hamilton-ui-streamlining-metadata?utm_source=chatgpt.com "Hamilton UI: Streamlining Metadata, Tracking, Lineage/Tracing, and Observability for Your Dataflows"
[7]: https://blog.dagworks.io/p/async-dataflows-in-hamilton?utm_source=chatgpt.com "Async Dataflows in Hamilton"
[8]: https://hamilton.dagworks.io/en/latest/reference/drivers/AsyncDriver/?utm_source=chatgpt.com "AsyncDriver - Hamilton"
[9]: https://docs.dagworks.io/adapter?utm_source=chatgpt.com "DAGWorks Tracking Adapter - DAGWorks Documentation"



# UI-facing semantic conventions spec for Hamilton

Goal: make **Hamilton UI + tracker tags** the authoritative “catalog + versioned contract surface” for your semantic layer, and make it **machine-readable** (registry compiler → serving tools, CI snapshots, docs).

This spec defines:

* exact **tag keys + required values**
* **naming + versioning rules** (node names vs semantic IDs vs dag_name)
* a **registry compiler** (Driver → `semantic_registry.json`) with validation
* optional **data validation hooks** (CI) so the UI/catalog cannot drift silently

---

## 1) Core invariants

### 1.1 Identity model

* **`semantic_id` is the stable public identifier** (what agents/clients use).
* **Hamilton node name** (function name) is implementation detail and may change.
* UI “DAG versions” are driven by **code changes** + your chosen **`dag_name`** conventions. (Treat `dag_name` as a stable *product key*.)

### 1.2 Minimal discoverability contract

Every public semantic output must satisfy:

* `layer == "semantic"`
* `semantic_id` exists
* `kind` exists (table/scalar/artifact)
* `entity` + `grain` exist

This is what enables both:

* UI filtering (semantic-only views), and
* registry compilation via `Driver.list_available_variables(tag_filter=...)`.

---

## 2) Tag taxonomy

### 2.1 Required tags for **public semantic outputs** (`layer="semantic"`)

These must exist on every semantic output node.

| Tag key       | Type |              Required | Example                               | Meaning                                    |
| ------------- | ---: | --------------------: | ------------------------------------- | ------------------------------------------ |
| `layer`       |  str |                     ✅ | `"semantic"`                          | Public semantic surface                    |
| `semantic_id` |  str |                     ✅ | `"function.risk_score.v1"`            | Stable API identifier                      |
| `kind`        |  str |                     ✅ | `"table"` / `"scalar"` / `"artifact"` | Output class                               |
| `entity`      |  str |                     ✅ | `"function"` / `"module"`             | Entity type                                |
| `grain`       |  str |                     ✅ | `"per_function"` / `"per_module"`     | Row-level grain                            |
| `version`     |  str |                     ✅ | `"1"`                                 | Semantic schema version (not code version) |
| `schema_ref`  |  str |  ✅ for `kind="table"` | `"semantic.function_risk_v1"`         | Logical schema name                        |
| `entity_keys` |  str |  ✅ for `kind="table"` | `"repo,commit,goid_h128"`             | Keys uniquely identifying entity rows      |
| `join_keys`   |  str |  ✅ for `kind="table"` | `"repo,commit,goid_h128"`             | Keys to join across semantic tables        |
| `dtype`       |  str | ✅ for `kind!="table"` | `"float64"` / `"str"`                 | Scalar/series dtype                        |
| `stability`   |  str |                     ✅ | `"experimental"` / `"stable"`         | Client-facing stability                    |

### 2.2 Strongly recommended tags (all semantic outputs)

These are not strictly required, but they make the UI genuinely useful.

| Tag key             | Example                                      | Why                         |
| ------------------- | -------------------------------------------- | --------------------------- |
| `owner`             | `"codeintel"` / `"platform"`                 | Ownership routing + triage  |
| `description`       | `"Risk score (v1) derived from LOC + cyclo"` | UI catalog readability      |
| `unit`              | `"score"` / `"ms"` / `"count"`               | Prevents metric misuse      |
| `pii`               | `"none"` / `"possible"` / `"yes"`            | Governance / capture policy |
| `source_system`     | `"duckdb"` / `"ibis"` / `"pandas"`           | Interpretation + debugging  |
| `materialization`   | `"duckdb_view"` / `"parquet"`                | How users should consume it |
| `materialized_name` | `"semantic.v_function_risk_v1"`              | Stable external object name |

### 2.3 Optional layer tags (non-public nodes)

Use these to keep the UI navigable without polluting the semantic registry.

* `layer="raw"`: ingestion / source-of-truth nodes
* `layer="intermediate"`: internal transforms/features
* `layer="docs"`: doc/export views intended for humans, not agents

The registry compiler in §4 defaults to `layer="semantic"` only.

---

## 3) Naming & versioning rules

### 3.1 Node name rules

* Node names can change; **semantic_id cannot** (treat as API).
* Never embed semantic versioning into node names; keep it in `semantic_id` and `version`.

### 3.2 Stable public object naming (DuckDB / warehouse)

If you materialize:

* always produce a stable external name (e.g., `semantic.v_function_risk_v1`)
* store it in tags:

  * `materialization="duckdb_view"`
  * `materialized_name="semantic.v_function_risk_v1"`

### 3.3 `dag_name` rules (UI version stream)

Treat `dag_name` as a **product key**, not an execution instance.

Recommended:

* `dag_name = "codeintel::semantic_v1"` (stable)
* per-run metadata goes in tracker tags:

  * `repo`, `commit`, `run_kind` (`full|incremental|ci`), `dataset_snapshot_id`, etc.

Avoid including commit in `dag_name` unless you explicitly want “one UI version per commit.”

### 3.4 Intermediate nodes: namespace aggressively

Everything produced by pipes/extracts/subdags should be namespaced so UI browsing is clean:

* intermediate nodes → `namespace="prep"` / `"feat"` / `"int"` etc.
* semantic outputs should live at top-level (no namespace) unless you intentionally group by domain.

---

## 4) Registry compiler

### 4.1 Registry schema (what you write to `semantic_registry.json`)

One row per semantic output (keyed by `semantic_id`).

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

@dataclass(frozen=True)
class SemanticEntry:
    semantic_id: str
    node_name: str               # Hamilton execution key
    kind: str                    # table|scalar|artifact
    entity: str
    grain: str
    version: str
    schema_ref: str | None
    entity_keys: tuple[str, ...]
    join_keys: tuple[str, ...]
    dtype: str | None
    stability: str
    materialization: str | None
    materialized_name: str | None
    tags: Mapping[str, Any]
    python_type: str | None      # for debugging (stringified)
```

### 4.2 Compiler + validator (drop-in)

```python
from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

REQUIRED_BASE = {"layer", "semantic_id", "kind", "entity", "grain", "version", "stability"}

def _split_keys(v: Any) -> tuple[str, ...]:
    if v is None:
        return tuple()
    if isinstance(v, (list, tuple)):
        return tuple(str(x).strip() for x in v if str(x).strip())
    s = str(v)
    return tuple(x.strip() for x in s.split(",") if x.strip())

def compile_semantic_registry(dr, *, layer: str = "semantic") -> dict[str, SemanticEntry]:
    """
    Pulls semantic outputs from Hamilton metadata (tags) and returns semantic_id -> SemanticEntry.

    Contract: every semantic output must have layer=semantic and semantic_id tag.
    """
    nodes = dr.list_available_variables(tag_filter={"layer": layer, "semantic_id": None})
    out: dict[str, SemanticEntry] = {}

    for n in nodes:
        tags = dict(n.tags)
        missing = REQUIRED_BASE - tags.keys()
        if missing:
            raise ValueError(f"Node {n.name} missing required tags: {sorted(missing)}")

        kind = str(tags["kind"])
        schema_ref = tags.get("schema_ref")
        entity_keys = _split_keys(tags.get("entity_keys"))
        join_keys = _split_keys(tags.get("join_keys"))
        dtype = tags.get("dtype")

        if kind == "table":
            for k in ("schema_ref", "entity_keys", "join_keys"):
                if not tags.get(k):
                    raise ValueError(f"Semantic table node {n.name} missing tag: {k}")
            if not entity_keys or not join_keys:
                raise ValueError(f"Semantic table node {n.name} must declare non-empty entity_keys/join_keys")
        else:
            if not dtype:
                raise ValueError(f"Non-table semantic node {n.name} must declare dtype")

        semantic_id = str(tags["semantic_id"])
        if semantic_id in out:
            raise ValueError(f"Duplicate semantic_id={semantic_id} on nodes {out[semantic_id].node_name} and {n.name}")

        out[semantic_id] = SemanticEntry(
            semantic_id=semantic_id,
            node_name=n.name,
            kind=kind,
            entity=str(tags["entity"]),
            grain=str(tags["grain"]),
            version=str(tags["version"]),
            schema_ref=str(schema_ref) if schema_ref else None,
            entity_keys=entity_keys,
            join_keys=join_keys,
            dtype=str(dtype) if dtype else None,
            stability=str(tags["stability"]),
            materialization=str(tags.get("materialization")) if tags.get("materialization") else None,
            materialized_name=str(tags.get("materialized_name")) if tags.get("materialized_name") else None,
            tags=tags,
            python_type=str(getattr(n, "type", None)) if getattr(n, "type", None) is not None else None,
        )

    return out

def write_semantic_registry(path: str, registry: dict[str, SemanticEntry]) -> None:
    payload = {k: asdict(v) for k, v in registry.items()}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, sort_keys=True, indent=2, default=str)
```

### 4.3 CI snapshot contract

In CI, you want:

* `semantic_registry.json` diff reviewable
* failure if:

  * duplicate `semantic_id`
  * missing required tags
  * missing `entity_keys/join_keys` for tables
  * missing `dtype` for scalars/artifacts

Optionally, in CI only, also execute each semantic table on a small fixture dataset and verify:

* declared `entity_keys` columns exist
* declared `join_keys` columns exist
* if you tag a `schema_ref`, verify output columns match the schema definition you keep elsewhere (Pandera/Ibis schema, etc.)

---

## 5) Tagging helpers (to prevent typos)

### 5.1 Semantic output decorator factory

```python
from __future__ import annotations

from hamilton.function_modifiers import tag

def semantic_output(
    *,
    semantic_id: str,
    kind: str,
    entity: str,
    grain: str,
    version: str,
    stability: str = "experimental",
    schema_ref: str | None = None,
    entity_keys: str | None = None,
    join_keys: str | None = None,
    dtype: str | None = None,
    materialization: str | None = None,
    materialized_name: str | None = None,
    owner: str = "codeintel",
    description: str | None = None,
    pii: str = "none",
):
    tags = {
        "layer": "semantic",
        "semantic_id": semantic_id,
        "kind": kind,
        "entity": entity,
        "grain": grain,
        "version": version,
        "stability": stability,
        "owner": owner,
        "pii": pii,
    }
    if description:
        tags["description"] = description
    if schema_ref:
        tags["schema_ref"] = schema_ref
    if entity_keys:
        tags["entity_keys"] = entity_keys
    if join_keys:
        tags["join_keys"] = join_keys
    if dtype:
        tags["dtype"] = dtype
    if materialization:
        tags["materialization"] = materialization
    if materialized_name:
        tags["materialized_name"] = materialized_name
    return tag(**tags)
```

---

## 6) End-to-end example (what “closed loop” looks like)

### 6.1 Define a semantic table node

```python
from __future__ import annotations

import pandas as pd

@semantic_output(
    semantic_id="function.risk_score.v1",
    kind="table",
    entity="function",
    grain="per_function",
    version="1",
    stability="stable",
    schema_ref="semantic.function_risk_v1",
    entity_keys="repo,commit,goid_h128",
    join_keys="repo,commit,goid_h128",
    materialization="duckdb_view",
    materialized_name="semantic.v_function_risk_v1",
    description="Function risk score v1 (LOC + cyclo + callgraph signals)",
)
def semantic_function_risk_v1(function_facts: pd.DataFrame, call_graph_edges: pd.DataFrame) -> pd.DataFrame:
    ...
```

### 6.2 Build driver with tracker tags aligned to the same semantic vocabulary

```python
from hamilton import driver
from hamilton_sdk import adapters

tracker = adapters.HamiltonTracker(
    project_id=PROJECT_ID,
    username=USER,
    dag_name="codeintel::semantic_v1",
    tags={
        "environment": ENV,
        "team": "CodeIntel",
        "semantic_version": "v1",
        "repo": repo,
        "commit": commit,
        "run_kind": run_kind,
    },
    hamilton_api_url=API_URL,
    hamilton_ui_url=UI_URL,
)

dr = (
    driver.Builder()
    .with_modules(...)
    .with_config(...)
    .with_adapters(tracker)
    .build()
)
```

### 6.3 Compile registry from the DAG (single source of truth)

```python
reg = compile_semantic_registry(dr)
write_semantic_registry("semantic_registry.json", reg)
```

### 6.4 Make UI authoritative

Operationally, you now have:

* UI shows *exactly* the semantic outputs (filter `layer=semantic`)
* `semantic_registry.json` is derived from the same tags the UI uses
* serving layer can read `semantic_registry.json` and map `semantic_id -> node_name -> execute/materialize`

---

## 7) Recommended “definition of done” checks

1. **Registry snapshot** checked into repo (or generated in CI and diffed).
2. **No duplicate semantic IDs** (compiler enforces).
3. **Required tags** enforced (compiler enforces).
4. **Key columns exist** in CI fixture execution for table outputs.
5. **UI tags** (tracker tags + node tags) share common vocabulary (`environment`, `repo`, `commit`, `semantic_version`) so UI filters naturally match your system concepts.

---

If you share your current tag usage (or a handful of representative semantic nodes), I can adapt this spec into a repo-ready `docs/semantic_conventions.md` + a `src/codeintel/serving/semantic_registry_compiler.py` that matches your existing entity keys (`repo/commit/module/goid_h128/urn`) and your dataset contract naming.


# addendum - detailing for maximal coverage #

Below is a **maximal, “turn every knob” observability stack** for Apache Hamilton that stays within **Hamilton’s built-in logging + telemetry capabilities** (plus Hamilton’s first-party SDK/plugins). The goal is: **you can always answer “which node are we in?”, “why is it slow?”, “what did it produce?”, “was it cached?”, and “where did it blow up?”**—without guessing.

---

## 1) Terminal logging: the fast “where is it hanging?” baseline

### 1.1 Enable cache event logging (even if you’re not “debugging caching”)

Hamilton’s caching subsystem exposes a dedicated logger: **`hamilton.caching`**. Setting it to `INFO` shows high-signal events like `GET_RESULT` vs `EXECUTE_NODE`; `DEBUG` is noisier but more complete. ([Hamilton][1])

```python
import logging

logger = logging.getLogger("hamilton.caching")
logger.setLevel(logging.INFO)          # DEBUG for step-by-step
logger.addHandler(logging.StreamHandler())
```

Hamilton documents the cache log line structure as:
`{node_name}::{task_id}::{actor}::{event_type}::{message}` (empty parts omitted). ([Hamilton][1])

**Why you want this even during “hang” debugging:** the *last* cache log line you see is usually the node you entered right before you got stuck.

---

## 2) Lifecycle adapters: node-by-node timing, inputs/outputs, progress, and breakpoints

Lifecycle adapters are Hamilton’s built-in “execution hooks” system. You attach them via `Builder.with_adapters(...)`. ([Hamilton][2])

### 2.1 PrintLn: the “always-on flight recorder”

`lifecycle.PrintLn` is the simplest high-signal adapter:

* **verbosity=1**: prints node name + time
* **verbosity=2**: prints inputs + results too (careful with huge objects)
* `node_filter`: callable `(node_name, node_tags) -> bool` or list/str to restrict which nodes log
* `print_fn`: can be `print` or any function like `logger.info` ([Hamilton][3])

```python
from hamilton import driver, lifecycle

print_adapter = lifecycle.PrintLn(
    verbosity=1,
    print_fn=print,      # or logger.info
    node_filter=None,    # or ["core.ast_nodes", ...] or callable
)
```

This is the **single best “hang detector”** because you’ll see a **pre-node** line and (if it finishes) a **post-node** line. If you get the “before” line but never the “after” line—*that* node is your culprit.

### 2.2 Progress bars (tqdm / rich): “is anything moving?”

Hamilton provides progress-bar lifecycle adapters:

* `plugins.h_tqdm.ProgressBar`
* `plugins.h_rich.RichProgressBar` (requires `sf-hamilton[rich]`) ([Hamilton][2])

These hook graph + node execution and make “stalled vs slow” visually obvious.

### 2.3 Type checking hook: catch silent “wrong type” cascades

`lifecycle.FunctionInputOutputTypeChecker(check_input=True, check_output=True)` strictly checks runtime values vs annotated types. It’s intentionally strict. ([Hamilton][4])

This is extremely useful when performance problems are actually “wrong-shaped data” problems (e.g., you thought you were passing an Arrow Table but you’re passing a generator, which then triggers expensive coercion somewhere downstream).

### 2.4 GracefulErrorAdapter: keep going to get partial outputs (diagnostics mode)

If you’re failing (not hanging) and want *max visibility*, `GracefulErrorAdapter` lets the DAG proceed by replacing failing branches with a sentinel value, so you can still see what executed and what got skipped. It’s explicitly designed to “proceed despite failure” by pruning downstream branches. ([Hamilton][5])

It can also handle `Parallelizable` nodes with `try_all_parallel` behavior. ([Hamilton][5])

### 2.5 PDBDebugger: drop into the exact node that’s suspicious

`lifecycle.PDBDebugger` can inject `pdb` **before/during/after** a selected node (via `node_filter`). ([Hamilton][6])

---

## 3) Visualization: “what will execute?” (and save it before you run)

Hamilton supports:

* `dr.display_all_functions(...)` to show the full DAG
* `dr.visualize_execution(final_vars, inputs, overrides)` to show the *executed subgraph* for a specific run request ([Hamilton][7])

Hamilton explicitly recommends generating the execution visualization **before** running, because if execution fails first, you won’t get the figure. ([Hamilton][7])

```python
final_vars = ["your", "targets"]
inputs = {...}
overrides = {...}

dr.visualize_execution(final_vars=final_vars, inputs=inputs, overrides=overrides)
dr.execute(final_vars=final_vars, inputs=inputs, overrides=overrides)
```

For your situation (“where are the hangups?”), this gives you a concrete, *finite* set of nodes to watch in logs/telemetry.

---

## 4) Cache structured logs: deterministic, queryable run evidence (JSONL + programmatic)

Even if you don’t want caching for speedups, the caching adapter can be used as a **structured observability channel**.

### 4.1 Turn on structured logs (+ write JSONL)

Hamilton documents:

* `Driver.cache.logs(level="info"|"debug", run_id=...)`
* `.with_cache(log_to_file=True)` appends events to a `.jsonl` file “as they happen” (ideal for production debugging) ([Hamilton][1])

Also note: `Driver.cache.last_run_id` is the **last started** run, not necessarily the last completed run. ([Hamilton][8])

### 4.2 What you can inspect (public API)

Hamilton exposes public cache introspection helpers like:

* `dr.cache.get_cache_key(run_id=..., node_name=..., task_id=...)`
* `dr.cache.get_data_version(...)`
* `dr.cache.logs(...)` with shapes that change depending on `run_id` and task-based execution ([Hamilton][8])

This is especially useful if you have **task-based/dynamic** execution: logs can key by `(node_name, task_id)` when relevant. ([Hamilton][8])

### 4.3 Cache visualization (hits vs executes)

After execution, `dr.cache.view_run(...)` produces a DAG visualization with cached nodes highlighted. ([Hamilton][1])
(And Hamilton notes `.view_run()` doesn’t currently support task-based execution / `Parallelizable/Collect`.) ([Hamilton][1])

---

## 5) Hamilton UI telemetry (best single tool for “what’s slow?”)

Hamilton has a first-party UI that provides:

* run tracking + telemetry (history/comparisons)
* feature/artifact catalog
* DAG visualizer + code browser
* project/DAG versioning ([Hamilton][9])

### 5.1 Run it locally (fastest path)

Hamilton’s doc: `pip install "sf-hamilton[ui,sdk]"` then `hamilton ui` to launch on localhost (local mode). ([Hamilton][9])

### 5.2 Attach the tracker to your Driver

Add `HamiltonTracker(...)` via `Builder.with_adapters(tracker)` (works with existing code). ([Hamilton][9])

It can also be pointed at remote/self-hosted deployments via `hamilton_api_url` / `hamilton_ui_url`. ([Hamilton][9])

### 5.3 Control capture volume (critical for huge objects)

Hamilton documents SDK constants (in `hamilton_sdk.tracking.constants`) such as:

* `CAPTURE_DATA_STATISTICS` (default `True`)
* `MAX_LIST_LENGTH_CAPTURE` (default `50`)
* `MAX_DICT_LENGTH_CAPTURE` (default `100`)
* `DEFAULT_CONFIG_URI` (default `~/.hamilton.conf`) ([Hamilton][9])

You can override them via:

1. module defaults
2. config file
3. env vars prefixed `HAMILTON_`
4. direct assignment (highest precedence) ([Hamilton][9])

This matters because “capture data statistics” can be super helpful—but can also become non-trivial overhead on large Arrow/DF objects, so you’ll want a **debug profile** and a **production profile**.

---

## 6) Tracing (still Hamilton-native): DDOGTracer for “span-per-node” timelines

If you want a true tracing view (spans around node execution), Hamilton ships a Datadog lifecycle adapter:

`plugins.h_ddog.DDOGTracer(root_name, include_causal_links=False, service=None)`.

Hamilton documents that it traces node execution and supports vanilla + task-based synchronous + multithreading/Ray/Dask, but **not async (yet)**. ([Hamilton][10])

This can be a very strong “hang detector” because you’ll see one span blowing out in duration.

---

## 7) The lifecycle API surface area (for maximal custom diagnostics)

If you outgrow the built-ins, Hamilton’s lifecycle API gives you clean hook points:

* `NodeExecutionHook` with `run_before_node_execution(...)` / `run_after_node_execution(...)`
* `GraphExecutionHook` (pre/post graph execute)
* `GraphConstructionHook` (post graph construct)
* Task hooks for task-based/dynamic execution (submission/return/execute/grouping) ([Hamilton][2])

For example, `NodeExecutionHook` explicitly receives `node_name`, `node_tags`, `node_kwargs`, `node_return_type`, `task_id`, and `run_id` in both before/after hooks. ([Hamilton][11])

That’s enough to build a **JSONL “span log”** per run without touching your node code.

---

## 8) A “max insight” recommended stack for diagnosing hangs (single-machine)

If you want the shortest path to actionable signal:

1. **`PrintLn(verbosity=1)`** (logs node start/end + duration; gives you “where it’s stuck”). ([Hamilton][3])
2. **`RichProgressBar`** (human feedback, obvious stalls). ([Hamilton][2])
3. **Cache logger `hamilton.caching` at INFO** (correlate cache hits vs executes). ([Hamilton][12])
4. **Hamilton UI + HamiltonTracker** (waterfall/run telemetry + artifacts). ([Hamilton][9])
5. **Optional: DDOGTracer** if you want a tracing backend view. ([Hamilton][10])

And remember: Hamilton lets you debug by **requesting intermediate nodes as outputs**, without modifying the DAG code—useful for isolating where things go sideways. ([Hamilton][13])

---

If you want, I can take your current **CodeIntel DAG target list** and give you a **drop-in “diagnostics profile”** (exact adapters + exact node filters for each stage: AST, CST, SCIP, syntax_augment, CPG) so you can flip between:

* `profile="dev_fast"` (minimal telemetry),
* `profile="debug_hangs"` (max timing + light payload),
* `profile="debug_payloads"` (targeted verbosity=2 + UI data stats for specific nodes),

…without changing the underlying DAG logic.

[1]: https://hamilton.apache.org/concepts/caching/ "Caching - Hamilton"
[2]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/ "Lifecycle Adapters - Hamilton"
[3]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/PrintLn/ "lifecycle.PrintLn - Hamilton"
[4]: https://hamilton.apache.org/reference/lifecycle-hooks/FunctionInputOutputTypeChecker/ "lifecycle.FunctionInputOutputTypeChecker - Hamilton"
[5]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/GracefulErrorAdapter/ "lifecycle.GracefulErrorAdapter - Hamilton"
[6]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/PDBDebugger/?utm_source=chatgpt.com "lifecycle.PDBDebugger - Hamilton"
[7]: https://hamilton.staged.apache.org/concepts/visualization/ "Visualization - Hamilton"
[8]: https://hamilton.apache.org/reference/caching/caching-logic/ "Caching logic - Hamilton"
[9]: https://hamilton.apache.org/concepts/ui/ "UI Overview - Hamilton"
[10]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/DDOGTracer/ "plugins.h_ddog.DDOGTracer - Hamilton"
[11]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/NodeExecutionHook/ "lifecycle.NodeExecutionHook - Hamilton"
[12]: https://hamilton.apache.org/how-tos/caching-tutorial/ "Caching - Hamilton"
[13]: https://hamilton.apache.org/how-tos/llm-workflows/ "LLM workflows - Hamilton"

Here’s a single “maximal logging + telemetry” **drop-in Python script** that turns on essentially *everything* Hamilton gives you for observability in one place:

* **Lifecycle adapters:** node start/end timing, progress bars, type checking, graceful error continuation, optional PDB injection
* **Cache structured logs:** live JSONL + programmatic access to per-node cache events
* **Hamilton UI telemetry:** run tracking + per-node timing/metadata capture
* **Tracing:** optional Datadog tracer (span-per-node)
* **Lifecycle API hooks:** custom JSONL flight recorder for *graph + node events* (before/after hooks)
* **Hang diagnostics:** Python `faulthandler` watchdog dumps stack traces if the process stops making progress

Hamilton supports attaching adapters with `Builder.with_adapters(...)` and enabling cache logging with `Builder.with_cache(log_to_file=True)` (writes JSONL as events happen). ([Hamilton][1])
`PrintLn` provides node timing and supports `verbosity` and `node_filter`. ([Hamilton][2])
`FunctionInputOutputTypeChecker` is strict type enforcement. ([Hamilton][3])
`GracefulErrorAdapter` lets the graph proceed by injecting a sentinel value on failures. ([Hamilton][4])
`PDBDebugger` can break before/during/after selected nodes. ([Hamilton][5])
`RichProgressBar` provides progress bars. ([Hamilton][6])
Hamilton UI telemetry is enabled via `hamilton_sdk.adapters.HamiltonTracker` and can be tuned via env vars / constants. ([Hamilton][7])
Cache event logging can be streamed via logger `hamilton.caching`. ([Hamilton][8])

> **Note:** This is intentionally “maximal.” It will add overhead. Use `node_filter` to focus on suspected stages when narrowing a hang.

```python
"""
diagnostics_hamilton_max.py

Maximal logging + telemetry setup for Apache Hamilton runs.
- Works with a normal driver.Builder() workflow
- No changes required to your node functions
- Built-in Hamilton instrumentation + optional first-party plugins
"""

from __future__ import annotations

import faulthandler
import json
import logging
import logging.config
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

# --- Hamilton imports ---
from hamilton import driver
from hamilton.lifecycle import default as lifecycle_default
from hamilton.lifecycle.api import (
    GraphConstructionHook,
    GraphExecutionHook,
    NodeExecutionHook,
    TaskExecutionHook,
    TaskGroupingHook,
    TaskReturnHook,
    TaskSubmissionHook,
)

# Optional plugins (import guarded)
try:
    from hamilton.plugins.h_rich import RichProgressBar
except Exception:  # pragma: no cover
    RichProgressBar = None

try:
    from hamilton.plugins.h_tqdm import ProgressBar as TqdmProgressBar
except Exception:  # pragma: no cover
    TqdmProgressBar = None

try:
    from hamilton.plugins.h_ddog import DDOGTracer
except Exception:  # pragma: no cover
    DDOGTracer = None

# Optional Hamilton UI tracker (first-party SDK)
try:
    from hamilton_sdk import adapters as sdk_adapters
    from hamilton_sdk.tracking import constants as sdk_constants
except Exception:  # pragma: no cover
    sdk_adapters = None
    sdk_constants = None


# ----------------------------
# Logging configuration (JSONL + human console)
# ----------------------------

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.time(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {"()": JsonFormatter},
            "plain": {"format": "%(asctime)s %(levelname)s %(name)s - %(message)s"},
        },
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "plain", "level": "INFO"},
            "jsonl_file": {
                "class": "logging.FileHandler",
                "formatter": "json",
                "level": "DEBUG",
                "filename": str(out_dir / "app.log.jsonl"),
                "mode": "a",
                "encoding": "utf-8",
            },
        },
        "root": {"handlers": ["console", "jsonl_file"], "level": "DEBUG"},
        "loggers": {
            # High-signal Hamilton cache events (GET_RESULT / EXECUTE_NODE, etc.)
            "hamilton.caching": {"level": "INFO", "handlers": ["console", "jsonl_file"], "propagate": False},
            # If you want more internal Hamilton detail:
            "hamilton": {"level": "INFO", "handlers": ["console", "jsonl_file"], "propagate": False},
        },
    }
    logging.config.dictConfig(logging_config)


def enable_faulthandler(out_dir: Path, watchdog_seconds: int = 120) -> None:
    """
    Dumps stack traces if the process hangs and stops making progress.
    - dump_traceback_later repeats every watchdog_seconds
    - SIGUSR1 can be used to dump on demand (Unix)
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    fh_path = out_dir / "faulthandler_stacks.log"
    fh_file = fh_path.open("a", encoding="utf-8")

    faulthandler.enable(fh_file)
    faulthandler.dump_traceback_later(watchdog_seconds, repeat=True, file=fh_file)

    # On-demand stack dump (mac/linux): kill -USR1 <pid>
    if hasattr(signal, "SIGUSR1"):
        faulthandler.register(signal.SIGUSR1, file=fh_file, all_threads=True)


# ----------------------------
# Utility: cheap summaries for kwargs/results
# ----------------------------

def summarize_value(v: Any) -> Dict[str, Any]:
    try:
        import pyarrow as pa
    except Exception:
        pa = None

    out: Dict[str, Any] = {"type": type(v).__name__}

    if pa is not None:
        if isinstance(v, pa.Table):
            out.update({"rows": v.num_rows, "cols": v.num_columns})
            return out
        if isinstance(v, pa.RecordBatch):
            out.update({"rows": v.num_rows, "cols": v.num_columns})
            return out

    # Avoid consuming iterators/generators
    if isinstance(v, (str, bytes, bytearray)):
        out["len"] = len(v)
        return out
    if isinstance(v, (list, tuple, set, dict)):
        out["len"] = len(v)
        return out

    # Common dataframe shapes without importing heavy deps
    shape = getattr(v, "shape", None)
    if shape is not None:
        out["shape"] = tuple(shape)  # type: ignore[arg-type]
        return out

    return out


# ----------------------------
# Maximal lifecycle hook: JSONL flight recorder
# ----------------------------

class JSONLFlightRecorder(
    GraphConstructionHook,
    GraphExecutionHook,
    NodeExecutionHook,
    # These task hooks are most relevant in dynamic/task-based execution,
    # but harmless to include (you'll just see fewer events if not enabled).
    TaskSubmissionHook,
    TaskExecutionHook,
    TaskReturnHook,
    TaskGroupingHook,
):
    def __init__(
        self,
        out_path: Path,
        *,
        include_inputs: bool = True,
        include_results: bool = False,  # results can be huge; default off
        include_input_summaries: bool = True,
        include_result_summaries: bool = True,
        flush: bool = True,
    ) -> None:
        self.out_path = out_path
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.out_path.open("a", encoding="utf-8")
        self.flush = flush

        self.include_inputs = include_inputs
        self.include_results = include_results
        self.include_input_summaries = include_input_summaries
        self.include_result_summaries = include_result_summaries

        # (run_id, task_id, node_name) -> start_ts
        self._node_start: Dict[Tuple[str, Optional[str], str], float] = {}
        self._run_start: Dict[str, float] = {}

    def _emit(self, event: Dict[str, Any]) -> None:
        self._fp.write(json.dumps(event, ensure_ascii=False) + "\n")
        if self.flush:
            self._fp.flush()

    # --- Graph construction ---
    def run_after_graph_construction(self, *, graph, config: Dict[str, Any], **kwargs: Any) -> None:
        self._emit(
            {
                "event": "graph_constructed",
                "ts": time.time(),
                "graph_type": type(graph).__name__,
                # best-effort: node count if available
                "node_count": getattr(graph, "get_nodes", lambda: None)() and len(graph.get_nodes()),  # type: ignore[misc]
                "config_keys": sorted(list(config.keys())),
            }
        )

    # --- Graph execution ---
    def run_before_graph_execution(
        self,
        *,
        graph,
        final_vars: List[str],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
        execution_path,
        run_id: str,
        **kwargs: Any,
    ) -> None:
        self._run_start[run_id] = time.perf_counter()
        payload: Dict[str, Any] = {
            "event": "graph_start",
            "ts": time.time(),
            "run_id": run_id,
            "final_vars": list(final_vars),
            "execution_path_len": len(execution_path) if execution_path is not None else None,
            "inputs_keys": sorted(list(inputs.keys())),
            "overrides_keys": sorted(list(overrides.keys())),
        }
        if self.include_inputs and self.include_input_summaries:
            payload["inputs_summary"] = {k: summarize_value(v) for k, v in inputs.items()}
        self._emit(payload)

    def run_after_graph_execution(
        self,
        *,
        graph,
        success: bool,
        error: Exception | None,
        results: Dict[str, Any] | None,
        run_id: str,
        **kwargs: Any,
    ) -> None:
        elapsed = None
        if run_id in self._run_start:
            elapsed = time.perf_counter() - self._run_start[run_id]
        payload: Dict[str, Any] = {
            "event": "graph_end",
            "ts": time.time(),
            "run_id": run_id,
            "success": success,
            "elapsed_s": elapsed,
            "error": repr(error) if error else None,
        }
        if results is not None:
            payload["result_keys"] = sorted(list(results.keys()))
            if self.include_results and self.include_result_summaries:
                payload["results_summary"] = {k: summarize_value(v) for k, v in results.items()}
        self._emit(payload)

    # --- Node execution ---
    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        task_id: str | None,
        run_id: str,
        node_input_types: Dict[str, Any],
        **future_kwargs: Any,
    ) -> None:
        key = (run_id, task_id, node_name)
        self._node_start[key] = time.perf_counter()
        payload: Dict[str, Any] = {
            "event": "node_start",
            "ts": time.time(),
            "run_id": run_id,
            "task_id": task_id,
            "node": node_name,
            "tags": node_tags,
            "return_type": getattr(node_return_type, "__name__", str(node_return_type)),
            "thread_id": threading.get_ident(),
        }
        if self.include_inputs:
            payload["kwargs_keys"] = sorted(list(node_kwargs.keys()))
            if self.include_input_summaries:
                payload["kwargs_summary"] = {k: summarize_value(v) for k, v in node_kwargs.items()}
            # This is often very useful when debugging wrong-shape inputs
            payload["input_types"] = {k: str(t) for k, t in (node_input_types or {}).items()}
        self._emit(payload)

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        result: Any,
        error: Exception | None,
        success: bool,
        task_id: str | None,
        run_id: str,
        **future_kwargs: Any,
    ) -> None:
        key = (run_id, task_id, node_name)
        elapsed = None
        start = self._node_start.pop(key, None)
        if start is not None:
            elapsed = time.perf_counter() - start

        payload: Dict[str, Any] = {
            "event": "node_end",
            "ts": time.time(),
            "run_id": run_id,
            "task_id": task_id,
            "node": node_name,
            "success": success,
            "elapsed_s": elapsed,
            "error": repr(error) if error else None,
        }
        if self.include_results:
            if self.include_result_summaries:
                payload["result_summary"] = summarize_value(result)
            else:
                payload["result_type"] = type(result).__name__
        self._emit(payload)

    # --- Task hooks (dynamic execution only; included for completeness) ---
    def run_before_task_submission(self, *, task_id: str, run_id: str, nodes, **kwargs: Any) -> None:
        self._emit({"event": "task_submit", "ts": time.time(), "run_id": run_id, "task_id": task_id, "nodes": [n.name for n in nodes]})

    def run_before_task_execution(self, *, task_id: str, run_id: str, nodes, **kwargs: Any) -> None:
        self._emit({"event": "task_start", "ts": time.time(), "run_id": run_id, "task_id": task_id, "nodes": [n.name for n in nodes]})

    def run_after_task_execution(self, *, task_id: str, run_id: str, nodes, success: bool, error: Exception | None, **kwargs: Any) -> None:
        self._emit({"event": "task_end", "ts": time.time(), "run_id": run_id, "task_id": task_id, "success": success, "error": repr(error) if error else None})

    def run_after_task_return(self, *, task_id: str, run_id: str, success: bool, error: Exception | None, **kwargs: Any) -> None:
        self._emit({"event": "task_return", "ts": time.time(), "run_id": run_id, "task_id": task_id, "success": success, "error": repr(error) if error else None})

    def run_after_task_grouping(self, *, run_id: str, task_ids: List[str], **kwargs: Any) -> None:
        self._emit({"event": "task_grouped", "ts": time.time(), "run_id": run_id, "task_ids": task_ids})


# ----------------------------
# Driver builder with maximal instrumentation
# ----------------------------

@dataclass(frozen=True)
class MaxDiagConfig:
    out_dir: Path = Path("./_hamilton_diag")
    cache_dir: Path = Path("./.hamilton_cache")
    cache_log_to_file: bool = True

    # PrintLn is high-signal but can be noisy
    println_verbosity: int = 1
    println_node_filter: Callable[[str, Dict[str, Any]], bool] | List[str] | str | None = None

    # Type checking is strict and can be expensive on huge structures
    enable_type_checker: bool = True

    # Graceful errors is useful if you want “partial completion”
    enable_graceful_errors: bool = False

    # PDB breakpoints (set node_filter to a list of suspicious nodes)
    enable_pdb: bool = False
    pdb_node_filter: Callable[[str, Dict[str, Any]], bool] | List[str] | str | None = None

    # UI tracking (requires sf-hamilton[sdk] + UI running)
    enable_ui_tracker: bool = False
    ui_project_id: int | None = None
    ui_username: str | None = None
    ui_dag_name: str = "codeintel"
    ui_tags: Dict[str, str] | None = None

    # Tracing
    enable_ddog: bool = False
    ddog_root_name: str = "hamilton"
    ddog_include_causal_links: bool = False


def build_driver_max_diagnostics(
    *,
    modules: List[Any],
    config: Dict[str, Any],
    diag: MaxDiagConfig,
) -> driver.Driver:
    diag.out_dir.mkdir(parents=True, exist_ok=True)

    # Configure SDK capture knobs (if installed)
    if sdk_constants is not None:
        # “maximal capture” knobs (tune if too heavy)
        sdk_constants.CAPTURE_DATA_STATISTICS = True
        sdk_constants.MAX_LIST_LENGTH_CAPTURE = 500
        sdk_constants.MAX_DICT_LENGTH_CAPTURE = 500

    # Base lifecycle adapters
    adapters: List[Any] = []

    # 1) PrintLn
    adapters.append(
        lifecycle_default.PrintLn(
            verbosity=diag.println_verbosity,
            node_filter=diag.println_node_filter,
            print_fn=logging.getLogger("hamilton.println").info,
        )
    )

    # 2) Progress bar (prefer rich; fallback to tqdm)
    if RichProgressBar is not None:
        adapters.append(RichProgressBar(run_desc="Hamilton run", collect_desc="Collect"))
    elif TqdmProgressBar is not None:
        adapters.append(TqdmProgressBar())

    # 3) Strict type checker
    if diag.enable_type_checker:
        adapters.append(lifecycle_default.FunctionInputOutputTypeChecker(check_input=True, check_output=True))

    # 4) Graceful error adapter (optional)
    if diag.enable_graceful_errors:
        SENTINEL = object()
        adapters.append(
            lifecycle_default.GracefulErrorAdapter(
                error_to_catch=Exception,
                sentinel_value=SENTINEL,
                try_all_parallel=True,
            )
        )

    # 5) PDB debugger (optional)
    if diag.enable_pdb:
        adapters.append(
            lifecycle_default.PDBDebugger(
                node_filter=diag.pdb_node_filter,
                before=False,
                during=True,
                after=False,
            )
        )

    # 6) JSONL flight recorder (always on in “maximal” mode)
    adapters.append(
        JSONLFlightRecorder(
            diag.out_dir / "hamilton_flight.jsonl",
            include_inputs=True,
            include_results=False,          # flip to True if you can tolerate it
            include_input_summaries=True,
            include_result_summaries=True,
        )
    )

    # 7) Hamilton UI tracker (optional)
    if diag.enable_ui_tracker and sdk_adapters is not None:
        if diag.ui_project_id is None or diag.ui_username is None:
            raise ValueError("UI tracker enabled but ui_project_id/ui_username not set.")
        adapters.append(
            sdk_adapters.HamiltonTracker(
                project_id=diag.ui_project_id,
                username=diag.ui_username,
                dag_name=diag.ui_dag_name,
                tags=diag.ui_tags or {"env": "DEV"},
            )
        )

    # 8) Datadog tracer (optional)
    if diag.enable_ddog and DDOGTracer is not None:
        adapters.append(
            DDOGTracer(
                root_name=diag.ddog_root_name,
                include_causal_links=diag.ddog_include_causal_links,
                service=None,
            )
        )

    # Build driver with cache + adapters
    dr = (
        driver.Builder()
        .with_config(config)
        .with_modules(*modules)
        .with_cache(path=str(diag.cache_dir), log_to_file=diag.cache_log_to_file)
        .with_adapters(*adapters)
        .build()
    )
    return dr


# ----------------------------
# Example usage
# ----------------------------

def main() -> None:
    out_dir = Path("./_hamilton_diag")
    configure_logging(out_dir)
    enable_faulthandler(out_dir, watchdog_seconds=120)

    # Per Hamilton caching tutorial, this logger shows GET_RESULT / EXECUTE_NODE cache events.
    # (You can crank to DEBUG for more.)  See docs for the logger name.  :contentReference[oaicite:8]{index=8}
    logging.getLogger("hamilton.caching").setLevel(logging.INFO)

    # TODO: import your CodeIntel Hamilton function modules here:
    # from codeintel.build.hamilton.native import discovery
    # modules = discovery.native_modules()   # or your explicit list
    modules: List[Any] = []  # <-- replace

    # TODO: config for your run (build env, options, etc.)
    config: Dict[str, Any] = {}

    diag = MaxDiagConfig(
        out_dir=out_dir,
        cache_dir=Path("./.hamilton_cache"),
        cache_log_to_file=True,
        println_verbosity=1,             # set 2 for inputs/results (can be huge)
        println_node_filter=None,        # or ["syntax_augment__frames", ...]
        enable_type_checker=True,
        enable_graceful_errors=False,
        enable_pdb=False,               # set True + pdb_node_filter to break into a node
        enable_ui_tracker=False,         # set True if you have Hamilton UI + SDK configured
        ui_project_id=None,
        ui_username=None,
    )

    dr = build_driver_max_diagnostics(modules=modules, config=config, diag=diag)

    final_vars = ["graph.cpg_edges"]  # replace with your targets
    inputs: Dict[str, Any] = {}
    overrides: Dict[str, Any] = {}

    # Optional: visualize what will execute (useful to confirm execution_path)
    try:
        dr.visualize_execution(
            final_vars=final_vars,
            inputs=inputs,
            overrides=overrides,
            render_kwargs={"view": False},
            output_file_path=str(out_dir / "execution_path.png"),
        )
    except Exception:
        logging.getLogger("hamilton.diag").exception("visualize_execution failed (graphviz missing?)")

    # Run
    try:
        results = dr.execute(final_vars=final_vars, inputs=inputs, overrides=overrides)
        logging.getLogger("hamilton.diag").info("Run completed. result_keys=%s", list(results.keys()))
    except Exception:
        logging.getLogger("hamilton.diag").exception("Run failed")

    # Dump cache structured logs to file for postmortem
    try:
        cache_logs = dr.cache.logs(level="debug")  # shape depends on run_id usage
        (out_dir / "cache_logs.json").write_text(json.dumps(cache_logs, default=str, indent=2), encoding="utf-8")
        logging.getLogger("hamilton.diag").info("Wrote cache logs to %s", out_dir / "cache_logs.json")
    except Exception:
        logging.getLogger("hamilton.diag").exception("Failed to write cache logs")

    # Visualize cache hits (if possible)
    try:
        # dr.cache.view_run() creates a visualization with cached nodes highlighted. :contentReference[oaicite:9]{index=9}
        dr.cache.view_run(output_file_path=str(out_dir / "cache_view_run.png"))
    except Exception:
        logging.getLogger("hamilton.diag").exception("cache.view_run failed")


if __name__ == "__main__":
    main()
```

### How to use this effectively in your situation (hang/stall debugging)

* Start with:

  * `PrintLn(verbosity=1)` (already in the snippet) — you’ll see the last node that started but never ended. ([Hamilton][2])
  * `faulthandler.dump_traceback_later(120, repeat=True)` — you’ll get stack traces every 2 minutes if it’s stuck.
  * Cache JSONL logs (`with_cache(log_to_file=True)`) — you’ll get continuous cache event logs on disk. ([Hamilton][9])
* When you identify the suspicious node, set:

  * `diag.println_node_filter = ["that_node_name"]`
  * `diag.enable_pdb=True` + `diag.pdb_node_filter=["that_node_name"]` (to break inside it). ([Hamilton][5])

If you tell me **one concrete run target** you execute (e.g., `t__cpg` or `graph.cpg_edges`) and the **last printed node name** before the stall, I can suggest the smallest node-filtering and hook settings that usually pinpoint the exact cause in one run.

[1]: https://hamilton.apache.org/reference/drivers/Driver/ "Builder - Hamilton"
[2]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/PrintLn/ "lifecycle.PrintLn - Hamilton"
[3]: https://hamilton.apache.org/reference/lifecycle-hooks/FunctionInputOutputTypeChecker/?utm_source=chatgpt.com "lifecycle.FunctionInputOutputTypeChecker - Hamilton"
[4]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/GracefulErrorAdapter/ "lifecycle.GracefulErrorAdapter - Hamilton"
[5]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/PDBDebugger/ "lifecycle.PDBDebugger - Hamilton"
[6]: https://hamilton.staged.apache.org/reference/lifecycle-hooks/RichProgressBar/?utm_source=chatgpt.com "plugins.h_rich.RichProgressBar - Hamilton"
[7]: https://hamilton.apache.org/concepts/ui/ "UI Overview - Hamilton"
[8]: https://hamilton.apache.org/how-tos/caching-tutorial/ "Caching - Hamilton"
[9]: https://hamilton.apache.org/concepts/caching/ "Caching - Hamilton"
