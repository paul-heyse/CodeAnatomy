
# Apache Hamilton on a single machine: multi-CPU execution feature catalog

This is a **catalog-first inventory** of the ways Hamilton can exploit **multiple CPU cores on one box**, grouped into semantically “clean” execution sections (the same way your Hamilton reference docs cluster features).

---

## 0) The mental model (3 ways Hamilton can “use more cores”)

1. **Per-node delegation via a GraphAdapter (V1/legacy executor)**
   Hamilton still walks the DAG, but each node is submitted to an underlying concurrency system (threadpool, async, Ray, Dask, …). Hamilton calls this “using an adapter.” ([hamilton.apache.org][1])

2. **Task-based execution via `Parallelizable[T]` / `Collect[T]` (V2 executor / dynamic execution)**
   You explicitly mark a **map→reduce** region; Hamilton groups nodes into **tasks**, spawns dynamic tasks, and runs them in parallel via configurable executors. ([hamilton.apache.org][1])

3. **“Free parallelism” inside your node implementations**
   Even if Hamilton is single-threaded, you can still burn multiple cores if your nodes call native libraries that release the GIL / run their own thread pools (NumPy/BLAS, DuckDB, Polars, PyArrow, etc.). Hamilton’s own ThreadPool adapter explicitly calls out this distinction (CPU-heavy Python won’t scale with threads unless it releases the GIL). ([hamilton.apache.org][2])

---

## 1) Execution backbone choices (what actually changes “multi-core-ness”)

### 1.1 DefaultGraphExecutor (baseline)

* **Execution shape:** simple recursive DFS execution “in order, in memory”. ([hamilton.apache.org][3])
* **Parallelism:** only “limited parallelism through graph adapters” (i.e., you add a GraphAdapter). ([hamilton.apache.org][3])
* **Cannot** handle `Parallelizable[]/Collect[]` graphs. ([hamilton.apache.org][3])

### 1.2 TaskBasedGraphExecutor (dynamic/task-based)

* **Execution shape:** “groups nodes into tasks” + “parallel execution/dynamic spawning of nodes,” then executes “task by task” until completion. ([hamilton.apache.org][3])
* **Required for:** `Parallelizable/Collect` dynamic blocks. ([hamilton.apache.org][1])
* **How you get it:** only exposed via `Builder.enable_dynamic_execution(allow_experimental_mode=...)` (you’re not expected to instantiate it directly). ([hamilton.apache.org][3])

---

## 2) Approach A — GraphAdapter-based parallel execution (no `Parallelizable/Collect` needed)

Hamilton’s docs define this as: “farm out execution of each node/function” to a system that resolves futures. ([hamilton.apache.org][1])

### 2.1 `h_threadpool.FutureAdapter` (ThreadPoolExecutor per node)

**API surface**

* `driver.Builder().with_adapter(FutureAdapter(...)).build()` ([hamilton.apache.org][1])
* Constructor signature: `FutureAdapter(max_workers=None, thread_name_prefix='', result_builder=None)` ([hamilton.apache.org][2])

**When it uses multiple cores**

* Best for **I/O-bound** DAGs.
* For CPU-bound work, benefits are limited **unless your functions release the GIL**. (Hamilton calls this out explicitly.) ([hamilton.apache.org][2])

**Constraints**

* Does **not** support DAGs containing `Parallelizable` & `Collect` nodes. ([hamilton.apache.org][2])

**Key knobs**

* `max_workers`: your thread concurrency ceiling. ([hamilton.apache.org][2])

### 2.2 Other GraphAdapters you can run “locally”

The parallel execution concept page lists additional adapters like **Ray** and **Dask** adapters. ([hamilton.apache.org][1])
Even though they’re often used for clusters, they can also be used in **single-machine local mode** (Ray local; Dask LocalCluster), which can give you **process-based** parallelism (i.e., actually multiple CPU cores) at the cost of serialization. ([hamilton.apache.org][1])

> Practical stance: for **true CPU parallelism in Python**, you generally want **process-based** execution (multiprocessing / Ray / Dask), not threads. Python’s own docs note that `ProcessPoolExecutor` uses multiprocessing to sidestep the GIL but requires picklable objects (and has “**main** must be importable” constraints). ([Python documentation][4])

---

## 3) Approach B — Task-based “map/reduce” parallelism (`Parallelizable[T]` + `Collect[T]`)

Hamilton calls this “pluggable execution” and highlights:

* grouping nodes into “tasks”
* executing tasks in parallel with an executor of your choice ([hamilton.apache.org][1])

### 3.1 The type markers (the semantic contract)

* A function returning `Parallelizable[T]` declares “map over these items (possibly in parallel).” ([DAGWorks][5])
* A downstream parameter typed as `Collect[T]` declares the “reduce/collect” boundary. ([DAGWorks][5])

### 3.2 Turning it on: `enable_dynamic_execution(allow_experimental_mode=True)`

* Required for the driver to parse/execute `Parallelizable/Collect` graphs. ([hamilton.apache.org][6])
* Builder docs show the minimal pattern + note that “reasonable defaults are used for local and remote executors” if you don’t specify them. ([hamilton.apache.org][6])

### 3.3 Configuring “how many cores” via executors (Builder surface)

**Core Builder knobs (dynamic execution control plane)**

* `with_local_executor(local_executor: TaskExecutor)` ([hamilton.apache.org][3])
* `with_remote_executor(remote_executor: TaskExecutor)` ([hamilton.apache.org][3])
* `with_grouping_strategy(grouping_strategy: GroupingStrategy)` (how nodes are grouped into tasks) ([hamilton.apache.org][3])
* `with_execution_manager(execution_manager: ExecutionManager)` (assign executors to node groups; can’t be combined with local/remote executor setters) ([hamilton.apache.org][3])

**Documented single-machine example (process-based)**

* `.with_local_executor(executors.SynchronousLocalTaskExecutor())`
* `.with_remote_executor(executors.MultiProcessingExecutor(max_tasks=5))` ([hamilton.apache.org][6])

This is the “most direct” Hamilton-native way to exploit multiple CPU cores for CPU-bound work on one machine.

**Documented multi-thread option (still single machine)**

* Hamilton’s own comparison docs show using `.with_remote_executor(executors.MultiThreadingExecutor(5))` for parallel batch workloads. ([hamilton.apache.org][7])
  (Threads help most when tasks are I/O bound or GIL-releasing.)

### 3.4 Default behavior when you just flip the flag

A Hamilton blog example states: after enabling dynamic execution, “by default Hamilton will use multithreading to parallelize any work between a `Parallelizable` and `Collect`.” ([DAGWorks][5])

### 3.5 Running on Ray or Dask *locally* as “remote executor”

The same blog shows that swapping the remote executor to:

* `h_ray.RayTaskExecutor()` (after `ray.init()`) ([DAGWorks][5])
* `h_dask.DaskExecutor(client=distributed.Client(LocalCluster()))` ([DAGWorks][5])

…lets you keep the exact same DAG logic while changing the execution engine (and both can run on one machine). ([DAGWorks][5])

### 3.6 Known caveats (important for a “multi-core on one box” setup)

From the official dynamic execution doc:

* **No nested `Parallelizable/Collect` blocks** (one level of parallelization). ([hamilton.apache.org][1])
* **Multiprocessing serialization is suboptimal** (uses default pickle today; may break for some objects; they mention plans for joblib/cloudpickle). ([hamilton.apache.org][1])
* **`Collect[]` inputs limited to one per function** (current limitation). ([hamilton.apache.org][1])

And from the blog (Ray/Dask caveats):

* inputs/outputs must be serializable; parallelism is bounded by the local Ray/Dask “cluster” config; collect can create memory pressure because it brings results back together. ([DAGWorks][5])

---

## 4) Execution customization & observability (the “parallelism ergonomics” layer)

These don’t create cores, but they’re how you *operate* multi-core runs without guessing.

### 4.1 Lifecycle hooks for task-based execution (dynamic mode)

Example: `TaskGroupingHook` exists specifically to capture information after:

* task expansion (post `Parallelizable` expansion) and
* task grouping (group of tasks created). ([hamilton.apache.org][8])

This is the seam where you add:

* per-task telemetry,
* queue depth / backpressure signals,
* structured logging (e.g., “task_id -> inputs fingerprint”), etc.

### 4.2 UI for profiling parallel runs (optional, but very practical)

Hamilton’s open-source UI advertises:

* telemetry (run history + run data),
* feature/artifact catalog,
* DAG visualizer/lineage,
* project explorer + DAG versions. ([hamilton.apache.org][9])

For multi-core execution, the win is: you can quickly answer “did my parallelism help?” and “which nodes/tasks dominate wall time?”

---

## 5) “Single machine” decision guide (what to choose)

* **CPU-bound pure Python nodes:**
  Prefer **task-based dynamic execution + `MultiProcessingExecutor`** (or **Ray/Dask locally**) to bypass the GIL. ([hamilton.apache.org][6])

* **I/O-bound nodes (APIs, DB reads, network):**
  `FutureAdapter(max_workers=...)` (GraphAdapter approach) or dynamic execution with multi-threading can be simpler and avoids multiprocess serialization issues. ([hamilton.apache.org][2])

* **Mixed workloads / “map reduce” natural shape:**
  Use `Parallelizable/Collect` so you can cleanly push the “map” region into whichever executor (multiprocess, Ray, Dask) you want. ([hamilton.apache.org][1])

---

## 6) Project reference you already have

Your existing internal Hamilton “advanced decorator / builder / execution” reference is a good complement for the non-parallel surfaces (decorators, modifiers, etc.). 

---

If you want the next increment (like your other catalogs): I can expand **each cluster above** into “deep dive pages” with (a) minimal runnable code templates for each execution mode, (b) a “serialization-safe dependency injection” pattern for multiprocess/Ray/Dask, and (c) a micro-benchmark harness that reports speedup vs worker count (so you can empirically pick `max_tasks/max_workers`).

[1]: https://hamilton.apache.org/concepts/parallel-task/ "Dynamic DAGs/Parallel Execution - Hamilton"
[2]: https://hamilton.apache.org/reference/graph-adapters/ThreadPoolFutureAdapter/ "h_threadpool.FutureAdapter - Hamilton"
[3]: https://hamilton.apache.org/reference/drivers/Driver/ "Builder - Hamilton"
[4]: https://docs.python.org/3/library/concurrent.futures.html?utm_source=chatgpt.com "concurrent.futures — Launching parallel tasks — Python 3.14 ..."
[5]: https://blog.dagworks.io/p/rag-ingestion-and-chunking-using "RAG: ingestion and chunking using Hamilton and scaling to Ray, Dask, or PySpark "
[6]: https://hamilton.apache.org/concepts/builder/ "Builder - Hamilton"
[7]: https://hamilton.apache.org/code-comparisons/langchain/?utm_source=chatgpt.com "LangChain - Hamilton - Apache Software Foundation"
[8]: https://hamilton.apache.org/reference/lifecycle-hooks/TaskGroupingHook/ "lifecycle.api.TaskGroupingHook - Hamilton"
[9]: https://hamilton.apache.org/concepts/ui/ "UI Overview - Hamilton"

Below are the “deep dive pages” for **single-machine multi-CPU** Hamilton execution, with:

* **(a)** minimal runnable code templates (per mode),
* **(b)** a **serialization-safe DI pattern** for multiprocess/Ray/Dask,
* **(c)** a **micro-benchmark harness** to empirically pick `max_tasks` / `max_workers`.

---

# Deep dive 1 — GraphAdapter threading: `h_threadpool.FutureAdapter`

### What it is

A GraphAdapter that **submits each node** to a `ThreadPoolExecutor` as Hamilton walks the DAG. It avoids serialization overhead (same process), and is best for **I/O-bound** graphs. It’s less useful for CPU-heavy pure Python unless code releases the GIL. It also does **not** support `Parallelizable/Collect`. ([Hamilton][1])

### Minimal template (two parallel branches)

**`flow_io_demo.py`**

```python
from __future__ import annotations
import time

def sleep_s() -> float:
    return 0.25

def fetch_a(sleep_s: float) -> str:
    time.sleep(sleep_s)  # simulate I/O
    return "A"

def fetch_b(sleep_s: float) -> str:
    time.sleep(sleep_s)  # simulate I/O
    return "B"

def combined(fetch_a: str, fetch_b: str) -> str:
    return fetch_a + fetch_b
```

**`run_futureadapter.py`**

```python
from __future__ import annotations
import time

from hamilton import driver
from hamilton.plugins.h_threadpool import FutureAdapter

import flow_io_demo as flow

def main() -> None:
    dr = (
        driver.Builder()
        .with_modules(flow)
        .with_adapter(FutureAdapter(max_workers=32))  # knob: threadpool size
        .build()
    )

    t0 = time.perf_counter()
    out = dr.execute(["combined"], inputs={})
    dt = time.perf_counter() - t0
    print(out["combined"], f"{dt:.3f}s")

if __name__ == "__main__":
    main()
```

### Power knobs & failure modes

* **Knob:** `FutureAdapter(max_workers=...)` controls max threads. ([Hamilton][1])
* **CPU bound:** may not speed up unless your work releases the GIL. ([Hamilton][1])
* **Not compatible with `Parallelizable/Collect`:** explicitly unsupported. ([Hamilton][1])

---

# Deep dive 2 — GraphAdapter process backends: `RayGraphAdapter` / `DaskGraphAdapter`

These still follow the “submit each node” adapter model (Hamilton walks the DAG), but now **node outputs cross process boundaries**, so serialization costs matter. The parallel execution concept page calls this out directly. ([Hamilton][2])

## 2A) `h_dask.DaskGraphAdapter` (single machine multi-core via Dask)

DaskGraphAdapter is explicitly positioned for **multi-core on a single machine** (and also clusters), and exposes knobs like `use_delayed` / `compute_at_end` with explicit warnings that serialization costs can outweigh benefits when using `delayed`. ([Hamilton][3])

**`run_dask_graphadapter.py`**

```python
from __future__ import annotations

from dask.distributed import Client, LocalCluster
from hamilton import driver
from hamilton.plugins.h_dask import DaskGraphAdapter

import flow_io_demo as flow

def main() -> None:
    cluster = LocalCluster(n_workers=4, threads_per_worker=1)
    client = Client(cluster)

    adapter = DaskGraphAdapter(
        client,
        use_delayed=True,     # wraps functions with dask.delayed (may add serialization cost)
        compute_at_end=True,  # calls compute() in result building
    )

    dr = (
        driver.Builder()
        .with_modules(flow)
        .with_adapter(adapter)
        .build()
    )
    out = dr.execute(["combined"], inputs={})
    print(out["combined"])

    client.shutdown()
    cluster.close()

if __name__ == "__main__":
    main()
```

## 2B) `h_ray.RayGraphAdapter` (single machine multi-core via Ray)

RayGraphAdapter explicitly targets “multiple cores on a single machine,” but warns that **serialization costs** can outweigh benefits, and that you are limited by machine memory. ([hamilton.staged.apache.org][4])
It requires a **result builder** (e.g., `base.DictResult`) to shape `execute()` output. ([hamilton.staged.apache.org][4])

**`run_ray_graphadapter.py`**

```python
from __future__ import annotations

from hamilton import base, driver
from hamilton.plugins.h_ray import RayGraphAdapter

import flow_io_demo as flow

def main() -> None:
    adapter = RayGraphAdapter(
        result_builder=base.DictResult(),
        ray_init_config={"num_cpus": 8},
        shutdown_ray_on_completion=True,
    )

    dr = (
        driver.Builder()
        .with_modules(flow)
        .with_adapter(adapter)
        .build()
    )
    out = dr.execute(["combined"], inputs={})
    print(out["combined"])

if __name__ == "__main__":
    main()
```

---

# Deep dive 3 — Task-based dynamic execution: `Parallelizable[T]` + `Collect[T]`

### What it is

Hamilton’s **V2 / task-based executor** groups nodes into “tasks” and can execute them in parallel; you enable this by calling `enable_dynamic_execution(allow_experimental_mode=True)`. ([Hamilton][5])

Default grouping behavior (important!):

* Nodes in the **map block** between `Parallelizable` and `Collect` are grouped and repeated per item, and run on the **remote executor**. ([Hamilton][2])

### Minimal “map → reduce” DAG

**`flow_dynamic_demo.py`**

```python
from __future__ import annotations

import time
from hamilton.htypes import Parallelizable, Collect

def n_items() -> int:
    return 100

def kind() -> str:
    # "io" or "cpu"
    return "io"

def sleep_s() -> float:
    return 0.01

def cpu_iters() -> int:
    return 200_000

def items(n_items: int) -> Parallelizable[int]:
    for i in range(n_items):
        yield i

def work(items: int, kind: str, sleep_s: float, cpu_iters: int) -> int:
    if kind == "io":
        time.sleep(sleep_s)
        return items
    if kind == "cpu":
        x = 0
        for j in range(cpu_iters):
            x = (x + (items + j) * (items + j)) % 1_000_000_007
        return x
    raise ValueError(f"Unknown kind={kind!r}")

def total(work: Collect[int]) -> int:
    return sum(work)
```

### Template A: Multi-thread remote executor (good for I/O)

Hamilton’s examples show setting a `MultiThreadingExecutor(max_tasks=...)` as the remote executor. ([hub.dagworks.io][6])

**`run_dynamic_threads.py`**

```python
from __future__ import annotations

from hamilton import driver
from hamilton.execution import executors

import flow_dynamic_demo as flow

def main() -> None:
    dr = (
        driver.Builder()
        .with_modules(flow)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .with_remote_executor(executors.MultiThreadingExecutor(max_tasks=32))
        .build()
    )
    out = dr.execute(["total"], inputs={})
    print(out["total"])

if __name__ == "__main__":
    main()
```

### Template B: Multi-process remote executor (good for CPU)

Hamilton’s docs show using `MultiProcessingExecutor(max_tasks=...)`. ([Hamilton][7])
Process-based parallelism is how you actually “fully leverage multiple processors” in Python (it side-steps the GIL) — but it requires serialization. ([Python documentation][8])

**`run_dynamic_processes.py`**

```python
from __future__ import annotations

from hamilton import driver
from hamilton.execution import executors

import flow_dynamic_demo as flow

def main() -> None:
    dr = (
        driver.Builder()
        .with_modules(flow)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=8))
        .build()
    )
    out = dr.execute(["total"], inputs={})
    print(out["total"])

if __name__ == "__main__":
    main()
```

### Key caveats to design around

Hamilton lists these explicitly:

* **No nested `Parallelizable/Collect` blocks** (one level)
* **Multiprocessing serialization uses default pickle today** (can break for some objects); Ray/Dask are noted as working well; joblib/cloudpickle support planned
* **Only one `Collect[]` input per function** ([Hamilton][2])

---

# Deep dive 4 — Dynamic execution + Ray/Dask task executors (still single machine)

If you’re using `Parallelizable/Collect`, you can swap the **remote executor** to Ray/Dask with essentially “one line” in Hamilton’s examples. ([DAGWorks][9])
They also call out the core caveats: serialization, concurrency is limited by cluster config (both can run locally), and the collect step can cause memory pressure. ([DAGWorks][9])

## 4A) RayTaskExecutor (local Ray)

```python
from __future__ import annotations

import ray
from hamilton import driver
from hamilton.plugins import h_ray

import flow_dynamic_demo as flow

def main() -> None:
    ray.init()  # local ray runtime

    dr = (
        driver.Builder()
        .with_modules(flow)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_config({})
        .with_remote_executor(h_ray.RayTaskExecutor())
        .build()
    )
    out = dr.execute(["total"], inputs={})
    print(out["total"])

    ray.shutdown()

if __name__ == "__main__":
    main()
```

## 4B) DaskExecutor (local Dask)

```python
from __future__ import annotations

from dask.distributed import Client, LocalCluster
from hamilton import driver
from hamilton.plugins import h_dask

import flow_dynamic_demo as flow

def main() -> None:
    cluster = LocalCluster()
    client = Client(cluster)

    dr = (
        driver.Builder()
        .with_modules(flow)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_config({})
        .with_remote_executor(h_dask.DaskExecutor(client=client))
        .build()
    )
    out = dr.execute(["total"], inputs={})
    print(out["total"])

    client.shutdown()

if __name__ == "__main__":
    main()
```

---

# Deep dive 5 — Serialization-safe dependency injection (multiprocess/Ray/Dask)

Hamilton’s dynamic execution docs describe the problem and two concrete solutions:

* inputs/outputs to remote execution are pickled/unpickled (can break for DB/OpenAI clients),
* **instantiate the object inside each parallel block** by making it depend on something inside the block,
* or implement a wrapper using `__get_state__` / `__set_state__`. ([Hamilton][2])

Below are “best practice” patterns that map cleanly to both **MultiProcessingExecutor** and **Ray/Dask**.

## Pattern 1 (default): “Config in, client inside, data out”

**Rule:** only pass **serializable config** across the boundary; create the unpicklable client inside the remote task; return only serializable outputs.

```python
from __future__ import annotations
from dataclasses import dataclass
import os
from hamilton.htypes import Parallelizable, Collect

@dataclass(frozen=True)
class ApiConfig:
    base_url: str
    api_key_env: str = "API_KEY"  # store env var name, not secret

def api_config() -> ApiConfig:
    return ApiConfig(base_url="https://example.invalid")

def items() -> Parallelizable[str]:
    yield from ["a", "b", "c"]

def fetch_one(items: str, api_config: ApiConfig) -> dict:
    # instantiate inside the parallel block
    token = os.environ[api_config.api_key_env]
    # client = SomeClient(api_config.base_url, token)  # unpicklable OK here
    # return client.fetch(items)
    return {"item": items, "ok": True}  # must be serializable

def all_results(fetch_one: Collect[dict]) -> list[dict]:
    return list(fetch_one)
```

Why this works in Hamilton:

* the **map block** between `Parallelizable` and `Collect` is what gets repeated and executed on the remote executor. ([Hamilton][2])

## Pattern 2: “Worker-local cache” to avoid re-creating clients per task

Use a module-level cache (`lru_cache`) so each **worker process** builds the client once.

```python
from __future__ import annotations
from functools import lru_cache

@lru_cache(maxsize=None)
def _client_for(base_url: str, api_key_env: str):
    import os
    token = os.environ[api_key_env]
    # return SomeClient(base_url, token)
    return (base_url, token)  # placeholder

def fetch_one(items: str, api_config) -> dict:
    client = _client_for(api_config.base_url, api_config.api_key_env)
    # use client...
    return {"item": items, "client_base": client[0]}
```

## Pattern 3: “Picklable wrapper” via `__getstate__`/`__setstate__`

Only use this if you *must* pass an object through boundaries (often you don’t). Hamilton mentions this as an option. ([Hamilton][2])

```python
from __future__ import annotations
from dataclasses import dataclass

@dataclass
class ClientWrapper:
    base_url: str
    api_key_env: str
    _client: object | None = None

    def _ensure(self) -> object:
        if self._client is None:
            import os
            token = os.environ[self.api_key_env]
            # self._client = SomeClient(self.base_url, token)
            self._client = (self.base_url, token)  # placeholder
        return self._client

    def __getstate__(self):
        # Keep it minimal & safe — don’t serialize raw secrets.
        return {"base_url": self.base_url, "api_key_env": self.api_key_env}

    def __setstate__(self, state):
        self.base_url = state["base_url"]
        self.api_key_env = state["api_key_env"]
        self._client = None
```

---

# Deep dive 6 — Micro-benchmark harness (speedup vs worker count)

This harness benchmarks the **dynamic executor** because it maps cleanly to:

* threads (`MultiThreadingExecutor`),
* processes (`MultiProcessingExecutor`),
* optional Ray/Dask task executors.

It prints a CSV so you can quickly see the knee of the curve.

## 6A) Dataflow (reuse the earlier DAG)

Use `flow_dynamic_demo.py` exactly as in Deep dive 3.

## 6B) Runner: vary worker counts + measure wall time

**`bench_parallel.py`**

```python
from __future__ import annotations

import argparse
import statistics
import time
from typing import Iterable

from hamilton import driver
from hamilton.execution import executors

import flow_dynamic_demo as flow


def _build_driver(mode: str, workers: int):
    b = (
        driver.Builder()
        .with_modules(flow)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
    )

    if mode == "sync":
        # run map tasks synchronously (baseline)
        remote = executors.SynchronousLocalTaskExecutor()
    elif mode == "threads":
        remote = executors.MultiThreadingExecutor(max_tasks=workers)
    elif mode == "processes":
        remote = executors.MultiProcessingExecutor(max_tasks=workers)
    else:
        raise ValueError(f"Unknown mode={mode!r} (use sync|threads|processes)")

    return b.with_remote_executor(remote).build()


def _run_once(dr, *, kind: str, n_items: int, cpu_iters: int, sleep_s: float) -> float:
    t0 = time.perf_counter()
    dr.execute(
        ["total"],
        inputs={
            "kind": kind,
            "n_items": n_items,
            "cpu_iters": cpu_iters,
            "sleep_s": sleep_s,
        },
    )
    return time.perf_counter() - t0


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["sync", "threads", "processes"], default="threads")
    p.add_argument("--kind", choices=["io", "cpu"], default="io")
    p.add_argument("--n-items", type=int, default=200)
    p.add_argument("--cpu-iters", type=int, default=200_000)
    p.add_argument("--sleep-s", type=float, default=0.01)
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--workers", type=str, default="1,2,4,8,16,32")
    args = p.parse_args(argv)

    workers_list = [int(x.strip()) for x in args.workers.split(",") if x.strip()]

    print("mode,kind,workers,median_s,min_s,max_s")  # CSV header
    for w in workers_list:
        dr = _build_driver(args.mode, w)
        dts = [
            _run_once(
                dr,
                kind=args.kind,
                n_items=args.n_items,
                cpu_iters=args.cpu_iters,
                sleep_s=args.sleep_s,
            )
            for _ in range(args.repeats)
        ]
        print(
            f"{args.mode},{args.kind},{w},"
            f"{statistics.median(dts):.6f},{min(dts):.6f},{max(dts):.6f}"
        )


if __name__ == "__main__":
    # required for multiprocess on many platforms
    main()
```

## Interpreting results (what you should expect)

* For `--kind cpu`, **threads often flatten** (GIL), while **processes scale** until serialization + overhead dominate. Python’s multiprocessing is explicitly process-based and “side-steps the GIL.” ([Python documentation][8])
* For `--kind io`, threads should scale well; Hamilton’s FutureAdapter docs recommend threads for I/O-bound work. ([Hamilton][1])
* If multiprocess runs fail with pickling errors, apply the DI patterns in Deep dive 5; Hamilton documents that multiprocessing serialization is currently default pickle and can break for certain clients. ([Hamilton][2])

---


[1]: https://hamilton.apache.org/reference/graph-adapters/ThreadPoolFutureAdapter/ "h_threadpool.FutureAdapter - Hamilton"
[2]: https://hamilton.apache.org/concepts/parallel-task/ "Dynamic DAGs/Parallel Execution - Hamilton"
[3]: https://hamilton.apache.org/reference/graph-adapters/DaskGraphAdapter/ "h_dask.DaskGraphAdapter - Hamilton"
[4]: https://hamilton.staged.apache.org/reference/graph-adapters/RayGraphAdapter/ "h_ray.RayGraphAdapter - Hamilton"
[5]: https://hamilton.apache.org/reference/drivers/Driver/ "Builder - Hamilton"
[6]: https://hub.dagworks.io/docs/Users/elijahbenizzy/parallel_load_dataframes_s3/ "parallel_load_dataframes_s3 | Hamilton Dataflow Hub"
[7]: https://hamilton.apache.org/concepts/builder/ "Builder - Hamilton"
[8]: https://docs.python.org/3/library/multiprocessing.html?utm_source=chatgpt.com "multiprocessing — Process-based parallelism — Python 3.14.2 ..."
[9]: https://blog.dagworks.io/p/rag-ingestion-and-chunking-using "RAG: ingestion and chunking using Hamilton and scaling to Ray, Dask, or PySpark "

Below is a **deep dive on how Apache Hamilton uses Dask**, focusing on the *actual integration surfaces*, their knobs, and how to choose between them.

---

## 1) Where Dask “plugs in” to Hamilton

Hamilton supports **two parallelization approaches**, and Dask shows up in **both**:

1. **GraphAdapter approach**: Hamilton walks the DAG and submits each node to an adapter; Dask is one of those adapters (`h_dask.DaskGraphAdapter`). This is the “don’t change my functions” route. ([Hamilton][1])

2. **Task-based dynamic execution**: you mark a map/reduce boundary with `Parallelizable[]` and `Collect[]`, and delegate execution of the *map tasks* to an executor. In the ecosystem, Dask can be used here via `h_dask.DaskExecutor` (shown in DAGWorks examples). ([Hamilton][1])

If your goal is “**use Dask on one machine across cores**” *or* “**scale out to a Dask cluster**”, Hamilton supports both patterns. ([Hamilton][2])

---

## 2) Integration A: `h_dask.DaskGraphAdapter` (run the entire DAG on Dask)

### What it does

`DaskGraphAdapter` “runs the entire Hamilton DAG on dask” by walking the Hamilton graph and translating it onto Dask. ([Hamilton][2])

Hamilton explicitly positions this adapter for:

* **multi-core on a single machine**
* **distributed execution on a Dask cluster**
* scaling to “any size of data supported by Dask” (assuming you load via Dask loaders) ([Hamilton][2])

It also notes it “works best with Pandas 2.0+ and pyarrow backend.” ([Hamilton][2])

### Installation

Hamilton’s DaskGraphAdapter page calls out:

* `pip install sf-hamilton[dask]` ([Hamilton][2])

### The constructor surface (the knobs that matter)

Signature (as documented):

````python
DaskGraphAdapter(
    dask_client: Client,
    result_builder: ResultMixin | None = None,
    visualize_kwargs: dict | None = None,
    use_delayed: bool = True,
    compute_at_end: bool = True,
)
``` :contentReference[oaicite:7]{index=7}

Key parameters:

- **`use_delayed`** (default `True`): wraps every function with `dask.delayed`. Hamilton warns you *generally don’t need this* if you’re already operating on native Dask collections (DataFrame/Series/etc.) because those are already lazy; “use delayed if you want to farm out computation.” :contentReference[oaicite:8]{index=8}

- **`compute_at_end`** (default `True`): whether Hamilton calls `.compute()` in the result builder “to kick off computation.” :contentReference[oaicite:9]{index=9}

- **`visualize_kwargs`**: if provided, Hamilton can visualize the Dask task graph via Dask’s visualization hooks. :contentReference[oaicite:10]{index=10}

- **`result_builder`**: controls how the output is shaped; can be a Dask-specific result builder. :contentReference[oaicite:11]{index=11}

### “Mode selection”: how to pick `use_delayed` / `compute_at_end`
Hamilton documents both *recommended uses* and *behavior*:

- If `use_delayed=True`, Hamilton warns “serialization costs can outweigh the benefits of parallelism,” so you should benchmark; it also says it may “naively wrap all your functions with delayed” which is best when computations are slow or the DAG is highly parallelizable. :contentReference[oaicite:12]{index=12}

- `build_result()` behavior depends on the combination of `use_delayed` and `compute_at_end` (Hamilton documents the cases explicitly). :contentReference[oaicite:13]{index=13}

Practical mapping:

**A) Generic Python object DAG (you want Dask to schedule node execution)**
- `use_delayed=True`
- `compute_at_end=True` (typical)  
Hamilton wraps nodes with delayed and computes at the end. :contentReference[oaicite:14]{index=14}

**B) You want a lazy handle back (to embed the Hamilton DAG into a bigger Dask graph)**
- `use_delayed=True`
- `compute_at_end=False`  
This yields delayed objects rather than pulling results into the driver process. :contentReference[oaicite:15]{index=15}

**C) You’re using native Dask collections (Dask DataFrame/Series/Array)**
- `use_delayed=False` (Hamilton says delayed usually isn’t needed here)
- `compute_at_end=False` if you want to return the Dask collection; compute later yourself. :contentReference[oaicite:16]{index=16}

### Minimal runnable template (LocalCluster on one machine)
Dask’s docs: `Client()` with no args creates a local scheduler + workers; `Client()` is also shorthand for creating a `LocalCluster()`. :contentReference[oaicite:17]{index=17}

```python
from dask.distributed import Client, LocalCluster
from hamilton import driver
from hamilton.plugins.h_dask import DaskGraphAdapter

import my_flow  # your Hamilton module

cluster = LocalCluster(n_workers=4, threads_per_worker=1, processes=True)
client = Client(cluster)

adapter = DaskGraphAdapter(
    client,
    use_delayed=True,
    compute_at_end=True,
)

dr = (
    driver.Builder()
    .with_modules(my_flow)
    .with_adapter(adapter)
    .build()
)

out = dr.execute(["some_output"], inputs={})
````

Notes for the cluster:

* `Client()` sets up workers/threads based on machine cores; `processes=False` can be preferable when computations release the GIL (e.g., heavy NumPy/Dask array). ([Dask][3])
* The Dask dashboard is typically at `http://localhost:8787/status` when running locally (per Dask docs). ([Dask][3])

### Data loading: don’t “pandas read then dask later”

Hamilton’s DaskGraphAdapter docs explicitly say scaling depends on loading data via Dask loaders and recommends encapsulating loaders in a module. ([Hamilton][2])

---

## 3) Result shaping: `h_dask.DaskDataFrameResult` (Dask result builder)

Hamilton includes a Dask result builder:

* `hamilton.plugins.h_dask.DaskDataFrameResult.build_result(**outputs)` builds a **Dask DataFrame** from requested outputs. ([Hamilton][4])

Assumptions called out:

1. output order mirrors join order
2. it “massages types into dask types where it can”
3. scalars/objects are duplicated using an indexed template; assumes a **single partition** ([Hamilton][4])

### When you use it

* You’re using Hamilton in a “feature-column” style and want a single Dask DataFrame output assembled from multiple nodes/columns.

### Template: “return a Dask DataFrame as the Hamilton output”

Combine it with the adapter and choose whether to compute.

* If you want **lazy Dask DataFrame output**:

  * `use_delayed=False`, `compute_at_end=False` (Hamilton recommends this pairing with `DaskDataFrameResult` for producing a Dask DataFrame you can later convert). ([Hamilton][2])

```python
from dask.distributed import Client, LocalCluster
from hamilton import driver
from hamilton.plugins import h_dask

import features_flow  # functions that return series/columns, etc.

cluster = LocalCluster()
client = Client(cluster)

adapter = h_dask.DaskGraphAdapter(
    client,
    result_builder=h_dask.DaskDataFrameResult(),
    use_delayed=False,
    compute_at_end=False,  # return a dask dataframe
)

dr = driver.Builder().with_modules(features_flow).with_adapter(adapter).build()
ddf = dr.execute(["col_a", "col_b"], inputs={})  # shaped into a dask dataframe by result_builder
```

---

## 4) Integration B: Dynamic execution + `h_dask.DaskExecutor` (Parallelizable/Collect)

### What it is

Hamilton’s **task-based dynamic execution** uses `Parallelizable[]` and `Collect[]` to define a parallel map/reduce region, and then you supply an executor to run the parallel tasks. ([Hamilton][1])

A canonical example for Dask (from DAGWorks) is:

* start a local Dask cluster (`LocalCluster`, `Client`)
* pass `h_dask.DaskExecutor(client=client)` as the **remote executor**
* enable dynamic execution with `enable_dynamic_execution(allow_experimental_mode=True)` ([DAGWorks][5])

### Minimal template (single machine)

```python
from dask import distributed
from hamilton import driver
from hamilton.plugins import h_dask

import my_parallel_flow  # includes Parallelizable/Collect nodes

cluster = distributed.LocalCluster()
client = distributed.Client(cluster)

dr = (
    driver.Builder()
    .with_modules(my_parallel_flow)
    .enable_dynamic_execution(allow_experimental_mode=True)
    .with_config({})
    .with_remote_executor(h_dask.DaskExecutor(client=client))
    .build()
)

result = dr.execute(["final_collected_output"], inputs={...})
client.shutdown()
```

([DAGWorks][5])

### The caveats you must design for

The same example calls out the big operational realities:

1. **Serialization**: inputs/outputs across the parallelized region must be serializable. ([DAGWorks][5])
2. **Concurrency** depends on the Dask cluster configuration (local is fine). ([DAGWorks][5])
3. **Memory at collect**: the “collect/reduce” step brings outputs into memory; if you hit OOM, persist artifacts in workers and return pointers/paths. ([DAGWorks][5])

---

## 5) Choosing between DaskGraphAdapter vs DaskExecutor

### Use `DaskGraphAdapter` when…

* You want to keep the DAG “normal” (no `Parallelizable/Collect` refactor).
* You want “node-level scheduling”: Hamilton submits each node to Dask (often a lot of tasks).
* You want the option to wrap everything in `dask.delayed` (and accept its overhead). ([Hamilton][1])

### Use dynamic execution + `DaskExecutor` when…

* Your workload naturally has a **map/reduce** or “per-item processing” shape.
* You want Hamilton to group the map region into tasks and then farm those tasks out (often fewer, coarser tasks than per-node delayed wrapping).
* You’re already using `Parallelizable/Collect` for scale. ([Hamilton][1])

---

## 6) Single-machine tuning that matters for Hamilton workloads

Dask docs give you the core levers:

### 6.1 Threads vs processes

* `Client()` creates a local scheduler + workers/threads based on CPU cores. ([Dask][3])
* `Client(processes=False)` runs workers in threads (sometimes preferable if computations release the GIL, like NumPy/Dask array). ([Dask][3])
* `LocalCluster(n_workers=…, threads_per_worker=…, processes=…)` gives explicit control. ([Dask][3])

**Hamilton implication**

* If Hamilton nodes are **pure Python CPU-bound**, you generally want **processes=True** (avoid GIL).
* If nodes are **NumPy/Arrow/DuckDB/Polars heavy**, threads can work well (many operations release the GIL).

### 6.2 Dashboard-driven sanity checking

Dask points you to the dashboard at `http://localhost:8787/status` for local clusters. ([Dask][3])
For Hamilton-on-Dask, it’s the fastest way to answer:

* “Am I actually parallel?”
* “Do I have 10k tiny tasks (overhead)?”
* “Is collect pulling too much into memory?”

---

## 7) Practical “gotchas” specific to Hamilton+Dask

### 7.1 `use_delayed=True` can create “too many tasks”

Hamilton explicitly warns that with `use_delayed=True` it can wrap all functions with delayed, and serialization overhead can outweigh parallelism unless tasks are sufficiently heavy. ([Hamilton][2])

**Rule of thumb**

* If your DAG has *many small nodes*, prefer:

  * native Dask DataFrame-style computation (`use_delayed=False`), or
  * dynamic execution grouping + DaskExecutor (coarser tasks).

### 7.2 Don’t wrap Dask DataFrames in delayed unless you mean to

Hamilton states it’s “probably not necessary” to mix delayed with Dask collections because they’re already lazy; you use delayed to farm out computation. ([Hamilton][2])

### 7.3 “Pandas code mostly works” but loader choice is the real pivot

Hamilton (and the Hamilton paper) both make the point that Dask implements a subset of the Pandas API, so many transformations may not need changes—*but loading data via Dask is the key to scaling*. ([Hamilton][2])

---

## 8) A “best practice” pattern for CodeIntel-style repos

If you want this to be robust and predictable:

* Put **all Dask cluster creation + client wiring** in one place (script or runtime module).
* Keep Hamilton functions:

  * **pure** (no hidden globals that don’t serialize),
  * **typed** so you know when you’re operating on Pandas vs Dask DataFrames.
* Pick one of two explicit styles:

  1. **Dask-collections-first**: functions take/return Dask DataFrames/Series; run with `DaskGraphAdapter(use_delayed=False)`.
  2. **Task-parallel-first**: functions operate on serializable units; parallelize with `Parallelizable/Collect` and `DaskExecutor`.

---
 

[1]: https://hamilton.apache.org/concepts/parallel-task/ "Dynamic DAGs/Parallel Execution - Hamilton"
[2]: https://hamilton.apache.org/reference/graph-adapters/DaskGraphAdapter/ "h_dask.DaskGraphAdapter - Hamilton"
[3]: https://docs.dask.org/en/stable/deploying-python.html "Python API — Dask  documentation"
[4]: https://hamilton.apache.org/reference/result-builders/Dask/ "Dask - Hamilton"
[5]: https://blog.dagworks.io/p/rag-ingestion-and-chunking-using "RAG: ingestion and chunking using Hamilton and scaling to Ray, Dask, or PySpark "

Below is a **comparable deep dive** on how **Ray** is used in **Apache Hamilton**, mirroring the structure from the Dask deep dive: integration surfaces, core knobs, runnable templates, and the “gotchas” that matter in real pipelines.

---

## 1) Where Ray “plugs in” to Hamilton (two distinct integration styles)

Hamilton supports **two approaches to parallel execution**:

1. **GraphAdapter approach** — Hamilton walks the DAG and submits **each node** to an adapter (Ray/Dask/threadpool/etc.). ([hamilton.apache.org][1])
2. **Task-based dynamic execution** — you mark **map→reduce** regions with `Parallelizable[]` / `Collect[]`, and delegate *those tasks* to a “remote executor” (Ray/Dask/etc.). ([hamilton.apache.org][1])

Ray appears in Hamilton in both ways:

* **`h_ray.RayGraphAdapter`** (GraphAdapter / node-level scheduling) ([hamilton.staged.apache.org][2])
* **`h_ray.RayTaskExecutor`** (dynamic execution / task-level scheduling inside `Parallelizable/Collect`) — documented in DAGWorks examples. ([blog.dagworks.io][3])

---

## 2) Integration A — `h_ray.RayGraphAdapter` (run the *whole* DAG “on Ray”)

### What it does

`RayGraphAdapter` “delegates execution of the individual nodes” to Ray, i.e. Hamilton **walks the graph and translates it to run onto Ray**. ([hamilton.staged.apache.org][2])

Hamilton’s docs explicitly position it for:

* **multi-core on a single machine**
* **distributed computation** on a Ray cluster
  …but with caveats: you’re still **limited by machine memory** and **serialization costs can outweigh parallelism**, so you should benchmark. ([hamilton.staged.apache.org][2])

### Install

Hamilton’s Ray adapter page says to install via:

* `pip install sf-hamilton[ray]` ([hamilton.staged.apache.org][2])

### API surface (the knobs that matter)

Hamilton documents the constructor as:

`RayGraphAdapter(result_builder, ray_init_config=None, shutdown_ray_on_completion=False)` ([hamilton.staged.apache.org][2])

Key parameters:

* **`result_builder` (required)**: controls output shaping (dict, pandas DF, etc.). ([hamilton.staged.apache.org][2])
* **`ray_init_config`**: passed through to Ray init behavior (connect to a cluster or start local with custom config). ([hamilton.staged.apache.org][2])
* **`shutdown_ray_on_completion`**: whether Hamilton shuts Ray down at the end. ([hamilton.staged.apache.org][2])
* **Experimental**: docs warn signature changes are possible. ([hamilton.staged.apache.org][2])

### Supported return types & Pandas caveat

* Works for any object **serializable by Ray**. ([hamilton.staged.apache.org][2])
* Hamilton explicitly says Ray **does not do anything special about Pandas** (so big pandas objects can become a serialization + memory story). ([hamilton.staged.apache.org][2])

### Minimal runnable template (single machine)

```python
import ray
from hamilton import base, driver
from hamilton.plugins.h_ray import RayGraphAdapter

import my_flow  # your Hamilton functions module(s)

ray.init()  # local ray runtime (multi-core)

adapter = RayGraphAdapter(
    result_builder=base.DictResult(),   # or base.PandasDataFrameResult(), etc.
    ray_init_config=None,
    shutdown_ray_on_completion=False,
)

dr = (
    driver.Builder()
    .with_modules(my_flow)
    .with_adapter(adapter)
    .build()
)

out = dr.execute(["some_output"], inputs={})
print(out["some_output"])
```

### “Local vs cluster” initialization: what to put in `ray_init_config`

Ray’s official `ray.init` docs explain how address resolution works (connect to provided address, or connect to existing, or start local by default; `address="auto"` errors if none found; `address="local"` forces a new local instance). ([Ray][4])

Typical patterns:

* **Local single machine**: `ray.init()` or `ray_init_config={"num_cpus": 8}` ([Ray][4])
* **Connect to existing cluster**: pass `address=...` (Ray docs explain address formats and `ray://` usage). ([Ray][4])

---

## 3) Integration B — Dynamic execution + `h_ray.RayTaskExecutor` (Parallelizable/Collect “map region” on Ray)

### What it is

Hamilton’s dynamic execution mode:

* groups nodes into “tasks”
* can execute those tasks in parallel with a chosen executor ([hamilton.apache.org][1])

In DAGWorks’ example, you enable dynamic execution and set the **remote executor** to Ray with a one-line swap:

```python
from hamilton.plugins import h_ray
ray.init()

dr = (
   driver.Builder()
   .with_modules(doc_pipeline)
   .enable_dynamic_execution(allow_experimental_mode=True)
   .with_config({})
   .with_remote_executor(h_ray.RayTaskExecutor())
   .build()
)
...
ray.shutdown()
```

([blog.dagworks.io][3])

### When to prefer this over `RayGraphAdapter`

Use dynamic execution + `RayTaskExecutor` when:

* your workload naturally has a **map/reduce** shape (per-item processing),
* you want to parallelize *only the region between* `Parallelizable` and `Collect`, rather than turning **every node** into a Ray task. (This often avoids “too many tiny tasks.”) ([hamilton.apache.org][1])

### Minimal template (single machine)

**Flow (Parallelizable → Collect):**

```python
from hamilton.htypes import Parallelizable, Collect
import time

def items(n: int) -> Parallelizable[int]:
    for i in range(n):
        yield i

def work(items: int, sleep_s: float) -> int:
    time.sleep(sleep_s)
    return items * items

def total(work: Collect[int]) -> int:
    return sum(work)
```

**Driver (RayTaskExecutor):**

```python
import ray
from hamilton import driver
from hamilton.plugins import h_ray

import my_parallel_flow

ray.init()

dr = (
    driver.Builder()
    .with_modules(my_parallel_flow)
    .enable_dynamic_execution(allow_experimental_mode=True)
    .with_config({})
    .with_remote_executor(h_ray.RayTaskExecutor())
    .build()
)

out = dr.execute(["total"], inputs={"n": 1000, "sleep_s": 0.001})
print(out["total"])

ray.shutdown()
```

### Caveats you must design around (same “big 3” as Dask)

The DAGWorks Ray/Dask scaling example calls out:

1. **Serialization**: inputs/outputs into/out of what’s parallelized must be serializable. ([blog.dagworks.io][3])
2. **Concurrency**: bounded by the Ray cluster configuration (local is fine, but it’s still resource-limited). ([blog.dagworks.io][3])
3. **Memory at collect**: “collect/reduce” brings outputs back together; if you OOM, store large results in workers and return pointers/paths. ([blog.dagworks.io][3])

---

## 4) Ray fundamentals that matter *specifically* for Hamilton pipelines

### 4.1 Serialization & the object store (why “big objects” can bite)

Ray processes don’t share memory; objects transferred between workers are serialized. Ray uses a shared-memory **Plasma object store** (and supports cloudpickle / Pickle protocol 5) and has optimizations like **zero-copy reads for NumPy arrays** on the same node. ([Ray][5])

**Hamilton implication**

* If your Hamilton nodes return large NumPy arrays / Arrow buffers, Ray can be efficient.
* If they return huge pandas objects, you can hit overhead quickly (and Hamilton explicitly says Ray doesn’t do anything special for pandas). ([Ray][5])

### 4.2 Object store sizing & “why did Ray OOM?”

`ray.init` exposes `object_store_memory` and documents a default behavior (object store size defaults to ~30% of system memory, capped by shm and 200GB, though configurable). ([Ray][4])

**Hamilton implication**

* RayGraphAdapter chains node outputs as Ray objects; heavy intermediate artifacts live in the object store.
* If you see object store pressure, you either:

  * reduce intermediate size / persist to disk and pass references, or
  * adjust Ray’s object store memory budget. ([Ray][4])

### 4.3 CPU resource accounting (how “#cores” is decided)

Ray’s docs define `num_cpus` in `ray.init()` and resource-based scheduling; tasks/actors can request fractional CPUs (`@ray.remote(num_cpus=0.5)` etc.). ([Ray][4])

**Hamilton implication**

* When using Hamilton’s Ray integrations, Ray’s scheduler is the final authority on concurrency.
* If you need fine-grained per-task resource shaping, Ray supports it, but Hamilton’s RayGraphAdapter docs don’t describe per-node resource overrides—so treat that as something you’d do **inside your node code** (e.g., using your own Ray remote tasks/actors), not something Hamilton guarantees out-of-the-box. ([hamilton.staged.apache.org][2])

---

## 5) Choosing between RayGraphAdapter vs RayTaskExecutor (rule-of-thumb)

### Prefer `RayGraphAdapter` when…

* you want “drop-in” Ray without refactoring your DAG to `Parallelizable/Collect`
* you’re okay with **node-level task submission** and potential serialization overhead per node ([hamilton.apache.org][1])

### Prefer dynamic execution + `RayTaskExecutor` when…

* you have a natural “per-item processing” block and want coarser-grained parallelism
* you want to contain serialization boundaries to the `Parallelizable → Collect` region ([hamilton.apache.org][1])

---

## 6) Practical “gotchas” checklist (Hamilton + Ray)

* **Benchmark first**: Hamilton explicitly warns serialization costs can outweigh benefits. ([hamilton.staged.apache.org][2])
* **Don’t expect pandas magic**: Hamilton explicitly says Ray does nothing special for pandas. ([hamilton.staged.apache.org][2])
* **Treat collect as a memory cliff** (dynamic execution): if outputs are large, store them remotely and return references. ([blog.dagworks.io][3])
* **Tune Ray init for your machine**: `num_cpus`, `object_store_memory`, and `address` modes matter. ([Ray][4])

---

If you want the next increment (same as what you did for Dask): I can turn this into **Ray deep-dive pages** with:

* a clean “mode matrix” (`RayGraphAdapter` vs `RayTaskExecutor`) + when to use which,
* serialization-safe DI patterns *specifically for Ray* (actors for client pools, object store pinning avoidance),
* and a small benchmark harness that compares: ThreadPool vs RayGraphAdapter vs RayTaskExecutor on (a) I/O-bound and (b) CPU-bound map/reduce blocks.

[1]: https://hamilton.apache.org/concepts/parallel-task/?utm_source=chatgpt.com "Dynamic DAGs/Parallel Execution - Hamilton"
[2]: https://hamilton.staged.apache.org/reference/graph-adapters/RayGraphAdapter/?utm_source=chatgpt.com "h_ray.RayGraphAdapter - Hamilton"
[3]: https://blog.dagworks.io/p/rag-ingestion-and-chunking-using?utm_source=chatgpt.com "RAG: ingestion and chunking using Hamilton and scaling to Ray ..."
[4]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html?utm_source=chatgpt.com "ray.init — Ray 2.53.0 - Ray Docs"
[5]: https://docs.ray.io/en/latest/ray-core/objects/serialization.html?utm_source=chatgpt.com "Serialization — Ray 2.53.0 - Ray Docs"

Below are the **Ray deep-dive pages** you asked for—structured like the Dask increment:

1. a clean **mode matrix** (RayGraphAdapter vs RayTaskExecutor) + when to use which
2. **serialization-safe DI patterns** tailored to Ray (actors for client pools + object store pinning avoidance)
3. a small **benchmark harness** comparing **ThreadPool vs RayGraphAdapter vs RayTaskExecutor** on **I/O** and **CPU** map/reduce blocks

---

# Page 1 — Mode matrix (RayGraphAdapter vs RayTaskExecutor)

Hamilton has **two** parallelism approaches:

* **Adapter approach**: Hamilton walks the DAG and submits each node to an adapter (threadpool, Ray, Dask, …). ([hamilton.apache.org][1])
* **Task-based dynamic approach**: use `Parallelizable[]/Collect[]` and delegate those tasks to an executor (Ray/Dask/etc.). ([hamilton.apache.org][1])

## Mode matrix

| Dimension                  | RayGraphAdapter (adapter / node-level)                                                                                                 | RayTaskExecutor (dynamic / task-level)                                                                                                                    |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **What you change in DAG** | **Nothing** (no `Parallelizable/Collect` needed)                                                                                       | You add `Parallelizable[]` + `Collect[]` for map/reduce                                                                                                   |
| **Granularity**            | **Per node** in the Hamilton DAG                                                                                                       | **Per “map block instance”** between `Parallelizable` → `Collect`                                                                                         |
| **Serialization boundary** | Often “every node output” crosses Ray boundaries                                                                                       | Mostly “inputs into the map block / outputs from the map block”                                                                                           |
| **Best for**               | DAGs with **coarse nodes** and/or **high natural parallelism** across independent branches                                             | Workloads that naturally look like **map/reduce** (per-item processing)                                                                                   |
| **Main risk**              | **Too many tiny tasks**, plus serialization overhead per node                                                                          | **Collect step memory cliff** + serialization of boundary inputs                                                                                          |
| **Key knobs**              | `ray_init_config`, `result_builder`, shutdown behavior                                                                                 | Ray cluster config (via `ray.init`) + task size/shape in map block                                                                                        |
| **Hamilton docs stance**   | Explicitly warns serialization costs can outweigh benefits; memory-limited; Pandas not special-cased ([hamilton.staged.apache.org][2]) | Example usage shown via `with_remote_executor(h_ray.RayTaskExecutor())`; same caveats: serialization, concurrency, collect-memory ([blog.dagworks.io][3]) |

## Decision rules (practical)

* Pick **RayTaskExecutor** if you can naturally express your workload as:
  **produce items → do work per item → collect**. This keeps Ray overhead concentrated in the “map region,” instead of on every node. ([hamilton.apache.org][1])

* Pick **RayGraphAdapter** if:

  * you *cannot* (or don’t want to) refactor to `Parallelizable/Collect`, and
  * your DAG already has **big nodes** and **wide parallel branches** (so per-node scheduling overhead is amortized). ([hamilton.staged.apache.org][2])

---

# Page 2 — RayGraphAdapter deep dive (adapter / node-level scheduling)

## What it is

`hamilton.plugins.h_ray.RayGraphAdapter` delegates execution of **individual nodes** in a Hamilton graph to Ray: Hamilton still “walks the graph,” but each node runs remotely and returns a Ray object reference that gets resolved as needed. ([hamilton.staged.apache.org][2])

## Signature & core knobs

The docs define:

````text
RayGraphAdapter(result_builder: ResultMixin,
                ray_init_config: Dict[str, Any] = None,
                shutdown_ray_on_completion: bool = False)
``` :contentReference[oaicite:7]{index=7}

Knobs:
- `result_builder` (**required**) — shapes outputs (dict, dataframe, etc.) :contentReference[oaicite:8]{index=8}  
- `ray_init_config` — passed through to `ray.init` config (connect to cluster or start local) :contentReference[oaicite:9]{index=9}  
- `shutdown_ray_on_completion` — whether to shut Ray down after execution :contentReference[oaicite:10]{index=10}  

Hamilton notes this adapter is **experimental** (signature could change). :contentReference[oaicite:11]{index=11}

## Hamilton-stated caveats (don’t skip these)
Hamilton explicitly says:
- you’re limited by **machine memory** (can’t scale arbitrarily) :contentReference[oaicite:12]{index=12}  
- **serialization costs** can outweigh parallelism—benchmark :contentReference[oaicite:13]{index=13}  
- Ray does **nothing special for Pandas** :contentReference[oaicite:14]{index=14}  

## Minimal runnable template (single machine)
```python
import ray
from hamilton import base, driver
from hamilton.plugins.h_ray import RayGraphAdapter

import my_flow  # your Hamilton module

adapter = RayGraphAdapter(
    result_builder=base.DictResult(),
    ray_init_config={"num_cpus": 8, "include_dashboard": False},  # local CPU budget
    shutdown_ray_on_completion=True,
)

dr = (
    driver.Builder()
    .with_modules(my_flow)
    .with_adapter(adapter)
    .build()
)

out = dr.execute(["some_output"], inputs={})
print(out["some_output"])
````

Notes:

* `ray.init`’s `num_cpus` controls CPU resources Ray advertises/schedules against. ([Ray][4])
* If you want Ray’s dashboard, omit `include_dashboard=False`.

## When RayGraphAdapter tends to win

* **Wide DAGs** with multiple independent branches (e.g., lots of I/O calls or large, coarse transforms)
* **Coarse nodes**: “heavy enough” to amortize Ray scheduling + serialization

## When it tends to lose

* DAGs with **many small nodes** (Ray overhead dominates)
* Nodes returning **large Python objects** (especially large pandas objects), because each node output crossing process boundaries becomes serialization + object-store pressure

---

# Page 3 — RayTaskExecutor deep dive (dynamic execution / task-level scheduling)

## What it is

Hamilton’s task-based executor uses `Parallelizable[]/Collect[]` to define a **map/reduce** region, then runs the map tasks on a “remote executor.” Hamilton’s parallel-task concept page spells out that the block between `Parallelizable` and `Collect` is grouped into tasks and run remotely. ([hamilton.apache.org][1])

DAGWorks shows the Ray wiring as:

````python
from hamilton.plugins import h_ray
ray.init()
...
.with_remote_executor(h_ray.RayTaskExecutor())
...
ray.shutdown()
``` :contentReference[oaicite:17]{index=17}

## Minimal runnable template
### Flow (map/reduce)
```python
from hamilton.htypes import Parallelizable, Collect
import time

def items(n: int) -> Parallelizable[int]:
    for i in range(n):
        yield i

def work(items: int, sleep_s: float) -> int:
    time.sleep(sleep_s)      # I/O-ish
    return items * items

def total(work: Collect[int]) -> int:
    return sum(work)
````

### Driver (RayTaskExecutor)

```python
import ray
from hamilton import driver
from hamilton.execution import executors
from hamilton.plugins import h_ray

import my_parallel_flow

ray.init(num_cpus=8, include_dashboard=False)

dr = (
    driver.Builder()
    .with_modules(my_parallel_flow)
    .enable_dynamic_execution(allow_experimental_mode=True)
    .with_config({})
    .with_local_executor(executors.SynchronousLocalTaskExecutor())
    .with_remote_executor(h_ray.RayTaskExecutor())
    .build()
)

out = dr.execute(["total"], inputs={"n": 1000, "sleep_s": 0.001})
print(out["total"])

ray.shutdown()
```

## Known caveats (Hamilton’s and Ray’s)

Hamilton’s dynamic-execution docs call out:

* no nested `Parallelizable/Collect` blocks
* collect inputs limited (currently)
* serialization concerns (and notes Ray/Dask work well vs multiprocessing’s default pickle issues) ([hamilton.apache.org][1])

DAGWorks also highlights the big operational caveats:

* everything crossing the parallelized region must be serializable
* concurrency depends on Ray config
* collect step can blow memory; store large outputs remotely and return pointers ([blog.dagworks.io][3])

---

# Page 4 — Serialization-safe DI patterns for Ray (actors + object store pinning avoidance)

This is the part that makes Ray “feel” reliable in real systems.

## 4.1 Pattern: config-only across boundaries (baseline)

**Rule:** pass only **small, picklable config** into the parallel region; instantiate clients inside the remote task.

Why: Hamilton warns that adapter-based parallelism implies serialization (and dynamic execution similarly serializes boundary objects). ([hamilton.apache.org][1])

```python
from dataclasses import dataclass
import os

@dataclass(frozen=True)
class ApiCfg:
    base_url: str
    api_key_env: str = "API_KEY"

def api_cfg() -> ApiCfg:
    return ApiCfg(base_url="https://example.invalid")

def work(items: int, api_cfg: ApiCfg) -> dict:
    token = os.environ[api_cfg.api_key_env]
    # client = Client(api_cfg.base_url, token)  # created inside worker
    return {"item": items, "ok": True}
```

## 4.2 Pattern: Ray actor “client pool” (best practice for unpicklable clients)

Ray’s core model is tasks + actors operating on “remote objects.” ([Ray][5])
Use an **actor** to own the unpicklable client and expose RPC-like methods.

Key note: Ray strongly recommends setting `num_cpus` explicitly for actors to avoid surprises. ([Ray][6])

```python
import ray

@ray.remote(num_cpus=0.25)
class ClientActor:
    def __init__(self, base_url: str, api_key_env: str):
        import os
        token = os.environ[api_key_env]
        # self.client = Client(base_url, token)
        self.base_url = base_url
        self.token = token

    def fetch(self, item: int) -> dict:
        # return self.client.fetch(item)
        return {"item": item, "base": self.base_url}

def get_actor(base_url: str, api_key_env: str):
    # named actor gives you reuse across tasks
    name = f"client::{base_url}"
    try:
        return ray.get_actor(name)
    except ValueError:
        return ClientActor.options(name=name).remote(base_url, api_key_env)
```

**How to use from Hamilton tasks**

* Pass only `base_url` / `api_key_env` into the parallel region
* Inside `work`, call `get_actor(...).fetch.remote(item)` and `ray.get` if you want the concrete result

This avoids ever pickling a live client connection.

## 4.3 Pattern: avoid “object store pinning” (the silent OOM killer)

Ray uses an object store per node, and objects are **pinned** as long as an `ObjectRef` is “in scope” anywhere in the cluster (distributed reference counting). ([Ray][7])

Practical implications:

* Returning large values from many tasks can fill the object store.
* Holding onto refs (or embedding refs inside other objects) can keep data pinned longer than you think.

### Debugging pinned refs

Ray provides `ray memory` to inspect which `ObjectRef` references are held and may cause `ObjectStoreFullError`. ([Ray][7])

### Avoidance rules

* Don’t return huge objects per item unless you must.
* Prefer writing big artifacts to persistent storage (parquet/files/db) in the worker and returning a **small pointer** (path/URI/key).
* Delete references you don’t need (`del ref`) so objects can be evicted when out of scope. (Ray evicts based on reference counting.) ([Ray][7])

### Special warning: serialized ObjectRefs can stay pinned

Ray’s serialization docs warn that if you serialize an `ObjectRef` out-of-band (e.g., cloudpickle), its value remains pinned and must be explicitly freed via a **private API** (`ray._private.internal_api.free`). ([Ray][8])
I’d treat that as a last resort and prefer designing so refs don’t escape unintentionally.

## 4.4 Pattern: “collect pointers, not payloads”

Hamilton’s `Collect` step can be a memory cliff (DAGWorks calls this out directly). ([blog.dagworks.io][3])
A robust compromise is:

* map tasks return **small pointers**
* collect aggregates pointers
* downstream stages stream-load or batch-resolve them

---

# Page 5 — Benchmark harness (ThreadPool vs RayGraphAdapter vs RayTaskExecutor)

Goal: a tiny runner that compares wall time and speedup curves on:

* **I/O-bound** per-item work (sleep / API-like)
* **CPU-bound** per-item work (pure Python loop)

### Important comparability note

* `FutureAdapter` (ThreadPool) and `RayGraphAdapter` are **adapter-based** and **cannot** execute `Parallelizable/Collect` graphs (that’s a known constraint for the default executor + adapters). ([hamilton.apache.org][1])
* So the harness runs:

  * **ThreadPool + RayGraphAdapter** on a **static fan-out** Hamilton graph (N item nodes + reduce)
  * **RayTaskExecutor** on a **dynamic map/reduce** Hamilton graph (`Parallelizable/Collect`)

The per-item compute is identical; the graph encoding differs because Hamilton’s two parallelization systems are different by design.

---

## 5.1 `bench_ray_modes.py` (single file)

```python
from __future__ import annotations

import argparse
import json
import statistics
import time
import types
from dataclasses import dataclass
from typing import Any

from hamilton import base, driver

# Adapter baseline
from hamilton.plugins.h_threadpool import FutureAdapter

# Ray modes
from hamilton.plugins.h_ray import RayGraphAdapter
from hamilton.plugins import h_ray


@dataclass(frozen=True)
class WorkSpec:
    kind: str          # "io" | "cpu"
    n_items: int
    sleep_s: float
    cpu_iters: int


def make_static_fanout_module(mod_name: str, spec: WorkSpec) -> types.ModuleType:
    """
    Static fan-out "map/reduce" graph:
      work_0 .. work_{n-1} depend on kind/sleep_s/cpu_iters
      total(work_0, ..., work_{n-1}) sums results
    This can run under GraphAdapters (ThreadPool, RayGraphAdapter).
    """
    src_lines = [
        "from __future__ import annotations",
        "import time",
        "",
        "def kind() -> str:",
        f"    return {spec.kind!r}",
        "",
        "def sleep_s() -> float:",
        f"    return {spec.sleep_s!r}",
        "",
        "def cpu_iters() -> int:",
        f"    return {spec.cpu_iters!r}",
        "",
        "def _work(item: int, kind: str, sleep_s: float, cpu_iters: int) -> int:",
        "    if kind == 'io':",
        "        time.sleep(sleep_s)",
        "        return item",
        "    if kind == 'cpu':",
        "        x = 0",
        "        for j in range(cpu_iters):",
        "            x = (x + (item + j) * (item + j)) % 1_000_000_007",
        "        return x",
        "    raise ValueError(kind)",
        "",
    ]

    # work_i nodes
    for i in range(spec.n_items):
        src_lines += [
            f"def work_{i}(kind: str, sleep_s: float, cpu_iters: int) -> int:",
            f"    return _work({i}, kind, sleep_s, cpu_iters)",
            "",
        ]

    # total node signature with all work_i deps
    params = ", ".join([f"work_{i}: int" for i in range(spec.n_items)])
    args = ", ".join([f"work_{i}" for i in range(spec.n_items)])
    src_lines += [
        f"def total({params}) -> int:",
        f"    return sum([{args}])",
        "",
    ]

    mod = types.ModuleType(mod_name)
    exec("\n".join(src_lines), mod.__dict__)
    return mod


def make_dynamic_module(mod_name: str) -> types.ModuleType:
    """
    Dynamic map/reduce graph for RayTaskExecutor.
    """
    src = """
from __future__ import annotations
import time
from hamilton.htypes import Parallelizable, Collect

def items(n_items: int) -> Parallelizable[int]:
    for i in range(n_items):
        yield i

def work(items: int, kind: str, sleep_s: float, cpu_iters: int) -> int:
    if kind == "io":
        time.sleep(sleep_s)
        return items
    if kind == "cpu":
        x = 0
        for j in range(cpu_iters):
            x = (x + (items + j) * (items + j)) % 1_000_000_007
        return x
    raise ValueError(kind)

def total(work: Collect[int]) -> int:
    return sum(work)
"""
    mod = types.ModuleType(mod_name)
    exec(src, mod.__dict__)
    return mod


def time_run(fn, repeats: int) -> dict[str, float]:
    dts = [fn() for _ in range(repeats)]
    return {
        "median_s": statistics.median(dts),
        "min_s": min(dts),
        "max_s": max(dts),
    }


def bench_threadpool(spec: WorkSpec, workers: int, repeats: int) -> dict[str, Any]:
    mod = make_static_fanout_module("fanout_threadpool", spec)
    dr = (
        driver.Builder()
        .with_modules(mod)
        .with_adapter(FutureAdapter(max_workers=workers))
        .build()
    )
    stats = time_run(lambda: (dr.execute(["total"], inputs={})["total"],)[0] or 0, repeats)
    return {"mode": "threadpool", "workers": workers, **stats}


def bench_ray_graphadapter(spec: WorkSpec, workers: int, repeats: int) -> dict[str, Any]:
    mod = make_static_fanout_module("fanout_ray", spec)
    adapter = RayGraphAdapter(
        result_builder=base.DictResult(),
        ray_init_config={"num_cpus": workers, "include_dashboard": False},
        shutdown_ray_on_completion=True,
    )
    dr = (
        driver.Builder()
        .with_modules(mod)
        .with_adapter(adapter)
        .build()
    )
    stats = time_run(lambda: (dr.execute(["total"], inputs={})["total"],)[0] or 0, repeats)
    return {"mode": "ray_graphadapter", "workers": workers, **stats}


def bench_ray_taskexecutor(spec: WorkSpec, workers: int, repeats: int) -> dict[str, Any]:
    import ray

    mod = make_dynamic_module("dyn_ray")
    ray.init(num_cpus=workers, include_dashboard=False)

    dr = (
        driver.Builder()
        .with_modules(mod)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_config({})
        .with_remote_executor(h_ray.RayTaskExecutor())
        .build()
    )

    def one():
        out = dr.execute(
            ["total"],
            inputs={
                "kind": spec.kind,
                "n_items": spec.n_items,
                "sleep_s": spec.sleep_s,
                "cpu_iters": spec.cpu_iters,
            },
        )
        return out["total"]

    stats = time_run(one, repeats)
    ray.shutdown()
    return {"mode": "ray_taskexecutor", "workers": workers, **stats}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--kind", choices=["io", "cpu"], default="io")
    p.add_argument("--n-items", type=int, default=200)
    p.add_argument("--sleep-s", type=float, default=0.01)
    p.add_argument("--cpu-iters", type=int, default=300_000)
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--workers", type=str, default="1,2,4,8")
    p.add_argument("--out", type=str, default="bench_ray_modes.json")
    args = p.parse_args()

    spec = WorkSpec(args.kind, args.n_items, args.sleep_s, args.cpu_iters)
    workers_list = [int(x.strip()) for x in args.workers.split(",") if x.strip()]

    rows = []
    for w in workers_list:
        rows.append(bench_threadpool(spec, w, args.repeats))
        rows.append(bench_ray_graphadapter(spec, w, args.repeats))
        rows.append(bench_ray_taskexecutor(spec, w, args.repeats))

    # Compute speedup vs workers=1 per mode
    def key(r): return (r["mode"], r["workers"])
    by_mode = {}
    for r in rows:
        by_mode.setdefault(r["mode"], {})[r["workers"]] = r
    for mode, mp in by_mode.items():
        base_med = mp[min(mp.keys())]["median_s"]
        for w, r in mp.items():
            r["speedup_vs_1"] = (base_med / r["median_s"]) if r["median_s"] > 0 else 0.0

    out = {"spec": spec.__dict__, "rows": rows}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, sort_keys=True)
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
```

### How to run

```bash
pip install sf-hamilton[ray]
python bench_ray_modes.py --kind io  --n-items 400 --sleep-s 0.01 --workers 1,2,4,8
python bench_ray_modes.py --kind cpu --n-items 200 --cpu-iters 300000 --workers 1,2,4,8
```

### What to look for

* **I/O case**: threadpool often does well; RayTaskExecutor may win when map tasks are sufficiently heavy and/or you want cluster portability. Hamilton explicitly notes adapters can incur serialization costs (benchmark). ([hamilton.apache.org][1])
* **CPU case**: Ray typically helps when per-task compute is large enough to amortize overhead; tune `n_items` and `cpu_iters`.

---



[1]: https://hamilton.apache.org/concepts/parallel-task/?utm_source=chatgpt.com "Dynamic DAGs/Parallel Execution - Hamilton"
[2]: https://hamilton.staged.apache.org/reference/graph-adapters/RayGraphAdapter/ "h_ray.RayGraphAdapter - Hamilton"
[3]: https://blog.dagworks.io/p/rag-ingestion-and-chunking-using?utm_source=chatgpt.com "RAG: ingestion and chunking using Hamilton and scaling ..."
[4]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.init.html?utm_source=chatgpt.com "ray.init — Ray 2.53.0 - Ray Docs"
[5]: https://docs.ray.io/en/latest/ray-core/objects.html?utm_source=chatgpt.com "Objects — Ray 2.53.0"
[6]: https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote.html?utm_source=chatgpt.com "ray.remote — Ray 2.53.0 - Ray Docs"
[7]: https://docs.ray.io/en/latest/ray-core/scheduling/memory-management.html?utm_source=chatgpt.com "Memory Management — Ray 2.53.0"
[8]: https://docs.ray.io/en/latest/ray-core/objects/serialization.html?utm_source=chatgpt.com "Serialization — Ray 2.53.0 - Ray Docs"

## Deep dive: Hamilton’s **Dask GraphAdapter** (`h_dask.DaskGraphAdapter`)

This is the **“adapter approach”** where Hamilton **walks the DAG** and delegates node execution to Dask, rather than using `Parallelizable[]/Collect[]` dynamic execution. The adapter is explicitly described as: “Runs the entire Hamilton DAG on dask… walks the graph and translates it to run onto Dask.” ([Hamilton][1])

Hamilton’s own docs emphasize that for this adapter, the **central design choice** is whether you run your DAG via **`dask.delayed`** or via **native Dask collection types** (DataFrame/Array/Bag). ([Hamilton][1])

---

# 1) What you get (and what you don’t)

### What it’s good for

Hamilton lists the intended scaling modes:

* **Multi-core single machine ✅**
* **Distributed on a Dask cluster ✅**
* Scales to “any size supported by Dask” *if you load data appropriately via Dask loaders* ([Hamilton][1])

And it “works best with Pandas 2.0+ and pyarrow backend.” ([Hamilton][1])

### What the adapter *actually* does with your `Client`

A critical line that’s easy to miss:

> `dask_client – the dask client – **we don’t do anything with it**, but thought that it would be useful to wire through here.` ([Hamilton][1])

So in practice, **you** create the `Client()` largely to:

* establish the scheduler (distributed vs local),
* control workers/threads/memory,
* access the dashboard/logs.

Dask confirms that instantiating a `Client()` “creates a scheduler … with workers and threads” and **overrides whatever default scheduler was previously set**. ([Dask][2])

---

# 2) The control plane: `use_delayed` and `compute_at_end`

Hamilton documents these knobs directly:

* `use_delayed` (default `True`): wraps every node function with `dask.delayed` (backwards-compatible behavior). Hamilton notes it’s “probably not necessary” when you’re already using Dask DataFrames/Series because those are already lazy; “use delayed if you want to farm out computation.” ([Hamilton][1])
* `compute_at_end` (default `True`): whether Hamilton calls `.compute()` in the result builder to kick off computation. ([Hamilton][1])

### The behavior matrix (Hamilton’s own semantics)

Hamilton documents `build_result()` behavior as a set of cases: ([Hamilton][1])

| `use_delayed` | `compute_at_end` | What `dr.execute()` gives you                                                                   |
| ------------- | ---------------- | ----------------------------------------------------------------------------------------------- |
| True          | True             | A **materialized** result shaped by the `result_builder`                                        |
| True          | False            | A **delayed object** (you must compute later)                                                   |
| False         | False            | Whatever the `result_builder` returns (often a **Dask collection**)                             |
| False         | True             | Works only if `result_builder` returns a **Dask type**, because Hamilton will try to compute it |

---

# 3) The two “right” ways to use DaskGraphAdapter

Hamilton basically supports two sane archetypes:

## Archetype A — **Delayed-first** (generic Python objects, per-node task scheduling)

Use when:

* nodes are *not* operating on Dask DataFrames/Arrays,
* you want Dask to parallelize *node execution*.

But: Hamilton explicitly warns that with `use_delayed=True` “serialization costs can outweigh the benefits,” and that the adapter can “naively wrap all your functions with delayed,” scheduling them across workers—useful when computation is slow or the graph is highly parallelizable. ([Hamilton][1])

### Minimal implementation (local multi-core)

```python
from dask.distributed import Client, LocalCluster
from hamilton import driver
from hamilton.plugins.h_dask import DaskGraphAdapter

import my_flow  # your Hamilton module

cluster = LocalCluster(n_workers=4, threads_per_worker=1, processes=True)
client = Client(cluster)  # sets Dask scheduler + dashboard, etc. :contentReference[oaicite:10]{index=10}

adapter = DaskGraphAdapter(
    client,
    use_delayed=True,
    compute_at_end=True,
)

dr = driver.Builder().with_modules(my_flow).with_adapter(adapter).build()
out = dr.execute(["final_output"], inputs={})
```

### Watchouts unique to delayed-first

**1) “Too many tasks” is your main footgun.**
Dask delayed has per-task overhead (“a few hundred microseconds”), which becomes a problem if you apply `dask.delayed` too finely. Dask’s best practice is to batch or use Dask collections instead. ([Dask][3])
This matters because Hamilton may wrap **every** node when `use_delayed=True`. ([Hamilton][1])

**Mitigations**

* Prefer **coarser nodes** (do more work per node).
* If your DAG is “wide but tiny nodes,” switch to **Dask collections mode** (next section).
* Consider returning fewer intermediate objects (especially large ones) between nodes.

**2) Avoid global state / side effects / mutation.**
Dask’s delayed best practices explicitly warn:

* avoid global state (distributed/multiprocessing yields confusing errors), ([Dask][3])
* don’t mutate inputs, ([Dask][3])
* don’t rely on side effects (work only happens when you compute). ([Dask][3])

This maps cleanly to Hamilton node hygiene: **pure-ish, deterministic functions**.

**3) Don’t pass large concrete objects repeatedly into delayed tasks.**
Dask warns that repeatedly passing concrete large inputs causes hashing/overhead; it recommends “delay your data once.” ([Dask][3])
Hamilton implication: avoid huge Python objects as `inputs={...}` that feed many nodes; prefer loading via Dask (below) or wrapping the object once in a single upstream node.

---

## Archetype B — **Dask-collections-first** (Dask DataFrame/Array/Bag inside nodes)

Use when:

* you want true “pandas-like” scale-out (bigger-than-memory),
* you’re operating on Dask DataFrame/Array APIs.

Hamilton explicitly recommends that if you’re on a cluster and using Dask object types, set:

* `use_delayed=False`
* `compute_at_end=False` ([Hamilton][1])

And reiterates that mixing delayed with Dask objects is “probably not necessary.” ([Hamilton][1])

### Minimal implementation (return a Dask DataFrame result)

```python
from dask.distributed import Client
from hamilton import driver
from hamilton.plugins import h_dask

import features_flow  # nodes return dd.Series / dd.DataFrame pieces

client = Client()  # local cluster shorthand; sets scheduler/workers :contentReference[oaicite:19]{index=19}

adapter = h_dask.DaskGraphAdapter(
    client,
    result_builder=h_dask.DaskDataFrameResult(),
    use_delayed=False,
    compute_at_end=False,  # return dask collection, compute later
)

dr = driver.Builder().with_modules(features_flow).with_adapter(adapter).build()

ddf = dr.execute(["col_a", "col_b"], inputs={})
# ddf is a Dask DataFrame; call ddf.compute() when you *actually* want pandas
```

### The killer watchout: **never wrap Dask collections in delayed**

Dask’s own guidance is blunt:

> When you place a Dask array or Dask DataFrame into a delayed call, the function will receive the NumPy or Pandas equivalent… this might crash your workers. ([Dask][3])

So if your pipeline is Dask-collections-first, leaving Hamilton’s default `use_delayed=True` can accidentally force conversions to pandas/numpy in the wrong places. That’s why Hamilton recommends `use_delayed=False` for this mode. ([Hamilton][1])

### Dask DataFrame operational watchouts (that will surface in Hamilton nodes)

If your Hamilton nodes do heavy joins/shuffles/indexing, Dask’s DataFrame best practices become “Hamilton best practices” too:

* Dask itself says if data fits in RAM, pandas is often faster/easier. ([Dask][4])
* Setting an index and shuffling are expensive; do them infrequently. ([Dask][4])
* Persisting can be useful on distributed systems, but can block optimizer behaviors and should be used sparingly. ([Dask][4])

---

# 4) Result shaping: `result_builder` and the Dask-specific builder

### Default result builder

Hamilton says `result_builder` is optional and “defaults to pandas dataframe.” ([Hamilton][1])
This is often *not* what you want for large Dask workloads (because it brings results back into the driver process).

### `DaskDataFrameResult` (what it does + its sharp edges)

Hamilton’s Dask result builder builds a Dask DataFrame from outputs, but with assumptions: ([Hamilton][5])

1. output order mirrors join order,
2. it “massages” types into Dask types where it can,
3. otherwise duplicates scalars/objects using a template input and **assumes a single partition**.

**Practical implications**

* The list you pass to `execute([...])` becomes part of the semantics (ordering matters).
* “Assumes a single partition” can surprise you if you’re building multi-partition outputs and expect broadcasting behavior.

---

# 5) Cluster setup: what matters for Hamilton + DaskGraphAdapter

### Create a `Client` intentionally

Dask docs:

* `Client()` sets up a scheduler + workers/threads based on machine cores. ([Dask][2])
* `Client(processes=False)` keeps workers in-process; preferable when computations release the GIL (common for NumPy/Dask Array) and to avoid inter-worker communication. ([Dask][2])
* `Client()` is shorthand for `LocalCluster()` + `Client(cluster)`. ([Dask][2])

### Memory limits (and why they may “not work”)

LocalCluster docs note:

* `memory_limit` is only enforced when `processes=True`, and even then it’s “best-effort” (workers can still exceed it). ([Dask][2])

### Dashboard and ops hooks

Dask docs point to the local dashboard at `http://localhost:8787/status`. ([Dask][2])
That’s where you’ll quickly see if Hamilton created:

* a million tiny delayed tasks (overhead / scheduler saturation),
* a small high-level graph (healthy Dask collection pipeline),
* repeated recomputation because you didn’t persist where needed.

---

# 6) Visualization: DaskGraphAdapter vs Dask’s own graph views

### `visualize_kwargs` in the adapter

Hamilton exposes `visualize_kwargs` for “visualizing the graph using dask’s internals.” ([Hamilton][1])

### Graphviz can choke on large graphs

Dask warns Graphviz “takes a while on graphs larger than about 100 nodes,” and you may need to simplify computations for visualization. ([Dask][6])
This matters particularly for `use_delayed=True` because Hamilton may wrap every node and produce a large low-level task graph. ([Hamilton][1])

### Prefer high-level graph visualization for Dask collections

Dask recommends looking at the **high level graph** and visualizing via `.dask.visualize()`. ([Dask][6])
If you’re Dask-collections-first, this is often dramatically more readable than the low-level task graph.

---

# 7) A “do this / avoid this” checklist

### Do this

* **Decide your mode up front**:

  * generic python + per-node parallelism → `use_delayed=True`
  * Dask DataFrame/Array pipeline → `use_delayed=False, compute_at_end=False` ([Hamilton][1])
* Create a `Client()` early to set the scheduler and observe with the dashboard. ([Dask][2])
* Keep node functions pure: avoid global state, mutation, and side effects. ([Dask][3])
* Watch task granularity: delayed tasks have overhead; batch or use Dask collections when tasks get tiny. ([Dask][3])

### Avoid this

* **Don’t leave `use_delayed=True`** if upstream nodes return Dask DataFrames/Arrays (it can coerce them to pandas/numpy inside delayed calls). ([Dask][3])
* Don’t build a pipeline that returns a pandas dataframe by default if the dataset is genuinely larger-than-memory (default result builder is pandas). ([Hamilton][1])
* Don’t expect Graphviz visualization to work well on huge graphs. ([Dask][6])

---

If you tell me which of these two target shapes you’re after—**(A) node-parallel delayed** vs **(B) Dask-collections-first (dd.DataFrame)**—I can give you a “golden” implementation template (including recommended `LocalCluster` settings, a safe `result_builder` choice, and a set of anti-pattern tests you can add to CI).

[1]: https://hamilton.apache.org/reference/graph-adapters/DaskGraphAdapter/ "h_dask.DaskGraphAdapter - Hamilton"
[2]: https://docs.dask.org/en/stable/deploying-python.html "Python API — Dask  documentation"
[3]: https://docs.dask.org/en/latest/delayed-best-practices.html "Best Practices — Dask  documentation"
[4]: https://docs.dask.org/en/stable/dataframe-best-practices.html "Dask DataFrames Best Practices — Dask  documentation"
[5]: https://hamilton.apache.org/reference/result-builders/Dask/ "Dask - Hamilton"
[6]: https://docs.dask.org/en/stable/graphviz.html "Visualize task graphs — Dask  documentation"

Below is a “golden” **node-parallel / delayed-first** (`use_delayed=True`) template for **Hamilton + `h_dask.DaskGraphAdapter`**, plus **recommended `LocalCluster` presets**, a **safe `result_builder`**, and **CI anti-pattern tests** that directly target the failure modes Hamilton and Dask warn about.

---

# 1) Golden implementation template (delayed-first / node-parallel)

## 1.1 Recommended runtime wiring

Key facts from Hamilton’s DaskGraphAdapter docs:

* It “walks the graph and translates it to run onto Dask.” ([Hamilton][1])
* With `use_delayed=True`, it can “naively wrap all your functions with delayed” and warns serialization costs may outweigh parallelism—**benchmark**. ([Hamilton][1])
* `result_builder` is optional but **defaults to pandas dataframe** (usually not what you want for generic workflows). ([Hamilton][1])
* `execute_node()` “returns a dask delayed object.” ([Hamilton][1])
* The adapter wires through a `Client`, but “we don’t do anything with it” (so cluster config is on you). ([Hamilton][1])

### File: `runtime/hamilton_dask_delayed_driver.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable

from dask.distributed import Client, LocalCluster
from hamilton import base, driver
from hamilton.plugins.h_dask import DaskGraphAdapter


@dataclass(frozen=True)
class DaskLocalProfile:
    """
    Golden default for delayed-first Hamilton graphs on a single machine.

    Two presets:
      - "cpu": more processes, 1 thread each (avoid thread oversubscription)
      - "io": fewer processes, more threads per worker (good for I/O-ish work)
    """
    kind: str = "cpu"  # "cpu" | "io"
    n_workers: int | None = None
    threads_per_worker: int | None = None
    processes: bool | None = None
    memory_limit: str | int | None = None
    dashboard_address: str = ":8787"


def _default_workers() -> int:
    # keep 1 core for the scheduler / OS
    return max(1, (os.cpu_count() or 2) - 1)


def make_local_client(profile: DaskLocalProfile) -> Client:
    if profile.kind == "cpu":
        n_workers = profile.n_workers or _default_workers()
        threads_per_worker = profile.threads_per_worker or 1
        processes = True if profile.processes is None else profile.processes
    elif profile.kind == "io":
        n_workers = profile.n_workers or max(1, _default_workers() // 2)
        threads_per_worker = profile.threads_per_worker or 8
        # Dask docs: processes=False can be preferable when computations release the GIL
        # and you want to avoid inter-worker communication. :contentReference[oaicite:5]{index=5}
        processes = False if profile.processes is None else profile.processes
    else:
        raise ValueError(f"Unknown profile.kind={profile.kind!r}")

    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        processes=processes,
        memory_limit=profile.memory_limit,
        dashboard_address=profile.dashboard_address,
    )
    return Client(cluster)


def build_driver(*, modules: Iterable[object], client: Client) -> driver.Driver:
    adapter = DaskGraphAdapter(
        dask_client=client,
        # SAFE default for delayed-first: return a dict, not a pandas dataframe.
        # Hamilton default is pandas dataframe if you don't specify. :contentReference[oaicite:6]{index=6}
        result_builder=base.DictResult(),
        use_delayed=True,
        compute_at_end=True,
        # Optional: pass visualize kwargs through to dask.visualize() if you want.
        # visualize_kwargs={"filename": "hamilton_dask_graph.svg"}
    )
    return driver.Builder().with_modules(*modules).with_adapter(adapter).build()


def run_flow(dr: driver.Driver, outputs: list[str], inputs: dict) -> dict:
    """
    compute_at_end=True => execute() returns materialized results
    as shaped by result_builder. :contentReference[oaicite:7]{index=7}
    """
    return dr.execute(outputs, inputs=inputs)
```

### Why those `LocalCluster` defaults?

* Creating a `Client()` starts a local scheduler and workers/threads based on your machine cores. ([Dask][2])
* `Client(processes=False)` / `LocalCluster(processes=False)` is sometimes preferable when computations release the GIL (common in NumPy/Dask Array) and avoids inter-worker communication overhead. ([Dask][2])
* For **pure-Python CPU work**, you usually want **processes=True** + **threads_per_worker=1** to avoid oversubscription and to use multiple cores effectively.

---

## 1.2 Example Hamilton flow (designed to parallelize well)

### File: `flows/example_delayed_flow.py`

```python
from __future__ import annotations

import time


def sleep_s() -> float:
    return 0.05


def fetch_a(sleep_s: float) -> str:
    time.sleep(sleep_s)
    return "A"


def fetch_b(sleep_s: float) -> str:
    time.sleep(sleep_s)
    return "B"


def combined(fetch_a: str, fetch_b: str) -> str:
    return fetch_a + fetch_b
```

This creates two independent branches (`fetch_a`, `fetch_b`) that Dask can schedule concurrently once `sleep_s` is available.

---

# 2) “Safe” `result_builder` choice (delayed-first)

For delayed-first you typically want outputs that are:

* **small** and easy to serialize back to the driver process, and
* not automatically coerced into a big dataframe.

**Recommended default:** `base.DictResult()`
Because Hamilton notes the adapter defaults to a **pandas dataframe** if you don’t override `result_builder`. ([Hamilton][1])

When would you *not* use `DictResult()`?

* If your end output is naturally a dataframe and you *know* it fits in memory.
* If you explicitly want a Dask dataframe result you’d typically switch to the Dask-collections-first shape (different mode).

---

# 3) Operational considerations & watchouts (delayed-first)

These are the ones that *actually bite* in production.

## 3.1 Task granularity: avoid “too many tasks”

Dask warns: “Every delayed task has an overhead of a few hundred microseconds… can become a problem if you apply `dask.delayed` too finely.” ([Dask][3])

Hamilton’s adapter can “wrap all your functions with delayed,” so if your DAG has hundreds/thousands of micro-nodes, scheduler overhead can dominate. ([Hamilton][1])

**Rule of thumb**

* If the median node runtime is sub-millisecond, you’re likely in “too many tasks” land.
* Merge tiny nodes into fewer coarse nodes (or batch inside a node).

## 3.2 Never wrap Dask collections inside delayed nodes (anti-pattern)

Dask explicitly warns: putting a Dask DataFrame/Array into a delayed call causes the function to receive the **pandas/numpy** equivalent, which can crash workers if large. ([Dask][3])

For delayed-first mode, your safest policy is:

* **Don’t return Dask collections from Hamilton nodes**
* **Don’t accept Dask collections as inputs** unless you really know you want them materialized

## 3.3 Avoid repeated large concrete inputs across many nodes

Dask notes that each time you pass a concrete (non-delayed) large input, Dask hashes it for task naming; doing that repeatedly can be slow and can send data repeatedly to workers. ([Dask][3])

**Hamilton implication**

* Don’t feed a huge Python object through `inputs={...}` that many nodes depend on.
* Prefer: load inside a single upstream node, or delay the data once, or store in shared storage and pass references.

## 3.4 Dashboard-driven validation (the fastest way to catch real problems)

* The dashboard starts when you create a `Client`, and locally it’s served at `http://localhost:8787/status` by default (unless the port is taken). ([Dask][2])
  Use it to confirm:
* you’re actually parallel,
* you don’t have a million tiny tasks,
* memory per worker isn’t spiking unexpectedly.

---

# 4) Anti-pattern tests you can add to CI

These are **purpose-built for delayed-first** and directly reference Dask’s documented pitfalls.

## 4.1 Test: no nested/local functions (a common “can’t pickle” root cause)

### File: `tests/test_no_locals_in_node_functions.py`

```python
from __future__ import annotations

import inspect
from types import ModuleType
from typing import Iterable


def iter_module_functions(mods: Iterable[ModuleType]):
    for m in mods:
        for name, obj in vars(m).items():
            if name.startswith("_"):
                continue
            if inspect.isfunction(obj) and obj.__module__ == m.__name__:
                yield m.__name__, name, obj


def test_no_local_functions_in_hamilton_modules():
    import flows.example_delayed_flow as flow

    bad = []
    for modname, fnname, fn in iter_module_functions([flow]):
        # Nested functions usually show "<locals>" in qualname and often break serialization.
        if "<locals>" in fn.__qualname__:
            bad.append(f"{modname}.{fnname} is nested: {fn.__qualname__}")

    assert not bad, "Found nested/local functions:\n" + "\n".join(bad)
```

## 4.2 Test: task-graph size budget (catches “micro-node explosion”)

This uses Hamilton’s documented behavior that with `use_delayed=True`, node execution returns delayed objects. ([Hamilton][1])
Dask delayed best practices warn about too many tasks. ([Dask][3])

### File: `tests/test_task_graph_budget.py`

```python
from __future__ import annotations

from dask.distributed import Client, LocalCluster
from hamilton import base, driver
from hamilton.plugins.h_dask import DaskGraphAdapter


def test_task_graph_not_exploding():
    import flows.example_delayed_flow as flow

    cluster = LocalCluster(n_workers=2, threads_per_worker=2, processes=False)
    client = Client(cluster)

    # compute_at_end=False lets us inspect the delayed graph instead of executing it.
    adapter = DaskGraphAdapter(
        dask_client=client,
        result_builder=base.DictResult(),
        use_delayed=True,
        compute_at_end=False,
    )
    dr = driver.Builder().with_modules(flow).with_adapter(adapter).build()

    # Execute returns a delayed object when use_delayed=True. :contentReference[oaicite:18]{index=18}
    delayed_result = dr.execute(["combined"], inputs={})

    # delayed_result is typically a dask.delayed.Delayed (or a structure containing them).
    # We handle both conservatively.
    def count_tasks(x) -> int:
        if hasattr(x, "dask"):
            return len(x.dask)
        if isinstance(x, dict):
            return sum(count_tasks(v) for v in x.values())
        if isinstance(x, (list, tuple)):
            return sum(count_tasks(v) for v in x)
        return 0

    tasks = count_tasks(delayed_result)

    # Budget: adjust to your repo. This is tiny for the example flow.
    assert tasks <= 200, f"Task graph too large: {tasks}"

    client.close()
    cluster.close()
```

## 4.3 Test: forbid Dask collections inside delayed-first graphs

Dask explicitly warns against calling delayed on Dask collections (df/array become pandas/numpy). ([Dask][3])

### File: `tests/test_no_dask_collections_in_delayed_mode.py`

```python
from __future__ import annotations

import inspect
from types import ModuleType
from typing import Iterable


def iter_module_functions(mods: Iterable[ModuleType]):
    for m in mods:
        for name, obj in vars(m).items():
            if name.startswith("_"):
                continue
            if inspect.isfunction(obj) and obj.__module__ == m.__name__:
                yield obj


def test_flow_does_not_return_dask_collections():
    # Optional dependency: only applies if your repo uses dask.dataframe/array.
    try:
        import dask.dataframe as dd
        import dask.array as da
    except Exception:
        return

    import flows.example_delayed_flow as flow

    # Heuristic static check: type annotations should not mention dd.DataFrame/da.Array in delayed-first flows.
    offenders = []
    for fn in iter_module_functions([flow]):
        ann = getattr(fn, "__annotations__", {})
        if any(t in (dd.DataFrame, dd.Series, da.Array) for t in ann.values() if isinstance(t, type)):
            offenders.append(fn.__name__)

    assert not offenders, f"Dask collections returned/accepted in delayed-first flow: {offenders}"
```

## 4.4 (Optional but strong) “no global state” and “no side effects” lint gates

Dask delayed best practices explicitly list “Avoid global state” and “Don’t rely on side effects.” ([Dask][3])
You can enforce a lightweight heuristic in CI (AST scan for `global`, file I/O calls, or mutation of module-level variables). I didn’t hard-code that here because repos vary, but it’s usually worth adding once you know your acceptable side effects policy.

---

# 5) A “golden defaults” checklist for delayed-first

* **Set `use_delayed=True`**, accept that Hamilton may wrap every node; keep nodes coarse enough to amortize overhead. ([Hamilton][1])
* **Use `base.DictResult()`** (override the adapter’s pandas-default result builder). ([Hamilton][1])
* **Use `LocalCluster(processes=True, threads_per_worker=1)`** for CPU-bound pure Python; consider `processes=False` for GIL-releasing workloads. ([Dask][2])
* Use the dashboard to validate concurrency and memory (`Client()` starts it). ([Dask][4])
* Add CI tests for:

  * nested/local functions (serialization pain),
  * task graph budget (scheduler overload),
  * no Dask collections in delayed-first mode (avoids accidental pandas/numpy materialization). ([Dask][3])


[1]: https://hamilton.apache.org/reference/graph-adapters/DaskGraphAdapter/ "h_dask.DaskGraphAdapter - Hamilton"
[2]: https://docs.dask.org/en/stable/deploying-python.html "Python API — Dask  documentation"
[3]: https://docs.dask.org/en/latest/delayed-best-practices.html "Best Practices — Dask  documentation"
[4]: https://docs.dask.org/en/latest/dashboard.html "Dashboard Diagnostics — Dask  documentation"
