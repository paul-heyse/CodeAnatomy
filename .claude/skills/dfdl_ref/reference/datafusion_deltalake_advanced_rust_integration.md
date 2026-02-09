## 1) Rust→Python packaging & distribution pipeline (PyO3 / wheels / CI)

This chapter is the “ship it” layer: produce **importable** native modules + **portable wheels** + **reproducible CI artifacts**.

---

### A. Decide the artifact strategy first (because it locks everything else)

#### A1) Wheel explosion vs abi3 compression

You have two viable product shapes:

**Shape 1 — “per-Python” wheels (max capability):**

* You build one wheel **per Python minor** per OS/arch (`cp310`, `cp311`, …).
* Use this when you need full CPython API surface, or when dependencies force it.

**Shape 2 — “abi3” wheels (min wheel count, best ops):**

* You build one wheel **per OS/arch** that works across Python ≥ *min version* (limited/stable ABI).
* Maturin’s tutorial explicitly shows abi3 builds (“abi3 support for Python ≥ 3.8”). ([maturin.rs][1])
* PyO3’s docs explain abi3 is compiled against a minimum Python version but can run on newer versions. ([pyo3.rs][2])

**Actionable rule:** default to abi3 **unless** you have a hard reason not to (e.g., C-API you need isn’t in the limited ABI).

#### A2) Linux portability target: manylinux vs musllinux

On Linux, “it imports on my machine” is meaningless unless you hit a portable platform tag.

* **manylinux** is the glibc-based portability spec; **PEP 600** defines the modern “perennial” manylinux scheme. ([Python Enhancement Proposals (PEPs)][3])
* **musllinux** is the musl-based portability spec (Alpine etc); **PEP 656** defines musllinux tags. ([Python Enhancement Proposals (PEPs)][4])

Maturin’s distribution guide is explicit: Linux wheels must obey manylinux/musllinux portability, and you typically need a manylinux docker image or Zig to achieve it for PyPI publishing. ([maturin.rs][5])

---

### B. Minimal “correct” project skeleton (module import actually works)

#### B1) Module naming invariants (common footgun)

**Distribution/package name** (what pip installs) vs **module name** (what `import` uses) differ:

* Maturin states: package name comes from `Cargo.toml` `[package].name`, while the importable module name comes from `[lib].name`. ([GitHub][6])

**Action:** make `[lib].name` match the `#[pymodule] fn <name>(...)` symbol name. The maturin tutorial warns Python cannot import if this name doesn’t match. ([maturin.rs][1])

#### B2) Cargo.toml: cdylib + (optionally) rlib

PyO3’s build guide: build an extension module as a **`cdylib`**, and (for manual builds) set `PYO3_BUILD_EXTENSION_MODULE`. ([pyo3.rs][7])

Typical pattern (also keeps Rust tests happy by including `rlib`):

```toml
# Cargo.toml
[package]
name = "your_dist_name"          # pip install your_dist_name
version = "0.1.0"
edition = "2021"

[lib]
name = "your_module_name"        # import your_module_name
crate-type = ["cdylib", "rlib"]

[dependencies]
pyo3 = { version = "0.27", features = ["extension-module"] }
```

PyO3 itself documents `extension-module` usage in its crate docs. ([Docs.rs][8])

If using **abi3**, enable it via features; maturin’s bindings doc calls out `abi3` and `abi3-pyXX` feature formats. ([maturin.rs][9])

Example:

```toml
pyo3 = { version = "0.27", features = ["extension-module", "abi3-py38"] }
```

(And note PyO3’s constraint: you can’t target `abi3-py38` if your *host* Python is older than 3.8.) ([pyo3.rs][10])

#### B3) pyproject.toml: make builds PEP 517/518-native

Maturin is a PEP 517 backend; its tutorial shows the minimal `pyproject.toml`:

```toml
[build-system]
requires = ["maturin>=1.0,<2.0"]
build-backend = "maturin"
```

([maturin.rs][1])

**Why this matters operationally:** you can build via `pip wheel .`, `python -m build`, CI build frontends, etc., without “special casing” Rust.

---

### C. Local inner loop (fast dev workflow)

#### C1) “editable-ish” install into your venv

Maturin tutorial:

* `maturin develop` builds and installs directly into current virtualenv
* `maturin build` produces wheels for distribution ([maturin.rs][1])

Commands:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip maturin

maturin develop          # dev build + install into venv
maturin develop -r       # optimized build (release)
python -c "import your_module_name; print(your_module_name)"
```

The tutorial also shows `-r/--release` as the speed-testing knob. ([maturin.rs][1])

#### C2) Manual build (only when debugging build/link issues)

PyO3’s guide gives the exact manual loop:

* set `PYO3_BUILD_EXTENSION_MODULE=1`
* `cargo build`
* rename the built shared library to `.so` / `.pyd` as appropriate ([pyo3.rs][7])

This is useful when diagnosing linker inputs or symbol visibility independent of wheel tooling.

---

### D. Linux portability: manylinux/musllinux and the dynamic-linking reality

#### D1) What “manylinux/musllinux” means in practice

Maturin’s distribution guide lays out the core issue: builds may pick up libraries/symbols only present on the build machine; portable Linux wheels must conform to manylinux/musllinux. ([maturin.rs][5])

Also: it explicitly recommends at least **manylinux2014** for Rust (Rust requires glibc ≥ 2.17), and suggests enforcing the same manylinux version as your build image (e.g., `--manylinux 2014`). ([maturin.rs][5])

#### D2) Maturin’s built-in “auditwheel-like” behavior

Maturin can:

* check manylinux compliance (it calls it an “auditwheel reimplementation”)
* tag the wheel appropriately
* bundle extra shared libraries if needed (requires `patchelf`; installable via `pip install maturin[patchelf]`) ([maturin.rs][5])

This matters for DataFusion/delta-rs ecosystems: if you accidentally introduce a native dependency, you need deterministic bundling behavior and tooling to detect it early.

#### D3) If you must repair wheels explicitly (Linux/macOS/Windows)

Even if you use maturin, it’s useful to understand the *canonical* repair tools because CI flows often embed them:

* **Linux:** `auditwheel repair -w {dest_dir} {wheel}` is the default repair strategy used by cibuildwheel. ([cibuildwheel.pypa.io][11])
  And the packaging discussion explains what repair does: copies non-manylinux libs into the wheel, renames uniquely, patches ELF deps, sets RUN_PATH so no `LD_LIBRARY_PATH` is needed. ([Discussions on Python.org][12])

* **macOS:** `delocate-listdeps` + `delocate-wheel ...` is the canonical approach (also shown as cibuildwheel defaults). ([cibuildwheel.pypa.io][11])
  delocate’s README explains `-w` (output dir) and that it copies external deps into the wheel and patches extensions to use the copies. ([GitHub][13])

* **Windows:** there’s no built-in auditwheel equivalent in std tooling; the packaging community points to **delvewheel** as the “bundle DLL deps into the wheel” tool. ([Discussions on Python.org][14])

If you’re building a best-in-class pipeline, you should be able to *reason about* and *override* these repair steps when a dependency appears.

---

### E. Cross-compilation strategies (Docker vs Zig vs native builds)

#### E1) Cross-compile Linux/macOS with Docker images

Maturin explicitly recommends special docker images for manylinux compliance and notes `maturin-action` simplifies this in GitHub Actions. ([maturin.rs][5])

#### E2) Cross-compile Linux with Zig (high leverage for aarch64)

Maturin supports linking via **Zig cc** (`--zig`) and provides a concrete example:

```bash
pip install ziglang
maturin build --release --target aarch64-unknown-linux-gnu --zig
```

([maturin.rs][5])

This is usually the most practical way to add Linux aarch64 wheels without provisioning native aarch64 runners.

#### E3) Cross-compile Windows (advanced; decide if you want this)

Maturin documents an experimental PyO3 feature `generate-import-lib` and also mentions using `cargo-xwin` to fetch Windows CRT/SDK headers/libs automatically; it also notes MSVC targets require `llvm-dlltool` or `lib.exe` and includes a Microsoft license consent caveat. ([maturin.rs][5])

**Best practice:** if you don’t *need* cross-compiling to Windows, build Windows wheels on Windows runners (simpler + fewer footguns).

---

### F. CI: recommended patterns (multi-platform, reproducible, PyPI-safe)

You have two “good” starting points. Use whichever matches your customization tolerance.

#### F1) Generate a baseline workflow, then edit (fastest path)

Maturin can generate a GitHub Actions workflow:

```bash
mkdir -p .github/workflows
maturin generate-ci github > .github/workflows/CI.yml
```

([maturin.rs][5])

It also documents the generator flags (platform selection includes manylinux/musllinux/windows/macos by default). ([maturin.rs][5])

#### F2) Hand-authored workflow using the maturin GitHub Action (more control)

The action’s README shows:

* `uses: PyO3/maturin-action@v1`
* `manylinux` control
* example args including `--release --locked --compatibility pypi`
* and the “generate CI” command as well ([GitHub][15])

**Template (good defaults):**

* `--locked` ensures Cargo.lock is honored (reproducibility)
* `--compatibility pypi` prevents building unsupported target tags ([maturin.rs][5])

```yaml
name: wheels

on:
  push:
  pull_request:
  release:
    types: [published]

jobs:
  build-wheels:
    strategy:
      fail-fast: false
      matrix:
        include:
          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            manylinux: 2014
          - os: ubuntu-latest
            target: aarch64-unknown-linux-gnu
            manylinux: 2014
          - os: macos-latest
            target: x86_64-apple-darwin
          - os: macos-latest
            target: aarch64-apple-darwin
          - os: windows-latest
            target: x86_64-pc-windows-msvc

    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4

      - name: Build wheels
        uses: PyO3/maturin-action@v1
        with:
          command: build
          args: --release --locked --compatibility pypi
          target: ${{ matrix.target }}
          manylinux: ${{ matrix.manylinux }}

      - uses: actions/upload-artifact@v4
        with:
          name: wheels-${{ matrix.target }}
          path: target/wheels/*.whl
```

**Why `manylinux: 2014`?** Maturin recommends enforcing the same manylinux version as the docker image, and notes Rust requires glibc ≥ 2.17 ⇒ manylinux2014 baseline. ([maturin.rs][5])

#### F3) Publishing: token vs trusted publishing (OIDC)

Maturin’s generated workflow defaults to API token auth, but it also documents using PyPI trusted publishing (OIDC) and what to change in workflow permissions (`id-token: write`). ([maturin.rs][5])

---

### G. “Best-in-class” hardening checklist (what you enforce so agents don’t regress it)

**Reproducibility**

* Commit `Cargo.lock`, enforce `maturin build --locked` (or `--frozen`) in CI. ([maturin.rs][5])
* Pin your maturin version in CI (`maturin-version:` in action inputs). ([GitHub][15])

**Portability**

* Linux: build in manylinux image or use Zig (`--zig`), always `--compatibility pypi`. ([maturin.rs][5])
* If you ever introduce a nonstandard shared lib, require bundling (maturin needs `patchelf`). ([maturin.rs][5])

**Artifact contract**

* Always build both wheels + sdist (`--sdist` / `maturin sdist`) so downstream can rebuild from source. ([maturin.rs][5])

**Smoke tests (minimum)**

* In CI, after building wheel: `pip install <wheel>` then `python -c "import your_module; ..."` on each platform.

---

### Where this links to the next deep dive

Once you can reliably ship wheels, the next integration chapter is: **Python runtime architecture** (SessionContext lifecycle / pooling / isolation). That chapter will directly consume these build artifacts (wheel install), and will standardize a “single import provides Rust-backed DataFusion/Delta capabilities” surface.

[1]: https://www.maturin.rs/tutorial.html "Tutorial - Maturin User Guide"
[2]: https://pyo3.rs/v0.27.2/building-and-distribution/multiple-python-versions?utm_source=chatgpt.com "Supporting multiple Python versions"
[3]: https://peps.python.org/pep-0600/?utm_source=chatgpt.com "PEP 600 – Future 'manylinux' Platform Tags for Portable Linux ..."
[4]: https://peps.python.org/pep-0656/?utm_source=chatgpt.com "PEP 656 – Platform Tag for Linux Distributions Using Musl"
[5]: https://www.maturin.rs/distribution.html "Distribution - Maturin User Guide"
[6]: https://github.com/PyO3/maturin?utm_source=chatgpt.com "PyO3/maturin: Build and publish crates ..."
[7]: https://pyo3.rs/main/building-and-distribution.html "Building and distribution - PyO3 user guide"
[8]: https://docs.rs/pyo3/latest/src/pyo3/lib.rs.html?utm_source=chatgpt.com "lib.rs - source"
[9]: https://www.maturin.rs/bindings.html?utm_source=chatgpt.com "Bindings"
[10]: https://pyo3.rs/main/building-and-distribution.html?utm_source=chatgpt.com "Building and distribution"
[11]: https://cibuildwheel.pypa.io/en/1.x/options/?utm_source=chatgpt.com "Options - cibuildwheel"
[12]: https://discuss.python.org/t/auditwheel-repair-without-the-wheel-e-g-developer-install/5915?utm_source=chatgpt.com "Auditwheel repair... without the wheel (e.g. developer install)"
[13]: https://github.com/matthew-brett/delocate?utm_source=chatgpt.com "matthew-brett/delocate: Find and copy needed dynamic ..."
[14]: https://discuss.python.org/t/delocate-auditwheel-but-for-windows/2589?utm_source=chatgpt.com "Delocate/auditwheel, but for Windows?` - Packaging"
[15]: https://github.com/PyO3/maturin-action "GitHub - PyO3/maturin-action: GitHub Action to install and run a custom maturin command with built-in support for cross compilation"

## 2) Python runtime architecture: SessionContext lifecycle, pooling, and isolation

### 2.0 Mental model: what a `SessionContext` *is* (and why isolation is hard)

A `SessionContext` is **both**:

1. A **planner** (optimizer + physical planner) + multi-threaded executor
2. A **mutable session registry**: catalogs/schemas/tables/views/UDFs/object-store prefixes + config state

Python API signature (current): `SessionContext(config=None, runtime=None)` where:

* `config` is `SessionConfig` (plan/execution options)
* `runtime` is `RuntimeEnvBuilder` (aka `RuntimeConfig` in Python) (memory/disk pool + spill temp files) ([datafusion.apache.org][1])

That “mutable registry” property is what creates cross-request bleed unless you explicitly scope and clean up.

---

## 2.1 Configuration split: plan shape vs runtime feasibility (know where each knob lives)

### A) **Plan shape knobs** (`SessionConfig` + SQL `SET datafusion.execution.* / optimizer.*`)

These primarily change:

* partitioning / parallelism shape
* optimizer rewrites (join repartitioning, pruning, pushdowns)
* batch sizing behavior (vectorization + merge/spill characteristics)

Python: `SessionConfig` convenience methods exist for common switches (examples below). ([datafusion.apache.org][1])
Core DataFusion: the same options are also settable via SQL `SET ...` (and env vars) and cover execution/optimizer/catal og defaults. ([datafusion.apache.org][2])

**Canonical Python pattern**

```python
from datafusion import SessionConfig

cfg = (
    SessionConfig()
    .with_target_partitions(16)
    .with_batch_size(8192)
    .with_repartition_joins(True)
    .with_repartition_aggregations(True)
    .with_repartition_windows(True)
    .with_parquet_pruning(True)
    # escape hatch for any ConfigOptions key:
    .set("datafusion.execution.parquet.pushdown_filters", "true")
)
```

`with_target_partitions`, `with_batch_size`, and `.set(key,value)` are explicitly documented in the Python API. ([datafusion.apache.org][3])

**SQL equivalent (useful when you need per-query tuning)**

```sql
SET datafusion.execution.target_partitions = '4';
SET datafusion.execution.batch_size = '1024';
```

DataFusion documents `SET` as the supported mechanism for configuration and gives these exact knobs as tuning levers. ([datafusion.apache.org][2])

### B) **Runtime feasibility knobs** (`RuntimeEnvBuilder` / `RuntimeConfig` + SQL `SET datafusion.runtime.*`)

These decide whether the plan can *actually run* within your resource constraints:

* bounded vs unbounded memory pool
* spill strategy and disk temp file behavior
* temp paths and disk manager enablement
* runtime caches (list-files cache, parquet metadata cache)

Python runtime builder methods (all documented): ([datafusion.apache.org][3])

* `with_fair_spill_pool(size_bytes)` / `with_greedy_memory_pool(size_bytes)` / `with_unbounded_memory_pool()`
* `with_disk_manager_os()` / `with_disk_manager_specified(*paths)` / `with_disk_manager_disabled()`
* `with_temp_file_path(path)`

**Canonical Python runtime profile**

```python
from datafusion import RuntimeEnvBuilder

rt = (
    RuntimeEnvBuilder()
    .with_disk_manager_specified("/mnt/df-spill")   # or .with_disk_manager_os()
    .with_fair_spill_pool(8 * 1024**3)             # 8 GiB pool
    .with_temp_file_path("/mnt/df-spill")          # explicit temp dir
)
```

The docs explain what each pool is for (fair vs greedy) and that disabling the disk manager makes temp file creation error. ([datafusion.apache.org][3])

**Runtime configs via SQL**
DataFusion’s runtime settings include:

* `datafusion.runtime.memory_limit`
* `datafusion.runtime.temp_directory`
* `datafusion.runtime.metadata_cache_limit`
* `datafusion.runtime.list_files_cache_limit` / TTL, etc. ([datafusion.apache.org][2])

Example:

```sql
SET datafusion.runtime.memory_limit = '2G';
SET datafusion.runtime.temp_directory = '/mnt/df-spill';
```

([datafusion.apache.org][2])

### C) Cross-coupling you must internalize (spill + partitions + batch size)

DataFusion’s tuning guide explicitly calls out:

* with `FairSpillPool`, memory is **divided evenly among partitions**
* higher `target_partitions` ⇒ less memory/partition ⇒ spilling triggers earlier
* `batch_size` affects how spilled data is merged; smaller can reduce re-spills under tight memory ([datafusion.apache.org][2])

This becomes critical when you run *multiple concurrent queries* (see §2.3).

---

## 2.2 Session-scoped state: what “leaks” unless you scope it

`SessionContext` exposes session state operations in Python, including:

* `register_table`, `register_parquet`, `register_listing_table`, `register_record_batches`, `register_object_store`
* `register_udf` / `register_udaf` / `register_udwf` / `register_udtf`
* `deregister_table`
* catalog access (`catalog`, `catalog_names`)
* `session_id()` for correlation ([arrow.staged.apache.org][4])

Also: `SessionContext.global_ctx()` returns a wrapper around a **global** internal context (shared state by definition). Treat this as a footgun for multi-tenant services unless you enforce strict read-only semantics. ([datafusion.apache.org][3])

---

## 2.3 Concurrency model in a Python service: two layers of parallelism

### Layer 1: *in-query* parallelism (DataFusion partitions)

DataFusion parallelizes using partitions; Python docs show configuring `target_partitions` and using `df.repartition(...)` or `df.repartition_by_hash(...)`. ([datafusion.apache.org][1])

**Rule:** `target_partitions` is your *upper bound* on concurrent work inside one query.

### Layer 2: *cross-query* concurrency (your service)

If you run N queries concurrently and each query tries to use “all cores”, you get oversubscription and more spilling (especially with bounded pools).

**Operational pattern (CPU-aware gating):**

* decide a global concurrency cap: `MAX_INFLIGHT_QUERIES`
* set per-query partitions ≈ `cores / MAX_INFLIGHT_QUERIES`
* optionally reduce `batch_size` under tight memory

DataFusion’s tuning guide gives the rationale and explicit `SET` examples for `target_partitions` + `batch_size` under memory pressure. ([datafusion.apache.org][2])

### Don’t block your async server

DataFusion Python exposes *streaming* execution (not just `collect()`):

* `DataFrame.execute_stream()` returns a `RecordBatchStream` (async iterator available) ([datafusion.apache.org][5])
* `DataFrame.__arrow_c_stream__()` exports an Arrow C Stream and states results are produced incrementally (no full materialization) ([datafusion.apache.org][5])
* `RecordBatchStream` supports `__aiter__` / `__anext__` in Python ([datafusion.apache.org][6])

**Service pattern:** prefer streaming for large outputs; keep `collect()` for small results only.

---

## 2.4 Isolation strategies: pick one (and implement it fully)

### Strategy A — **Context-per-request** (max isolation, simplest correctness)

Create a new `SessionContext` for each request/run, register what you need, execute, drop it.

**When to choose:** multi-tenant query service, arbitrary user SQL, or frequent temp registrations.

```python
from datafusion import SessionContext, SessionConfig, RuntimeEnvBuilder

def new_ctx(run_id: str) -> SessionContext:
    rt = (RuntimeEnvBuilder()
          .with_disk_manager_specified(f"/mnt/df-spill/{run_id}")
          .with_fair_spill_pool(4 * 1024**3))
    cfg = (SessionConfig()
           .with_create_default_catalog_and_schema(True)
           .with_default_catalog_and_schema("svc", run_id)   # per-run default schema
           .with_target_partitions(8))
    return SessionContext(cfg, rt)
```

`with_default_catalog_and_schema` + `with_create_default_catalog_and_schema(True)` is the documented way to set default names. ([datafusion.apache.org][1])

### Strategy B — **Context pool** (best production default when you want reuse)

Pre-create K contexts (K = allowed concurrent queries), each pre-registered with:

* stable object stores
* stable UDFs
* stable catalogs/schemas (if any)

Then: **one request checks out one context** (no concurrent mutation), registers ephemeral tables/views, runs, and cleans up via `deregister_table`.

Key APIs:

* `deregister_table(name)` exists in Python ([arrow.staged.apache.org][4])
* listing tables via `tables()` exists ([arrow.staged.apache.org][4])

Skeleton:

```python
import queue
from contextlib import contextmanager
from datafusion import SessionContext

class DFContextPool:
    def __init__(self, size: int, factory):
        self._q = queue.Queue(maxsize=size)
        for _ in range(size):
            self._q.put(factory())

    @contextmanager
    def checkout(self) -> SessionContext:
        ctx = self._q.get()
        try:
            yield ctx
        finally:
            # TODO: deregister tmp tables/views here
            self._q.put(ctx)
```

**Why this works:** you avoid “shared mutable registry” races by construction.

### Strategy C — **Shared context** (only if you can enforce read-only semantics)

Use one long-lived context, but treat it like a database server:

* register all stable tables/providers once
* disallow DDL/DML/Statements for user queries

Python provides `SQLOptions` and `sql_with_options`:

* `SQLOptions.with_allow_ddl`, `with_allow_dml`, `with_allow_statements` are documented ([datafusion.apache.org][7])
* `sql_with_options` validates the query against these options ([datafusion.apache.org][3])

```python
from datafusion import SQLOptions

READONLY = (
    SQLOptions()
    .with_allow_ddl(False)
    .with_allow_dml(False)
    .with_allow_statements(False)  # blocks SET / BEGIN, etc
)

df = ctx.sql_with_options("SELECT ...", READONLY)
```

Use this only if you **never** need per-request `register_table` / temp views (or you wrap mutation in a lock + strict naming + cleanup).

---

## 2.5 Safe scoping rules (tables/views/UDFs/object stores/run-id)

### Tables / views

**Scope:** session (context)
**Creation:** `register_table`, `register_parquet`, `register_record_batches`, etc ([arrow.staged.apache.org][4])
**Removal:** `deregister_table(name)` ([arrow.staged.apache.org][4])

**Run-id isolation pattern (works everywhere):**

* allocate `run_id`
* prefix every ephemeral object: `tmp_{run_id}__{purpose}`
* after query: deregister anything with that prefix

### UDFs / UDAFs / UDWFs / UDTFs

**Scope:** session (context)
Register once per context (`ctx.register_udf(...)`, etc). ([arrow.staged.apache.org][4])
In a pool, do this at pool construction time.

### Object stores

Python docs show:

* create store object (e.g., `AmazonS3`)
* register with prefix like `"s3://"`
* then read/register tables from that URL space ([datafusion.apache.org][8])

```python
from datafusion.object_store import AmazonS3
s3 = AmazonS3(bucket_name="yellow-trips", region="us-east-1", ...)
ctx.register_object_store("s3://", s3, None)
ctx.register_parquet("trips", "s3://yellow-trips/")
```

([datafusion.apache.org][8])

**Isolation rule:** object store registration is session-scoped; do it at context creation (pool factory) and treat it as immutable thereafter.

### Catalog / schema (namespacing)

Python docs:

* by default, one catalog + schema (`datafusion` / `default`) ([datafusion.apache.org][9])
* you can create memory catalogs/schemas and register them ([datafusion.apache.org][8])
* you can set default catalog/schema in `SessionConfig` ([datafusion.apache.org][1])

**Two practical approaches:**

1. **Table-name isolation** (prefix names): simplest, no catalog mechanics
2. **Per-run default schema** using `with_default_catalog_and_schema("svc", run_id)` (best when SQL is heavy and you want unqualified names to be run-local)

### Delta Lake integration (DataFusion ↔ delta-rs)

Python docs show you can register a `deltalake.DeltaTable` directly as a table provider (DataFusion ≥ 43 + delta-rs exposing the provider interface). ([datafusion.apache.org][8])

```python
from deltalake import DeltaTable
dt = DeltaTable("path_to_table")
ctx.register_table("my_delta", dt)
```

([datafusion.apache.org][8])

They also note older delta-rs can fall back to `to_pyarrow_dataset()` but loses pushdown capabilities. ([datafusion.apache.org][8])

---

## 2.6 “Best-in-class” default blueprint (what I’d standardize in your deployment)

### Blueprint: pooled contexts + streaming + strict mutation boundaries

1. **Pool size** = max inflight queries you’re willing to run
2. Each pooled context is created with:

   * bounded spill pool (`with_fair_spill_pool`)
   * disk manager pointing at a dedicated spill root (`with_disk_manager_specified`)
   * `SessionConfig.with_target_partitions(PARTITIONS_PER_QUERY)`
3. At checkout:

   * allocate `run_id`
   * register ephemeral tables with `tmp_{run_id}__...`
4. Execute via:

   * `execute_stream()` / async iteration for large results ([datafusion.apache.org][5])
   * `collect()` only for small results ([datafusion.apache.org][5])
5. Cleanup:

   * `deregister_table` all `tmp_{run_id}__*` ([arrow.staged.apache.org][4])

### Blueprint: read-only SQL surface for untrusted queries

If you allow arbitrary SQL from users/agents:

* run with `sql_with_options(..., SQLOptions(...))` and disable DDL/DML/Statements ([datafusion.apache.org][7])
* pre-register only approved tables/providers

---

## 2.7 What we should deep dive next

The next chapter that naturally follows this is:

**3) Streaming-first data interchange: Arrow C Stream as the “wire ABI”**
Because once your lifecycle + isolation is correct, the next bottleneck/footgun is **materializing results** (collect/to_arrow_table) vs pushing streams across Python↔Rust boundaries (`__arrow_c_stream__`, `execute_stream`, partitioned streams). ([datafusion.apache.org][5])

[1]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.SessionContext.html "datafusion.SessionContext — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/record_batch/index.html?utm_source=chatgpt.com "datafusion.record_batch"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"
[8]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[9]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"

## 3) Streaming-first data interchange: Arrow C Stream as the “wire ABI”

### 3.1 Pick the *streaming* surface first (and make it your default)

DataFusion gives you **three** practical “stream out” surfaces; only one is truly universal across Python + Rust:

1. **`__arrow_c_stream__` (PyCapsule / Arrow C Stream interface)**

   * DataFusion DataFrames implement it; calling it triggers execution and yields **record batches incrementally** (lazy, pull-based). ([datafusion.apache.org][1])
   * This is the closest thing to a *stable ABI* in Python-land because consumers only need the protocol, not DataFusion-specific types. ([Apache Arrow][2])

2. **`execute_stream()` / `execute_stream_partitioned()` (DataFusion-native streaming)**

   * `execute_stream()` returns a `RecordBatchStream` over a single partition; `execute_stream_partitioned()` returns one stream per partition. ([datafusion.apache.org][3])
   * Best when you want explicit partition control / async polling. ([datafusion.apache.org][4])

3. **Materialization (`collect()`, `to_arrow_table()`, `pa.table(df)`, `to_pandas()`, …)**

   * `collect()` returns all batches (and ordering is *not guaranteed* unless you explicitly sort). ([Apache Arrow][5])
   * `to_arrow_table()` and `pa.table(df)` are explicitly *eager*: they collect everything into a single table. ([Apache Arrow][5])

**Best-in-class API default:** return a *stream*, not a table. Concretely: return an object that implements `__arrow_c_stream__` (DataFusion `DataFrame` already does), and provide opt-in helpers that materialize.

---

### 3.2 The “universal adapter”: `pyarrow.RecordBatchReader.from_stream(obj)`

PyArrow exposes a standard importer:

```python
import pyarrow as pa

reader = pa.RecordBatchReader.from_stream(obj)
for batch in reader:
    ...  # process as produced
```

* `RecordBatchStreamReader.from_stream` accepts any object implementing the Arrow PyCapsule stream protocol, i.e. has `__arrow_c_stream__`. ([Apache Arrow][6])
* DataFusion explicitly recommends this exact pattern for DataFrames, and clarifies `pa.table(df)` is eager (collects entire result). ([datafusion.apache.org][4])

**Actionable design rule:** standardize every “data-out” boundary in your Python service as either:

* **(A)** “ArrowStreamExportable” (anything with `__arrow_c_stream__`), or
* **(B)** `pyarrow.RecordBatchReader` (which itself is stream-compatible and iterable). ([Apache Arrow][7])

---

### 3.3 Treat Arrow C Stream / PyCapsule as a stable ABI (what is actually standardized)

The Arrow PyCapsule interface specification standardizes:

* **Capsule names** (for streams: `arrow_array_stream`) and
* **lifetime semantics**: capsules should carry destructors calling the release callback to prevent leaks if never consumed. ([Apache Arrow][2])

This matters operationally because it’s what makes this a real ABI boundary:

* producer and consumer do **not** need compile-time knowledge of each other
* resource cleanup is defined even in “dropped without consuming” cases ([Apache Arrow][2])

---

## 3.4 Rust extensions ⇄ DataFusion Python via Arrow C Stream

### A) **Python → Rust**: accept *any* `__arrow_c_stream__` producer (DataFusion DF, PyArrow reader, Polars DF, …)

**Recommended Rust strategy:** don’t special-case DataFusion; accept the protocol.

The `arrow_pyarrow` crate documents that:

* `pyarrow.RecordBatchReader` can be imported as an `ArrowArrayStreamReader`
* and any object implementing the ArrayStream PyCapsule interface (e.g., `pyarrow.Table`) can be imported via `PyArrowType<ArrowArrayStreamReader>`—and that streaming is preferred over eager `Vec<RecordBatch>`. ([Apache Arrow][8])

Minimal shape (illustrative; adapt imports/versioning to your stack):

```rust
use pyo3::prelude::*;
use arrow_pyarrow::PyArrowType;
use arrow::ffi_stream::ArrowArrayStreamReader;

#[pyfunction]
fn consume_stream(obj: PyObject) -> PyResult<usize> {
    Python::with_gil(|py| {
        // obj can be a DataFusion DataFrame, a pyarrow.RecordBatchReader, etc.
        let PyArrowType(mut reader): PyArrowType<ArrowArrayStreamReader> = obj.extract(py)?;
        let mut rows = 0usize;

        while let Some(batch) = reader.next() {
            let batch = batch.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            rows += batch.num_rows();
        }
        Ok(rows)
    })
}
```

If you ever need to drop down to raw FFI, Arrow provides `ArrowArrayStreamReader::from_raw(*mut FFI_ArrowArrayStream)` (unsafe) as the primitive for importing a C stream pointer. ([Docs.rs][9])

**Operational constraint:** PyCapsule streams are pointer-based in-process handoffs (not a network format). The spec explicitly frames capsules as wrappers around C interface pointers with destructors for safety. ([Apache Arrow][2])

---

### B) **Rust → Python → DataFusion**: return a stream object, then let DataFusion ingest it

On the Python side, DataFusion can create a DataFrame from *any* Arrow producer that implements either `__arrow_c_stream__` or `__arrow_c_array__`:

```python
df = ctx.from_arrow(obj)   # obj can be a stream-exportable object
```

([datafusion.apache.org][10])

So your Rust extension should return one of:

* a `pyarrow.RecordBatchReader` (best interop), or
* a Python object implementing `__arrow_c_stream__`.

`arrow_pyarrow` explicitly calls out exporting either an `ArrowArrayStreamReader` or `Box<dyn RecordBatchReader + Send>` as `pyarrow.RecordBatchReader` (and notes the boxed reader is often easier). ([Apache Arrow][8])

This is the clean “wire ABI” loop:

**Rust** produces RecordBatch stream → **Python** receives `RecordBatchReader` → **DataFusion** ingests via `from_arrow()` → downstream can re-export via `__arrow_c_stream__`. ([datafusion.apache.org][10])

---

## 3.5 DataFusion results ⇄ delta-rs writers without `to_arrow_table()`

### A) **The zero-drama path**: DataFusion DF → `RecordBatchReader` → `write_deltalake`

`write_deltalake` explicitly accepts:

* Pandas DF, PyArrow Table, **iterator of PyArrow RecordBatches** (streaming), and API ref also lists **`pyarrow.RecordBatchReader`**. ([delta-io.github.io][11])

So do:

```python
import pyarrow as pa
from deltalake import write_deltalake

reader = pa.RecordBatchReader.from_stream(df)   # streaming, no full materialization
write_deltalake(table_uri, reader, mode="append")
```

* DataFusion documents that `RecordBatchReader.from_stream(df)` avoids materializing all batches, while `pa.table(df)` is eager. ([datafusion.apache.org][4])
* delta-rs documents streaming inputs directly (iterator of RecordBatches / RecordBatchReader). ([delta-io.github.io][11])

### B) **If you need per-partition control**: `execute_stream_partitioned()` + controlled append

DataFusion exposes one stream per partition:

```python
streams = df.execute_stream_partitioned()
```

([datafusion.apache.org][4])

If you *append* partition-by-partition (multiple commits), you’ll trade throughput vs transaction overhead. Usually prefer a single writer call unless you have a strong reason.

### C) Writer-side batching knobs (Delta writes are not “just Arrow batches”)

delta-rs lets you tune Parquet writing via `WriterProperties` (and bloom filters / per-column props). ([delta-io.github.io][11])

This is separate from DataFusion’s `batch_size`. In practice:

* DataFusion batch size controls compute vectorization + spill readback chunking
* delta-rs writer properties control Parquet encoding/stats/bloom behavior and file layout ([datafusion.apache.org][12])

---

## 3.6 Backpressure + batching policy

### A) Backpressure: keep it pull-based end-to-end

Backpressure “works” when **the consumer controls `next()`**:

* DataFusion’s Arrow export is lazy and explicitly says downstream readers pull batches on demand. ([datafusion.apache.org][1])
* PyArrow’s `from_stream()` reads batches as you iterate. ([Apache Arrow][6])

**Anti-patterns (silently materialize):**

* `pa.table(df)` (eager) ([datafusion.apache.org][4])
* `reader.read_all()` (turns stream into a table) ([Apache Arrow][6])
* `df.to_arrow_table()` / `df.to_pandas()` (collect+convert) ([Apache Arrow][5])

### B) Batch sizing: tune `target_partitions` *and* `batch_size` together

DataFusion’s tuning guide is explicit for memory-limited streaming workloads:

* with `FairSpillPool`, memory is divided among partitions
* higher `target_partitions` ⇒ less memory per partition ⇒ more frequent spill path
* when spilling, data is read back in `batch_size` chunks; lowering it can reduce follow-on spills ([datafusion.apache.org][13])

Concrete SQL knobs:

```sql
SET datafusion.execution.target_partitions = 4;
SET datafusion.execution.batch_size = 1024;
```

([datafusion.apache.org][12])

**Service default rule of thumb:**

* pick a conservative `target_partitions` per in-flight query (avoid oversubscription)
* keep `batch_size` high (vectorization) unless memory/spill behavior forces it down ([datafusion.apache.org][12])

### C) Partitioned output streams: throughput vs determinism

`execute_stream_partitioned()` returns one stream per partition, and DataFusion shows the recommended pattern: consume each stream in its own asyncio task. ([datafusion.apache.org][4])

```python
import asyncio

async def consume(stream):
    async for batch in stream:
        ...

streams = list(df.execute_stream_partitioned())
await asyncio.gather(*(consume(s) for s in streams))
```

([datafusion.apache.org][4])

This is the highest-throughput pattern because you preserve partition parallelism while maintaining backpressure per partition.

---

## 3.7 Deterministic ordering constraints (don’t accidentally assume order)

DataFusion’s API doc is blunt: **unless some order is specified in the plan, result order is not guaranteed**. ([Apache Arrow][5])

Implications for “streaming as ABI”:

* If downstream logic depends on stable row order (IDs, merges, “first seen wins”), you **must** impose ordering (`ORDER BY` / `sort`) and often reduce to a single output stream (at the cost of parallelism). ([Apache Arrow][5])
* If you only need deterministic *partition assignment* (e.g., stable sharding), use hash repartitioning and then consume partitioned streams; DataFusion exposes `execute_stream_partitioned()` specifically for “partition boundaries are important.” ([datafusion.apache.org][4])

---

## 3.8 What I would standardize as your “wire contract” (Python deployment + Rust acceleration)

### Contract

* **Inputs:** accept any object implementing `__arrow_c_stream__` (Arrow stream exportable). ([Apache Arrow][2])
* **Outputs:** return either:

  * DataFusion `DataFrame` (already stream exportable), or
  * `pyarrow.RecordBatchReader` (portable to delta-rs and back into DataFusion via `from_arrow`). ([datafusion.apache.org][4])

### Two mandatory helper APIs

* `as_reader(x) -> pa.RecordBatchReader`: `pa.RecordBatchReader.from_stream(x)` ([Apache Arrow][6])
* `write_delta(uri, x, **opts)`: `write_deltalake(uri, as_reader(x), ...)` ([delta-io.github.io][11])

This gives you a single “wire ABI” that composes:
**Rust extensions ⇄ DataFusion Python ⇄ delta-rs** without forcing eager tables anywhere.

[1]: https://datafusion.apache.org/python/user-guide/io/arrow.html "Arrow — Apache Arrow DataFusion  documentation"
[2]: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html "The Arrow PyCapsule Interface — Apache Arrow v23.0.0"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[5]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.DataFrame.html "datafusion.DataFrame — Apache Arrow DataFusion  documentation"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchStreamReader.html "pyarrow.ipc.RecordBatchStreamReader — Apache Arrow v23.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html "pyarrow.RecordBatchReader — Apache Arrow v23.0.0"
[8]: https://arrow.apache.org/rust/arrow_pyarrow/index.html "arrow_pyarrow - Rust"
[9]: https://docs.rs/arrow/latest/arrow/array/ffi_stream/struct.ArrowArrayStreamReader.html?utm_source=chatgpt.com "ArrowArrayStreamReader in arrow::array::ffi_stream - Rust"
[10]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[11]: https://delta-io.github.io/delta-rs/usage/writing/ "Writing Delta Tables - Delta Lake Documentation"
[12]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[13]: https://datafusion.apache.org/_sources/user-guide/configs.md.txt "datafusion.apache.org"

## 4) Engine-level caching & statistics: DataFusion runtime cache stack

### 4.0 The bottleneck you’re actually fixing

For “DataFusion + Delta on object stores”, latency is often dominated by **metadata IO**, not scanning:

* **Directory/listing**: object-store `LIST` + per-object `HEAD` / metadata fetch (many objects ⇒ many round-trips)
* **Parquet metadata**: footer + stats + (optionally) page index decoding
* **Statistics**: used for pruning and planning; often re-derived repeatedly unless cached

DataFusion’s cache stack exists to make repeated queries over the same locations stop paying those costs. ([datafusion.apache.org][1])

---

# 4.1 Cache stack map (what caches exist, what they cache, and what breaks)

Think “outer → inner”:

```
ListingTable (directory/table location)
  ├─ ListFilesCache        (LIST + ObjectMeta results)
  ├─ FileStatisticsCache   (per-file statistics used when listing/planning)
  └─ FileMetadataCache     (Parquet footers/page indexes/etc)
        └─ Parquet predicate cache (filter pushdown intermediate cache)
```

### A) `ListFilesCache` (listing + object metadata)

**What it saves:** repeated “list this path” + object metadata fetches for the same table location (remote stores benefit most). ([datafusion.apache.org][2])

**Staleness semantics (this is the footgun):**

* If enabled, DataFusion **won’t see added/removed files** until the cache entry expires; default TTL can be infinite (“no expiration”). ([datafusion.apache.org][3])
* You *must* decide whether you want “fast but stale” or “fresh but slower”.

**Key runtime knobs (SQL `SET`):**

* `datafusion.runtime.list_files_cache_limit` (default `1M`)
* `datafusion.runtime.list_files_cache_ttl` (default `NULL` / infinite) ([datafusion.apache.org][4])

**Disable (when you need fresh listings):**

```sql
SET datafusion.runtime.list_files_cache_limit TO "0K";
```

([datafusion.apache.org][3])

### B) `FileMetadataCache` (Parquet footer + stats + page index)

**What it saves:** repeated reads/decodes of Parquet file metadata (huge win for “many small point reads” / repeated queries). DataFusion 50+ calls out automatic caching for ListingTable scans. ([datafusion.apache.org][1])

**Correctness semantics:**

* Default cache is **LRU** with default limit ~`50MB`.
* If the underlying file changes, the cache is **automatically invalidated**. ([datafusion.apache.org][1])

**Key runtime knob (SQL `SET`):**

* `datafusion.runtime.metadata_cache_limit` (default `50M`) ([datafusion.apache.org][4])

**Disable (useful for multi-tenant “no cross-request residue”):**

* Setting limit to `0` disables metadata caching. ([datafusion.apache.org][1])

### C) `FileStatisticsCache` (stats used during listing/planning)

**What it saves:** repeated extraction/reading of per-file statistics when listing files (currently “Parquet only” per docs). ([Docs.rs][5])

**Important gating knob:** stats aren’t even collected unless enabled:

* CLI docs explicitly: `datafusion.execution.collect_statistics` must be enabled to populate the statistics cache. ([datafusion.apache.org][2])

### D) Parquet predicate cache (filter pushdown path)

If you enable filter pushdown, DataFusion can cache intermediate predicate evaluation results; the key is:

* `datafusion.execution.parquet.max_predicate_cache_size` (0 disables; NULL uses parquet reader default) ([datafusion.apache.org][4])

This is a *memory trade*: useful for repeated predicate evaluation within read/decoding, but can blow up memory if you don’t cap it.

---

# 4.2 Runtime configuration keys: what can be set via SQL vs what you should “pin”

### A) What DataFusion explicitly supports changing via SQL `SET`

DataFusion documents that configuration options can be set **programmatically**, via **environment**, or via SQL `SET`. Runtime settings *specifically* are also settable via `SET`. ([datafusion.apache.org][4])

**Runtime keys you’ll care about most (defaults included):**

* `datafusion.runtime.list_files_cache_limit` = `1M`
* `datafusion.runtime.list_files_cache_ttl` = `NULL` (infinite)
* `datafusion.runtime.metadata_cache_limit` = `50M`
* `datafusion.runtime.memory_limit` = `NULL`
* `datafusion.runtime.temp_directory` = `NULL`
* `datafusion.runtime.max_temp_directory_size` = `100G` ([datafusion.apache.org][4])

### B) What you must “pin” in a Python service (even if SQL can set it)

In a service, “SET is allowed” is a security + correctness hazard:

* pooled contexts + untrusted queries can mutate cache TTL/limits and silently affect later requests
* your performance regressions become non-reproducible

**Pinning rule:** treat these as *baseline policy* configured at context creation (or at pool checkout), and disallow SQL statements that can change them.

In DataFusion Python, you can block `SET` by running queries with `SQLOptions.with_allow_statements(False)`. ([datafusion.apache.org][6])

---

# 4.3 Python control surface: what you can actually control without Rust embedding

### A) Python *can* set config keys (including runtime.*) via `SessionConfig.set`

`SessionConfig.set(key: str, value: str)` exists and returns a new config object. ([datafusion.apache.org][6])

So your “cache profile” in Python should be a dictionary of config keys + values, applied to `SessionConfig` once at context construction.

**Example: a sane “remote object store + repeated queries” profile**

```python
from datafusion import SessionConfig, SessionContext, RuntimeEnvBuilder

cfg = (
    SessionConfig()
    # runtime caches
    .set("datafusion.runtime.list_files_cache_limit", "64M")
    .set("datafusion.runtime.list_files_cache_ttl", "30s")  # freshness bound
    .set("datafusion.runtime.metadata_cache_limit", "256M")
    # statistics + pruning
    .set("datafusion.execution.collect_statistics", "true")
    .with_parquet_pruning(True)
    # optional: predicate cache cap if you enable pushdown filters elsewhere
    .set("datafusion.execution.parquet.max_predicate_cache_size", "64M")
)

rt = (
    RuntimeEnvBuilder()
    .with_disk_manager_os()
    .with_greedy_memory_pool(8 * 1024**3)
)

ctx = SessionContext(cfg, rt)
```

Notes:

* `with_parquet_pruning(True)` is the “row-group skipping” toggle at the Python API level. ([datafusion.apache.org][7])
* `max_predicate_cache_size` is only relevant when you enable filter pushdown. ([datafusion.apache.org][4])

### B) Python cannot (currently) inject custom cache implementations

The Python `RuntimeEnvBuilder` surface exposes disk manager + memory pool knobs, but **not** `CacheManagerConfig` injection. ([datafusion.apache.org][6])

So in pure Python you realistically control:

* cache **limits / TTLs** (via config keys)
* whether caches are effectively **disabled** (set limit to `0`)
* whether `SET` is allowed (use `SQLOptions`) ([datafusion.apache.org][3])

---

# 4.4 Staleness policies that won’t surprise you later

### Policy 1: “Delta snapshot pinned” (best for correctness + cacheability)

If your reads are pinned to a version/time-travel snapshot, you *want* stable file lists:

* `list_files_cache_ttl = NULL` (infinite) is acceptable
* large `list_files_cache_limit` is a win
* metadata cache is “safe” because Parquet files are immutable in typical Delta workflows

This aligns with the documented “no expiration means you won’t see added/removed files until provider is recreated” behavior. ([datafusion.apache.org][3])

### Policy 2: “Always-latest table” (best for freshness)

If you want “latest commits visible quickly”:

* set `list_files_cache_ttl` to something explicit (e.g., `10s`, `30s`, `2m`) ([datafusion.apache.org][4])
* or disable list-files caching entirely (`0K`) when correctness demands it ([datafusion.apache.org][3])

### Policy 3: Multi-tenant isolation (no cross-request cache residue)

* set `metadata_cache_limit = 0` (disable metadata caching) ([datafusion.apache.org][1])
* potentially disable `list_files_cache` too
* or use **context-per-tenant** (from chapter 2) so caches never cross tenants

---

# 4.5 Introspection & debugging: prove caches are doing something

### A) Confirm config at runtime

DataFusion CLI docs show:

* `SHOW ALL` to list settings
* `SHOW <key>` to inspect specific values
* `SET <key> = <value>` to change them ([datafusion.apache.org][8])

In a service, you can still run:

```sql
SHOW datafusion.runtime.metadata_cache_limit;
SHOW datafusion.runtime.list_files_cache_limit;
SHOW datafusion.runtime.list_files_cache_ttl;
```

(Use *read-only* SQL options if you accept user SQL.) ([datafusion.apache.org][6])

### B) Inspect cache contents (best done in `datafusion-cli`)

DataFusion CLI exposes table functions:

* `metadata_cache()` (file metadata cache)
* `statistics_cache()` (file statistics cache)
* `list_files_cache()` (list files cache) ([datafusion.apache.org][2])

Example:

```sql
SELECT * FROM metadata_cache();
SELECT * FROM list_files_cache();
SELECT * FROM statistics_cache();
```

([datafusion.apache.org][2])

---

# 4.6 Rust embedding boundary: CacheManagerConfig and custom caches (what “best-in-class” eventually means)

### A) Where caches actually live (Rust-level architecture)

A `RuntimeEnv` manages memory, disk, cache manager, and object store registry. ([Docs.rs][9])
The `CacheManager` “provides default cache implementations” and allows custom ones via traits. ([Docs.rs][10])

### B) The configuration object you override: `CacheManagerConfig`

`CacheManagerConfig` fields and semantics are explicit:

* list files cache has a TTL; changes to underlying storage are not visible until entry expires; default TTL can be infinite
* metadata cache limit is a byte limit
* file statistics cache is optional (default disabled) ([Docs.rs][5])

### C) Default implementations you can reuse in Rust

DataFusion provides a `DefaultListFilesCache` with LRU eviction and TTL support. ([Docs.rs][11])
It also exposes `FileMetadataCacheEntry` to surface cache state (useful for your own introspection). ([Docs.rs][12])

### D) The “Python cannot do this” step: inject custom caches

In Rust, you can build a `RuntimeEnv` with a custom `CacheManagerConfig` (and then either embed DataFusion fully in Rust, or share the runtime across sessions). This is the seam where “disk-backed cache”, “shared cache across pooled contexts”, or “tenant-keyed caches” live. ([Docs.rs][5])

**Practical integration recommendation (matches your chapter-3 streaming ABI):**

* if you need custom caches, implement the query engine in Rust (DataFusion + delta-rs), configure `RuntimeEnv`/`CacheManagerConfig` there, and expose **Arrow C Stream** results to Python
* don’t fight the Python bindings’ lack of cache injection; treat Python as orchestration and Rust as the “engine” boundary

(You already have the packaging + streaming groundwork to make this clean.)

---

## “Do this next” checklist (agent-ready)

1. **Define cache profiles** as code (strings):

   * `remote_fast_stale`, `remote_fast_ttl30s`, `fresh_no_list_cache`, `multi_tenant_no_caches`
2. Apply via `SessionConfig.set(...)` in your SessionContext factory. ([datafusion.apache.org][6])
3. If you accept user SQL, run it with `SQLOptions.with_allow_statements(False)` to prevent `SET`. ([datafusion.apache.org][6])
4. Add a CI perf harness that runs the same query twice and asserts:

   * second run has fewer object-store round trips (via your telemetry) **or**
   * `datafusion-cli` shows cache hits via `metadata_cache()`/`list_files_cache()`. ([datafusion.apache.org][2])
5. If you outgrow Python-only control, move cache injection into Rust and export streams (chapter 3). ([Docs.rs][10])

[1]: https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/?utm_source=chatgpt.com "Apache DataFusion 50.0.0 Released"
[2]: https://datafusion.apache.org/user-guide/cli/functions.html?utm_source=chatgpt.com "CLI Specific Functions — Apache DataFusion documentation"
[3]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[4]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[5]: https://docs.rs/datafusion/latest/datafusion/execution/cache/cache_manager/struct.CacheManagerConfig.html "CacheManagerConfig in datafusion::execution::cache::cache_manager - Rust"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/index.html?utm_source=chatgpt.com "Apache Arrow DataFusion documentation"
[8]: https://datafusion.apache.org/user-guide/cli/usage.html?utm_source=chatgpt.com "Usage — Apache DataFusion documentation"
[9]: https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeEnv.html?utm_source=chatgpt.com "RuntimeEnv in datafusion::execution::runtime_env - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/execution/cache/cache_manager/struct.CacheManager.html?utm_source=chatgpt.com "CacheManager in datafusion::execution - cache_manager"
[11]: https://docs.rs/datafusion/latest/datafusion/execution/cache/struct.DefaultListFilesCache.html?utm_source=chatgpt.com "DefaultListFilesCache in datafusion::execution::cache - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/execution/cache/cache_manager/struct.FileMetadataCacheEntry.html?utm_source=chatgpt.com "FileMetadataCacheEntry in datafusion::execution::cache"

## 5) Observability: query metrics, tracing, and production debugging

### 5.1 DataFusion operator metrics: where they surface + how to use them

#### A) SQL surface: `EXPLAIN`, `EXPLAIN ANALYZE`, `EXPLAIN ANALYZE VERBOSE`

**Plan-only (no execution):**

```sql
EXPLAIN SELECT ...;
EXPLAIN VERBOSE SELECT ...;   -- more debug detail / intermediate plans
```

`EXPLAIN` syntax + formats are documented (tree/indent/etc). ([Apache DataFusion][1])

**Plan + runtime metrics (executes query, discards results):**

```sql
EXPLAIN ANALYZE SELECT ...;
EXPLAIN ANALYZE VERBOSE SELECT ...;   -- per-partition metrics
```

* `EXPLAIN ANALYZE` shows *plan with `metrics=[...]` attached per operator* and supports `VERBOSE` for per-partition metrics. ([Apache DataFusion][1])
* DataFusion’s “Reading Explain Plans” guide is the canonical “how to interpret the metrics” reference (including that times/counters are summed across cores). ([Apache DataFusion][2])

**Controlling verbosity (production sanity):**

```sql
SET datafusion.explain.analyze_level = summary;  -- concise
SET datafusion.explain.analyze_level = dev;      -- full metrics
```

This option was explicitly introduced to reduce `EXPLAIN ANALYZE` verbosity and is now a first-class knob. ([Apache DataFusion][3])

#### B) Python surface: `DataFrame.explain(verbose=False, analyze=False)` (prints, returns `None`)

In DataFusion Python, `DataFrame.explain(...)` **prints** the plan, and with `analyze=True` it runs and prints metrics. ([Apache DataFusion][4])

```python
df.explain()                         # plan only
df.explain(verbose=True)             # more detail
df.explain(analyze=True)             # plan + metrics (executes, discards results)
df.explain(verbose=True, analyze=True)
```

#### C) Metrics taxonomy you should standardize around

DataFusion exposes “baseline” metrics like:

* `elapsed_compute` (CPU time in the operator)
* `output_rows`, `output_batches`, `output_bytes` ([Apache DataFusion][5])

And operator-specific metrics (esp. scans + pruning):

* `bytes_scanned`, `metadata_load_time`, pruning counters (row groups pruned, etc.) ([Apache DataFusion][2])

**Production reading rule (fast triage):** scan operators bottom-up and look for “big” in:

* `metadata_load_time` (often object-store / footer IO bound)
* `bytes_scanned` (pushdown/pruning failing)
* `elapsed_compute` concentrated in Filter/Sort/Aggregate/Join (CPU bound) ([Apache DataFusion][2])

---

### 5.2 “Metrics tables”: turning metrics into structured rows (not just printed text)

You’ll want *tabular* metrics for:

* regressions (“operator X got 3× slower”)
* SLOs (p95 scan CPU vs join CPU)
* dashboards (bytes scanned, rows pruned, spill counts)

You have two realistic implementations.

#### A) Python-only (cheap, but second-run + parsing)

Because `EXPLAIN ANALYZE` discards results by design, a Python-only service typically does:

1. Run query normally (stream results).
2. If slow / sampled, run a **second** `EXPLAIN ANALYZE` and parse the printed plan into a metrics table.

**Important constraint:** DataFusion users have explicitly asked for “results + explain analyze in one run”, implying it’s not generally available as a single API today. ([GitHub][6])

**Minimal pattern:**

* Maintain a slow-query threshold (e.g. 2s) and/or sampling rate (e.g. 1%).
* On trigger: execute `EXPLAIN ANALYZE` with `analyze_level=summary` to keep overhead bounded. ([Apache DataFusion][3])

#### B) Rust embedding (best-in-class): traverse `ExecutionPlan` and emit an Arrow table

This is the *correct* solution if you’re building a serious platform.

DataFusion’s Rust `ExecutionPlan` trait exposes:

* `metrics() -> Option<MetricsSet>` (a stable snapshot of operator metrics) ([Docs.rs][7])
* The metrics types are explicitly stabilized via `datafusion::physical_plan::metrics` re-exports. ([Docs.rs][8])

**Design: “metrics exporter” as a Rust function you call from Python**

* Input: physical plan root (`Arc<dyn ExecutionPlan>`)
* Output: `RecordBatch` (or `pyarrow.Table`) with schema like:

```text
(query_id, node_id, operator, partition, metric_name, metric_value, unit, labels_json)
```

**Core algorithm (Rust):**

* Depth-first walk the plan tree (`children()`), assign stable `node_id` path (e.g. `0.1.0`).
* For each node, call `plan.metrics()`; flatten `MetricsSet` into rows.
* Return Arrow batches via the Arrow C Stream ABI (your chapter 3 baseline).

This gives you:

* **one-run** metrics capture (no second execution)
* structured metrics that are joinable to traces/logs

---

### 5.3 Distributed tracing for DataFusion: two tiers

#### Tier 1 (no code changes): DataFusion CLI profiling & IO traces

DataFusion CLI now has a built-in **object store profiling mode**:

* CLI flag: `--object-store-profiling [disabled|summary|trace]`
* Interactive: `\object_store_profiling [disabled|summary|trace]`
  and it prints detailed object store operations performed during execution. ([Apache DataFusion][9])

Use this when you’re diagnosing “why so many LIST/GETs?” without instrumenting your service.

#### Tier 2 (engine-integrated): `datafusion-tracing` extension + `tracing`/OTel

The community `datafusion-tracing` project instruments DataFusion query execution with Rust `tracing` spans and integrates with OpenTelemetry; it can also record DataFusion metrics and preview partial results. ([GitHub][10])

**Key integration point:** it’s attached via `SessionStateBuilder` / physical optimizer instrumentation rules (so spans appear around plan construction + execution). ([GitHub][10])

**Production recommendation**

* If you’re already embedding DataFusion in Rust (for caching/metrics reasons), adopt `datafusion-tracing` and expose traces via OTLP to your collector.
* If you’re Python-first (DataFusion Python bindings), treat DataFusion as a “black box” and rely on:

  * query-level spans in Python
  * object-store profiling in CLI for deep digs
  * optional Rust sidecar “engine module” when you need full fidelity.

---

### 5.4 Storage IO spans: “instrument the object store, not the query”

If you can wrap the Rust `object_store::ObjectStore`, you get precise spans for:

* `LIST`, `HEAD`, `GET` ranges, sizes, durations, errors

The `instrumented_object_store` crate exists specifically for this:

* It wraps any ObjectStore and adds tracing spans for operations, recording paths/sizes/errors and working with OpenTelemetry. ([Docs.rs][11])
* It shows how to register the instrumented store into a DataFusion `SessionContext`. ([Docs.rs][11])

This is the most direct way to answer: “was it slow because of scanning, parquet footer reads, LIST storms, or network?”

---

### 5.5 delta-rs OpenTelemetry: enable it in Python (today)

The `deltalake` Python package explicitly supports OpenTelemetry tracing. The documented enablement flow is:

```python
import os
import deltalake
from deltalake import write_deltalake, DeltaTable

os.environ["RUST_LOG"] = "deltalake=debug"
# tracing uses a default HTTP endpoint or OTEL_EXPORTER_OTLP_ENDPOINT
# auth via OTEL_EXPORTER_OTLP_HEADERS (e.g. honeycomb header)
deltalake.init_tracing()

write_deltalake("my_table", data)
dt = DeltaTable("my_table")
df = dt.to_pandas()
```

This is directly documented (including `RUST_LOG`, `OTEL_EXPORTER_OTLP_ENDPOINT`, and `OTEL_EXPORTER_OTLP_HEADERS`). ([PyPI][12])

**Operational rule:** call `deltalake.init_tracing()` once at process start (not per request). Then treat delta operations as “spanned” automatically.

---

### 5.6 Minimum observability contract for a Python DataFusion+Delta service

You want enough signal that any “slow/wrong” ticket can be answered from artifacts *without reproducing*.

#### Contract outputs (per query / per run)

1. **Correlation IDs**

* `run_id` (your stable run correlation)
* `query_id` (unique per query)
* `session_id` (DataFusion session id if you expose it; useful for pooling boundaries)

2. **Trace**

* One **root span per query** in Python (`query_id`, `run_id`, tenant, dataset)
* Child spans:

  * `plan` (build/optimize)
  * `execute` (streaming duration)
  * `write_delta` / `read_delta` (delta operations)

3. **Plan snapshots**

* logical plan and physical plan snapshots:

  * `df.explain(verbose=True, analyze=False)` (printed plan)
  * optionally physical plan via `df.execution_plan()` (you can display it; Python exposes the physical plan object). ([Apache DataFusion][4])

4. **Metrics**

* Always: wall-clock duration, rows returned/produced
* On slow/sampled:

  * `EXPLAIN ANALYZE` summary (or one-run Rust metrics table if embedded) ([Apache DataFusion][3])

5. **Storage IO visibility**

* Either:

  * instrumented object store spans (best) ([Docs.rs][11])
  * or CLI `object_store_profiling` traces for offline reproduction ([Apache DataFusion][9])

#### Collector/export plumbing (keep it boring)

OpenTelemetry strongly recommends sending to an OpenTelemetry Collector in production; the docs provide a concrete “run collector via docker” example for OTLP. ([OpenTelemetry][13])
Use this as your baseline so:

* Python OTel exporters and Rust OTel exporters both talk to the same OTLP endpoint
* your backend (Datadog / Honeycomb / etc.) is configured in the collector, not in-app

**Cross-language correlation reality check:** Python OTel spans and Rust OTel spans may not automatically share a single trace context. Don’t bet your operability on that. Instead:

* include `run_id` + `query_id` as span attributes everywhere (Python spans, DataFusion tracing spans, delta spans)
* that makes joining in the backend deterministic even if trace IDs differ

---

### 5.7 “Do this next” implementation checklist (high ROI)

1. **Pin `datafusion.explain.analyze_level=summary`** in your service config profile (avoid verbose default). ([Apache DataFusion][3])
2. **Implement slow-query capture**:

   * record `df.explain(verbose=True, analyze=False)` always
   * if `duration > threshold`: run `EXPLAIN ANALYZE` and store the output (or export Rust metrics table) ([Apache DataFusion][2])
3. **Enable delta tracing in production** via `deltalake.init_tracing()` + OTLP env vars. ([PyPI][12])
4. **Add storage IO spans**:

   * if you embed Rust: wrap object stores with `instrumented_object_store` and register them into DataFusion. ([Docs.rs][11])
   * if not: use `datafusion-cli --object-store-profiling trace` for reproduction runs. ([Apache DataFusion][9])
5. **(Best-in-class)**: move “metrics table export” into Rust by traversing `ExecutionPlan::metrics()` and emitting Arrow batches. ([Docs.rs][7])

If you want to continue linearly: the next chapter after observability is usually **6) Object-store & credential hardening (shared config across delta-rs + DataFusion)**, because once you can *see* IO, you’ll want to *control* it (timeouts/retries/endpoints/credential sources) coherently across both stacks.

[1]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/blog/output/2025/11/25/datafusion-51.0.0/ "Apache DataFusion 51.0.0 Released - Apache DataFusion Blog"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/metrics.html "Metrics — Apache DataFusion  documentation"
[6]: https://github.com/apache/datafusion/discussions/13096?utm_source=chatgpt.com "Getting results and explain analyze plan at the same time"
[7]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html "ExecutionPlan in datafusion::physical_plan - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/physical_plan/metrics/index.html "datafusion::physical_plan::metrics - Rust"
[9]: https://datafusion.apache.org/user-guide/cli/usage.html?utm_source=chatgpt.com "Usage — Apache DataFusion documentation"
[10]: https://github.com/datafusion-contrib/datafusion-tracing "GitHub - datafusion-contrib/datafusion-tracing: Integration of opentelemetry with the tracing crate"
[11]: https://docs.rs/instrumented-object-store?utm_source=chatgpt.com "instrumented_object_store - Rust"
[12]: https://pypi.org/project/deltalake/ "deltalake · PyPI"
[13]: https://opentelemetry.io/docs/languages/python/exporters/?utm_source=chatgpt.com "Exporters"

## 6) Object-store & credential hardening across DataFusion + delta-rs

The goal is **one storage/credential policy** that drives **both**:

* DataFusion file/table scans (Parquet/JSON/CSV “s3://…”, “gs://…”, “az://…”) via `register_object_store`, and
* delta-rs table reads/writes (DeltaTable + write APIs) via `storage_options` + env.

If you don’t unify this, you get classic production failures: “DataFusion can read the Parquet but DeltaTable can’t”, different endpoints for MinIO/LocalStack, mismatched timeouts/retries, and non-reproducible auth.

---

### 6.1 Canonical identity: URL scheme + prefix rules (this is non-negotiable)

#### A) DataFusion: **register by scheme prefix**, not by bucket

DataFusion’s Python user guide shows the canonical pattern:

```python
ctx.register_object_store("s3://", s3, None)
ctx.register_parquet("trips", "s3://yellow-trips/")
```

…and explicitly uses `"s3://"` as the prefix, not `"s3://bucket"`. ([Apache DataFusion][1])

There is a real-world footgun: registering the object store with a bucket-specific prefix (e.g. `"s3://my-bucket"`) can fail with “No suitable object store found”, while `"s3://"` works. ([GitHub][2])
**Rule:** always register at the scheme root (`"s3://"`, `"gs://"`, `"az://"`/etc) unless you *really* know the resolver behavior you’re depending on.

#### B) delta-rs: backend selection comes from URL scheme

delta-rs documents:

* `storage_options` is backend-specific, and backend is derived from the URL you pass (S3, Azure, GCS schemes). ([delta-io.github.io][3])
* S3 URIs include `s3://…` and `s3a://…`. ([delta-io.github.io][3])

**Rule:** pick *one* canonical scheme for a given Delta table location and use it everywhere (especially for writes + locking; see §6.4).

---

### 6.2 Build a single “StorageProfile” that materializes both configs

You want **one config object** with two emitters:

* `to_datafusion_object_store()` → DataFusion `AmazonS3(...)` / `GoogleCloud(...)` / `MicrosoftAzure(...)`
* `to_deltalake_storage_options()` → `dict[str,str]` passed to `DeltaTable(..., storage_options=...)` and `write_deltalake(..., storage_options=...)`

#### What DataFusion Python exposes (S3)

The DataFusion Python object store constructor surface is small and explicit:

`AmazonS3(bucket_name, region=None, access_key_id=None, secret_access_key=None, endpoint=None, allow_http=False, imdsv1_fallback=False)` ([arrow.staged.apache.org][4])

That’s enough for: region, static keys, custom endpoints (MinIO/LocalStack), allow HTTP, IMDS fallback.

#### What delta-rs exposes (S3)

delta-rs expects `storage_options` (and/or env vars). The S3 integration docs show typical keys like:

* `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
* `AWS_ENDPOINT_URL` (for custom endpoints)
* `AWS_ALLOW_HTTP` (for MinIO HTTP)
* writer safety toggles (`AWS_S3_LOCKING_PROVIDER`, `aws_conditional_put`, etc.) ([delta-io.github.io][5])

It also has **advanced object store tuning** keys for concurrency + retries + backoff (critical on object stores): ([delta-io.github.io][6])

* `OBJECT_STORE_CONCURRENCY_LIMIT`
* `max_retries`
* `retry_timeout`
* `backoff_config.init_backoff`, `backoff_config.max_backoff`, `backoff_config.base`

**Rule:** your profile must produce *both* the DataFusion constructor args and delta-rs `storage_options` from the same source-of-truth fields.

---

### 6.3 A hardened “S3 profile” template (AWS + S3-compatible)

#### A) Credential strategy hierarchy (production-grade)

Use this order of preference:

1. **Workload identity / instance metadata** (no static secrets in env)
2. **Short-lived session creds** via env (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
3. **Static keys** only for local dev / controlled automation

The AWS CLI documents the standard env var names and override semantics (env overrides profile settings). ([AWS Documentation][7])

**Sanity check command (always do this in deploy/debug scripts):**

```bash
aws sts get-caller-identity
```

#### B) DataFusion: register store + read files

From the DataFusion user guide’s S3 example (explicit registration + then reading `s3://bucket/...`). ([Apache DataFusion][1])

```python
import os
from datafusion import SessionContext
from datafusion.object_store import AmazonS3

bucket = "my-bucket"
s3 = AmazonS3(
    bucket_name=bucket,
    region=os.getenv("AWS_REGION"),
    access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint=os.getenv("AWS_ENDPOINT_URL"),   # optional (MinIO/LocalStack)
    allow_http=os.getenv("AWS_ALLOW_HTTP") == "true",
)

ctx = SessionContext()
ctx.register_object_store("s3://", s3, None)
ctx.register_parquet("p", f"s3://{bucket}/path/to/*.parquet")
df = ctx.table("p")
```

**Hardening rules:**

* Don’t accept user-controlled `register_object_store` or `SET`-style mutation if this is a service.
* Register once per pooled context (your chapter 2 pool design).

#### C) delta-rs: load/write Delta tables with the same profile

delta-rs “Usage” shows `storage_options` as the canonical injection point. ([delta-io.github.io][3])

```python
from deltalake import DeltaTable, write_deltalake

storage_options = {
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "...",
    "AWS_SECRET_ACCESS_KEY": "...",
    "AWS_SESSION_TOKEN": "...",          # if using temp creds
    "AWS_ENDPOINT_URL": "http://localhost:9000",  # MinIO/LocalStack
    "AWS_ALLOW_HTTP": "true",            # required for HTTP MinIO
}

dt = DeltaTable("s3://my-bucket/delta/table", storage_options=storage_options)
write_deltalake("s3://my-bucket/delta/table", data, storage_options=storage_options, mode="append")
```

MinIO + HTTP + endpoint requirements are explicitly documented for delta-rs. ([delta-io.github.io][8])

---

### 6.4 Safe concurrent writes: DynamoDB lock vs conditional put (S3 vs S3-compatible)

#### A) AWS S3 (no mutual exclusion): use DynamoDB locking provider

delta-rs docs are explicit:

* you need a locking provider for safe concurrent writes on AWS S3
* delta-rs uses DynamoDB for this
* they provide the AWS CLI command to create the lock table ([delta-io.github.io][5])

`storage_options` keys for locking: ([delta-io.github.io][5])

* `AWS_S3_LOCKING_PROVIDER = dynamodb`
* `DELTA_DYNAMO_TABLE_NAME = delta_log` (or your name)

Create the DynamoDB table (from docs): ([delta-io.github.io][5])

```bash
aws dynamodb create-table \
  --table-name delta_log \
  --attribute-definitions AttributeName=tablePath,AttributeType=S AttributeName=fileName,AttributeType=S \
  --key-schema AttributeName=tablePath,KeyType=HASH AttributeName=fileName,KeyType=RANGE \
  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

**Critical scheme constraint:** the lock key includes `tablePath`, and Spark uses `s3a://`. If you mix `s3://` and `s3a://` between writers, you will silently get “two different lock namespaces.” delta-rs explicitly warns that writers must match the tablePath precisely and calls out the `s3a://` alignment. ([delta-io.github.io][9])

**Custom DynamoDB endpoint:** when using LocalStack, set `AWS_ENDPOINT_URL_DYNAMODB` in `storage_options`. ([delta-io.github.io][9])
And you can fully override the DynamoDB credential set separately (`AWS_ACCESS_KEY_ID_DYNAMODB`, etc.). ([delta-io.github.io][9])

#### B) MinIO / Cloudflare R2: use conditional puts (no DynamoDB)

delta-rs documents that R2/MinIO support conditional puts, so you can avoid DynamoDB by setting `aws_conditional_put` (example uses `'etag'`). ([delta-io.github.io][8])

---

### 6.5 Permissions: least privilege without breaking Delta semantics

delta-rs notes that even “append-only” workflows can require deletes because of temporary log files; it lists required S3 permissions and that DynamoDB permissions are needed when using locking. ([delta-io.github.io][5])

**Rule:** don’t design IAM policies that omit `s3:DeleteObject` just because you “only append” unless you’ve validated the exact write path you use.

---

### 6.6 Controlling retries/timeouts/concurrency coherently

#### A) delta-rs: you *can* tune these today (and should)

Use the advanced object storage configuration keys: ([delta-io.github.io][6])

* cap concurrency (`OBJECT_STORE_CONCURRENCY_LIMIT`)
* tune retries (`max_retries`, `retry_timeout`, `backoff_config.*`)

This is where you prevent “LIST storms” and stabilize tail latency on object stores.

#### B) DataFusion Python: object store tuning is limited

DataFusion’s Python `AmazonS3` exposes endpoint + allow_http + IMDS fallback, but **not** the full HTTP client knobs (retry policy, timeouts, backoff). ([arrow.staged.apache.org][4])

If you need “best in class” IO control for DataFusion scans, you typically cross the boundary into Rust and build the store using AWS SDK retry/timeout configuration, then register it.

AWS SDK for Rust documents:

* configurable connect/operation/attempt timeouts + stalled stream protection ([AWS Documentation][10])
* configurable retry strategies + what is considered retryable ([AWS Documentation][11])

And the DataFusion S3 object store ecosystem shows constructors that accept `RetryConfig` / `TimeoutConfig` (example `S3FileSystem::new(... RetryConfig, TimeoutConfig ...)`). ([Docs.rs][12])

**Practical guidance:**

* If you’re Python-first and only need retry/timeout control for Delta IO, rely on delta-rs advanced storage_options (§6.6A).
* If you need retry/timeout control for *DataFusion file scans*, plan for a small Rust “engine shim” (build object store with your policy → expose to Python via your established extension pipeline).

---

### 6.7 Custom endpoints: LocalStack/MinIO, one policy for all clients

For AWS tooling (CLI/SDKs), AWS documents global + service-specific endpoint env vars (e.g., `AWS_ENDPOINT_URL` and `AWS_ENDPOINT_URL_DYNAMODB`). ([AWS Documentation][13])
This maps cleanly to delta-rs’s documented use of `AWS_ENDPOINT_URL_DYNAMODB` overrides for locking provider. ([delta-io.github.io][9])

For DataFusion Python MinIO, use:

* `AmazonS3(... endpoint="http://localhost:9000", allow_http=True)` and register `"s3://"` ([arrow.staged.apache.org][4])

For delta-rs MinIO, use:

* `AWS_ENDPOINT_URL=http://localhost:9000` and `AWS_ALLOW_HTTP=true` ([delta-io.github.io][8])

---

## What to standardize in your codebase (the “contract”)

1. **One canonical table URL scheme per dataset** (`s3a://` vs `s3://`) and enforce it (especially if DynamoDB locking is involved). ([delta-io.github.io][9])
2. **One `StorageProfile` schema** that outputs:

   * DataFusion object store constructor args + `register_object_store("s3://", …)` usage ([Apache DataFusion][1])
   * delta-rs `storage_options` dict including advanced tuning keys ([delta-io.github.io][3])
3. **One “connectivity+identity probe”** in every environment:

   * `aws sts get-caller-identity`
   * and at least one “read a tiny object” operation through both stacks (DataFusion read and DeltaTable open)

If you want to continue in-order, the next deep dive is usually:

**7) Operational correctness policies above the libraries** (snapshot pinning policy, retry classification, commit conflict handling, deterministic plan+config artifacts), because object-store hardening sets the stage for deterministic, debuggable behavior under concurrency.

[1]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[2]: https://github.com/nautechsystems/nautilus_trader/issues/3120?utm_source=chatgpt.com "Support custom S3 endpoints for DataFusion object store"
[3]: https://delta-io.github.io/delta-rs/python/usage.html "Usage — delta-rs  documentation"
[4]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.object_store.object_store.html "datafusion.object_store.object_store — Apache Arrow DataFusion  documentation"
[5]: https://delta-io.github.io/delta-rs/integrations/object-storage/s3/ "AWS S3 Storage Backend - Delta Lake Documentation"
[6]: https://delta-io.github.io/delta-rs/integrations/object-storage/special_configuration/ "Advanced object storage configuration - Delta Lake Documentation"
[7]: https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-envvars.html?utm_source=chatgpt.com "Configuring environment variables for the AWS CLI"
[8]: https://delta-io.github.io/delta-rs/integrations/object-storage/s3-like/ "CloudFlare R2 & Minio - Delta Lake Documentation"
[9]: https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/ "Writing to S3 with a locking provider - Delta Lake Documentation"
[10]: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/timeouts.html?utm_source=chatgpt.com "Configuring timeouts in the AWS SDK for Rust"
[11]: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/retries.html?utm_source=chatgpt.com "Configuring retries in the AWS SDK for Rust"
[12]: https://docs.rs/datafusion-objectstore-s3 "datafusion_objectstore_s3 - Rust"
[13]: https://docs.aws.amazon.com/sdkref/latest/guide/feature-ss-endpoints.html?utm_source=chatgpt.com "Service-specific endpoints - AWS SDKs and Tools"

## 7) Operational correctness policies above the libraries

This chapter is about turning Apache DataFusion + Delta Lake primitives into **repeatable, debuggable, race-resistant** production behavior.

---

# 7.1 Deterministic plan artifacts (regression-proofing)

### 7.1.1 Why plans are *not* self-contained

A query “plan” is not just “the SQL”:

* DataFusion produces **logical** and **physical** plans; the physical plan depends on both **hardware/config** and **data layout/files**, so the *same query* can yield different physical plans across environments. ([Apache DataFusion][1])

**Policy:** capture a *Plan Bundle* per query that includes:

1. **Optimized logical plan** (post optimizer)
2. **Physical plan** (execution plan)
3. **Key config slice** (anything that can change planning/execution)
4. **Engine build identity** (DataFusion + delta-rs versions)
5. **Snapshot identity** for every table read/written (see §7.2)

---

## 7.1.2 Plan capture surfaces (Python-first, deterministic outputs)

### A) Capture from DataFusion Python APIs (preferred: no SQL parsing edge cases)

The Python `DataFrame` exposes:

* `logical_plan()`, `optimized_logical_plan()`, `execution_plan()` ([arrow.staged.apache.org][2])
* streaming export that never materializes results (for execution paths, not for artifacts) ([Apache DataFusion][3])

The plan objects support:

* `display()`, `display_indent()`, `display_indent_schema()`, `display_graphviz()` for stable text rendering
* `to_proto()` for binary serialization (with the caveat that “tables created in memory from record batches” aren’t supported for proto serialization) ([Apache DataFusion][4])

**Artifact-grade capture function**

```python
import base64
import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

import pyarrow as pa

@dataclass(frozen=True)
class DFPlanBundle:
    query_id: str
    logical_plan_indent_schema: str
    optimized_logical_plan_indent_schema: str
    physical_plan_proto_b64: Optional[str]
    logical_plan_proto_b64: Optional[str]
    df_settings: Dict[str, str]  # name -> value

def _df_settings(ctx) -> Dict[str, str]:
    # deterministic: pull session config via information_schema view
    # (works like SHOW ALL)
    df = ctx.sql("select name, value from information_schema.df_settings")
    rows = df.to_pylist()  # small, safe to materialize
    return {r["name"]: r["value"] for r in rows}

def capture_plan_bundle(ctx, df, query_id: str) -> DFPlanBundle:
    lp = df.logical_plan()                # LogicalPlan
    olp = df.optimized_logical_plan()     # LogicalPlan
    ep = df.execution_plan()              # ExecutionPlan

    lp_txt = lp.display_indent_schema()
    olp_txt = olp.display_indent_schema()

    # proto serialization (best for exact diffs); may fail for in-memory tables
    lp_pb = lp.to_proto()
    ep_pb = ep.to_proto()

    return DFPlanBundle(
        query_id=query_id,
        logical_plan_indent_schema=lp_txt,
        optimized_logical_plan_indent_schema=olp_txt,
        logical_plan_proto_b64=base64.b64encode(lp_pb).decode("ascii") if lp_pb else None,
        physical_plan_proto_b64=base64.b64encode(ep_pb).decode("ascii") if ep_pb else None,
        df_settings=_df_settings(ctx),
    )
```

* `optimized_logical_plan()` / `execution_plan()` are explicit API surfaces. ([arrow.staged.apache.org][2])
* `information_schema.df_settings` is the canonical “SHOW ALL as rows” view. ([Apache DataFusion][5])

### B) Capture via SQL `EXPLAIN` (human-readable regression diffs)

DataFusion SQL supports:

```sql
EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] statement
```

([Apache DataFusion][6])

**Recommended default formats**

* **`FORMAT tree`** for high-level structure diffs
* **`EXPLAIN VERBOSE`** when you need optimizer-rule-by-rule visibility (great for “why did this rewrite disappear?”). ([Apache DataFusion][7])

**Hardening knobs for consistent output**

* `datafusion.explain.format` controls default format (indent vs tree) ([Apache DataFusion][8])
* enable plan stats in explain: `datafusion.explain.show_statistics=true` (so you can spot pruning/stat drift) ([Apache DataFusion][8])
* choose `EXPLAIN ANALYZE` verbosity: `datafusion.explain.analyze_level=summary|dev` ([Apache DataFusion][8])

**One-time “profile config” to pin**

```sql
SET datafusion.explain.format = 'tree';
SET datafusion.explain.show_statistics = 'true';
SET datafusion.explain.analyze_level = 'summary';
```

(These config keys are documented, including the analyze_level semantics.) ([Apache DataFusion][8])

---

## 7.1.3 Config slice: what must be captured, not inferred

**Policy:** store *all* `information_schema.df_settings` for each query, but also compute a “key slice” for fast diffs:

* `datafusion.execution.*` (partitioning, batch size, pruning, stats collection) ([Apache DataFusion][9])
* `datafusion.runtime.*` (memory limit, temp dir, metadata/listing caches) ([Apache DataFusion][9])
* `datafusion.optimizer.*` (behavioral rewrites) ([Apache DataFusion][10])
* `datafusion.explain.*` (artifact verbosity changes diff output!) ([Apache DataFusion][8])
* `datafusion.sql_parser.*` (dialect normalization and span collection can change plans) ([Apache DataFusion][8])

---

# 7.2 Deterministic snapshot identity (time travel + caching + safety)

### 7.2.1 The SnapshotKey contract

Define a stable snapshot identity for every read/write:

```text
SnapshotKey = (canonical_table_uri, resolved_version)
```

Where **resolved_version is always an integer** you can log/cache against (even if the caller time-traveled by timestamp).

delta-rs models a `DeltaTable` as the state of a table at a particular version and exposes `version()`. ([delta-io.github.io][11])

---

## 7.2.2 Resolve version deterministically (Python / delta-rs)

### A) Load latest, record resolved version

```python
from deltalake import DeltaTable

dt = DeltaTable(table_uri, storage_options=storage_options)
resolved = dt.version()
```

The constructor + `version()` pattern is documented. ([delta-io.github.io][11])

### B) Time travel (version OR timestamp), then “collapse” to resolved version

delta-rs supports `load_as_version(version: int | str | datetime)` where strings are RFC3339 / ISO 8601. ([delta-io.github.io][12])

```python
from datetime import datetime, timezone
from deltalake import DeltaTable

dt = DeltaTable(table_uri, storage_options=storage_options)

# time travel by timestamp
dt.load_as_version("2018-01-26T18:30:09Z")     # RFC3339
resolved = dt.version()                        # now an integer
```

([delta-io.github.io][12])

### C) Constructor time travel (when you already know the version)

`DeltaTable(table_uri, version=...)` is explicitly supported. ([delta-io.github.io][12])

```python
dt = DeltaTable(table_uri, version=42, storage_options=storage_options)
```

---

## 7.2.3 Canonicalize the table URI (or your locks/caches won’t line up)

If you write to S3 with DynamoDB locking, delta-rs uses a `tablePath` key as part of the lock primary key; **all writers must match it exactly**. The docs explicitly note Spark uses `s3a://`, and delta-rs must be configured accordingly to interoperate. ([delta-io.github.io][13])

**Policy:** implement `canonical_table_uri()` that normalizes:

* scheme (`s3a://` vs `s3://`), trailing slashes, and “equivalent” hostnames
* path normalization rules you enforce across all writers (DataFusion readers + delta-rs writers)

This is *operational correctness*, not “style”.

---

## 7.2.4 VACUUM + log cleanup: what breaks snapshot identity

Time travel depends on *both*:

* transaction log history, and
* data files referenced by that history.

**VACUUM permanently limits time travel.**

* Delta Lake utility docs: after running vacuum, time travel to versions older than the retention window is lost; vacuum deletes data files (not log files). ([Delta Lake][14])
* delta-rs docs: vacuum can make some past versions invalid; it retains files within a window (default ~1 week), so shorter-range time travel still works. ([delta-io.github.io][15])
* Databricks docs: vacuum retention threshold defaults to 7 days (implementation detail, but matches common defaults). ([Databricks Documentation][16])

**Log cleanup also limits reconstruction.**
delta-rs exposes `cleanup_metadata()` to delete expired log files based on `delta.logRetentionDuration` (30 days by default). ([delta-io.github.io][12])
Delta utility docs describe `delta.logRetentionDuration` as the knob controlling log file retention. ([Delta Lake][14])

### Practical policy

* For *reproducible analytics / caching*: always pin reads to `(uri, resolved_version)` and store that key in your manifests.
* For *“latest view”* queries: resolve latest to a version at query start and log it (you can still present “latest” externally while keeping internal determinism).

---

## 7.2.5 Retention policy must be explicit (and consistent with runtime)

Delta Lake exposes table properties for:

* `delta.logRetentionDuration`
* `delta.deletedFileRetentionDuration`
  and also `delta.appendOnly` (see §7.3). ([Delta Lake][17])

The table properties reference notes `delta.deletedFileRetentionDuration` should be large enough to avoid failures with concurrent readers/writers when vacuuming. ([Delta Lake][18])

**Policy:** version your retention policy alongside code:

* `retention_profile = {logRetention, deletedFileRetention, vacuum_schedule}`
* enforce “no vacuum” for tables that require long time travel windows
* enforce “vacuum allowed” only when your job SLA window and concurrency constraints are satisfied

---

# 7.3 Concurrency policies for write paths (retry rules + conflict classification)

### 7.3.1 Start from Delta’s concurrency model

Delta uses optimistic concurrency control:

1. read snapshot
2. write new files
3. validate-and-commit (fail with concurrent modification exception if conflicts) ([Delta Lake][19])

Delta’s official conflict matrix:

* INSERT vs INSERT: **cannot conflict**
* UPDATE/DELETE/MERGE can conflict with INSERT and each other
* compaction can conflict with UPDATE/DELETE/MERGE and with compaction ([Delta Lake][19])

**Meaning:** you only get “easy concurrency” if you are truly append-only.

---

## 7.3.2 Decide your writer class: append-only vs mixed writers

### A) Append-only mode (best default for a Python-first platform)

Enforce:

* Only `write_deltalake(..., mode="append")` style writes (no rewrites)
* Table property `delta.appendOnly=true` to block updates/deletes at the table level ([Delta Lake][18])

delta-rs exposes table metadata configuration and explicitly mentions `delta.appendOnly` as meaning “not meant to have data deleted”. ([delta-io.github.io][15])

### B) Mixed-writer mode (MERGE/UPDATE/DELETE/OPTIMIZE allowed)

You must operationalize:

* partitioning and disjoint conditions so concurrent writers touch disjoint file sets (Delta docs call this out explicitly) ([Delta Lake][19])
* “metadata change windows” (schema/properties/protocol upgrades) with no concurrent writes (see conflict types below) ([Delta Lake][19])

---

## 7.3.3 Storage-level safety: S3 requires a locking story

For AWS S3, delta-rs requires a locking provider for safe concurrent writes because S3 does not guarantee mutual exclusion; delta-rs uses DynamoDB for locking. ([delta-io.github.io][20])

Key operational constraints:

* You enable locking via `AWS_S3_LOCKING_PROVIDER='dynamodb'` and optionally `DELTA_DYNAMO_TABLE_NAME`. ([delta-io.github.io][13])
* **All writers must match `tablePath` precisely** (including scheme like `s3a://`) or they won’t coordinate. ([delta-io.github.io][13])
* delta-rs notes it does *not* read credentials from local `.aws/config` / `.aws/creds`; plan your credential injection accordingly (env/web identity/storage_options). ([delta-io.github.io][13])

**Policy:** “write concurrency enabled” is a table capability that you declare, test, and enforce (not a best-effort toggle).

---

## 7.3.4 Conflict exception classification (what to retry vs fail fast)

Delta’s documented conflict exceptions include explicit causes and recommended fixes: ([Delta Lake][19])

### Class 1 — Retryable *only if you can re-run the entire transaction*

These imply your read set is invalid; you must re-read and rebuild outputs:

* `ConcurrentAppendException`: someone added files in partitions you read. ([Delta Lake][19])
* `ConcurrentDeleteReadException`: someone deleted a file you read. ([Delta Lake][19])
* `ConcurrentDeleteDeleteException`: two ops delete/rewrite the same files (often compaction). ([Delta Lake][19])

**Policy:** auto-retry only when:

* the job is deterministic and idempotent given a new snapshot, and
* the cost of reprocessing is acceptable.

(Delta’s own issue tracker discussion on “automatically retry failed transactions” highlights that retries may require reprocessing and can be expensive.) ([GitHub][21])

### Class 2 — Do not retry; fix the deployment / sequencing

* `MetadataChangedException`: schema/properties changed concurrently (ALTER TABLE or schema evolution). ([Delta Lake][19])
* `ProtocolChangedException`: table upgraded/replaced or multiple writers created/replaced simultaneously; may require upgrading the reader/writer. ([Delta Lake][19])
* `ConcurrentTransactionException`: streaming checkpoint reuse; indicates orchestration bug (don’t retry; fix checkpoint uniqueness). ([Delta Lake][19])

**Policy:** these are “stop-the-world” events:

* gate schema/protocol/property changes behind a maintenance window
* embed protocol/version checks in your deployment boot

---

## 7.3.5 delta-rs specific write correctness: schema evolution must be explicit

delta-rs documents that `write_deltalake` raises `ValueError` if the incoming schema differs from the table’s schema, and you must opt into schema evolution via `schema_mode="overwrite"|"merge"`. ([delta-io.github.io][22])

**Policy:** default `schema_mode=None` (strict), and require an explicit rollout plan for schema evolution:

* schema change PR → compatibility check → maintenance window → enable `schema_mode="merge"` for one deployment → revert to strict

This keeps “MetadataChangedException”-style failures from being random. ([Delta Lake][19])

---

# 7.4 Minimum “correctness contract” manifest (what you always persist)

For every query/run (even internal pipelines), persist a JSON manifest that includes:

### A) Planning / execution reproducibility

* `query_text` or “logical spec id”
* DataFusion `optimized_logical_plan` (text) + `logical_plan_proto` (bytes)
* DataFusion `physical_plan_proto` (bytes) + partition count
* `df_settings` snapshot (`information_schema.df_settings`) ([Apache DataFusion][5])

### B) Snapshot identity (reads + writes)

For each Delta table touched:

* `canonical_table_uri`
* `resolved_version` (integer)
* retention knobs (`delta.logRetentionDuration`, `delta.deletedFileRetentionDuration` if known/queried)
* `write_mode`, `schema_mode`, `partition_by`
* storage safety mode (locking provider / conditional put / unsafe rename toggle) ([delta-io.github.io][12])

### C) Concurrency outcomes

* commit result: `new_version` (post-write reload + `dt.version()`)
* exception classification if failed (mapped to “retryable vs fatal” buckets above)
* vacuum policy tags (whether your pipeline expects vacuum cleanup for zombie files)

delta-rs transaction docs explicitly describe how failed conflicting transactions can stage Parquet files that aren’t committed (“zombie” files), which later vacuum can remove. ([delta-io.github.io][23])

---

If you want to continue the sequence, the next deep dive is typically **8) Integration testing & conformance harness**, because these policies only stay real if you have golden fixtures that assert: (a) plans/config snapshots match expectations, (b) snapshot identity survives vacuum/log cleanup scenarios, and (c) concurrency retries behave exactly as specified.

[1]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[2]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.DataFrame.html "datafusion.DataFrame — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/sql/information_schema.html?utm_source=chatgpt.com "Information Schema — Apache DataFusion documentation"
[6]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/library-user-guide/query-optimizer.html?utm_source=chatgpt.com "Query Optimizer — Apache DataFusion documentation"
[8]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[9]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[10]: https://datafusion.apache.org/user-guide/cli/usage.html "Usage — Apache DataFusion  documentation"
[11]: https://delta-io.github.io/delta-rs/usage/loading-table/ "Loading a table - Delta Lake Documentation"
[12]: https://delta-io.github.io/delta-rs/api/delta_table/ "DeltaTable - Delta Lake Documentation"
[13]: https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/ "Writing to S3 with a locking provider - Delta Lake Documentation"
[14]: https://docs.delta.io/delta-utility/ "Table utility commands | Delta Lake"
[15]: https://delta-io.github.io/delta-rs/python/usage.html?utm_source=chatgpt.com "Usage — delta-rs documentation"
[16]: https://docs.databricks.com/aws/en/delta/vacuum?utm_source=chatgpt.com "Remove unused data files with vacuum | Databricks on AWS"
[17]: https://docs.delta.io/delta-batch/ "Table batch reads and writes | Delta Lake"
[18]: https://docs.delta.io/table-properties/?utm_source=chatgpt.com "Delta Table Properties Reference"
[19]: https://docs.delta.io/concurrency-control/ "Concurrency control | Delta Lake"
[20]: https://delta-io.github.io/delta-rs/integrations/object-storage/s3/ "AWS S3 Storage Backend - Delta Lake Documentation"
[21]: https://github.com/delta-io/delta/issues/445?utm_source=chatgpt.com "Automatically retry failed transactions due to write conflicts"
[22]: https://delta-io.github.io/delta-rs/usage/writing/?utm_source=chatgpt.com "Writing Delta Tables - Delta Lake Documentation"
[23]: https://delta-io.github.io/delta-rs/how-delta-lake-works/delta-lake-acid-transactions/ "Transactions - Delta Lake Documentation"

## 8) Integration testing & conformance harness

You want a harness that proves (and keeps proving) the three “above-library” policies from chapter 7:

* **(A) Plan/config determinism**: the *optimized logical plan* + *physical plan* + *key settings* are captured and diffable
* **(B) Snapshot identity determinism**: every read/write maps to a stable `(table_uri, resolved_version)` key; behavior under VACUUM / log cleanup is explicit
* **(C) Concurrency correctness**: safe write coordination (locking / conditional puts), plus retry classification that matches Delta’s conflict model

---

# 8.1 Test taxonomy: split fast correctness from slow conformance

### Layer 0 — Unit tests (no containers, no object store)

* Pure Python: plan bundle serialization, config-slice selection, conflict classifier mapping, “golden string canonicalization”.
* Goal: run on every commit in <5s.

### Layer 1 — Local FS integration (no containers)

* Use `write_deltalake()` to create a local delta table; load with `DeltaTable(version=...)`; register into DataFusion; run queries; capture plan bundles.
* Goal: validate your *core integration wiring* without S3 complexity.
  `write_deltalake` is the canonical “create if missing” entry point and accepts Arrow streaming inputs. ([delta-io.github.io][1])

### Layer 2 — S3-like semantics (MinIO) integration

* Exercises: endpoint config, HTTP allowance, conditional puts, listing behavior, and IO failure modes.
* Goal: you catch “works on local FS but dies on S3” issues early.

### Layer 3 — AWS-style semantics (LocalStack S3 + DynamoDB) conformance

* This is where you validate **safe concurrent writes** (locking provider, tablePath scheme matching, and retry policy behavior).
* Delta-rs’ S3 locking doc provides the authoritative knobs + required DynamoDB schema + `s3a://` scheme constraint. ([delta-io.github.io][2])

---

# 8.2 Harness layout (repo structure that scales)

A workable layout that keeps artifacts versioned and reviewable:

```
tests/
  conftest.py
  harness/
    profiles.py            # StorageProfile + DF config profiles
    plan_bundle.py         # capture + canonicalize + persist
    snapshot_key.py        # (uri, resolved_version) utilities
    conflicts.py           # classify exceptions -> retry/fail
  integration/
    test_df_delta_smoke.py
    test_plan_golden.py
    test_snapshot_identity.py
    test_vacuum_retention.py
    test_cleanup_metadata.py
    test_concurrent_writes_localstack.py
golden/
  plans/
    <testcase>.approved.txt   # or YAML/JSON golden
scripts/
  it_up_localstack.sh
  it_down_localstack.sh
  it_up_minio.sh
  it_seed_storage.sh
docker/
  compose.localstack.yml
  compose.minio.yml
```

Golden storage mechanism options:

* **Approval/golden-master** style: `ApprovalTests.Python` compares “received” vs “approved” artifacts. ([GitHub][3])
* **File-driven golden parametrization**: `pytest-golden` lets you glob YAML “golden” files as both inputs and expected outputs. ([PyPI][4])

Pick one and standardize (I usually prefer approval tests for large plan text blobs).

---

# 8.3 Local integration primitives (what every test uses)

## 8.3.1 Create deterministic delta tables

Use `write_deltalake(table_or_uri, data, ...)` as your single writer surface. Its signature explicitly supports Arrow stream exportables, `partition_by`, `mode`, `configuration`, `schema_mode`, and `storage_options`. ([delta-io.github.io][1])

Key rule: **do not “handcraft” `_delta_log`**—always create via the writer.

### Minimal deterministic writer

```python
import pyarrow as pa
from deltalake import write_deltalake

def seed_table(uri: str, *, mode: str, storage_options: dict[str, str] | None = None) -> None:
    tbl = pa.table(
        {
            "k": pa.array([1, 2, 3], pa.int64()),
            "v": pa.array([10, 20, 30], pa.int64()),
            "p": pa.array(["a", "a", "b"], pa.string()),  # partition column
        }
    )
    write_deltalake(
        uri,
        tbl,
        mode=mode,
        partition_by=["p"],
        storage_options=storage_options,
        # optional: table properties / metadata config at creation time
        configuration={
            # Example: append-only policy (also test in harness)
            "delta.appendOnly": "true",
        },
    )
```

Notes:

* `write_deltalake` raises `ValueError` on schema mismatch unless you opt in via `schema_mode="merge"|"overwrite"`, which is itself a conformance surface you should test. ([delta-io.github.io][5])
* Table property `delta.appendOnly` is an official Delta property that enforces append-only semantics. ([Delta Lake][6])

## 8.3.2 Load snapshots deterministically

Your SnapshotKey is always `(canonical_uri, resolved_version)`; `DeltaTable(version=...)` is the actual enforcement mechanism (not “best effort”). `DeltaTable` is explicitly versioned and supports multiple backends. ([delta-io.github.io][7])

```python
from deltalake import DeltaTable

dt_latest = DeltaTable(uri, storage_options=storage_options)
v_latest = dt_latest.version()          # resolved_version
dt_v0 = DeltaTable(uri, version=0, storage_options=storage_options)
```

(Also note useful tuning knobs for tests with huge logs: `log_buffer_size` and `without_files` exist as DeltaTable constructor parameters.) ([delta-io.github.io][7])

## 8.3.3 Register Delta into DataFusion (smoke test)

DataFusion Python supports registering a `deltalake.DeltaTable` as a table provider (DataFusion ≥ 43). ([datafusion.apache.org][8])

```python
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
dt = DeltaTable(uri, storage_options=storage_options)
ctx.register_table("t", dt)

df = ctx.sql("select p, sum(v) as s from t group by p order by p")
rows = df.to_pylist()     # keep results tiny in tests
assert rows == [{"p": "a", "s": 30}, {"p": "b", "s": 30}]
```

---

# 8.4 Golden plan bundles (A): exact artifacts + stable diffs

## 8.4.1 Use plan protobuf when possible; fall back to text

DataFusion Python exposes:

* plan objects (`LogicalPlan`, `ExecutionPlan`) with `display*()` for stable text
* `to_proto()` / `from_proto()` for exact binary snapshots
  …but **proto serialization does not support tables created in-memory from record batches**. ([datafusion.apache.org][9])

**Policy:** golden plan tests must use *externalized* tables (Delta or file-backed), not `register_record_batches()`.

### Capture

```python
import base64
import json
from datafusion.plan import LogicalPlan, ExecutionPlan

def plan_bundle(df, ctx) -> dict:
    lp = df.logical_plan()
    olp = df.optimized_logical_plan()
    ep = df.execution_plan()

    # text (review-friendly)
    out = {
        "optimized_logical_plan": olp.display_indent_schema(),
        "physical_plan": ep.display_indent(),
    }

    # proto (exact)
    try:
        out["optimized_logical_plan_proto_b64"] = base64.b64encode(olp.to_proto()).decode("ascii")
        out["physical_plan_proto_b64"] = base64.b64encode(ep.to_proto()).decode("ascii")
    except Exception:
        out["optimized_logical_plan_proto_b64"] = None
        out["physical_plan_proto_b64"] = None

    # full config capture via information_schema.df_settings (SHOW ALL)
    settings_df = ctx.sql("select name, setting from information_schema.df_settings")
    items = sorted(settings_df.to_pylist(), key=lambda r: r["name"])
    out["df_settings"] = {r["name"]: r["setting"] for r in items}

    return out
```

* `display_indent_schema()` and `to_proto()` are the documented plan surfaces. ([datafusion.apache.org][9])
* `information_schema.df_settings` is the documented “SHOW ALL as rows” surface. ([datafusion.apache.org][10])

### Golden compare (ApprovalTests pattern)

* Serialize the bundle to a canonical string (stable key ordering, no timestamps).
* Approve it once; thereafter diffs are PR-reviewable. ([GitHub][3])

---

# 8.5 Snapshot identity tests (B): time travel + cache keys

## 8.5.1 SnapshotKey assertions

For each test table:

1. Load latest, record `resolved_version = dt.version()`
2. Create `DeltaTable(uri, version=resolved_version)` and re-load
3. Assert: same schema, same `get_add_actions()` (file set), same query results

`get_add_actions()` returns the current “add actions” parsed from the transaction log (low-level but deterministic). ([delta-io.github.io][11])

```python
dt = DeltaTable(uri, storage_options=storage_options)
v = dt.version()

adds_latest = dt.get_add_actions(flatten=True).to_pylist()

dt2 = DeltaTable(uri, version=v, storage_options=storage_options)
adds_v = dt2.get_add_actions(flatten=True).to_pylist()

assert adds_v == adds_latest
```

## 8.5.2 DataFusion snapshot pinning = register a versioned DeltaTable

Pinning is as simple as:

```python
ctx.register_table("t", DeltaTable(uri, version=v, storage_options=storage_options))
```

…and then plan + results must be stable across runs.

---

# 8.6 VACUUM / log cleanup conformance (B): “expected breakage” is part of correctness

## 8.6.1 VACUUM tests: file deletion + time travel loss

Delta “VACUUM” deletes **data files** older than the retention threshold that are no longer referenced; after vacuum, time travel to versions older than retention is lost. ([Delta Lake][12])
In delta-rs Python, `vacuum(retention_hours=..., dry_run=...)` uses `delta.deletedFileRetentionDuration` if unset, else defaults to ~1 week. ([delta-io.github.io][11])

**Test pattern (local FS or S3):**

1. Create table v0
2. Overwrite or delete to produce unreferenced files (v1)
3. `vacuum(dry_run=True, retention_hours=0)` to list candidates
4. `vacuum(dry_run=False, retention_hours=0)` to delete
5. Assert:

   * latest version still reads
   * reading **older version’s data** fails (or yields missing-file error you explicitly classify as “expected after vacuum”)

You don’t need brittle “exact file list” assertions; you need *behavioral guarantees*.

## 8.6.2 cleanup_metadata tests: log retention correctness

delta-rs exposes `cleanup_metadata()` and documents it deletes expired log files based on `delta.logRetentionDuration` (30 days default). ([delta-io.github.io][7])

Delta’s utility docs clarify:

* vacuum deletes data files, not log files
* log files are deleted after checkpointing and are controlled by `delta.logRetentionDuration` ([Delta Lake][12])

**Test pattern:**

* Generate multiple versions
* Force a checkpoint (`create_checkpoint()`) and run `cleanup_metadata()`
* Assert:

  * latest snapshot still loads and `dt.version()` unchanged
  * (optional on local FS) `_delta_log` count decreases when you configure a short log retention for the test table

`create_checkpoint()` is a first-class delta-rs API. ([delta-io.github.io][7])

---

# 8.7 Concurrency & retry conformance (C): lock correctness + conflict classifier

This is **two separate assertions**:

1. **Safe concurrent writes are enabled and correctly configured**
2. **Your retry policy reacts correctly to conflicts**

## 8.7.1 LocalStack setup (S3 + DynamoDB) for “AWS-style” locking tests

LocalStack runs AWS APIs locally on an “edge port” (commonly `localhost:4566`); AWS CLI calls can be routed via `--endpoint-url`. ([Amazon Web Services, Inc.][13])
Docker’s LocalStack guide shows the recommended `awscli-local` (`awslocal`) workflow and bucket creation. ([Docker Documentation][14])

**Bring up + init (example commands)**

```bash
docker compose -f docker/compose.localstack.yml up -d

pip install awscli-local
awslocal s3 mb s3://test-bucket

# Create DynamoDB lock table required by delta-rs
awslocal dynamodb create-table \
  --table-name delta_log \
  --attribute-definitions AttributeName=tablePath,AttributeType=S AttributeName=fileName,AttributeType=S \
  --key-schema AttributeName=tablePath,KeyType=HASH AttributeName=fileName,KeyType=RANGE \
  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

The delta-rs “Writing to S3 with a locking provider” doc provides:

* required `AWS_S3_LOCKING_PROVIDER='dynamodb'`
* optional `DELTA_DYNAMO_TABLE_NAME`
* the DynamoDB schema and the create-table command
* the critical requirement that all writers match `tablePath` precisely (notably `s3a://` for Spark interop)
* LocalStack endpoint override via `AWS_ENDPOINT_URL_DYNAMODB` ([delta-io.github.io][2])

## 8.7.2 Storage options you enforce in tests

```python
storage_options = {
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_ENDPOINT_URL": "http://localhost:4566",
    "AWS_S3_LOCKING_PROVIDER": "dynamodb",
    "DELTA_DYNAMO_TABLE_NAME": "delta_log",
    "AWS_ENDPOINT_URL_DYNAMODB": "http://localhost:4566",
}
uri = "s3a://test-bucket/it/table"   # enforce s3a:// tablePath consistency
```

All of those knobs are explicitly described in the locking-provider documentation. ([delta-io.github.io][2])

## 8.7.3 “Locking is actually on” conformance test

Run two concurrent writers (processes) doing `write_deltalake(..., mode="append")` to the *same* table and assert:

* both succeed
* final version increments by 2
* data contains both writers’ rows

This validates: credential wiring + endpoint wiring + lock table wiring + scheme consistency.

## 8.7.4 Conflict classifier + retry conformance

Delta Lake’s official concurrency control doc defines:

* the OCC stages (read → write → validate/commit)
* which operations can conflict (INSERT vs INSERT can’t; UPDATE/DELETE/MERGE can conflict)
* the canonical conflict exception families (`ConcurrentAppendException`, `ConcurrentDeleteReadException`, `MetadataChangedException`, `ProtocolChangedException`, …) ([Delta Lake][15])

**Harness approach:**

* Unit-test the classifier mapping by feeding it exception messages containing these canonical names.
* Integration-test *at least one real conflict* by running a rewrite operation concurrently with an append:

  * Process A: `DeltaTable.delete(predicate=...)` (rewrites files)
  * Process B: `write_deltalake(..., mode="append")`
  * Assert your wrapper:

    * retries on retryable conflict families
    * fails-fast on metadata/protocol conflicts

(Your “conflict family → retry/fail” policy is exactly what you lock down in unit tests, and then validate with one or two integration reproductions.)

---

# 8.8 CI execution contracts (what you run where)

### Fast (default CI job)

```bash
pytest -q -m "not (minio or localstack)"   # local FS integration only
```

### Nightly / scheduled (full conformance)

```bash
pytest -q -m "localstack"
pytest -q -m "minio"
```

Artifacts you upload from CI for every failing integration test:

* plan bundle (text + proto b64)
* df_settings snapshot
* snapshot keys for each table touched
* LocalStack/MinIO logs (when backend tests fail)

That gives you “why was this slow/wrong?” *even in CI*, without rerunning.

---

If you want to proceed to chapter 9 next, the natural continuation is **“Public Python API design for best-in-class integration”** (the façade that returns streaming results by default, enforces SnapshotKeys, and centralizes storage profiles + retry policy).

[1]: https://delta-io.github.io/delta-rs/api/delta_writer/ "Writer - Delta Lake Documentation"
[2]: https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/ "Writing to S3 with a locking provider - Delta Lake Documentation"
[3]: https://github.com/approvals/ApprovalTests.Python?utm_source=chatgpt.com "approvals/ApprovalTests.Python"
[4]: https://pypi.org/project/pytest-golden/?utm_source=chatgpt.com "pytest-golden"
[5]: https://delta-io.github.io/delta-rs/usage/writing/?utm_source=chatgpt.com "Writing Delta Tables - Delta Lake Documentation"
[6]: https://docs.delta.io/table-properties/?utm_source=chatgpt.com "Delta Table Properties Reference"
[7]: https://delta-io.github.io/delta-rs/api/delta_table/ "DeltaTable - Delta Lake Documentation"
[8]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[9]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[10]: https://datafusion.apache.org/_sources/user-guide/sql/information_schema.md.txt "datafusion.apache.org"
[11]: https://delta-io.github.io/delta-rs/python/api_reference.html?utm_source=chatgpt.com "API Reference — delta-rs documentation"
[12]: https://docs.delta.io/delta-utility/?utm_source=chatgpt.com "Table utility commands"
[13]: https://aws.amazon.com/blogs/awsmarketplace/accelerating-software-delivery-localstack-cloud-emulator-aws-marketplace/ "Accelerating software delivery using LocalStack Cloud Emulator from AWS Marketplace | AWS Marketplace"
[14]: https://docs.docker.com/guides/localstack/ "AWS development with LocalStack | Docker Docs"
[15]: https://docs.delta.io/concurrency-control/ "Concurrency control | Delta Lake"

## 9) Public Python API design for best-in-class DataFusion + delta-rs integration

The façade’s job is to make the “right thing” *the easiest thing*:

* **Streaming-first results** (Arrow C Stream everywhere; no accidental `to_arrow_table()` / `collect()` explosions)
* **SnapshotKey enforcement** (`(table_uri, resolved_version)` is recorded + returned for every read/write)
* **Centralized policy** (storage profile + SQL allowances + retry rules + plan/config artifacts)

Below is a concrete, agent-friendly API surface that composes directly with DataFusion Python + delta-rs Python.

---

# 9.1 Public contract: the 6 invariants you enforce

### I1) Queries return a stream by default

DataFusion DataFrames are lazy and only execute on terminal ops, and they implement `__arrow_c_stream__` for zero-copy, on-demand batch streaming. ([datafusion.apache.org][1])

**API rule:** `engine.query(...).stream` returns an Arrow stream exportable; materialization is explicit (`.to_table()`, `.to_pylist()`, etc.).

### I2) Every table read is pinned to a SnapshotKey

A delta-rs `DeltaTable` represents a table at a particular version and exposes that version; `storage_options` is the supported injection point. ([delta-io.github.io][2])

**API rule:** `engine.open_delta(uri, as_of=...)` returns `(DeltaTable, SnapshotKey)` and you always register *that* into DataFusion.

### I3) SQL execution is validated (read-only by default)

DataFusion Python exposes `SQLOptions` and `sql_with_options`, and the defaults allow DDL/DML/statements unless you disable them. ([datafusion.apache.org][3])

**API rule:** `engine.query(sql, readonly=True)` uses:

```python
SQLOptions().with_allow_ddl(False).with_allow_dml(False).with_allow_statements(False)
```

to block `CREATE`, `INSERT`, `SET`, etc. ([datafusion.apache.org][3])

### I4) Per-query ephemeral state is always cleaned up

DataFusion sessions have mutable registries (tables/views/UDFs). You must deregister ephemeral tables at end of request; Python exposes `deregister_table(name)`. ([datafusion.apache.org][3])

### I5) Plan + config artifacts are captured deterministically

* DataFusion exposes `optimized_logical_plan()` and `execution_plan()` on `DataFrame`. ([datafusion.apache.org][4])
* `information_schema.df_settings` exists and is the tabular “SHOW ALL” equivalent (capture it as your config snapshot). ([datafusion.apache.org][5])
* Plan protobuf exists (`to_proto()`), but in-memory record-batch tables aren’t supported for proto serialization (you must handle fallback). ([datafusion.apache.org][6])

### I6) Writes obey an explicit concurrency + retry policy

Delta’s conflict exception families are documented (e.g., `ConcurrentAppendException`, `MetadataChangedException`, `ProtocolChangedException`, …). ([Delta Lake][7])

**API rule:** retries are driven by a classifier that maps conflict families → retryable vs fatal (and “re-run whole transaction” semantics).

---

# 9.2 Public object model (what you export)

A minimal, stable top-level surface:

```python
# fusiondelta/__init__.py
from .engine import FusionDeltaEngine
from .profiles import EngineProfile, StorageProfile
from .snapshots import SnapshotSpec, SnapshotKey
from .sources import DeltaSource, Source
from .results import QueryResult, PlanBundle, WriteResult
from .retry import RetryPolicy, CommitConflict
```

### Core types

* **EngineProfile**: pinned DataFusion SessionConfig + RuntimeEnvBuilder knobs
* **StorageProfile**: produces both:

  * DataFusion object-store registration inputs (for scans)
  * delta-rs `storage_options` dict (for DeltaTable + writes)
* **SnapshotSpec**: “latest | version | timestamp” input
* **SnapshotKey**: `(canonical_uri, resolved_version)` returned everywhere
* **DeltaSource**: `{uri, as_of, alias, options}` used to register tables
* **QueryResult**: streaming result wrapper + artifacts
* **RetryPolicy**: retry classifier + backoff semantics
* **PlanBundle**: optimized logical plan, physical plan, df_settings, plus IDs

---

# 9.3 The engine façade: construction + pooling (public API)

### 9.3.1 Engine creation

You want one call site for the service:

```python
engine = FusionDeltaEngine(
    storage=StorageProfile.from_env(),
    profile=EngineProfile.production(),
    pool_size=8,
)
```

**Under the hood**, each pooled `SessionContext` is constructed from `(SessionConfig, RuntimeEnvBuilder)` as the supported Python configuration mechanism. ([datafusion.apache.org][3])

### 9.3.2 Mandatory: enable information_schema

Your plan bundles rely on `information_schema.df_settings`. Ensure it’s enabled in the pinned profile:

```python
cfg = SessionConfig().with_information_schema(True)
```

This knob is explicitly exposed as `SessionConfig.with_information_schema`. ([datafusion.apache.org][3])

---

# 9.4 Query API: streaming-first, snapshot-enforced, read-only by default

## 9.4.1 QuerySpec

```python
@dataclass(frozen=True)
class QuerySpec:
    sql: str
    sources: Mapping[str, "Source"]         # alias -> source (e.g., DeltaSource)
    param_values: Mapping[str, Any] | None  # scalar substitution post-parse
    readonly: bool = True                   # enforce SQLOptions
    capture_plan: bool = True               # plan bundle capture
```

### Parameterization choices (avoid unsafe string templating)

DataFusion Python `SessionContext.sql()` / `sql_with_options()` supports:

* `param_values` for scalar substitution *after parsing*
* `named_params` for string/DataFrame substitution inside query text ([datafusion.apache.org][3])

**Policy:** expose `param_values` publicly; keep `named_params` behind an “unsafe” gate.

## 9.4.2 Public call

```python
result = engine.query(
    "select p, sum(v) as s from t group by p order by p",
    sources={"t": DeltaSource(uri="s3a://bucket/table", as_of="latest")},
    param_values=None,
)
```

### Implementation sketch (the 10 steps)

```python
def query(self, sql: str, *, sources: Mapping[str, Source], param_values=None, readonly=True) -> QueryResult:
    qid = self._new_query_id()
    run_id = self._current_run_id()

    with self._pool.checkout() as sess:
        ctx = sess.ctx

        # 1) Register all sources (tables/views)
        snapshot_keys = {}
        tmp_names = []
        for alias, src in sources.items():
            name = self._scoped_name(run_id, alias)
            tmp_names.append(name)
            table, sk = src.register(ctx, name, self.storage)  # src may resolve delta snapshot first
            snapshot_keys[alias] = sk

        # 2) Restrict SQL surface
        options = self._readonly_sql_options() if readonly else None

        # 3) Build DataFrame using sql_with_options (validation)
        df = ctx.sql_with_options(sql, options, param_values=param_values) if options else ctx.sql(sql, param_values=param_values)

        # 4) Capture plan bundle + df_settings (optional)
        plan = capture_plan_bundle(ctx, df) if self.profile.capture_plan else None

        # 5) Return streaming result wrapper
        return QueryResult(
            query_id=qid,
            run_id=run_id,
            session_id=ctx.session_id(),
            df=df,
            snapshot_keys=snapshot_keys,
            plan=plan,
            _cleanup=lambda: self._cleanup_tables(ctx, tmp_names),
        )
```

Key primitives above are all real:

* `sql_with_options` validates allowed query types and supports `param_values`. ([datafusion.apache.org][3])
* `SQLOptions` defaults allow DDL/DML/statements unless disabled. ([datafusion.apache.org][3])
* `session_id()` is available for correlation. ([datafusion.apache.org][3])
* `deregister_table(name)` exists for cleanup. ([datafusion.apache.org][3])

---

# 9.5 QueryResult: streaming by default, explicit materialization

### 9.5.1 “Stream” is the primary interface

DataFusion DataFrames implement `__arrow_c_stream__`, and the docs explicitly show building a `pyarrow.RecordBatchReader` without materializing all batches: `pa.RecordBatchReader.from_stream(df)`. ([datafusion.apache.org][1])

Expose:

```python
class QueryResult:
    def stream(self) -> "ArrowStreamExportable":
        return self.df   # DataFrame is stream exportable

    def reader(self) -> "pyarrow.RecordBatchReader":
        import pyarrow as pa
        return pa.RecordBatchReader.from_stream(self.df)

    def __iter__(self):
        return iter(self.df)  # yields batches lazily
```

DataFusion user guide explicitly documents:

* iteration over `df` yields batches lazily
* `execute_stream()` / `execute_stream_partitioned()` for stream control ([datafusion.apache.org][1])

### 9.5.2 Materialization helpers (opt-in)

DataFusion exposes terminal operations:

* `to_arrow_table()` (eager), `to_pylist()`, `to_pandas()`, etc. ([datafusion.apache.org][4])

**Policy:** keep them, but mark as “small result only” in your docstrings and optionally guard by a row/byte limit.

---

# 9.6 Table sources: enforce SnapshotKey at registration time

## 9.6.1 DeltaSource.register()

DeltaTable supports:

* construction with `version=...`
* `storage_options=...`
  and it represents a table state at a particular version. ([delta-io.github.io][8])

```python
@dataclass(frozen=True)
class DeltaSource(Source):
    uri: str
    as_of: SnapshotSpec | None = None   # None/"latest" | int | timestamp string

    def register(self, ctx, name: str, storage: StorageProfile) -> tuple[object, SnapshotKey]:
        from deltalake import DeltaTable

        dt = DeltaTable(self.uri, storage_options=storage.to_deltalake_options())
        if self.as_of not in (None, "latest"):
            dt.load_as_version(self.as_of)      # timestamp or version
        v = dt.version()                        # resolved integer
        sk = SnapshotKey(uri=storage.canonicalize_uri(self.uri), version=v)

        # Register pinned snapshot into DataFusion
        ctx.register_table(name, DeltaTable(self.uri, version=v, storage_options=storage.to_deltalake_options()))
        return dt, sk
```

delta-rs explicitly documents the S3 URL formats (`s3://`, `s3a://`) and that backend config can be provided via `storage_options` (or environment). ([delta-io.github.io][2])

## 9.6.2 Non-Delta sources (Arrow-in, file scans)

If you want your façade to accept arbitrary Arrow streams (e.g., from Rust extensions), DataFusion Python’s `SessionContext.from_arrow()` accepts any object implementing `__arrow_c_stream__` or `__arrow_c_array__`. ([datafusion.apache.org][3])

That means your public API can treat “Arrow stream” as a universal in-memory source type.

---

# 9.7 PlanBundle: deterministic artifacts for regressions

### 9.7.1 Capture plans

DataFusion `DataFrame` exposes:

* `optimized_logical_plan()` and `execution_plan()` (physical plan) ([datafusion.apache.org][4])
* `explain(verbose=False, analyze=False)` (optionally run+metrics) ([datafusion.apache.org][4])

### 9.7.2 Capture df_settings

Use `information_schema.df_settings` (documented directly as “SHOW ALL”). ([datafusion.apache.org][5])

```python
settings = ctx.sql("select * from information_schema.df_settings").to_pylist()
```

### 9.7.3 Proto serialization with fallback

DataFusion plan `to_proto()` exists, but **tables created in memory from record batches aren’t supported** for proto serialization—so your façade must:

* attempt proto
* on failure, store text renderings (`display_indent_schema`, etc.) ([datafusion.apache.org][6])

---

# 9.8 Write API: stream in, enforce concurrency policy, return new SnapshotKey

### 9.8.1 `engine.write_delta(...)` should accept streams

delta-rs documents `write_deltalake` accepts a Pandas DF, a PyArrow Table, **or an iterator of PyArrow RecordBatches** (streaming input). ([delta-io.github.io][9])

So your façade should accept:

* `QueryResult` (convert to reader)
* any ArrowStreamExportable (convert to reader)
* a `pyarrow.Table` (for small writes)

```python
def write_delta(self, uri: str, data: ArrowStreamExportable, *, mode="append", schema_mode=None) -> WriteResult:
    from deltalake import write_deltalake
    reader = self._as_reader(data)  # e.g., pa.RecordBatchReader.from_stream(...)
    write_deltalake(uri, reader, mode=mode, schema_mode=schema_mode, storage_options=self.storage.to_deltalake_options())
    # re-open to get resolved new version
    dt = DeltaTable(uri, storage_options=self.storage.to_deltalake_options())
    return WriteResult(snapshot=SnapshotKey(self.storage.canonicalize_uri(uri), dt.version()))
```

delta-rs docs also spell out `schema_mode="overwrite"|"merge"` behavior and strict default. ([delta-io.github.io][9])

---

# 9.9 Concurrency + retry policy (codified, not “best effort”)

### 9.9.1 Conflict families (source of truth)

Delta Lake’s concurrency control doc enumerates the conflict exception families you must treat explicitly. ([Delta Lake][7])

**Classifier policy (example)**

* **Retryable** (requires full transaction rebuild):
  `ConcurrentAppendException`, `ConcurrentDeleteReadException`, `ConcurrentDeleteDeleteException`
* **Fatal / human intervention**:
  `MetadataChangedException`, `ProtocolChangedException`, `ConcurrentTransactionException` ([Delta Lake][7])

### 9.9.2 Storage safety policy: AWS S3 locking is not optional

delta-rs documents:

* safe concurrent writes to AWS S3 require an external locking mechanism (DynamoDB)
* the `tablePath` is part of the primary key and **must match precisely** across writers (Spark uses `s3a://`)
* delta-rs does **not** read credentials from local `.aws/config` or `.aws/creds` ([delta-io.github.io][10])

This becomes a façade-level policy:

* `StorageProfile` declares whether a location is `aws_s3_locked`, `conditional_put`, or `unsafe`
* `write_delta` refuses to run if policy is missing (unless caller explicitly opts into unsafe via config)

---

# 9.10 Operational “one-liner” debugging commands your façade should expose

Expose these as `engine.debug.*` helpers so agents don’t have to remember them:

### Settings snapshot

```sql
select * from information_schema.df_settings;
```

(DataFusion documents this as the “SHOW ALL” equivalent.) ([datafusion.apache.org][5])

### Explain (plan vs plan+metrics)

Use `df.explain(verbose=True, analyze=False)` and (only when needed) `analyze=True`. ([datafusion.apache.org][4])

### Partitioned streaming for throughput tests

```python
streams = df.execute_stream_partitioned()
```

(one stream per partition) ([datafusion.apache.org][4])

---

## What to implement first (highest ROI sequence)

1. **QueryResult streaming** (`pa.RecordBatchReader.from_stream(df)` as your default output shape) ([datafusion.apache.org][1])
2. **DeltaSource snapshot resolution** (always return `SnapshotKey`) ([delta-io.github.io][2])
3. **Read-only SQL gating** via `SQLOptions` + `sql_with_options` ([datafusion.apache.org][3])
4. **PlanBundle capture** (optimized logical + physical + df_settings; proto with fallback) ([datafusion.apache.org][4])
5. **Write façade** that ingests streams and returns post-commit SnapshotKey ([delta-io.github.io][9])
6. **RetryPolicy + S3 locking enforcement** as hard rules, not suggestions ([Delta Lake][7])

If you want, the next deep-dive after this is: **“Facade implementation patterns for Rust-accelerated extensions”** (how you expose Rust engines/UDF registries behind the same ArrowStreamExportable contract, without leaking DataFusion internals into Python call sites).

[1]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[2]: https://delta-io.github.io/delta-rs/python/usage.html "Usage — delta-rs  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html?utm_source=chatgpt.com "datafusion.plan — Apache Arrow DataFusion documentation"
[7]: https://docs.delta.io/concurrency-control/?utm_source=chatgpt.com "Concurrency control"
[8]: https://delta-io.github.io/delta-rs/api/delta_table/ "DeltaTable - Delta Lake Documentation"
[9]: https://delta-io.github.io/delta-rs/usage/writing/ "Writing Delta Tables - Delta Lake Documentation"
[10]: https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/ "Writing to S3 with a locking provider - Delta Lake Documentation"
