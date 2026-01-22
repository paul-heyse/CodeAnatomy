
Below are **detailed technical narratives + representative code snippets** for the 5 “do next” items, *anchored to your current code structure* (`src/extract/repo_scan.py`, `src/extract/*_extract.py`, `src/extract/helpers.py`, `src/engine/materialize.py`, `src/hamilton_pipeline/modules/extraction.py`). I’ll also do a **deep dive on the key library functions** involved (Git, stdlib, DataFusion Arrow interop, PyArrow writers, delta-rs writer, concurrency, free-threaded 3.13, LibCST + tree-sitter incremental hooks), with citations.

---

# 0) The target extraction architecture in one picture

Your current extraction path is effectively:

**repo_scan → build big `list[dict]` → Arrow table → Ibis plan → `plan.to_table()` → normalize → return TableLike**

The “do next” items move you toward:

**repo_scan (cheap manifest) → DataFusion “worklist” query → extractors emit batches → streaming writer (Delta/Parquet) → (optional) re-open as DataFusion table for downstream transforms**

The key design primitives we’ll introduce:

1. **Repo manifest**: cheap scan + optional hashing, no bytes/text blobs by default.
2. **Worklist**: DataFusion SQL join to decide which files actually need work.
3. **Batch emitter**: extractors yield *row batches* or *RecordBatch* batches.
4. **Unified writer**: `write_extract_outputs(name, data)` for Delta/Parquet without full materialization.
5. **Parallel map**: process pool by default; thread pool only when free-threaded (or I/O bound).

---

# 1) Refactor `repo_scan`: git-aware listing + `os.walk` pruning + `hashlib.file_digest` + don’t store bytes/text by default

## 1.1 What changes (design)

### A) File listing: prefer Git when available

Use `git ls-files` to enumerate:

* tracked files (`--cached`)
* untracked, non-ignored (`--others --exclude-standard`)
* NUL delimit output (`-z`) for safe parsing (handles weird filenames cleanly).

`git ls-files` is designed to “merge the file listing in the index with the actual working directory list” and supports these flags; `-z` makes output NUL-terminated and disables quoting. ([Git][1])

### B) Fallback file listing: `os.walk` with in-place directory pruning

The stdlib explicitly documents that with `topdown=True`, you can modify `dirnames` in-place to prune recursion (skip entire subtrees). ([Python documentation][2])

### C) Hashing: `hashlib.file_digest` instead of `read_bytes()`

Python’s `hashlib.file_digest(fileobj, digest)` exists specifically for *efficient hashing of a file object* (no “slurp whole file into memory” pattern). ([Python documentation][3])

### D) Encoding detection: only when you actually need text

If you include text, `tokenize.detect_encoding(readline)` is the canonical way to detect Python source encoding (PEP 263 cookies, BOM), and it calls `readline` at most twice. ([Python documentation][4])

### E) Default output: **manifest-first**

In your codebase, extractors already fall back to reading bytes from `abs_path` when `bytes/text` are absent (`bytes_from_file_ctx` / `text_from_file_ctx`). So “don’t store bytes/text” works without breaking extraction—**but massively reduces repo_files table size**.

---

## 1.2 Representative implementation (drop-in modules)

### 1.2.1 `extract/repo_scan_git.py`

```python
# src/extract/repo_scan_git.py
from __future__ import annotations

import subprocess
from pathlib import Path
from collections.abc import Iterator

def iter_git_ls_files(repo_root: Path) -> Iterator[str]:
    """
    Uses: git ls-files -z --cached --others --exclude-standard
      --cached = tracked files (default selection)
      --others = untracked files
      --exclude-standard = apply standard ignore rules
      -z = NUL terminated, no quoting
    """
    cmd = [
        "git", "-C", str(repo_root),
        "ls-files",
        "-z",
        "--cached",
        "--others",
        "--exclude-standard",
    ]
    out = subprocess.check_output(cmd)  # subprocess spawns a child process and captures output :contentReference[oaicite:4]{index=4}
    for raw in out.split(b"\x00"):
        if not raw:
            continue
        yield raw.decode("utf-8", errors="surrogateescape")
```

Git semantics + flags are from the `git-ls-files` manual. ([Git][1])

---

### 1.2.2 `extract/repo_scan_fs.py` (fallback)

```python
# src/extract/repo_scan_fs.py
from __future__ import annotations

import os
from pathlib import Path
from collections.abc import Iterator

def iter_walk_files(
    repo_root: Path,
    *,
    exclude_dirs: set[str],
    follow_symlinks: bool,
) -> Iterator[Path]:
    # stdlib: you can prune recursion by modifying dirnames in-place when topdown=True :contentReference[oaicite:6]{index=6}
    for root, dirnames, filenames in os.walk(repo_root, topdown=True, followlinks=follow_symlinks):
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs]
        for fn in filenames:
            yield Path(root, fn)
```

---

### 1.2.3 Patch `extract/repo_scan.py` (core changes)

Key changes:

* set defaults `include_bytes=False`, `include_text=False`
* stop `read_bytes()` just to hash; compute hash from stream
* add a “git-first” iterator

```python
# src/extract/repo_scan.py (representative edits)

import hashlib
import io
import tokenize
from dataclasses import dataclass
from pathlib import Path

from extract.repo_scan_git import iter_git_ls_files
from extract.repo_scan_fs import iter_walk_files

@dataclass(frozen=True)
class RepoScanOptions:
    ...
    include_bytes: bool = False   # NEW DEFAULT
    include_text: bool = False    # NEW DEFAULT
    include_sha256: bool = True   # NEW (lets you disable hashing if desired)
    max_file_bytes: int | None = None
    ...

def _sha256_path(path: Path) -> str:
    # hashlib.file_digest reads from the file object efficiently :contentReference[oaicite:7]{index=7}
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()

def _detect_encoding_and_decode_path(path: Path) -> tuple[str, str | None]:
    # tokenize.detect_encoding implements Python source encoding rules :contentReference[oaicite:8]{index=8}
    try:
        with path.open("rb") as f:
            encoding, _ = tokenize.detect_encoding(f.readline)
            f.seek(0)
            data = f.read()
    except (OSError, LookupError, SyntaxError):
        return "utf-8", None
    try:
        return encoding, data.decode(encoding, errors="replace")
    except (LookupError, UnicodeError):
        return encoding, None

def iter_repo_files(repo_root: Path, options: RepoScanOptions):
    repo_root = repo_root.resolve()
    git_dir = repo_root / ".git"

    if git_dir.exists() and git_dir.is_dir():
        for rel_s in iter_git_ls_files(repo_root):
            rel = Path(rel_s)
            if _is_excluded_dir(rel, options.exclude_dirs):
                continue
            if options.exclude_globs and _matches_any_glob(rel.as_posix(), options.exclude_globs):
                continue
            yield rel
        return

    # fallback: os.walk pruning
    for abs_path in iter_walk_files(
        repo_root,
        exclude_dirs=set(options.exclude_dirs),
        follow_symlinks=options.follow_symlinks,
    ):
        rel = abs_path.relative_to(repo_root)
        # emulate include_globs for fallback (or precompile patterns)
        if not any(abs_path.match(pat) for pat in options.include_globs):
            continue
        if options.exclude_globs and _matches_any_glob(rel.as_posix(), options.exclude_globs):
            continue
        yield rel

def _build_repo_file_row(rel: Path, repo_root: Path, options: RepoScanOptions):
    abs_path = (repo_root / rel).resolve()
    try:
        st = abs_path.stat()
    except OSError:
        return None

    # optional size guard (if you still want it)
    if options.max_file_bytes is not None and st.st_size > options.max_file_bytes:
        return None

    file_sha256 = _sha256_path(abs_path) if options.include_sha256 else None

    encoding = None
    text = None
    data = None

    if options.include_text:
        encoding, text = _detect_encoding_and_decode_path(abs_path)
    if options.include_bytes:
        try:
            data = abs_path.read_bytes()
        except OSError:
            data = None

    return {
        "file_id": None,
        "path": rel.as_posix(),
        "abs_path": str(abs_path),
        "size_bytes": int(st.st_size),
        "file_sha256": file_sha256,
        "encoding": encoding,
        "text": text,
        "bytes": data,
    }
```

---

## 1.3 Patch points (repo_scan PR-style checklist)

**PR-EXTRACT-01: “repo_scan manifest-first”**

* `src/extract/repo_scan.py`

  * change defaults (`include_bytes/include_text=False`)
  * replace `_build_repo_file_row` hashing path to `hashlib.file_digest` ([Python documentation][3])
  * add git-first iterator using `git ls-files -z --cached --others --exclude-standard` ([Git][1])
  * fallback to `os.walk` pruning (mutate `dirnames[:]`) ([Python documentation][2])
  * use `tokenize.detect_encoding` only when `include_text=True` ([Python documentation][4])
* `src/hamilton_pipeline/modules/extraction.py`

  * update defaults for `repo_include_text/repo_include_bytes` to false if that’s acceptable for your workloads

---

# 2) Add DataFusion worklists per extractor: `repo_files ⟂ prior_extract_output` (changed/missing-only extraction)

## 2.1 What changes (design)

Instead of handing *all* file contexts to every extractor, you compute a **worklist**:

* missing output rows (file never extracted)
* stale output rows (file hash differs)
* optionally, invalidated by incremental closure (dependency-driven rebuild)

This is a perfect fit for DataFusion because it’s:

* join-heavy
* filter-heavy
* benefits from predicate pushdown and table statistics

And DataFusion lets you stream results via Arrow C stream when exporting. DataFusion DataFrames implement `__arrow_c_stream__`; calling it triggers execution and yields batches incrementally (lazy, pull-based). ([Apache DataFusion][5])

## 2.2 Representative implementation

### 2.2.1 A “worklist builder” module

```python
# src/extract/worklists.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable

import pyarrow as pa
from datafusion import SessionContext

from extract.helpers import FileContext

@dataclass(frozen=True)
class ExtractWorklist:
    dataset: str
    file_contexts: tuple[FileContext, ...]

def _collect_file_contexts(df) -> tuple[FileContext, ...]:
    # DataFusion -> Arrow via __arrow_c_stream__ is streaming-capable :contentReference[oaicite:14]{index=14}
    t = pa.table(df)  # consumes the stream (still can be large; for huge lists, iterate batches)
    return tuple(FileContext.from_repo_row(row) for row in t.to_pylist())

def worklist_for_dataset(
    ctx: SessionContext,
    *,
    dataset_name: str,
    repo_files_table: str = "repo_files_v1",
    output_table: str,
    output_sha_expr: str,
) -> ExtractWorklist:
    """
    output_sha_expr: SQL expression producing the file hash in output table
                     (varies by dataset; often attrs['file_sha256']).
    """
    # Works even if output table missing columns, as long as output_sha_expr is valid.
    sql = f"""
    SELECT r.file_id, r.path, r.abs_path, r.file_sha256, r.encoding, r.text, r.bytes
    FROM {repo_files_table} r
    LEFT JOIN {output_table} o
      ON o.file_id = r.file_id
    WHERE o.file_id IS NULL
       OR ({output_sha_expr}) IS NULL
       OR ({output_sha_expr}) <> r.file_sha256
    """
    df = ctx.sql(sql)
    return ExtractWorklist(
        dataset=dataset_name,
        file_contexts=_collect_file_contexts(df),
    )
```

**Notes**

* You’ll tailor `output_sha_expr` per dataset:

  * `ast_files_v1`: `o.attrs['file_sha256']` (as stored in your extractor)
  * `libcst_files_v1`: `o.attrs['file_sha256']`
  * `tree_sitter_files_v1`: `o.attrs['file_sha256']`
  * bytecode: similar

### 2.2.2 Use it in the Hamilton extraction nodes

You already have `repo_files_extract` based on `IncrementalImpact`. Layer the per-extractor worklist on top:

```python
# src/hamilton_pipeline/modules/extraction.py (representative)
from extract.worklists import worklist_for_dataset

def ast_bundle(...):
    df_ctx = extract_execution_context.ctx.runtime.datafusion.session_context()

    wl = worklist_for_dataset(
        df_ctx,
        dataset_name="ast_files_v1",
        repo_files_table="repo_files_extract_v1",  # or a temp view name you register
        output_table="ast_files_v1",
        output_sha_expr="o.attrs['file_sha256']",
    )
    tables = extract_ast_tables(
        repo_files=repo_files_extract,
        file_contexts=wl.file_contexts,   # only changed/missing
        ...
    )
    return ...
```

### 2.2.3 “Repo hash reuse” (optional but huge): avoid re-hashing unchanged files

If you add `mtime_ns` to `repo_files_v1`, you can compute:

* **cheap current stat scan**
* DataFusion join with previous repo_files snapshot to reuse `file_sha256` where `(size, mtime)` unchanged
* hash only the remainder

(This is a bigger schema change; but it’s the real unlock if repo hashing becomes the bottleneck.)

---

## 2.3 Deep dive: DataFusion Arrow interoperability you rely on

* **Export**: DataFusion DataFrames implement `__arrow_c_stream__` and yield batches incrementally (lazy execution). ([Apache DataFusion][5])
* **Import**: `SessionContext.from_arrow()` accepts objects implementing `__arrow_c_stream__` / `__arrow_c_array__`, including RecordBatchReader and Table. ([Apache DataFusion][5])

These two together mean you can keep “worklist extraction” *and* later “batch ingestion” engine-native without bespoke adapters.

---

# 3) Standardize batch emission for AST/CST/tree-sitter (stop building huge Python lists)

## 3.1 What changes (design)

Right now:

* AST supports batching in **row_batches**, but `ibis_plan_from_row_batches` still concatenates all tables (`pa.concat_tables`) → still RAM-heavy.
* CST and tree-sitter build full `rows: list[dict]`.

Goal:

* Every extractor implements `iter_*_row_batches(...)` or `iter_*_record_batches(...)`
* A shared helper converts row batches → `pa.RecordBatch` (schema-aligned)
* Downstream writer consumes an **iterable of RecordBatch** (no concat)

## 3.2 Library deep dive: the exact Arrow constructors you want

* `pyarrow.RecordBatch.from_pylist(rows, schema=...)` builds a RecordBatch from list-of-row-dicts. ([Apache Arrow][6])
* `pyarrow.dataset.write_dataset(data=iterable_of_RecordBatch, schema=...)` supports an **iterable of RecordBatch**, but requires a schema when given an iterable. ([Apache Arrow][7])

That pair is the simplest batch pipeline in pure Python.

## 3.3 Shared batching utility

```python
# src/extract/batching.py
from __future__ import annotations
from collections.abc import Iterable, Iterator, Mapping, Sequence
import pyarrow as pa

def record_batches_from_row_batches(
    *,
    schema: pa.Schema,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
) -> Iterator[pa.RecordBatch]:
    for batch in row_batches:
        if not batch:
            continue
        # RecordBatch.from_pylist is a supported constructor :contentReference[oaicite:19]{index=19}
        yield pa.RecordBatch.from_pylist(list(batch), schema=schema)
```

## 3.4 Exemplar extractor patch: `tree_sitter_extract.py`

Add `batch_size` and implement row batching.

```python
# src/extract/tree_sitter_extract.py (representative edits)
@dataclass(frozen=True)
class TreeSitterExtractOptions:
    ...
    batch_size: int | None = 512  # NEW

def _iter_ts_rows(...):
    for file_ctx in iter_contexts(repo_files, file_contexts):
        row = _extract_ts_file_row(...)
        if row is not None:
            yield row

def _iter_ts_row_batches(..., batch_size: int):
    batch = []
    for row in _iter_ts_rows(...):
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
```

Then instead of `rows = _collect_ts_rows(...)` you can do:

* (intermediate step) `ibis_plan_from_row_batches` (still concatenates, but removes giant python list)
* (target step) stream to writer (next sections)

## 3.5 Exemplar extractor patch: `cst_extract.py`

Same: add `batch_size`, but be careful about `FullRepoManager` reuse:

* build repo_manager once
* run batches where each worker uses `repo_manager.get_cache_for_path(...)`

---

# 4) Generalize `write_ast_outputs` → `write_extract_outputs` for *all* extract datasets (Delta/Parquet), and make it streaming-friendly

## 4.1 What changes (design)

Today:

* `materialize_extract_plan()` always does `plan.to_table()` (full materialization)
* then `write_ast_outputs(name, normalized, ctx=ctx)` (AST-only)

Target:

* `write_extract_outputs(name, data, ctx=ctx)` supports:

  * **Delta**: prefer DataFusion-native write when you have a DF; otherwise use `write_deltalake` with RecordBatch iterator.
  * **Parquet/IPC/CSV**: use `pyarrow.dataset.write_dataset` with iterable of RecordBatch.

## 4.2 Library deep dive: the two canonical streaming writers

### A) PyArrow dataset writer (Parquet/IPC/CSV)

`pyarrow.dataset.write_dataset` accepts:

* Table/RecordBatch/RecordBatchReader,
* **or an iterable of RecordBatch** (schema required),
  and can write in parallel with `use_threads=True`. ([Apache Arrow][7])

### B) delta-rs `write_deltalake`

`write_deltalake` accepts a Pandas DF, PyArrow Table, **or an iterator of PyArrow RecordBatches** (append/overwrite modes). ([delta-rs-docs.pages.dev][8])

## 4.3 Representative unified writer

```python
# src/engine/materialize_extract.py
from __future__ import annotations
from collections.abc import Iterable
import pyarrow as pa
import pyarrow.dataset as ds
from deltalake.writer import write_deltalake

from arrowdsl.core.interop import TableLike, RecordBatchReaderLike

def _iter_batches(data: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch]) -> Iterable[pa.RecordBatch]:
    if isinstance(data, pa.Table):
        yield from data.to_batches()
        return
    if hasattr(data, "__iter__") and hasattr(data, "schema"):
        # RecordBatchReaderLike: yields RecordBatch
        yield from data  # type: ignore[misc]
        return
    yield from data  # already an iterable of batches

def write_extract_outputs(
    name: str,
    data: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
    *,
    schema: pa.Schema,
    location_format: str,
    base_dir: str,
) -> None:
    if location_format == "delta":
        # delta-rs: accepts iterator of RecordBatches :contentReference[oaicite:22]{index=22}
        write_deltalake(base_dir, _iter_batches(data), mode="append", schema=schema)
        return

    # pyarrow.dataset.write_dataset accepts iterable of RecordBatch (schema required) :contentReference[oaicite:23]{index=23}
    ds.write_dataset(
        data=_iter_batches(data),
        base_dir=base_dir,
        format=location_format,  # "parquet" / "ipc" / "csv"
        schema=schema,
        use_threads=True,
        existing_data_behavior="overwrite_or_ignore",
        basename_template="part-{i}",
    )
```

### Where it hooks into your code immediately

In `src/extract/helpers.py`, change:

```python
from engine.materialize import write_ast_outputs
...
write_ast_outputs(name, normalized, ctx=ctx)
```

to:

```python
from engine.materialize_extract import write_extract_outputs
...
write_extract_outputs(name, normalized_or_reader_or_batches, ...)
```

But you’ll also want a **location resolver** in `DataFusionRuntimeProfile`:

* you already have `ast_dataset_location()`, `_bytecode_dataset_location()`, etc.
  Add:
* `dataset_location(name: str) -> DatasetLocation | None` that dispatches by dataset name.

---

# 5) Parallelize AST/CST/tree-sitter with a process pool (threads only when free-threaded 3.13)

## 5.1 Library deep dive: why ProcessPoolExecutor is the default

`concurrent.futures.ProcessPoolExecutor` uses multiprocessing, **sidestepping the GIL**, but requires **picklable** callables/args and an importable `__main__`. ([Python documentation][9])

So for CPU-heavy parsing (AST / LibCST / tree-sitter queries), ProcessPoolExecutor is typically correct.

## 5.2 Free-threaded Python 3.13: when threads become real

CPython 3.13 introduced a free-threaded build where the GIL can be disabled, and you can check runtime GIL status via `sys._is_gil_enabled()`. ([Python documentation][10])

So you can choose:

* if free-threaded build **and** GIL disabled → `ThreadPoolExecutor` can scale for CPU parsing
* else → `ProcessPoolExecutor`

## 5.3 LibCST-specific nuance: resolve caches before forking

LibCST’s `FullRepoManager` has a `resolve_cache()` and the docs note it’s normally called by `get_cache_for_path()`, but doing a single resolution pass *before forking* can be desirable to control when cache resolution happens. ([LibCST][11])

That’s directly relevant to your process-pool plan: you don’t want every worker paying the cache resolution cost in parallel.

## 5.4 tree-sitter incremental hooks (optional but synergistic)

Tree-sitter Python bindings support:

* `Parser.parse(source, old_tree=...)` for incremental parse. ([Tree-sitter][12])
* `Tree.changed_ranges(new_tree)` to restrict re-query regions. ([Tree-sitter][13])

In a *process pool*, you typically don’t keep long-lived old trees per file, so incremental parse isn’t as helpful unless you keep a persistent worker (advanced). But it’s still valuable in a daemon/service mode.

## 5.5 Representative parallel helper

```python
# src/extract/parallel.py
from __future__ import annotations
import os
import sys
import sysconfig
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from collections.abc import Iterable, Iterator, Callable
from typing import TypeVar

T = TypeVar("T")
U = TypeVar("U")

def _gil_disabled() -> bool:
    # free-threaded docs: sys._is_gil_enabled() indicates whether GIL is enabled :contentReference[oaicite:29]{index=29}
    fn = getattr(sys, "_is_gil_enabled", None)
    if callable(fn):
        return not bool(fn())
    # build-time indicator
    return sysconfig.get_config_var("Py_GIL_DISABLED") == 1  # :contentReference[oaicite:30]{index=30}

def parallel_map(
    items: Iterable[T],
    fn: Callable[[T], U],
    *,
    max_workers: int | None = None,
    prefer_threads_when_nogil: bool = True,
) -> Iterator[U]:
    if prefer_threads_when_nogil and _gil_disabled():
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            yield from ex.map(fn, items)
        return

    # ProcessPoolExecutor side-steps the GIL but requires picklable work :contentReference[oaicite:31]{index=31}
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        yield from ex.map(fn, items)
```

## 5.6 Exemplar extractor patch (tree-sitter)

Make `_extract_ts_file_row` callable on a “job” struct containing only picklable fields: `(abs_path, file_id, path, file_sha256, options)`.

Worker initializes its own `Parser` and `Language` (or uses module-level cached language).

---

# PR-style patch plan for the two concrete files you asked about

## PR-EXTRACT-01: repo_scan refactor (exact patch points)

* **`src/extract/repo_scan.py`**

  * change defaults: `include_bytes=False`, `include_text=False`
  * use `hashlib.file_digest` for sha256 ([Python documentation][3])
  * only call `tokenize.detect_encoding` if `include_text=True` ([Python documentation][4])
  * replace `glob("**/*.py")` path walk with:

    * Git mode using `git ls-files -z --cached --others --exclude-standard` ([Git][1])
    * fallback `os.walk` pruning (`dirnames[:] = ...`) ([Python documentation][2])
* **new files**

  * `src/extract/repo_scan_git.py`
  * `src/extract/repo_scan_fs.py`

## PR-EXTRACT-02: exemplar extractor (tree-sitter) batch + parallel skeleton

* **`src/extract/tree_sitter_extract.py`**

  * add `batch_size`, `max_workers`, `parallel` flags to `TreeSitterExtractOptions`
  * refactor `_collect_ts_rows` → `_iter_ts_rows` + `_iter_ts_row_batches`
  * optionally use `parallel_map` for per-file row extraction
* **new file**

  * `src/extract/parallel.py`

---

If you want, I can go one level deeper and produce:

* a concrete `dataset_location(name)` dispatcher for `DataFusionRuntimeProfile`,
* a fully working `write_extract_outputs` that respects your existing `DatasetLocation` / scan options,
* and a “safe-by-default” parallelization strategy for LibCST (`resolve_cache()` before forking, then per-worker parsing). ([LibCST][11])

[1]: https://git-scm.com/docs/git-ls-files "Git - git-ls-files Documentation"
[2]: https://docs.python.org/3/library/os.html "https://docs.python.org/3/library/os.html"
[3]: https://docs.python.org/3/library/hashlib.html?utm_source=chatgpt.com "hashlib — Secure hashes and message digests"
[4]: https://docs.python.org/3/library/tokenize.html "https://docs.python.org/3/library/tokenize.html"
[5]: https://datafusion.apache.org/python/user-guide/io/arrow.html "https://datafusion.apache.org/python/user-guide/io/arrow.html"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html "https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html "https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html"
[8]: https://delta-rs-docs.pages.dev/usage/writing-delta-tables/ "https://delta-rs-docs.pages.dev/usage/writing-delta-tables/"
[9]: https://docs.python.org/3/library/concurrent.futures.html "https://docs.python.org/3/library/concurrent.futures.html"
[10]: https://docs.python.org/3/howto/free-threading-python.html "https://docs.python.org/3/howto/free-threading-python.html"
[11]: https://libcst.readthedocs.io/en/latest/metadata.html "https://libcst.readthedocs.io/en/latest/metadata.html"
[12]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Parser.html "https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Parser.html"
[13]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Tree.html "https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Tree.html"



Below is the “one level deeper” layer, written to drop cleanly into your current codebase patterns.

I’m going to give you:

1. a **concrete `dataset_location(name)` dispatcher** for `DataFusionRuntimeProfile` (covers AST/bytecode + SCIP + registry catalogs, and gives you a single canonical place to ask “where do I read/write this dataset?”)

2. a **fully working `write_extract_outputs`** that:

   * resolves **DatasetLocation + DatasetSpec precedence** the same way your registry does,
   * uses **DataFusion-native Parquet writes** (partition_by + sort_by) when appropriate,
   * uses **delta-rs `write_deltalake`** for Delta writes (streaming supported),
   * falls back to **pyarrow.dataset.write_dataset** for non-parquet formats (streaming supported, schema required for iterables) ([Apache Arrow][1])

3. a **safe-by-default LibCST parallelization strategy** that:

   * calls **`FullRepoManager.resolve_cache()` before forking** (LibCST explicitly recommends this) ([LibCST][2])
   * uses **ProcessPoolExecutor with `mp_context=multiprocessing.get_context("fork")`** when you need inherited caches (and notes the Python stdlib semantics / pitfalls) ([Python documentation][3])
   * keeps the “hard” FullRepoManager objects **out of pickling**, relying on fork inheritance.

---

## 1) `DataFusionRuntimeProfile.dataset_location(name)` dispatcher

### Why this matters

Right now, “location lookup” is fragmented:

* AST has `_ast_dataset_location() / ast_dataset_location()`
* bytecode has `_bytecode_dataset_location()`
* SCIP has `scip_dataset_locations`
* everything else is typically in your **registry catalogs** (`registry_catalogs: Mapping[str, DatasetCatalog]`)

But writers/materializers want **one call** that answers:

> “Given dataset name X, where is it stored (format/path/options)?”

### Patch (add to `src/datafusion_engine/runtime.py` inside `DataFusionRuntimeProfile`)

```python
# src/datafusion_engine/runtime.py
from dataclasses import replace
from ibis_engine.registry import DatasetLocation
from datafusion_engine.schema_registry import SCIP_VIEW_SCHEMA_MAP

class DataFusionRuntimeProfile:
    ...

    def dataset_location(self, name: str) -> DatasetLocation | None:
        """
        Unified dispatcher for dataset locations.

        Precedence:
          1) Built-in profile-configured datasets (AST, bytecode)
          2) Explicit SCIP dataset locations (supports alias->schema mapping)
          3) Registry catalogs (any DatasetCatalog attached to the profile)
        """
        # 1) built-in datasets
        if name == "ast_files_v1":
            return self._ast_dataset_location()
        if name == "bytecode_files_v1":
            return self._bytecode_dataset_location()

        # 2) SCIP dataset locations: support both requested names and mapped schema names
        if name in self.scip_dataset_locations:
            return self.scip_dataset_locations[name]
        for requested, loc in self.scip_dataset_locations.items():
            schema_name = SCIP_VIEW_SCHEMA_MAP.get(requested, requested)
            if schema_name == name:
                return loc

        # 3) registry catalogs (covers libcst_files_v1, tree_sitter_files_v1, symtable_files_v1, etc.)
        for _, catalog in sorted(self.registry_catalogs.items()):
            if catalog.has(name):
                return catalog.get(name)

        return None

    def dataset_location_or_raise(self, name: str) -> DatasetLocation:
        loc = self.dataset_location(name)
        if loc is None:
            raise KeyError(f"No dataset location configured for {name!r}.")
        return loc
```

**How this composes with your existing registration system:**
Your DataFusion registry bridge already has “resolve scan policy precedence” helpers (location override > dataset_spec defaults). We’ll reuse those in the writer next.

---

## 2) Fully working `write_extract_outputs` (respects DatasetLocation + scan policy)

### Key mechanics (library behavior)

* **DataFusion → Arrow streaming**: DataFusion DataFrames implement `__arrow_c_stream__`; batches are produced incrementally and the full result is not materialized in memory ([Apache DataFusion][4])
* **DataFusion ingestion**: `SessionContext.from_arrow()` accepts objects implementing `__arrow_c_stream__` / `__arrow_c_array__` ([Apache DataFusion][5])
* **DataFusion parquet writes**: `DataFrameWriteOptions` supports `partition_by` and `sort_by` (plus single file output) ([Apache DataFusion][4])
* **Delta writes (delta-rs)**: `write_deltalake(...)` accepts Arrow sources including an iterator/stream of RecordBatches; it supports `schema_mode`, `configuration`, `target_file_size`, etc. ([Delta][6])

  * If you pass an *iterable* of batches, you must provide schema (practically: wrap it in a `RecordBatchReader` via `RecordBatchReader.from_batches(schema, batches)`) ([Delta][6])
* **PyArrow dataset writes**: `pyarrow.dataset.write_dataset` accepts an iterable of RecordBatch, but **schema must be provided** when data is an iterable ([Apache Arrow][1])

### New module: `src/engine/materialize_extract_outputs.py`

This is designed to be a direct sibling of `engine/materialize.py`, and to be callable from `extract/helpers.materialize_extract_plan`.

```python
# src/engine/materialize_extract_outputs.py
from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
import pyarrow.dataset as ds
from deltalake import write_deltalake

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.bridge import datafusion_from_arrow, datafusion_write_parquet
from datafusion_engine.runtime import diagnostics_arrow_ingest_hook
from ibis_engine.registry import (
    DatasetLocation,
    resolve_datafusion_scan_options,
    resolve_delta_schema_policy,
    resolve_delta_write_policy,
)
from schema_spec.policies import DataFusionWritePolicy
from storage.deltalake.config import delta_schema_configuration, delta_write_configuration


@dataclass(frozen=True)
class ExtractWriteResult:
    dataset: str
    format: str
    path: str
    mode: str
    rows: int | None
    write_policy: Mapping[str, object] | None = None


def _iter_batches(value: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    if isinstance(value, pa.Table):
        yield from value.to_batches()
        return
    if isinstance(value, pa.RecordBatchReader):
        yield from value
        return
    # assume iterable of RecordBatch
    yield from cast("Iterable[pa.RecordBatch]", value)


def _as_stream(
    value: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
    *,
    schema: pa.Schema,
) -> RecordBatchReaderLike:
    """
    Ensure we have something stream-exportable with a schema.
    RecordBatchReader.from_batches accepts an iterable of batches. :contentReference[oaicite:9]{index=9}
    """
    if isinstance(value, pa.RecordBatchReader):
        return value
    if isinstance(value, pa.Table):
        return pa.RecordBatchReader.from_batches(value.schema, value.to_batches())
    return pa.RecordBatchReader.from_batches(schema, _iter_batches(value))


def _write_policy_from_location(
    runtime_profile,
    *,
    location: DatasetLocation,
    schema: pa.Schema,
) -> DataFusionWritePolicy | None:
    """
    Derive DataFusion write policy from:
      - location.datafusion_scan / dataset_spec.datafusion_scan (partition_cols + file_sort_order)
      - runtime_profile.write_policy (compression/rowgroup knobs + optional overrides)
    """
    scan = resolve_datafusion_scan_options(location)
    available = set(schema.names)

    default_partitions: tuple[str, ...] = ()
    default_sort: tuple[str, ...] = ()
    if scan is not None:
        default_partitions = tuple(name for name, _ in scan.partition_cols if name in available)
        default_sort = tuple(name for name in scan.file_sort_order if name in available)

    base = getattr(runtime_profile, "write_policy", None)
    if base is None:
        if not default_partitions and not default_sort:
            return None
        return DataFusionWritePolicy(partition_by=default_partitions, sort_by=default_sort)

    return DataFusionWritePolicy(
        partition_by=tuple(base.partition_by) if base.partition_by else default_partitions,
        single_file_output=base.single_file_output,
        sort_by=tuple(base.sort_by) if base.sort_by else default_sort,
        parquet_compression=base.parquet_compression,
        parquet_statistics_enabled=base.parquet_statistics_enabled,
        parquet_row_group_size=base.parquet_row_group_size,
    )


def _record_write(runtime_profile, result: ExtractWriteResult) -> None:
    sink = getattr(runtime_profile, "diagnostics_sink", None)
    if sink is None:
        return
    sink.record_artifact(
        "extract_output_writes_v1",
        {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": result.dataset,
            "format": result.format,
            "path": result.path,
            "mode": result.mode,
            "rows": result.rows,
            "write_policy": dict(result.write_policy) if result.write_policy else None,
        },
    )


def write_extract_outputs(
    name: str,
    value: TableLike | RecordBatchReaderLike | Iterable[pa.RecordBatch],
    *,
    ctx: ExecutionContext,
    mode: str = "append",
) -> None:
    """
    Generic extract output writer driven by DatasetLocation.
    - Parquet: DataFusion-native write (partition_by + sort_by) when possible
    - Delta: delta-rs write_deltalake (streaming supported) :contentReference[oaicite:10]{index=10}
    - Other: pyarrow.dataset.write_dataset (schema required for iterables) :contentReference[oaicite:11]{index=11}
    """
    runtime_profile = ctx.runtime.datafusion
    if runtime_profile is None:
        return

    location = runtime_profile.dataset_location(name)  # new dispatcher
    if location is None:
        return

    # Choose schema for streaming cases where it must be explicit.
    # Prefer location’s dataset_spec schema if present, else fall back to runtime schema registry.
    schema = None
    if location.dataset_spec is not None:
        schema = cast("pa.Schema", location.dataset_spec.schema())
    else:
        # fall back to DataFusion schema registry
        from datafusion_engine.schema_registry import schema_for
        schema = schema_for(name)

    # 1) DELTA
    if location.format == "delta":
        delta_write_policy = resolve_delta_write_policy(location)
        delta_schema_policy = resolve_delta_schema_policy(location)

        configuration: dict[str, str | None] = {}
        # Persist table properties (e.g. delta.targetFileSize, stats columns, column mapping)
        cfg_a = delta_write_configuration(delta_write_policy)
        if cfg_a:
            configuration.update(cfg_a)
        cfg_b = delta_schema_configuration(delta_schema_policy)
        if cfg_b:
            configuration.update(cfg_b)

        # Ensure the data is a schema-bearing Arrow stream for best streaming behavior.
        stream = _as_stream(value, schema=schema)

        # delta-rs write_deltalake supports schema_mode and configuration, and accepts Arrow streams. :contentReference[oaicite:12]{index=12}
        write_deltalake(
            str(location.path),
            stream,
            mode=mode,
            schema_mode=(delta_schema_policy.schema_mode if delta_schema_policy else None),
            configuration=configuration or None,
            storage_options=dict(location.storage_options) if location.storage_options else None,
            target_file_size=(delta_write_policy.target_file_size if delta_write_policy else None),
        )

        _record_write(
            runtime_profile,
            ExtractWriteResult(
                dataset=name,
                format="delta",
                path=str(location.path),
                mode=mode,
                rows=(value.num_rows if isinstance(value, pa.Table) else None),
                write_policy={
                    "configuration": configuration or None,
                    "schema_mode": (delta_schema_policy.schema_mode if delta_schema_policy else None),
                    "target_file_size": (delta_write_policy.target_file_size if delta_write_policy else None),
                },
            ),
        )
        return

    # 2) PARQUET via DataFusion-native writer (preferred)
    if location.format == "parquet":
        df_ctx = runtime_profile.session_context()
        temp_name = f"__extract_write_{name}_{uuid.uuid4().hex}"

        ingest_hook = (
            diagnostics_arrow_ingest_hook(runtime_profile.diagnostics_sink)
            if runtime_profile.diagnostics_sink is not None
            else None
        )
        # SessionContext.from_arrow accepts Arrow stream/array providers. :contentReference[oaicite:13]{index=13}
        df = datafusion_from_arrow(df_ctx, name=temp_name, value=value, ingest_hook=ingest_hook)

        policy = _write_policy_from_location(runtime_profile, location=location, schema=df.schema())
        try:
            payload = datafusion_write_parquet(df, path=str(location.path), policy=policy)
        finally:
            with contextlib.suppress(Exception):
                df_ctx.deregister_table(temp_name)

        _record_write(
            runtime_profile,
            ExtractWriteResult(
                dataset=name,
                format="parquet",
                path=str(location.path),
                mode="copy",
                rows=(value.num_rows if isinstance(value, pa.Table) else None),
                write_policy=payload,
            ),
        )
        return

    # 3) Fallback: pyarrow.dataset.write_dataset for ipc/csv/etc (streaming)
    # write_dataset accepts iterable of RecordBatch but schema must be provided. :contentReference[oaicite:14]{index=14}
    ds.write_dataset(
        data=_iter_batches(value),
        base_dir=str(location.path),
        format=str(location.format),
        schema=schema,
        use_threads=True,
        existing_data_behavior="overwrite_or_ignore",
        basename_template="part-{i}",
    )

    _record_write(
        runtime_profile,
        ExtractWriteResult(
            dataset=name,
            format=str(location.format),
            path=str(location.path),
            mode="copy",
            rows=(value.num_rows if isinstance(value, pa.Table) else None),
            write_policy={"writer": "pyarrow.dataset.write_dataset"},
        ),
    )
```

### Patch point: replace the AST-only write hook

In `src/extract/helpers.py`, swap:

```python
from engine.materialize import write_ast_outputs
...
write_ast_outputs(name, normalized, ctx=ctx)
```

with:

```python
from engine.materialize_extract_outputs import write_extract_outputs
...
write_extract_outputs(name, normalized, ctx=ctx, mode="append")
```

Now *any* extract output that has a configured location will write.

---

## 3) Safe-by-default parallel LibCST extraction (resolve_cache before forking)

### What we are optimizing for

When you enable either:

* `FullyQualifiedNameProvider`
* `TypeInferenceProvider`

LibCST’s `FullRepoManager` builds provider caches by calling each provider’s `gen_cache` and storing a mapping of path→cache. The docs note:

* `resolve_cache()` is normally called by `get_cache_for_path`, **but** if you want “a single cache resolution pass before forking,” you should call it explicitly. ([LibCST][2])

That is exactly your desired pattern:

1. build + resolve caches once in parent
2. fork workers so each inherits cache memory (copy-on-write)
3. workers parse files and call `repo_manager.get_cache_for_path(rel_path)` (fast, no re-resolution)

### Why `mp_context="fork"` is non-negotiable here

Python’s `ProcessPoolExecutor` supports `mp_context`, which controls start method; the docs explicitly show it and warn about pickling constraints. ([Python documentation][3])
If you don’t fork, you can’t cheaply share the precomputed LibCST caches.

---

### Patch (representative) inside `src/extract/cst_extract.py`

#### A) add options

```python
# src/extract/cst_extract.py
from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class CSTExtractOptions:
    ...
    parallel: bool = True
    max_workers: int | None = None
    chunksize: int = 8
    mp_start_method: Literal["fork", "spawn", "forkserver"] = "fork"
```

#### B) global repo manager (fork-inherited)

```python
# src/extract/cst_extract.py
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

_GLOBAL_REPO_MANAGER: FullRepoManager | None = None

def _cst_row_worker(job: tuple[FileContext, CSTExtractOptions]) -> dict[str, object] | None:
    file_ctx, options = job
    # IMPORTANT: repo manager is not pickled; it is inherited by fork
    repo_manager = _GLOBAL_REPO_MANAGER
    return _cst_file_row(
        file_ctx,
        options=options,
        evidence_plan=None,
        repo_manager=repo_manager,
    )
```

#### C) the parallel collector (fork-only when repo_manager exists)

```python
def _collect_cst_file_rows(
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None,
    *,
    options: CSTExtractOptions,
    evidence_plan: EvidencePlan | None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    contexts = list(iter_contexts(repo_files, file_contexts))

    # Build and resolve repo cache BEFORE forking (LibCST explicitly recommends this) :contentReference[oaicite:17]{index=17}
    repo_manager = _build_repo_manager(options, contexts)

    # Decide whether parallelism is safe
    want_parallel = options.parallel and (options.max_workers is None or options.max_workers != 1)

    # If we need repo_manager caches, require fork
    if repo_manager is not None:
        if options.mp_start_method != "fork":
            # safe-by-default: don’t try to pickle repo_manager into spawn workers
            want_parallel = False

    if not want_parallel:
        for file_ctx in contexts:
            row = _cst_file_row(
                file_ctx,
                options=options,
                evidence_plan=evidence_plan,
                repo_manager=repo_manager,
            )
            if row is not None:
                rows.append(row)
        return rows

    # Fork workers so they inherit repo_manager (copy-on-write)
    global _GLOBAL_REPO_MANAGER
    _GLOBAL_REPO_MANAGER = repo_manager

    # ProcessPoolExecutor supports mp_context (start method control). :contentReference[oaicite:18]{index=18}
    mp_ctx = mp.get_context(options.mp_start_method)
    jobs = [(ctx, options) for ctx in contexts]

    with ProcessPoolExecutor(
        max_workers=options.max_workers,
        mp_context=mp_ctx,
    ) as ex:
        # ProcessPoolExecutor.map supports chunksize (important for overhead). :contentReference[oaicite:19]{index=19}
        for row in ex.map(_cst_row_worker, jobs, chunksize=options.chunksize):
            if row is not None:
                rows.append(row)

    return rows
```

### Notes on behavior and robustness

* This relies on `fork` semantics: **the manager + resolved cache live in memory and are inherited**.
* It avoids pickling the `FullRepoManager`.
* It still honors the LibCST `MetadataWrapper(..., cache=...)` approach you already use; `get_cache_for_path()` remains the per-file “fast path.” ([LibCST][2])
* If you move to Python versions where default start method changes (or if you run under environments that force `spawn`), the “safe-by-default” behavior is to fall back to sequential rather than silently exploding runtime cost.

### Optional: threads on free-threaded 3.13+

If you ever want to conditionally use threads (instead of processes) on free-threaded builds, Python provides `sys._is_gil_enabled()` to detect if the GIL is actually disabled. ([Python documentation][7])
But for LibCST **with FullRepoManager**, I’d still default to process+fork unless you’ve validated thread safety of all involved providers.

---

## Minimal patch checklist (so you can implement fast)

1. `src/datafusion_engine/runtime.py`

   * add `dataset_location()` and `dataset_location_or_raise()`

2. `src/engine/materialize_extract_outputs.py`

   * add the unified writer module

3. `src/extract/helpers.py`

   * replace `write_ast_outputs(...)` with `write_extract_outputs(...)`

4. `src/extract/cst_extract.py`

   * add the safe parallel collector (fork-only when FullRepoManager is required)


[1]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html?utm_source=chatgpt.com "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[2]: https://libcst.readthedocs.io/en/latest/metadata.html "Metadata — LibCST  documentation"
[3]: https://docs.python.org/3/library/concurrent.futures.html "concurrent.futures — Launching parallel tasks — Python 3.14.2 documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[6]: https://delta-io.github.io/delta-rs/api/delta_writer/ "Writer - Delta Lake Documentation"
[7]: https://docs.python.org/3/howto/free-threading-python.html?utm_source=chatgpt.com "Python support for free threading — Python 3.14.2 ..."
