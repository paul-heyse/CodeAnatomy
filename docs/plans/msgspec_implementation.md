Below are three “drop-in” **msgspec** patterns mapped directly to what I see in your repo today:

* `src/incremental/cdf_cursors.py` → dataclass + `from_dict()` + `_coerce_int`
* `src/obs/manifest.py` → many dataclasses + `asdict()` / ad-hoc JSON-ish payloads
* `src/obs/repro.py` → large “run bundle” payloads + `_normalize_value()` / `_stable_repr()`

The goal is the same in each case: **replace “dict-shaping + coercion” with typed schemas** that serialize/validate fast, while staying compatible with your existing “payload hash” and Delta/Arrow write flows.

Key msgspec facts we’ll lean on:

* `msgspec.Struct` supports config like `frozen`, `kw_only`, `omit_defaults`, `forbid_unknown_fields`, `tag`, `rename`, etc. ([jcristharif.com][1])
* `msgspec.json.encode/decode` are the fast path; `decode(..., strict=False)` enables broader string→number coercions; `encode(..., order='deterministic'|'sorted')` gives stable ordering for hashing. ([jcristharif.com][1])
* `msgspec.to_builtins` + `msgspec.convert` are explicitly intended as “pre/post processors” when pairing msgspec with other serializers (like orjson). ([jcristharif.com][2])
* Constraints can be expressed via `Annotated[..., msgspec.Meta(...)]` and enforced on typed decode. ([jcristharif.com][3])

---

## Pattern 0: one central codec module (JSON + orjson interop + stable hashing)

Put this somewhere like `src/obs/serde_msgspec.py` (or `src/core/serde.py`) and make every artifact use it.

```python
# src/obs/serde_msgspec.py
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, TypeVar

import msgspec
import msgspec.json
import orjson

T = TypeVar("T")

# Good default: stable bytes for hashing, but don't reorder struct fields by name.
_DEFAULT_ORDER = "deterministic"  # see msgspec.json.encode(..., order=...) docs

def dumps_json(obj: Any, *, pretty: bool = False) -> bytes:
    raw = msgspec.json.encode(obj, order=_DEFAULT_ORDER)
    if not pretty:
        return raw
    # format() pretty-prints an existing JSON message
    return msgspec.json.format(raw, indent=2)

def loads_json(buf: bytes, type: type[T], *, strict: bool = True) -> T:
    # strict=False enables wider string->non-string coercions during typed decode
    return msgspec.json.decode(buf, type=type, strict=strict)

def sha256_json(obj: Any) -> str:
    return hashlib.sha256(dumps_json(obj, pretty=False)).hexdigest()

def dumps_orjson(obj: Any, *, sort_keys: bool = True) -> bytes:
    # Interop mode: convert to pure builtins, then let orjson handle it.
    builtins = msgspec.to_builtins(obj)
    opt = orjson.OPT_SORT_KEYS if sort_keys else 0
    return orjson.dumps(builtins, option=opt)

def loads_orjson(buf: bytes, type: type[T], *, strict: bool = True) -> T:
    # Decode with orjson, validate/convert with msgspec
    return msgspec.convert(orjson.loads(buf), type=type, strict=strict)
```

Why this matters in your codebase:

* you can **delete** most “defensive parsing” helpers (`_coerce_int`, many `_normalize_value` call sites)
* you get **stable bytes** for hashing by construction (`order='deterministic'`) ([jcristharif.com][1])
* you can choose msgspec-native JSON (fastest) vs orjson (interop knobs) without changing artifact schemas ([jcristharif.com][2])

---

## Example 1: rewrite `CdfCursor` and store (kill `_coerce_int`)

Your current `cdf_cursors.py` has the classic shape: dataclass → `to_dict()` → json dump; `from_dict()` + `_coerce_int()` to tolerate strings.

This becomes:

```python
# src/incremental/cdf_cursors_msgspec.py
from __future__ import annotations

from pathlib import Path
from typing import Annotated

import msgspec
import msgspec.json

# Optional: validate last_version >= 0 on typed decode
NonNegInt = Annotated[int, msgspec.Meta(ge=0)]

class CdfCursor(msgspec.Struct, frozen=True, kw_only=True):
    dataset_name: str
    last_version: NonNegInt

# Decoder/encoder objects are slightly faster if reused
_CURSOR_DECODER = msgspec.json.Decoder(type=CdfCursor, strict=False)
_CURSOR_ENCODER = msgspec.json.Encoder(order="sorted")  # stable + human-ish (field-name sorted)

def _cursor_path(base: Path, dataset_name: str) -> Path:
    safe = dataset_name.replace("/", "_").replace("\\", "_")
    return base / f"{safe}.cursor.json"

def save_cursor(base: Path, cursor: CdfCursor) -> None:
    base.mkdir(parents=True, exist_ok=True)
    raw = _CURSOR_ENCODER.encode(cursor)
    pretty = msgspec.json.format(raw, indent=2)
    _cursor_path(base, cursor.dataset_name).write_bytes(pretty)

def load_cursor(base: Path, dataset_name: str) -> CdfCursor | None:
    path = _cursor_path(base, dataset_name)
    if not path.exists():
        return None
    try:
        return _CURSOR_DECODER.decode(path.read_bytes())
    except (OSError, msgspec.ValidationError, msgspec.DecodeError):
        return None
```

Notes:

* `strict=False` replaces your `_coerce_int()` by allowing broader coercions during typed decode (string → int, etc.). ([jcristharif.com][1])
* Constraints (`Meta(ge=0)`) give you a **real invariant**: corrupted cursors fail fast. ([jcristharif.com][3])
* If you *want* strict forward-compat (fail on unknown fields), set `forbid_unknown_fields=True` on the struct. ([jcristharif.com][1])

---

## Example 2: manifest schema as msgspec.Struct (replace `asdict()` + “JSON-ish”)

In `src/obs/manifest.py`, you currently define `DatasetRecord`, `RuleRecord`, `OutputRecord`, `ExtractRecord`, `Manifest`, then call `asdict()` in `Manifest.to_dict()`.

A representative msgspec rewrite looks like:

```python
# src/obs/manifest_models_msgspec.py
from __future__ import annotations

from typing import Literal
import msgspec

from core_types import JsonDict  # you already have a JSON-safe recursive alias

ManifestMode = Literal["relspec", "normalize", "compile", "cpg"]  # example
DatasetKind = Literal["relationship_input", "relationship_output", "cpg_output", "input", "intermediate"]

class DatasetRecord(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True):
    name: str
    kind: DatasetKind
    path: str | None = None
    format: str | None = None
    delta_version: int | None = None
    delta_features: dict[str, str] | None = None
    delta_commit_metadata: JsonDict | None = None
    delta_protocol: JsonDict | None = None
    delta_history: JsonDict | None = None
    delta_write_policy: JsonDict | None = None
    delta_schema_policy: JsonDict | None = None
    delta_constraints: list[str] | None = None

    rows: int | None = None
    columns: int | None = None
    schema_fingerprint: str | None = None
    ddl_fingerprint: str | None = None
    ordering_level: str | None = None
    ordering_keys: list[list[str]] | None = None
    schema: list[JsonDict] | None = None

class RuleRecord(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True):
    name: str
    output_dataset: str
    kind: str
    contract_name: str | None
    priority: int
    inputs: list[str] = msgspec.field(default_factory=list)
    evidence: JsonDict | None = None
    confidence_policy: JsonDict | None = None
    ambiguity_policy: JsonDict | None = None

class OutputRecord(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True):
    name: str
    rows: int | None = None
    schema_fingerprint: str | None = None
    ddl_fingerprint: str | None = None
    ordering_level: str | None = None
    ordering_keys: list[list[str]] | None = None
    plan_hash: str | None = None
    profile_hash: str | None = None
    writer_strategy: str | None = None
    input_fingerprints: list[str] | None = None
    dataset_fingerprint: str | None = None

class ExtractRecord(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True):
    name: str
    alias: str
    template: str | None = None
    evidence_family: str | None = None
    coordinate_system: str | None = None
    evidence_rank: int | None = None
    ambiguity_policy: str | None = None
    required_columns: list[str] = msgspec.field(default_factory=list)
    sources: list[str] = msgspec.field(default_factory=list)
    schema_fingerprint: str | None = None
    error_rows: int | None = None

class Manifest(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True):
    manifest_version: int
    created_at_unix_s: int

    repo_root: str | None
    relspec_mode: str
    work_dir: str | None
    output_dir: str | None

    datasets: list[DatasetRecord] = msgspec.field(default_factory=list)
    rules: list[RuleRecord] = msgspec.field(default_factory=list)
    outputs: list[OutputRecord] = msgspec.field(default_factory=list)
    extracts: list[ExtractRecord] = msgspec.field(default_factory=list)

    params: JsonDict = msgspec.field(default_factory=dict)
    repro: JsonDict = msgspec.field(default_factory=dict)
    notes: JsonDict = msgspec.field(default_factory=dict)
```

Then your writer stays structurally identical, but it stops depending on `asdict()`:

```python
# src/obs/manifest_write_msgspec.py
from __future__ import annotations
import pyarrow as pa
import msgspec

from core_types import ensure_path, PathLike
from ibis_engine.io_bridge import IbisDatasetWriteOptions, IbisDeltaWriteOptions, write_ibis_dataset_delta
from obs.manifest_models_msgspec import Manifest

def write_manifest_delta(manifest: Manifest, path: PathLike, *, overwrite: bool, execution) -> str:
    target = ensure_path(path)
    if target.exists() and not overwrite:
        raise FileExistsError(f"Manifest already exists at {target}.")

    payload = msgspec.to_builtins(manifest)  # -> only builtin JSON-ish types :contentReference[oaicite:9]{index=9}
    table = pa.Table.from_pylist([payload])

    options = IbisDeltaWriteOptions(mode="overwrite" if overwrite else "error",
                                   schema_mode="overwrite" if overwrite else None)
    result = write_ibis_dataset_delta(
        table,
        str(target),
        options=IbisDatasetWriteOptions(execution=execution, writer_strategy="datafusion", delta_options=options),
    )
    return result.path
```

Why this is a real upgrade for your manifest:

* You can turn on `forbid_unknown_fields=True` for **strict** schema evolution (or keep default “ignore unknown” for forwards-compat). ([jcristharif.com][1])
* `omit_defaults=True` shrinks artifacts and reduces noise (especially in your many `None` fields). ([jcristharif.com][1])
* You can add `tag=True` + unions later if you want versioned manifest variants. ([jcristharif.com][1])

---

## Example 3: typed repro bundle snapshot + drop `_normalize_value` for most cases

`collect_repro_info()` currently returns an untyped `JsonDict`. A msgspec version gives you:

* stable schema
* optional fields without hand-wiring
* deterministic JSON bytes for hashing (if you want)

```python
# src/obs/repro_models_msgspec.py
from __future__ import annotations

import msgspec
from core_types import JsonDict

class PythonInfo(msgspec.Struct, frozen=True, kw_only=True):
    version: str
    executable: str

class PlatformInfo(msgspec.Struct, frozen=True, kw_only=True):
    platform: str
    machine: str
    python_implementation: str

class PackageVersions(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True):
    pyarrow: str | None = None
    datafusion: str | None = None
    ibis_framework: str | None = None
    sqlglot: str | None = None
    sf_hamilton: str | None = None
    libcst: str | None = None

class GitInfo(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True):
    present: bool
    head: str | None = None
    ref: str | None = None
    commit: str | None = None
    error: str | None = None

class ReproInfo(msgspec.Struct, frozen=True, kw_only=True, omit_defaults=True):
    python: PythonInfo
    platform: PlatformInfo
    packages: PackageVersions
    git: GitInfo
    extra: JsonDict | None = None
```

And the capture function can return `ReproInfo` (your callers can still convert to builtins for Delta writes):

```python
# src/obs/repro_capture_msgspec.py
from __future__ import annotations

import platform
import sys
from importlib import metadata as importlib_metadata
from importlib.metadata import PackageNotFoundError

import msgspec

from core_types import JsonDict
from obs.repro_models_msgspec import (
    GitInfo, PackageVersions, PlatformInfo, PythonInfo, ReproInfo
)

def _pkg_version(name: str) -> str | None:
    try:
        return importlib_metadata.version(name)
    except PackageNotFoundError:
        return None

def collect_repro_info_struct(repo_root: str | None, *, extra: JsonDict | None = None) -> ReproInfo:
    # You can keep try_get_git_info() as-is, then `msgspec.convert` it into GitInfo
    git_dict = {"present": False} if not repo_root else {"present": True}  # placeholder
    git = msgspec.convert(git_dict, type=GitInfo, strict=False)

    return ReproInfo(
        python=PythonInfo(version=sys.version, executable=sys.executable),
        platform=PlatformInfo(
            platform=platform.platform(),
            machine=platform.machine(),
            python_implementation=platform.python_implementation(),
        ),
        packages=PackageVersions(
            pyarrow=_pkg_version("pyarrow"),
            datafusion=_pkg_version("datafusion"),
            ibis_framework=_pkg_version("ibis-framework"),
            sqlglot=_pkg_version("sqlglot"),
            sf_hamilton=_pkg_version("sf-hamilton") or _pkg_version("hamilton"),
            libcst=_pkg_version("libcst"),
        ),
        git=git,
        extra=extra,
    )
```

From here you have two “artifact lanes”:

* **internal-fast + stable hash**: `msgspec.json.encode(repro, order="deterministic")` ([jcristharif.com][1])
* **interop**: `orjson.dumps(msgspec.to_builtins(repro), OPT_SORT_KEYS)` ([jcristharif.com][2])

And you can stop doing `_normalize_value()` on most of these payloads, because `msgspec` already supports many stdlib types (UUID/datetime/etc.) and will raise a crisp `ValidationError` when something doesn’t fit the schema. ([jcristharif.com][4])

---

## How I’d instruct an AI agent to implement this safely

1. **Create one codec module** (`obs/serde_msgspec.py`) and make it the only place that chooses:

   * msgspec JSON vs orjson JSON
   * strict vs tolerant decoding
   * canonical hashing rules (`order='deterministic'`) ([jcristharif.com][1])

2. **Start with “leaf artifacts”** (like `CdfCursor`) where today’s code has manual coercion. This gives quick wins and low blast radius.

3. **Migrate “top-level artifacts” next** (Manifest / ReproInfo). Keep your existing Delta writer functions; just swap `asdict()` for `msgspec.to_builtins()`.

4. **Versioning strategy (optional, but future-proof)**

   * Add `schema_version: Literal["v1"]` to every artifact struct
   * If you later introduce “v2”, use **tagged unions** (`tag=True` / `tag_field`) for clean decode across versions. ([jcristharif.com][1])

If you want, I can also sketch a “minimum-change” patch plan specifically for:

* `src/incremental/cdf_cursors.py` (direct rewrite)
* `src/obs/manifest.py` (swap dataclasses → msgspec.Struct for the serialized record types only; keep builder helpers)
* `src/obs/repro.py` (introduce `ReproInfo` struct while keeping the old dict-returning function as a thin wrapper)

[1]: https://jcristharif.com/msgspec/api.html "API Docs"
[2]: https://jcristharif.com/msgspec/converters.html "Converters"
[3]: https://jcristharif.com/msgspec/constraints.html?utm_source=chatgpt.com "Constraints - msgspec"
[4]: https://jcristharif.com/msgspec/supported-types.html "Supported Types"
