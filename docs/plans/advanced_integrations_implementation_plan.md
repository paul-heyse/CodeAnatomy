## Advanced Integrations Implementation Plan

This plan captures the integration upgrades identified after reviewing
`docs/python_library_reference/advanced_integrations.md` against the current
codebase. Each scope item includes the target pattern, file touch list, and an
implementation checklist.

### Scope 1: Canonical SQLGlot AST identity + lineage-driven scan columns

**Objective:** Move query identity and dependency extraction to canonical
SQLGlot ASTs (serde dump) and compute required input columns via lineage.

**Pattern snippet**
```python
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping

from sqlglot import Expression
from sqlglot.lineage import lineage
from sqlglot.optimizer import scope
from sqlglot.serde import dump


def canonical_ast_fingerprint(expr: Expression) -> str:
    payload = dump(expr)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def required_columns(expr: Expression, *, output_columns: Iterable[str]) -> dict[str, set[str]]:
    scope_root = scope.build_scope(expr)
    deps: dict[str, set[str]] = {}
    for col in output_columns:
        tree = lineage(
            col,
            expr,
            scope=scope_root,
            trim_selects=True,
            copy=False,
        )
        deps[col] = {node.name for node in tree.find_all("Column")}
    return deps
```

**Target files**
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/lineage.py`
- `src/datafusion_engine/bridge.py`
- `src/obs/repro.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [ ] Add serde-based fingerprint helper and swap plan hash usage where appropriate.
- [ ] Add lineage extraction helper that returns required base columns.
- [ ] Emit canonical AST payloads into repro/manifest artifacts.
- [ ] Use required column sets to drive scan projections for dataset reads.
- [ ] Add tests that prove AST fingerprint stability across formatting changes.

---

### Scope 2: Execute SQLGlot ASTs directly in the Ibis raw SQL lane

**Objective:** Avoid string SQL when backend supports `raw_sql(Expression)`.

**Pattern snippet**
```python
from __future__ import annotations

from sqlglot import Expression


def execute_raw_sql(backend: object, expr: Expression) -> object:
    raw = getattr(backend, "raw_sql", None)
    if callable(raw):
        try:
            return raw(expr)
        except TypeError:
            pass
    sql = expr.sql(dialect="datafusion_ext", unsupported_level="raise")
    return raw(sql)
```

**Target files**
- `src/ibis_engine/runner.py`
- `src/sqlglot_tools/optimizer.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Add `raw_sql(Expression)` capability detection and fallback to strings.
- [ ] Ensure diagnostics capture the canonical AST regardless of execution lane.
- [ ] Add tests for AST execution on the DataFusion backend.

---

### Scope 3: Runtime profile fingerprint expansion (Arrow pools + SQLGlot policy + Ibis options)

**Objective:** Make profile hash reflect Arrow thread pools, SQLGlot policy, and
Ibis compilation knobs (e.g., `fuse_selects`).

**Pattern snippet**
```python
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class SqlGlotPolicyFingerprint:
    read_dialect: str
    write_dialect: str
    rules: tuple[str, ...]
    generator_kwargs: dict[str, object]


def runtime_profile_fingerprint(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
```

**Target files**
- `src/engine/runtime_profile.py`
- `src/arrowdsl/core/context.py`
- `src/sqlglot_tools/optimizer.py`
- `src/ibis_engine/backend.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [ ] Introduce a unified runtime profile payload (DataFusion + Arrow + SQLGlot + Ibis).
- [ ] Replace `datafusion_settings_hash` usage in manifests with unified profile hash.
- [ ] Persist the unified profile payload in repro bundles.
- [ ] Add tests covering profile hash changes when any policy changes.

---

### Scope 4: Dataset discovery with inspection + URI normalization + `_metadata` fast path

**Objective:** Add a factory-based dataset discovery pipeline that supports
`FileSystem.from_uri`, `FileSystemDatasetFactory.inspect`, and metadata sidecars.

**Pattern snippet**
```python
from __future__ import annotations

import pyarrow.dataset as ds
from pyarrow import fs


def build_dataset_from_uri(uri: str, *, schema=None, strict: bool = True) -> ds.Dataset:
    filesystem, path = fs.FileSystem.from_uri(uri)
    selector = fs.FileSelector(path, recursive=True)
    opts = ds.FileSystemFactoryOptions(
        partition_base_dir=path,
        exclude_invalid_files=True,
        selector_ignore_prefixes=[".", "_"],
    )
    factory = ds.FileSystemDatasetFactory(filesystem, selector, ds.ParquetFileFormat(), opts)
    promote = "default" if strict else "permissive"
    inspected = factory.inspect(promote_options=promote)
    return factory.finish(schema=inspected)
```

**Target files**
- `src/arrowdsl/plan/source_normalize.py`
- `src/arrowdsl/plan/scan_builder.py`
- `src/arrowdsl/io/parquet.py`

**Implementation checklist**
- [ ] Add `from_uri` normalization and return `(filesystem, path)` consistently.
- [ ] Add factory-based discovery with strict/permissive schema inspection.
- [ ] Add `_metadata` fast path when metadata sidecars exist.
- [ ] Emit discovery policy and inspected schema as artifacts.

---

### Scope 5: Unified writer contract across Arrow and DataFusion lanes

**Objective:** Centralize dataset layout policy (threading, ordering, partitioning,
row group sizing, file visitor) and apply it consistently across writer strategies.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WriteSpec:
    basename_template: str
    use_threads: bool
    preserve_order: bool
    max_rows_per_file: int | None
    min_rows_per_group: int | None
    max_rows_per_group: int | None
    partition_cols: tuple[str, ...] = ()
    partitioning_flavor: str | None = "hive"


def apply_write_spec(reader, *, spec: WriteSpec, base_dir: str, file_visitor=None) -> None:
    ds.write_dataset(
        reader,
        base_dir=base_dir,
        format="parquet",
        basename_template=spec.basename_template,
        use_threads=spec.use_threads,
        preserve_order=spec.preserve_order,
        max_rows_per_file=spec.max_rows_per_file or 0,
        min_rows_per_group=spec.min_rows_per_group or 0,
        max_rows_per_group=spec.max_rows_per_group or 0,
        partitioning=spec.partition_cols or None,
        partitioning_flavor=spec.partitioning_flavor,
        file_visitor=file_visitor,
    )
```

**Target files**
- `src/arrowdsl/io/parquet.py`
- `src/ibis_engine/io_bridge.py`
- `src/engine/materialize.py`

**Implementation checklist**
- [ ] Add a `WriteSpec` type that fully captures `write_dataset` policy knobs.
- [ ] Ensure Arrow and DataFusion writer paths honor the same `WriteSpec`.
- [ ] Standardize file visitor + metadata sidecars for both lanes.
- [ ] Add tests validating ordering and partition layout behavior.

---

### Scope 6: Schema fingerprint fidelity (metadata + extension types + DDL hash)

**Objective:** Include schema/field metadata and extension type identity in the
storage fingerprint; add a separate DDL fingerprint for execution identity.

**Pattern snippet**
```python
from __future__ import annotations

import hashlib
import json


def schema_fingerprint_payload(schema) -> dict[str, object]:
    return {
        "fields": [
            {
                "name": f.name,
                "type": str(f.type),
                "nullable": bool(f.nullable),
                "metadata": dict(f.metadata or {}),
                "extension": getattr(f.type, "extension_name", None),
            }
            for f in schema
        ],
        "metadata": dict(schema.metadata or {}),
    }


def schema_fingerprint(schema) -> str:
    payload = schema_fingerprint_payload(schema)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
```

**Target files**
- `src/arrowdsl/schema/serialization.py`
- `src/schema_spec/system.py`
- `src/obs/manifest.py`
- `src/obs/repro.py`

**Implementation checklist**
- [ ] Extend schema serialization to include metadata + extension types.
- [ ] Add DDL fingerprint derived from SQLGlot column defs.
- [ ] Update manifest and repro outputs to store both fingerprints.
- [ ] Add tests ensuring metadata changes affect schema fingerprint.

---

### Scope 7: Unified nested data contract (ExplodeSpec + idx + keep_empty)

**Objective:** Implement a single nested-data contract with explicit offsets,
parent alignment, and ordering across Ibis, DataFusion, and Arrow kernels.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExplodeSpec:
    parent_keys: tuple[str, ...]
    list_col: str
    value_col: str
    idx_col: str | None = "idx"
    keep_empty: bool = True


def explode_with_idx(table, spec: ExplodeSpec):
    # Ibis: .unnest(list_col, offset=idx_col, keep_empty=keep_empty)
    # DataFusion: generate idx via range/cardinality when needed
    # Arrow: list_parent_indices + list_flatten + idx computation
    ...
```

**Target files**
- `src/cpg/relationship_plans.py`
- `src/datafusion_engine/kernels.py`
- `src/arrowdsl/compute/kernels.py`
- `src/arrowdsl/compute/expr_core.py`

**Implementation checklist**
- [ ] Define `ExplodeSpec` and a dispatch helper for Ibis/DF/Arrow lanes.
- [ ] Ensure idx is 0-based and ordering is explicit (`parent_key, idx`).
- [ ] Implement `keep_empty` behavior across all lanes.
- [ ] Add golden tests for empty/null lists and list<struct> unpack.

---

### Scope 8: Unified FunctionSpec registry (cross-lane identity + fingerprints)

**Objective:** Centralize function definitions (signature, volatility, naming,
lane availability) and emit a registry fingerprint into build identity.

**Pattern snippet**
```python
from __future__ import annotations

from dataclasses import dataclass
import pyarrow as pa


@dataclass(frozen=True)
class FunctionSpec:
    func_id: str
    engine_name: str
    kind: str
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    volatility: str
    arg_names: tuple[str, ...] | None = None
    lanes: tuple[str, ...] = ("ibis_builtin", "ibis_pyarrow", "df_udf", "df_rust")
```

**Target files**
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/udf_registry.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/engine/runtime_profile.py`
- `src/obs/manifest.py`

**Implementation checklist**
- [ ] Create a unified registry that describes all functions in one place.
- [ ] Add lane-specific installers driven from the registry.
- [ ] Add registry fingerprint to build identity and manifest.
- [ ] Add tests that registry changes affect build fingerprints.

---

### Deliverables Summary

- Canonical SQLGlot AST identity + lineage for required columns.
- Unified runtime profile fingerprint across Arrow, SQLGlot, Ibis, DataFusion.
- Dataset discovery with factory inspection + `_metadata` fast path.
- Single writer contract applied across Arrow and DataFusion strategies.
- Schema fingerprint fidelity (metadata + extension types) and DDL hash.
- Unified nested data contract with deterministic `idx` semantics.
- Cross-lane FunctionSpec registry and fingerprinting.
