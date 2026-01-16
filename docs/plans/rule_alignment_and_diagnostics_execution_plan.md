# Rule Alignment and Diagnostics Execution Plan (Aligned to Current Repo)

Status: proposed

Decisions captured
- Strict lane enforcement warns only (no run failure).
- Parquet diagnostics live under `run_bundles/<run>/relspec/`.

## Scope 0: Baseline alignment with current architecture

Narrative
This plan builds on existing infrastructure rather than replacing it. Kernel lanes, ordering
enforcement, schema metadata, and run bundles already exist. The work below focuses on
explicit policy wiring, queryable diagnostics, and preventing Python scalarization where
a columnar path exists.

Code pattern
```python
# Use existing kernel lane diagnostics and schema metadata as the baseline.
# New work should extend these, not replace them.
```

Target files
- src/arrowdsl/kernel/registry.py
- src/arrowdsl/compute/kernels.py
- src/arrowdsl/schema/metadata.py
- src/obs/repro.py
- src/relspec/rules/diagnostics.py

Implementation checklist
- [ ] Confirm kernel lane diagnostics are emitted for all CPG rules.
- [ ] Confirm ordering metadata is present on relationship outputs.
- [ ] Keep rule diagnostics and Substrait artifacts in run bundles.
- [ ] Avoid adding new lane enums or schema metadata keys.

## Scope 1: Pipeline policy object and explicit policy registry wiring

Narrative
Introduce a single pipeline policy object that can be passed through the Hamilton graph.
It should drive lane enforcement (warn only), confidence and ambiguity policies, and
list filter gating. This removes implicit global registry usage and makes policies explicit
and reproducible.

Code pattern
```python
# src/relspec/pipeline_policy.py
from dataclasses import dataclass, field
from arrowdsl.kernel.registry import KernelLane
from relspec.rules.policies import PolicyRegistry

@dataclass(frozen=True)
class KernelLanePolicy:
    allowed: tuple[KernelLane, ...] = (
        KernelLane.DF_UDF,
        KernelLane.BUILTIN,
        KernelLane.ARROW_FALLBACK,
    )
    on_violation: str = "warn"

@dataclass(frozen=True)
class PipelinePolicy:
    policy_registry: PolicyRegistry = field(default_factory=PolicyRegistry)
    kernel_lanes: KernelLanePolicy = field(default_factory=KernelLanePolicy)
```

Target files
- src/relspec/pipeline_policy.py
- src/hamilton_pipeline/modules/inputs.py
- src/hamilton_pipeline/modules/cpg_build.py
- src/relspec/policies.py
- src/normalize/policies.py
- src/relspec/policy_registry.py

Implementation checklist
- [ ] Add `PipelinePolicy` and expose it as a Hamilton input node.
- [ ] Thread the policy registry into rule defaulting and validation.
- [ ] Deprecate implicit global registry usage with warnings.
- [ ] Align lane enforcement to warn only in diagnostics.

## Scope 2: Rule diagnostics parquet export in run bundles

Narrative
We already capture rule diagnostics as Arrow tables and write `.arrow` snapshots.
Add a parquet dataset export under `run_bundles/<run>/relspec/` so diagnostics are
queryable with DataFusion. This should use existing tables, not rebuild new ones.

Code pattern
```python
# src/obs/repro.py
from arrowdsl.io.parquet import DatasetWriteConfig, ParquetWriteOptions, write_dataset_parquet

if context.rule_diagnostics is not None:
    target = relspec_dir / "rule_diagnostics"
    config = DatasetWriteConfig(opts=ParquetWriteOptions(), overwrite=True)
    write_dataset_parquet(context.rule_diagnostics, target, config=config)
```

Target files
- src/obs/repro.py
- src/arrowdsl/io/parquet.py
- src/obs/parquet_writers.py
- src/relspec/rules/diagnostics.py

Implementation checklist
- [ ] Add a parquet writer helper in `obs` to standardize dataset outputs.
- [ ] Write `rule_diagnostics` parquet under `run_bundles/<run>/relspec/`.
- [ ] Keep existing `.arrow` outputs for backwards compatibility.
- [ ] Update run bundle documentation to mention parquet diagnostics.

## Scope 3: DataFusion fallback and explain diagnostics as parquet datasets

Narrative
Fallbacks and explains are currently JSON. Convert them to small parquet datasets to make
run bundles queryable. Reuse existing collectors in `datafusion_engine.runtime`.

Code pattern
```python
# src/obs/diagnostics_tables.py
import pyarrow as pa
from obs.diagnostics_schemas import DATAFUSION_FALLBACKS_V1

def datafusion_fallbacks_table(events: list[dict[str, object]]) -> pa.Table:
    rows = [{"reason": e.get("reason"), "sql": e.get("sql")} for e in events]
    return pa.Table.from_pylist(rows, schema=DATAFUSION_FALLBACKS_V1)
```

Target files
- src/obs/diagnostics_schemas.py
- src/obs/diagnostics_tables.py
- src/datafusion_engine/runtime.py
- src/hamilton_pipeline/modules/outputs.py
- src/obs/repro.py

Implementation checklist
- [ ] Add schema definitions for fallback and explain tables.
- [ ] Build tables from collector snapshots in outputs.
- [ ] Write parquet datasets under `run_bundles/<run>/relspec/`.
- [ ] Keep JSON snapshots for compatibility, but prefer parquet in tooling.

## Scope 4: Arrow-native parameter table signatures and parquet writes

Narrative
Parameter table signatures still use Python list materialization. Replace this with
Arrow-native hashing and ensure param table data uses dataset writers for consistent
metadata and layout.

Code pattern
```python
# src/ibis_engine/param_tables.py
import hashlib
import pyarrow as pa
import pyarrow.compute as pc

def param_signature_from_array(*, logical_name: str, values: pa.Array | pa.ChunkedArray) -> str:
    casted = pc.cast(values, pa.large_string(), safe=False)
    idx = pc.sort_indices(casted)
    sorted_vals = pc.take(casted, idx)
    hashed = pc.hash(sorted_vals)
    buf = hashed.buffers()[1]
    digest = hashlib.sha256(logical_name.encode("utf-8"))
    digest.update(buf.to_pybytes() if buf is not None else b"")
    return digest.hexdigest()
```

Target files
- src/ibis_engine/param_tables.py
- src/hamilton_pipeline/modules/params.py
- src/obs/repro.py
- src/arrowdsl/io/parquet.py

Implementation checklist
- [ ] Add `param_signature_from_array` and migrate callers.
- [ ] Deprecate Python list signatures with a warning.
- [ ] Write param tables using dataset writers in run bundles.
- [ ] Ensure signatures are included in `params/signatures.json`.

## Scope 5: Scan telemetry parquet export

Narrative
Scan telemetry already exists as a table. Add a parquet dataset export under
`run_bundles/<run>/relspec/` to enable queryable scans without parsing manifests.

Code pattern
```python
# src/obs/repro.py
if context.relspec_scan_telemetry is not None:
    target = relspec_dir / "scan_telemetry"
    config = DatasetWriteConfig(opts=ParquetWriteOptions(), overwrite=True)
    write_dataset_parquet(context.relspec_scan_telemetry, target, config=config)
```

Target files
- src/hamilton_pipeline/modules/outputs.py
- src/obs/repro.py
- src/arrowdsl/plan/metrics.py
- src/arrowdsl/plan/scan_telemetry.py

Implementation checklist
- [ ] Thread `relspec_scan_telemetry` into `RunBundleContext`.
- [ ] Export the telemetry table as parquet in run bundles.
- [ ] Keep manifest summary fields unchanged.
- [ ] Add a quick check in diagnostics to confirm dataset presence.

## Scope 6: Rule execution events table (per-rule metrics)

Narrative
Add a per-rule execution events table to capture rule-level runtime metrics and
schema fingerprints. This becomes the core dataset for "why did this edge exist"
and deterministic debugging. Implement as an optional observer to avoid impact
when disabled.

Code pattern
```python
# src/relspec/rules/exec_events.py
import pyarrow as pa
from arrowdsl.schema.serialization import schema_fingerprint

RELSPEC_RULE_EXEC_V1 = pa.schema([
    pa.field("rule_name", pa.string(), nullable=False),
    pa.field("output_dataset", pa.string(), nullable=False),
    pa.field("rows_out", pa.int64(), nullable=True),
    pa.field("schema_fingerprint", pa.string(), nullable=True),
    pa.field("duration_ms", pa.float64(), nullable=True),
])
```

Target files
- src/relspec/rules/exec_events.py
- src/relspec/compiler.py
- src/relspec/engine.py
- src/hamilton_pipeline/modules/outputs.py
- src/obs/repro.py

Implementation checklist
- [ ] Define the rule exec schema and row builder.
- [ ] Add an observer hook to rule execution paths.
- [ ] Store the table in `RunBundleContext`.
- [ ] Write parquet under `run_bundles/<run>/relspec/`.

## Scope 7: Semantic regression gates (plan diffs and determinism)

Narrative
Add fast gates that fail when plan signatures or determinism guarantees change.
Use existing cached plan signature utilities and compare outputs between runs
with different thread counts.

Code pattern
```python
# tests/integration/test_rule_semantics.py
from relspec.rules.cache import rule_plan_signatures_cached

def test_rule_plan_signatures_stable() -> None:
    signatures = rule_plan_signatures_cached()
    assert signatures
```

Target files
- tests/integration/test_rule_semantics.py
- src/relspec/rules/cache.py
- tests/e2e/test_full_pipeline_repo.py
- src/hamilton_pipeline/execution.py

Implementation checklist
- [ ] Add a plan-signature snapshot test.
- [ ] Add a determinism test that runs with different thread counts.
- [ ] Assert stable IDs and row counts for key outputs.
- [ ] Add documentation to the run bundle readme.

## Scope order (recommended)

1) Scope 1: pipeline policy object and explicit registry wiring
2) Scope 2: rule diagnostics parquet export
3) Scope 3: DataFusion fallback/explain parquet datasets
4) Scope 4: Arrow-native param signatures
5) Scope 5: scan telemetry parquet export
6) Scope 6: rule execution events table
7) Scope 7: semantic regression gates

