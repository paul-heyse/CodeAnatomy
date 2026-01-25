# Evidence Contract Integration Plan

## Goal
Make evidence scheduling and validation contract-aware by integrating `SchemaContract` into the evidence catalog, dataset spec modules, and runtime validation/diagnostics. This aligns evidence resolution with the DataFusion introspection cache and eliminates ad‑hoc schema inference paths.

---

## Scope 1: EvidenceCatalog contract awareness

**Intent**
Promote `SchemaContract` to a first-class source of truth in `relspec.evidence.EvidenceCatalog`, enabling contract-driven column/type/metadata population and direct validation against the DataFusion information_schema snapshot.

**Representative pattern**
```python
# relspec/evidence.py
from datafusion_engine.schema_contracts import SchemaContract, SchemaViolation

@dataclass
class EvidenceCatalog:
    sources: set[str] = field(default_factory=set)
    columns_by_dataset: dict[str, set[str]] = field(default_factory=dict)
    types_by_dataset: dict[str, dict[str, str]] = field(default_factory=dict)
    metadata_by_dataset: dict[str, dict[bytes, bytes]] = field(default_factory=dict)
    contracts_by_dataset: dict[str, SchemaContract] = field(default_factory=dict)
    contract_violations_by_dataset: dict[str, tuple[SchemaViolation, ...]] = field(
        default_factory=dict
    )

    def register_contract(
        self,
        name: str,
        contract: SchemaContract,
        *,
        snapshot: IntrospectionSnapshot | None = None,
    ) -> None:
        self.sources.add(name)
        self.contracts_by_dataset[name] = contract
        self.columns_by_dataset[name] = {col.name for col in contract.columns}
        self.types_by_dataset[name] = {
            col.name: contract._arrow_type_to_sql(col.arrow_type) for col in contract.columns
        }
        if snapshot is not None:
            violations = contract.validate_against_introspection(snapshot)
            self.contract_violations_by_dataset[name] = tuple(violations)
```

**Target files**
- `src/relspec/evidence.py`
- `src/relspec/graph_edge_validation.py`

**Implementation checklist**
- [ ] Add `contracts_by_dataset` and `contract_violations_by_dataset` to `EvidenceCatalog`.
- [ ] Implement `register_contract` to populate columns/types from contract definitions.
- [ ] Add helpers for contract-derived registration from dataset specs/contracts (see Scope 2).
- [ ] Expose contract violations in evidence validation results.

---

## Scope 2: DatasetSpec → SchemaContract conversion helpers

**Intent**
Create a single canonical conversion path from `DatasetSpec` / `ContractSpec` / `TableSchemaContract` to `SchemaContract` to avoid scattered, inconsistent logic.

**Representative pattern**
```python
# datafusion_engine/schema_contracts.py
from schema_spec.system import DatasetSpec

def schema_contract_from_dataset_spec(name: str, spec: DatasetSpec) -> SchemaContract:
    table_schema = spec.table_spec.to_arrow_schema()
    partitions = ()
    if spec.datafusion_scan is not None:
        partitions = tuple(spec.datafusion_scan.partition_cols)
    table_contract = TableSchemaContract(file_schema=table_schema, partition_cols=partitions)
    return schema_contract_from_table_schema_contract(
        table_name=name,
        contract=table_contract,
    )
```

**Target files**
- `src/datafusion_engine/schema_contracts.py`
- `src/schema_spec/system.py` (optional shim if this is preferred as canonical)

**Implementation checklist**
- [ ] Add `schema_contract_from_dataset_spec` helper.
- [ ] Add `schema_contract_from_contract_spec` helper (via `ContractSpec.table_schema`).
- [ ] Ensure both helpers account for partition columns (nullable=False in contract).
- [ ] Export helpers in `__all__`.

---

## Scope 3: EvidenceCatalog integration with dataset specs

**Intent**
Enable EvidenceCatalog to register contract data directly from dataset spec modules (normalize, relspec, future datasets).

**Representative pattern**
```python
# relspec/evidence.py
from datafusion_engine.schema_contracts import schema_contract_from_dataset_spec
from schema_spec.system import DatasetSpec

    def register_from_dataset_spec(
        self,
        name: str,
        spec: DatasetSpec,
        *,
        snapshot: IntrospectionSnapshot | None = None,
    ) -> None:
        contract = schema_contract_from_dataset_spec(name, spec)
        self.register_contract(name, contract, snapshot=snapshot)
```

**Target files**
- `src/relspec/evidence.py`
- `src/normalize/registry_runtime.py` (for normalize datasets)
- `src/relspec/contracts.py` (for relation output datasets)

**Implementation checklist**
- [ ] Add `register_from_dataset_spec` to `EvidenceCatalog`.
- [ ] Add `register_from_contract_spec` to `EvidenceCatalog`.
- [ ] Update `initial_evidence_from_plan` to use dataset specs when available.
- [ ] Ensure runtime integration uses cached introspection snapshots when provided.

---

## Scope 4: Schema contract exports for dataset modules

**Intent**
Expose `SchemaContract` directly from dataset definition modules, enabling evidence and validation systems to remain spec-driven.

**Representative pattern**
```python
# normalize/contracts.py
from datafusion_engine.schema_contracts import schema_contract_from_dataset_spec

@cache
def normalize_evidence_schema_contract() -> SchemaContract:
    spec = normalize_evidence_spec()
    return schema_contract_from_dataset_spec(spec.name, spec)
```

**Target files**
- `src/normalize/contracts.py`
- `src/relspec/contracts.py`
- `src/normalize/registry_runtime.py` (expose runtime contract helpers)

**Implementation checklist**
- [ ] Add `*_schema_contract()` functions in normalize and relspec modules.
- [ ] Add `dataset_schema_contract()` in `normalize/registry_runtime.py`.
- [ ] Add `dataset_contract_violations()` helper in normalize runtime to validate against info_schema.
- [ ] Update module `__all__` exports.

---

## Scope 5: Contract-aware edge validation & diagnostics

**Intent**
Ensure evidence graph validation fails fast when contract violations exist and surface diagnostics for contract drift.

**Representative pattern**
```python
# relspec/graph_edge_validation.py
violations = catalog.contract_violations_by_dataset.get(edge_data.name)
if violations:
    return EdgeValidationResult(
        source_name=edge_data.name,
        target_task=task_name,
        is_valid=False,
        contract_violations=tuple(str(v) for v in violations),
        ...,
    )
```

**Target files**
- `src/relspec/graph_edge_validation.py`
- `src/hamilton_pipeline/modules/task_execution.py`

**Implementation checklist**
- [ ] Extend `EdgeValidationResult` and `TaskValidationResult` to include contract violations.
- [ ] Update `validate_edge_requirements*` to fail on contract violations.
- [ ] Record diagnostics artifact when violations are present.
- [ ] Update any downstream consumers that rely on validation result shapes.

---

## Scope 6: Introspection snapshot injection

**Intent**
Standardize passing `IntrospectionSnapshot` into evidence/contract validation to avoid repeated information_schema queries.

**Representative pattern**
```python
# relspec/evidence.py
from datafusion_engine.introspection import introspection_cache_for_ctx

snapshot = None
if ctx is not None:
    snapshot = introspection_cache_for_ctx(ctx).snapshot

# Pass snapshot into contract-aware registration
```

**Target files**
- `src/relspec/evidence.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/modules/task_execution.py`

**Implementation checklist**
- [ ] Add optional `ctx: SessionContext | None` or `snapshot: IntrospectionSnapshot | None` plumbing.
- [ ] Ensure snapshot is shared across evidence validation and plan compilation.

---

## Decommission & Delete Candidates (after plan completion)

These are safe to decommission **only if** the contract-driven flow is fully adopted everywhere:

**Functions**
- `EvidenceCatalog.register_from_registry` (if all datasets have a `DatasetSpec` contract path). (`src/relspec/evidence.py`)
- `_schema_from_registry` (if contract/spec lookup replaces registry fallback). (`src/relspec/evidence.py`)
- `_schema_from_source` (optional; keep if supporting arbitrary schema-bearing objects remains required). (`src/relspec/evidence.py`)

**Files / Modules**
- None required to delete immediately. However, the following become soft-deprecated as contract coverage increases:
  - Any ad‑hoc schema inference utilities that bypass DatasetSpec/ContractSpec metadata.

---

## Rollout / Sequencing

1) Add SchemaContract conversion helpers (Scope 2).
2) Add EvidenceCatalog contract fields + registration helpers (Scope 1 + 3).
3) Wire dataset modules to expose SchemaContract (Scope 4).
4) Update evidence scheduling and validation to enforce contract violations (Scope 5).
5) Inject shared introspection snapshots into evidence flows (Scope 6).

---

## Acceptance Gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
- New contract validation tests for evidence/scheduling (golden failures for drift).
- Evidence scheduling uses the cached information_schema snapshot (no redundant queries).
