# Derived Spec Generators With Delta Freshness Checks

## Goals
- Avoid re-writing Delta registry artifacts when canonical specs are unchanged.
- Keep all derived spec outputs tied to a deterministic signature of their sources.
- Provide a consistent mechanism across CPG, normalize, extract, and relspec outputs.

## Approach Summary
- Compute a stable signature from canonical spec sources (rows + schema + templates).
- Store the signature alongside Delta outputs (diagnostics table or commit metadata).
- Skip regeneration when signatures match; regenerate when signatures differ.
- Keep a manual override (`force`) for one-off rebuilds.

---

## Scope 1: Shared Signature + Freshness Utilities

### Target file list
- `src/storage/deltalake/registry_freshness.py` (new)
- `src/storage/deltalake/registry_models.py`

### Pattern to deploy
```python
@dataclass(frozen=True)
class RegistrySignature:
    registry: str
    signature: str
    source: str


def signature_from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    schema: pa.Schema,
) -> str:
    normalized = stable_rows(rows)
    payload = {
        "schema": schema.to_string(),
        "rows": normalized,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def should_regenerate(
    *,
    current: str | None,
    next_signature: str,
    force: bool,
) -> bool:
    return force or current is None or current != next_signature
```

### Implementation checklist
- [ ] Define `RegistrySignature` model + stable hashing helpers.
- [ ] Add a signature table schema (or extend diagnostics with a signature row).
- [ ] Provide a reader for existing signature from Delta outputs.
- [ ] Provide `should_regenerate()` guard for writers.

---

## Scope 2: Persist Signatures With Registry Outputs

### Target file list
- `src/storage/deltalake/registry_models.py`
- `src/storage/deltalake/delta.py`
- `src/storage/deltalake/registry_runner.py`

### Pattern to deploy
```python
SIGNATURE_SCHEMA = pa.schema(
    [
        pa.field("registry", pa.string(), nullable=False),
        pa.field("signature", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
    ],
    metadata={b"spec_kind": b"registry_signatures"},
)


def signature_table(signatures: Sequence[RegistrySignature]) -> pa.Table:
    rows = [sig.__dict__ for sig in signatures]
    return table_from_rows(SIGNATURE_SCHEMA, rows)
```

### Implementation checklist
- [ ] Add signature table builder to `RegistryBuildResult` or a sibling helper.
- [ ] Ensure signature table is written next to diagnostics.
- [ ] Teach `registry_runner` how to read the prior signature for a target.
- [ ] Add a `force` flag to `RegistryWriteOptions`.

---

## Scope 3: CPG Derived Spec Generators + Freshness

### Target file list
- `src/storage/deltalake/cpg_registry.py`
- `src/storage/deltalake/registry_runner.py`
- `src/cpg/registry_tables.py`
- `src/cpg/registry_specs.py`
- `src/cpg/spec_tables.py`

### Pattern to deploy
```python
@dataclass(frozen=True)
class CpgRegistrySignatureInput:
    kinds_contracts: Mapping[str, object]
    kinds_derivations: Sequence[object]
    registry_rows: Sequence[DatasetRow]
    registry_fields: Sequence[ArrowFieldSpec]


def cpg_signature_input() -> CpgRegistrySignatureInput:
    return CpgRegistrySignatureInput(
        kinds_contracts=NODE_KIND_CONTRACTS,
        kinds_derivations=DERIVATIONS,
        registry_rows=DATASET_ROWS,
        registry_fields=FIELD_CATALOG.entries(),
    )


def cpg_signature() -> str:
    payload = to_stable_dict(cpg_signature_input())
    return signature_from_payload("cpg", payload)
```

### Implementation checklist
- [ ] Add a CPG signature input builder from independent specs.
- [ ] Store the CPG signature with registry diagnostics.
- [ ] Guard `write_registry_delta()` with `should_regenerate()`.
- [ ] Ensure deterministic ordering for contracts/derivations/rows.

---

## Scope 4: Normalize Derived Spec Generators + Freshness

### Target file list
- `src/storage/deltalake/normalize_registry.py` (new)
- `src/storage/deltalake/registry_runner.py`
- `src/normalize/registry_specs.py`
- `src/normalize/spec_tables.py`
- `src/relspec/normalize/rule_registry_specs.py`

### Pattern to deploy
```python
@dataclass(frozen=True)
class NormalizeSignatureInput:
    dataset_rows: Sequence[DatasetRow]
    registry_templates: Sequence[RegistryTemplate]
    rule_families: Sequence[NormalizeRuleFamilySpec]


def normalize_signature() -> str:
    payload = {
        "rows": stable_rows(DATASET_ROWS),
        "templates": stable_templates(template_names()),
        "rule_families": stable_rule_families(rule_family_specs()),
    }
    return signature_from_payload("normalize", payload)
```

### Implementation checklist
- [ ] Introduce `normalize_registry.py` with spec-table builders.
- [ ] Export outputs under `output_dir/registry/normalize`.
- [ ] Store `normalize` signature in the signature table.
- [ ] Add normalize target to `registry_runner`.

---

## Scope 5: Extract Derived Spec Generators + Freshness

### Target file list
- `src/storage/deltalake/extract_registry.py` (new)
- `src/storage/deltalake/registry_runner.py`
- `src/extract/registry_definitions.py`
- `src/extract/spec_tables.py`
- `src/relspec/extract/registry_template_specs.py`
- `src/relspec/extract/registry_policies.py`

### Pattern to deploy
```python
@dataclass(frozen=True)
class ExtractSignatureInput:
    template_specs: Sequence[DatasetTemplateSpec]
    policy_rows: Sequence[DatasetPolicyRow]
    dataset_rows: Sequence[ExtractDatasetRowSpec]


def extract_signature() -> str:
    payload = {
        "templates": stable_templates(DATASET_TEMPLATE_SPECS),
        "policies": stable_policy_rows(all_policy_rows()),
        "rows": stable_rows(dataset_row_specs()),
    }
    return signature_from_payload("extract", payload)
```

### Implementation checklist
- [ ] Introduce `extract_registry.py` with spec-table builders.
- [ ] Export outputs under `output_dir/registry/extract`.
- [ ] Store `extract` signature in the signature table.
- [ ] Add extract target to `registry_runner`.

---

## Scope 6: Parity + Optional Deep Hash

### Target file list
- `tests/unit/test_registry_freshness.py` (new)
- `tests/unit/test_registry_parity.py` (new)
- `src/storage/deltalake/registry_freshness.py`

### Pattern to deploy
```python
def content_hash(table: pa.Table) -> str:
    ordered = table.sort_by(sorted(table.schema.names))
    payload = ordered.to_pydict()
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
```

### Implementation checklist
- [ ] Add tests that signature changes only when inputs change.
- [ ] Add parity tests for generator output vs stored Delta.
- [ ] Keep deep hash optional behind a flag (avoid cost in default runs).

---

## Scope 7: Integration and Rollout

### Target file list
- `src/storage/deltalake/registry_runner.py`
- `src/graph/product_build.py`

### Pattern to deploy
```python
result = run_registry_exports(
    output_dir,
    write_options=RegistryWriteOptions(force=force),
)
```

### Implementation checklist
- [ ] Extend `RegistryWriteOptions` with `force` and `skip_if_unchanged`.
- [ ] Ensure pipeline path respects freshness checks before builds.
- [ ] Provide a minimal API to read signatures for diagnostics.

---

## Notes
- All signatures must be deterministic (stable ordering + canonical serialization).
- Keep signature inputs limited to independent specs (no runtime-only data).
- Use the existing diagnostics table or add a sibling `registry_signatures` table.
- Continue using `encode_union_table()` before Delta writes.
