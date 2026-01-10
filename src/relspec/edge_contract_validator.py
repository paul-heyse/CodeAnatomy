from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union


# Default mapping: relationship output dataset -> edge kinds it can emit downstream.
# You can override this in config or add more datasets as you expand the registry.
DEFAULT_REL_OUTPUT_TO_EDGE_KINDS: Dict[str, Tuple[str, ...]] = {
    # name_ref_id + symbol + roles => defines/references/reads/writes
    "rel_name_symbol": (
        "PY_DEFINES_SYMBOL",
        "PY_REFERENCES_SYMBOL",
        "PY_READS_SYMBOL",
        "PY_WRITES_SYMBOL",
    ),
    # import_alias_id + symbol => import edges
    "rel_import_symbol": (
        "PY_IMPORTS_SYMBOL",
    ),
    # call_id + symbol => call edges (preferred)
    "rel_callsite_symbol": (
        "PY_CALLS_SYMBOL",
    ),
    # call_id + qname_id => call edges (fallback)
    "rel_callsite_qname": (
        "PY_CALLS_QNAME",
    ),
}


@dataclass(frozen=True)
class EdgeContractValidationConfig:
    """
    Configuration for validating relationship output contracts against CPG EdgeKind contracts.
    """
    dataset_to_edge_kinds: Mapping[str, Sequence[str]] = DEFAULT_REL_OUTPUT_TO_EDGE_KINDS

    # If True, we error when a dataset is mapped to edge kinds but has no contract_name.
    require_contract_name: bool = True

    # If True, we error if multiple rules produce same output_dataset with different contract_name.
    require_single_contract_per_output_dataset: bool = True

    # If True, we error when an edge kind is unknown in the Ultimate registry.
    error_on_unknown_edge_kind: bool = True


def _load_edge_kind_required_props() -> Dict[str, Set[str]]:
    """
    Loads EDGE_KIND_CONTRACTS from codeintel_cpg.cpg.kinds_ultimate.

    Returns:
      { "EDGE_KIND_NAME": {"required_prop_a", ...}, ... }
    """
    from codeintel_cpg.cpg.kinds_ultimate import EDGE_KIND_CONTRACTS  # lazy import

    out: Dict[str, Set[str]] = {}
    for edge_kind_enum, contract in EDGE_KIND_CONTRACTS.items():
        # edge_kind_enum is an Enum value; contract.required_props is dict prop->PropSpec
        out[str(edge_kind_enum.value)] = set(contract.required_props.keys())
    return out


def _stringify_edge_kinds(edge_kinds: Sequence[Any]) -> Tuple[str, ...]:
    out: List[str] = []
    for ek in edge_kinds:
        if ek is None:
            continue
        if hasattr(ek, "value"):  # Enum
            out.append(str(ek.value))
        else:
            out.append(str(ek))
    return tuple(out)


def _contract_available_fields(contract: Any) -> Set[str]:
    """
    Returns available fields for validation:
      - schema column names
      - contract.virtual_fields (if present)
      - contract.available_fields() (if present)
    """
    # Prefer explicit helper if present
    if hasattr(contract, "available_fields") and callable(contract.available_fields):
        try:
            return set(contract.available_fields())
        except Exception:
            pass

    fields: Set[str] = set()

    schema = getattr(contract, "schema", None)
    if schema is not None and hasattr(schema, "names"):
        try:
            fields |= set(schema.names)
        except Exception:
            pass

    virtual = getattr(contract, "virtual_fields", None)
    if virtual:
        try:
            fields |= set(virtual)
        except Exception:
            pass

    return fields


def validate_relationship_output_contracts_for_edge_kinds(
    *,
    rules: Sequence[Any],
    contract_catalog: Any,
    config: Optional[EdgeContractValidationConfig] = None,
) -> None:
    """
    Validates that whenever a relationship output dataset feeds a CPG edge kind X, the dataset's
    output contract provides all required_props for that edge kind.

    "Provides" means:
      - the prop exists as a column in contract.schema, OR
      - the prop is declared in contract.virtual_fields (downstream injected).

    Raises ValueError with a readable multi-error report.
    """
    cfg = config or EdgeContractValidationConfig()
    required_props_by_edge_kind = _load_edge_kind_required_props()

    # Group rules by output_dataset
    rules_by_out: Dict[str, List[Any]] = {}
    for r in rules:
        out_ds = getattr(r, "output_dataset", None)
        if out_ds:
            rules_by_out.setdefault(str(out_ds), []).append(r)

    errors: List[str] = []

    # Validate each output dataset that maps to some edge kinds
    for out_ds, edge_kinds in cfg.dataset_to_edge_kinds.items():
        edge_kinds_s = _stringify_edge_kinds(edge_kinds)

        # If no rules produce this dataset, skip (you might not enable all rules in all runs)
        out_rules = rules_by_out.get(out_ds, [])
        if not out_rules:
            continue

        # Determine contract_name(s) used by the rules producing this output dataset
        contract_names = sorted({str(getattr(r, "contract_name", "") or "") for r in out_rules})
        contract_names = [c for c in contract_names if c]  # drop empty

        if cfg.require_contract_name and not contract_names:
            errors.append(
                f"[relspec] Output dataset '{out_ds}' is mapped to edge kinds {list(edge_kinds_s)} "
                f"but the producing rules have no contract_name set."
            )
            continue

        if cfg.require_single_contract_per_output_dataset and len(set(contract_names)) > 1:
            errors.append(
                f"[relspec] Output dataset '{out_ds}' has inconsistent contract_name across rules: {contract_names}. "
                f"All rules producing the same output dataset must share a single contract."
            )
            continue

        if not contract_names:
            continue  # allowed if require_contract_name=False

        contract_name = contract_names[0]

        # Get contract from catalog
        if not hasattr(contract_catalog, "get"):
            errors.append(f"[relspec] Contract catalog has no .get(); cannot validate '{contract_name}'.")
            continue

        contract = contract_catalog.get(contract_name)
        if contract is None:
            errors.append(
                f"[relspec] Missing contract '{contract_name}' in contract catalog (needed for '{out_ds}')."
            )
            continue

        available_fields = _contract_available_fields(contract)

        # Now validate required props for each edge kind
        for ek in edge_kinds_s:
            if ek not in required_props_by_edge_kind:
                if cfg.error_on_unknown_edge_kind:
                    errors.append(
                        f"[relspec] Unknown edge kind '{ek}' referenced by dataset_to_edge_kinds for '{out_ds}'. "
                        f"Not found in Ultimate EDGE_KIND_CONTRACTS."
                    )
                continue

            req = required_props_by_edge_kind[ek]
            missing = sorted([p for p in req if p not in available_fields])

            if missing:
                errors.append(
                    f"[relspec] Contract '{contract_name}' for output dataset '{out_ds}' does not provide "
                    f"required edge props for edge kind '{ek}': missing={missing}. "
                    f"Available fields={sorted(list(available_fields))}. "
                    f"Fix by either (a) adding columns to the relationship output schema, or "
                    f"(b) declaring these as Contract.virtual_fields if injected downstream."
                )

    if errors:
        raise ValueError("Edge contract validation failed:\n" + "\n".join(f" - {e}" for e in errors))
