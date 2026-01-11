"""Validate relationship output contracts against edge-kind requirements."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from arrowdsl.contracts import Contract
from cpg.kinds_ultimate import EDGE_KIND_CONTRACTS
from relspec.model import RelationshipRule
from relspec.registry import ContractCatalog

DEFAULT_REL_OUTPUT_TO_EDGE_KINDS: dict[str, tuple[str, ...]] = {
    "rel_name_symbol": (
        "PY_DEFINES_SYMBOL",
        "PY_REFERENCES_SYMBOL",
        "PY_READS_SYMBOL",
        "PY_WRITES_SYMBOL",
    ),
    "rel_import_symbol": ("PY_IMPORTS_SYMBOL",),
    "rel_callsite_symbol": ("PY_CALLS_SYMBOL",),
    "rel_callsite_qname": ("PY_CALLS_QNAME",),
}


@dataclass(frozen=True)
class EdgeContractValidationConfig:
    """Configuration for validating output contracts against edge kind requirements."""

    dataset_to_edge_kinds: Mapping[str, Sequence[str]] = DEFAULT_REL_OUTPUT_TO_EDGE_KINDS
    require_contract_name: bool = True
    require_single_contract_per_output_dataset: bool = True
    error_on_unknown_edge_kind: bool = True


@dataclass(frozen=True)
class EdgeContractValidationContext:
    """Context bundle for edge contract validation."""

    contract_catalog: ContractCatalog
    required_props_by_edge_kind: Mapping[str, set[str]]
    config: EdgeContractValidationConfig


def _load_edge_kind_required_props() -> dict[str, set[str]]:
    """Load required props from the Ultimate edge-kind contracts.

    Returns
    -------
    dict[str, set[str]]
        Mapping of edge kind names to required property names.
    """
    out: dict[str, set[str]] = {}
    for edge_kind_enum, contract in EDGE_KIND_CONTRACTS.items():
        out[str(edge_kind_enum.value)] = set(contract.required_props.keys())
    return out


def _stringify_edge_kinds(edge_kinds: Sequence[object]) -> tuple[str, ...]:
    out: list[str] = []
    for edge_kind in edge_kinds:
        if edge_kind is None:
            continue
        value = getattr(edge_kind, "value", None)
        out.append(str(value) if value is not None else str(edge_kind))
    return tuple(out)


def _contract_available_fields(contract: Contract) -> set[str]:
    """Return available fields for contract validation.

    Parameters
    ----------
    contract:
        Contract to inspect.

    Returns
    -------
    set[str]
        Available field names for the contract.
    """
    return set(contract.available_fields())


def _validate_edge_kinds(
    ctx: EdgeContractValidationContext,
    *,
    out_ds: str,
    contract_name: str,
    edge_kinds: Sequence[str],
    available_fields: set[str],
) -> list[str]:
    errors: list[str] = []
    for edge_kind in edge_kinds:
        if edge_kind not in ctx.required_props_by_edge_kind:
            if ctx.config.error_on_unknown_edge_kind:
                errors.append(
                    f"[relspec] Unknown edge kind '{edge_kind}' referenced for '{out_ds}'. "
                    "Not found in Ultimate EDGE_KIND_CONTRACTS."
                )
            continue

        required_props = ctx.required_props_by_edge_kind[edge_kind]
        missing = sorted(prop for prop in required_props if prop not in available_fields)

        if missing:
            errors.append(
                f"[relspec] Contract '{contract_name}' for output dataset '{out_ds}' does not provide "
                f"required edge props for edge kind '{edge_kind}': missing={missing}. "
                f"Available fields={sorted(available_fields)}. Fix by adding columns to the "
                "relationship output schema or declaring them as Contract.virtual_fields."
            )
    return errors


def _validate_output_dataset(
    ctx: EdgeContractValidationContext,
    *,
    out_ds: str,
    edge_kinds: Sequence[object],
    out_rules: Sequence[RelationshipRule],
) -> list[str]:
    if not out_rules:
        return []

    edge_kinds_s = _stringify_edge_kinds(edge_kinds)
    contract_names = sorted({rule.contract_name or "" for rule in out_rules})
    contract_names = [name for name in contract_names if name]

    if ctx.config.require_contract_name and not contract_names:
        return [
            (
                f"[relspec] Output dataset '{out_ds}' is mapped to edge kinds "
                f"{list(edge_kinds_s)} but the producing rules have no contract_name set."
            )
        ]

    if ctx.config.require_single_contract_per_output_dataset and len(set(contract_names)) > 1:
        return [
            (
                f"[relspec] Output dataset '{out_ds}' has inconsistent contract_name across rules: "
                f"{contract_names}. All rules producing the same output dataset must share a single "
                "contract."
            )
        ]

    if not contract_names:
        return []

    contract_name = contract_names[0]
    contract = ctx.contract_catalog.get(contract_name)
    available_fields = _contract_available_fields(contract)
    return _validate_edge_kinds(
        ctx,
        out_ds=out_ds,
        contract_name=contract_name,
        edge_kinds=edge_kinds_s,
        available_fields=available_fields,
    )


def validate_relationship_output_contracts_for_edge_kinds(
    *,
    rules: Sequence[RelationshipRule],
    contract_catalog: ContractCatalog,
    config: EdgeContractValidationConfig | None = None,
) -> None:
    """Validate output contracts against edge kind required properties.

    Raises
    ------
    ValueError
        Raised when validation fails with a multi-error report.
    """
    cfg = config or EdgeContractValidationConfig()
    ctx = EdgeContractValidationContext(
        contract_catalog=contract_catalog,
        required_props_by_edge_kind=_load_edge_kind_required_props(),
        config=cfg,
    )

    rules_by_out: dict[str, list[RelationshipRule]] = {}
    for rule in rules:
        rules_by_out.setdefault(rule.output_dataset, []).append(rule)

    errors: list[str] = []

    for out_ds, edge_kinds in cfg.dataset_to_edge_kinds.items():
        out_rules = rules_by_out.get(out_ds, [])
        errors.extend(
            _validate_output_dataset(
                ctx,
                out_ds=out_ds,
                edge_kinds=edge_kinds,
                out_rules=out_rules,
            )
        )

    if errors:
        msg = "Edge contract validation failed:\n" + "\n".join(f" - {err}" for err in errors)
        raise ValueError(msg)
