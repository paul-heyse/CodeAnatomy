"""Validation helpers for centralized rule definitions."""

from __future__ import annotations

from collections.abc import Sequence

from extract.registry_bundles import bundle
from extract.registry_pipelines import post_kernels_for_postprocess
from relspec.rules.definitions import (
    ExtractPayload,
    NormalizePayload,
    RelationshipPayload,
    RuleDefinition,
)


def validate_rule_definitions(rules: Sequence[RuleDefinition]) -> None:
    """Validate centralized rule definitions.

    Raises
    ------
    ValueError
        Raised when definitions are invalid or inconsistent.
    """
    seen: set[str] = set()
    for rule in rules:
        if rule.name in seen:
            msg = f"Duplicate rule definition name: {rule.name!r}."
            raise ValueError(msg)
        seen.add(rule.name)
        _validate_payload(rule)
        _validate_stages(rule)


def _validate_payload(rule: RuleDefinition) -> None:
    payload = rule.payload
    if rule.domain == "cpg":
        if not isinstance(payload, RelationshipPayload):
            msg = f"RuleDefinition {rule.name!r} missing relationship payload."
            raise ValueError(msg)
        return
    if rule.domain == "normalize":
        if payload is not None and not isinstance(payload, NormalizePayload):
            msg = f"RuleDefinition {rule.name!r} has invalid normalize payload."
            raise ValueError(msg)
        return
    if rule.domain == "extract":
        if not isinstance(payload, ExtractPayload):
            msg = f"RuleDefinition {rule.name!r} missing extract payload."
            raise ValueError(msg)
        _validate_extract_payload(rule.name, payload)
        return


def _validate_extract_payload(name: str, payload: ExtractPayload) -> None:
    available = _extract_available_fields(payload)
    for key in payload.join_keys:
        if key not in available:
            msg = f"Extract rule {name!r} join_keys references missing field: {key!r}"
            raise ValueError(msg)
    for derived in payload.derived_ids:
        for key in derived.required:
            if key not in available:
                msg = (
                    f"Extract rule {name!r} derived id {derived.name!r} "
                    f"references missing field: {key!r}"
                )
                raise ValueError(msg)
    if payload.postprocess is not None:
        _validate_postprocess_kernel(name, payload.postprocess)


def _validate_postprocess_kernel(name: str, kernel_name: str) -> None:
    try:
        post_kernels_for_postprocess(kernel_name)
    except KeyError as exc:
        msg = f"Extract rule {name!r} has unknown postprocess kernel: {kernel_name!r}"
        raise ValueError(msg) from exc


def _extract_available_fields(payload: ExtractPayload) -> set[str]:
    available = set(payload.fields) | set(payload.row_fields) | set(payload.row_extras)
    if not payload.bundles:
        return available
    for bundle_name in payload.bundles:
        available.update(field.name for field in bundle(bundle_name).fields)
    return available


def _validate_stages(rule: RuleDefinition) -> None:
    names = [stage.name for stage in rule.stages]
    if len(set(names)) != len(names):
        msg = f"RuleDefinition {rule.name!r} contains duplicate stages."
        raise ValueError(msg)


__all__ = ["validate_rule_definitions"]
