"""Canonical step decoding for CLI and run plan execution."""

from __future__ import annotations

from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_strict, decode_json_strict
from tools.cq.run.spec import RunStep
from tools.cq.run.step_payload_normalization import normalize_step_payload


def parse_run_step_json(raw: str) -> RunStep:
    """Parse a single run step from JSON string.

    Args:
        raw: JSON string representing a single run step.

    Returns:
        RunStep: Parsed run step union member.

    Raises:
        BoundaryDecodeError: If JSON is malformed or does not match RunStep schema.
    """
    try:
        raw_payload = decode_json_strict(raw, type_=dict[str, object])
        normalized = normalize_step_payload(raw_payload)
        return convert_strict(normalized, type_=RunStep)
    except BoundaryDecodeError as exc:
        msg = f"Invalid run step JSON: {exc}"
        raise BoundaryDecodeError(msg) from exc


def parse_run_steps_json(raw: str) -> list[RunStep]:
    """Parse a list of run steps from JSON array string.

    Args:
        raw: JSON string representing an array of run steps.

    Returns:
        list[RunStep]: Parsed run step list.

    Raises:
        BoundaryDecodeError: If JSON is malformed or does not match RunStep schema.
    """
    try:
        raw_payloads = decode_json_strict(raw, type_=list[dict[str, object]])
        return [
            convert_strict(normalize_step_payload(item), type_=RunStep) for item in raw_payloads
        ]
    except BoundaryDecodeError as exc:
        msg = f"Invalid run steps JSON array: {exc}"
        raise BoundaryDecodeError(msg) from exc


__all__ = [
    "parse_run_step_json",
    "parse_run_steps_json",
]
