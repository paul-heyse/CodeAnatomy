"""Thin Python facade for the Rust codeanatomy_engine."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, NoReturn

import msgspec

if TYPE_CHECKING:
    from engine.spec_builder import SemanticExecutionSpec


class EngineError(RuntimeError):
    """Base error for Rust engine execution failures."""

    exit_code: int = 1


class EngineValidationError(EngineError):
    """Spec validation or contract mismatch failure."""

    exit_code: int = 2


class EngineCompileError(EngineError):
    """Logical plan compilation failure."""

    exit_code: int = 3


class EngineRuleViolationError(EngineError):
    """Rulepack validation or policy rule violation."""

    exit_code: int = 4


class EngineRuntimeError(EngineError):
    """Runtime execution or materialization failure."""

    exit_code: int = 5


_ERROR_STAGE_MAP: dict[str, type[EngineError]] = {
    "validation": EngineValidationError,
    "compilation": EngineCompileError,
    "rule_violation": EngineRuleViolationError,
    "runtime": EngineRuntimeError,
    "materialization": EngineRuntimeError,
}


def _raise_engine_error(exc: Exception) -> NoReturn:
    """Classify and raise a typed EngineError from a Rust engine exception.

    Prioritizes structured error codes from Rust when available, falling back
    to pattern-based classification for backward compatibility.

    Args:
        exc: The caught exception from Rust engine boundary.

    Raises:
        EngineValidationError: Spec validation or contract mismatch failure.
        EngineCompileError: Logical plan compilation failure.
        EngineRuleViolationError: Rulepack validation or policy rule violation.
        EngineRuntimeError: Runtime execution or materialization failure.
        EngineError: Generic engine failure (fallback).
    """
    # Structured error code path (future Rust error codes)
    error_stage = getattr(exc, "error_stage", None)
    if isinstance(error_stage, str) and error_stage in _ERROR_STAGE_MAP:
        error_cls = _ERROR_STAGE_MAP[error_stage]
        raise error_cls(str(exc)) from exc

    # Pattern-based fallback (current behavior, preserved)
    message = str(exc)
    lowered = message.lower()
    if "invalid spec json" in lowered or "failed to parse spec" in lowered:
        raise EngineValidationError(message) from exc
    if "failed to compile plans" in lowered:
        if "rule" in lowered or "policy" in lowered or "safety" in lowered:
            raise EngineRuleViolationError(message) from exc
        raise EngineCompileError(message) from exc
    if "failed to build session" in lowered or "failed to register inputs" in lowered:
        raise EngineRuntimeError(message) from exc
    if "materialization failed" in lowered:
        raise EngineRuntimeError(message) from exc
    if isinstance(exc, ValueError):
        raise EngineValidationError(message) from exc
    raise EngineError(message) from exc


def _spec_payload_with_input_overrides(
    *,
    semantic_spec: SemanticExecutionSpec,
    extraction_inputs: dict[str, str],
) -> dict[str, object]:
    payload_raw = msgspec.to_builtins(semantic_spec)
    if not isinstance(payload_raw, dict):
        msg = "SemanticExecutionSpec payload must serialize to a mapping"
        raise TypeError(msg)
    payload: dict[str, object] = dict(payload_raw)
    payload.pop("spec_hash", None)
    raw_relations = payload.get("input_relations", [])
    relations: list[dict[str, object]] = []
    if isinstance(raw_relations, (list, tuple)):
        relations.extend(dict(relation) for relation in raw_relations if isinstance(relation, dict))
    seen: set[str] = set()
    for relation in relations:
        logical_name = relation.get("logical_name")
        if isinstance(logical_name, str):
            seen.add(logical_name)
            if logical_name in extraction_inputs:
                relation["delta_location"] = extraction_inputs[logical_name]
    for logical_name, delta_location in extraction_inputs.items():
        if logical_name in seen:
            continue
        relations.append(
            {
                "logical_name": logical_name,
                "delta_location": delta_location,
                "requires_lineage": False,
                "version_pin": None,
            }
        )
    payload["input_relations"] = relations
    return payload


def execute_cpg_build(
    extraction_inputs: dict[str, str],
    semantic_spec: SemanticExecutionSpec,
    environment_class: str = "medium",
) -> dict[str, object]:
    """Submit spec + inputs to the Rust engine and return the result envelope.

    Args:
        extraction_inputs: Map of logical_name -> Delta location for extraction outputs.
        semantic_spec: The compiled semantic execution spec.
        environment_class: One of "small", "medium", "large".

    Returns:
        RunResult dict with outputs, metrics, and fingerprints.

    Raises:
        ImportError: If the codeanatomy_engine Rust extension is not built.
    """
    try:
        engine_module = importlib.import_module("codeanatomy_engine")
    except ImportError:
        msg = (
            "codeanatomy_engine Rust extension not built. "
            "Run: bash scripts/rebuild_rust_artifacts.sh"
        )
        raise ImportError(msg) from None
    session_factory_cls = engine_module.SessionFactory
    semantic_plan_compiler_cls = engine_module.SemanticPlanCompiler
    cpg_materializer_cls = engine_module.CpgMaterializer

    spec_payload = _spec_payload_with_input_overrides(
        semantic_spec=semantic_spec,
        extraction_inputs=extraction_inputs,
    )
    spec_json = msgspec.json.encode(spec_payload).decode()

    factory = session_factory_cls.from_class(environment_class)
    compiler = semantic_plan_compiler_cls()
    try:
        compiled = compiler.compile(spec_json)
    except Exception as exc:  # noqa: BLE001 - normalize engine boundary errors
        _raise_engine_error(exc)

    materializer = cpg_materializer_cls()
    try:
        result = materializer.execute(factory, compiled)
    except Exception as exc:  # noqa: BLE001 - normalize engine boundary errors
        _raise_engine_error(exc)

    return msgspec.json.decode(result.to_json(), type=dict[str, object])
