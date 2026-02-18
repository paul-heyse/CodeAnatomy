"""Typed enrichment payload contracts for smart-search matches."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from tools.cq.core.enrichment_mode import (
    DEFAULT_INCREMENTAL_ENRICHMENT_MODE,
    IncrementalEnrichmentModeV1,
    parse_incremental_enrichment_mode,
)
from tools.cq.search.enrichment.contracts import EnrichmentMeta
from tools.cq.search.enrichment.core import (
    coerce_enrichment_status,
    parse_incremental_enrichment,
    parse_python_enrichment,
    parse_rust_enrichment,
)
from tools.cq.search.enrichment.incremental_facts import IncrementalFacts
from tools.cq.search.enrichment.python_facts import PythonEnrichmentFacts
from tools.cq.search.enrichment.rust_facts import RustEnrichmentFacts


class RustTreeSitterEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Typed Rust enrichment payload wrapper."""

    schema_version: int = 1
    payload: RustEnrichmentFacts | None = None
    meta: EnrichmentMeta | None = None
    extras: dict[str, object] = msgspec.field(default_factory=dict)


class PythonEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Typed Python enrichment payload wrapper."""

    schema_version: int = 1
    payload: PythonEnrichmentFacts | None = None
    meta: EnrichmentMeta | None = None
    extras: dict[str, object] = msgspec.field(default_factory=dict)


class IncrementalEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    omit_defaults=True,
    forbid_unknown_fields=True,
):
    """Typed incremental enrichment payload wrapper."""

    schema_version: int = 1
    mode: IncrementalEnrichmentModeV1 = DEFAULT_INCREMENTAL_ENRICHMENT_MODE
    payload: IncrementalFacts | None = None
    extras: dict[str, object] = msgspec.field(default_factory=dict)


def _copy_payload(payload: Mapping[str, object]) -> dict[str, object]:
    return {key: value for key, value in payload.items() if isinstance(key, str)}


def _coerce_meta(payload: Mapping[str, object], *, language: str) -> EnrichmentMeta:
    raw_meta = payload.get("meta")
    meta_payload = raw_meta if isinstance(raw_meta, Mapping) else payload
    status = coerce_enrichment_status(meta_payload.get("enrichment_status"))
    sources_raw = meta_payload.get("enrichment_sources")
    sources = (
        [item for item in sources_raw if isinstance(item, str)]
        if isinstance(sources_raw, list)
        else []
    )
    degrade_reason = meta_payload.get("degrade_reason")
    payload_size_hint = meta_payload.get("payload_size_hint")
    dropped_fields = meta_payload.get("dropped_fields")
    truncated_fields = meta_payload.get("truncated_fields")
    return EnrichmentMeta(
        language=language,
        enrichment_status=status,
        enrichment_sources=sources,
        degrade_reason=degrade_reason if isinstance(degrade_reason, str) else None,
        payload_size_hint=payload_size_hint if isinstance(payload_size_hint, int) else None,
        dropped_fields=(
            [item for item in dropped_fields if isinstance(item, str)]
            if isinstance(dropped_fields, list)
            else None
        ),
        truncated_fields=(
            [item for item in truncated_fields if isinstance(item, str)]
            if isinstance(truncated_fields, list)
            else None
        ),
    )


def _typed_row(value: object) -> dict[str, object]:
    built = msgspec.to_builtins(value, order="deterministic", str_keys=True)
    return built if isinstance(built, dict) else {}


def _python_extras(payload: Mapping[str, object]) -> dict[str, object]:
    consumed = {
        "meta",
        "resolution",
        "behavior",
        "structure",
        "structural",
        "signature",
        "call",
        "import",
        "import_",
        "class_shape",
        "locals",
        "parse_quality",
    }
    return {key: value for key, value in payload.items() if key not in consumed}


def _rust_extras(payload: Mapping[str, object]) -> dict[str, object]:
    consumed = {
        "meta",
        "structure",
        "definition",
        "macro_expansion",
        "node_kind",
        "scope_kind",
        "scope_name",
        "scope_chain",
        "visibility",
        "attributes",
        "item_role",
        "macro_expansion_results",
        "language",
        "enrichment_status",
        "enrichment_sources",
        "degrade_reason",
        "payload_size_hint",
        "dropped_fields",
        "truncated_fields",
    }
    return {key: value for key, value in payload.items() if key not in consumed}


def _incremental_extras(payload: Mapping[str, object]) -> dict[str, object]:
    consumed = {
        "mode",
        "anchor",
        "semantic",
        "sym",
        "dis",
        "inspect",
        "compound",
        "details",
        "stage_status",
        "stage_errors",
        "timings_ms",
        "session_errors",
    }
    return {key: value for key, value in payload.items() if key not in consumed}


def wrap_rust_enrichment(
    payload: object,
) -> RustTreeSitterEnrichmentV1 | None:
    """Wrap Rust enrichment payload into a typed contract.

    Returns:
        Typed Rust enrichment wrapper, or `None` when payload is unsupported.
    """
    if isinstance(payload, RustTreeSitterEnrichmentV1):
        return payload
    if isinstance(payload, RustEnrichmentFacts):
        return RustTreeSitterEnrichmentV1(
            payload=payload,
            meta=EnrichmentMeta(language="rust"),
        )
    if not isinstance(payload, Mapping):
        return None
    payload_map = _copy_payload(payload)
    return RustTreeSitterEnrichmentV1(
        payload=parse_rust_enrichment(payload_map),
        meta=_coerce_meta(payload_map, language="rust"),
        extras=_rust_extras(payload_map),
    )


def wrap_python_enrichment(
    payload: object,
) -> PythonEnrichmentV1 | None:
    """Wrap Python enrichment payload into a typed contract.

    Returns:
        Typed Python enrichment wrapper, or `None` when payload is unsupported.
    """
    if isinstance(payload, PythonEnrichmentV1):
        return payload
    if isinstance(payload, PythonEnrichmentFacts):
        return PythonEnrichmentV1(
            payload=payload,
            meta=EnrichmentMeta(language="python"),
        )
    if not isinstance(payload, Mapping):
        return None
    payload_map = _copy_payload(payload)
    return PythonEnrichmentV1(
        payload=parse_python_enrichment(payload_map),
        meta=_coerce_meta(payload_map, language="python"),
        extras=_python_extras(payload_map),
    )


def wrap_incremental_enrichment(
    payload: object,
    *,
    mode: IncrementalEnrichmentModeV1 = DEFAULT_INCREMENTAL_ENRICHMENT_MODE,
) -> IncrementalEnrichmentV1 | None:
    """Wrap incremental enrichment payload into a typed contract.

    Returns:
        Typed incremental enrichment wrapper, or `None` when payload is unsupported.
    """
    if isinstance(payload, IncrementalEnrichmentV1):
        return payload
    if isinstance(payload, IncrementalFacts):
        return IncrementalEnrichmentV1(payload=payload, mode=mode)
    if not isinstance(payload, Mapping):
        return None
    payload_map = _copy_payload(payload)
    parsed_mode = parse_incremental_enrichment_mode(payload_map.get("mode"), default=mode)
    return IncrementalEnrichmentV1(
        mode=parsed_mode,
        payload=parse_incremental_enrichment(payload_map),
        extras=_incremental_extras(payload_map),
    )


def rust_enrichment_facts(payload: RustTreeSitterEnrichmentV1 | None) -> RustEnrichmentFacts | None:
    """Extract typed Rust enrichment facts.

    Returns:
        Rust enrichment facts payload when present.
    """
    if payload is None:
        return None
    return payload.payload


def python_enrichment_facts(payload: PythonEnrichmentV1 | None) -> PythonEnrichmentFacts | None:
    """Extract typed Python enrichment facts.

    Returns:
        Python enrichment facts payload when present.
    """
    if payload is None:
        return None
    return payload.payload


def incremental_enrichment_facts(
    payload: IncrementalEnrichmentV1 | None,
) -> IncrementalFacts | None:
    """Extract typed incremental enrichment facts.

    Returns:
        Incremental enrichment facts payload when present.
    """
    if payload is None:
        return None
    return payload.payload


def rust_enrichment_payload(payload: RustTreeSitterEnrichmentV1 | None) -> dict[str, object]:
    """Convert typed Rust wrapper into builtins mapping.

    Returns:
        Builtins mapping for the Rust enrichment payload.
    """
    if payload is None:
        return {}
    out = dict(payload.extras)
    if payload.meta is not None:
        out.update(_typed_row(payload.meta))
    facts = payload.payload
    if facts is None:
        return out
    if facts.structure is not None:
        out["structure"] = _typed_row(facts.structure)
    if facts.definition is not None:
        out["definition"] = _typed_row(facts.definition)
    if facts.macro_expansion is not None:
        out["macro_expansion"] = _typed_row(facts.macro_expansion)
        macro_rows = out["macro_expansion"].get("macro_expansion_results")
        if isinstance(macro_rows, list):
            out["macro_expansion_results"] = macro_rows
    return out


def python_enrichment_payload(payload: PythonEnrichmentV1 | None) -> dict[str, object]:
    """Convert typed Python wrapper into builtins mapping.

    Returns:
        Builtins mapping for the Python enrichment payload.
    """
    if payload is None:
        return {}
    out = dict(payload.extras)
    if payload.meta is not None:
        out["meta"] = _typed_row(payload.meta)
    facts = payload.payload
    if facts is None:
        return out
    if facts.resolution is not None:
        out["resolution"] = _typed_row(facts.resolution)
    if facts.behavior is not None:
        out["behavior"] = _typed_row(facts.behavior)
    if facts.structure is not None:
        out["structural"] = _typed_row(facts.structure)
    if facts.signature is not None:
        out["signature"] = _typed_row(facts.signature)
    if facts.call is not None:
        out["call"] = _typed_row(facts.call)
    if facts.import_ is not None:
        out["import"] = _typed_row(facts.import_)
    if facts.class_shape is not None:
        out["class_shape"] = _typed_row(facts.class_shape)
    if facts.locals is not None:
        out["locals"] = _typed_row(facts.locals)
    if facts.parse_quality is not None:
        out["parse_quality"] = _typed_row(facts.parse_quality)
    return out


def incremental_enrichment_payload(payload: IncrementalEnrichmentV1 | None) -> dict[str, object]:
    """Convert typed incremental wrapper into builtins mapping.

    Returns:
        Builtins mapping for the incremental enrichment payload.
    """
    if payload is None:
        return {}
    out = dict(payload.extras)
    out["schema_version"] = payload.schema_version
    out["mode"] = payload.mode.value
    facts = payload.payload
    if facts is None:
        return out
    if facts.anchor is not None:
        out["anchor"] = _typed_row(facts.anchor)
    if facts.semantic:
        out["semantic"] = dict(facts.semantic)
    if facts.sym:
        out["sym"] = dict(facts.sym)
    if facts.dis:
        out["dis"] = dict(facts.dis)
    if facts.inspect:
        out["inspect"] = dict(facts.inspect)
    if facts.compound:
        out["compound"] = dict(facts.compound)
    if facts.details:
        out["details"] = [dict(item) for item in facts.details]
    if facts.stage_status:
        out["stage_status"] = dict(facts.stage_status)
    if facts.stage_errors:
        out["stage_errors"] = dict(facts.stage_errors)
    if facts.timings_ms:
        out["timings_ms"] = dict(facts.timings_ms)
    if facts.session_errors:
        out["session_errors"] = dict(facts.session_errors)
    return out


__all__ = [
    "DEFAULT_INCREMENTAL_ENRICHMENT_MODE",
    "IncrementalEnrichmentModeV1",
    "IncrementalEnrichmentV1",
    "PythonEnrichmentV1",
    "RustTreeSitterEnrichmentV1",
    "incremental_enrichment_facts",
    "incremental_enrichment_payload",
    "parse_incremental_enrichment_mode",
    "python_enrichment_facts",
    "python_enrichment_payload",
    "rust_enrichment_facts",
    "rust_enrichment_payload",
    "wrap_incremental_enrichment",
    "wrap_python_enrichment",
    "wrap_rust_enrichment",
]
