"""Typed contracts for CQ multi-language search summaries and diagnostics."""

from __future__ import annotations

# ruff: noqa: DOC201
from collections.abc import Mapping, Sequence
from typing import Literal, cast

import msgspec

from tools.cq.core.serialization import to_builtins
from tools.cq.query.language import QueryLanguage, QueryLanguageScope

Severity = Literal["info", "warning", "error"]


class LanguagePartitionStats(msgspec.Struct, omit_defaults=True):
    """Per-language summary partition statistics."""

    matches: int = 0
    files_scanned: int = 0
    scanned_files: int = 0
    scanned_files_is_estimate: bool = False
    matched_files: int = 0
    total_matches: int = 0
    truncated: bool = False
    timed_out: bool = False
    caps_hit: str = "none"
    error: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> LanguagePartitionStats:
        """Create a partition stats payload from a raw mapping."""
        if payload is None:
            return cls()
        payload_map: Mapping[str, object] = payload

        def _to_int(key: str, default: int = 0) -> int:
            value = payload_map.get(key)
            if isinstance(value, bool):
                return default
            if isinstance(value, int):
                return value
            return default

        def _to_bool(key: str, *, default: bool = False) -> bool:
            value = payload_map.get(key)
            if isinstance(value, bool):
                return value
            return default

        error = payload_map.get("error")
        return cls(
            matches=_to_int("matches"),
            files_scanned=_to_int("files_scanned"),
            scanned_files=_to_int("scanned_files", _to_int("files_scanned")),
            scanned_files_is_estimate=_to_bool("scanned_files_is_estimate"),
            matched_files=_to_int("matched_files"),
            total_matches=_to_int("total_matches", _to_int("matches")),
            truncated=_to_bool("truncated"),
            timed_out=_to_bool("timed_out"),
            caps_hit=str(payload_map.get("caps_hit", "none")),
            error=error if isinstance(error, str) and error else None,
        )


class CapabilitySupport(msgspec.Struct):
    """Support descriptor for a feature in one language."""

    supported: bool = False
    level: str = "none"


class LanguageCapabilities(msgspec.Struct, omit_defaults=True):
    """Capability matrix emitted in summary payloads."""

    python: dict[str, CapabilitySupport] = msgspec.field(default_factory=dict)
    rust: dict[str, CapabilitySupport] = msgspec.field(default_factory=dict)
    shared: dict[str, bool] = msgspec.field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> LanguageCapabilities:
        """Create typed capabilities from a loose mapping payload."""
        if payload is None:
            return cls()
        payload_map: Mapping[str, object] = payload

        def _coerce_lang_bucket(key: str) -> dict[str, CapabilitySupport]:
            raw = payload_map.get(key)
            if not isinstance(raw, Mapping):
                return {}
            bucket: dict[str, CapabilitySupport] = {}
            for feature, item in raw.items():
                if not isinstance(feature, str):
                    continue
                if isinstance(item, Mapping):
                    supported = bool(item.get("supported", False))
                    level = item.get("level", "none")
                    bucket[feature] = CapabilitySupport(
                        supported=supported,
                        level=str(level),
                    )
            return bucket

        shared_raw = payload_map.get("shared")
        shared: dict[str, bool] = {}
        if isinstance(shared_raw, Mapping):
            for feature, value in shared_raw.items():
                if isinstance(feature, str):
                    shared[feature] = bool(value)

        return cls(
            python=_coerce_lang_bucket("python"),
            rust=_coerce_lang_bucket("rust"),
            shared=shared,
        )


class CrossLanguageDiagnostic(msgspec.Struct, omit_defaults=True):
    """Structured cross-language diagnostic payload."""

    code: str = "ML000"
    severity: Severity = "info"
    message: str = ""
    intent: str = "unspecified"
    languages: list[str] = msgspec.field(default_factory=list)
    counts: dict[str, int] = msgspec.field(default_factory=dict)
    remediation: str = ""
    feature: str | None = None
    language: str | None = None
    capability_level: str | None = None

    @classmethod
    def from_mapping(  # noqa: C901
        cls, payload: Mapping[str, object] | None
    ) -> CrossLanguageDiagnostic:
        """Create a typed diagnostic from a loose mapping."""
        if payload is None:
            return cls()
        payload_map: Mapping[str, object] = payload

        def _to_languages(value: object) -> list[str]:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
                return []
            return [str(item) for item in value]

        def _to_counts(value: object) -> dict[str, int]:
            if not isinstance(value, Mapping):
                return {}
            counts: dict[str, int] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    continue
                if isinstance(item, bool):
                    continue
                if isinstance(item, int):
                    counts[key] = item
            return counts

        severity_value = str(payload_map.get("severity", "info"))
        if severity_value not in {"info", "warning", "error"}:
            severity_value = "info"

        feature = payload_map.get("feature")
        language = payload_map.get("language")
        capability_level = payload_map.get("capability_level")
        return cls(
            code=str(payload_map.get("code", "ML000")),
            severity=cast("Severity", severity_value),
            message=str(payload_map.get("message", "")),
            intent=str(payload_map.get("intent", "unspecified")),
            languages=_to_languages(payload_map.get("languages")),
            counts=_to_counts(payload_map.get("counts")),
            remediation=str(payload_map.get("remediation", "")),
            feature=feature if isinstance(feature, str) else None,
            language=language if isinstance(language, str) else None,
            capability_level=(capability_level if isinstance(capability_level, str) else None),
        )


class EnrichmentTelemetryBucket(msgspec.Struct, omit_defaults=True):
    """Telemetry counters for one language enrichment pipeline."""

    applied: int = 0
    degraded: int = 0
    skipped: int = 0
    cache_hits: int | None = None
    cache_misses: int | None = None
    cache_evictions: int | None = None


class EnrichmentTelemetry(msgspec.Struct, omit_defaults=True):
    """Search enrichment telemetry by language."""

    python: EnrichmentTelemetryBucket = msgspec.field(default_factory=EnrichmentTelemetryBucket)
    rust: EnrichmentTelemetryBucket = msgspec.field(default_factory=EnrichmentTelemetryBucket)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object] | None) -> EnrichmentTelemetry | None:
        """Create telemetry from a mapping payload.

        Returns ``None`` when payload is missing or malformed.
        """
        if payload is None:
            return None

        def _bucket(value: object) -> EnrichmentTelemetryBucket:
            if not isinstance(value, Mapping):
                return EnrichmentTelemetryBucket()
            value_map: Mapping[str, object] = value

            def _to_int(key: str) -> int | None:
                raw = value_map.get(key)
                if isinstance(raw, bool):
                    return None
                if isinstance(raw, int):
                    return raw
                return None

            return EnrichmentTelemetryBucket(
                applied=_to_int("applied") or 0,
                degraded=_to_int("degraded") or 0,
                skipped=_to_int("skipped") or 0,
                cache_hits=_to_int("cache_hits"),
                cache_misses=_to_int("cache_misses"),
                cache_evictions=_to_int("cache_evictions"),
            )

        return cls(
            python=_bucket(payload.get("python")),
            rust=_bucket(payload.get("rust")),
        )


class SearchSummaryContract(msgspec.Struct):
    """Canonical multi-language summary contract for CQ outputs."""

    lang_scope: QueryLanguageScope
    language_order: list[QueryLanguage]
    languages: dict[QueryLanguage, LanguagePartitionStats]
    cross_language_diagnostics: list[CrossLanguageDiagnostic] = msgspec.field(default_factory=list)
    language_capabilities: LanguageCapabilities = msgspec.field(
        default_factory=LanguageCapabilities
    )
    enrichment_telemetry: EnrichmentTelemetry | None = None


def coerce_language_partitions(
    *,
    language_order: Sequence[QueryLanguage],
    partitions: Mapping[QueryLanguage, Mapping[str, object]],
) -> dict[QueryLanguage, LanguagePartitionStats]:
    """Normalize language partition payloads using typed stats structs."""
    normalized: dict[QueryLanguage, LanguagePartitionStats] = {}
    for lang in language_order:
        normalized[lang] = LanguagePartitionStats.from_mapping(partitions.get(lang))
    for lang, payload in partitions.items():
        if lang not in normalized:
            normalized[lang] = LanguagePartitionStats.from_mapping(payload)
    return normalized


def coerce_diagnostics(
    diagnostics: Sequence[CrossLanguageDiagnostic | Mapping[str, object]] | None,
) -> list[CrossLanguageDiagnostic]:
    """Normalize diagnostics into typed contract rows."""
    if not diagnostics:
        return []
    rows: list[CrossLanguageDiagnostic] = []
    for item in diagnostics:
        if isinstance(item, CrossLanguageDiagnostic):
            rows.append(item)
        elif isinstance(item, Mapping):
            rows.append(CrossLanguageDiagnostic.from_mapping(item))
    return rows


def coerce_language_capabilities(
    value: LanguageCapabilities | Mapping[str, object] | None,
) -> LanguageCapabilities:
    """Normalize language capability payload into typed contract."""
    if isinstance(value, LanguageCapabilities):
        return value
    if isinstance(value, Mapping):
        return LanguageCapabilities.from_mapping(value)
    return LanguageCapabilities()


def summary_contract_to_dict(
    contract: SearchSummaryContract,
    *,
    common: Mapping[str, object] | None,
) -> dict[str, object]:
    """Serialize summary contract to a builtins mapping and merge common keys."""
    summary: dict[str, object] = dict(common) if common is not None else {}
    summary["lang_scope"] = contract.lang_scope
    summary["language_order"] = list(contract.language_order)
    summary["languages"] = {
        lang: _language_partition_to_dict(partition)
        for lang, partition in contract.languages.items()
    }
    summary["cross_language_diagnostics"] = diagnostics_to_dicts(
        contract.cross_language_diagnostics
    )
    summary["language_capabilities"] = _language_capabilities_to_dict(
        contract.language_capabilities
    )
    if contract.enrichment_telemetry is not None:
        summary["enrichment_telemetry"] = cast(
            "dict[str, object]", to_builtins(contract.enrichment_telemetry)
        )
    return summary


def _language_partition_to_dict(partition: LanguagePartitionStats) -> dict[str, object]:
    payload: dict[str, object] = {
        "matches": partition.matches,
        "files_scanned": partition.files_scanned,
        "scanned_files": partition.scanned_files,
        "scanned_files_is_estimate": partition.scanned_files_is_estimate,
        "matched_files": partition.matched_files,
        "total_matches": partition.total_matches,
        "truncated": partition.truncated,
        "timed_out": partition.timed_out,
        "caps_hit": partition.caps_hit,
    }
    if partition.error:
        payload["error"] = partition.error
    return payload


def _language_capabilities_to_dict(
    capabilities: LanguageCapabilities,
) -> dict[str, object]:
    def _bucket_to_dict(bucket: Mapping[str, CapabilitySupport]) -> dict[str, object]:
        return {
            feature: {
                "supported": support.supported,
                "level": support.level,
            }
            for feature, support in bucket.items()
        }

    return {
        "python": _bucket_to_dict(capabilities.python),
        "rust": _bucket_to_dict(capabilities.rust),
        "shared": dict(capabilities.shared),
    }


def diagnostic_to_dict(diagnostic: CrossLanguageDiagnostic) -> dict[str, object]:
    """Serialize a diagnostic row to a builtins mapping."""
    return cast("dict[str, object]", to_builtins(diagnostic))


def diagnostics_to_dicts(
    diagnostics: Sequence[CrossLanguageDiagnostic],
) -> list[dict[str, object]]:
    """Serialize typed diagnostics to list of builtins mappings."""
    return [diagnostic_to_dict(item) for item in diagnostics]


__all__ = [
    "CapabilitySupport",
    "CrossLanguageDiagnostic",
    "EnrichmentTelemetry",
    "EnrichmentTelemetryBucket",
    "LanguageCapabilities",
    "LanguagePartitionStats",
    "SearchSummaryContract",
    "coerce_diagnostics",
    "coerce_language_capabilities",
    "coerce_language_partitions",
    "diagnostic_to_dict",
    "diagnostics_to_dicts",
    "summary_contract_to_dict",
]
