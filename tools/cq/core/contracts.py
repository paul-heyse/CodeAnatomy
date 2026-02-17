"""Shared helpers for CQ msgspec contract boundary serialization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import msgspec

from tools.cq.core.contract_codec import (
    require_mapping as require_contract_mapping,
)
from tools.cq.core.contract_codec import (
    to_contract_builtins,
    to_public_dict,
)
from tools.cq.core.structs import CqOutputStruct, CqStruct
from tools.cq.core.toolchain import Toolchain

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult, Finding, RunMeta
    from tools.cq.search._shared.search_contracts import SearchSummaryContract

LanguageToken = Literal["python", "rust"]
LanguageScopeToken = Literal["auto", "python", "rust"]


class ContractEnvelope(CqOutputStruct, frozen=True):
    """Generic typed envelope for contract payload transport."""

    payload: dict[str, object]


def contract_to_builtins(value: object) -> object:
    """Serialize a CQ contract object into builtins recursively.

    Returns:
        object: Builtins-only representation safe for JSON rendering.
    """
    if isinstance(value, msgspec.Struct):
        return to_public_dict(value)
    if (
        isinstance(value, list)
        and value
        and all(isinstance(item, msgspec.Struct) for item in value)
    ):
        return [to_public_dict(cast("msgspec.Struct", item)) for item in value]
    return to_contract_builtins(value)


def summary_contract_to_mapping(
    contract: object,
    *,
    common: Mapping[str, object] | None,
) -> dict[str, object]:
    """Serialize canonical search summary contract to mapping payload.

    Returns:
        dict[str, object]: Summary mapping with deterministic, renderer-ready fields.
    """
    from tools.cq.search._shared.search_contracts import summary_contract_to_dict

    return summary_contract_to_dict(
        cast("SearchSummaryContract", contract),
        common=common,
    )


def require_mapping(value: object) -> dict[str, object]:
    """Return mapping payload or raise a deterministic contract error.

    Returns:
        dict[str, object]: Builtins payload when the contract is mapping-shaped.

    Raises:
        TypeError: If the payload is not mapping-shaped after conversion.
    """
    try:
        return require_contract_mapping(value)
    except TypeError as exc:
        raise TypeError(str(exc)) from exc


class SummaryBuildRequest(CqStruct, frozen=True):
    """Input contract for canonical multilang summary assembly."""

    lang_scope: LanguageScopeToken
    languages: Mapping[LanguageToken, Mapping[str, object]]
    common: Mapping[str, object] | None = None
    language_order: tuple[LanguageToken, ...] | None = None
    cross_language_diagnostics: Sequence[Mapping[str, object]] | None = None
    language_capabilities: Mapping[str, object] | None = None
    enrichment_telemetry: Mapping[str, object] | None = None


class MergeResultsRequest(CqStruct, frozen=True):
    """Input contract for multi-language CQ result merge."""

    scope: LanguageScopeToken
    results: Mapping[LanguageToken, CqResult]
    run: RunMeta
    diagnostics: Sequence[Finding] | None = None
    diagnostic_payloads: Sequence[Mapping[str, object]] | None = None
    language_capabilities: Mapping[str, object] | None = None
    summary_common: Mapping[str, object] | None = None
    include_section_language_prefix: bool = True


class UuidIdentityContractV1(CqStruct, frozen=True):
    """Sortable UUID contract for CQ runtime identity fields."""

    run_id: str
    artifact_id: str
    cache_key_uses_uuid: bool = False
    run_uuid_version: int | None = None
    run_created_ms: int | None = None


class CallsMacroRequestV1(CqStruct, frozen=True):
    """Core-layer request contract for calls macro execution."""

    root: Path
    function_name: str
    tc: Toolchain
    argv: tuple[str, ...] = ()


__all__ = [
    "CallsMacroRequestV1",
    "ContractEnvelope",
    "MergeResultsRequest",
    "SummaryBuildRequest",
    "UuidIdentityContractV1",
    "contract_to_builtins",
    "require_mapping",
    "summary_contract_to_mapping",
]
