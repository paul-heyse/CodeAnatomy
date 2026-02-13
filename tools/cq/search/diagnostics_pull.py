"""Shared diagnostics pull helpers for LSP clients."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import cast

from tools.cq.search.lsp.capabilities import coerce_capabilities, supports_method

_FAIL_OPEN_EXCEPTIONS = (OSError, RuntimeError, TimeoutError, ValueError, TypeError)


def pull_text_document_diagnostics(
    session: object,
    uri: str,
) -> tuple[dict[str, object], ...] | None:
    """Pull diagnostics via ``textDocument/diagnostic`` when available.

    Returns:
        Normalized diagnostic rows, or `None` when unsupported/unavailable.
    """
    if not _supports_diagnostic_method(session, "textDocument/diagnostic"):
        return None
    request = _request_fn(session)
    if request is None:
        return None

    try:
        response = request(
            "textDocument/diagnostic",
            {
                "textDocument": {"uri": uri},
                "identifier": "cq",
            },
        )
    except _FAIL_OPEN_EXCEPTIONS:
        return None

    return _normalize_diagnostic_response(response, default_uri=uri)


def pull_workspace_diagnostics(
    session: object,
) -> tuple[dict[str, object], ...] | None:
    """Pull diagnostics via ``workspace/diagnostic`` when available.

    Returns:
        Normalized diagnostic rows, or `None` when unsupported/unavailable.
    """
    if not _supports_diagnostic_method(session, "workspace/diagnostic"):
        return None
    request = _request_fn(session)
    if request is None:
        return None

    try:
        response = request("workspace/diagnostic", {"identifier": "cq"})
    except _FAIL_OPEN_EXCEPTIONS:
        return None

    return _normalize_diagnostic_response(response, default_uri=None)


def _normalize_diagnostic_response(
    response: object,
    *,
    default_uri: str | None,
) -> tuple[dict[str, object], ...]:
    if isinstance(response, Mapping):
        rows = _rows_from_mapping_response(response, default_uri=default_uri)
        if rows:
            return tuple(rows)
    if isinstance(response, Sequence):
        return tuple(_rows_from_sequence_response(response, default_uri=default_uri))
    return ()


def _rows_from_mapping_response(
    response: Mapping[str, object],
    *,
    default_uri: str | None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    items = response.get("items")
    if isinstance(items, Sequence):
        rows.extend(_rows_from_sequence_response(items, default_uri=default_uri))

    related_documents = response.get("relatedDocuments")
    if isinstance(related_documents, Mapping):
        for uri, report in related_documents.items():
            if not isinstance(uri, str) or not isinstance(report, Mapping):
                continue
            rows.extend(_diagnostic_rows_from_report(cast("Mapping[str, object]", report), uri=uri))

    if rows:
        return rows
    return _diagnostic_rows_from_report(response, uri=default_uri)


def _rows_from_sequence_response(
    response: Sequence[object],
    *,
    default_uri: str | None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in response:
        if isinstance(item, Mapping):
            rows.extend(
                _diagnostic_rows_from_report(cast("Mapping[str, object]", item), uri=default_uri)
            )
    return rows


def _diagnostic_rows_from_report(
    report: Mapping[str, object],
    *,
    uri: str | None = None,
) -> list[dict[str, object]]:
    report_uri = report.get("uri")
    uri_value = report_uri if isinstance(report_uri, str) else uri

    diagnostics_raw = report.get("diagnostics")
    if not isinstance(diagnostics_raw, Sequence):
        return []

    version_value = report.get("version")
    version = version_value if isinstance(version_value, int) else None

    rows: list[dict[str, object]] = []
    for diagnostic in diagnostics_raw:
        row = _diagnostic_row_from_entry(diagnostic, uri=uri_value, version=version)
        if row is not None:
            rows.append(row)
    return rows


def _diagnostic_row_from_entry(
    diagnostic: object,
    *,
    uri: str | None,
    version: int | None,
) -> dict[str, object] | None:
    if not isinstance(diagnostic, Mapping):
        return None
    start = _extract_start_position(diagnostic)
    if start is None:
        return None

    code_description_href = _extract_code_description_href(diagnostic)
    tags = _extract_int_tuple(diagnostic.get("tags"))
    related = _extract_related_information(diagnostic.get("relatedInformation"))
    data = _extract_data_mapping(diagnostic.get("data"))

    message = diagnostic.get("message")
    severity = diagnostic.get("severity")
    code = diagnostic.get("code")
    return {
        "uri": uri or "",
        "message": message if isinstance(message, str) else "",
        "severity": severity if isinstance(severity, int) else 0,
        "code": code if isinstance(code, str) else None,
        "code_description_href": code_description_href,
        "tags": tags,
        "version": version,
        "related_information": related,
        "data": data,
        "line": start[0],
        "col": start[1],
    }


def _extract_start_position(diagnostic: Mapping[str, object]) -> tuple[int, int] | None:
    range_data = diagnostic.get("range")
    if not isinstance(range_data, Mapping):
        return None
    start = range_data.get("start")
    if not isinstance(start, Mapping):
        return None

    line_value = start.get("line")
    char_value = start.get("character")
    line = line_value if isinstance(line_value, int) else 0
    col = char_value if isinstance(char_value, int) else 0
    return line, col


def _extract_code_description_href(diagnostic: Mapping[str, object]) -> str | None:
    code_description = diagnostic.get("codeDescription")
    if not isinstance(code_description, Mapping):
        return None
    href = code_description.get("href")
    return href if isinstance(href, str) else None


def _extract_int_tuple(value: object) -> tuple[int, ...]:
    if not isinstance(value, Sequence):
        return ()
    return tuple(item for item in value if isinstance(item, int))


def _extract_related_information(value: object) -> tuple[dict[str, object], ...]:
    if not isinstance(value, Sequence):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, Mapping))


def _extract_data_mapping(value: object) -> dict[str, object] | None:
    if isinstance(value, Mapping):
        return dict(value)
    return None


def _request_fn(session: object) -> Callable[[str, dict[str, object]], object] | None:
    request = getattr(session, "_send_request", None)
    if callable(request):
        return cast("Callable[[str, dict[str, object]], object]", request)
    return None


def _supports_diagnostic_method(session: object, method: str) -> bool:
    server_caps = _resolve_server_caps(session)
    if not server_caps:
        return False
    return supports_method(server_caps, method)


def _resolve_server_caps(session: object) -> dict[str, object]:
    raw_caps = getattr(session, "_server_capabilities", None)
    if isinstance(raw_caps, Mapping):
        return coerce_capabilities(raw_caps)

    env = getattr(session, "_session_env", None)
    caps = getattr(env, "capabilities", None)
    server_caps = getattr(caps, "server_caps", None)
    if server_caps is None:
        return {}

    return {
        "definitionProvider": bool(getattr(server_caps, "definition_provider", False)),
        "referencesProvider": bool(getattr(server_caps, "references_provider", False)),
        "diagnosticProvider": getattr(server_caps, "diagnostic_provider_raw", None),
        "workspaceDiagnosticProvider": getattr(
            server_caps,
            "workspace_diagnostic_provider_raw",
            None,
        ),
    }


__all__ = [
    "pull_text_document_diagnostics",
    "pull_workspace_diagnostics",
]
