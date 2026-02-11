# ruff: noqa: DOC201,C901,PLR0914,ANN202,SIM103
"""Shared diagnostics pull helpers for LSP clients."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast


def pull_text_document_diagnostics(
    session: object,
    *,
    uri: str,
) -> tuple[dict[str, object], ...] | None:
    """Pull diagnostics via `textDocument/diagnostic` when available."""
    if not _supports_method(session, "textDocument/diagnostic"):
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
    except Exception:  # noqa: BLE001 - fail-open by design
        return None

    return _normalize_diagnostic_response(response, default_uri=uri)


def pull_workspace_diagnostics(
    session: object,
) -> tuple[dict[str, object], ...] | None:
    """Pull diagnostics via `workspace/diagnostic` when available."""
    if not _supports_method(session, "workspace/diagnostic"):
        return None
    request = _request_fn(session)
    if request is None:
        return None

    try:
        response = request(
            "workspace/diagnostic",
            {
                "identifier": "cq",
            },
        )
    except Exception:  # noqa: BLE001 - fail-open by design
        return None

    return _normalize_diagnostic_response(response, default_uri=None)


def _normalize_diagnostic_response(
    response: object,
    *,
    default_uri: str | None,
) -> tuple[dict[str, object], ...]:
    rows: list[dict[str, object]] = []

    if isinstance(response, Mapping):
        items = response.get("items")
        if isinstance(items, Sequence):
            for item in items:
                if isinstance(item, Mapping):
                    rows.extend(_diagnostic_rows_from_report(cast("Mapping[str, object]", item)))
        related_documents = response.get("relatedDocuments")
        if isinstance(related_documents, Mapping):
            for uri, report in related_documents.items():
                if not isinstance(uri, str) or not isinstance(report, Mapping):
                    continue
                rows.extend(
                    _diagnostic_rows_from_report(cast("Mapping[str, object]", report), uri=uri)
                )
        if rows:
            return tuple(rows)

    if isinstance(response, Sequence):
        for item in response:
            if isinstance(item, Mapping):
                rows.extend(
                    _diagnostic_rows_from_report(
                        cast("Mapping[str, object]", item), uri=default_uri
                    )
                )
        return tuple(rows)

    if isinstance(response, Mapping):
        rows.extend(_diagnostic_rows_from_report(response, uri=default_uri))
    return tuple(rows)


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
        if not isinstance(diagnostic, Mapping):
            continue
        range_data = diagnostic.get("range")
        if not isinstance(range_data, Mapping):
            continue
        start = range_data.get("start")
        if not isinstance(start, Mapping):
            continue

        code_description = diagnostic.get("codeDescription")
        code_description_href = None
        if isinstance(code_description, Mapping):
            href = code_description.get("href")
            if isinstance(href, str):
                code_description_href = href

        tags_raw = diagnostic.get("tags")
        tags: tuple[int, ...] = ()
        if isinstance(tags_raw, Sequence):
            tags = tuple(tag for tag in tags_raw if isinstance(tag, int))

        related_raw = diagnostic.get("relatedInformation")
        related: tuple[dict[str, object], ...] = ()
        if isinstance(related_raw, Sequence):
            related = tuple(dict(item) for item in related_raw if isinstance(item, Mapping))

        data_raw = diagnostic.get("data")
        data = dict(data_raw) if isinstance(data_raw, Mapping) else None

        rows.append(
            {
                "uri": uri_value or "",
                "message": diagnostic.get("message")
                if isinstance(diagnostic.get("message"), str)
                else "",
                "severity": diagnostic.get("severity", 0)
                if isinstance(diagnostic.get("severity"), int)
                else 0,
                "code": diagnostic.get("code") if isinstance(diagnostic.get("code"), str) else None,
                "code_description_href": code_description_href,
                "tags": tags,
                "version": version,
                "related_information": related,
                "data": data,
                "line": start.get("line", 0) if isinstance(start.get("line"), int) else 0,
                "col": start.get("character", 0) if isinstance(start.get("character"), int) else 0,
            }
        )
    return rows


def _request_fn(session: object):
    request = getattr(session, "_send_request", None)
    if callable(request):
        return request
    return None


def _supports_method(session: object, method: str) -> bool:
    env = getattr(session, "_session_env", None)
    caps = getattr(env, "capabilities", None)
    server_caps = getattr(caps, "server_caps", None)
    if server_caps is None:
        return False

    if method == "textDocument/diagnostic":
        return bool(getattr(server_caps, "publish_diagnostics", True))
    if method == "workspace/diagnostic":
        return True
    return False


__all__ = [
    "pull_text_document_diagnostics",
    "pull_workspace_diagnostics",
]
