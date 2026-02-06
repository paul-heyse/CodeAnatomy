"""Helpers for inspecting Delta commit metadata in tests."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

_STANDARD_COMMIT_INFO_KEYS: set[str] = {
    "appId",
    "clusterId",
    "engineInfo",
    "inCommitTimestamp",
    "isBlindAppend",
    "isolationLevel",
    "operation",
    "operationMetrics",
    "operationParameters",
    "readVersion",
    "timestamp",
    "txnId",
    "txnVersion",
    "userMetadata",
}


def latest_commit_metadata(table_path: Path) -> dict[str, str]:
    """Return custom commit metadata from the latest Delta log entry.

    Returns:
    -------
    dict[str, str]
        Parsed user metadata key/value pairs, if available.
    """
    commit_info = latest_commit_info(table_path)
    if not commit_info:
        return {}
    return _extract_commit_metadata(commit_info)


def latest_commit_info(table_path: Path) -> dict[str, object]:
    """Return the commitInfo payload from the latest Delta log JSON file.

    Returns:
    -------
    dict[str, object]
        Commit info payload when available.
    """
    log_dir = table_path / "_delta_log"
    json_files = [path for path in log_dir.glob("*.json") if path.stem.isdigit()]
    if not json_files:
        return {}
    latest = max(json_files, key=_delta_log_version)
    for line in latest.read_text().splitlines():
        payload = _parse_json(line)
        if payload is None:
            continue
        commit_info = payload.get("commitInfo")
        if isinstance(commit_info, Mapping):
            return dict(commit_info)
    return {}


def _delta_log_version(path: Path) -> int:
    try:
        return int(path.stem)
    except ValueError:
        return -1


def _parse_json(line: str) -> dict[str, object] | None:
    try:
        payload = json.loads(line)
    except ValueError:
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _extract_commit_metadata(commit_info: Mapping[str, object]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    user_metadata = commit_info.get("userMetadata")
    if isinstance(user_metadata, Mapping):
        metadata.update(_stringify_mapping(user_metadata))
    elif isinstance(user_metadata, str) and user_metadata.strip():
        decoded = _parse_json(user_metadata)
        if decoded is not None:
            metadata.update(_stringify_mapping(decoded))
    for key, value in commit_info.items():
        if key in _STANDARD_COMMIT_INFO_KEYS or value is None:
            continue
        if isinstance(value, Mapping):
            metadata.update(_stringify_mapping(value))
            continue
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            continue
        metadata.setdefault(str(key), str(value))
    return metadata


def _stringify_mapping(payload: Mapping[str, object]) -> dict[str, str]:
    return {str(key): str(value) for key, value in payload.items() if value is not None}


__all__ = ["latest_commit_info", "latest_commit_metadata"]
