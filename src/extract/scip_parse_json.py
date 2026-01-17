"""Parse SCIP indexes using scip print --json as a fallback."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping
from pathlib import Path

import ijson


class ScipJsonError(RuntimeError):
    """Base exception for SCIP JSON parsing failures."""


class ScipJsonOutputError(ScipJsonError):
    """Raised when scip print output cannot be captured."""


class ScipJsonCommandError(ScipJsonError):
    """Raised when scip print returns a non-zero exit code."""

    def __init__(self, rc: int, stderr: str) -> None:
        msg = f"scip print failed rc={rc}\n{stderr}"
        super().__init__(msg)


class ScipJsonObject:
    """Dictionary-backed object that supports attribute access."""

    def __init__(self, data: Mapping[str, object]) -> None:
        self._data = dict(data)

    def __getattr__(self, name: str) -> object:
        """Return attribute values using snake_case or camelCase lookup.

        Returns
        -------
        object
            Wrapped attribute value.

        Raises
        ------
        AttributeError
            Raised when the attribute is not present in the underlying data.
        """
        value = _lookup_key(self._data, name)
        if value is _MISSING:
            raise AttributeError(name)
        return _wrap_json(value)


_MISSING = object()


def _snake_to_camel(value: str) -> str:
    """Convert snake_case to lowerCamelCase.

    Returns
    -------
    str
        Converted string.
    """
    parts = value.split("_")
    if not parts:
        return value
    return parts[0] + "".join(p.title() for p in parts[1:])


def _lookup_key(data: Mapping[str, object], name: str) -> object:
    """Lookup a key by snake_case or camelCase.

    Returns
    -------
    object
        Value for the lookup or the sentinel when missing.
    """
    if name in data:
        return data[name]
    alt = _snake_to_camel(name)
    if alt in data:
        return data[alt]
    return _MISSING


def _wrap_json(value: object) -> object:
    """Wrap dict/list values in ScipJsonObject for attribute access.

    Returns
    -------
    object
        Wrapped JSON value.
    """
    if isinstance(value, dict):
        return ScipJsonObject(value)
    if isinstance(value, list):
        return [_wrap_json(item) for item in value]
    return value


def _scip_json_items(index_path: Path, scip_cli_bin: str, prefix: str) -> list[object]:
    """Stream JSON items from scip print --json for a prefix.

    Returns
    -------
    list[object]
        Parsed JSON items for the given prefix.

    Raises
    ------
    ScipJsonOutputError
        Raised when scip output cannot be captured.
    ScipJsonCommandError
        Raised when scip print exits non-zero.
    """
    proc = subprocess.Popen(
        [scip_cli_bin, "print", "--json", str(index_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.stdout is None:
        proc.kill()
        raise ScipJsonOutputError
    try:
        items = list(ijson.items(proc.stdout, prefix))
    finally:
        proc.stdout.close()
        rc = proc.wait()
        if rc != 0:
            err = proc.stderr.read() if proc.stderr else ""
            raise ScipJsonCommandError(rc, err)
    return items


def _first_non_empty(values: list[list[object]]) -> list[object]:
    """Return the first non-empty list from a list of lists.

    Returns
    -------
    list[object]
        First non-empty list, or an empty list when none exist.
    """
    for value in values:
        if value:
            return value
    return []


def parse_index_json(index_path: Path, scip_cli_bin: str) -> ScipJsonObject:
    """Parse index.scip using scip print --json as a fallback.

    Parameters
    ----------
    index_path:
        Path to index.scip.
    scip_cli_bin:
        scip CLI binary path.

    Returns
    -------
    ScipJsonObject
        JSON-backed index object.
    """
    metadata_items = _scip_json_items(index_path, scip_cli_bin, "metadata")
    raw_metadata: dict[str, object] = {}
    if metadata_items:
        first_item = metadata_items[0]
        if isinstance(first_item, dict):
            raw_metadata = first_item
    metadata = raw_metadata

    documents = _scip_json_items(index_path, scip_cli_bin, "documents.item")
    symbol_information = _first_non_empty(
        [
            _scip_json_items(index_path, scip_cli_bin, "symbolInformation.item"),
            _scip_json_items(index_path, scip_cli_bin, "symbol_information.item"),
        ]
    )
    external_symbols = _first_non_empty(
        [
            _scip_json_items(index_path, scip_cli_bin, "externalSymbols.item"),
            _scip_json_items(index_path, scip_cli_bin, "external_symbols.item"),
        ]
    )

    data: dict[str, object] = {
        "metadata": metadata,
        "documents": documents,
        "symbolInformation": symbol_information,
        "externalSymbols": external_symbols,
    }
    return ScipJsonObject(data)
