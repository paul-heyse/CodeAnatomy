"""Msgspec schema introspection helpers."""

from __future__ import annotations

import msgspec

from incremental.cdf_cursors import CdfCursor, CdfCursorStore
from obs.diagnostics import PreparedStatementSpec
from sqlglot_tools.optimizer import AstArtifact

_SCHEMA_TYPES: dict[str, type[msgspec.Struct]] = {
    "cdf_cursor": CdfCursor,
    "cdf_cursor_store": CdfCursorStore,
    "prepared_statement_spec": PreparedStatementSpec,
    "sqlglot_ast_artifact": AstArtifact,
}

TYPE_INFO: dict[str, msgspec.inspect.Type] = {
    name: msgspec.inspect.type_info(schema_type) for name, schema_type in _SCHEMA_TYPES.items()
}

__all__ = ["TYPE_INFO"]
