"""Tree-sitter query packs for Python extraction."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256

from tree_sitter import Language, Query


@dataclass(frozen=True)
class QuerySpec:
    """Named query specification."""

    name: str
    source: str
    allow_non_local: bool = False


@dataclass(frozen=True)
class TreeSitterQueryPack:
    """Compiled query pack and metadata."""

    version: str
    queries: Mapping[str, Query]
    sources: Mapping[str, str]

    def metadata(self) -> dict[str, str]:
        names = ",".join(sorted(self.queries))
        return {
            "query_pack_version": self.version,
            "query_pack_names": names,
        }


_PY_QUERY_SPECS: tuple[QuerySpec, ...] = (
    QuerySpec(
        name="defs",
        source="""
        (function_definition name: (identifier) @def.name) @def.node
        (class_definition name: (identifier) @def.name) @def.node
        (decorated_definition
          definition: (function_definition name: (identifier) @def.name) @def.node)
        (decorated_definition
          definition: (class_definition name: (identifier) @def.name) @def.node)
        """,
    ),
    QuerySpec(
        name="calls",
        source="""
        (call function: (identifier) @call.name) @call.node
        (call function: (attribute) @call.attr) @call.node
        """,
    ),
    QuerySpec(
        name="imports",
        source="""
        (import_statement name: (dotted_name) @import.module) @import.node
        (import_statement
          name: (aliased_import
            name: (dotted_name) @import.module
            alias: (identifier) @import.alias)) @import.node
        (import_from_statement
          module_name: (dotted_name) @import.from
          name: (dotted_name) @import.name) @import.node
        (import_from_statement
          module_name: (dotted_name) @import.from
          name: (aliased_import
            name: (dotted_name) @import.name
            alias: (identifier) @import.alias)) @import.node
        (import_from_statement
          module_name: (relative_import) @import.relative
          name: (dotted_name) @import.name) @import.node
        (import_from_statement
          module_name: (relative_import) @import.relative
          name: (aliased_import
            name: (dotted_name) @import.name
            alias: (identifier) @import.alias)) @import.node
        """,
    ),
    QuerySpec(
        name="docstrings",
        source="""
        (module . (expression_statement (string) @doc.string)) @doc.owner
        (class_definition
          body: (block . (expression_statement (string) @doc.string))) @doc.owner
        (function_definition
          body: (block . (expression_statement (string) @doc.string))) @doc.owner
        """,
    ),
)


def compile_query_pack(language: Language) -> TreeSitterQueryPack:
    """Compile and validate the Python query pack."""
    sources: dict[str, str] = {}
    queries: dict[str, Query] = {}
    for spec in _PY_QUERY_SPECS:
        query = Query(language, spec.source)
        _lint_query(spec, query)
        sources[spec.name] = spec.source
        queries[spec.name] = query
    return TreeSitterQueryPack(
        version=_pack_version(sources),
        queries=queries,
        sources=sources,
    )


def _pack_version(sources: Mapping[str, str]) -> str:
    digest = sha256()
    for name in sorted(sources):
        digest.update(name.encode("utf-8"))
        digest.update(b"\n")
        digest.update(sources[name].encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _lint_query(spec: QuerySpec, query: Query) -> None:
    for idx in range(query.pattern_count):
        if not query.is_pattern_rooted(idx):
            msg = f"Query {spec.name!r} pattern[{idx}] is not rooted."
            raise ValueError(msg)
        if query.is_pattern_non_local(idx) and not spec.allow_non_local:
            msg = f"Query {spec.name!r} pattern[{idx}] is non-local."
            raise ValueError(msg)
