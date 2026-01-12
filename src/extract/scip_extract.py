"""Extract SCIP metadata and occurrences into Arrow tables using shared helpers."""

from __future__ import annotations

import importlib
import logging
import os
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import ModuleType
from typing import Literal, overload

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext, OrderingLevel, RuntimeProfile
from arrowdsl.core.ids import prefixed_hash_id
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    DataTypeLike,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
)
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec
from arrowdsl.schema.arrays import build_struct, set_or_append_column
from arrowdsl.schema.schema import SchemaMetadataSpec, empty_table
from extract.nested_lists import (
    LargeListAccumulator,
    StructLargeListViewAccumulator,
)
from extract.postprocess import encoding_columns_from_metadata, encoding_projection
from extract.scip_parse_json import parse_index_json
from extract.scip_proto_loader import load_scip_pb2_from_build
from extract.spec_helpers import (
    DatasetRegistration,
    infer_ordering_keys,
    merge_metadata_specs,
    options_metadata_spec,
    ordering_metadata_spec,
    register_dataset,
)
from extract.tables import (
    align_plan,
    finalize_plan_bundle,
    flatten_struct_field,
    materialize_plan,
    plan_from_rows,
    unify_tables,
)
from schema_spec.specs import ArrowFieldSpec, NestedFieldSpec, scip_range_bundle

SCHEMA_VERSION = 1
RANGE_LEN_SHORT = 3
RANGE_LEN_FULL = 4

LOGGER = logging.getLogger(__name__)

type Row = dict[str, object]


@dataclass(frozen=True)
class SCIPIndexOptions:
    """Configure scip-python index invocation."""

    repo_root: Path
    project_name: str
    project_version: str | None = None
    project_namespace: str | None = None
    output_path: Path | None = None
    environment_json: Path | None = None
    target_only: str | None = None
    scip_python_bin: str = "scip-python"
    node_max_old_space_mb: int | None = None
    timeout_s: int | None = None
    extra_args: Sequence[str] = ()


@dataclass(frozen=True)
class SCIPParseOptions:
    """Configure index.scip parsing."""

    prefer_protobuf: bool = True
    allow_json_fallback: bool = False
    scip_pb2_import: str | None = None
    scip_cli_bin: str = "scip"
    build_dir: Path | None = None
    health_check: bool = False
    log_counts: bool = False
    dictionary_encode_strings: bool = False


@dataclass(frozen=True)
class SCIPExtractResult:
    """Hold extracted SCIP tables for metadata, documents, and symbols."""

    scip_metadata: TableLike
    scip_documents: TableLike
    scip_occurrences: TableLike
    scip_symbol_information: TableLike
    scip_symbol_relationships: TableLike
    scip_external_symbol_information: TableLike
    scip_diagnostics: TableLike


SCIP_SIGNATURE_OCCURRENCE_TYPE = pa.struct(
    [
        ("symbol", pa.string()),
        ("symbol_roles", pa.int32()),
        ("range", pa.list_(pa.int32())),
    ]
)

SCIP_SIGNATURE_DOCUMENTATION_TYPE = pa.struct(
    [
        ("text", pa.string()),
        ("language", pa.string()),
        ("occurrences", pa.large_list_view(SCIP_SIGNATURE_OCCURRENCE_TYPE)),
    ]
)

_SIG_DOC_OCCURRENCE_FIELDS = flatten_struct_field(
    pa.field("occurrence", SCIP_SIGNATURE_OCCURRENCE_TYPE)
)
SIG_DOC_OCCURRENCE_KEYS = tuple(
    field.name.split(".", 1)[1] for field in _SIG_DOC_OCCURRENCE_FIELDS
)

ENCODING_META = {"encoding": "dictionary"}

_SCIP_METADATA_FIELDS = [
    ArrowFieldSpec(name="tool_name", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="tool_version", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="project_root", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="text_document_encoding", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="protocol_version", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="meta", dtype=pa.map_(pa.string(), pa.string())),
]

_SCIP_DOCUMENT_FIELDS = [
    ArrowFieldSpec(name="document_id", dtype=pa.string()),
    ArrowFieldSpec(name="path", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="language", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="position_encoding", dtype=pa.string(), metadata=ENCODING_META),
]

_SCIP_OCCURRENCE_FIELDS = [
    ArrowFieldSpec(name="occurrence_id", dtype=pa.string()),
    ArrowFieldSpec(name="document_id", dtype=pa.string()),
    ArrowFieldSpec(name="path", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="symbol", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
    ArrowFieldSpec(name="syntax_kind", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="override_documentation", dtype=pa.large_list(pa.string())),
    *scip_range_bundle(include_len=True).fields,
    *scip_range_bundle(prefix="enc_", include_len=True).fields,
]

_SCIP_SYMBOL_INFO_FIELDS = [
    ArrowFieldSpec(name="symbol_info_id", dtype=pa.string()),
    ArrowFieldSpec(name="symbol", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="display_name", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="kind", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="enclosing_symbol", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="documentation", dtype=pa.large_list(pa.string())),
    ArrowFieldSpec(
        name="signature_documentation",
        dtype=SCIP_SIGNATURE_DOCUMENTATION_TYPE,
    ),
]

_SCIP_SYMBOL_REL_FIELDS = [
    ArrowFieldSpec(name="relationship_id", dtype=pa.string()),
    ArrowFieldSpec(name="symbol", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="related_symbol", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="is_reference", dtype=pa.bool_()),
    ArrowFieldSpec(name="is_implementation", dtype=pa.bool_()),
    ArrowFieldSpec(name="is_type_definition", dtype=pa.bool_()),
    ArrowFieldSpec(name="is_definition", dtype=pa.bool_()),
]

_SCIP_EXTERNAL_SYMBOL_INFO_FIELDS = [
    ArrowFieldSpec(name="symbol_info_id", dtype=pa.string()),
    ArrowFieldSpec(name="symbol", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="display_name", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="kind", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="enclosing_symbol", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="documentation", dtype=pa.large_list(pa.string())),
    ArrowFieldSpec(
        name="signature_documentation",
        dtype=SCIP_SIGNATURE_DOCUMENTATION_TYPE,
    ),
]

_SCIP_DIAGNOSTIC_FIELDS = [
    ArrowFieldSpec(name="diagnostic_id", dtype=pa.string()),
    ArrowFieldSpec(name="document_id", dtype=pa.string()),
    ArrowFieldSpec(name="path", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="severity", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="code", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="message", dtype=pa.string()),
    ArrowFieldSpec(name="source", dtype=pa.string(), metadata=ENCODING_META),
    ArrowFieldSpec(name="tags", dtype=pa.large_list(pa.string())),
    *scip_range_bundle().fields,
]

_SCIP_METADATA_COLUMNS = tuple(field.name for field in _SCIP_METADATA_FIELDS)
_SCIP_DOCUMENT_COLUMNS = tuple(field.name for field in _SCIP_DOCUMENT_FIELDS)
_SCIP_OCCURRENCE_COLUMNS = tuple(field.name for field in _SCIP_OCCURRENCE_FIELDS)
_SCIP_SYMBOL_INFO_COLUMNS = tuple(field.name for field in _SCIP_SYMBOL_INFO_FIELDS)
_SCIP_SYMBOL_REL_COLUMNS = tuple(field.name for field in _SCIP_SYMBOL_REL_FIELDS)
_SCIP_EXTERNAL_SYMBOL_INFO_COLUMNS = tuple(
    field.name for field in _SCIP_EXTERNAL_SYMBOL_INFO_FIELDS
)
_SCIP_DIAGNOSTIC_COLUMNS = tuple(field.name for field in _SCIP_DIAGNOSTIC_FIELDS)

_SCIP_METADATA_EXTRA = {
    b"extractor_name": b"scip",
    b"extractor_version": str(SCHEMA_VERSION).encode("utf-8"),
}

_SCIP_METADATA_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_SCIP_METADATA_COLUMNS),
    extra=_SCIP_METADATA_EXTRA,
)
_SCIP_DOCUMENTS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_SCIP_DOCUMENT_COLUMNS),
    extra=_SCIP_METADATA_EXTRA,
)
_SCIP_OCCURRENCES_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_SCIP_OCCURRENCE_COLUMNS),
    extra=_SCIP_METADATA_EXTRA,
)
_SCIP_SYMBOL_INFO_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_SCIP_SYMBOL_INFO_COLUMNS),
    extra=_SCIP_METADATA_EXTRA,
)
_SCIP_SYMBOL_REL_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_SCIP_SYMBOL_REL_COLUMNS),
    extra=_SCIP_METADATA_EXTRA,
)
_SCIP_EXTERNAL_SYMBOL_INFO_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_SCIP_EXTERNAL_SYMBOL_INFO_COLUMNS),
    extra=_SCIP_METADATA_EXTRA,
)
_SCIP_DIAGNOSTICS_METADATA = ordering_metadata_spec(
    OrderingLevel.IMPLICIT,
    keys=infer_ordering_keys(_SCIP_DIAGNOSTIC_COLUMNS),
    extra=_SCIP_METADATA_EXTRA,
)

SCIP_METADATA_SPEC = register_dataset(
    name="scip_metadata_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=_SCIP_METADATA_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec.simple(*[field.name for field in _SCIP_METADATA_FIELDS]),
        metadata_spec=_SCIP_METADATA_METADATA,
    ),
)

SCIP_DOCUMENTS_SPEC = register_dataset(
    name="scip_documents_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=_SCIP_DOCUMENT_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec.simple(*[field.name for field in _SCIP_DOCUMENT_FIELDS]),
        metadata_spec=_SCIP_DOCUMENTS_METADATA,
    ),
)

SCIP_OCCURRENCES_SPEC = register_dataset(
    name="scip_occurrences_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=_SCIP_OCCURRENCE_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec.simple(*[field.name for field in _SCIP_OCCURRENCE_FIELDS]),
        metadata_spec=_SCIP_OCCURRENCES_METADATA,
    ),
)

SCIP_SYMBOL_INFO_SPEC = register_dataset(
    name="scip_symbol_info_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=_SCIP_SYMBOL_INFO_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec.simple(*[field.name for field in _SCIP_SYMBOL_INFO_FIELDS]),
        metadata_spec=_SCIP_SYMBOL_INFO_METADATA,
    ),
)

SCIP_SYMBOL_RELATIONSHIPS_SPEC = register_dataset(
    name="scip_symbol_relationships_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=_SCIP_SYMBOL_REL_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec.simple(*[field.name for field in _SCIP_SYMBOL_REL_FIELDS]),
        metadata_spec=_SCIP_SYMBOL_REL_METADATA,
    ),
)

SCIP_EXTERNAL_SYMBOL_INFO_SPEC = register_dataset(
    name="scip_external_symbol_info_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=_SCIP_EXTERNAL_SYMBOL_INFO_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec.simple(*[field.name for field in _SCIP_EXTERNAL_SYMBOL_INFO_FIELDS]),
        metadata_spec=_SCIP_EXTERNAL_SYMBOL_INFO_METADATA,
    ),
)

SCIP_DIAGNOSTICS_SPEC = register_dataset(
    name="scip_diagnostics_v1",
    version=SCHEMA_VERSION,
    bundles=(),
    fields=_SCIP_DIAGNOSTIC_FIELDS,
    registration=DatasetRegistration(
        query_spec=QuerySpec.simple(*[field.name for field in _SCIP_DIAGNOSTIC_FIELDS]),
        metadata_spec=_SCIP_DIAGNOSTICS_METADATA,
    ),
)

SCIP_METADATA_SCHEMA = SCIP_METADATA_SPEC.schema()
SCIP_DOCUMENTS_SCHEMA = SCIP_DOCUMENTS_SPEC.schema()
SCIP_OCCURRENCES_SCHEMA = SCIP_OCCURRENCES_SPEC.schema()
SCIP_SYMBOL_INFO_SCHEMA = SCIP_SYMBOL_INFO_SPEC.schema()
SCIP_SYMBOL_RELATIONSHIPS_SCHEMA = SCIP_SYMBOL_RELATIONSHIPS_SPEC.schema()
SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA = SCIP_EXTERNAL_SYMBOL_INFO_SPEC.schema()
SCIP_DIAGNOSTICS_SCHEMA = SCIP_DIAGNOSTICS_SPEC.schema()


def _scip_metadata_specs(
    parse_opts: SCIPParseOptions,
) -> dict[str, SchemaMetadataSpec]:
    run_meta = options_metadata_spec(options=parse_opts)
    return {
        "scip_metadata": merge_metadata_specs(_SCIP_METADATA_METADATA, run_meta),
        "scip_documents": merge_metadata_specs(_SCIP_DOCUMENTS_METADATA, run_meta),
        "scip_occurrences": merge_metadata_specs(_SCIP_OCCURRENCES_METADATA, run_meta),
        "scip_symbol_information": merge_metadata_specs(_SCIP_SYMBOL_INFO_METADATA, run_meta),
        "scip_symbol_relationships": merge_metadata_specs(_SCIP_SYMBOL_REL_METADATA, run_meta),
        "scip_external_symbol_information": merge_metadata_specs(
            _SCIP_EXTERNAL_SYMBOL_INFO_METADATA, run_meta
        ),
        "scip_diagnostics": merge_metadata_specs(_SCIP_DIAGNOSTICS_METADATA, run_meta),
    }


SCIP_METADATA_QUERY = SCIP_METADATA_SPEC.query()
SCIP_DOCUMENTS_QUERY = SCIP_DOCUMENTS_SPEC.query()
SCIP_OCCURRENCES_QUERY = SCIP_OCCURRENCES_SPEC.query()
SCIP_SYMBOL_INFO_QUERY = SCIP_SYMBOL_INFO_SPEC.query()
SCIP_SYMBOL_RELATIONSHIPS_QUERY = SCIP_SYMBOL_RELATIONSHIPS_SPEC.query()
SCIP_EXTERNAL_SYMBOL_INFO_QUERY = SCIP_EXTERNAL_SYMBOL_INFO_SPEC.query()
SCIP_DIAGNOSTICS_QUERY = SCIP_DIAGNOSTICS_SPEC.query()


def _scip_index_command(
    opts: SCIPIndexOptions,
    *,
    out: Path,
    env_json: Path | None,
) -> list[str]:
    cmd: list[str] = [
        opts.scip_python_bin,
        "index",
        ".",
        "--project-name",
        opts.project_name,
        "--output",
        str(out),
    ]
    if opts.project_version:
        cmd.extend(["--project-version", opts.project_version])
    if opts.project_namespace:
        cmd.extend(["--project-namespace", opts.project_namespace])
    if opts.target_only:
        cmd.extend(["--target-only", opts.target_only])
    if env_json is not None:
        cmd.extend(["--environment", str(env_json)])
    cmd.extend(list(opts.extra_args))
    return cmd


def _node_options_env(node_max_old_space_mb: int | None) -> dict[str, str] | None:
    if node_max_old_space_mb is None:
        return None
    flag = f"--max-old-space-size={node_max_old_space_mb}"
    env = os.environ.copy()
    existing = env.get("NODE_OPTIONS", "")
    if flag not in existing:
        env["NODE_OPTIONS"] = f"{existing} {flag}".strip()
    return env


def run_scip_python_index(opts: SCIPIndexOptions) -> Path:
    """Run scip-python to produce an index.scip file.

    Parameters
    ----------
    opts:
        Index invocation options.

    Returns
    -------
    pathlib.Path
        Path to the generated index.scip file.

    Raises
    ------
    RuntimeError
        Raised when scip-python exits with a non-zero status.
    FileNotFoundError
        Raised when the output file is not found after execution.
    """
    repo_root = opts.repo_root.resolve()
    out = opts.output_path or (repo_root / "build" / "scip" / "index.scip")
    if not out.is_absolute():
        out = repo_root / out
    out.parent.mkdir(parents=True, exist_ok=True)

    env_json = opts.environment_json
    if env_json is not None and not env_json.is_absolute():
        env_json = repo_root / env_json

    cmd = _scip_index_command(opts, out=out, env_json=env_json)
    env = _node_options_env(opts.node_max_old_space_mb)

    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
        env=env,
        timeout=opts.timeout_s,
    )
    if proc.returncode != 0:
        msg = f"scip-python failed.\ncmd={cmd}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}\n"
        raise RuntimeError(msg)

    if not out.exists():
        msg = f"scip-python reported success but output not found: {out}"
        raise FileNotFoundError(msg)
    return out


def _normalize_range(rng: Sequence[int]) -> tuple[int, int, int, int, int] | None:
    """Normalize SCIP occurrence ranges to a consistent 4-tuple.

    Returns
    -------
    tuple[int, int, int, int, int]
        Normalized start/end positions and a range length marker.
    """
    if len(rng) == RANGE_LEN_SHORT:
        line, start_c, end_c = rng
        return int(line), int(start_c), int(line), int(end_c), RANGE_LEN_SHORT
    if len(rng) >= RANGE_LEN_FULL:
        sl, sc, el, ec = rng[:RANGE_LEN_FULL]
        return int(sl), int(sc), int(el), int(ec), RANGE_LEN_FULL
    return None


def _normalize_string_items(items: Sequence[object]) -> list[str | None]:
    out: list[str | None] = []
    for item in items:
        if item is None:
            out.append(None)
        elif isinstance(item, str):
            out.append(item)
        else:
            out.append(str(item))
    return out


def _load_scip_pb2(parse_opts: SCIPParseOptions) -> ModuleType | None:
    if parse_opts.scip_pb2_import:
        try:
            return importlib.import_module(parse_opts.scip_pb2_import)
        except ImportError:
            return None

    build_dir = parse_opts.build_dir or Path("build") / "scip"
    try:
        return load_scip_pb2_from_build(build_dir)
    except (FileNotFoundError, RuntimeError):
        pass

    try:
        return importlib.import_module("scip_pb2")
    except ImportError:
        return None


def _parse_index_protobuf(index_path: Path, scip_pb2: ModuleType) -> object:
    data = index_path.read_bytes()
    index = scip_pb2.Index()
    index.ParseFromString(data)
    return index


def parse_index_scip(index_path: Path, parse_opts: SCIPParseOptions | None = None) -> object:
    """Parse index.scip into a protobuf Index object.

    Parameters
    ----------
    index_path:
        Path to the index.scip file.
    parse_opts:
        Parsing options.

    Returns
    -------
    object
        Parsed protobuf Index instance.

    Raises
    ------
    RuntimeError
        Raised when SCIP protobuf bindings cannot be imported.
    """
    parse_opts = parse_opts or SCIPParseOptions()
    if parse_opts.build_dir is None:
        parse_opts = replace(parse_opts, build_dir=index_path.parent)
    scip_pb2 = _load_scip_pb2(parse_opts)
    if scip_pb2 is not None and hasattr(scip_pb2, "Index") and parse_opts.prefer_protobuf:
        return _parse_index_protobuf(index_path, scip_pb2)
    if parse_opts.allow_json_fallback:
        return parse_index_json(index_path, parse_opts.scip_cli_bin)
    if scip_pb2 is not None and hasattr(scip_pb2, "Index"):
        return _parse_index_protobuf(index_path, scip_pb2)
    msg = (
        "SCIP protobuf bindings not available.\n"
        "Run scripts/scip_proto_codegen.py to generate build/scip/scip_pb2.py."
    )
    raise RuntimeError(msg)


def _prefixed_hash(prefix: str, arrays: Sequence[ArrayLike | ChunkedArrayLike]) -> ArrayLike:
    return prefixed_hash_id(arrays, prefix=prefix)


def _set_column(
    table: TableLike,
    name: str,
    values: ArrayLike | ChunkedArrayLike,
) -> TableLike:
    return set_or_append_column(table, name, values)


def _with_document_ids(table: TableLike, *, path_col: str = "path") -> TableLike:
    if table.num_rows == 0 or path_col not in table.column_names:
        return table
    ids = _prefixed_hash("scip_doc", [table[path_col]])
    return _set_column(table, "document_id", ids)


def add_scip_document_ids(table: TableLike, *, path_col: str = "path") -> TableLike:
    """Add document_id values derived from a path column.

    Returns
    -------
    TableLike
        Updated table with a document_id column.
    """
    return _with_document_ids(table, path_col=path_col)


def _with_occurrence_ids(table: TableLike) -> TableLike:
    if table.num_rows == 0:
        return table
    occ_index = pa.array(range(table.num_rows), type=pa.int64())
    ids = _prefixed_hash(
        "scip_occ",
        [
            table["document_id"],
            occ_index,
            table["start_line"],
            table["start_char"],
            table["end_line"],
            table["end_char"],
        ],
    )
    return _set_column(table, "occurrence_id", ids)


def add_scip_occurrence_ids(table: TableLike) -> TableLike:
    """Add occurrence_id values derived from document/span columns.

    Returns
    -------
    TableLike
        Updated table with an occurrence_id column.
    """
    return _with_occurrence_ids(table)


def _with_diagnostic_ids(table: TableLike) -> TableLike:
    if table.num_rows == 0:
        return table
    diag_index = pa.array(range(table.num_rows), type=pa.int64())
    ids = _prefixed_hash(
        "scip_diag",
        [
            table["document_id"],
            diag_index,
            table["start_line"],
            table["start_char"],
            table["end_line"],
            table["end_char"],
        ],
    )
    return _set_column(table, "diagnostic_id", ids)


def _with_symbol_ids(table: TableLike, *, prefix: str) -> TableLike:
    if table.num_rows == 0:
        return table
    ids = _prefixed_hash(prefix, [table["symbol"]])
    return _set_column(table, "symbol_info_id", ids)


def _with_relationship_ids(table: TableLike) -> TableLike:
    if table.num_rows == 0:
        return table
    ids = _prefixed_hash(
        "scip_rel",
        [
            table["symbol"],
            table["related_symbol"],
            table["is_reference"],
            table["is_implementation"],
            table["is_type_definition"],
            table["is_definition"],
        ],
    )
    return _set_column(table, "relationship_id", ids)


def _index_counts(index: object) -> dict[str, int]:
    docs = getattr(index, "documents", [])
    occurrences = 0
    diagnostics = 0
    for doc in docs:
        occs = getattr(doc, "occurrences", [])
        occurrences += len(occs)
        for occ in occs:
            diagnostics += len(getattr(occ, "diagnostics", []))
    return {
        "documents": len(docs),
        "occurrences": occurrences,
        "diagnostics": diagnostics,
        "symbol_information": len(getattr(index, "symbol_information", [])),
        "external_symbols": len(getattr(index, "external_symbols", [])),
    }


def assert_scip_index_health(index: object) -> None:
    """Raise when the SCIP index is missing core data.

    Raises
    ------
    ValueError
        Raised when the index has no documents or occurrences.
    """
    counts = _index_counts(index)
    if counts["documents"] == 0:
        msg = "SCIP index has no documents."
        raise ValueError(msg)
    if counts["occurrences"] == 0:
        msg = "SCIP index has no occurrences."
        raise ValueError(msg)


def _metadata_rows(index: object, *, parse_opts: SCIPParseOptions | None = None) -> list[Row]:
    metadata = getattr(index, "metadata", None)
    tool_info = getattr(metadata, "tool_info", None)
    tool_name = getattr(tool_info, "name", None)
    tool_version = getattr(tool_info, "version", None)
    project_root = getattr(metadata, "project_root", None)
    protocol_version = getattr(metadata, "protocol_version", None)
    text_document_encoding = getattr(metadata, "text_document_encoding", None)
    meta: dict[str, str] = {}
    if parse_opts is not None:
        meta["prefer_protobuf"] = str(parse_opts.prefer_protobuf).lower()
        meta["allow_json_fallback"] = str(parse_opts.allow_json_fallback).lower()
        if parse_opts.scip_pb2_import:
            meta["scip_pb2_import"] = parse_opts.scip_pb2_import
        if parse_opts.build_dir is not None:
            meta["build_dir"] = str(parse_opts.build_dir)
    return [
        {
            "tool_name": tool_name,
            "tool_version": tool_version,
            "project_root": project_root,
            "text_document_encoding": str(text_document_encoding)
            if text_document_encoding is not None
            else None,
            "protocol_version": str(protocol_version) if protocol_version is not None else None,
            "meta": meta or None,
        }
    ]


def _array(values: Sequence[object], data_type: DataTypeLike) -> ArrayLike:
    return pa.array(values, type=data_type)


@dataclass
class _DocAccumulator:
    document_ids: list[str | None] = field(default_factory=list)
    paths: list[str | None] = field(default_factory=list)
    languages: list[str | None] = field(default_factory=list)
    position_encodings: list[str | None] = field(default_factory=list)

    def append(
        self,
        rel_path: str | None,
        language: object,
        position_encoding: object,
    ) -> None:
        self.document_ids.append(None)
        self.paths.append(rel_path)
        self.languages.append(str(language) if language is not None else None)
        self.position_encodings.append(
            str(position_encoding) if position_encoding is not None else None
        )

    def to_table(self) -> TableLike:
        return pa.Table.from_arrays(
            [
                _array(self.document_ids, pa.string()),
                _array(self.paths, pa.string()),
                _array(self.languages, pa.string()),
                _array(self.position_encodings, pa.string()),
            ],
            schema=SCIP_DOCUMENTS_SCHEMA,
        )


@dataclass
class _OccurrenceAccumulator:
    occurrence_ids: list[str | None] = field(default_factory=list)
    document_ids: list[str | None] = field(default_factory=list)
    paths: list[str | None] = field(default_factory=list)
    symbols: list[str | None] = field(default_factory=list)
    symbol_roles: list[int] = field(default_factory=list)
    syntax_kind: list[str | None] = field(default_factory=list)
    override_docs: LargeListAccumulator[str | None] = field(default_factory=LargeListAccumulator)
    start_line: list[int | None] = field(default_factory=list)
    start_char: list[int | None] = field(default_factory=list)
    end_line: list[int | None] = field(default_factory=list)
    end_char: list[int | None] = field(default_factory=list)
    range_len: list[int] = field(default_factory=list)
    enc_start_line: list[int | None] = field(default_factory=list)
    enc_start_char: list[int | None] = field(default_factory=list)
    enc_end_line: list[int | None] = field(default_factory=list)
    enc_end_char: list[int | None] = field(default_factory=list)
    enc_range_len: list[int | None] = field(default_factory=list)

    def append_from_occurrence(
        self,
        occurrence: object,
        rel_path: str | None,
    ) -> tuple[int, int, int, int] | None:
        norm = _normalize_range(list(getattr(occurrence, "range", [])))
        if norm is None:
            sl = sc = el = ec = None
            rlen = 0
            default_range = None
        else:
            sl, sc, el, ec, rlen = norm
            default_range = (sl, sc, el, ec)

        enc_norm = _normalize_range(list(getattr(occurrence, "enclosing_range", [])))
        if enc_norm is None:
            esl = esc = eel = eec = None
            elen = None
        else:
            esl, esc, eel, eec, elen = enc_norm

        self.occurrence_ids.append(None)
        self.document_ids.append(None)
        self.paths.append(rel_path)
        self.symbols.append(getattr(occurrence, "symbol", None))
        self.symbol_roles.append(int(getattr(occurrence, "symbol_roles", 0) or 0))
        self.syntax_kind.append(
            str(getattr(occurrence, "syntax_kind", None))
            if hasattr(occurrence, "syntax_kind")
            else None
        )
        override_docs = getattr(occurrence, "override_documentation", []) or []
        self.override_docs.append(_normalize_string_items(override_docs))
        self.start_line.append(sl)
        self.start_char.append(sc)
        self.end_line.append(el)
        self.end_char.append(ec)
        self.range_len.append(rlen)
        self.enc_start_line.append(esl if elen else None)
        self.enc_start_char.append(esc if elen else None)
        self.enc_end_line.append(eel if elen else None)
        self.enc_end_char.append(eec if elen else None)
        self.enc_range_len.append(elen if elen else None)
        return default_range

    def to_table(self) -> TableLike:
        override_doc = OVERRIDE_DOC_SPEC.builder(self)
        return pa.Table.from_arrays(
            [
                _array(self.occurrence_ids, pa.string()),
                _array(self.document_ids, pa.string()),
                _array(self.paths, pa.string()),
                _array(self.symbols, pa.string()),
                _array(self.symbol_roles, pa.int32()),
                _array(self.syntax_kind, pa.string()),
                override_doc,
                _array(self.start_line, pa.int32()),
                _array(self.start_char, pa.int32()),
                _array(self.end_line, pa.int32()),
                _array(self.end_char, pa.int32()),
                _array(self.range_len, pa.int32()),
                _array(self.enc_start_line, pa.int32()),
                _array(self.enc_start_char, pa.int32()),
                _array(self.enc_end_line, pa.int32()),
                _array(self.enc_end_char, pa.int32()),
                _array(self.enc_range_len, pa.int32()),
            ],
            schema=SCIP_OCCURRENCES_SCHEMA,
        )


def _build_override_docs(acc: _OccurrenceAccumulator) -> ArrayLike:
    return acc.override_docs.build(value_type=pa.string())


OVERRIDE_DOC_SPEC = NestedFieldSpec(
    name="override_documentation",
    dtype=pa.large_list(pa.string()),
    builder=_build_override_docs,
)


@dataclass
class _DiagnosticAccumulator:
    diagnostic_ids: list[str | None] = field(default_factory=list)
    document_ids: list[str | None] = field(default_factory=list)
    paths: list[str | None] = field(default_factory=list)
    severity: list[str | None] = field(default_factory=list)
    code: list[str | None] = field(default_factory=list)
    message: list[str | None] = field(default_factory=list)
    source: list[str | None] = field(default_factory=list)
    tags: LargeListAccumulator[str | None] = field(default_factory=LargeListAccumulator)
    start_line: list[int | None] = field(default_factory=list)
    start_char: list[int | None] = field(default_factory=list)
    end_line: list[int | None] = field(default_factory=list)
    end_char: list[int | None] = field(default_factory=list)

    def append_from_diag(
        self,
        diag: object,
        rel_path: str | None,
        default_range: tuple[int, int, int, int] | None,
    ) -> None:
        drng = list(getattr(diag, "range", []))
        norm = _normalize_range(drng) if drng else None
        if norm is None:
            if default_range is None:
                dsl = dsc = del_ = dec_ = None
            else:
                dsl, dsc, del_, dec_ = default_range
        else:
            dsl, dsc, del_, dec_, _ = norm

        self.diagnostic_ids.append(None)
        self.document_ids.append(None)
        self.paths.append(rel_path)
        self.severity.append(
            str(getattr(diag, "severity", None)) if hasattr(diag, "severity") else None
        )
        self.code.append(str(getattr(diag, "code", None)) if hasattr(diag, "code") else None)
        self.message.append(getattr(diag, "message", None))
        self.source.append(getattr(diag, "source", None))
        tags = getattr(diag, "tags", []) if hasattr(diag, "tags") else []
        if tags is None:
            tags = []
        self.tags.append(_normalize_string_items(tags))
        self.start_line.append(dsl)
        self.start_char.append(dsc)
        self.end_line.append(del_)
        self.end_char.append(dec_)

    def to_table(self) -> TableLike:
        tags = DIAG_TAGS_SPEC.builder(self)
        return pa.Table.from_arrays(
            [
                _array(self.diagnostic_ids, pa.string()),
                _array(self.document_ids, pa.string()),
                _array(self.paths, pa.string()),
                _array(self.severity, pa.string()),
                _array(self.code, pa.string()),
                _array(self.message, pa.string()),
                _array(self.source, pa.string()),
                tags,
                _array(self.start_line, pa.int32()),
                _array(self.start_char, pa.int32()),
                _array(self.end_line, pa.int32()),
                _array(self.end_char, pa.int32()),
            ],
            schema=SCIP_DIAGNOSTICS_SCHEMA,
        )


def _build_diag_tags(acc: _DiagnosticAccumulator) -> ArrayLike:
    return acc.tags.build(value_type=pa.string())


DIAG_TAGS_SPEC = NestedFieldSpec(
    name="tags",
    dtype=pa.large_list(pa.string()),
    builder=_build_diag_tags,
)


def _sig_doc_occurrence_accumulator() -> StructLargeListViewAccumulator:
    return StructLargeListViewAccumulator.with_fields(("symbol", "symbol_roles", "range"))


@dataclass
class _SymbolInfoAccumulator:
    symbol_info_ids: list[str | None] = field(default_factory=list)
    symbols: list[str | None] = field(default_factory=list)
    display_names: list[str | None] = field(default_factory=list)
    kinds: list[str | None] = field(default_factory=list)
    enclosing_symbols: list[str | None] = field(default_factory=list)
    documentation: LargeListAccumulator[str | None] = field(default_factory=LargeListAccumulator)
    sig_doc_texts: list[str | None] = field(default_factory=list)
    sig_doc_languages: list[str | None] = field(default_factory=list)
    sig_doc_is_null: list[bool] = field(default_factory=list)
    sig_doc_occurrences: StructLargeListViewAccumulator = field(
        default_factory=_sig_doc_occurrence_accumulator
    )

    def append(self, symbol_info: object) -> None:
        self.symbol_info_ids.append(None)
        self.symbols.append(getattr(symbol_info, "symbol", None))
        self.display_names.append(getattr(symbol_info, "display_name", None))
        self.kinds.append(
            str(getattr(symbol_info, "kind", None)) if hasattr(symbol_info, "kind") else None
        )
        self.enclosing_symbols.append(
            getattr(symbol_info, "enclosing_symbol", None)
            if hasattr(symbol_info, "enclosing_symbol")
            else None
        )
        self._append_documentation(symbol_info)
        sig_doc = getattr(symbol_info, "signature_documentation", None)
        self._append_signature_documentation(sig_doc)

    def _append_documentation(self, symbol_info: object) -> None:
        docs = getattr(symbol_info, "documentation", None)
        if not hasattr(symbol_info, "documentation") or docs is None:
            docs = []
        self.documentation.append(_normalize_string_items(docs))

    def _append_signature_documentation(self, sig_doc: object | None) -> None:
        if sig_doc is None:
            self._append_empty_signature_documentation()
            return
        self._append_signature_doc_fields(sig_doc)
        self._append_signature_doc_occurrences(sig_doc)

    def _append_empty_signature_documentation(self) -> None:
        self.sig_doc_texts.append(None)
        self.sig_doc_languages.append(None)
        self.sig_doc_is_null.append(True)
        self.sig_doc_occurrences.append_rows([])

    def _append_signature_doc_fields(self, sig_doc: object) -> None:
        text = getattr(sig_doc, "text", None)
        if text is None or isinstance(text, str):
            self.sig_doc_texts.append(text)
        else:
            self.sig_doc_texts.append(str(text))
        language = getattr(sig_doc, "language", None)
        self.sig_doc_languages.append(str(language) if language is not None else None)
        self.sig_doc_is_null.append(False)

    def _append_signature_doc_occurrences(self, sig_doc: object) -> None:
        occurrences = getattr(sig_doc, "occurrences", None)
        if occurrences is None:
            occurrences = []
        rows: list[dict[str, object]] = []
        for occ in occurrences:
            range_values: list[int] = []
            for value in getattr(occ, "range", []) or []:
                if isinstance(value, bool):
                    continue
                if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
                    range_values.append(int(value))
            rows.append(
                dict(
                    zip(
                        SIG_DOC_OCCURRENCE_KEYS,
                        (
                            getattr(occ, "symbol", None),
                            int(getattr(occ, "symbol_roles", 0) or 0),
                            range_values,
                        ),
                        strict=True,
                    )
                )
            )
        self.sig_doc_occurrences.append_rows(rows)

    def to_table(self, *, schema: SchemaLike) -> TableLike:
        documentation = SYMBOL_DOC_SPEC.builder(self)
        sig_doc = SIG_DOC_SPEC.builder(self)
        return pa.Table.from_arrays(
            [
                _array(self.symbol_info_ids, pa.string()),
                _array(self.symbols, pa.string()),
                _array(self.display_names, pa.string()),
                _array(self.kinds, pa.string()),
                _array(self.enclosing_symbols, pa.string()),
                documentation,
                sig_doc,
            ],
            schema=schema,
        )


def _build_symbol_docs(acc: _SymbolInfoAccumulator) -> ArrayLike:
    return acc.documentation.build(value_type=pa.string())


def _build_sig_doc_occurrences(acc: _SymbolInfoAccumulator) -> ArrayLike:
    return acc.sig_doc_occurrences.build(
        field_types={
            "symbol": pa.string(),
            "symbol_roles": pa.int32(),
            "range": pa.list_(pa.int32()),
        }
    )


def _build_signature_doc(acc: _SymbolInfoAccumulator) -> ArrayLike:
    return build_struct(
        {
            "text": pa.array(acc.sig_doc_texts, type=pa.string()),
            "language": pa.array(acc.sig_doc_languages, type=pa.string()),
            "occurrences": SIG_DOC_OCCURRENCES_SPEC.builder(acc),
        },
        mask=pa.array(acc.sig_doc_is_null, type=pa.bool_()),
    )


SYMBOL_DOC_SPEC = NestedFieldSpec(
    name="documentation",
    dtype=pa.large_list(pa.string()),
    builder=_build_symbol_docs,
)

SIG_DOC_OCCURRENCES_SPEC = NestedFieldSpec(
    name="occurrences",
    dtype=pa.large_list_view(SCIP_SIGNATURE_OCCURRENCE_TYPE),
    builder=_build_sig_doc_occurrences,
)

SIG_DOC_SPEC = NestedFieldSpec(
    name="signature_documentation",
    dtype=SCIP_SIGNATURE_DOCUMENTATION_TYPE,
    builder=_build_signature_doc,
)


@dataclass
class _RelationshipAccumulator:
    relationship_ids: list[str | None] = field(default_factory=list)
    symbols: list[str | None] = field(default_factory=list)
    related_symbols: list[str | None] = field(default_factory=list)
    is_reference: list[bool] = field(default_factory=list)
    is_implementation: list[bool] = field(default_factory=list)
    is_type_definition: list[bool] = field(default_factory=list)
    is_definition: list[bool] = field(default_factory=list)

    def append(self, symbol: str | None, relationship: object) -> None:
        related = getattr(relationship, "symbol", None)
        if symbol is None or related is None:
            return
        self.relationship_ids.append(None)
        self.symbols.append(symbol)
        self.related_symbols.append(related)
        self.is_reference.append(bool(getattr(relationship, "is_reference", False)))
        self.is_implementation.append(bool(getattr(relationship, "is_implementation", False)))
        self.is_type_definition.append(bool(getattr(relationship, "is_type_definition", False)))
        self.is_definition.append(bool(getattr(relationship, "is_definition", False)))

    def to_table(self) -> TableLike:
        return pa.Table.from_arrays(
            [
                _array(self.relationship_ids, pa.string()),
                _array(self.symbols, pa.string()),
                _array(self.related_symbols, pa.string()),
                _array(self.is_reference, pa.bool_()),
                _array(self.is_implementation, pa.bool_()),
                _array(self.is_type_definition, pa.bool_()),
                _array(self.is_definition, pa.bool_()),
            ],
            schema=SCIP_SYMBOL_RELATIONSHIPS_SCHEMA,
        )


def _document_tables(index: object) -> tuple[TableLike, TableLike, TableLike]:
    doc_tables: list[TableLike] = []
    occ_tables: list[TableLike] = []
    diag_tables: list[TableLike] = []

    for doc in getattr(index, "documents", []):
        docs = _DocAccumulator()
        occs = _OccurrenceAccumulator()
        diags = _DiagnosticAccumulator()

        rel_path = getattr(doc, "relative_path", None)
        docs.append(
            rel_path,
            getattr(doc, "language", None),
            getattr(doc, "position_encoding", None),
        )

        for occ in getattr(doc, "occurrences", []):
            default_range = occs.append_from_occurrence(occ, rel_path)
            for diag in getattr(occ, "diagnostics", []):
                diags.append_from_diag(diag, rel_path, default_range)

        doc_tables.append(docs.to_table())
        occ_tables.append(occs.to_table())
        diag_tables.append(diags.to_table())

    if not doc_tables:
        return (
            empty_table(SCIP_DOCUMENTS_SCHEMA),
            empty_table(SCIP_OCCURRENCES_SCHEMA),
            empty_table(SCIP_DIAGNOSTICS_SCHEMA),
        )
    return (
        unify_tables(doc_tables),
        unify_tables(occ_tables),
        unify_tables(diag_tables),
    )


def _symbol_tables(index: object) -> tuple[TableLike, TableLike, TableLike]:
    sym = _SymbolInfoAccumulator()
    ext = _SymbolInfoAccumulator()
    rels = _RelationshipAccumulator()
    for si in getattr(index, "symbol_information", []):
        sym.append(si)
        symbol = getattr(si, "symbol", None)
        for rel in getattr(si, "relationships", []):
            rels.append(symbol, rel)
    for si in getattr(index, "external_symbols", []):
        ext.append(si)
    return (
        sym.to_table(schema=SCIP_SYMBOL_INFO_SCHEMA),
        rels.to_table(),
        ext.to_table(schema=SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA),
    )


def _finalize_plan(
    plan: Plan,
    *,
    schema: SchemaLike,
    query: QuerySpec,
    exec_ctx: ExecutionContext,
    encode_columns: Sequence[str] = (),
) -> Plan:
    plan = query.apply_to_plan(plan, ctx=exec_ctx)
    plan = align_plan(
        plan,
        schema=schema,
        ctx=exec_ctx,
    )
    if encode_columns:
        exprs, names = encoding_projection(encode_columns, available=schema.names)
        plan = plan.project(exprs, names, ctx=exec_ctx)
    return plan


def _finalize_table(
    table: TableLike,
    *,
    schema: SchemaLike,
    query: QuerySpec,
    exec_ctx: ExecutionContext,
    encode_columns: Sequence[str] = (),
) -> TableLike:
    plan = _finalize_plan(
        Plan.table_source(table),
        schema=schema,
        query=query,
        exec_ctx=exec_ctx,
        encode_columns=encode_columns,
    )
    return plan.to_table(ctx=exec_ctx)


def _extract_tables_from_index(
    index: object,
    *,
    parse_opts: SCIPParseOptions,
    exec_ctx: ExecutionContext,
) -> SCIPExtractResult:
    plans = _extract_plan_bundle_from_index(
        index,
        parse_opts=parse_opts,
        exec_ctx=exec_ctx,
    )
    metadata_specs = _scip_metadata_specs(parse_opts)
    return SCIPExtractResult(
        scip_metadata=materialize_plan(
            plans["scip_metadata"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_metadata"],
        ),
        scip_documents=materialize_plan(
            plans["scip_documents"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_documents"],
        ),
        scip_occurrences=materialize_plan(
            plans["scip_occurrences"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_occurrences"],
        ),
        scip_symbol_information=materialize_plan(
            plans["scip_symbol_information"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_symbol_information"],
        ),
        scip_symbol_relationships=materialize_plan(
            plans["scip_symbol_relationships"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_symbol_relationships"],
        ),
        scip_external_symbol_information=materialize_plan(
            plans["scip_external_symbol_information"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_external_symbol_information"],
        ),
        scip_diagnostics=materialize_plan(
            plans["scip_diagnostics"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_diagnostics"],
        ),
    )


def _extract_plan_bundle_from_index(
    index: object,
    *,
    parse_opts: SCIPParseOptions,
    exec_ctx: ExecutionContext,
) -> dict[str, Plan]:
    meta_rows = _metadata_rows(index, parse_opts=parse_opts)
    t_docs, t_occs, t_diags = _document_tables(index)

    t_meta_plan = plan_from_rows(meta_rows, schema=SCIP_METADATA_SCHEMA, label="scip_metadata")
    t_syms, t_rels, t_ext = _symbol_tables(index)

    t_docs = _with_document_ids(t_docs)
    t_occs = _with_document_ids(t_occs)
    t_occs = _with_occurrence_ids(t_occs)
    t_diags = _with_document_ids(t_diags)
    t_diags = _with_diagnostic_ids(t_diags)
    t_syms = _with_symbol_ids(t_syms, prefix="scip_sym")
    t_rels = _with_relationship_ids(t_rels)
    t_ext = _with_symbol_ids(t_ext, prefix="scip_ext_sym")

    encode_meta = (
        encoding_columns_from_metadata(SCIP_METADATA_SCHEMA)
        if parse_opts.dictionary_encode_strings
        else []
    )
    encode_docs = (
        encoding_columns_from_metadata(SCIP_DOCUMENTS_SCHEMA)
        if parse_opts.dictionary_encode_strings
        else []
    )
    encode_occs = (
        encoding_columns_from_metadata(SCIP_OCCURRENCES_SCHEMA)
        if parse_opts.dictionary_encode_strings
        else []
    )
    encode_syms = (
        encoding_columns_from_metadata(SCIP_SYMBOL_INFO_SCHEMA)
        if parse_opts.dictionary_encode_strings
        else []
    )
    encode_rels = (
        encoding_columns_from_metadata(SCIP_SYMBOL_RELATIONSHIPS_SCHEMA)
        if parse_opts.dictionary_encode_strings
        else []
    )
    encode_ext = (
        encoding_columns_from_metadata(SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA)
        if parse_opts.dictionary_encode_strings
        else []
    )
    encode_diags = (
        encoding_columns_from_metadata(SCIP_DIAGNOSTICS_SCHEMA)
        if parse_opts.dictionary_encode_strings
        else []
    )

    return {
        "scip_metadata": _finalize_plan(
            t_meta_plan,
            schema=SCIP_METADATA_SCHEMA,
            query=SCIP_METADATA_QUERY,
            exec_ctx=exec_ctx,
            encode_columns=encode_meta,
        ),
        "scip_documents": _finalize_plan(
            Plan.table_source(t_docs),
            schema=SCIP_DOCUMENTS_SCHEMA,
            query=SCIP_DOCUMENTS_QUERY,
            exec_ctx=exec_ctx,
            encode_columns=encode_docs,
        ),
        "scip_occurrences": _finalize_plan(
            Plan.table_source(t_occs),
            schema=SCIP_OCCURRENCES_SCHEMA,
            query=SCIP_OCCURRENCES_QUERY,
            exec_ctx=exec_ctx,
            encode_columns=encode_occs,
        ),
        "scip_symbol_information": _finalize_plan(
            Plan.table_source(t_syms),
            schema=SCIP_SYMBOL_INFO_SCHEMA,
            query=SCIP_SYMBOL_INFO_QUERY,
            exec_ctx=exec_ctx,
            encode_columns=encode_syms,
        ),
        "scip_symbol_relationships": _finalize_plan(
            Plan.table_source(t_rels),
            schema=SCIP_SYMBOL_RELATIONSHIPS_SCHEMA,
            query=SCIP_SYMBOL_RELATIONSHIPS_QUERY,
            exec_ctx=exec_ctx,
            encode_columns=encode_rels,
        ),
        "scip_external_symbol_information": _finalize_plan(
            Plan.table_source(t_ext),
            schema=SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA,
            query=SCIP_EXTERNAL_SYMBOL_INFO_QUERY,
            exec_ctx=exec_ctx,
            encode_columns=encode_ext,
        ),
        "scip_diagnostics": _finalize_plan(
            Plan.table_source(t_diags),
            schema=SCIP_DIAGNOSTICS_SCHEMA,
            query=SCIP_DIAGNOSTICS_QUERY,
            exec_ctx=exec_ctx,
            encode_columns=encode_diags,
        ),
    }


@overload
def extract_scip_tables(
    *,
    scip_index_path: str | None,
    repo_root: str | None,
    ctx: ExecutionContext | None = None,
    parse_opts: SCIPParseOptions | None = None,
    prefer_reader: Literal[False] = False,
) -> Mapping[str, TableLike]: ...


@overload
def extract_scip_tables(
    *,
    scip_index_path: str | None,
    repo_root: str | None,
    ctx: ExecutionContext | None = None,
    parse_opts: SCIPParseOptions | None = None,
    prefer_reader: Literal[True],
) -> Mapping[str, TableLike | RecordBatchReaderLike]: ...


def extract_scip_tables(
    *,
    scip_index_path: str | None,
    repo_root: str | None,
    ctx: ExecutionContext | None = None,
    parse_opts: SCIPParseOptions | None = None,
    prefer_reader: bool = False,
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Extract SCIP tables as a name-keyed bundle.

    Parameters
    ----------
    scip_index_path:
        Path to the index.scip file or ``None``.
    repo_root:
        Optional repository root used for resolving relative paths.
    ctx:
        Execution context for plan execution.
    parse_opts:
        Parsing options.
    prefer_reader:
        When True, return streaming readers when possible.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted SCIP outputs keyed by output name.
    """
    parse_opts = parse_opts or SCIPParseOptions()
    exec_ctx = ctx or ExecutionContext(runtime=RuntimeProfile(name="DEFAULT"))
    metadata_specs = _scip_metadata_specs(parse_opts)
    if scip_index_path is None:
        plans = {
            "scip_metadata": Plan.table_source(empty_table(SCIP_METADATA_SCHEMA)),
            "scip_documents": Plan.table_source(empty_table(SCIP_DOCUMENTS_SCHEMA)),
            "scip_occurrences": Plan.table_source(empty_table(SCIP_OCCURRENCES_SCHEMA)),
            "scip_symbol_information": Plan.table_source(empty_table(SCIP_SYMBOL_INFO_SCHEMA)),
            "scip_symbol_relationships": Plan.table_source(
                empty_table(SCIP_SYMBOL_RELATIONSHIPS_SCHEMA)
            ),
            "scip_external_symbol_information": Plan.table_source(
                empty_table(SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA)
            ),
            "scip_diagnostics": Plan.table_source(empty_table(SCIP_DIAGNOSTICS_SCHEMA)),
        }
    else:
        index_path = Path(scip_index_path)
        if repo_root is not None and not index_path.is_absolute():
            index_path = Path(repo_root) / index_path
        index = parse_index_scip(index_path, parse_opts=parse_opts)
        if parse_opts.log_counts or parse_opts.health_check:
            counts = _index_counts(index)
            LOGGER.info(
                "SCIP index stats: documents=%d occurrences=%d diagnostics=%d symbols=%d "
                "external_symbols=%d",
                counts["documents"],
                counts["occurrences"],
                counts["diagnostics"],
                counts["symbol_information"],
                counts["external_symbols"],
            )
        if parse_opts.health_check:
            assert_scip_index_health(index)
        plans = _extract_plan_bundle_from_index(
            index,
            parse_opts=parse_opts,
            exec_ctx=exec_ctx,
        )

    return finalize_plan_bundle(
        plans,
        ctx=exec_ctx,
        prefer_reader=prefer_reader,
        metadata_specs=metadata_specs,
    )
