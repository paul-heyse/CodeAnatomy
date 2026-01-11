"""Extract SCIP metadata and occurrences into Arrow tables."""

from __future__ import annotations

import importlib
import logging
import os
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import ModuleType

import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.empty import empty_table
from arrowdsl.ids import hash64_from_arrays
from arrowdsl.nested import build_list_array, build_struct_array
from arrowdsl.schema import align_to_schema
from extract.scip_parse_json import parse_index_json
from extract.scip_proto_loader import load_scip_pb2_from_build
from schema_spec.core import ArrowFieldSpec, TableSchemaSpec

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

    scip_metadata: pa.Table
    scip_documents: pa.Table
    scip_occurrences: pa.Table
    scip_symbol_information: pa.Table
    scip_symbol_relationships: pa.Table
    scip_external_symbol_information: pa.Table
    scip_diagnostics: pa.Table


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
        ("occurrences", pa.list_(SCIP_SIGNATURE_OCCURRENCE_TYPE)),
    ]
)

SCIP_METADATA_SPEC = TableSchemaSpec(
    name="scip_metadata_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="tool_name", dtype=pa.string()),
        ArrowFieldSpec(name="tool_version", dtype=pa.string()),
        ArrowFieldSpec(name="project_root", dtype=pa.string()),
        ArrowFieldSpec(name="text_document_encoding", dtype=pa.string()),
        ArrowFieldSpec(name="protocol_version", dtype=pa.string()),
    ],
)

SCIP_DOCUMENTS_SPEC = TableSchemaSpec(
    name="scip_documents_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="document_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="language", dtype=pa.string()),
        ArrowFieldSpec(name="position_encoding", dtype=pa.string()),
    ],
)

SCIP_OCCURRENCES_SPEC = TableSchemaSpec(
    name="scip_occurrences_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="occurrence_id", dtype=pa.string()),
        ArrowFieldSpec(name="document_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="symbol", dtype=pa.string()),
        ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
        ArrowFieldSpec(name="syntax_kind", dtype=pa.string()),
        ArrowFieldSpec(name="override_documentation", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="start_line", dtype=pa.int32()),
        ArrowFieldSpec(name="start_char", dtype=pa.int32()),
        ArrowFieldSpec(name="end_line", dtype=pa.int32()),
        ArrowFieldSpec(name="end_char", dtype=pa.int32()),
        ArrowFieldSpec(name="range_len", dtype=pa.int32()),
        ArrowFieldSpec(name="enc_start_line", dtype=pa.int32()),
        ArrowFieldSpec(name="enc_start_char", dtype=pa.int32()),
        ArrowFieldSpec(name="enc_end_line", dtype=pa.int32()),
        ArrowFieldSpec(name="enc_end_char", dtype=pa.int32()),
        ArrowFieldSpec(name="enc_range_len", dtype=pa.int32()),
    ],
)

SCIP_SYMBOL_INFO_SPEC = TableSchemaSpec(
    name="scip_symbol_info_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="symbol_info_id", dtype=pa.string()),
        ArrowFieldSpec(name="symbol", dtype=pa.string()),
        ArrowFieldSpec(name="display_name", dtype=pa.string()),
        ArrowFieldSpec(name="kind", dtype=pa.string()),
        ArrowFieldSpec(name="enclosing_symbol", dtype=pa.string()),
        ArrowFieldSpec(name="documentation", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="signature_documentation", dtype=SCIP_SIGNATURE_DOCUMENTATION_TYPE),
    ],
)

SCIP_SYMBOL_RELATIONSHIPS_SPEC = TableSchemaSpec(
    name="scip_symbol_relationships_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="relationship_id", dtype=pa.string()),
        ArrowFieldSpec(name="symbol", dtype=pa.string()),
        ArrowFieldSpec(name="related_symbol", dtype=pa.string()),
        ArrowFieldSpec(name="is_reference", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_implementation", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_type_definition", dtype=pa.bool_()),
        ArrowFieldSpec(name="is_definition", dtype=pa.bool_()),
    ],
)

SCIP_EXTERNAL_SYMBOL_INFO_SPEC = TableSchemaSpec(
    name="scip_external_symbol_info_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="symbol_info_id", dtype=pa.string()),
        ArrowFieldSpec(name="symbol", dtype=pa.string()),
        ArrowFieldSpec(name="display_name", dtype=pa.string()),
        ArrowFieldSpec(name="kind", dtype=pa.string()),
        ArrowFieldSpec(name="enclosing_symbol", dtype=pa.string()),
        ArrowFieldSpec(name="documentation", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="signature_documentation", dtype=SCIP_SIGNATURE_DOCUMENTATION_TYPE),
    ],
)

SCIP_DIAGNOSTICS_SPEC = TableSchemaSpec(
    name="scip_diagnostics_v1",
    fields=[
        ArrowFieldSpec(name="schema_version", dtype=pa.int32(), nullable=False),
        ArrowFieldSpec(name="diagnostic_id", dtype=pa.string()),
        ArrowFieldSpec(name="document_id", dtype=pa.string()),
        ArrowFieldSpec(name="path", dtype=pa.string()),
        ArrowFieldSpec(name="severity", dtype=pa.string()),
        ArrowFieldSpec(name="code", dtype=pa.string()),
        ArrowFieldSpec(name="message", dtype=pa.string()),
        ArrowFieldSpec(name="source", dtype=pa.string()),
        ArrowFieldSpec(name="tags", dtype=pa.list_(pa.string())),
        ArrowFieldSpec(name="start_line", dtype=pa.int32()),
        ArrowFieldSpec(name="start_char", dtype=pa.int32()),
        ArrowFieldSpec(name="end_line", dtype=pa.int32()),
        ArrowFieldSpec(name="end_char", dtype=pa.int32()),
    ],
)

SCIP_METADATA_SCHEMA = SCIP_METADATA_SPEC.to_arrow_schema()
SCIP_DOCUMENTS_SCHEMA = SCIP_DOCUMENTS_SPEC.to_arrow_schema()
SCIP_OCCURRENCES_SCHEMA = SCIP_OCCURRENCES_SPEC.to_arrow_schema()
SCIP_SYMBOL_INFO_SCHEMA = SCIP_SYMBOL_INFO_SPEC.to_arrow_schema()
SCIP_SYMBOL_RELATIONSHIPS_SCHEMA = SCIP_SYMBOL_RELATIONSHIPS_SPEC.to_arrow_schema()
SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA = SCIP_EXTERNAL_SYMBOL_INFO_SPEC.to_arrow_schema()
SCIP_DIAGNOSTICS_SCHEMA = SCIP_DIAGNOSTICS_SPEC.to_arrow_schema()


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


def _prefixed_hash(prefix: str, arrays: Sequence[pa.Array | pa.ChunkedArray]) -> pa.Array:
    hashed = hash64_from_arrays(arrays, prefix=prefix)
    return pc.binary_join_element_wise(pa.scalar(prefix), pc.cast(hashed, pa.string()), ":")


def _set_column(
    table: pa.Table,
    name: str,
    values: pa.Array | pa.ChunkedArray,
) -> pa.Table:
    if name in table.column_names:
        idx = table.schema.get_field_index(name)
        return table.set_column(idx, name, values)
    return table.append_column(name, values)


def _with_document_ids(table: pa.Table, *, path_col: str = "path") -> pa.Table:
    if table.num_rows == 0 or path_col not in table.column_names:
        return table
    ids = _prefixed_hash("scip_doc", [table[path_col]])
    return _set_column(table, "document_id", ids)


def add_scip_document_ids(table: pa.Table, *, path_col: str = "path") -> pa.Table:
    """Add document_id values derived from a path column.

    Returns
    -------
    pa.Table
        Updated table with a document_id column.
    """
    return _with_document_ids(table, path_col=path_col)


def _with_occurrence_ids(table: pa.Table) -> pa.Table:
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


def add_scip_occurrence_ids(table: pa.Table) -> pa.Table:
    """Add occurrence_id values derived from document/span columns.

    Returns
    -------
    pa.Table
        Updated table with an occurrence_id column.
    """
    return _with_occurrence_ids(table)


def _with_diagnostic_ids(table: pa.Table) -> pa.Table:
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


def _with_symbol_ids(table: pa.Table, *, prefix: str) -> pa.Table:
    if table.num_rows == 0:
        return table
    ids = _prefixed_hash(prefix, [table["symbol"]])
    return _set_column(table, "symbol_info_id", ids)


def _with_relationship_ids(table: pa.Table) -> pa.Table:
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


def _align_table(table: pa.Table, *, schema: pa.Schema) -> pa.Table:
    aligned, _ = align_to_schema(table, schema=schema, safe_cast=True, on_error="unsafe")
    return aligned


def _dictionary_encode(table: pa.Table, columns: Sequence[str]) -> pa.Table:
    for col in columns:
        if col not in table.column_names:
            continue
        idx = table.schema.get_field_index(col)
        encoded = pc.call_function("dictionary_encode", [table[col]])
        if isinstance(encoded, pa.Scalar):
            msg = f"Dictionary encoding returned scalar for column '{col}'."
            raise TypeError(msg)
        table = table.set_column(idx, col, encoded)
    return table


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


def _metadata_rows(index: object) -> list[Row]:
    metadata = getattr(index, "metadata", None)
    tool_info = getattr(metadata, "tool_info", None)
    tool_name = getattr(tool_info, "name", None)
    tool_version = getattr(tool_info, "version", None)
    project_root = getattr(metadata, "project_root", None)
    protocol_version = getattr(metadata, "protocol_version", None)
    text_document_encoding = getattr(metadata, "text_document_encoding", None)
    return [
        {
            "schema_version": SCHEMA_VERSION,
            "tool_name": tool_name,
            "tool_version": tool_version,
            "project_root": project_root,
            "text_document_encoding": str(text_document_encoding)
            if text_document_encoding is not None
            else None,
            "protocol_version": str(protocol_version) if protocol_version is not None else None,
        }
    ]


def _offsets_start() -> list[int]:
    return [0]


def _array(values: Sequence[object], data_type: pa.DataType) -> pa.Array:
    return pa.array(values, type=data_type)


@dataclass
class _DocAccumulator:
    schema_versions: list[int] = field(default_factory=list)
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
        self.schema_versions.append(SCHEMA_VERSION)
        self.document_ids.append(None)
        self.paths.append(rel_path)
        self.languages.append(str(language) if language is not None else None)
        self.position_encodings.append(
            str(position_encoding) if position_encoding is not None else None
        )

    def to_table(self) -> pa.Table:
        return pa.Table.from_arrays(
            [
                _array(self.schema_versions, pa.int32()),
                _array(self.document_ids, pa.string()),
                _array(self.paths, pa.string()),
                _array(self.languages, pa.string()),
                _array(self.position_encodings, pa.string()),
            ],
            schema=SCIP_DOCUMENTS_SCHEMA,
        )


@dataclass
class _OccurrenceAccumulator:
    schema_versions: list[int] = field(default_factory=list)
    occurrence_ids: list[str | None] = field(default_factory=list)
    document_ids: list[str | None] = field(default_factory=list)
    paths: list[str | None] = field(default_factory=list)
    symbols: list[str | None] = field(default_factory=list)
    symbol_roles: list[int] = field(default_factory=list)
    syntax_kind: list[str | None] = field(default_factory=list)
    override_doc_offsets: list[int] = field(default_factory=_offsets_start)
    override_doc_values: list[str | None] = field(default_factory=list)
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

        self.schema_versions.append(SCHEMA_VERSION)
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
        for doc in override_docs:
            if doc is None:
                self.override_doc_values.append(None)
            elif isinstance(doc, str):
                self.override_doc_values.append(doc)
            else:
                self.override_doc_values.append(str(doc))
        self.override_doc_offsets.append(len(self.override_doc_values))
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

    def to_table(self) -> pa.Table:
        override_doc = build_list_array(
            pa.array(self.override_doc_offsets, type=pa.int32()),
            pa.array(self.override_doc_values, type=pa.string()),
        )
        return pa.Table.from_arrays(
            [
                _array(self.schema_versions, pa.int32()),
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


@dataclass
class _DiagnosticAccumulator:
    schema_versions: list[int] = field(default_factory=list)
    diagnostic_ids: list[str | None] = field(default_factory=list)
    document_ids: list[str | None] = field(default_factory=list)
    paths: list[str | None] = field(default_factory=list)
    severity: list[str | None] = field(default_factory=list)
    code: list[str | None] = field(default_factory=list)
    message: list[str | None] = field(default_factory=list)
    source: list[str | None] = field(default_factory=list)
    tags_offsets: list[int] = field(default_factory=_offsets_start)
    tags_values: list[str | None] = field(default_factory=list)
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

        self.schema_versions.append(SCHEMA_VERSION)
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
        for tag in tags:
            if tag is None:
                self.tags_values.append(None)
            elif isinstance(tag, str):
                self.tags_values.append(tag)
            else:
                self.tags_values.append(str(tag))
        self.tags_offsets.append(len(self.tags_values))
        self.start_line.append(dsl)
        self.start_char.append(dsc)
        self.end_line.append(del_)
        self.end_char.append(dec_)

    def to_table(self) -> pa.Table:
        tags = build_list_array(
            pa.array(self.tags_offsets, type=pa.int32()),
            pa.array(self.tags_values, type=pa.string()),
        )
        return pa.Table.from_arrays(
            [
                _array(self.schema_versions, pa.int32()),
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


@dataclass
class _SymbolInfoAccumulator:
    schema_versions: list[int] = field(default_factory=list)
    symbol_info_ids: list[str | None] = field(default_factory=list)
    symbols: list[str | None] = field(default_factory=list)
    display_names: list[str | None] = field(default_factory=list)
    kinds: list[str | None] = field(default_factory=list)
    enclosing_symbols: list[str | None] = field(default_factory=list)
    documentation_offsets: list[int] = field(default_factory=_offsets_start)
    documentation_values: list[str | None] = field(default_factory=list)
    sig_doc_texts: list[str | None] = field(default_factory=list)
    sig_doc_languages: list[str | None] = field(default_factory=list)
    sig_doc_is_null: list[bool] = field(default_factory=list)
    sig_doc_occurrence_offsets: list[int] = field(default_factory=_offsets_start)
    sig_doc_occurrence_symbols: list[str | None] = field(default_factory=list)
    sig_doc_occurrence_roles: list[int] = field(default_factory=list)
    sig_doc_occurrence_range_offsets: list[int] = field(default_factory=_offsets_start)
    sig_doc_occurrence_range_values: list[int] = field(default_factory=list)

    def append(self, symbol_info: object) -> None:
        self.schema_versions.append(SCHEMA_VERSION)
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
        for doc in docs:
            if doc is None:
                self.documentation_values.append(None)
            elif isinstance(doc, str):
                self.documentation_values.append(doc)
            else:
                self.documentation_values.append(str(doc))
        self.documentation_offsets.append(len(self.documentation_values))

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
        self.sig_doc_occurrence_offsets.append(len(self.sig_doc_occurrence_symbols))

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
        for occ in occurrences:
            self._append_signature_doc_occurrence(occ)
        self.sig_doc_occurrence_offsets.append(len(self.sig_doc_occurrence_symbols))

    def _append_signature_doc_occurrence(self, occ: object) -> None:
        self.sig_doc_occurrence_symbols.append(getattr(occ, "symbol", None))
        self.sig_doc_occurrence_roles.append(int(getattr(occ, "symbol_roles", 0) or 0))
        for value in getattr(occ, "range", []) or []:
            if isinstance(value, bool):
                continue
            if isinstance(value, int) or (isinstance(value, str) and value.isdigit()):
                self.sig_doc_occurrence_range_values.append(int(value))
        self.sig_doc_occurrence_range_offsets.append(len(self.sig_doc_occurrence_range_values))

    def to_table(self, *, schema: pa.Schema) -> pa.Table:
        documentation = build_list_array(
            pa.array(self.documentation_offsets, type=pa.int32()),
            pa.array(self.documentation_values, type=pa.string()),
        )

        occ_range = build_list_array(
            pa.array(self.sig_doc_occurrence_range_offsets, type=pa.int32()),
            pa.array(self.sig_doc_occurrence_range_values, type=pa.int32()),
        )
        occurrences = build_list_array(
            pa.array(self.sig_doc_occurrence_offsets, type=pa.int32()),
            build_struct_array(
                {
                    "symbol": pa.array(self.sig_doc_occurrence_symbols, type=pa.string()),
                    "symbol_roles": pa.array(self.sig_doc_occurrence_roles, type=pa.int32()),
                    "range": occ_range,
                }
            ),
        )
        sig_doc = build_struct_array(
            {
                "text": pa.array(self.sig_doc_texts, type=pa.string()),
                "language": pa.array(self.sig_doc_languages, type=pa.string()),
                "occurrences": occurrences,
            },
            mask=pa.array(self.sig_doc_is_null, type=pa.bool_()),
        )
        return pa.Table.from_arrays(
            [
                _array(self.schema_versions, pa.int32()),
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


@dataclass
class _RelationshipAccumulator:
    schema_versions: list[int] = field(default_factory=list)
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
        self.schema_versions.append(SCHEMA_VERSION)
        self.relationship_ids.append(None)
        self.symbols.append(symbol)
        self.related_symbols.append(related)
        self.is_reference.append(bool(getattr(relationship, "is_reference", False)))
        self.is_implementation.append(bool(getattr(relationship, "is_implementation", False)))
        self.is_type_definition.append(bool(getattr(relationship, "is_type_definition", False)))
        self.is_definition.append(bool(getattr(relationship, "is_definition", False)))

    def to_table(self) -> pa.Table:
        return pa.Table.from_arrays(
            [
                _array(self.schema_versions, pa.int32()),
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


def _document_tables(index: object) -> tuple[pa.Table, pa.Table, pa.Table]:
    docs = _DocAccumulator()
    occs = _OccurrenceAccumulator()
    diags = _DiagnosticAccumulator()

    for doc in getattr(index, "documents", []):
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

    return docs.to_table(), occs.to_table(), diags.to_table()


def _symbol_tables(index: object) -> tuple[pa.Table, pa.Table, pa.Table]:
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


def _extract_tables_from_index(index: object, *, parse_opts: SCIPParseOptions) -> SCIPExtractResult:
    meta_rows = _metadata_rows(index)
    t_docs, t_occs, t_diags = _document_tables(index)

    t_meta = pa.Table.from_pylist(meta_rows, schema=SCIP_METADATA_SCHEMA)
    t_syms, t_rels, t_ext = _symbol_tables(index)

    t_docs = _with_document_ids(t_docs)
    t_occs = _with_document_ids(t_occs)
    t_occs = _with_occurrence_ids(t_occs)
    t_diags = _with_document_ids(t_diags)
    t_diags = _with_diagnostic_ids(t_diags)
    t_syms = _with_symbol_ids(t_syms, prefix="scip_sym")
    t_rels = _with_relationship_ids(t_rels)
    t_ext = _with_symbol_ids(t_ext, prefix="scip_ext_sym")

    t_meta = _align_table(t_meta, schema=SCIP_METADATA_SCHEMA)
    t_docs = _align_table(t_docs, schema=SCIP_DOCUMENTS_SCHEMA)
    t_occs = _align_table(t_occs, schema=SCIP_OCCURRENCES_SCHEMA)
    t_syms = _align_table(t_syms, schema=SCIP_SYMBOL_INFO_SCHEMA)
    t_rels = _align_table(t_rels, schema=SCIP_SYMBOL_RELATIONSHIPS_SCHEMA)
    t_ext = _align_table(t_ext, schema=SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA)
    t_diags = _align_table(t_diags, schema=SCIP_DIAGNOSTICS_SCHEMA)

    if parse_opts.dictionary_encode_strings:
        t_meta = _dictionary_encode(
            t_meta,
            [
                "tool_name",
                "tool_version",
                "project_root",
                "text_document_encoding",
                "protocol_version",
            ],
        )
        t_docs = _dictionary_encode(t_docs, ["path", "language", "position_encoding"])
        t_occs = _dictionary_encode(t_occs, ["path", "symbol", "syntax_kind"])
        t_syms = _dictionary_encode(
            t_syms,
            ["symbol", "display_name", "kind", "enclosing_symbol"],
        )
        t_rels = _dictionary_encode(t_rels, ["symbol", "related_symbol"])
        t_ext = _dictionary_encode(
            t_ext,
            ["symbol", "display_name", "kind", "enclosing_symbol"],
        )
        t_diags = _dictionary_encode(t_diags, ["path", "severity", "code", "source"])

    return SCIPExtractResult(
        scip_metadata=t_meta,
        scip_documents=t_docs,
        scip_occurrences=t_occs,
        scip_symbol_information=t_syms,
        scip_symbol_relationships=t_rels,
        scip_external_symbol_information=t_ext,
        scip_diagnostics=t_diags,
    )


def extract_scip_tables(
    *,
    scip_index_path: str | None,
    repo_root: str | None,
    ctx: object | None = None,
    parse_opts: SCIPParseOptions | None = None,
) -> dict[str, pa.Table]:
    """Extract SCIP tables as a name-keyed bundle.

    Parameters
    ----------
    scip_index_path:
        Path to the index.scip file or ``None``.
    repo_root:
        Optional repository root used for resolving relative paths.
    ctx:
        Execution context (unused).
    parse_opts:
        Parsing options.

    Returns
    -------
    dict[str, pyarrow.Table]
        Extracted SCIP tables keyed by output name.
    """
    _ = ctx
    parse_opts = parse_opts or SCIPParseOptions()
    if scip_index_path is None:
        empty_result = SCIPExtractResult(
            scip_metadata=empty_table(SCIP_METADATA_SCHEMA),
            scip_documents=empty_table(SCIP_DOCUMENTS_SCHEMA),
            scip_occurrences=empty_table(SCIP_OCCURRENCES_SCHEMA),
            scip_symbol_information=empty_table(SCIP_SYMBOL_INFO_SCHEMA),
            scip_symbol_relationships=empty_table(SCIP_SYMBOL_RELATIONSHIPS_SCHEMA),
            scip_external_symbol_information=empty_table(SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA),
            scip_diagnostics=empty_table(SCIP_DIAGNOSTICS_SCHEMA),
        )
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
        empty_result = _extract_tables_from_index(index, parse_opts=parse_opts)

    return {
        "scip_metadata": empty_result.scip_metadata,
        "scip_documents": empty_result.scip_documents,
        "scip_occurrences": empty_result.scip_occurrences,
        "scip_symbol_information": empty_result.scip_symbol_information,
        "scip_symbol_relationships": empty_result.scip_symbol_relationships,
        "scip_external_symbol_information": empty_result.scip_external_symbol_information,
        "scip_diagnostics": empty_result.scip_diagnostics,
    }
