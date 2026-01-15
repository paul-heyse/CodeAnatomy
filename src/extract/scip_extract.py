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
from typing import TYPE_CHECKING, Literal, overload

import pyarrow as pa

from arrowdsl.compute.expr_core import ENC_UTF8, ENC_UTF16, ENC_UTF32
from arrowdsl.compute.macros import normalize_string_items
from arrowdsl.core.context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import (
    ArrayLike,
    DataTypeLike,
    RecordBatchReaderLike,
    SchemaLike,
    TableLike,
)
from arrowdsl.plan.plan import Plan
from arrowdsl.plan.query import QuerySpec
from arrowdsl.plan.runner import materialize_plan, run_plan_bundle
from arrowdsl.schema.build import struct_array_from_dicts, table_from_arrays
from arrowdsl.schema.nested_builders import LargeListAccumulator
from arrowdsl.schema.schema import SchemaMetadataSpec, empty_table, unify_tables
from extract.plan_helpers import apply_evidence_projection, plan_from_rows
from extract.registry_fields import (
    SCIP_SIGNATURE_DOCUMENTATION_TYPE,
)
from extract.registry_specs import (
    dataset_query,
    dataset_row_schema,
    dataset_schema,
    dataset_schema_policy,
    normalize_options,
    postprocess_table,
)
from extract.schema_ops import metadata_spec_for_dataset, normalize_plan_with_policy

if TYPE_CHECKING:
    from arrowdsl.schema.policy import SchemaPolicy
    from extract.evidence_plan import EvidencePlan
from extract.scip_parse_json import parse_index_json
from extract.scip_proto_loader import load_scip_pb2_from_build
from schema_spec.specs import NestedFieldSpec

RANGE_LEN_SHORT = 3
RANGE_LEN_FULL = 4

LOGGER = logging.getLogger(__name__)

type Row = dict[str, object]

SCIP_LINE_BASE = 0
SCIP_END_EXCLUSIVE = True


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


SIG_DOC_OCCURRENCE_KEYS = ("symbol", "symbol_roles", "range")


def _signature_doc_occurrence_rows(sig_doc: object) -> list[dict[str, object]]:
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
    return rows


def _signature_doc_entry(sig_doc: object | None) -> dict[str, object] | None:
    if sig_doc is None:
        return None
    text = getattr(sig_doc, "text", None)
    if text is not None and not isinstance(text, str):
        text = str(text)
    language = getattr(sig_doc, "language", None)
    return {
        "text": text,
        "language": str(language) if language is not None else None,
        "occurrences": _signature_doc_occurrence_rows(sig_doc),
    }


SCIP_METADATA_QUERY = dataset_query("scip_metadata_v1")
SCIP_DOCUMENTS_QUERY = dataset_query("scip_documents_v1")
SCIP_OCCURRENCES_QUERY = dataset_query("scip_occurrences_v1")
SCIP_SYMBOL_INFO_QUERY = dataset_query("scip_symbol_info_v1")
SCIP_SYMBOL_RELATIONSHIPS_QUERY = dataset_query("scip_symbol_relationships_v1")
SCIP_EXTERNAL_SYMBOL_INFO_QUERY = dataset_query("scip_external_symbol_info_v1")
SCIP_DIAGNOSTICS_QUERY = dataset_query("scip_diagnostics_v1")

SCIP_METADATA_SCHEMA = dataset_schema("scip_metadata_v1")
SCIP_DOCUMENTS_SCHEMA = dataset_schema("scip_documents_v1")
SCIP_OCCURRENCES_SCHEMA = dataset_schema("scip_occurrences_v1")
SCIP_SYMBOL_INFO_SCHEMA = dataset_schema("scip_symbol_info_v1")
SCIP_SYMBOL_RELATIONSHIPS_SCHEMA = dataset_schema("scip_symbol_relationships_v1")
SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA = dataset_schema("scip_external_symbol_info_v1")
SCIP_DIAGNOSTICS_SCHEMA = dataset_schema("scip_diagnostics_v1")

SCIP_METADATA_ROW_SCHEMA = dataset_row_schema("scip_metadata_v1")


def _scip_metadata_specs(
    parse_opts: SCIPParseOptions,
) -> dict[str, SchemaMetadataSpec]:
    return {
        "scip_metadata": metadata_spec_for_dataset("scip_metadata_v1", options=parse_opts),
        "scip_documents": metadata_spec_for_dataset("scip_documents_v1", options=parse_opts),
        "scip_occurrences": metadata_spec_for_dataset("scip_occurrences_v1", options=parse_opts),
        "scip_symbol_information": metadata_spec_for_dataset(
            "scip_symbol_info_v1", options=parse_opts
        ),
        "scip_symbol_relationships": metadata_spec_for_dataset(
            "scip_symbol_relationships_v1", options=parse_opts
        ),
        "scip_external_symbol_information": metadata_spec_for_dataset(
            "scip_external_symbol_info_v1", options=parse_opts
        ),
        "scip_diagnostics": metadata_spec_for_dataset("scip_diagnostics_v1", options=parse_opts),
    }


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


def _col_unit_from_position_encoding(value: object | None) -> str:
    if isinstance(value, int):
        return _encoding_from_int(value)
    if isinstance(value, str):
        return _encoding_from_text(value)
    return "utf32"


def _encoding_from_int(value: int) -> str:
    encoding_map: dict[int, str] = {
        ENC_UTF8: "utf8",
        ENC_UTF16: "utf16",
        ENC_UTF32: "utf32",
    }
    return encoding_map.get(value, "utf32")


def _encoding_from_text(value: str) -> str:
    text = value.strip().upper()
    if "UTF8" in text:
        return "utf8"
    if "UTF16" in text:
        return "utf16"
    if "UTF32" in text:
        return "utf32"
    return "utf32"


def _string_list_accumulator() -> LargeListAccumulator[str | None]:
    return LargeListAccumulator()


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
    normalized_opts = normalize_options("scip", parse_opts, SCIPParseOptions)
    if normalized_opts.build_dir is None:
        normalized_opts = replace(normalized_opts, build_dir=index_path.parent)
    scip_pb2 = _load_scip_pb2(normalized_opts)
    if scip_pb2 is not None and hasattr(scip_pb2, "Index") and normalized_opts.prefer_protobuf:
        return _parse_index_protobuf(index_path, scip_pb2)
    if normalized_opts.allow_json_fallback:
        return parse_index_json(index_path, normalized_opts.scip_cli_bin)
    if scip_pb2 is not None and hasattr(scip_pb2, "Index"):
        return _parse_index_protobuf(index_path, scip_pb2)
    msg = (
        "SCIP protobuf bindings not available.\n"
        "Run scripts/scip_proto_codegen.py to generate build/scip/scip_pb2.py."
    )
    raise RuntimeError(msg)


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
        columns = {
            "document_id": _array(self.document_ids, pa.string()),
            "path": _array(self.paths, pa.string()),
            "language": _array(self.languages, pa.string()),
            "position_encoding": _array(self.position_encodings, pa.string()),
        }
        return table_from_arrays(SCIP_DOCUMENTS_SCHEMA, columns=columns, num_rows=len(self.paths))


@dataclass
class _OccurrenceAccumulator:
    occurrence_ids: list[str | None] = field(default_factory=list)
    document_ids: list[str | None] = field(default_factory=list)
    paths: list[str | None] = field(default_factory=list)
    symbols: list[str | None] = field(default_factory=list)
    symbol_roles: list[int] = field(default_factory=list)
    syntax_kind: list[str | None] = field(default_factory=list)
    override_docs: LargeListAccumulator[str | None] = field(
        default_factory=_string_list_accumulator
    )
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
    line_base: list[int] = field(default_factory=list)
    col_unit: list[str] = field(default_factory=list)
    end_exclusive: list[bool] = field(default_factory=list)

    def append_from_occurrence(
        self,
        occurrence: object,
        rel_path: str | None,
        *,
        col_unit: str,
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
        self.override_docs.append(normalize_string_items(override_docs))
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
        self.line_base.append(SCIP_LINE_BASE)
        self.col_unit.append(col_unit)
        self.end_exclusive.append(SCIP_END_EXCLUSIVE)
        return default_range

    def to_table(self) -> TableLike:
        override_doc = OVERRIDE_DOC_SPEC.builder(self)
        columns = {
            "occurrence_id": _array(self.occurrence_ids, pa.string()),
            "document_id": _array(self.document_ids, pa.string()),
            "path": _array(self.paths, pa.string()),
            "symbol": _array(self.symbols, pa.string()),
            "symbol_roles": _array(self.symbol_roles, pa.int32()),
            "syntax_kind": _array(self.syntax_kind, pa.string()),
            "override_documentation": override_doc,
            "start_line": _array(self.start_line, pa.int32()),
            "start_char": _array(self.start_char, pa.int32()),
            "end_line": _array(self.end_line, pa.int32()),
            "end_char": _array(self.end_char, pa.int32()),
            "range_len": _array(self.range_len, pa.int32()),
            "enc_start_line": _array(self.enc_start_line, pa.int32()),
            "enc_start_char": _array(self.enc_start_char, pa.int32()),
            "enc_end_line": _array(self.enc_end_line, pa.int32()),
            "enc_end_char": _array(self.enc_end_char, pa.int32()),
            "enc_range_len": _array(self.enc_range_len, pa.int32()),
            "line_base": _array(self.line_base, pa.int32()),
            "col_unit": _array(self.col_unit, pa.string()),
            "end_exclusive": _array(self.end_exclusive, pa.bool_()),
        }
        return table_from_arrays(
            SCIP_OCCURRENCES_SCHEMA,
            columns=columns,
            num_rows=len(self.paths),
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
    tags: LargeListAccumulator[str | None] = field(default_factory=_string_list_accumulator)
    start_line: list[int | None] = field(default_factory=list)
    start_char: list[int | None] = field(default_factory=list)
    end_line: list[int | None] = field(default_factory=list)
    end_char: list[int | None] = field(default_factory=list)
    line_base: list[int] = field(default_factory=list)
    col_unit: list[str] = field(default_factory=list)
    end_exclusive: list[bool] = field(default_factory=list)

    def append_from_diag(
        self,
        diag: object,
        rel_path: str | None,
        default_range: tuple[int, int, int, int] | None,
        *,
        col_unit: str,
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
        self.tags.append(normalize_string_items(tags))
        self.start_line.append(dsl)
        self.start_char.append(dsc)
        self.end_line.append(del_)
        self.end_char.append(dec_)
        self.line_base.append(SCIP_LINE_BASE)
        self.col_unit.append(col_unit)
        self.end_exclusive.append(SCIP_END_EXCLUSIVE)

    def to_table(self) -> TableLike:
        tags = DIAG_TAGS_SPEC.builder(self)
        columns = {
            "diagnostic_id": _array(self.diagnostic_ids, pa.string()),
            "document_id": _array(self.document_ids, pa.string()),
            "path": _array(self.paths, pa.string()),
            "severity": _array(self.severity, pa.string()),
            "code": _array(self.code, pa.string()),
            "message": _array(self.message, pa.string()),
            "source": _array(self.source, pa.string()),
            "tags": tags,
            "start_line": _array(self.start_line, pa.int32()),
            "start_char": _array(self.start_char, pa.int32()),
            "end_line": _array(self.end_line, pa.int32()),
            "end_char": _array(self.end_char, pa.int32()),
            "line_base": _array(self.line_base, pa.int32()),
            "col_unit": _array(self.col_unit, pa.string()),
            "end_exclusive": _array(self.end_exclusive, pa.bool_()),
        }
        return table_from_arrays(
            SCIP_DIAGNOSTICS_SCHEMA,
            columns=columns,
            num_rows=len(self.paths),
        )


def _build_diag_tags(acc: _DiagnosticAccumulator) -> ArrayLike:
    return acc.tags.build(value_type=pa.string())


DIAG_TAGS_SPEC = NestedFieldSpec(
    name="tags",
    dtype=pa.large_list(pa.string()),
    builder=_build_diag_tags,
)


@dataclass
class _SymbolInfoAccumulator:
    symbol_info_ids: list[str | None] = field(default_factory=list)
    symbols: list[str | None] = field(default_factory=list)
    display_names: list[str | None] = field(default_factory=list)
    kinds: list[str | None] = field(default_factory=list)
    enclosing_symbols: list[str | None] = field(default_factory=list)
    documentation: LargeListAccumulator[str | None] = field(
        default_factory=_string_list_accumulator
    )
    sig_doc_entries: list[dict[str, object] | None] = field(default_factory=list)

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
        self.sig_doc_entries.append(_signature_doc_entry(sig_doc))

    def _append_documentation(self, symbol_info: object) -> None:
        docs = getattr(symbol_info, "documentation", None)
        if not hasattr(symbol_info, "documentation") or docs is None:
            docs = []
        self.documentation.append(normalize_string_items(docs))

    def to_table(self, *, schema: SchemaLike) -> TableLike:
        documentation = SYMBOL_DOC_SPEC.builder(self)
        sig_doc = SIG_DOC_SPEC.builder(self)
        columns = {
            "symbol_info_id": _array(self.symbol_info_ids, pa.string()),
            "symbol": _array(self.symbols, pa.string()),
            "display_name": _array(self.display_names, pa.string()),
            "kind": _array(self.kinds, pa.string()),
            "enclosing_symbol": _array(self.enclosing_symbols, pa.string()),
            "documentation": documentation,
            "signature_documentation": sig_doc,
        }
        return table_from_arrays(schema, columns=columns, num_rows=len(self.symbols))


def _build_symbol_docs(acc: _SymbolInfoAccumulator) -> ArrayLike:
    return acc.documentation.build(value_type=pa.string())


def _build_signature_doc(acc: _SymbolInfoAccumulator) -> ArrayLike:
    return struct_array_from_dicts(
        acc.sig_doc_entries,
        struct_type=SCIP_SIGNATURE_DOCUMENTATION_TYPE,
    )


SYMBOL_DOC_SPEC = NestedFieldSpec(
    name="documentation",
    dtype=pa.large_list(pa.string()),
    builder=_build_symbol_docs,
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
        columns = {
            "relationship_id": _array(self.relationship_ids, pa.string()),
            "symbol": _array(self.symbols, pa.string()),
            "related_symbol": _array(self.related_symbols, pa.string()),
            "is_reference": _array(self.is_reference, pa.bool_()),
            "is_implementation": _array(self.is_implementation, pa.bool_()),
            "is_type_definition": _array(self.is_type_definition, pa.bool_()),
            "is_definition": _array(self.is_definition, pa.bool_()),
        }
        return table_from_arrays(
            SCIP_SYMBOL_RELATIONSHIPS_SCHEMA,
            columns=columns,
            num_rows=len(self.symbols),
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
        position_encoding = getattr(doc, "position_encoding", None)
        docs.append(rel_path, getattr(doc, "language", None), position_encoding)
        col_unit = _col_unit_from_position_encoding(position_encoding)

        for occ in getattr(doc, "occurrences", []):
            default_range = occs.append_from_occurrence(occ, rel_path, col_unit=col_unit)
            for diag in getattr(occ, "diagnostics", []):
                diags.append_from_diag(diag, rel_path, default_range, col_unit=col_unit)

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


@dataclass(frozen=True)
class _ScipFinalizeSpec:
    dataset_name: str
    policy: SchemaPolicy
    query: QuerySpec


def _finalize_plan(
    plan: Plan,
    *,
    spec: _ScipFinalizeSpec,
    exec_ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> Plan:
    plan = spec.query.apply_to_plan(plan, ctx=exec_ctx)
    plan = apply_evidence_projection(
        spec.dataset_name,
        plan,
        ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    return normalize_plan_with_policy(plan, policy=spec.policy, ctx=exec_ctx)


def _finalize_table(
    table: TableLike,
    *,
    spec: _ScipFinalizeSpec,
    exec_ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> TableLike:
    plan = _finalize_plan(
        Plan.table_source(table),
        spec=spec,
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    return materialize_plan(plan, ctx=exec_ctx)


def _extract_tables_from_index(
    index: object,
    *,
    parse_opts: SCIPParseOptions,
    exec_ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> SCIPExtractResult:
    plans = _extract_plan_bundle_from_index(
        index,
        parse_opts=parse_opts,
        exec_ctx=exec_ctx,
        evidence_plan=evidence_plan,
    )
    metadata_specs = _scip_metadata_specs(parse_opts)
    return SCIPExtractResult(
        scip_metadata=materialize_plan(
            plans["scip_metadata"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_metadata"],
            attach_ordering_metadata=True,
        ),
        scip_documents=materialize_plan(
            plans["scip_documents"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_documents"],
            attach_ordering_metadata=True,
        ),
        scip_occurrences=materialize_plan(
            plans["scip_occurrences"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_occurrences"],
            attach_ordering_metadata=True,
        ),
        scip_symbol_information=materialize_plan(
            plans["scip_symbol_information"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_symbol_information"],
            attach_ordering_metadata=True,
        ),
        scip_symbol_relationships=materialize_plan(
            plans["scip_symbol_relationships"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_symbol_relationships"],
            attach_ordering_metadata=True,
        ),
        scip_external_symbol_information=materialize_plan(
            plans["scip_external_symbol_information"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_external_symbol_information"],
            attach_ordering_metadata=True,
        ),
        scip_diagnostics=materialize_plan(
            plans["scip_diagnostics"],
            ctx=exec_ctx,
            metadata_spec=metadata_specs["scip_diagnostics"],
            attach_ordering_metadata=True,
        ),
    )


def _scip_policies(
    parse_opts: SCIPParseOptions,
    exec_ctx: ExecutionContext,
) -> dict[str, SchemaPolicy]:
    encode = parse_opts.dictionary_encode_strings
    return {
        "scip_metadata_v1": dataset_schema_policy(
            "scip_metadata_v1",
            ctx=exec_ctx,
            options=parse_opts,
            enable_encoding=encode,
        ),
        "scip_documents_v1": dataset_schema_policy(
            "scip_documents_v1",
            ctx=exec_ctx,
            options=parse_opts,
            enable_encoding=encode,
        ),
        "scip_occurrences_v1": dataset_schema_policy(
            "scip_occurrences_v1",
            ctx=exec_ctx,
            options=parse_opts,
            enable_encoding=encode,
        ),
        "scip_symbol_info_v1": dataset_schema_policy(
            "scip_symbol_info_v1",
            ctx=exec_ctx,
            options=parse_opts,
            enable_encoding=encode,
        ),
        "scip_symbol_relationships_v1": dataset_schema_policy(
            "scip_symbol_relationships_v1",
            ctx=exec_ctx,
            options=parse_opts,
            enable_encoding=encode,
        ),
        "scip_external_symbol_info_v1": dataset_schema_policy(
            "scip_external_symbol_info_v1",
            ctx=exec_ctx,
            options=parse_opts,
            enable_encoding=encode,
        ),
        "scip_diagnostics_v1": dataset_schema_policy(
            "scip_diagnostics_v1",
            ctx=exec_ctx,
            options=parse_opts,
            enable_encoding=encode,
        ),
    }


def _extract_plan_bundle_from_index(
    index: object,
    *,
    parse_opts: SCIPParseOptions,
    exec_ctx: ExecutionContext,
    evidence_plan: EvidencePlan | None = None,
) -> dict[str, Plan]:
    meta_rows = _metadata_rows(index, parse_opts=parse_opts)
    t_docs, t_occs, t_diags = _document_tables(index)

    t_meta_plan = plan_from_rows(
        meta_rows,
        schema=SCIP_METADATA_ROW_SCHEMA,
        label="scip_metadata",
    )
    t_syms, t_rels, t_ext = _symbol_tables(index)

    t_docs = postprocess_table("scip_documents_v1", t_docs)
    t_occs = postprocess_table("scip_occurrences_v1", t_occs)
    t_diags = postprocess_table("scip_diagnostics_v1", t_diags)
    t_syms = postprocess_table("scip_symbol_info_v1", t_syms)
    t_rels = postprocess_table("scip_symbol_relationships_v1", t_rels)
    t_ext = postprocess_table("scip_external_symbol_info_v1", t_ext)
    policies = _scip_policies(parse_opts, exec_ctx=exec_ctx)

    def _spec(name: str, query: QuerySpec) -> _ScipFinalizeSpec:
        return _ScipFinalizeSpec(
            dataset_name=name,
            policy=policies[name],
            query=query,
        )

    return {
        "scip_metadata": _finalize_plan(
            t_meta_plan,
            spec=_spec("scip_metadata_v1", SCIP_METADATA_QUERY),
            exec_ctx=exec_ctx,
            evidence_plan=evidence_plan,
        ),
        "scip_documents": _finalize_plan(
            Plan.table_source(t_docs),
            spec=_spec("scip_documents_v1", SCIP_DOCUMENTS_QUERY),
            exec_ctx=exec_ctx,
            evidence_plan=evidence_plan,
        ),
        "scip_occurrences": _finalize_plan(
            Plan.table_source(t_occs),
            spec=_spec("scip_occurrences_v1", SCIP_OCCURRENCES_QUERY),
            exec_ctx=exec_ctx,
            evidence_plan=evidence_plan,
        ),
        "scip_symbol_information": _finalize_plan(
            Plan.table_source(t_syms),
            spec=_spec("scip_symbol_info_v1", SCIP_SYMBOL_INFO_QUERY),
            exec_ctx=exec_ctx,
            evidence_plan=evidence_plan,
        ),
        "scip_symbol_relationships": _finalize_plan(
            Plan.table_source(t_rels),
            spec=_spec("scip_symbol_relationships_v1", SCIP_SYMBOL_RELATIONSHIPS_QUERY),
            exec_ctx=exec_ctx,
            evidence_plan=evidence_plan,
        ),
        "scip_external_symbol_information": _finalize_plan(
            Plan.table_source(t_ext),
            spec=_spec("scip_external_symbol_info_v1", SCIP_EXTERNAL_SYMBOL_INFO_QUERY),
            exec_ctx=exec_ctx,
            evidence_plan=evidence_plan,
        ),
        "scip_diagnostics": _finalize_plan(
            Plan.table_source(t_diags),
            spec=_spec("scip_diagnostics_v1", SCIP_DIAGNOSTICS_QUERY),
            exec_ctx=exec_ctx,
            evidence_plan=evidence_plan,
        ),
    }


@dataclass(frozen=True)
class ScipExtractOptions:
    """Options for SCIP extraction table materialization."""

    parse_opts: SCIPParseOptions | None = None
    evidence_plan: EvidencePlan | None = None
    profile: str = "default"


@dataclass(frozen=True)
class ScipExtractContext:
    """Execution context inputs for SCIP extraction."""

    scip_index_path: str | None
    repo_root: str | None
    ctx: ExecutionContext | None = None
    profile: str = "default"

    def ensure_ctx(self) -> ExecutionContext:
        """Return the effective execution context.

        Returns
        -------
        ExecutionContext
            Provided context or a profile-derived context when missing.
        """
        if self.ctx is not None:
            return self.ctx
        return execution_context_factory(self.profile)


@overload
def extract_scip_tables(
    *,
    context: ScipExtractContext,
    options: ScipExtractOptions | None = None,
    prefer_reader: Literal[False] = False,
) -> Mapping[str, TableLike]: ...


@overload
def extract_scip_tables(
    *,
    context: ScipExtractContext,
    options: ScipExtractOptions | None = None,
    prefer_reader: Literal[True],
) -> Mapping[str, TableLike | RecordBatchReaderLike]: ...


def extract_scip_tables(
    *,
    context: ScipExtractContext,
    options: ScipExtractOptions | None = None,
    prefer_reader: bool = False,
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Extract SCIP tables as a name-keyed bundle.

    Parameters
    ----------
    context:
        Execution context bundle for SCIP extraction.
    options:
        Extraction options including parsing and evidence settings.
    prefer_reader:
        When True, return streaming readers when possible.

    Returns
    -------
    dict[str, TableLike | RecordBatchReaderLike]
        Extracted SCIP outputs keyed by output name.
    """
    options = options or ScipExtractOptions()
    normalized_opts = normalize_options("scip", options.parse_opts, SCIPParseOptions)
    evidence_plan = options.evidence_plan
    exec_ctx = context.ensure_ctx()
    scip_index_path = context.scip_index_path
    repo_root = context.repo_root
    metadata_specs = _scip_metadata_specs(normalized_opts)
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
        index = parse_index_scip(index_path, parse_opts=normalized_opts)
        if normalized_opts.log_counts or normalized_opts.health_check:
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
        if normalized_opts.health_check:
            assert_scip_index_health(index)
        plans = _extract_plan_bundle_from_index(
            index,
            parse_opts=normalized_opts,
            exec_ctx=exec_ctx,
            evidence_plan=evidence_plan,
        )

    return run_plan_bundle(
        plans,
        ctx=exec_ctx,
        prefer_reader=prefer_reader,
        metadata_specs=metadata_specs,
        attach_ordering_metadata=True,
    )
