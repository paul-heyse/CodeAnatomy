"""Extract SCIP index data into nested Arrow tables using shared helpers."""

from __future__ import annotations

import importlib
import logging
import os
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Literal, overload

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.ids import stable_id
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from extract.helpers import (
    ExtractMaterializeOptions,
    SpanSpec,
    apply_query_and_project,
    attrs_map,
    ibis_plan_from_rows,
    materialize_extract_plan,
    span_dict,
)
from extract.registry_specs import dataset_schema, normalize_options
from extract.schema_ops import ExtractNormalizeOptions
from extract.string_utils import normalize_string_items
from ibis_engine.plan import IbisPlan

if TYPE_CHECKING:
    from extract.evidence_plan import EvidencePlan
from extract.scip_proto_loader import load_scip_pb2_from_build

ENC_UTF8 = 1
ENC_UTF16 = 2
ENC_UTF32 = 3

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
    scip_pb2_import: str | None = None
    build_dir: Path | None = None
    health_check: bool = False
    log_counts: bool = False
    dictionary_encode_strings: bool = False


@dataclass(frozen=True)
class SCIPExtractResult:
    """Hold extracted SCIP index table."""

    scip_index: TableLike


SIG_DOC_OCCURRENCE_KEYS = ("symbol", "symbol_roles", "range")


def _signature_doc_occurrence_rows(sig_doc: object) -> list[dict[str, object]]:
    occurrences = getattr(sig_doc, "occurrences", None)
    occurrences_list: list[object] = [] if occurrences is None else list(occurrences)
    rows: list[dict[str, object]] = []
    for occ in occurrences_list:
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


SCIP_INDEX_SCHEMA = dataset_schema("scip_index_v1")


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


def _int_or_none(value: object | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _int_value(value: object | None, *, default: int = 0) -> int:
    parsed = _int_or_none(value)
    return default if parsed is None else parsed


def _string_value(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _int_list(values: Sequence[object]) -> list[int]:
    out: list[int] = []
    for value in values:
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            out.append(value)
            continue
        if isinstance(value, str):
            try:
                out.append(int(value))
            except ValueError:
                continue
    return out


def _metadata_entry(index: object) -> dict[str, object] | None:
    metadata = getattr(index, "metadata", None)
    if metadata is None:
        return None
    tool_info = getattr(metadata, "tool_info", None)
    tool_args: list[str | None] = []
    if tool_info is not None:
        raw_args = getattr(tool_info, "arguments", []) or []
        tool_args = normalize_string_items(list(raw_args))
    tool_entry = None
    if tool_info is not None:
        tool_entry = {
            "name": _string_value(getattr(tool_info, "name", None)),
            "version": _string_value(getattr(tool_info, "version", None)),
            "arguments": tool_args,
        }
    return {
        "protocol_version": _int_or_none(getattr(metadata, "protocol_version", None)),
        "tool_info": tool_entry,
        "project_root": _string_value(getattr(metadata, "project_root", None)),
        "text_document_encoding": _int_or_none(getattr(metadata, "text_document_encoding", None)),
    }


def _diagnostic_entry(diag: object) -> dict[str, object]:
    tags_raw = getattr(diag, "tags", []) or []
    return {
        "severity": _int_or_none(getattr(diag, "severity", None)),
        "code": _string_value(getattr(diag, "code", None)),
        "message": _string_value(getattr(diag, "message", None)),
        "source": _string_value(getattr(diag, "source", None)),
        "tags": _int_list(list(tags_raw)),
    }


def _range_span(range_raw: Sequence[int], *, col_unit: str) -> dict[str, object] | None:
    norm = _normalize_range(range_raw)
    if norm is None:
        return None
    start_line, start_col, end_line, end_col, _ = norm
    return span_dict(
        SpanSpec(
            start_line0=start_line,
            start_col=start_col,
            end_line0=end_line,
            end_col=end_col,
            end_exclusive=SCIP_END_EXCLUSIVE,
            col_unit=col_unit,
        )
    )


def _occurrence_entry(occurrence: object, *, col_unit: str) -> dict[str, object]:
    range_raw = _int_list(list(getattr(occurrence, "range", []) or []))
    enclosing_raw = _int_list(list(getattr(occurrence, "enclosing_range", []) or []))
    diagnostics = [
        _diagnostic_entry(diag) for diag in list(getattr(occurrence, "diagnostics", []) or [])
    ]
    override_docs = list(getattr(occurrence, "override_documentation", []) or [])
    return {
        "range_raw": range_raw,
        "range": _range_span(range_raw, col_unit=col_unit),
        "symbol": _string_value(getattr(occurrence, "symbol", None)),
        "symbol_roles": _int_value(getattr(occurrence, "symbol_roles", None)),
        "override_documentation": normalize_string_items(override_docs),
        "syntax_kind": _int_value(getattr(occurrence, "syntax_kind", None)),
        "diagnostics": diagnostics,
        "enclosing_range_raw": enclosing_raw,
        "enclosing_range": _range_span(enclosing_raw, col_unit=col_unit),
        "attrs": attrs_map({}),
    }


def _relationship_entry(relationship: object) -> dict[str, object] | None:
    related = getattr(relationship, "symbol", None)
    if related is None:
        return None
    return {
        "symbol": related,
        "is_reference": bool(getattr(relationship, "is_reference", False)),
        "is_implementation": bool(getattr(relationship, "is_implementation", False)),
        "is_type_definition": bool(getattr(relationship, "is_type_definition", False)),
        "is_definition": bool(getattr(relationship, "is_definition", False)),
    }


def _symbol_info_entry(symbol_info: object) -> dict[str, object]:
    docs_raw = getattr(symbol_info, "documentation", None) or []
    relationships_raw = getattr(symbol_info, "relationships", None) or []
    relationships = [
        entry for rel in list(relationships_raw) if (entry := _relationship_entry(rel)) is not None
    ]
    sig_doc = _signature_doc_entry(getattr(symbol_info, "signature_documentation", None))
    return {
        "symbol": _string_value(getattr(symbol_info, "symbol", None)),
        "documentation": normalize_string_items(list(docs_raw)),
        "signature_documentation": sig_doc,
        "relationships": relationships,
        "kind": _int_value(getattr(symbol_info, "kind", None)),
        "display_name": _string_value(getattr(symbol_info, "display_name", None)),
        "enclosing_symbol": _string_value(getattr(symbol_info, "enclosing_symbol", None)),
        "attrs": attrs_map({}),
    }


def _document_entry(doc: object) -> dict[str, object]:
    rel_path = _string_value(getattr(doc, "relative_path", None))
    position_encoding = getattr(doc, "position_encoding", None)
    col_unit = _col_unit_from_position_encoding(position_encoding)
    occurrences = [
        _occurrence_entry(occ, col_unit=col_unit)
        for occ in list(getattr(doc, "occurrences", []) or [])
    ]
    symbols = [_symbol_info_entry(symbol) for symbol in list(getattr(doc, "symbols", []) or [])]
    return {
        "relative_path": rel_path,
        "language": _string_value(getattr(doc, "language", None)),
        "text": _string_value(getattr(doc, "text", None)),
        "position_encoding": _int_or_none(position_encoding),
        "occurrences": occurrences,
        "symbols": symbols,
        "attrs": attrs_map({}),
    }


def _index_row(index: object, *, index_id: str) -> dict[str, object]:
    documents = [_document_entry(doc) for doc in list(getattr(index, "documents", []) or [])]
    symbol_info = list(getattr(index, "symbol_information", []) or [])
    if not symbol_info:
        for doc in list(getattr(index, "documents", []) or []):
            symbol_info.extend(list(getattr(doc, "symbols", []) or []))
    symbols = [_symbol_info_entry(si) for si in symbol_info]
    external_symbols = [
        _symbol_info_entry(si) for si in list(getattr(index, "external_symbols", []) or [])
    ]
    return {
        "index_id": index_id,
        "metadata": _metadata_entry(index),
        "documents": documents,
        "symbols": symbols,
        "external_symbols": external_symbols,
    }


def _build_scip_index_plan(
    rows: list[Row],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
) -> IbisPlan:
    raw = ibis_plan_from_rows("scip_index_v1", rows, row_schema=SCIP_INDEX_SCHEMA)
    return apply_query_and_project(
        "scip_index_v1",
        raw.expr,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


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
    """Extract the SCIP index table as a name-keyed bundle.

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
    normalize = ExtractNormalizeOptions(options=normalized_opts)
    rows: list[Row] = []
    if scip_index_path is not None:
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
        index_id = stable_id("scip_index", str(index_path.resolve()))
        rows.append(_index_row(index, index_id=index_id))

    plan = _build_scip_index_plan(
        rows,
        normalize=normalize,
        evidence_plan=evidence_plan,
    )
    return {
        "scip_index": materialize_extract_plan(
            "scip_index_v1",
            plan,
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=False,
            ),
        )
    }
