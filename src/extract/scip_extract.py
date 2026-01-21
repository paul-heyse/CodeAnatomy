"""Extract SCIP index data into Arrow tables using shared helpers."""

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
from datafusion_engine.extract_registry import normalize_options
from extract.helpers import (
    ExtractMaterializeOptions,
    apply_query_and_project,
    ibis_plan_from_row_batches,
    materialize_extract_plan,
)
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

SCIP_LINE_BASE = 0
SCIP_END_EXCLUSIVE = True
SCIP_SIGNATURE_COL_UNIT = "utf32"

ROLE_DEFINITION = 0x01
ROLE_IMPORT = 0x02
ROLE_WRITE_ACCESS = 0x04
ROLE_READ_ACCESS = 0x08
ROLE_GENERATED = 0x10
ROLE_TEST = 0x20
ROLE_FORWARD_DEFINITION = 0x40

LOGGER = logging.getLogger(__name__)

type Row = dict[str, object]

SCIP_OUTPUT_DATASETS: tuple[tuple[str, str], ...] = (
    ("scip_metadata", "scip_metadata_v1"),
    ("scip_index_stats", "scip_index_stats_v1"),
    ("scip_documents", "scip_documents_v1"),
    ("scip_document_texts", "scip_document_texts_v1"),
    ("scip_occurrences", "scip_occurrences_v1"),
    ("scip_symbol_information", "scip_symbol_information_v1"),
    ("scip_document_symbols", "scip_document_symbols_v1"),
    ("scip_external_symbol_information", "scip_external_symbol_information_v1"),
    ("scip_symbol_relationships", "scip_symbol_relationships_v1"),
    ("scip_signature_occurrences", "scip_signature_occurrences_v1"),
    ("scip_diagnostics", "scip_diagnostics_v1"),
)


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


@dataclass(frozen=True)
class ScipExtractOptions:
    """Options for SCIP extraction table materialization."""

    parse_opts: SCIPParseOptions | None = None
    evidence_plan: EvidencePlan | None = None
    profile: str = "default"
    include_document_text: bool = False
    include_document_symbols: bool = True
    include_signature_occurrences: bool = True
    batch_size: int | None = 5000


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


@dataclass
class _RowBatcher:
    """Accumulate row batches per dataset."""

    batch_size: int | None
    _current: dict[str, list[Row]]
    _batches: dict[str, list[list[Row]]]

    @classmethod
    def for_datasets(cls, dataset_names: Sequence[str], batch_size: int | None) -> _RowBatcher:
        current = {name: [] for name in dataset_names}
        batches = {name: [] for name in dataset_names}
        return cls(batch_size=batch_size, _current=current, _batches=batches)

    def append(self, dataset: str, row: Row) -> None:
        batch = self._current[dataset]
        batch.append(row)
        if self.batch_size is not None and len(batch) >= self.batch_size:
            self._batches[dataset].append(batch)
            self._current[dataset] = []

    def finalize(self) -> Mapping[str, list[list[Row]]]:
        for name, batch in self._current.items():
            if batch:
                self._batches[name].append(batch)
        return self._batches


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


def _range_fields(
    range_raw: Sequence[int],
    *,
    prefix: str = "",
    include_len: bool = True,
) -> dict[str, int | None]:
    normalized = f"{prefix}_" if prefix and not prefix.endswith("_") else prefix
    norm = _normalize_range(range_raw)
    if norm is None:
        fields = {
            f"{normalized}start_line": None,
            f"{normalized}start_char": None,
            f"{normalized}end_line": None,
            f"{normalized}end_char": None,
        }
        if include_len:
            fields[f"{normalized}range_len"] = None
        return fields
    start_line, start_char, end_line, end_char, range_len = norm
    fields = {
        f"{normalized}start_line": start_line,
        f"{normalized}start_char": start_char,
        f"{normalized}end_line": end_line,
        f"{normalized}end_char": end_char,
    }
    if include_len:
        fields[f"{normalized}range_len"] = range_len
    return fields


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
    docs = list(getattr(index, "documents", []) or [])
    occurrences = 0
    diagnostics = 0
    missing_posenc = 0
    for doc in docs:
        if getattr(doc, "position_encoding", None) is None:
            missing_posenc += 1
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
        "missing_position_encoding": missing_posenc,
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


def _document_id(path: str | None) -> str | None:
    if not path:
        return None
    return stable_id("scip_doc", path)


def _role_flags(symbol_roles: int) -> dict[str, bool]:
    return {
        "is_definition": (symbol_roles & ROLE_DEFINITION) != 0,
        "is_import": (symbol_roles & ROLE_IMPORT) != 0,
        "is_write": (symbol_roles & ROLE_WRITE_ACCESS) != 0,
        "is_read": (symbol_roles & ROLE_READ_ACCESS) != 0,
        "is_generated": (symbol_roles & ROLE_GENERATED) != 0,
        "is_test": (symbol_roles & ROLE_TEST) != 0,
        "is_forward_definition": (symbol_roles & ROLE_FORWARD_DEFINITION) != 0,
    }


def _syntax_kind_name(value: int | None, scip_pb2: ModuleType | None) -> str | None:
    if value is None or scip_pb2 is None:
        return None
    enum_cls = getattr(scip_pb2, "SyntaxKind", None)
    if enum_cls is None:
        return None
    try:
        return str(enum_cls.Name(int(value)))
    except (ValueError, TypeError):
        return None


def _symbol_kind_name(value: int | None, scip_pb2: ModuleType | None) -> str | None:
    if value is None or scip_pb2 is None:
        return None
    symbol_info = getattr(scip_pb2, "SymbolInformation", None)
    if symbol_info is None:
        return None
    enum_cls = getattr(symbol_info, "Kind", None)
    if enum_cls is None:
        return None
    try:
        return str(enum_cls.Name(int(value)))
    except (ValueError, TypeError):
        return None


def _metadata_row(index: object, *, index_id: str) -> Row:
    metadata = getattr(index, "metadata", None)
    tool_info = getattr(metadata, "tool_info", None) if metadata is not None else None
    raw_args = getattr(tool_info, "arguments", []) or [] if tool_info is not None else []
    tool_args = normalize_string_items(list(raw_args))
    return {
        "index_id": index_id,
        "protocol_version": _int_or_none(getattr(metadata, "protocol_version", None)),
        "tool_name": _string_value(getattr(tool_info, "name", None)),
        "tool_version": _string_value(getattr(tool_info, "version", None)),
        "tool_arguments": tool_args,
        "project_root": _string_value(getattr(metadata, "project_root", None)),
        "text_document_encoding": _int_or_none(getattr(metadata, "text_document_encoding", None)),
        "project_name": _string_value(getattr(index, "project_name", None)),
        "project_version": _string_value(getattr(index, "project_version", None)),
        "project_namespace": _string_value(getattr(index, "project_namespace", None)),
    }


def _index_stats_row(counts: Mapping[str, int], *, index_id: str) -> Row:
    return {
        "index_id": index_id,
        "document_count": counts.get("documents", 0),
        "occurrence_count": counts.get("occurrences", 0),
        "diagnostic_count": counts.get("diagnostics", 0),
        "symbol_count": counts.get("symbol_information", 0),
        "external_symbol_count": counts.get("external_symbols", 0),
        "missing_position_encoding_count": counts.get("missing_position_encoding", 0),
    }


def _symbol_info_base(
    symbol_info: object,
    *,
    scip_pb2: ModuleType | None,
) -> tuple[Row, object | None]:
    documentation_raw = getattr(symbol_info, "documentation", None) or []
    documentation = normalize_string_items(list(documentation_raw))
    sig_doc = getattr(symbol_info, "signature_documentation", None)
    sig_text = _string_value(getattr(sig_doc, "text", None))
    sig_lang = _string_value(getattr(sig_doc, "language", None))
    kind_value = _int_or_none(getattr(symbol_info, "kind", None))
    return (
        {
            "symbol": _string_value(getattr(symbol_info, "symbol", None)),
            "display_name": _string_value(getattr(symbol_info, "display_name", None)),
            "kind": kind_value,
            "kind_name": _symbol_kind_name(kind_value, scip_pb2),
            "enclosing_symbol": _string_value(getattr(symbol_info, "enclosing_symbol", None)),
            "documentation": documentation,
            "signature_text": sig_text,
            "signature_language": sig_lang,
        },
        sig_doc,
    )


def _signature_occurrence_rows(
    sig_doc: object,
    *,
    parent_symbol: str | None,
    scip_pb2: ModuleType | None,
) -> list[Row]:
    occurrences = list(getattr(sig_doc, "occurrences", []) or [])
    rows: list[Row] = []
    for occ in occurrences:
        range_raw = _int_list(list(getattr(occ, "range", []) or []))
        range_fields = _range_fields(range_raw, include_len=True)
        symbol_roles = _int_value(getattr(occ, "symbol_roles", None))
        flags = _role_flags(symbol_roles)
        syntax_kind = _int_or_none(getattr(occ, "syntax_kind", None))
        rows.append(
            {
                "parent_symbol": parent_symbol,
                "symbol": _string_value(getattr(occ, "symbol", None)),
                "symbol_roles": symbol_roles,
                "syntax_kind": syntax_kind,
                "syntax_kind_name": _syntax_kind_name(syntax_kind, scip_pb2),
                "range_raw": range_raw,
                **range_fields,
                "line_base": SCIP_LINE_BASE,
                "col_unit": SCIP_SIGNATURE_COL_UNIT,
                "end_exclusive": SCIP_END_EXCLUSIVE,
                **flags,
            }
        )
    return rows


def _relationship_rows(symbol_info: object) -> list[Row]:
    relationships = list(getattr(symbol_info, "relationships", None) or [])
    parent_symbol = _string_value(getattr(symbol_info, "symbol", None))
    rows: list[Row] = []
    for relationship in relationships:
        related = getattr(relationship, "symbol", None)
        if related is None:
            continue
        rows.append(
            {
                "symbol": parent_symbol,
                "related_symbol": _string_value(related),
                "is_reference": bool(getattr(relationship, "is_reference", False)),
                "is_implementation": bool(getattr(relationship, "is_implementation", False)),
                "is_type_definition": bool(getattr(relationship, "is_type_definition", False)),
                "is_definition": bool(getattr(relationship, "is_definition", False)),
            }
        )
    return rows


def _diagnostic_rows(
    occurrence: object,
    *,
    document_id: str | None,
    path: str | None,
    col_unit: str,
) -> list[Row]:
    range_raw = _int_list(list(getattr(occurrence, "range", []) or []))
    range_fields = _range_fields(range_raw, include_len=False)
    diagnostics = list(getattr(occurrence, "diagnostics", []) or [])
    rows: list[Row] = []
    for diag in diagnostics:
        tags_raw = getattr(diag, "tags", []) or []
        rows.append(
            {
                "document_id": document_id,
                "path": path,
                "severity": _int_or_none(getattr(diag, "severity", None)),
                "code": _string_value(getattr(diag, "code", None)),
                "message": _string_value(getattr(diag, "message", None)),
                "source": _string_value(getattr(diag, "source", None)),
                "tags": _int_list(list(tags_raw)),
                **range_fields,
                "line_base": SCIP_LINE_BASE,
                "col_unit": col_unit,
                "end_exclusive": SCIP_END_EXCLUSIVE,
            }
        )
    return rows


def _occurrence_row(
    occurrence: object,
    *,
    document_id: str | None,
    path: str | None,
    col_unit: str,
    scip_pb2: ModuleType | None,
) -> Row:
    range_raw = _int_list(list(getattr(occurrence, "range", []) or []))
    enclosing_raw = _int_list(list(getattr(occurrence, "enclosing_range", []) or []))
    range_fields = _range_fields(range_raw, include_len=True)
    enc_fields = _range_fields(enclosing_raw, prefix="enc", include_len=True)
    symbol_roles = _int_value(getattr(occurrence, "symbol_roles", None))
    flags = _role_flags(symbol_roles)
    syntax_kind = _int_or_none(getattr(occurrence, "syntax_kind", None))
    override_docs = list(getattr(occurrence, "override_documentation", []) or [])
    return {
        "document_id": document_id,
        "path": path,
        "symbol": _string_value(getattr(occurrence, "symbol", None)),
        "symbol_roles": symbol_roles,
        "syntax_kind": syntax_kind,
        "syntax_kind_name": _syntax_kind_name(syntax_kind, scip_pb2),
        "override_documentation": normalize_string_items(override_docs),
        "range_raw": range_raw,
        "enclosing_range_raw": enclosing_raw,
        **range_fields,
        **enc_fields,
        "line_base": SCIP_LINE_BASE,
        "col_unit": col_unit,
        "end_exclusive": SCIP_END_EXCLUSIVE,
        **flags,
    }


def _resolve_batch_size(options: ScipExtractOptions) -> int | None:
    if options.batch_size is None:
        return None
    if options.batch_size <= 0:
        msg = "batch_size must be a positive integer."
        raise ValueError(msg)
    return options.batch_size


def _build_scip_plan(
    name: str,
    row_batches: Sequence[Sequence[Row]],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
) -> IbisPlan:
    raw = ibis_plan_from_row_batches(name, row_batches)
    return apply_query_and_project(
        name,
        raw.expr,
        normalize=normalize,
        evidence_plan=evidence_plan,
        repo_id=normalize.repo_id,
    )


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
    """Extract the SCIP index tables as a name-keyed bundle.

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
    dataset_names = tuple(dataset for _, dataset in SCIP_OUTPUT_DATASETS)
    batch_size = _resolve_batch_size(options)
    batcher = _RowBatcher.for_datasets(dataset_names, batch_size)

    if scip_index_path is not None:
        index_path = Path(scip_index_path)
        if repo_root is not None and not index_path.is_absolute():
            index_path = Path(repo_root) / index_path
        if normalized_opts.build_dir is None:
            normalized_opts = replace(normalized_opts, build_dir=index_path.parent)
        index = parse_index_scip(index_path, parse_opts=normalized_opts)
        scip_pb2 = _load_scip_pb2(normalized_opts)
        counts = _index_counts(index)
        if normalized_opts.log_counts or normalized_opts.health_check:
            LOGGER.info(
                "SCIP index stats: documents=%d occurrences=%d diagnostics=%d symbols=%d "
                "external_symbols=%d missing_posenc=%d",
                counts["documents"],
                counts["occurrences"],
                counts["diagnostics"],
                counts["symbol_information"],
                counts["external_symbols"],
                counts["missing_position_encoding"],
            )
        if normalized_opts.health_check:
            assert_scip_index_health(index)
        index_id = stable_id("scip_index", str(index_path.resolve()))
        batcher.append("scip_metadata_v1", _metadata_row(index, index_id=index_id))
        batcher.append("scip_index_stats_v1", _index_stats_row(counts, index_id=index_id))

        documents = list(getattr(index, "documents", []) or [])
        for doc in documents:
            rel_path = _string_value(getattr(doc, "relative_path", None))
            document_id = _document_id(rel_path)
            position_encoding = _int_or_none(getattr(doc, "position_encoding", None))
            col_unit = _col_unit_from_position_encoding(position_encoding)
            batcher.append(
                "scip_documents_v1",
                {
                    "index_id": index_id,
                    "document_id": document_id,
                    "path": rel_path,
                    "language": _string_value(getattr(doc, "language", None)),
                    "position_encoding": position_encoding,
                },
            )
            if options.include_document_text:
                text = _string_value(getattr(doc, "text", None))
                if text is not None:
                    batcher.append(
                        "scip_document_texts_v1",
                        {
                            "document_id": document_id,
                            "path": rel_path,
                            "text": text,
                        },
                    )
            if options.include_document_symbols:
                symbols = list(getattr(doc, "symbols", []) or [])
                for symbol_info in symbols:
                    base, _sig_doc = _symbol_info_base(symbol_info, scip_pb2=scip_pb2)
                    row = {"document_id": document_id, "path": rel_path, **base}
                    batcher.append("scip_document_symbols_v1", row)

            occurrences = list(getattr(doc, "occurrences", []) or [])
            for occurrence in occurrences:
                occ_row = _occurrence_row(
                    occurrence,
                    document_id=document_id,
                    path=rel_path,
                    col_unit=col_unit,
                    scip_pb2=scip_pb2,
                )
                batcher.append("scip_occurrences_v1", occ_row)
                for diag_row in _diagnostic_rows(
                    occurrence,
                    document_id=document_id,
                    path=rel_path,
                    col_unit=col_unit,
                ):
                    batcher.append("scip_diagnostics_v1", diag_row)

        symbol_info = list(getattr(index, "symbol_information", []) or [])
        if not symbol_info:
            for doc in documents:
                symbol_info.extend(list(getattr(doc, "symbols", []) or []))
        for symbol in symbol_info:
            base, sig_doc = _symbol_info_base(symbol, scip_pb2=scip_pb2)
            batcher.append("scip_symbol_information_v1", base)
            if sig_doc is not None and options.include_signature_occurrences:
                parent_symbol = base.get("symbol")
                for row in _signature_occurrence_rows(
                    sig_doc,
                    parent_symbol=parent_symbol if isinstance(parent_symbol, str) else None,
                    scip_pb2=scip_pb2,
                ):
                    batcher.append("scip_signature_occurrences_v1", row)
            for row in _relationship_rows(symbol):
                batcher.append("scip_symbol_relationships_v1", row)

        external_symbols = list(getattr(index, "external_symbols", []) or [])
        for symbol in external_symbols:
            base, _sig_doc = _symbol_info_base(symbol, scip_pb2=scip_pb2)
            batcher.append("scip_external_symbol_information_v1", base)

    batches = batcher.finalize()
    plans = {
        dataset: _build_scip_plan(
            dataset,
            batches.get(dataset, []),
            normalize=normalize,
            evidence_plan=evidence_plan,
        )
        for dataset in dataset_names
    }
    return {
        output: materialize_extract_plan(
            dataset,
            plans[dataset],
            ctx=exec_ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=False,
            ),
        )
        for output, dataset in SCIP_OUTPUT_DATASETS
    }
