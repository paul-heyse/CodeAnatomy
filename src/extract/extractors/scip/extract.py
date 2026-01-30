"""Extract SCIP index data into Arrow tables using shared helpers."""

from __future__ import annotations

import importlib
import logging
import os
import subprocess
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Literal, cast, overload

import pyarrow as pa

from core_types import DeterminismTier
from core_types import RowPermissive as Row
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.encoding.policy import encode_table
from datafusion_engine.expr.span import ENC_UTF8, ENC_UTF16, ENC_UTF32
from datafusion_engine.extract.registry import normalize_options
from datafusion_engine.plan.bundle import DataFusionPlanBundle
from datafusion_engine.schema.alignment import align_table
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
from extract.coordination import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_row_batches,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions, schema_policy_for_dataset
from extract.infrastructure import BatchOptions
from extract.infrastructure.string_utils import normalize_string_items
from extract.session import ExtractSession, build_extract_session
from obs.otel.scopes import SCOPE_EXTRACT
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from datafusion_engine.schema.policy import SchemaPolicy
    from extract.coordination.evidence_plan import EvidencePlan

from extract.extractors.scip.setup import load_scip_pb2_from_build

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

SCIP_STREAMING_THRESHOLD_BYTES = 128 * 1024 * 1024

LOGGER = logging.getLogger(__name__)

SCIP_OUTPUT_DATASETS: tuple[tuple[str, str], ...] = (("scip_index", "scip_index_v1"),)


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
class ScipIndexRunReport:
    """Run report for scip-python index invocations."""

    command: Sequence[str]
    returncode: int
    stdout: str
    stderr: str
    duration_s: float


@dataclass(frozen=True)
class SCIPParseOptions:
    """Configure index.scip parsing."""

    prefer_protobuf: bool = True
    scip_pb2_import: str | None = None
    build_dir: Path | None = None
    health_check: bool = False
    log_counts: bool = False


@dataclass(frozen=True)
class ScipExtractOptions(BatchOptions):
    """Options for SCIP extraction table materialization."""

    parse_opts: SCIPParseOptions | None = None
    evidence_plan: EvidencePlan | None = None
    profile: str = "default"
    include_document_text: bool = False
    include_document_symbols: bool = True
    include_signature_occurrences: bool = True
    batch_size: int | None = 5000
    streaming_threshold_bytes: int | None = SCIP_STREAMING_THRESHOLD_BYTES


@dataclass(frozen=True)
class ScipExtractContext:
    """Execution context inputs for SCIP extraction."""

    scip_index_path: str | None
    repo_root: str | None
    runtime_spec: RuntimeProfileSpec | None = None
    session: ExtractSession | None = None
    profile: str = "default"

    def ensure_session(self) -> ExtractSession:
        """Return the effective extract session.

        Returns
        -------
        ExtractSession
            Provided session or a profile-derived session when missing.
        """
        if self.session is not None:
            return self.session
        runtime_spec = self.runtime_spec or resolve_runtime_profile(self.profile)
        return build_extract_session(runtime_spec)

    def ensure_runtime_profile(self) -> DataFusionRuntimeProfile:
        """Return the DataFusion runtime profile for SCIP extraction.

        Returns
        -------
        DataFusionRuntimeProfile
            Runtime profile derived from the extract session.
        """
        return self.ensure_session().engine_session.datafusion_profile


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


@dataclass(frozen=True)
class _ResolvedScipExtraction:
    """Resolved index and context for SCIP extraction."""

    index: object
    index_id: str | None
    index_path: Path
    scip_pb2: ModuleType | None
    parse_opts: SCIPParseOptions
    runtime_profile: DataFusionRuntimeProfile
    determinism_tier: DeterminismTier
    normalize: ExtractNormalizeOptions


@dataclass(frozen=True)
class _ScipDocumentContext:
    """Document context fields extracted from a SCIP index."""

    document_id: str | None
    path: str | None
    position_encoding: int | None
    col_unit: str


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


def run_scip_python_index(
    opts: SCIPIndexOptions,
    *,
    on_complete: Callable[[ScipIndexRunReport], None] | None = None,
) -> Path:
    """Run scip-python to produce an index.scip file.

    Parameters
    ----------
    opts:
        Index invocation options.
    on_complete:
        Optional callback invoked with the run report regardless of exit status.

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
    with stage_span(
        "extract.scip_index",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={
            "codeanatomy.extractor": "scip_python",
            "codeanatomy.repo_root": str(opts.repo_root),
        },
    ):
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

        start = time.monotonic()
        proc = subprocess.run(
            cmd,
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
            env=env,
            timeout=opts.timeout_s,
        )
        report = ScipIndexRunReport(
            command=tuple(cmd),
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
            duration_s=time.monotonic() - start,
        )
        if on_complete is not None:
            on_complete(report)
        if proc.returncode != 0:
            msg = (
                f"scip-python failed.\ncmd={cmd}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}\n"
            )
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
        fields: dict[str, int | None] = {
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


def _index_counts(index: object) -> tuple[dict[str, int], bool]:
    documents = getattr(index, "documents", []) or []
    symbol_info = getattr(index, "symbol_information", []) or []
    has_index_symbols = bool(symbol_info)
    occurrences = 0
    diagnostics = 0
    missing_posenc = 0
    text_count = 0
    text_bytes = 0
    symbol_count = len(symbol_info) if has_index_symbols else 0
    for doc in documents:
        if getattr(doc, "position_encoding", None) is None:
            missing_posenc += 1
        text = getattr(doc, "text", None)
        if text:
            text_count += 1
            if isinstance(text, str):
                text_bytes += len(text.encode("utf-8"))
            else:
                text_bytes += len(str(text).encode("utf-8"))
        if not has_index_symbols:
            symbol_count += len(getattr(doc, "symbols", []) or [])
        occs = getattr(doc, "occurrences", [])
        occurrences += len(occs)
        for occ in occs:
            diagnostics += len(getattr(occ, "diagnostics", []))
    counts = {
        "documents": len(documents),
        "occurrences": occurrences,
        "diagnostics": diagnostics,
        "symbol_information": symbol_count,
        "external_symbols": len(getattr(index, "external_symbols", []) or []),
        "missing_position_encoding": missing_posenc,
        "document_text_count": text_count,
        "document_text_bytes": text_bytes,
    }
    return counts, has_index_symbols


def assert_scip_index_health(index: object) -> None:
    """Raise when the SCIP index is missing core data.

    Raises
    ------
    ValueError
        Raised when the index has no documents or occurrences.
    """
    counts, _has_index_symbols = _index_counts(index)
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
    _ = path
    return None


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


def _metadata_row(index: object, *, index_id: str | None, index_path: Path) -> Row:
    metadata = getattr(index, "metadata", None)
    tool_info = getattr(metadata, "tool_info", None) if metadata is not None else None
    raw_args = getattr(tool_info, "arguments", []) or [] if tool_info is not None else []
    tool_args = normalize_string_items(list(raw_args))
    return {
        "index_id": index_id,
        "index_path": str(index_path),
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


def _strip_keys(row: Row, *, keys: Sequence[str]) -> Row:
    """Return a row mapping without the specified keys.

    Returns
    -------
    Row
        Row mapping with requested keys removed.
    """
    return {key: value for key, value in row.items() if key not in keys}


@dataclass(frozen=True)
class _ScipDocumentInputs:
    doc: object
    document_id: str | None
    path: str | None
    position_encoding: int | None
    col_unit: str
    include_document_text: bool
    include_document_symbols: bool
    scip_pb2: ModuleType | None


def _scip_document_row(inputs: _ScipDocumentInputs) -> Row:
    """Build a nested document payload for the SCIP index row.

    Returns
    -------
    Row
        Nested document payload for the SCIP index row.
    """
    text = (
        _string_value(getattr(inputs.doc, "text", None)) if inputs.include_document_text else None
    )
    symbols: list[Row] = []
    if inputs.include_document_symbols:
        for symbol_entry in getattr(inputs.doc, "symbols", []) or []:
            base, _sig_doc = _symbol_info_base(symbol_entry, scip_pb2=inputs.scip_pb2)
            symbols.append(base)
    occurrences: list[Row] = []
    diagnostics: list[Row] = []
    for occurrence in getattr(inputs.doc, "occurrences", []) or []:
        occurrences.append(
            _strip_keys(
                _occurrence_row(
                    occurrence,
                    document_id=inputs.document_id,
                    path=inputs.path,
                    col_unit=inputs.col_unit,
                    scip_pb2=inputs.scip_pb2,
                ),
                keys=("document_id", "path"),
            )
        )
        diagnostics.extend(
            _strip_keys(row, keys=("document_id", "path"))
            for row in _diagnostic_rows(
                occurrence,
                document_id=inputs.document_id,
                path=inputs.path,
                col_unit=inputs.col_unit,
            )
        )
    return {
        "document_id": inputs.document_id,
        "path": inputs.path,
        "language": _string_value(getattr(inputs.doc, "language", None)),
        "position_encoding": inputs.position_encoding,
        "text": text,
        "symbols": symbols,
        "occurrences": occurrences,
        "diagnostics": diagnostics,
    }


def _scip_index_row(
    resolved: _ResolvedScipExtraction,
    *,
    counts: Mapping[str, int],
    has_index_symbols: bool,
    options: ScipExtractOptions,
) -> Row:
    metadata = _strip_keys(
        _metadata_row(
            resolved.index,
            index_id=resolved.index_id,
            index_path=resolved.index_path,
        ),
        keys=("index_id", "index_path"),
    )
    index_stats = _strip_keys(
        _index_stats_row(
            counts,
            index_id=resolved.index_id,
            index_path=resolved.index_path,
        ),
        keys=("index_id", "index_path"),
    )
    documents = [
        _scip_document_row(
            _ScipDocumentInputs(
                doc=doc,
                document_id=document_id,
                path=rel_path,
                position_encoding=position_encoding,
                col_unit=col_unit,
                include_document_text=options.include_document_text,
                include_document_symbols=options.include_document_symbols,
                scip_pb2=resolved.scip_pb2,
            )
        )
        for doc, document_id, rel_path, position_encoding, col_unit in _iter_document_contexts(
            resolved.index
        )
    ]
    symbol_information = list(
        _iter_scip_symbol_information(
            resolved.index,
            has_index_symbols=has_index_symbols,
            scip_pb2=resolved.scip_pb2,
        )
    )
    external_symbol_information = list(
        _iter_scip_external_symbols(resolved.index, scip_pb2=resolved.scip_pb2)
    )
    symbol_relationships = list(
        _iter_scip_symbol_relationships(
            resolved.index,
            has_index_symbols=has_index_symbols,
        )
    )
    signature_occurrences: list[Row] = []
    if options.include_signature_occurrences:
        signature_occurrences = list(
            _iter_scip_signature_occurrences(
                resolved.index,
                include_signature_occurrences=options.include_signature_occurrences,
                has_index_symbols=has_index_symbols,
                scip_pb2=resolved.scip_pb2,
            )
        )
    return {
        "index_id": resolved.index_id,
        "index_path": str(resolved.index_path),
        "metadata": metadata,
        "index_stats": index_stats,
        "documents": documents,
        "symbol_information": symbol_information,
        "external_symbol_information": external_symbol_information,
        "symbol_relationships": symbol_relationships,
        "signature_occurrences": signature_occurrences,
    }


def _index_stats_row(counts: Mapping[str, int], *, index_id: str | None, index_path: Path) -> Row:
    return {
        "index_id": index_id,
        "index_path": str(index_path),
        "document_count": counts.get("documents", 0),
        "occurrence_count": counts.get("occurrences", 0),
        "diagnostic_count": counts.get("diagnostics", 0),
        "symbol_count": counts.get("symbol_information", 0),
        "external_symbol_count": counts.get("external_symbols", 0),
        "missing_position_encoding_count": counts.get("missing_position_encoding", 0),
        "document_text_count": counts.get("document_text_count", 0),
        "document_text_bytes": counts.get("document_text_bytes", 0),
    }


def _record_scip_index_stats(
    runtime_profile: DataFusionRuntimeProfile | None,
    *,
    index_id: str | None,
    index_path: Path,
    counts: Mapping[str, int],
) -> None:
    if runtime_profile is None:
        return
    payload = _index_stats_row(counts, index_id=index_id, index_path=index_path)
    payload["event_time_unix_ms"] = int(time.time() * 1000)
    from datafusion_engine.lineage.diagnostics import record_artifact

    record_artifact(runtime_profile, "scip_index_stats_v1", payload)


def _log_scip_counts(counts: Mapping[str, int], parse_opts: SCIPParseOptions) -> None:
    if not (parse_opts.log_counts or parse_opts.health_check):
        return
    LOGGER.info(
        "SCIP index stats: documents=%d occurrences=%d diagnostics=%d symbols=%d "
        "external_symbols=%d missing_posenc=%d text_docs=%d text_bytes=%d",
        counts["documents"],
        counts["occurrences"],
        counts["diagnostics"],
        counts["symbol_information"],
        counts["external_symbols"],
        counts["missing_position_encoding"],
        counts["document_text_count"],
        counts["document_text_bytes"],
    )


def _ensure_scip_health(counts: Mapping[str, int], parse_opts: SCIPParseOptions) -> None:
    if not parse_opts.health_check:
        return
    if counts["documents"] == 0:
        msg = "SCIP index has no documents."
        raise ValueError(msg)
    if counts["occurrences"] == 0:
        msg = "SCIP index has no occurrences."
        raise ValueError(msg)


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


def _iter_document_contexts(
    index: object,
) -> Iterator[tuple[object, str | None, str | None, int | None, str]]:
    for doc in getattr(index, "documents", []) or []:
        rel_path = _string_value(getattr(doc, "relative_path", None))
        document_id = _document_id(rel_path)
        position_encoding = _int_or_none(getattr(doc, "position_encoding", None))
        col_unit = _col_unit_from_position_encoding(position_encoding)
        yield doc, document_id, rel_path, position_encoding, col_unit


def _iter_scip_documents(
    index: object,
    *,
    index_id: str | None,
    index_path: Path,
) -> Iterator[Row]:
    for doc, document_id, rel_path, position_encoding, _col_unit in _iter_document_contexts(index):
        yield {
            "index_id": index_id,
            "index_path": str(index_path),
            "document_id": document_id,
            "path": rel_path,
            "language": _string_value(getattr(doc, "language", None)),
            "position_encoding": position_encoding,
        }


def _iter_scip_document_texts(
    index: object,
    *,
    include_document_text: bool,
) -> Iterator[Row]:
    if not include_document_text:
        return
    for doc, document_id, rel_path, _position_encoding, _col_unit in _iter_document_contexts(index):
        text = _string_value(getattr(doc, "text", None))
        if text is None:
            continue
        yield {
            "document_id": document_id,
            "path": rel_path,
            "text": text,
        }


def _iter_scip_document_symbols(
    index: object,
    *,
    include_document_symbols: bool,
    scip_pb2: ModuleType | None,
) -> Iterator[Row]:
    if not include_document_symbols:
        return
    for doc, document_id, rel_path, _position_encoding, _col_unit in _iter_document_contexts(index):
        for symbol_entry in getattr(doc, "symbols", []) or []:
            base, _sig_doc = _symbol_info_base(symbol_entry, scip_pb2=scip_pb2)
            yield {"document_id": document_id, "path": rel_path, **base}


def _iter_scip_occurrences(
    index: object,
    *,
    scip_pb2: ModuleType | None,
) -> Iterator[Row]:
    for doc, document_id, rel_path, _position_encoding, col_unit in _iter_document_contexts(index):
        for occurrence in getattr(doc, "occurrences", []) or []:
            yield _occurrence_row(
                occurrence,
                document_id=document_id,
                path=rel_path,
                col_unit=col_unit,
                scip_pb2=scip_pb2,
            )


def _iter_scip_diagnostics(index: object) -> Iterator[Row]:
    for doc, document_id, rel_path, _position_encoding, col_unit in _iter_document_contexts(index):
        for occurrence in getattr(doc, "occurrences", []) or []:
            yield from _diagnostic_rows(
                occurrence,
                document_id=document_id,
                path=rel_path,
                col_unit=col_unit,
            )


def _iter_symbol_source(index: object, *, has_index_symbols: bool) -> Iterator[object]:
    if has_index_symbols:
        yield from getattr(index, "symbol_information", []) or []
        return
    for doc in getattr(index, "documents", []) or []:
        yield from getattr(doc, "symbols", []) or []


def _iter_scip_symbol_information(
    index: object,
    *,
    has_index_symbols: bool,
    scip_pb2: ModuleType | None,
) -> Iterator[Row]:
    for symbol in _iter_symbol_source(index, has_index_symbols=has_index_symbols):
        base, _sig_doc = _symbol_info_base(symbol, scip_pb2=scip_pb2)
        yield base


def _iter_scip_signature_occurrences(
    index: object,
    *,
    include_signature_occurrences: bool,
    has_index_symbols: bool,
    scip_pb2: ModuleType | None,
) -> Iterator[Row]:
    if not include_signature_occurrences:
        return
    for symbol in _iter_symbol_source(index, has_index_symbols=has_index_symbols):
        base, sig_doc = _symbol_info_base(symbol, scip_pb2=scip_pb2)
        if sig_doc is None:
            continue
        parent_symbol = base.get("symbol")
        parent = parent_symbol if isinstance(parent_symbol, str) else None
        yield from _signature_occurrence_rows(
            sig_doc,
            parent_symbol=parent,
            scip_pb2=scip_pb2,
        )


def _iter_scip_symbol_relationships(
    index: object,
    *,
    has_index_symbols: bool,
) -> Iterator[Row]:
    for symbol in _iter_symbol_source(index, has_index_symbols=has_index_symbols):
        yield from _relationship_rows(symbol)


def _iter_scip_external_symbols(
    index: object,
    *,
    scip_pb2: ModuleType | None,
) -> Iterator[Row]:
    for symbol in getattr(index, "external_symbols", []) or []:
        base, _sig_doc = _symbol_info_base(symbol, scip_pb2=scip_pb2)
        yield base


def _append_rows(batcher: _RowBatcher, dataset: str, rows: Iterable[Row]) -> None:
    for row in rows:
        batcher.append(dataset, row)


def _resolve_scip_index(
    context: ScipExtractContext,
    parse_opts: SCIPParseOptions,
    runtime_profile: DataFusionRuntimeProfile,
) -> _ResolvedScipExtraction | None:
    scip_index_path = context.scip_index_path
    if scip_index_path is None:
        return None
    index_path = Path(scip_index_path)
    repo_root = context.repo_root
    if repo_root is not None and not index_path.is_absolute():
        index_path = Path(repo_root) / index_path
    resolved_opts = parse_opts
    if resolved_opts.build_dir is None:
        resolved_opts = replace(resolved_opts, build_dir=index_path.parent)
    index = parse_index_scip(index_path, parse_opts=resolved_opts)
    scip_pb2 = _load_scip_pb2(resolved_opts)
    index_id: str | None = None
    normalize = ExtractNormalizeOptions(options=resolved_opts)
    determinism_tier = context.ensure_session().engine_session.surface_policy.determinism_tier
    return _ResolvedScipExtraction(
        index=index,
        index_id=index_id,
        index_path=index_path,
        scip_pb2=scip_pb2,
        parse_opts=resolved_opts,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        normalize=normalize,
    )


def _resolve_batch_size(options: ScipExtractOptions) -> int | None:
    if options.batch_size is None:
        return None
    if options.batch_size <= 0:
        msg = "batch_size must be a positive integer."
        raise ValueError(msg)
    return options.batch_size


def _streaming_batch_size(options: ScipExtractOptions) -> int:
    resolved = _resolve_batch_size(options)
    return resolved if resolved is not None else 5000


def _normalize_batch(
    table: pa.Table,
    *,
    schema: pa.Schema,
    policy: SchemaPolicy,
) -> pa.Table:
    aligned = align_table(
        table,
        schema=schema,
        safe_cast=policy.safe_cast,
        keep_extra_columns=policy.keep_extra_columns,
        on_error=policy.on_error,
    )
    encoding = policy.encoding
    if encoding is None or not encoding.dictionary_cols:
        return cast("pa.Table", aligned)
    columns = [field.name for field in schema if field.name in encoding.dictionary_cols]
    encoded = encode_table(aligned, columns=columns)
    return cast("pa.Table", encoded)


def _reader_from_rows(
    name: str,
    rows: Iterable[Row],
    *,
    normalize: ExtractNormalizeOptions,
    batch_size: int,
) -> RecordBatchReaderLike:
    policy = schema_policy_for_dataset(
        name,
        options=normalize.options,
        repo_id=normalize.repo_id,
        enable_encoding=normalize.enable_encoding,
    )
    schema = cast("pa.Schema", policy.resolved_schema())

    def _batches() -> Iterator[pa.RecordBatch]:
        batch: list[Row] = []
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                table = pa.Table.from_pylist(batch)
                normalized = _normalize_batch(table, schema=schema, policy=policy)
                yield from normalized.to_batches()
                batch = []
        if batch:
            table = pa.Table.from_pylist(batch)
            normalized = _normalize_batch(table, schema=schema, policy=policy)
            yield from normalized.to_batches()

    return pa.RecordBatchReader.from_batches(schema, _batches())


def _should_stream_outputs(
    index_path: Path,
    *,
    options: ScipExtractOptions,
    prefer_reader: bool,
) -> bool:
    if not prefer_reader:
        return False
    threshold = options.streaming_threshold_bytes
    if threshold is None:
        return True
    try:
        return index_path.stat().st_size >= threshold
    except OSError:
        return True


def _build_scip_plan(
    name: str,
    row_batches: Sequence[Sequence[Row]],
    *,
    normalize: ExtractNormalizeOptions,
    evidence_plan: EvidencePlan | None,
    session: ExtractSession,
) -> DataFusionPlanBundle:
    return extract_plan_from_row_batches(
        name,
        row_batches,
        session=session,
        options=ExtractPlanOptions(
            normalize=normalize,
            evidence_plan=evidence_plan,
        ),
    )


def _extract_scip_tables_streaming(
    resolved: _ResolvedScipExtraction,
    options: ScipExtractOptions,
) -> Mapping[str, RecordBatchReaderLike]:
    counts, has_index_symbols = _index_counts(resolved.index)
    _log_scip_counts(counts, resolved.parse_opts)
    _ensure_scip_health(counts, resolved.parse_opts)
    _record_scip_index_stats(
        resolved.runtime_profile,
        index_id=resolved.index_id,
        index_path=resolved.index_path,
        counts=counts,
    )
    row = _scip_index_row(
        resolved,
        counts=counts,
        has_index_symbols=has_index_symbols,
        options=options,
    )
    batch_size = _streaming_batch_size(options)
    reader = _reader_from_rows(
        "scip_index_v1",
        (row,),
        normalize=resolved.normalize,
        batch_size=batch_size,
    )
    return {output: reader for output, _dataset in SCIP_OUTPUT_DATASETS}


def _extract_scip_tables_in_memory(
    resolved: _ResolvedScipExtraction,
    options: ScipExtractOptions,
    *,
    evidence_plan: EvidencePlan | None,
    session: ExtractSession,
    prefer_reader: bool,
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    counts, has_index_symbols = _index_counts(resolved.index)
    _log_scip_counts(counts, resolved.parse_opts)
    _ensure_scip_health(counts, resolved.parse_opts)
    _record_scip_index_stats(
        resolved.runtime_profile,
        index_id=resolved.index_id,
        index_path=resolved.index_path,
        counts=counts,
    )
    row = _scip_index_row(
        resolved,
        counts=counts,
        has_index_symbols=has_index_symbols,
        options=options,
    )
    plan = _build_scip_plan(
        "scip_index_v1",
        ([row],),
        normalize=resolved.normalize,
        evidence_plan=evidence_plan,
        session=session,
    )
    output, dataset = SCIP_OUTPUT_DATASETS[0]
    table = materialize_extract_plan(
        dataset,
        plan,
        runtime_profile=resolved.runtime_profile,
        determinism_tier=resolved.determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=resolved.normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=False,
        ),
    )
    return {output: table}


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
    with stage_span(
        "extract.scip_tables",
        stage="extract",
        scope_name=SCOPE_EXTRACT,
        attributes={
            "codeanatomy.extractor": "scip_tables",
            "codeanatomy.repo_root": context.repo_root,
            "codeanatomy.prefer_reader": prefer_reader,
        },
    ):
        options = options or ScipExtractOptions()
        normalized_opts = normalize_options("scip", options.parse_opts, SCIPParseOptions)
        evidence_plan = options.evidence_plan
        session = context.ensure_session()
        runtime_profile = context.ensure_runtime_profile()
        determinism_tier = session.engine_session.surface_policy.determinism_tier
        resolved = _resolve_scip_index(context, normalized_opts, runtime_profile)
        if resolved is not None and _should_stream_outputs(
            resolved.index_path, options=options, prefer_reader=prefer_reader
        ):
            return _extract_scip_tables_streaming(resolved, options)
        if resolved is not None:
            return _extract_scip_tables_in_memory(
                resolved,
                options,
                evidence_plan=evidence_plan,
                session=session,
                prefer_reader=prefer_reader,
            )
        dataset_names = tuple(dataset for _, dataset in SCIP_OUTPUT_DATASETS)
        normalize = ExtractNormalizeOptions(options=normalized_opts)
        plans = {
            dataset: _build_scip_plan(
                dataset,
                (),
                normalize=normalize,
                evidence_plan=evidence_plan,
                session=session,
            )
            for dataset in dataset_names
        }
        return {
            output: materialize_extract_plan(
                dataset,
                plans[dataset],
                runtime_profile=runtime_profile,
                determinism_tier=determinism_tier,
                options=ExtractMaterializeOptions(
                    normalize=normalize,
                    prefer_reader=prefer_reader,
                    apply_post_kernels=False,
                ),
            )
            for output, dataset in SCIP_OUTPUT_DATASETS
        }
