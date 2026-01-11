"""Extract SCIP metadata and occurrences into Arrow tables."""

from __future__ import annotations

import importlib
import json
import os
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType

import pyarrow as pa

from arrowdsl.empty import empty_table
from arrowdsl.ids import hash64_from_parts
from extract.scip_parse_json import parse_index_json
from extract.scip_proto_loader import load_scip_pb2_from_build

SCHEMA_VERSION = 1
RANGE_LEN_SHORT = 3
RANGE_LEN_FULL = 4

type Row = dict[str, object]


def _hash_id(prefix: str, *parts: str) -> str:
    # Row-wise hash64 IDs are needed while building dependent rows.
    hashed = hash64_from_parts(*parts, prefix=prefix)
    return f"{prefix}:{hashed}"


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


SCIP_METADATA_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("tool_name", pa.string()),
        ("tool_version", pa.string()),
        ("project_root", pa.string()),
        ("text_document_encoding", pa.string()),
        ("protocol_version", pa.string()),
    ]
)

SCIP_DOCUMENTS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("language", pa.string()),
        ("position_encoding", pa.string()),
    ]
)

SCIP_OCCURRENCES_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("occurrence_id", pa.string()),
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("symbol", pa.string()),
        ("symbol_roles", pa.int32()),
        ("syntax_kind", pa.string()),
        ("override_documentation", pa.list_(pa.string())),
        ("start_line", pa.int32()),
        ("start_char", pa.int32()),
        ("end_line", pa.int32()),
        ("end_char", pa.int32()),
        ("range_len", pa.int32()),
        ("enc_start_line", pa.int32()),
        ("enc_start_char", pa.int32()),
        ("enc_end_line", pa.int32()),
        ("enc_end_char", pa.int32()),
        ("enc_range_len", pa.int32()),
    ]
)

SCIP_SYMBOL_INFO_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("symbol_info_id", pa.string()),
        ("symbol", pa.string()),
        ("display_name", pa.string()),
        ("kind", pa.string()),
        ("enclosing_symbol", pa.string()),
        ("documentation", pa.list_(pa.string())),
        ("signature_documentation", pa.string()),
    ]
)

SCIP_SYMBOL_RELATIONSHIPS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("relationship_id", pa.string()),
        ("symbol", pa.string()),
        ("related_symbol", pa.string()),
        ("is_reference", pa.bool_()),
        ("is_implementation", pa.bool_()),
        ("is_type_definition", pa.bool_()),
        ("is_definition", pa.bool_()),
    ]
)

SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("symbol_info_id", pa.string()),
        ("symbol", pa.string()),
        ("display_name", pa.string()),
        ("kind", pa.string()),
        ("enclosing_symbol", pa.string()),
        ("documentation", pa.list_(pa.string())),
        ("signature_documentation", pa.string()),
    ]
)

SCIP_DIAGNOSTICS_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int32()),
        ("diagnostic_id", pa.string()),
        ("document_id", pa.string()),
        ("path", pa.string()),
        ("severity", pa.string()),
        ("code", pa.string()),
        ("message", pa.string()),
        ("source", pa.string()),
        ("tags", pa.list_(pa.string())),
        ("start_line", pa.int32()),
        ("start_char", pa.int32()),
        ("end_line", pa.int32()),
        ("end_char", pa.int32()),
    ]
)


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


def _diagnostic_rows(
    document_id: str,
    rel_path: object,
    occ_index: int,
    diagnostics: Sequence[object],
    default_range: tuple[int, int, int, int] | None,
) -> list[Row]:
    rows: list[Row] = []
    if default_range is None:
        sl = sc = el = ec = None
    else:
        sl, sc, el, ec = default_range
    for j, diag in enumerate(diagnostics):
        drng = list(getattr(diag, "range", []))
        norm = _normalize_range(drng) if drng else None
        if norm is None:
            dsl, dsc, del_, dec_ = sl, sc, el, ec
        else:
            dsl, dsc, del_, dec_, _ = norm
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "diagnostic_id": _hash_id("scip_diag", document_id, str(occ_index), str(j)),
                "document_id": document_id,
                "path": rel_path,
                "severity": str(getattr(diag, "severity", None))
                if hasattr(diag, "severity")
                else None,
                "code": str(getattr(diag, "code", None)) if hasattr(diag, "code") else None,
                "message": getattr(diag, "message", None),
                "source": getattr(diag, "source", None),
                "tags": [str(t) for t in getattr(diag, "tags", [])]
                if hasattr(diag, "tags")
                else [],
                "start_line": dsl,
                "start_char": dsc,
                "end_line": del_,
                "end_char": dec_,
            }
        )
    return rows


def _occurrence_rows(
    document_id: str,
    rel_path: object,
    occurrences: Sequence[object],
) -> tuple[list[Row], list[Row]]:
    occ_rows: list[Row] = []
    diag_rows: list[Row] = []
    for i, occ in enumerate(occurrences):
        norm = _normalize_range(list(getattr(occ, "range", [])))
        if norm is None:
            sl = sc = el = ec = None
            rlen = 0
            default_range = None
        else:
            sl, sc, el, ec, rlen = norm
            default_range = (sl, sc, el, ec)

        enc_norm = _normalize_range(list(getattr(occ, "enclosing_range", [])))
        if enc_norm is None:
            esl = esc = eel = eec = None
            elen = None
        else:
            esl, esc, eel, eec, elen = enc_norm

        occ_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "occurrence_id": _hash_id(
                    "scip_occ",
                    document_id,
                    str(i),
                    str(sl),
                    str(sc),
                    str(el),
                    str(ec),
                ),
                "document_id": document_id,
                "path": rel_path,
                "symbol": getattr(occ, "symbol", None),
                "symbol_roles": int(getattr(occ, "symbol_roles", 0) or 0),
                "syntax_kind": str(getattr(occ, "syntax_kind", None))
                if hasattr(occ, "syntax_kind")
                else None,
                "override_documentation": list(getattr(occ, "override_documentation", []))
                if hasattr(occ, "override_documentation")
                else [],
                "start_line": sl,
                "start_char": sc,
                "end_line": el,
                "end_char": ec,
                "range_len": rlen,
                "enc_start_line": esl if elen else None,
                "enc_start_char": esc if elen else None,
                "enc_end_line": eel if elen else None,
                "enc_end_char": eec if elen else None,
                "enc_range_len": elen if elen else None,
            }
        )

        diag_rows.extend(
            _diagnostic_rows(
                document_id,
                rel_path,
                i,
                getattr(occ, "diagnostics", []),
                default_range,
            )
        )
    return occ_rows, diag_rows


def _document_rows(index: object) -> tuple[list[Row], list[Row], list[Row]]:
    doc_rows: list[Row] = []
    occ_rows: list[Row] = []
    diag_rows: list[Row] = []

    for doc in getattr(index, "documents", []):
        rel_path = getattr(doc, "relative_path", None)
        language = getattr(doc, "language", None)
        position_encoding = getattr(doc, "position_encoding", None)

        document_id = _hash_id("scip_doc", rel_path or "")
        doc_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "document_id": document_id,
                "path": rel_path,
                "language": str(language) if language is not None else None,
                "position_encoding": str(position_encoding)
                if position_encoding is not None
                else None,
            }
        )

        occs, diags = _occurrence_rows(
            document_id,
            rel_path,
            getattr(doc, "occurrences", []),
        )
        occ_rows.extend(occs)
        diag_rows.extend(diags)

    return doc_rows, occ_rows, diag_rows


def _signature_doc_json(sig_doc: object | None) -> str | None:
    if sig_doc is None:
        return None
    text = getattr(sig_doc, "text", None)
    language = getattr(sig_doc, "language", None)
    occs = [
        {
            "symbol": getattr(occ, "symbol", None),
            "symbol_roles": int(getattr(occ, "symbol_roles", 0) or 0),
            "range": list(getattr(occ, "range", [])),
        }
        for occ in getattr(sig_doc, "occurrences", [])
    ]
    payload = {
        "text": text,
        "language": str(language) if language is not None else None,
        "occurrences": occs,
    }
    return json.dumps(payload, ensure_ascii=False)


def _symbol_rows(index: object) -> list[Row]:
    sym_rows: list[Row] = []
    for si in getattr(index, "symbol_information", []):
        symbol = getattr(si, "symbol", None)
        sig_doc = getattr(si, "signature_documentation", None)
        sym_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "symbol_info_id": _hash_id("scip_sym", symbol or ""),
                "symbol": symbol,
                "display_name": getattr(si, "display_name", None),
                "kind": str(getattr(si, "kind", None)) if hasattr(si, "kind") else None,
                "enclosing_symbol": getattr(si, "enclosing_symbol", None)
                if hasattr(si, "enclosing_symbol")
                else None,
                "documentation": list(getattr(si, "documentation", []))
                if hasattr(si, "documentation")
                else [],
                "signature_documentation": _signature_doc_json(sig_doc),
            }
        )
    return sym_rows


def _symbol_relationship_rows(index: object) -> list[Row]:
    rows: list[Row] = []
    for si in getattr(index, "symbol_information", []):
        symbol = getattr(si, "symbol", None)
        if not symbol:
            continue
        for rel in getattr(si, "relationships", []):
            related = getattr(rel, "symbol", None)
            if related is None:
                continue
            rel_id = _hash_id(
                "scip_rel",
                str(symbol),
                str(related),
                str(getattr(rel, "is_reference", False)),
                str(getattr(rel, "is_implementation", False)),
                str(getattr(rel, "is_type_definition", False)),
                str(getattr(rel, "is_definition", False)),
            )
            rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "relationship_id": rel_id,
                    "symbol": symbol,
                    "related_symbol": related,
                    "is_reference": bool(getattr(rel, "is_reference", False)),
                    "is_implementation": bool(getattr(rel, "is_implementation", False)),
                    "is_type_definition": bool(getattr(rel, "is_type_definition", False)),
                    "is_definition": bool(getattr(rel, "is_definition", False)),
                }
            )
    return rows


def _external_symbol_rows(index: object) -> list[Row]:
    rows: list[Row] = []
    for si in getattr(index, "external_symbols", []):
        symbol = getattr(si, "symbol", None)
        sig_doc = getattr(si, "signature_documentation", None)
        rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "symbol_info_id": _hash_id("scip_ext_sym", symbol or ""),
                "symbol": symbol,
                "display_name": getattr(si, "display_name", None),
                "kind": str(getattr(si, "kind", None)) if hasattr(si, "kind") else None,
                "enclosing_symbol": getattr(si, "enclosing_symbol", None)
                if hasattr(si, "enclosing_symbol")
                else None,
                "documentation": list(getattr(si, "documentation", []))
                if hasattr(si, "documentation")
                else [],
                "signature_documentation": _signature_doc_json(sig_doc),
            }
        )
    return rows


def _extract_tables_from_index(index: object) -> SCIPExtractResult:
    meta_rows = _metadata_rows(index)
    doc_rows, occ_rows, diag_rows = _document_rows(index)
    sym_rows = _symbol_rows(index)
    rel_rows = _symbol_relationship_rows(index)
    ext_rows = _external_symbol_rows(index)

    t_meta = pa.Table.from_pylist(meta_rows, schema=SCIP_METADATA_SCHEMA)
    t_docs = pa.Table.from_pylist(doc_rows, schema=SCIP_DOCUMENTS_SCHEMA)
    t_occs = pa.Table.from_pylist(occ_rows, schema=SCIP_OCCURRENCES_SCHEMA)
    t_syms = pa.Table.from_pylist(sym_rows, schema=SCIP_SYMBOL_INFO_SCHEMA)
    t_rels = pa.Table.from_pylist(rel_rows, schema=SCIP_SYMBOL_RELATIONSHIPS_SCHEMA)
    t_ext = pa.Table.from_pylist(ext_rows, schema=SCIP_EXTERNAL_SYMBOL_INFO_SCHEMA)
    t_diags = pa.Table.from_pylist(diag_rows, schema=SCIP_DIAGNOSTICS_SCHEMA)

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
        empty_result = _extract_tables_from_index(index)

    return {
        "scip_metadata": empty_result.scip_metadata,
        "scip_documents": empty_result.scip_documents,
        "scip_occurrences": empty_result.scip_occurrences,
        "scip_symbol_information": empty_result.scip_symbol_information,
        "scip_symbol_relationships": empty_result.scip_symbol_relationships,
        "scip_external_symbol_information": empty_result.scip_external_symbol_information,
        "scip_diagnostics": empty_result.scip_diagnostics,
    }
