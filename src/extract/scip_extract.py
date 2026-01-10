from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

import pyarrow as pa

from .repo_scan import stable_id

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SCIPIndexOptions:
    """
    Options for running scip-python as an external tool.

    Canonical invocation follows:
      scip-python index . --project-name <name> [--environment <env.json>] [--output <path>]
    :contentReference[oaicite:10]{index=10}:contentReference[oaicite:11]{index=11}
    """
    repo_root: Path
    project_name: str
    output_path: Optional[Path] = None
    environment_json: Optional[Path] = None
    extra_args: Sequence[str] = ()


@dataclass(frozen=True)
class SCIPParseOptions:
    """
    Options for parsing an index.scip.

    - prefer_protobuf=True: requires scip_pb2 generated from scip.proto and protobuf installed.
      Uses Index.ParseFromString(data). :contentReference[oaicite:12]{index=12}
    - If you want to avoid protobuf codegen, downstream can use `scip print --json` streaming,
      but that is a different interface and generally higher overhead. :contentReference[oaicite:13]{index=13}
    """
    prefer_protobuf: bool = True
    scip_pb2_import: Optional[str] = None  # e.g. "codeintel_cpg.extract.proto.scip_pb2"


@dataclass(frozen=True)
class SCIPExtractResult:
    scip_metadata: pa.Table
    scip_documents: pa.Table
    scip_occurrences: pa.Table
    scip_symbol_information: pa.Table
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
        ("path", pa.string()),  # Document.relative_path
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

        # occurrence range (token span)
        ("start_line", pa.int32()),
        ("start_char", pa.int32()),
        ("end_line", pa.int32()),
        ("end_char", pa.int32()),
        ("range_len", pa.int32()),

        # enclosing range (full syntactic span), when present
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


def run_scip_python_index(opts: SCIPIndexOptions) -> Path:
    """
    Run scip-python to produce index.scip.
    """
    repo_root = opts.repo_root.resolve()
    out = opts.output_path or (repo_root / "index.scip")

    cmd: list[str] = [
        "scip-python",
        "index",
        ".",
        "--project-name",
        opts.project_name,
        "--output",
        str(out),
    ]
    if opts.environment_json is not None:
        cmd.extend(["--environment", str(opts.environment_json)])
    cmd.extend(list(opts.extra_args))

    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "scip-python failed.\n"
            f"cmd={cmd}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}\n"
        )

    if not out.exists():
        raise FileNotFoundError(f"scip-python reported success but output not found: {out}")
    return out


def _normalize_range(rng: Sequence[int]) -> tuple[int, int, int, int, int]:
    """
    Occurrence.range is either:
      - [line, start_char, end_char]
      - [start_line, start_char, end_line, end_char]
    :contentReference[oaicite:14]{index=14}
    """
    if len(rng) == 3:
        line, start_c, end_c = rng
        return int(line), int(start_c), int(line), int(end_c), 3
    if len(rng) >= 4:
        sl, sc, el, ec = rng[:4]
        return int(sl), int(sc), int(el), int(ec), 4
    return 0, 0, 0, 0, 0


def parse_index_scip(index_path: Path, parse_opts: Optional[SCIPParseOptions] = None) -> Any:
    """
    Parse index.scip into a protobuf Index object (preferred).
    """
    parse_opts = parse_opts or SCIPParseOptions()
    if not parse_opts.prefer_protobuf:
        raise NotImplementedError("JSON-stream parsing is available, but this code path expects protobuf.")

    # Import generated scip_pb2
    scip_pb2 = None
    if parse_opts.scip_pb2_import:
        scip_pb2 = __import__(parse_opts.scip_pb2_import, fromlist=["Index"])
    else:
        try:
            import scip_pb2 as scip_pb2  # type: ignore
        except Exception:
            scip_pb2 = None

    if scip_pb2 is None or not hasattr(scip_pb2, "Index"):
        raise RuntimeError(
            "SCIP protobuf bindings not available.\n"
            "You must generate scip_pb2.py from scip.proto using protoc, then make it importable.\n"
            "See scip_python_overview.md for the canonical protoc invocation. :contentReference[oaicite:15]{index=15}"
        )

    data = index_path.read_bytes()
    index = scip_pb2.Index()
    index.ParseFromString(data)
    return index


def extract_scip_tables(index: Any) -> SCIPExtractResult:
    """
    Convert protobuf Index message into Arrow tables:
      - metadata
      - documents
      - occurrences
      - symbol_information
      - diagnostics (best-effort)
    """
    # Metadata
    tool_name = getattr(getattr(getattr(index, "metadata", None), "tool_info", None), "name", None)
    tool_version = getattr(getattr(getattr(index, "metadata", None), "tool_info", None), "version", None)
    project_root = getattr(getattr(index, "metadata", None), "project_root", None)
    protocol_version = getattr(getattr(index, "metadata", None), "protocol_version", None)
    text_document_encoding = getattr(getattr(index, "metadata", None), "text_document_encoding", None)

    meta_rows = [
        {
            "schema_version": SCHEMA_VERSION,
            "tool_name": tool_name,
            "tool_version": tool_version,
            "project_root": project_root,
            "text_document_encoding": str(text_document_encoding) if text_document_encoding is not None else None,
            "protocol_version": str(protocol_version) if protocol_version is not None else None,
        }
    ]

    doc_rows: list[dict] = []
    occ_rows: list[dict] = []
    sym_rows: list[dict] = []
    diag_rows: list[dict] = []

    # Documents + occurrences
    for doc in getattr(index, "documents", []):
        rel_path = getattr(doc, "relative_path", None)
        language = getattr(doc, "language", None)
        position_encoding = getattr(doc, "position_encoding", None)

        document_id = stable_id("scip_doc", rel_path or "")
        doc_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "document_id": document_id,
                "path": rel_path,
                "language": str(language) if language is not None else None,
                "position_encoding": str(position_encoding) if position_encoding is not None else None,
            }
        )

        for i, occ in enumerate(getattr(doc, "occurrences", [])):
            rng = list(getattr(occ, "range", []))
            sl, sc, el, ec, rlen = _normalize_range(rng)

            enc_rng = list(getattr(occ, "enclosing_range", []))
            esl, esc, eel, eec, elen = _normalize_range(enc_rng) if enc_rng else (0, 0, 0, 0, 0)

            symbol = getattr(occ, "symbol", None)
            roles = int(getattr(occ, "symbol_roles", 0) or 0)

            occ_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "occurrence_id": stable_id("scip_occ", document_id, str(i), str(sl), str(sc), str(el), str(ec)),
                    "document_id": document_id,
                    "path": rel_path,
                    "symbol": symbol,
                    "symbol_roles": roles,
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

            # Occurrence-attached diagnostics (optional)
            for j, d in enumerate(getattr(occ, "diagnostics", [])):
                drng = list(getattr(d, "range", []))
                dsl, dsc, del_, dec_, _ = _normalize_range(drng) if drng else (sl, sc, el, ec, 0)

                diag_rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "diagnostic_id": stable_id("scip_diag", document_id, str(i), str(j)),
                        "document_id": document_id,
                        "path": rel_path,
                        "severity": str(getattr(d, "severity", None)) if hasattr(d, "severity") else None,
                        "code": str(getattr(d, "code", None)) if hasattr(d, "code") else None,
                        "message": getattr(d, "message", None),
                        "source": getattr(d, "source", None),
                        "tags": [str(t) for t in getattr(d, "tags", [])] if hasattr(d, "tags") else [],
                        "start_line": dsl,
                        "start_char": dsc,
                        "end_line": del_,
                        "end_char": dec_,
                    }
                )

    # SymbolInformation
    for si in getattr(index, "symbol_information", []):
        symbol = getattr(si, "symbol", None)
        sym_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "symbol_info_id": stable_id("scip_sym", symbol or ""),
                "symbol": symbol,
                "display_name": getattr(si, "display_name", None),
                "kind": str(getattr(si, "kind", None)) if hasattr(si, "kind") else None,
                "enclosing_symbol": getattr(si, "enclosing_symbol", None) if hasattr(si, "enclosing_symbol") else None,
                "documentation": list(getattr(si, "documentation", [])) if hasattr(si, "documentation") else [],
            }
        )

    t_meta = pa.Table.from_pylist(meta_rows, schema=SCIP_METADATA_SCHEMA)
    t_docs = pa.Table.from_pylist(doc_rows, schema=SCIP_DOCUMENTS_SCHEMA)
    t_occs = pa.Table.from_pylist(occ_rows, schema=SCIP_OCCURRENCES_SCHEMA)
    t_syms = pa.Table.from_pylist(sym_rows, schema=SCIP_SYMBOL_INFO_SCHEMA)
    t_diags = pa.Table.from_pylist(diag_rows, schema=SCIP_DIAGNOSTICS_SCHEMA)

    return SCIPExtractResult(
        scip_metadata=t_meta,
        scip_documents=t_docs,
        scip_occurrences=t_occs,
        scip_symbol_information=t_syms,
        scip_diagnostics=t_diags,
    )
