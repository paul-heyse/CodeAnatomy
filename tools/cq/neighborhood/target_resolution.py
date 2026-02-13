"""Target parsing and resolution for neighborhood assembly.

Provides one canonical resolver used by both CLI command execution and run-plan
neighborhood steps.
"""

from __future__ import annotations

from pathlib import Path

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.definition_parser import extract_symbol_name
from tools.cq.core.snb_schema import DegradeEventV1
from tools.cq.core.structs import CqStruct
from tools.cq.neighborhood.scan_snapshot import ScanSnapshot

_TARGET_PARTS_WITH_COL = 3
_TARGET_PARTS_WITH_LINE = 2


class TargetSpec(CqStruct, frozen=True):
    """Parsed neighborhood target spec."""

    raw: str
    target_name: str | None = None
    target_file: str | None = None
    target_line: int | None = None
    target_col: int | None = None


class ResolvedTarget(CqStruct, frozen=True):
    """Resolved target used for bundle assembly."""

    target_name: str
    target_file: str
    target_line: int | None = None
    target_col: int | None = None
    target_uri: str | None = None
    symbol_hint: str | None = None
    resolution_kind: str = "unresolved"
    degrade_events: tuple[DegradeEventV1, ...] = ()


def parse_target_spec(target: str) -> TargetSpec:
    """Parse target string into target spec.

    Supports:
    - `path/to/file.py:line`
    - `path/to/file.py:line:col`
    - `symbol_name`

    Returns:
        Parsed target specification.
    """
    text = target.strip()
    if not text:
        return TargetSpec(raw=target)

    parts = text.split(":")
    if len(parts) >= _TARGET_PARTS_WITH_COL and parts[-1].isdigit() and parts[-2].isdigit():
        file_part = ":".join(parts[:-2]).strip()
        if file_part:
            return TargetSpec(
                raw=target,
                target_file=file_part,
                target_line=max(1, int(parts[-2])),
                target_col=max(0, int(parts[-1])),
            )

    if len(parts) >= _TARGET_PARTS_WITH_LINE and parts[-1].isdigit():
        file_part = ":".join(parts[:-1]).strip()
        if file_part:
            return TargetSpec(
                raw=target,
                target_file=file_part,
                target_line=max(1, int(parts[-1])),
            )

    return TargetSpec(raw=target, target_name=text)


def resolve_target(
    spec: TargetSpec,
    snapshot: ScanSnapshot,
    *,
    root: Path | None = None,
    allow_symbol_fallback: bool = True,
) -> ResolvedTarget:
    """Resolve a parsed target to file/name/anchor coordinates.

    Returns:
        Resolved target with coordinates, symbol hint, and degrade events.
    """
    degrades: list[DegradeEventV1] = []
    def_records = tuple(snapshot.def_records)

    anchor_result = _resolve_anchor_target(
        spec=spec,
        def_records=def_records,
        root=root,
        degrades=degrades,
    )
    if anchor_result is not None:
        return anchor_result

    file_symbol_result = _resolve_file_symbol_target(
        spec=spec,
        def_records=def_records,
        root=root,
        degrades=degrades,
    )
    if file_symbol_result is not None:
        return file_symbol_result

    symbol_name = _resolve_symbol_name(spec)
    if allow_symbol_fallback and symbol_name:
        symbol_result = _resolve_symbol_fallback_target(
            symbol_name=symbol_name,
            def_records=def_records,
            root=root,
            degrades=degrades,
        )
        if symbol_result is not None:
            return symbol_result

    return _build_unresolved_target(
        spec=spec, root=root, symbol_name=symbol_name, degrades=degrades
    )


def _resolve_anchor_target(
    *,
    spec: TargetSpec,
    def_records: tuple[SgRecord, ...],
    root: Path | None,
    degrades: list[DegradeEventV1],
) -> ResolvedTarget | None:
    if not spec.target_file or spec.target_line is None:
        return None
    anchor_candidates = _anchor_candidates(
        def_records=def_records,
        target_file=spec.target_file,
        line=spec.target_line,
        col=spec.target_col,
    )
    if anchor_candidates:
        if len(anchor_candidates) > 1:
            degrades.append(
                DegradeEventV1(
                    stage="target_resolution",
                    severity="warning",
                    category="ambiguous",
                    message=(
                        f"Anchor {spec.target_file}:{spec.target_line} matched "
                        f"{len(anchor_candidates)} definitions; choosing innermost"
                    ),
                )
            )
        chosen = anchor_candidates[0]
        name = _extract_name_from_text(chosen.text) or spec.target_name or spec.raw
        return ResolvedTarget(
            target_name=name,
            target_file=chosen.file,
            target_line=spec.target_line,
            target_col=spec.target_col,
            target_uri=_to_uri(root, chosen.file),
            symbol_hint=spec.target_name or name,
            resolution_kind="anchor",
            degrade_events=tuple(degrades),
        )
    degrades.append(
        DegradeEventV1(
            stage="target_resolution",
            severity="warning",
            category="not_found",
            message=(
                f"No definition found for anchor {spec.target_file}:{spec.target_line}; "
                "falling back to file-only resolution"
            ),
        )
    )
    if spec.target_name:
        return None
    fallback_name = Path(spec.target_file).stem
    normalized_file = _normalize_file_path(spec.target_file)
    return ResolvedTarget(
        target_name=fallback_name,
        target_file=normalized_file,
        target_line=spec.target_line,
        target_col=spec.target_col,
        target_uri=_to_uri(root, normalized_file),
        symbol_hint=fallback_name,
        resolution_kind="file_anchor_unresolved",
        degrade_events=tuple(degrades),
    )


def _resolve_file_symbol_target(
    *,
    spec: TargetSpec,
    def_records: tuple[SgRecord, ...],
    root: Path | None,
    degrades: list[DegradeEventV1],
) -> ResolvedTarget | None:
    if not spec.target_name or not spec.target_file:
        return None
    file_candidates = _name_candidates(
        def_records=def_records,
        name=spec.target_name,
        target_file=spec.target_file,
    )
    if file_candidates:
        if len(file_candidates) > 1:
            degrades.append(
                DegradeEventV1(
                    stage="target_resolution",
                    severity="warning",
                    category="ambiguous",
                    message=(
                        f"Multiple definitions for '{spec.target_name}' in "
                        f"{spec.target_file}; choosing first by source order"
                    ),
                )
            )
        chosen = file_candidates[0]
        return ResolvedTarget(
            target_name=spec.target_name,
            target_file=chosen.file,
            target_line=chosen.start_line,
            target_col=chosen.start_col,
            target_uri=_to_uri(root, chosen.file),
            symbol_hint=spec.target_name,
            resolution_kind="file_symbol",
            degrade_events=tuple(degrades),
        )
    degrades.append(
        DegradeEventV1(
            stage="target_resolution",
            severity="warning",
            category="not_found",
            message=(
                f"Definition '{spec.target_name}' not found in {spec.target_file}; "
                "falling back to symbol-only resolution"
            ),
        )
    )
    return None


def _resolve_symbol_name(spec: TargetSpec) -> str:
    return spec.target_name or Path(spec.target_file or spec.raw).stem


def _resolve_symbol_fallback_target(
    *,
    symbol_name: str,
    def_records: tuple[SgRecord, ...],
    root: Path | None,
    degrades: list[DegradeEventV1],
) -> ResolvedTarget | None:
    symbol_candidates = _name_candidates(
        def_records=def_records, name=symbol_name, target_file=None
    )
    if not symbol_candidates:
        return None
    chosen = symbol_candidates[0]
    if len(symbol_candidates) > 1:
        degrades.append(
            DegradeEventV1(
                stage="target_resolution",
                severity="warning",
                category="ambiguous",
                message=(
                    f"Symbol '{symbol_name}' matched {len(symbol_candidates)} definitions; "
                    "using deterministic first match"
                ),
            )
        )
    return ResolvedTarget(
        target_name=symbol_name,
        target_file=chosen.file,
        target_line=chosen.start_line,
        target_col=chosen.start_col,
        target_uri=_to_uri(root, chosen.file),
        symbol_hint=symbol_name,
        resolution_kind="symbol_fallback",
        degrade_events=tuple(degrades),
    )


def _build_unresolved_target(
    *,
    spec: TargetSpec,
    root: Path | None,
    symbol_name: str,
    degrades: list[DegradeEventV1],
) -> ResolvedTarget:
    if symbol_name:
        degrades.append(
            DegradeEventV1(
                stage="target_resolution",
                severity="error",
                category="not_found",
                message=f"Unable to resolve target '{symbol_name}'",
            )
        )
        return ResolvedTarget(
            target_name=symbol_name,
            target_file=_normalize_file_path(spec.target_file) if spec.target_file else "",
            target_line=spec.target_line,
            target_col=spec.target_col,
            target_uri=_to_uri(root, _normalize_file_path(spec.target_file))
            if spec.target_file
            else None,
            symbol_hint=symbol_name,
            resolution_kind="unresolved",
            degrade_events=tuple(degrades),
        )
    degrades.append(
        DegradeEventV1(
            stage="target_resolution",
            severity="error",
            category="invalid_target",
            message=f"Invalid target specification: {spec.raw!r}",
        )
    )
    return ResolvedTarget(
        target_name=spec.raw,
        target_file=_normalize_file_path(spec.target_file) if spec.target_file else "",
        target_line=spec.target_line,
        target_col=spec.target_col,
        target_uri=_to_uri(root, _normalize_file_path(spec.target_file))
        if spec.target_file
        else None,
        symbol_hint=spec.target_name,
        resolution_kind="invalid",
        degrade_events=tuple(degrades),
    )


def _anchor_candidates(
    *,
    def_records: tuple[SgRecord, ...],
    target_file: str,
    line: int,
    col: int | None,
) -> list[SgRecord]:
    normalized_target_file = _normalize_file_path(target_file)
    candidates = []
    for record in def_records:
        file_value = getattr(record, "file", None)
        if not isinstance(file_value, str):
            continue
        if _normalize_file_path(file_value) != normalized_target_file:
            continue
        start_line = _as_int(getattr(record, "start_line", None), 0)
        end_line = _as_int(getattr(record, "end_line", None), 0)
        if not (start_line <= line <= end_line):
            continue
        if col is not None:
            start_col = _as_int(getattr(record, "start_col", None), 0)
            end_col = _as_int(getattr(record, "end_col", None), 10_000)
            if line == start_line and col < start_col:
                continue
            if line == end_line and col > end_col:
                continue
        candidates.append(record)
    candidates.sort(key=_anchor_sort_key)
    return candidates


def _name_candidates(
    *,
    def_records: tuple[SgRecord, ...],
    name: str,
    target_file: str | None,
) -> list[SgRecord]:
    normalized_target_file = _normalize_file_path(target_file) if target_file is not None else None
    candidates: list[SgRecord] = []
    for record in def_records:
        file_value = getattr(record, "file", None)
        if target_file is not None:
            if not isinstance(file_value, str):
                continue
            if _normalize_file_path(file_value) != normalized_target_file:
                continue
        if _extract_name_from_text(str(getattr(record, "text", ""))) != name:
            continue
        candidates.append(record)
    candidates.sort(key=_deterministic_record_key)
    return candidates


def _anchor_sort_key(record: SgRecord) -> tuple[int, int, int, str]:
    start_line = record.start_line
    end_line = record.end_line
    span = max(0, end_line - start_line)
    return (
        span,
        start_line,
        record.start_col,
        record.text,
    )


def _deterministic_record_key(record: SgRecord) -> tuple[str, int, int, str]:
    return (
        record.file,
        record.start_line,
        record.start_col,
        record.text,
    )


def _extract_name_from_text(text: str) -> str:
    return extract_symbol_name(text, fallback=text.strip())


def _normalize_file_path(path: str) -> str:
    normalized = path.strip()
    if normalized.startswith("./"):
        normalized = normalized[2:]
    return Path(normalized).as_posix()


def _as_int(value: object, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    return default


def _to_uri(root: Path | None, file_path: str | None) -> str | None:
    if not file_path:
        return None
    candidate = Path(file_path)
    if root is not None and not candidate.is_absolute():
        candidate = root / candidate
    try:
        return candidate.resolve().as_uri()
    except ValueError:
        return None


__all__ = [
    "ResolvedTarget",
    "TargetSpec",
    "parse_target_spec",
    "resolve_target",
]
