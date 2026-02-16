"""Target parsing and resolution for neighborhood assembly."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

from tools.cq.core.pathing import normalize_repo_relative_path
from tools.cq.core.snb_schema import DegradeEventV1
from tools.cq.core.structs import CqStruct
from tools.cq.core.target_specs import TargetSpecV1
from tools.cq.search.pipeline.profiles import INTERACTIVE
from tools.cq.search.rg.adapter import find_symbol_candidates

ResolutionKind = Literal["anchor", "file_symbol", "symbol_fallback", "unresolved"]


class ResolvedTarget(CqStruct, frozen=True):
    """Resolved target used for bundle assembly."""

    target_name: str
    target_file: str
    target_line: int | None = None
    target_col: int | None = None
    target_uri: str | None = None
    symbol_hint: str | None = None
    resolution_kind: ResolutionKind = "unresolved"
    degrade_events: tuple[DegradeEventV1, ...] = ()


def resolve_target(
    spec: TargetSpecV1,
    *,
    root: Path,
    language: str,
    allow_symbol_fallback: bool = True,
) -> ResolvedTarget:
    """Resolve parsed target without full-repo neighborhood scans.

    Returns:
    -------
    ResolvedTarget
        Resolved file/symbol anchor with degrade diagnostics when needed.
    """
    degrades: list[DegradeEventV1] = []

    if spec.target_file:
        normalized_file = normalize_repo_relative_path(spec.target_file, root=root)
        if (root / normalized_file).exists():
            name = spec.target_name or Path(normalized_file).stem
            return ResolvedTarget(
                target_name=name,
                target_file=normalized_file,
                target_line=spec.target_line,
                target_col=spec.target_col,
                target_uri=_to_uri(root, normalized_file),
                symbol_hint=spec.target_name,
                resolution_kind="anchor" if spec.target_line is not None else "file_symbol",
                degrade_events=(),
            )
        degrades.append(
            DegradeEventV1(
                stage="target_resolution",
                severity="warning",
                category="file_missing",
                message=f"Target file not found: {normalized_file}",
            )
        )

    symbol_name = spec.target_name or Path(spec.target_file or spec.raw).stem
    if allow_symbol_fallback and symbol_name:
        symbol_match = _resolve_symbol_with_rg(root, symbol_name, language)
        if symbol_match is not None:
            file_path, line = symbol_match
            if degrades:
                degrades.append(
                    DegradeEventV1(
                        stage="target_resolution",
                        severity="info",
                        category="symbol_fallback",
                        message=f"Fell back to symbol match: {file_path}:{line}",
                    )
                )
            return ResolvedTarget(
                target_name=symbol_name,
                target_file=file_path,
                target_line=line,
                target_col=0,
                target_uri=_to_uri(root, file_path),
                symbol_hint=symbol_name,
                resolution_kind="symbol_fallback",
                degrade_events=tuple(degrades),
            )

    degrades.append(
        DegradeEventV1(
            stage="target_resolution",
            severity="warning",
            category="not_found",
            message=f"Unable to resolve target '{spec.raw}'",
        )
    )

    fallback_file = (
        normalize_repo_relative_path(spec.target_file, root=root) if spec.target_file else ""
    )
    return ResolvedTarget(
        target_name=symbol_name,
        target_file=fallback_file,
        target_line=spec.target_line,
        target_col=spec.target_col,
        target_uri=_to_uri(root, fallback_file),
        symbol_hint=symbol_name,
        resolution_kind="unresolved",
        degrade_events=tuple(degrades),
    )


def _resolve_symbol_with_rg(root: Path, symbol_name: str, language: str) -> tuple[str, int] | None:
    """Resolve first symbol occurrence via ripgrep scoped by language.

    Returns:
    -------
    tuple[str, int] | None
        Relative file path and 1-based line number for the first hit.
    """
    rows = find_symbol_candidates(
        root=root,
        symbol_name=symbol_name,
        lang_scope="rust" if language == "rust" else "python",
        limits=INTERACTIVE,
    )

    best: tuple[int, str, int] | None = None
    for rel_path, line_number, line_text in rows:
        score = _symbol_match_score(
            symbol_name=symbol_name,
            language=language,
            file_path=rel_path,
            line_text=line_text,
        )
        if best is None or score > best[0]:
            best = (score, rel_path, line_number)
    if best is None:
        return None
    return best[1], best[2]


def _symbol_match_score(
    *,
    symbol_name: str,
    language: str,
    file_path: str,
    line_text: str,
) -> int:
    name = re.escape(symbol_name)
    score = 100
    if language == "python":
        if re.search(rf"\bdef\s+{name}\b", line_text):
            score += 400
        elif re.search(rf"\bclass\s+{name}\b", line_text):
            score += 350
        elif re.search(rf"\b{name}\s*=", line_text):
            score += 200
    elif language == "rust":
        if re.search(rf"\bfn\s+{name}\b", line_text):
            score += 400
        elif re.search(rf"\b(struct|enum|trait)\s+{name}\b", line_text):
            score += 350

    if f"'{symbol_name}'" in line_text or f'"{symbol_name}"' in line_text:
        score -= 200
    if file_path.startswith("tests/"):
        score -= 20
    return score


def _to_uri(root: Path, relative_path: str) -> str | None:
    if not relative_path:
        return None
    candidate = (root / relative_path).resolve()
    return candidate.as_uri()


__all__ = [
    "ResolutionKind",
    "ResolvedTarget",
    "resolve_target",
]
