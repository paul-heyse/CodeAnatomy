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
        symbol_match, symbol_degrades = _resolve_symbol_with_rg(root, symbol_name, language)
        if symbol_match is not None:
            file_path, line = symbol_match
            degrades.extend(symbol_degrades)
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


def _resolve_symbol_with_rg(
    root: Path, symbol_name: str, language: str
) -> tuple[tuple[str, int] | None, tuple[DegradeEventV1, ...]]:
    """Resolve first symbol occurrence via ripgrep scoped by language.

    Returns:
    -------
    tuple[tuple[str, int] | None, tuple[DegradeEventV1, ...]]
        Best relative file path and 1-based line number plus degrade events.
    """
    rows = find_symbol_candidates(
        root=root,
        symbol_name=symbol_name,
        lang_scope="rust" if language == "rust" else "python",
        limits=INTERACTIVE,
    )

    ranked: list[tuple[int, str, int, str]] = []
    for rel_path, line_number, line_text in rows:
        score = _symbol_match_score(
            symbol_name=symbol_name,
            language=language,
            file_path=rel_path,
            line_text=line_text,
        )
        ranked.append((score, rel_path, line_number, line_text))
    if not ranked:
        return None, ()

    ranked.sort(key=lambda row: (-row[0], row[1], row[2], row[3]))
    best_score, best_file, best_line, _ = ranked[0]
    tied = [row for row in ranked if row[0] == best_score]

    degrades: list[DegradeEventV1] = []
    if language == "rust" and len(tied) > 1:
        degrades.append(
            DegradeEventV1(
                stage="target_resolution",
                severity="info",
                category="ambiguous_symbol",
                message=(
                    f"Multiple Rust symbol candidates for {symbol_name}; "
                    f"chose best-ranked {best_file}:{best_line}"
                ),
            )
        )
    return (best_file, best_line), tuple(degrades)


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
        score += _python_symbol_score_delta(name=name, line_text=line_text)
    elif language == "rust":
        score += _rust_symbol_score_delta(name=name, line_text=line_text)
    score += _symbol_penalty(symbol_name=symbol_name, file_path=file_path, line_text=line_text)
    return score


def _python_symbol_score_delta(*, name: str, line_text: str) -> int:
    if re.search(rf"\bdef\s+{name}\b", line_text):
        return 400
    if re.search(rf"\bclass\s+{name}\b", line_text):
        return 350
    if re.search(rf"\b{name}\s*=", line_text):
        return 200
    return 0


def _rust_symbol_score_delta(*, name: str, line_text: str) -> int:
    score = 0
    if re.search(rf"^\s*(pub(\([^)]*\))?\s+)?(async\s+)?fn\s+{name}\b", line_text):
        score += 600
    elif re.search(rf"^\s*fn\s+{name}\b", line_text):
        score += 500
    elif re.search(rf"^\s*(pub(\([^)]*\))?\s+)?(struct|enum|trait)\s+{name}\b", line_text):
        score += 350
    if re.search(r"^\s*impl\b", line_text):
        score -= 120
    leading_spaces = len(line_text) - len(line_text.lstrip())
    if leading_spaces > 0:
        score -= min(leading_spaces, 8) * 5
    return score


def _symbol_penalty(*, symbol_name: str, file_path: str, line_text: str) -> int:
    penalty = 0
    if f"'{symbol_name}'" in line_text or f'"{symbol_name}"' in line_text:
        penalty -= 200
    if file_path.startswith("tests/"):
        penalty -= 20
    return penalty


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
