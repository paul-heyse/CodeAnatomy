"""Target parsing and resolution for neighborhood assembly."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from tools.cq.core.snb_schema import DegradeEventV1
from tools.cq.core.structs import CqStruct

_TARGET_PARTS_WITH_COL = 3
_TARGET_PARTS_WITH_LINE = 2
_RG_ROW_MIN_PARTS = 2
_RG_ROW_WITH_TEXT_PARTS = 3


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

    Returns:
    -------
    TargetSpec
        Parsed file/line/column or symbol target metadata.
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
        normalized_file = _normalize_file_path(spec.target_file)
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

    fallback_file = _normalize_file_path(spec.target_file or "")
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
    extensions = ("rs",) if language == "rust" else ("py", "pyi")
    globs: list[str] = []
    for ext in extensions:
        globs.extend(["-g", f"**/*.{ext}"])

    cmd = [
        "rg",
        "--line-number",
        "--no-heading",
        "--fixed-strings",
        symbol_name,
        *globs,
        str(root),
    ]
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if proc.returncode not in {0, 1}:
        return None

    best: tuple[int, str, int] | None = None
    for row in proc.stdout.splitlines():
        parts = row.split(":", 2)
        if len(parts) < _RG_ROW_MIN_PARTS:
            continue
        file_part, line_part = parts[0], parts[1]
        if not file_part or not line_part.isdigit():
            continue
        file_path = Path(file_part)
        try:
            rel = file_path.resolve().relative_to(root.resolve())
        except ValueError:
            rel = file_path
        rel_path = _normalize_file_path(str(rel))
        line_number = max(1, int(line_part))
        line_text = parts[2] if len(parts) >= _RG_ROW_WITH_TEXT_PARTS else ""
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


def _normalize_file_path(path: str) -> str:
    return path.replace("\\", "/").lstrip("./")


def _to_uri(root: Path, relative_path: str) -> str | None:
    if not relative_path:
        return None
    candidate = (root / relative_path).resolve()
    return candidate.as_uri()


__all__ = [
    "ResolvedTarget",
    "TargetSpec",
    "parse_target_spec",
    "resolve_target",
]
