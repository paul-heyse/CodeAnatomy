"""Import-query finding helpers extracted from executor_definitions."""

from __future__ import annotations

from collections.abc import Callable

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.schema import Anchor, Finding
from tools.cq.core.scoring import build_detail_payload
from tools.cq.query.import_utils import (
    extract_from_import_alias,
    extract_from_import_name,
    extract_from_module_name,
    extract_rust_use_import_name,
    extract_simple_import_alias,
    extract_simple_import_name,
)


def import_match_key(import_record: SgRecord) -> tuple[str, int, int, int, int, str]:
    """Build stable dedupe key for import-query findings.

    Returns:
        Stable hashable key for one import record.
    """
    return (
        import_record.file,
        import_record.start_line,
        import_record.start_col,
        import_record.end_line,
        import_record.end_col,
        extract_import_name(import_record) or import_record.text.strip(),
    )


def dedupe_import_matches(import_records: list[SgRecord]) -> list[SgRecord]:
    """Drop duplicate import records emitted by overlapping rules.

    Returns:
        Deduplicated import records preserving first occurrence.
    """
    deduped: dict[tuple[str, int, int, int, int, str], SgRecord] = {}
    for import_record in import_records:
        key = import_match_key(import_record)
        if key not in deduped:
            deduped[key] = import_record
    return list(deduped.values())


def extract_import_name(record: SgRecord) -> str | None:
    """Extract imported name from an import record.

    Returns:
        Extracted import symbol or module name when available.
    """
    text = record.text.strip()
    kind = record.kind
    extractor_by_kind: dict[str, Callable[[str], str | None]] = {
        "import": extract_simple_import_name,
        "import_as": extract_simple_import_alias,
        "from_import": extract_from_import_name,
        "from_import_as": extract_from_import_alias,
        "from_import_multi": extract_from_module_name,
        "from_import_paren": extract_from_module_name,
        "use_declaration": extract_rust_use_import_name,
    }
    extractor = extractor_by_kind.get(kind)
    if extractor is None:
        return None
    return extractor(text)


def import_to_finding(import_record: SgRecord) -> Finding:
    """Convert an import record to a finding.

    Returns:
        Normalized import finding payload.
    """
    import_name = extract_import_name(import_record) or "unknown"
    anchor = Anchor(
        file=import_record.file,
        line=import_record.start_line,
        col=import_record.start_col,
        end_line=import_record.end_line,
        end_col=import_record.end_col,
    )
    category = (
        "from_import"
        if import_record.kind
        in {
            "from_import",
            "from_import_as",
            "from_import_multi",
            "from_import_paren",
        }
        else "import"
    )
    return Finding(
        category=category,
        message=f"{category}: {import_name}",
        anchor=anchor,
        severity="info",
        details=build_detail_payload(
            data={
                "kind": import_record.kind,
                "name": import_name,
                "text": import_record.text.strip(),
            }
        ),
    )


__all__ = [
    "dedupe_import_matches",
    "extract_import_name",
    "import_match_key",
    "import_to_finding",
]
