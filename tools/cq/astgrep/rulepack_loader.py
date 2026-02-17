"""YAML rule pack loader for ast-grep-py runtime rules."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import msgspec

from tools.cq.astgrep.sgpy_scanner import RecordType, RuleSpec
from tools.cq.core.structs import CqStruct
from tools.cq.core.typed_boundary import BoundaryDecodeError, decode_yaml_strict

_VALID_RECORD_TYPES: set[str] = {"def", "call", "import", "raise", "except", "assign_ctor"}


class CliRuleMetadata(CqStruct, frozen=True):
    """Metadata section from ast-grep CLI YAML rule format."""

    record: str = ""
    kind: str = ""


class CliRuleFile(CqStruct, frozen=True):
    """Single ast-grep CLI YAML rule file schema."""

    id: str
    language: str = "python"
    severity: str = "hint"
    rule: dict[str, object] = msgspec.field(default_factory=dict)
    metadata: CliRuleMetadata = msgspec.field(default_factory=CliRuleMetadata)


class UtilityRuleFile(CqStruct, frozen=True):
    """Utility matcher YAML schema."""

    id: str
    language: str = "python"
    rule: dict[str, object] = msgspec.field(default_factory=dict)


def load_cli_rule_file(
    path: Path,
    *,
    utils_by_language: dict[str, dict[str, dict[str, object]]] | None = None,
) -> RuleSpec | None:
    """Load a single CLI-mode YAML rule file and convert it to ``RuleSpec``.

    Returns:
    -------
    RuleSpec | None
        Converted rule when decoding succeeds, otherwise ``None``.
    """
    try:
        parsed = decode_yaml_strict(path.read_bytes(), type_=CliRuleFile)
    except (OSError, BoundaryDecodeError):
        return None

    record = parsed.metadata.record
    kind = parsed.metadata.kind
    if record not in _VALID_RECORD_TYPES or not kind:
        return None

    config: dict[str, object] = {"rule": dict(parsed.rule)}
    if utils_by_language:
        lang_utils = utils_by_language.get(parsed.language, {})
        if lang_utils:
            config["utils"] = {util_id: dict(rule) for util_id, rule in sorted(lang_utils.items())}

    return RuleSpec(
        rule_id=parsed.id,
        record_type=cast("RecordType", record),
        kind=kind,
        config=config,
    )


def load_utils(utils_dir: Path) -> dict[str, dict[str, dict[str, object]]]:
    """Load shared utility rules grouped by language and utility id.

    Returns:
    -------
    dict[str, dict[str, dict[str, object]]]
        Mapping of language to utility-rule definitions.
    """
    if not utils_dir.is_dir():
        return {}

    rows: dict[str, dict[str, dict[str, object]]] = {}
    for yaml_file in sorted(utils_dir.glob("*.yml")):
        try:
            parsed = decode_yaml_strict(yaml_file.read_bytes(), type_=UtilityRuleFile)
        except (OSError, BoundaryDecodeError):
            continue
        if not parsed.id or not parsed.rule:
            continue
        lang_rows = rows.setdefault(parsed.language, {})
        lang_rows[parsed.id] = dict(parsed.rule)
    return rows


def load_rules_from_directory(
    rule_dir: Path,
    *,
    utils_by_language: dict[str, dict[str, dict[str, object]]] | None = None,
) -> tuple[RuleSpec, ...]:
    """Load all YAML rule files from a directory into a ``RuleSpec`` tuple.

    Returns:
    -------
    tuple[RuleSpec, ...]
        Loaded and converted rules sorted by filename.
    """
    if not rule_dir.is_dir():
        return ()
    specs: list[RuleSpec] = []
    for yaml_file in sorted(rule_dir.glob("*.yml")):
        spec = load_cli_rule_file(yaml_file, utils_by_language=utils_by_language)
        if spec is not None:
            specs.append(spec)
    return tuple(specs)


def load_default_rulepacks(*, base: Path | None = None) -> dict[str, tuple[RuleSpec, ...]]:
    """Load all built-in rule packs from ``tools/cq/astgrep/rules``.

    Returns:
    -------
    dict[str, tuple[RuleSpec, ...]]
        Mapping of language to loaded rule tuples.
    """
    resolved_base = (base or Path(__file__).parent).resolve()
    rules_base = resolved_base / "rules"
    utils = load_utils(resolved_base / "utils")

    packs: dict[str, tuple[RuleSpec, ...]] = {}
    for facts_dir in sorted(rules_base.glob("*_facts")):
        lang = facts_dir.name.removesuffix("_facts")
        rules = load_rules_from_directory(facts_dir, utils_by_language=utils)
        if rules:
            packs[lang] = rules
    return packs


__all__ = [
    "CliRuleFile",
    "CliRuleMetadata",
    "UtilityRuleFile",
    "load_cli_rule_file",
    "load_default_rulepacks",
    "load_rules_from_directory",
    "load_utils",
]
