"""Evidence-driven extraction plan compilation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from extract.evidence_specs import EvidenceSpec as ExtractEvidenceSpec
from extract.evidence_specs import evidence_spec
from extract.normalize_ops import normalize_ops_for_output
from relspec.model import EvidenceSpec as RuleEvidenceSpec
from relspec.model import RelationshipRule


@dataclass(frozen=True)
class EvidenceRequirement:
    """Required evidence dataset and columns."""

    name: str
    alias: str
    template: str | None
    required_columns: tuple[str, ...]


@dataclass(frozen=True)
class EvidencePlan:
    """Compiled evidence plan describing required datasets and ops."""

    sources: tuple[str, ...]
    normalize_ops: tuple[str, ...]
    requirements: tuple[EvidenceRequirement, ...]

    def templates(self) -> tuple[str, ...]:
        """Return required extractor templates.

        Returns
        -------
        tuple[str, ...]
            Required template names.
        """
        return tuple(
            sorted({req.template for req in self.requirements if req.template is not None})
        )

    def requires_template(self, template: str) -> bool:
        """Return whether a template is required.

        Returns
        -------
        bool
            ``True`` when the template is required.
        """
        return template in self.templates()

    def requires_dataset(self, name: str) -> bool:
        """Return whether a dataset alias/name is required.

        Returns
        -------
        bool
            ``True`` when the dataset is required.
        """
        if name in self.sources:
            return True
        return any(name in {req.name, req.alias} for req in self.requirements)

    def required_columns_for(self, name: str) -> tuple[str, ...]:
        """Return required columns for a dataset alias/name.

        Returns
        -------
        tuple[str, ...]
            Required columns for the dataset.
        """
        for req in self.requirements:
            if name in {req.name, req.alias}:
                return req.required_columns
        return ()

    def datasets_for_template(self, template: str) -> tuple[str, ...]:
        """Return dataset aliases required for a template.

        Returns
        -------
        tuple[str, ...]
            Dataset aliases required for the template.
        """
        return tuple(sorted(req.alias for req in self.requirements if req.template == template))


def compile_evidence_plan(
    rules: Sequence[RelationshipRule],
    *,
    extra_sources: Sequence[str] = (),
) -> EvidencePlan:
    """Compile an evidence plan from relationship rules.

    Returns
    -------
    EvidencePlan
        Evidence plan describing required datasets and normalization ops.
    """
    sources: set[str] = set(extra_sources)
    required_columns: dict[str, set[str]] = {}

    for rule in rules:
        rule_sources = _rule_sources(rule)
        sources.update(rule_sources)
        _merge_required_columns(required_columns, rule_sources, rule.evidence)

    sources, normalize_ops = _expand_normalize_ops(sources)
    requirements = _requirements_from_sources(sources, required_columns)

    return EvidencePlan(
        sources=tuple(sorted(sources)),
        normalize_ops=tuple(sorted(normalize_ops)),
        requirements=requirements,
    )


def _rule_sources(rule: RelationshipRule) -> tuple[str, ...]:
    spec = rule.evidence
    if spec is None:
        return tuple(ref.name for ref in rule.inputs)
    return spec.sources or tuple(ref.name for ref in rule.inputs)


def _merge_required_columns(
    required_columns: dict[str, set[str]],
    sources: Iterable[str],
    spec: RuleEvidenceSpec | None,
) -> None:
    if spec is None or not spec.required_columns:
        return
    for name in sources:
        required_columns.setdefault(name, set()).update(spec.required_columns)


def _expand_normalize_ops(
    sources: set[str],
) -> tuple[set[str], set[str]]:
    normalize_ops: set[str] = set()
    pending = set(sources)
    while pending:
        name = pending.pop()
        for op in normalize_ops_for_output(name):
            normalize_ops.add(op.name)
            for input_name in op.inputs:
                if input_name not in sources:
                    sources.add(input_name)
                    pending.add(input_name)
    return sources, normalize_ops


def _requirements_from_sources(
    sources: Iterable[str],
    required_columns: Mapping[str, set[str]],
) -> tuple[EvidenceRequirement, ...]:
    requirements: list[EvidenceRequirement] = []
    seen: set[str] = set()
    for source in sorted(set(sources)):
        spec = _maybe_spec(source)
        if spec is None:
            continue
        if spec.name in seen:
            continue
        seen.add(spec.name)
        extra_columns = _required_columns_for_source(source, spec, required_columns)
        cols = tuple(sorted(set(spec.required_columns).union(extra_columns)))
        requirements.append(
            EvidenceRequirement(
                name=spec.name,
                alias=spec.alias,
                template=spec.template,
                required_columns=cols,
            )
        )
    return tuple(requirements)


def _maybe_spec(name: str) -> ExtractEvidenceSpec | None:
    try:
        return evidence_spec(name)
    except KeyError:
        return None


def _required_columns_for_source(
    source: str,
    spec: ExtractEvidenceSpec,
    required_columns: Mapping[str, set[str]],
) -> set[str]:
    extra: set[str] = set()
    for key in (source, spec.alias, spec.name):
        extra.update(required_columns.get(key, set()))
    return extra


__all__ = ["EvidencePlan", "EvidenceRequirement", "compile_evidence_plan"]
