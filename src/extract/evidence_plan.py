"""Inference-first evidence plan helpers for extraction gating."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from arrowdsl.core.scan_telemetry import ScanTelemetry
from datafusion_engine.extract_metadata import extract_metadata_by_name, extract_metadata_specs


@dataclass(frozen=True)
class EvidenceRequirement:
    """Requirement for an extract dataset and its required columns."""

    name: str
    required_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class EvidencePlan:
    """Evidence plan describing required extract datasets."""

    sources: tuple[str, ...]
    requirements: tuple[EvidenceRequirement, ...] = ()
    required_columns: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    telemetry: Mapping[str, Mapping[str, ScanTelemetry]] = field(default_factory=dict)

    def requires_dataset(self, name: str) -> bool:
        """Return whether the dataset name is required.

        Returns
        -------
        bool
            True when the dataset is required by the evidence plan.
        """
        if name in self.sources:
            return True
        return _expanded_dataset_name(name) in self.sources

    def requires_template(self, template: str) -> bool:
        """Return whether any dataset for a template is required.

        Returns
        -------
        bool
            True when any dataset for the template is required.
        """
        if not template:
            return False
        for row in extract_metadata_specs():
            if row.template != template:
                continue
            if row.name in self.sources or row.output_name() in self.sources:
                return True
        return False

    def required_columns_for(self, name: str) -> tuple[str, ...]:
        """Return required columns for a dataset name.

        Returns
        -------
        tuple[str, ...]
            Required columns for the dataset, if any.
        """
        if name in self.required_columns:
            return tuple(self.required_columns[name])
        resolved = _expanded_dataset_name(name)
        if resolved in self.required_columns:
            return tuple(self.required_columns[resolved])
        return ()


def compile_evidence_plan(
    rules: Sequence[object] | None = None,
    *,
    extra_sources: Sequence[str] = (),
    required_columns: Mapping[str, Sequence[str]] | None = None,
    telemetry_provider: Callable[[str], ScanTelemetry | None] | None = None,
) -> EvidencePlan:
    """Compile an evidence plan from inferred outputs.

    Parameters
    ----------
    rules : Sequence[object] | None
        Optional objects or output names to infer required datasets from.
    extra_sources : Sequence[str]
        Additional dataset sources to require.
    required_columns : Mapping[str, Sequence[str]] | None
        Optional required column overrides by dataset name.
    telemetry_provider : Callable[[str], ScanTelemetry | None] | None
        Optional telemetry provider for scan telemetry capture.

    Returns
    -------
    EvidencePlan
        Evidence plan describing required datasets and columns.
    """
    outputs = _outputs_from_rules(rules)
    if not outputs:
        outputs = _all_extract_outputs()
    sources = _expand_sources(tuple(outputs) + tuple(extra_sources))
    required_map = {
        key: tuple(value) for key, value in (required_columns or {}).items() if value is not None
    }
    requirements = tuple(
        EvidenceRequirement(name=source, required_columns=required_map.get(source, ()))
        for source in sources
    )
    telemetry = _telemetry_payload(sources, telemetry_provider=telemetry_provider)
    return EvidencePlan(
        sources=sources,
        requirements=requirements,
        required_columns=required_map,
        telemetry=telemetry,
    )


def _outputs_from_rules(rules: Sequence[object] | None) -> tuple[str, ...]:
    if not rules:
        return ()
    outputs: list[str] = []
    for item in rules:
        if isinstance(item, str):
            outputs.append(item)
            continue
        output = getattr(item, "output", None)
        if not isinstance(output, str) or not output:
            output = getattr(item, "output_dataset", None)
        if not isinstance(output, str) or not output:
            task = getattr(item, "task", None)
            if task is not None:
                output = getattr(task, "output", None)
        if isinstance(output, str) and output:
            outputs.append(output)
    return tuple(dict.fromkeys(outputs))


def _all_extract_outputs() -> tuple[str, ...]:
    return tuple(dict.fromkeys(row.output_name() for row in extract_metadata_specs()))


def _expanded_dataset_name(name: str) -> str:
    metadata = extract_metadata_by_name()
    row = metadata.get(name)
    if row is not None:
        return row.output_name()
    for candidate in metadata.values():
        if candidate.output_name() == name:
            return candidate.name
    return name


def _expand_sources(sources: Sequence[str]) -> tuple[str, ...]:
    expanded: list[str] = []
    metadata = extract_metadata_by_name()
    output_map = {row.output_name(): row.name for row in metadata.values()}
    for source in sources:
        if not source:
            continue
        expanded.append(source)
        row = metadata.get(source)
        if row is not None:
            expanded.append(row.output_name())
            continue
        mapped = output_map.get(source)
        if mapped is not None:
            expanded.append(mapped)
    return tuple(dict.fromkeys(expanded))


def _telemetry_payload(
    sources: Sequence[str],
    *,
    telemetry_provider: Callable[[str], ScanTelemetry | None] | None,
) -> Mapping[str, Mapping[str, ScanTelemetry]]:
    if telemetry_provider is None:
        return {}
    telemetry: dict[str, dict[str, ScanTelemetry]] = {}
    for source in sources:
        info = telemetry_provider(source)
        if info is None:
            continue
        telemetry.setdefault(source, {})[source] = info
    return telemetry


__all__ = ["EvidencePlan", "EvidenceRequirement", "compile_evidence_plan"]
