"""Extract task planning helpers for relspec scheduling."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from datafusion_engine.extract.adapter_registry import additional_required_inputs_for_template
from datafusion_engine.extract.bundles import dataset_name_for_output
from datafusion_engine.extract.extractors import ExtractorSpec, extractor_specs
from datafusion_engine.extract.metadata import ExtractMetadata, extract_metadata_specs
from relspec.inferred_deps import InferredDeps
from serde_msgspec import StructBaseStrict
from utils.hashing import hash_msgpack_canonical

_EXTRACT_PLAN_FINGERPRINT_VERSION = 1


class ExtractTaskSpec(StructBaseStrict, frozen=True):
    """Specification for an extract task template."""

    name: str
    outputs: tuple[str, ...]
    required_inputs: tuple[str, ...]
    supports_plan: bool


class ExtractOutputTask(StructBaseStrict, frozen=True):
    """Concrete task for a single extract output."""

    name: str
    output: str
    extractor: str
    required_inputs: tuple[str, ...]
    plan_fingerprint: str


def extract_plan_fingerprint(
    *,
    extractor: str,
    output: str,
    inputs: Sequence[str],
) -> str:
    """Return a stable plan fingerprint for an extract output task.

    Parameters
    ----------
    extractor
        Extractor template name.
    output
        Output dataset name.
    inputs
        Required inputs for the task.

    Returns:
    -------
    str
        Deterministic plan fingerprint.
    """
    payload = (
        ("version", _EXTRACT_PLAN_FINGERPRINT_VERSION),
        ("extractor", extractor),
        ("output", output),
        ("inputs", tuple(inputs)),
    )
    return hash_msgpack_canonical(payload)


def build_extract_tasks() -> tuple[ExtractTaskSpec, ...]:
    """Return extract task specs derived from registry metadata.

    Returns:
    -------
    tuple[ExtractTaskSpec, ...]
        Extract task specs by extractor template.
    """
    rows_by_template = _metadata_by_template()
    tasks: list[ExtractTaskSpec] = []
    for spec in extractor_specs():
        rows = rows_by_template.get(spec.template, ())
        outputs = tuple(row.name for row in rows)
        required_inputs = _resolve_required_inputs(spec)
        tasks.append(
            ExtractTaskSpec(
                name=spec.name,
                outputs=outputs,
                required_inputs=required_inputs,
                supports_plan=spec.supports_plan,
            )
        )
    return tuple(tasks)


def extract_output_tasks() -> tuple[ExtractOutputTask, ...]:
    """Return extract output tasks derived from extractor specs.

    Returns:
    -------
    tuple[ExtractOutputTask, ...]
        Output-scoped task specs for extract scheduling.
    """
    tasks = [task for spec in build_extract_tasks() for task in _output_tasks_for_spec(spec)]
    return tuple(sorted(tasks, key=lambda item: item.name))


def extract_output_task_map() -> Mapping[str, ExtractOutputTask]:
    """Return extract output tasks keyed by dataset name.

    Returns:
    -------
    Mapping[str, ExtractOutputTask]
        Mapping of output dataset name to extract task specification.
    """
    return {task.output: task for task in extract_output_tasks()}


def extract_inferred_deps() -> tuple[InferredDeps, ...]:
    """Return inferred dependency records for extract outputs.

    Returns:
    -------
    tuple[InferredDeps, ...]
        Inferred dependency entries for extract tasks.
    """
    return tuple(
        InferredDeps(
            task_name=task.name,
            output=task.output,
            inputs=task.required_inputs,
            required_columns={},
            required_types={},
            required_metadata={},
            plan_fingerprint=task.plan_fingerprint,
        )
        for task in extract_output_tasks()
    )


def extract_task_kind_map() -> dict[str, str]:
    """Return task kind mapping for extract outputs.

    Returns:
    -------
    dict[str, str]
        Mapping of task name to task kind label.
    """
    return {task.name: "extract" for task in extract_output_tasks()}


def _metadata_by_template() -> Mapping[str, tuple[ExtractMetadata, ...]]:
    grouped: dict[str, list[ExtractMetadata]] = {}
    for row in extract_metadata_specs():
        if row.template is None:
            continue
        grouped.setdefault(row.template, []).append(row)
    return {key: tuple(rows) for key, rows in grouped.items()}


def _resolve_required_inputs(spec: ExtractorSpec) -> tuple[str, ...]:
    merged = tuple(
        dict.fromkeys(
            (
                *spec.required_inputs,
                *additional_required_inputs_for_template(spec.name),
            )
        )
    )
    return tuple(_resolve_input_name(name) for name in merged)


def _resolve_input_name(name: str) -> str:
    try:
        dataset = dataset_name_for_output(name)
    except KeyError:
        return name
    if dataset is None:
        return name
    return dataset


def _output_tasks_for_spec(spec: ExtractTaskSpec) -> list[ExtractOutputTask]:
    if not spec.outputs:
        return []
    primary = spec.outputs[0]
    tasks: list[ExtractOutputTask] = []
    for output in spec.outputs:
        inputs = spec.required_inputs
        if output != primary:
            inputs = tuple(dict.fromkeys((*inputs, primary)))
        fingerprint = extract_plan_fingerprint(
            extractor=spec.name,
            output=output,
            inputs=inputs,
        )
        tasks.append(
            ExtractOutputTask(
                name=output,
                output=output,
                extractor=spec.name,
                required_inputs=inputs,
                plan_fingerprint=fingerprint,
            )
        )
    return tasks


__all__ = [
    "ExtractOutputTask",
    "ExtractTaskSpec",
    "build_extract_tasks",
    "extract_inferred_deps",
    "extract_output_task_map",
    "extract_output_tasks",
    "extract_plan_fingerprint",
    "extract_task_kind_map",
]
