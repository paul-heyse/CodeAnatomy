"""Sampling helpers for OpenTelemetry."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from opentelemetry.context import Context
from opentelemetry.sdk.trace.sampling import Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind, TraceState
from opentelemetry.util.types import Attributes


@dataclass(frozen=True)
class _SamplingArgs:
    parent_context: Context | None
    trace_id: int
    name: str
    kind: SpanKind | None
    attributes: Attributes | None
    links: Sequence[Link] | None
    trace_state: TraceState | None


_SAMPLING_PARAM_NAMES = (
    "parent_context",
    "trace_id",
    "name",
    "kind",
    "attributes",
    "links",
    "trace_state",
)


def _coerce_sampling_args(
    args: tuple[object, ...],
    kwargs: Mapping[str, object],
) -> _SamplingArgs:
    if len(args) > len(_SAMPLING_PARAM_NAMES):
        msg = "should_sample received too many positional arguments"
        raise TypeError(msg)
    values: dict[str, object] = dict(zip(_SAMPLING_PARAM_NAMES, args, strict=False))
    for key, value in kwargs.items():
        if key not in _SAMPLING_PARAM_NAMES:
            msg = f"should_sample received unexpected keyword argument {key!r}"
            raise TypeError(msg)
        if key in values:
            msg = f"should_sample received multiple values for {key!r}"
            raise TypeError(msg)
        values[key] = value
    trace_id_value = values.get("trace_id")
    name_value = values.get("name")
    if trace_id_value is None or name_value is None:
        msg = "should_sample missing required positional arguments"
        raise TypeError(msg)
    if not isinstance(trace_id_value, int):
        msg = "should_sample trace_id must be an int"
        raise TypeError(msg)
    if not isinstance(name_value, str):
        msg = "should_sample name must be a str"
        raise TypeError(msg)
    return _SamplingArgs(
        parent_context=cast("Context | None", values.get("parent_context")),
        trace_id=trace_id_value,
        name=name_value,
        kind=cast("SpanKind | None", values.get("kind")),
        attributes=cast("Attributes | None", values.get("attributes")),
        links=cast("Sequence[Link] | None", values.get("links")),
        trace_state=cast("TraceState | None", values.get("trace_state")),
    )


class SamplingRuleSampler(Sampler):
    """Sampler wrapper that stamps a sampling rule attribute."""

    _ATTRIBUTE_KEY = "otel.sampling.rule"

    def __init__(self, delegate: Sampler, rule: str) -> None:
        self._delegate = delegate
        self._rule = rule

    def should_sample(self, *args: object, **kwargs: object) -> SamplingResult:
        """Apply the delegate sampler and attach the sampling rule attribute.

        Returns
        -------
        SamplingResult
            Sampling decision with a stamped sampling rule attribute.
        """
        sampling_args = _coerce_sampling_args(args, kwargs)
        result = self._delegate.should_sample(
            parent_context=sampling_args.parent_context,
            trace_id=sampling_args.trace_id,
            name=sampling_args.name,
            kind=sampling_args.kind,
            attributes=sampling_args.attributes,
            links=sampling_args.links,
            trace_state=sampling_args.trace_state,
        )
        attrs = dict(result.attributes or {})
        attrs.setdefault(self._ATTRIBUTE_KEY, self._rule)
        return SamplingResult(result.decision, attrs, result.trace_state)

    def get_description(self) -> str:
        """Return a human-readable description of the sampler.

        Returns
        -------
        str
            Description of the sampler.
        """
        return f"SamplingRuleSampler({self._delegate.get_description()})"


def wrap_sampler(base: Sampler, *, rule: str | None) -> Sampler:
    """Wrap a sampler with sampling rule stamping when configured.

    Returns
    -------
    Sampler
        The base sampler or a wrapper that stamps the sampling rule.
    """
    if rule is None:
        return base
    rule_value = rule.strip()
    if not rule_value:
        return base
    return SamplingRuleSampler(base, rule_value)


__all__ = ["SamplingRuleSampler", "wrap_sampler"]
