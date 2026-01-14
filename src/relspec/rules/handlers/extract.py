"""Extract rule handler for centralized compilation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.core.interop import TableLike
from extract.registry_pipelines import post_kernels_for_postprocess
from relspec.rules.compiler import RuleHandler
from relspec.rules.definitions import ExtractPayload, RuleStage

if TYPE_CHECKING:
    from arrowdsl.core.context import ExecutionContext
    from relspec.rules.definitions import RuleDefinition, RuleDomain


@dataclass(frozen=True)
class ExtractRuleCompilation:
    """Compiled extract rule state for pipeline execution."""

    definition: RuleDefinition
    payload: ExtractPayload
    pipeline_ops: tuple[Mapping[str, object], ...]
    post_kernels: tuple[Callable[[TableLike], TableLike], ...]
    stages: tuple[RuleStage, ...]


@dataclass(frozen=True)
class ExtractRuleHandler(RuleHandler):
    """Compile extract rule definitions into executable plan metadata."""

    domain: RuleDomain = "extract"

    @staticmethod
    def compile_rule(
        rule: RuleDefinition,
        *,
        ctx: ExecutionContext,
    ) -> ExtractRuleCompilation:
        """Compile an extract rule definition into pipeline metadata.

        Returns
        -------
        ExtractRuleCompilation
            Compilation payload for extract execution.
        """
        _ = ctx
        payload = _extract_payload(rule)
        stages = rule.stages or _default_stages(payload)
        return ExtractRuleCompilation(
            definition=rule,
            payload=payload,
            pipeline_ops=rule.pipeline_ops,
            post_kernels=post_kernels_for_postprocess(payload.postprocess),
            stages=stages,
        )


def _extract_payload(rule: RuleDefinition) -> ExtractPayload:
    payload = rule.payload
    if not isinstance(payload, ExtractPayload):
        msg = f"RuleDefinition {rule.name!r} missing extract payload."
        raise TypeError(msg)
    return payload


def _default_stages(payload: ExtractPayload) -> tuple[RuleStage, ...]:
    enabled_when = _stage_enabled_when(payload)
    stages = [RuleStage(name="source", mode="source", enabled_when=enabled_when)]
    if payload.postprocess:
        stages.append(RuleStage(name=payload.postprocess, mode="post_kernel"))
    return tuple(stages)


def _stage_enabled_when(payload: ExtractPayload) -> str | None:
    if payload.enabled_when is None:
        return None
    if payload.enabled_when == "feature_flag" and payload.feature_flag:
        return f"feature_flag:{payload.feature_flag}"
    return payload.enabled_when


__all__ = ["ExtractRuleCompilation", "ExtractRuleHandler"]
