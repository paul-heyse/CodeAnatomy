"""Hamilton tag policy helpers for consistent metadata tagging."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TypeVar

from hamilton.function_modifiers import tag

from core.config_base import FingerprintableConfig, config_fingerprint

F = TypeVar("F", bound=Callable[..., object])


@dataclass(frozen=True)
class TagPolicy(FingerprintableConfig):
    """Define a consistent tag payload for Hamilton nodes."""

    layer: str
    kind: str
    artifact: str | None = None
    semantic_id: str | None = None
    entity: str | None = None
    grain: str | None = None
    version: str | None = None
    stability: str | None = None
    schema_ref: str | None = None
    materialization: str | None = None
    materialized_name: str | None = None
    entity_keys: Sequence[str] = ()
    join_keys: Sequence[str] = ()
    extra_tags: Mapping[str, str] | None = None

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the tag policy.

        Returns
        -------
        Mapping[str, object]
            Payload describing tag policy settings.
        """
        extra_payload = dict(self.extra_tags) if self.extra_tags is not None else None
        return {
            "layer": self.layer,
            "kind": self.kind,
            "artifact": self.artifact,
            "semantic_id": self.semantic_id,
            "entity": self.entity,
            "grain": self.grain,
            "version": self.version,
            "stability": self.stability,
            "schema_ref": self.schema_ref,
            "materialization": self.materialization,
            "materialized_name": self.materialized_name,
            "entity_keys": tuple(self.entity_keys),
            "join_keys": tuple(self.join_keys),
            "extra_tags": extra_payload,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the tag policy.

        Returns
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())

    def as_tags(self, *, artifact: str | None = None) -> dict[str, str | list[str]]:
        """Return a normalized tag payload.

        Parameters
        ----------
        artifact
            Override artifact name for this tag policy.

        Returns
        -------
        dict[str, str | list[str]]
            Tag payload for Hamilton metadata.

        Raises
        ------
        ValueError
            Raised when no artifact name is available.
        """
        resolved_artifact = artifact or self.artifact
        if resolved_artifact is None:
            msg = "TagPolicy requires an artifact name."
            raise ValueError(msg)
        tags: dict[str, str | list[str]] = {
            "layer": self.layer,
            "kind": self.kind,
            "artifact": resolved_artifact,
        }
        optional: dict[str, str | None] = {
            "semantic_id": self.semantic_id,
            "entity": self.entity,
            "grain": self.grain,
            "version": self.version,
            "stability": self.stability,
            "schema_ref": self.schema_ref,
            "materialization": self.materialization,
            "materialized_name": self.materialized_name,
        }
        for key, value in optional.items():
            if value is not None:
                tags[key] = str(value)
        if self.entity_keys:
            tags["entity_keys"] = _join_tags(self.entity_keys)
        if self.join_keys:
            tags["join_keys"] = _join_tags(self.join_keys)
        if self.extra_tags:
            tags.update({str(key): str(value) for key, value in self.extra_tags.items()})
        return tags


def apply_tag(policy: TagPolicy) -> Callable[[F], F]:
    """Apply a TagPolicy to a Hamilton function.

    Parameters
    ----------
    policy
        Tag policy to apply.

    Returns
    -------
    Callable[[F], F]
        Decorator that injects Hamilton tags.
    """

    def decorator(fn: F) -> F:
        tag_decorator = tag(bypass_reserved_namespaces_=False)
        tag_decorator.tags.update(policy.as_tags(artifact=policy.artifact or fn.__name__))
        return tag_decorator(fn)

    return decorator


def tag_outputs_payloads(
    policies: Mapping[str, TagPolicy],
) -> dict[str, dict[str, str | list[str]]]:
    """Return tag payloads for tag_outputs decorators.

    Parameters
    ----------
    policies
        Mapping of output name to TagPolicy.

    Returns
    -------
    dict[str, dict[str, str | list[str]]]
        Tag payloads keyed by output name.
    """
    payloads: dict[str, dict[str, str | list[str]]] = {}
    for name, policy in policies.items():
        payloads[name] = policy.as_tags(artifact=policy.artifact or name)
    return payloads


def tag_outputs_by_name(
    names: Sequence[str],
    *,
    layer: str,
    kind: str,
) -> dict[str, dict[str, str | list[str]]]:
    """Return tag payloads for output names using the same TagPolicy shape.

    Parameters
    ----------
    names
        Output names to tag.
    layer
        Tag layer for each output.
    kind
        Tag kind for each output.

    Returns
    -------
    dict[str, dict[str, str | list[str]]]
        Tag payloads keyed by output name.
    """
    policies = {name: TagPolicy(layer=layer, kind=kind, artifact=name) for name in names}
    return tag_outputs_payloads(policies)


def _join_tags(values: Sequence[str]) -> str:
    return ",".join(str(value) for value in values if value)


__all__ = ["TagPolicy", "apply_tag", "tag_outputs_by_name", "tag_outputs_payloads"]
