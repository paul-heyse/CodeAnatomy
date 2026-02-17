"""Call-target resolution helpers."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.macros.calls_target import resolve_target_definition, resolve_target_metadata

if TYPE_CHECKING:
    from tools.cq.core.types import QueryLanguage
    from tools.cq.macros.calls_target import AttachTargetMetadataRequestV1, TargetMetadataResultV1

__all__ = ["resolve_call_target", "resolve_call_target_metadata"]


def resolve_call_target(
    root: Path,
    function_name: str,
    *,
    target_language: QueryLanguage | None = None,
) -> tuple[str, int] | None:
    """Resolve call target definition location.

    Returns:
        tuple[str, int] | None: Target file/line location when resolvable.
    """
    return resolve_target_definition(
        root=root, function_name=function_name, target_language=target_language
    )


def resolve_call_target_metadata(request: AttachTargetMetadataRequestV1) -> TargetMetadataResultV1:
    """Resolve typed target metadata payload.

    Returns:
        object: Metadata payload describing the resolved target.
    """
    return resolve_target_metadata(request)
