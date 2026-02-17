"""Details-kind expansion macro."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.details_kinds import resolve_kind
from tools.cq.core.result_factory import build_error_result
from tools.cq.core.schema import Finding, Section, ms
from tools.cq.core.scoring import build_detail_payload
from tools.cq.macros.result_builder import MacroResultBuilder

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult
    from tools.cq.core.toolchain import Toolchain

ExpanderFn = Callable[[dict[str, object]], dict[str, object]]


def _identity_expand(handle: dict[str, object]) -> dict[str, object]:
    payload = handle.get("payload")
    if isinstance(payload, dict):
        return payload
    return dict(handle)


EXPANDERS: dict[str, ExpanderFn] = {
    "sym.scope_graph": _identity_expand,
    "sym.partitions": _identity_expand,
    "sym.binding_resolve": _identity_expand,
    "dis.cfg": _identity_expand,
    "dis.anchor_metrics": _identity_expand,
    "inspect.object_inventory": _identity_expand,
    "inspect.callsite_bind_check": _identity_expand,
}


def cmd_expand(
    *,
    root: Path,
    argv: list[str],
    tc: Toolchain | None,
    kind: str,
    handle: dict[str, object],
) -> CqResult:
    """Expand a details-kind handle into full payload.

    Returns:
        CqResult: Expansion result containing expanded details finding output.
    """
    spec = resolve_kind(kind)
    if spec is None:
        return build_error_result(
            macro="expand",
            root=root,
            argv=argv,
            tc=tc,
            started_ms=ms(),
            error=f"Unknown details kind: {kind}",
        )
    expander = EXPANDERS.get(kind)
    if expander is None:
        return build_error_result(
            macro="expand",
            root=root,
            argv=argv,
            tc=tc,
            started_ms=ms(),
            error=f"No expander registered for details kind: {kind}",
        )
    try:
        payload = expander(handle)
    except (RuntimeError, TypeError, ValueError) as exc:
        return build_error_result(
            macro="expand",
            root=root,
            argv=argv,
            tc=tc,
            started_ms=ms(),
            error=f"Failed to expand details payload: {exc}",
        )

    finding = Finding(
        category="details_expand",
        message=f"Expanded {kind}",
        severity="info",
        details=build_detail_payload(
            kind=kind,
            data={
                "kind": kind,
                "version": spec.version,
                "expanded": payload,
            },
        ),
    )
    builder = MacroResultBuilder(
        "expand",
        root=root,
        argv=argv,
        tc=tc,
    )
    builder.add_findings([finding])
    builder.add_section(Section(title="Expanded Details", findings=(finding,)))
    builder.set_summary(mode="expand", query=kind)
    return builder.build()


__all__ = ["cmd_expand"]
