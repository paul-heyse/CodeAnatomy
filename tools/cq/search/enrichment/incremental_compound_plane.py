"""Incremental compound enrichment joins across planes."""

from __future__ import annotations

from collections import defaultdict

from tools.cq.search._shared.helpers import coerce_count


def build_binding_join(
    ts_occurrences: list[dict[str, object]],
    dis_events: list[dict[str, object]],
) -> dict[str, object]:
    """Build join counts keyed by canonical ``binding_id``.

    Returns:
        dict[str, object]: Binding-join payload keyed by binding identity.
    """
    by_binding: dict[str, dict[str, object]] = {}
    for row in ts_occurrences:
        binding_id = row.get("binding_id")
        if not isinstance(binding_id, str) or not binding_id:
            continue
        bucket = by_binding.setdefault(
            binding_id,
            {"binding_id": binding_id, "ts": 0, "dis_defs": 0, "dis_uses": 0},
        )
        bucket["ts"] = coerce_count(bucket.get("ts")) + 1
    for row in dis_events:
        binding_id = row.get("binding_id")
        if not isinstance(binding_id, str) or not binding_id:
            continue
        bucket = by_binding.setdefault(
            binding_id,
            {"binding_id": binding_id, "ts": 0, "dis_defs": 0, "dis_uses": 0},
        )
        event = row.get("event")
        if event == "def":
            bucket["dis_defs"] = coerce_count(bucket.get("dis_defs")) + 1
        elif event == "use":
            bucket["dis_uses"] = coerce_count(bucket.get("dis_uses")) + 1
    return {"binding_join": by_binding}


def _decorator_callsite_metrics(ts_occurrences: list[dict[str, object]]) -> dict[str, object]:
    decorator_occurrences = 0
    callable_occurrences = 0
    for row in ts_occurrences:
        context = row.get("context")
        node_kind = row.get("node_kind")
        if context == "decorator":
            decorator_occurrences += 1
        if isinstance(node_kind, str) and "call" in node_kind:
            callable_occurrences += 1
    total = max(1, decorator_occurrences + callable_occurrences)
    return {
        "decorator_occurrences": decorator_occurrences,
        "callsite_occurrences": callable_occurrences,
        "decorator_ratio": round(decorator_occurrences / total, 4),
    }


def build_compound_bundle(
    *,
    ts_occurrences: list[dict[str, object]],
    dis_events: list[dict[str, object]],
) -> dict[str, object]:
    """Build compound joins and correctness backstops.

    Returns:
        dict[str, object]: Compound plane payload with join and coverage metrics.
    """
    join_payload = build_binding_join(ts_occurrences, dis_events)
    binding_join: dict[str, dict[str, object]] = {}
    raw_binding_join = join_payload.get("binding_join")
    if isinstance(raw_binding_join, dict):
        binding_join.update(
            {
                binding_id: row
                for binding_id, row in raw_binding_join.items()
                if isinstance(binding_id, str) and isinstance(row, dict)
            }
        )

    coverage = defaultdict(int)
    missing_in_dis: list[str] = []
    missing_in_ts: list[str] = []
    for binding_id, row in binding_join.items():
        if not isinstance(binding_id, str) or not isinstance(row, dict):
            continue
        ts_count = coerce_count(row.get("ts"))
        dis_defs = coerce_count(row.get("dis_defs"))
        dis_uses = coerce_count(row.get("dis_uses"))
        dis_total = dis_defs + dis_uses
        if ts_count > 0 and dis_total > 0:
            coverage["joined"] += 1
        elif ts_count > 0:
            coverage["ts_only"] += 1
            missing_in_dis.append(binding_id)
        elif dis_total > 0:
            coverage["dis_only"] += 1
            missing_in_ts.append(binding_id)

    return {
        "binding_join": binding_join,
        "join_metrics": {
            "bindings_total": len(binding_join),
            "joined": coverage["joined"],
            "ts_only": coverage["ts_only"],
            "dis_only": coverage["dis_only"],
        },
        "decorator_callsite_check": _decorator_callsite_metrics(ts_occurrences),
        "coverage_backstops": {
            "missing_in_dis": missing_in_dis[:128],
            "missing_in_ts": missing_in_ts[:128],
        },
    }


__all__ = ["build_binding_join", "build_compound_bundle"]
