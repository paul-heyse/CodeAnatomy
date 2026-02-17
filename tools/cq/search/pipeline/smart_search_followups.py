"""Follow-up suggestion generation for Smart Search."""

from __future__ import annotations

from tools.cq.core.schema import Finding
from tools.cq.core.scoring import build_detail_payload
from tools.cq.search._shared.types import QueryMode
from tools.cq.search.pipeline.smart_search_types import EnrichedMatch


def build_followups(
    matches: list[EnrichedMatch],
    query: str,
    mode: QueryMode,
) -> list[Finding]:
    """Generate actionable next commands from match context.

    Returns:
    -------
    list[Finding]
        Follow-up suggestions.
    """
    findings: list[Finding] = []

    if mode == QueryMode.IDENTIFIER:
        defs = [m for m in matches if m.category == "definition"]
        if defs:
            findings.append(
                Finding(
                    category="next_step",
                    message=f"Find callers: /cq calls {query}",
                    severity="info",
                    details=build_detail_payload(
                        kind="next_step",
                        data={"cmd": f"/cq calls {query}"},
                    ),
                )
            )
            findings.append(
                Finding(
                    category="next_step",
                    message=f'Find definitions: /cq q "entity=function name={query}"',
                    severity="info",
                    details=build_detail_payload(
                        kind="next_step",
                        data={"cmd": f'/cq q "entity=function name={query}"'},
                    ),
                )
            )
            findings.append(
                Finding(
                    category="next_step",
                    message=f'Find callers (transitive): /cq q "entity=function name={query} expand=callers(depth=2)"',
                    severity="info",
                    details=build_detail_payload(
                        kind="next_step",
                        data={
                            "cmd": f'/cq q "entity=function name={query} expand=callers(depth=2)"'
                        },
                    ),
                )
            )

        calls = [m for m in matches if m.category == "callsite"]
        if calls:
            findings.append(
                Finding(
                    category="next_step",
                    message=f"Analyze impact: /cq impact {query}",
                    severity="info",
                    details=build_detail_payload(
                        kind="next_step",
                        data={"cmd": f"/cq impact {query}"},
                    ),
                )
            )

    return findings


def generate_followup_suggestions(
    matches: list[EnrichedMatch],
    query: str,
    mode: QueryMode,
) -> list[Finding]:
    """Compatibility alias for follow-up generation.

    Returns:
    -------
    list[Finding]
        Follow-up suggestions derived from the match set.
    """
    return build_followups(matches, query, mode)


__all__ = ["build_followups", "generate_followup_suggestions"]
