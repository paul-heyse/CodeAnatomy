"""Domain-specific rule handlers."""

from relspec.rules.handlers.cpg import RelationshipRuleHandler
from relspec.rules.handlers.cpg_emit import (
    EdgeEmitRuleHandler,
    NodeEmitRuleHandler,
    PropEmitRuleHandler,
)
from relspec.rules.handlers.extract import ExtractRuleCompilation, ExtractRuleHandler
from relspec.rules.handlers.normalize import NormalizeRuleHandler

__all__ = [
    "EdgeEmitRuleHandler",
    "ExtractRuleCompilation",
    "ExtractRuleHandler",
    "NodeEmitRuleHandler",
    "NormalizeRuleHandler",
    "PropEmitRuleHandler",
    "RelationshipRuleHandler",
]
