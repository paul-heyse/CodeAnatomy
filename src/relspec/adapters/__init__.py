"""Domain adapters for centralized rule definitions."""

from relspec.adapters.cpg import CpgRuleAdapter
from relspec.adapters.extract import ExtractRuleAdapter
from relspec.adapters.factory import RuleFactoryRegistry, default_rule_factory_registry
from relspec.adapters.normalize import NormalizeRuleAdapter
from relspec.adapters.relationship_rules import RelspecRuleAdapter

__all__ = [
    "CpgRuleAdapter",
    "ExtractRuleAdapter",
    "NormalizeRuleAdapter",
    "RelspecRuleAdapter",
    "RuleFactoryRegistry",
    "default_rule_factory_registry",
]
