"""Pure analysis helpers shared across CQ macros."""

from .calls import classify_call_against_signature, classify_calls_against_signature
from .signature import SigParam, parse_signature
from .taint import TaintedSite, analyze_function_node, find_function_node

__all__ = [
    "SigParam",
    "TaintedSite",
    "analyze_function_node",
    "classify_call_against_signature",
    "classify_calls_against_signature",
    "find_function_node",
    "parse_signature",
]
