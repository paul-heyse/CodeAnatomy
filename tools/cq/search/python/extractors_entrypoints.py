"""Python enrichment entrypoint boundary.

This module is the stable import surface for Python enrichment entrypoints and
stage-runtime helpers. Implementation ownership lives in
`extractors_runtime_core.py`.
"""

from __future__ import annotations

import tools.cq.search.python.extractors_runtime_core as core
from tools.cq.search.python.extractors_analysis import (
    extract_behavior_summary,
    extract_generator_flag,
    extract_import_detail,
    find_ast_function,
)
from tools.cq.search.python.extractors_classification import (
    _unwrap_decorated,
    classify_item_role,
)
from tools.cq.search.python.extractors_runtime_astgrep import (
    extract_call_target,
    extract_class_context,
    extract_decorators,
    extract_scope_chain,
    extract_signature,
    extract_structural_context,
    truncate,
)
from tools.cq.search.python.extractors_runtime_core import (
    _build_agreement_section,
    _get_ast,
    _PythonEnrichmentState,
)
from tools.cq.search.python.extractors_runtime_state import build_stage_facts
from tools.cq.search.python.extractors_structure import extract_class_shape

_build_stage_facts = build_stage_facts
_classify_item_role = classify_item_role
_extract_behavior_summary = extract_behavior_summary
_extract_call_target = extract_call_target
_extract_class_context = extract_class_context
_extract_class_shape = extract_class_shape
_extract_decorators = extract_decorators
_extract_generator_flag = extract_generator_flag
_extract_import_detail = extract_import_detail
_extract_scope_chain = extract_scope_chain
_extract_signature = extract_signature
_extract_structural_context = extract_structural_context
_find_ast_function = find_ast_function
_truncate = truncate

# Public entrypoints
clear_python_enrichment_cache = core.clear_python_enrichment_cache
enrich_python_context_contract = core.enrich_python_context_contract
enrich_python_context_contract_by_byte_range = core.enrich_python_context_contract_by_byte_range
enrich_python_context = core.enrich_python_context
enrich_python_context_by_byte_range = core.enrich_python_context_by_byte_range
ensure_python_clear_callback_registered = core.ensure_python_clear_callback_registered
extract_python_byte_range = core.extract_python_byte_range
extract_python_node = core.extract_python_node

# Stage-runtime ownership surface
build_agreement_section = core.build_agreement_section
build_stage_fact_patch = core.build_stage_fact_patch
build_stage_facts_from_enrichment = core.build_stage_facts_from_enrichment
enrich_ast_grep_tier = core.enrich_ast_grep_tier
enrich_import_tier = core.enrich_import_tier
enrich_python_ast_tier = core.enrich_python_ast_tier
flatten_python_enrichment_facts = core.flatten_python_enrichment_facts
ingest_stage_fact_patch = core.ingest_stage_fact_patch
is_function_node = core.is_function_node
max_python_payload_bytes = core.max_python_payload_bytes
new_python_agreement_stage = core.new_python_agreement_stage
python_enrichment_crosscheck_env = core.python_enrichment_crosscheck_env

logger = core.logger

__all__ = [
    "_PythonEnrichmentState",
    "_build_agreement_section",
    "_build_stage_facts",
    "_classify_item_role",
    "_extract_behavior_summary",
    "_extract_call_target",
    "_extract_class_context",
    "_extract_class_shape",
    "_extract_decorators",
    "_extract_generator_flag",
    "_extract_import_detail",
    "_extract_scope_chain",
    "_extract_signature",
    "_extract_structural_context",
    "_find_ast_function",
    "_get_ast",
    "_truncate",
    "_unwrap_decorated",
    "clear_python_enrichment_cache",
    "enrich_python_context",
    "enrich_python_context_by_byte_range",
    "enrich_python_context_contract",
    "enrich_python_context_contract_by_byte_range",
    "ensure_python_clear_callback_registered",
    "extract_python_byte_range",
    "extract_python_node",
]
