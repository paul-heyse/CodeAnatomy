"""Relationship spec models, registry, and compiler."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from arrowdsl.spec.infra import DATASET_REF_STRUCT, SORT_KEY_STRUCT
    from relspec.compiler import (
        CompiledOutput,
        CompiledRule,
        FilesystemPlanResolver,
        InMemoryPlanResolver,
        PlanResolver,
        RelationshipRuleCompiler,
    )
    from relspec.contracts import (
        RELATION_OUTPUT_NAME,
        RELATION_OUTPUT_SCHEMA,
        relation_output_contract,
        relation_output_schema,
        relation_output_spec,
    )
    from relspec.model import (
        AddLiteralSpec,
        AmbiguityPolicy,
        CanonicalSortKernelSpec,
        ConfidencePolicy,
        DatasetRef,
        DedupeKernelSpec,
        DropColumnsSpec,
        EvidenceSpec,
        ExplodeListSpec,
        HashJoinConfig,
        IntervalAlignConfig,
        KernelSpec,
        ProjectConfig,
        RelationshipRule,
        RenameColumnsSpec,
        RuleKind,
        WinnerSelectConfig,
    )
    from relspec.registry import ContractCatalog, DatasetCatalog, DatasetLocation
    from relspec.rules.spec_tables import (
        AMBIGUITY_STRUCT,
        CONFIDENCE_STRUCT,
        EVIDENCE_STRUCT,
        HASH_JOIN_STRUCT,
        INTERVAL_ALIGN_STRUCT,
        PROJECT_EXPR_STRUCT,
        PROJECT_STRUCT,
        RULES_SCHEMA,
        WINNER_SELECT_STRUCT,
    )

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AMBIGUITY_STRUCT": ("relspec.rules.spec_tables", "AMBIGUITY_STRUCT"),
    "CONFIDENCE_STRUCT": ("relspec.rules.spec_tables", "CONFIDENCE_STRUCT"),
    "DATASET_REF_STRUCT": ("arrowdsl.spec.infra", "DATASET_REF_STRUCT"),
    "EVIDENCE_STRUCT": ("relspec.rules.spec_tables", "EVIDENCE_STRUCT"),
    "HASH_JOIN_STRUCT": ("relspec.rules.spec_tables", "HASH_JOIN_STRUCT"),
    "INTERVAL_ALIGN_STRUCT": ("relspec.rules.spec_tables", "INTERVAL_ALIGN_STRUCT"),
    "PROJECT_EXPR_STRUCT": ("relspec.rules.spec_tables", "PROJECT_EXPR_STRUCT"),
    "PROJECT_STRUCT": ("relspec.rules.spec_tables", "PROJECT_STRUCT"),
    "RELATION_OUTPUT_NAME": ("relspec.contracts", "RELATION_OUTPUT_NAME"),
    "RELATION_OUTPUT_SCHEMA": ("relspec.contracts", "RELATION_OUTPUT_SCHEMA"),
    "RULES_SCHEMA": ("relspec.rules.spec_tables", "RULES_SCHEMA"),
    "SORT_KEY_STRUCT": ("arrowdsl.spec.infra", "SORT_KEY_STRUCT"),
    "WINNER_SELECT_STRUCT": ("relspec.rules.spec_tables", "WINNER_SELECT_STRUCT"),
    "AddLiteralSpec": ("relspec.model", "AddLiteralSpec"),
    "AmbiguityPolicy": ("relspec.model", "AmbiguityPolicy"),
    "CanonicalSortKernelSpec": ("relspec.model", "CanonicalSortKernelSpec"),
    "CompiledOutput": ("relspec.compiler", "CompiledOutput"),
    "CompiledRule": ("relspec.compiler", "CompiledRule"),
    "ConfidencePolicy": ("relspec.model", "ConfidencePolicy"),
    "ContractCatalog": ("relspec.registry", "ContractCatalog"),
    "DatasetCatalog": ("relspec.registry", "DatasetCatalog"),
    "DatasetLocation": ("relspec.registry", "DatasetLocation"),
    "DatasetRef": ("relspec.model", "DatasetRef"),
    "DedupeKernelSpec": ("relspec.model", "DedupeKernelSpec"),
    "DropColumnsSpec": ("relspec.model", "DropColumnsSpec"),
    "EvidenceSpec": ("relspec.model", "EvidenceSpec"),
    "ExplodeListSpec": ("relspec.model", "ExplodeListSpec"),
    "FilesystemPlanResolver": ("relspec.compiler", "FilesystemPlanResolver"),
    "HashJoinConfig": ("relspec.model", "HashJoinConfig"),
    "InMemoryPlanResolver": ("relspec.compiler", "InMemoryPlanResolver"),
    "IntervalAlignConfig": ("relspec.model", "IntervalAlignConfig"),
    "KernelSpec": ("relspec.model", "KernelSpec"),
    "PlanResolver": ("relspec.compiler", "PlanResolver"),
    "ProjectConfig": ("relspec.model", "ProjectConfig"),
    "RelationshipRule": ("relspec.model", "RelationshipRule"),
    "RelationshipRuleCompiler": ("relspec.compiler", "RelationshipRuleCompiler"),
    "RenameColumnsSpec": ("relspec.model", "RenameColumnsSpec"),
    "RuleKind": ("relspec.model", "RuleKind"),
    "WinnerSelectConfig": ("relspec.model", "WinnerSelectConfig"),
    "relation_output_contract": ("relspec.contracts", "relation_output_contract"),
    "relation_output_schema": ("relspec.contracts", "relation_output_schema"),
    "relation_output_spec": ("relspec.contracts", "relation_output_spec"),
}


def __getattr__(name: str) -> object:
    target = _EXPORT_MAP.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_EXPORT_MAP))


__all__ = [
    "AMBIGUITY_STRUCT",
    "CONFIDENCE_STRUCT",
    "DATASET_REF_STRUCT",
    "EVIDENCE_STRUCT",
    "HASH_JOIN_STRUCT",
    "INTERVAL_ALIGN_STRUCT",
    "PROJECT_EXPR_STRUCT",
    "PROJECT_STRUCT",
    "RELATION_OUTPUT_NAME",
    "RELATION_OUTPUT_SCHEMA",
    "RULES_SCHEMA",
    "SORT_KEY_STRUCT",
    "WINNER_SELECT_STRUCT",
    "AddLiteralSpec",
    "AmbiguityPolicy",
    "CanonicalSortKernelSpec",
    "CompiledOutput",
    "CompiledRule",
    "ConfidencePolicy",
    "ContractCatalog",
    "DatasetCatalog",
    "DatasetLocation",
    "DatasetRef",
    "DedupeKernelSpec",
    "DropColumnsSpec",
    "EvidenceSpec",
    "ExplodeListSpec",
    "FilesystemPlanResolver",
    "HashJoinConfig",
    "InMemoryPlanResolver",
    "IntervalAlignConfig",
    "KernelSpec",
    "PlanResolver",
    "ProjectConfig",
    "RelationshipRule",
    "RelationshipRuleCompiler",
    "RenameColumnsSpec",
    "RuleKind",
    "WinnerSelectConfig",
    "relation_output_contract",
    "relation_output_schema",
    "relation_output_spec",
]
