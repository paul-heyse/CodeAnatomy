"""Metadata-driven extractor adapter registry."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractTemplateAdapter:
    """Declarative adapter contract for an extractor template."""

    name: str
    required_inputs: tuple[str, ...] = ()
    supports_plan: bool = False
    extra_required_inputs: tuple[str, ...] = ()

    def merged_required_inputs(self) -> tuple[str, ...]:
        """Return required inputs with template-specific extras de-duplicated."""
        return tuple(dict.fromkeys((*self.required_inputs, *self.extra_required_inputs)))


_ADAPTERS: dict[str, ExtractTemplateAdapter] = {
    "ast": ExtractTemplateAdapter(
        name="ast",
        required_inputs=("repo_files",),
        supports_plan=True,
    ),
    "cst": ExtractTemplateAdapter(
        name="cst",
        required_inputs=("repo_files",),
        supports_plan=True,
    ),
    "tree_sitter": ExtractTemplateAdapter(
        name="tree_sitter",
        required_inputs=("repo_files",),
        supports_plan=True,
    ),
    "bytecode": ExtractTemplateAdapter(
        name="bytecode",
        required_inputs=("repo_files",),
        supports_plan=True,
    ),
    "symtable": ExtractTemplateAdapter(
        name="symtable",
        required_inputs=("repo_files",),
        supports_plan=True,
    ),
    "scip": ExtractTemplateAdapter(
        name="scip",
        required_inputs=("scip_index_path", "repo_root"),
        supports_plan=True,
    ),
    "repo_scan": ExtractTemplateAdapter(
        name="repo_scan",
        required_inputs=("repo_root",),
        supports_plan=False,
    ),
    "python_imports": ExtractTemplateAdapter(
        name="python_imports",
        required_inputs=(),
        supports_plan=True,
        extra_required_inputs=("ast_imports", "cst_imports", "ts_imports"),
    ),
    "python_external": ExtractTemplateAdapter(
        name="python_external",
        required_inputs=("python_imports", "repo_root"),
        supports_plan=True,
    ),
}


def extract_template_adapter(name: str) -> ExtractTemplateAdapter:
    """Return the adapter contract for a template."""
    return _ADAPTERS[name]


def maybe_extract_template_adapter(name: str) -> ExtractTemplateAdapter | None:
    """Return the adapter contract for a template when available."""
    return _ADAPTERS.get(name)


def extract_template_adapters() -> tuple[ExtractTemplateAdapter, ...]:
    """Return all known adapters in deterministic order."""
    return tuple(_ADAPTERS[name] for name in sorted(_ADAPTERS))


def required_inputs_for_template(name: str, *, include_extra: bool = True) -> tuple[str, ...]:
    """Return required inputs for an extractor template."""
    adapter = extract_template_adapter(name)
    if include_extra:
        return adapter.merged_required_inputs()
    return adapter.required_inputs


def additional_required_inputs_for_template(name: str) -> tuple[str, ...]:
    """Return template-specific extra dependency inputs."""
    adapter = maybe_extract_template_adapter(name)
    if adapter is None:
        return ()
    return adapter.extra_required_inputs


def supports_plan_for_template(name: str) -> bool:
    """Return whether a template supports compile-time plan generation."""
    adapter = maybe_extract_template_adapter(name)
    return False if adapter is None else adapter.supports_plan


__all__ = [
    "ExtractTemplateAdapter",
    "additional_required_inputs_for_template",
    "extract_template_adapter",
    "extract_template_adapters",
    "maybe_extract_template_adapter",
    "required_inputs_for_template",
    "supports_plan_for_template",
]
