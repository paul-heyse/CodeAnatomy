"""Thin Python facade for the Rust codeanatomy_engine."""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from engine.spec_builder import SemanticExecutionSpec


def execute_cpg_build(
    extraction_inputs: dict[str, str],
    semantic_spec: SemanticExecutionSpec,
    environment_class: str = "medium",
) -> dict[str, object]:
    """Submit spec + inputs to the Rust engine and return the result envelope.

    Args:
        extraction_inputs: Map of logical_name -> Delta location for extraction outputs.
        semantic_spec: The compiled semantic execution spec.
        environment_class: One of "small", "medium", "large".

    Returns:
        RunResult dict with outputs, metrics, and fingerprints.

    Raises:
        ImportError: If the codeanatomy_engine Rust extension is not built.
    """
    try:
        from codeanatomy_engine import (
            CpgMaterializer,
            SemanticPlanCompiler,
            SessionFactory,
        )
    except ImportError:
        msg = (
            "codeanatomy_engine Rust extension not built. "
            "Run: bash scripts/rebuild_rust_artifacts.sh"
        )
        raise ImportError(msg) from None

    spec_json = msgspec.json.encode(semantic_spec).decode()

    factory = SessionFactory.from_class(environment_class)
    session = factory.build_session(spec_json)
    session.register_inputs(extraction_inputs)

    compiler = SemanticPlanCompiler(session)
    compiled = compiler.compile(spec_json)

    materializer = CpgMaterializer(session)
    result = materializer.execute(compiled)

    return msgspec.json.decode(result.to_json())
