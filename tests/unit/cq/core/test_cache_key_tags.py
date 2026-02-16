"""Tests for test_cache_key_tags."""

from __future__ import annotations

from tools.cq.core.cache import build_namespace_cache_tag, build_scope_hash


def test_build_namespace_cache_tag_uses_canonical_atom_order() -> None:
    """Preserve canonical namespace-tag atom order for cache key introspection."""
    scope_hash = build_scope_hash({"paths": ("src",), "globs": ("*.py",)})
    assert scope_hash is not None

    tag = build_namespace_cache_tag(
        workspace="/repo",
        language="PYTHON",
        namespace="search_enrichment",
        scope_hash=scope_hash,
        snapshot="ABCDEF1234567890ABCDEF1234567890",
        run_id="run-123",
    )

    atoms = tag.split("|")
    assert [atom.split(":", maxsplit=1)[0] for atom in atoms] == [
        "ws",
        "lang",
        "ns",
        "scope",
        "snap",
        "run",
    ]
    assert atoms[1] == "lang:python"
    assert atoms[2] == "ns:search_enrichment"
    assert atoms[4].startswith("snap:abcdef1234567890abcdef12")
    assert atoms[5].startswith("run:")
    assert "run-123" not in atoms[5]



def test_build_namespace_cache_tag_is_stable_for_equivalent_scope_payloads() -> None:
    """Normalize scope inputs before hashing so equivalent payloads map to equal tags."""
    scope_hash_a = build_scope_hash(
        {
            "paths": {"src", "tests"},
            "globs": ("*.py", "*.pyi"),
        }
    )
    scope_hash_b = build_scope_hash(
        {
            "globs": ("*.py", "*.pyi"),
            "paths": {"tests", "src"},
        }
    )

    assert scope_hash_a == scope_hash_b
    assert scope_hash_a is not None

    tag_a = build_namespace_cache_tag(
        workspace="/repo",
        language="python",
        namespace="query_entity_fragment",
        scope_hash=scope_hash_a,
        snapshot="fedcba9876543210fedcba9876543210",
    )
    tag_b = build_namespace_cache_tag(
        workspace="/repo",
        language="python",
        namespace="query_entity_fragment",
        scope_hash=scope_hash_b,
        snapshot="fedcba9876543210fedcba9876543210",
    )

    assert tag_a == tag_b
