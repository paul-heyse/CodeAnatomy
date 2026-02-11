"""Tests for Rust LSP contract types and coercion functions."""

from __future__ import annotations

import msgspec
from tools.cq.search.rust_lsp_contracts import (
    LspCapabilitySnapshotV1,
    LspClientCapabilitySnapshotV1,
    LspClientPublishDiagnosticsCapsV1,
    LspExperimentalCapabilitySnapshotV1,
    LspServerCapabilitySnapshotV1,
    LspSessionEnvV1,
    RustCallGraph,
    RustCallLink,
    RustDiagnosticV1,
    RustDocSymbol,
    RustLspEnrichmentPayload,
    RustLspTarget,
    RustSymbolGrounding,
    RustTypeHierarchy,
    RustTypeLink,
    coerce_rust_lsp_payload,
    rust_lsp_payload_to_dict,
)


def test_lsp_server_capability_snapshot_roundtrip() -> None:
    """Test msgspec roundtrip for LspServerCapabilitySnapshotV1."""
    caps = LspServerCapabilitySnapshotV1(
        definition_provider=True,
        hover_provider=True,
        semantic_tokens_provider_raw={"legend": {"tokenTypes": ["keyword"]}},
    )
    data = msgspec.to_builtins(caps)
    assert isinstance(data, dict)
    assert data["definition_provider"] is True
    assert data["hover_provider"] is True
    decoded = msgspec.convert(data, LspServerCapabilitySnapshotV1)
    assert decoded.definition_provider is True
    assert decoded.hover_provider is True


def test_lsp_capability_snapshot_roundtrip() -> None:
    """Test msgspec roundtrip for LspCapabilitySnapshotV1."""
    caps = LspCapabilitySnapshotV1(
        server_caps=LspServerCapabilitySnapshotV1(definition_provider=True),
        client_caps=LspClientCapabilitySnapshotV1(
            publish_diagnostics=LspClientPublishDiagnosticsCapsV1(enabled=True)
        ),
        experimental_caps=LspExperimentalCapabilitySnapshotV1(server_status_notification=True),
    )
    data = msgspec.to_builtins(caps)
    assert isinstance(data, dict)
    decoded = msgspec.convert(data, LspCapabilitySnapshotV1)
    assert decoded.server_caps.definition_provider is True
    assert decoded.client_caps.publish_diagnostics.enabled is True
    assert decoded.experimental_caps.server_status_notification is True


def test_lsp_session_env_roundtrip() -> None:
    """Test msgspec roundtrip for LspSessionEnvV1."""
    env = LspSessionEnvV1(
        server_name="rust-analyzer",
        server_version="0.3.1",
        position_encoding="utf-16",
        workspace_health="ok",
        quiescent=True,
        config_fingerprint="abc123",
        refresh_events=("workspace/didChangeConfiguration",),
    )
    data = msgspec.to_builtins(env)
    assert isinstance(data, dict)
    assert data["server_name"] == "rust-analyzer"
    assert data["workspace_health"] == "ok"
    decoded = msgspec.convert(data, LspSessionEnvV1)
    assert decoded.server_name == "rust-analyzer"
    assert decoded.workspace_health == "ok"
    assert decoded.quiescent is True


def test_rust_diagnostic_roundtrip() -> None:
    """Test msgspec roundtrip for RustDiagnosticV1."""
    diag = RustDiagnosticV1(
        uri="file:///path/to/file.rs",
        range_start_line=10,
        range_start_col=5,
        range_end_line=10,
        range_end_col=15,
        severity=1,
        code="E0308",
        source="rust-analyzer",
        message="mismatched types",
        related_info=({"location": {"uri": "file:///other.rs"}},),
        data={"fix": "add type annotation"},
    )
    data = msgspec.to_builtins(diag)
    assert isinstance(data, dict)
    assert data["uri"] == "file:///path/to/file.rs"
    assert data["severity"] == 1
    decoded = msgspec.convert(data, RustDiagnosticV1)
    assert decoded.uri == "file:///path/to/file.rs"
    assert decoded.message == "mismatched types"


def test_rust_lsp_target_roundtrip() -> None:
    """Test msgspec roundtrip for RustLspTarget."""
    target = RustLspTarget(
        uri="file:///path/to/file.rs",
        range_start_line=5,
        range_start_col=10,
        range_end_line=5,
        range_end_col=20,
        container_name="MyStruct",
    )
    data = msgspec.to_builtins(target)
    assert isinstance(data, dict)
    decoded = msgspec.convert(data, RustLspTarget)
    assert decoded.uri == "file:///path/to/file.rs"
    assert decoded.container_name == "MyStruct"


def test_rust_call_link_roundtrip() -> None:
    """Test msgspec roundtrip for RustCallLink."""
    link = RustCallLink(
        name="my_function",
        kind=12,
        uri="file:///path/to/file.rs",
        range_start_line=10,
        range_start_col=5,
        from_ranges=((1, 2, 3, 4), (5, 6, 7, 8)),
    )
    data = msgspec.to_builtins(link)
    assert isinstance(data, dict)
    decoded = msgspec.convert(data, RustCallLink)
    assert decoded.name == "my_function"
    assert decoded.from_ranges == ((1, 2, 3, 4), (5, 6, 7, 8))


def test_rust_type_link_roundtrip() -> None:
    """Test msgspec roundtrip for RustTypeLink."""
    link = RustTypeLink(
        name="MyTrait",
        kind=11,
        uri="file:///path/to/file.rs",
        range_start_line=20,
        range_start_col=0,
    )
    data = msgspec.to_builtins(link)
    assert isinstance(data, dict)
    decoded = msgspec.convert(data, RustTypeLink)
    assert decoded.name == "MyTrait"
    assert decoded.kind == 11


def test_rust_doc_symbol_roundtrip() -> None:
    """Test msgspec roundtrip for RustDocSymbol with children."""
    symbol = RustDocSymbol(
        name="MyStruct",
        kind=5,
        range_start_line=10,
        range_start_col=0,
        range_end_line=20,
        range_end_col=1,
        children=(
            RustDocSymbol(
                name="field1",
                kind=8,
                range_start_line=11,
                range_start_col=4,
                range_end_line=11,
                range_end_col=10,
            ),
        ),
    )
    data = msgspec.to_builtins(symbol)
    assert isinstance(data, dict)
    decoded = msgspec.convert(data, RustDocSymbol)
    assert decoded.name == "MyStruct"
    assert len(decoded.children) == 1
    assert decoded.children[0].name == "field1"


def test_rust_symbol_grounding_roundtrip() -> None:
    """Test msgspec roundtrip for RustSymbolGrounding."""
    grounding = RustSymbolGrounding(
        definitions=(
            RustLspTarget(
                uri="file:///path/to/file.rs",
                range_start_line=5,
                range_start_col=0,
                range_end_line=5,
                range_end_col=10,
            ),
        ),
        references=(
            RustLspTarget(
                uri="file:///path/to/other.rs",
                range_start_line=15,
                range_start_col=5,
                range_end_line=15,
                range_end_col=15,
            ),
        ),
    )
    data = msgspec.to_builtins(grounding)
    assert isinstance(data, dict)
    decoded = msgspec.convert(data, RustSymbolGrounding)
    assert len(decoded.definitions) == 1
    assert len(decoded.references) == 1


def test_rust_call_graph_roundtrip() -> None:
    """Test msgspec roundtrip for RustCallGraph."""
    graph = RustCallGraph(
        incoming_callers=(RustCallLink(name="caller1", kind=12, uri="file:///a.rs"),),
        outgoing_callees=(RustCallLink(name="callee1", kind=12, uri="file:///b.rs"),),
    )
    data = msgspec.to_builtins(graph)
    assert isinstance(data, dict)
    decoded = msgspec.convert(data, RustCallGraph)
    assert len(decoded.incoming_callers) == 1
    assert len(decoded.outgoing_callees) == 1


def test_rust_type_hierarchy_roundtrip() -> None:
    """Test msgspec roundtrip for RustTypeHierarchy."""
    hierarchy = RustTypeHierarchy(
        supertypes=(RustTypeLink(name="Trait1", kind=11, uri="file:///a.rs"),),
        subtypes=(RustTypeLink(name="Impl1", kind=5, uri="file:///b.rs"),),
    )
    data = msgspec.to_builtins(hierarchy)
    assert isinstance(data, dict)
    decoded = msgspec.convert(data, RustTypeHierarchy)
    assert len(decoded.supertypes) == 1
    assert len(decoded.subtypes) == 1


def test_rust_lsp_enrichment_payload_roundtrip() -> None:
    """Test msgspec roundtrip for RustLspEnrichmentPayload."""
    payload = RustLspEnrichmentPayload(
        session_env=LspSessionEnvV1(server_name="rust-analyzer", workspace_health="ok"),
        symbol_grounding=RustSymbolGrounding(
            definitions=(RustLspTarget(uri="file:///a.rs", range_start_line=10),)
        ),
        hover_text="Function signature",
    )
    data = msgspec.to_builtins(payload)
    assert isinstance(data, dict)
    decoded = msgspec.convert(data, RustLspEnrichmentPayload)
    assert decoded.session_env.server_name == "rust-analyzer"
    assert decoded.hover_text == "Function signature"
    assert len(decoded.symbol_grounding.definitions) == 1


def test_coerce_rust_lsp_payload_none() -> None:
    """Test coerce_rust_lsp_payload with None input returns valid empty payload."""
    payload = coerce_rust_lsp_payload(None)
    assert isinstance(payload, RustLspEnrichmentPayload)
    assert payload.session_env.workspace_health == "unknown"
    assert len(payload.diagnostics) == 0
    assert len(payload.symbol_grounding.definitions) == 0


def test_coerce_rust_lsp_payload_with_session_env() -> None:
    """Test coerce_rust_lsp_payload normalizes session_env correctly."""
    raw = {
        "session_env": {
            "server_name": "rust-analyzer",
            "server_version": "0.3.1",
            "workspace_health": "ok",
            "quiescent": True,
        }
    }
    payload = coerce_rust_lsp_payload(raw)
    assert payload.session_env.server_name == "rust-analyzer"
    assert payload.session_env.workspace_health == "ok"
    assert payload.session_env.quiescent is True


def test_coerce_rust_lsp_payload_with_diagnostics() -> None:
    """Test coerce_rust_lsp_payload normalizes diagnostics correctly."""
    raw = {
        "diagnostics": [
            {
                "uri": "file:///path/to/file.rs",
                "range_start_line": 10,
                "range_start_col": 5,
                "range_end_line": 10,
                "range_end_col": 15,
                "severity": 1,
                "message": "error message",
            }
        ]
    }
    payload = coerce_rust_lsp_payload(raw)
    assert len(payload.diagnostics) == 1
    assert payload.diagnostics[0].uri == "file:///path/to/file.rs"
    assert payload.diagnostics[0].severity == 1


def test_coerce_rust_lsp_payload_with_symbol_grounding() -> None:
    """Test coerce_rust_lsp_payload normalizes symbol_grounding correctly."""
    raw = {
        "symbol_grounding": {
            "definitions": [
                {
                    "uri": "file:///path/to/file.rs",
                    "range_start_line": 5,
                    "range_start_col": 0,
                    "range_end_line": 5,
                    "range_end_col": 10,
                }
            ]
        }
    }
    payload = coerce_rust_lsp_payload(raw)
    assert len(payload.symbol_grounding.definitions) == 1
    assert payload.symbol_grounding.definitions[0].uri == "file:///path/to/file.rs"


def test_rust_lsp_payload_to_dict_roundtrip() -> None:
    """Test rust_lsp_payload_to_dict roundtrips with coerce_rust_lsp_payload."""
    original = RustLspEnrichmentPayload(
        session_env=LspSessionEnvV1(
            server_name="rust-analyzer",
            workspace_health="ok",
            quiescent=True,
        ),
        diagnostics=(
            RustDiagnosticV1(
                uri="file:///a.rs",
                severity=1,
                message="error",
            ),
        ),
        hover_text="hover content",
    )

    # Convert to dict
    as_dict = rust_lsp_payload_to_dict(original)
    assert isinstance(as_dict, dict)

    # Coerce back to typed payload
    roundtripped = coerce_rust_lsp_payload(as_dict)
    assert roundtripped.session_env.server_name == "rust-analyzer"
    assert roundtripped.session_env.workspace_health == "ok"
    assert len(roundtripped.diagnostics) == 1
    assert roundtripped.hover_text == "hover content"


def test_coerce_rust_lsp_payload_with_malformed_data() -> None:
    """Test coerce_rust_lsp_payload handles malformed data gracefully."""
    raw = {
        "session_env": "not a dict",  # Invalid type
        "diagnostics": [
            {"uri": None},  # Missing required uri string
            {"uri": "file:///valid.rs", "severity": 2, "message": "warning"},  # Valid
        ],
        "symbol_grounding": {
            "definitions": [
                {"uri": None},  # Invalid - no uri
                {"uri": "file:///valid.rs"},  # Valid
            ]
        },
    }
    payload = coerce_rust_lsp_payload(raw)

    # Should use defaults for invalid session_env
    assert payload.session_env.workspace_health == "unknown"

    # Should skip invalid diagnostics, keep valid one
    assert len(payload.diagnostics) == 1
    assert payload.diagnostics[0].uri == "file:///valid.rs"

    # Should skip invalid targets, keep valid one
    assert len(payload.symbol_grounding.definitions) == 1
    assert payload.symbol_grounding.definitions[0].uri == "file:///valid.rs"
