"""Tests for Rust fallback search helper."""

from __future__ import annotations

from pathlib import Path

from tools.cq.macros._rust_fallback import rust_fallback_search

MIN_RUST_MATCHES = 2


class TestRustFallbackSearch:
    """Tests for the shared Rust fallback helper."""

    def test_returns_empty_for_no_rust_files(self, tmp_path: Path) -> None:
        """Test that search returns empty when no Rust files exist."""
        findings, _diags, stats = rust_fallback_search(
            root=tmp_path,
            pattern="nonexistent",
            macro_name="calls",
        )
        assert findings == []
        assert stats["matches"] == 0

    def test_returns_findings_for_rust_matches(self, tmp_path: Path) -> None:
        """Test that search returns findings when Rust files match."""
        rust_dir = tmp_path / "src"
        rust_dir.mkdir()
        rust_file = rust_dir / "main.rs"
        rust_file.write_text('fn build_graph() {\n    println!("hello");\n}\n')
        findings, _diags, stats = rust_fallback_search(
            root=tmp_path,
            pattern="build_graph",
            macro_name="calls",
        )
        assert len(findings) >= 1
        assert findings[0].category == "rust_reference"
        matches = stats["matches"]
        assert isinstance(matches, int)
        assert matches >= 1

    def test_capability_diagnostics_present(self, tmp_path: Path) -> None:
        """Test that capability diagnostics are generated."""
        _findings, diags, _stats = rust_fallback_search(
            root=tmp_path,
            pattern="anything",
            macro_name="impact",
        )
        # Diagnostics are generated for macros with limited Rust support
        assert len(diags) >= 1
        assert diags[0].category == "capability_limitation"

    def test_fail_open_on_error(self, tmp_path: Path) -> None:
        """Test that errors don't propagate."""
        # Use a non-existent root path
        findings, _diags, _stats = rust_fallback_search(
            root=tmp_path / "nonexistent",
            pattern="anything",
            macro_name="calls",
        )
        assert findings == []

    def test_finding_anchor_uses_relative_path(self, tmp_path: Path) -> None:
        """Test that finding anchors use relative paths from root."""
        rust_dir = tmp_path / "src"
        rust_dir.mkdir()
        rust_file = rust_dir / "lib.rs"
        rust_file.write_text("pub fn my_func() -> i32 { 42 }\n")
        findings, _diags, _stats = rust_fallback_search(
            root=tmp_path,
            pattern="my_func",
            macro_name="calls",
        )
        assert len(findings) >= 1
        anchor = findings[0].anchor
        assert anchor is not None
        # Should be a relative path, not absolute
        assert not anchor.file.startswith("/")
        assert "lib.rs" in anchor.file

    def test_finding_detail_payload_has_language(self, tmp_path: Path) -> None:
        """Test that finding detail payloads include language metadata."""
        rust_dir = tmp_path / "src"
        rust_dir.mkdir()
        rust_file = rust_dir / "lib.rs"
        rust_file.write_text("pub fn helper() {}\n")
        findings, _diags, _stats = rust_fallback_search(
            root=tmp_path,
            pattern="helper",
            macro_name="calls",
        )
        assert len(findings) >= 1
        details = findings[0].details
        assert details is not None
        assert details.kind == "rust_reference"
        assert details.data["language"] == "rust"
        assert details.data["evidence_kind"] == "rg_only"

    def test_partition_stats_structure(self, tmp_path: Path) -> None:
        """Test that partition stats have expected keys."""
        _findings, _diags, stats = rust_fallback_search(
            root=tmp_path,
            pattern="anything",
            macro_name="calls",
        )
        assert "matches" in stats
        assert "files_scanned" in stats
        assert "matched_files" in stats
        assert "total_matches" in stats

    def test_multiple_matches_in_same_file(self, tmp_path: Path) -> None:
        """Test that multiple matches in one file are counted correctly."""
        rust_dir = tmp_path / "src"
        rust_dir.mkdir()
        rust_file = rust_dir / "lib.rs"
        rust_file.write_text("fn do_work() { }\nfn do_more_work() {\n    do_work();\n}\n")
        findings, _diags, stats = rust_fallback_search(
            root=tmp_path,
            pattern="do_work",
            macro_name="calls",
        )
        # Should find at least the definition and the call
        assert len(findings) >= MIN_RUST_MATCHES
        assert stats["matched_files"] == 1
        matches = stats["matches"]
        assert isinstance(matches, int)
        assert matches >= MIN_RUST_MATCHES
