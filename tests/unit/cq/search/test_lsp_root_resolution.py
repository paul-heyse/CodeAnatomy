"""Unit tests for LSP provider root resolution."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.lsp.root_resolution import resolve_lsp_provider_root


def _write(path: Path, content: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_resolve_python_provider_root_prefers_nearest_workspace(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    _write(repo_root / "pyproject.toml", "[project]\nname='repo'\n")
    nested = repo_root / "packages" / "service"
    _write(nested / "pyproject.toml", "[project]\nname='service'\n")
    target = nested / "app" / "handlers.py"
    _write(target, "def handle() -> None:\n    pass\n")

    resolved = resolve_lsp_provider_root(
        language="python",
        command_root=repo_root,
        file_path=target,
    )

    assert resolved == nested.resolve()


def test_resolve_rust_provider_root_prefers_nearest_cargo(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    _write(repo_root / "Cargo.toml", "[workspace]\nmembers=['crates/core']\n")
    nested = repo_root / "crates" / "core"
    _write(nested / "Cargo.toml", "[package]\nname='core'\nversion='0.1.0'\n")
    target = nested / "src" / "lib.rs"
    _write(target, "pub fn compute() -> i32 { 1 }\n")

    resolved = resolve_lsp_provider_root(
        language="rust",
        command_root=repo_root,
        file_path=target,
    )

    assert resolved == nested.resolve()


def test_resolve_provider_root_falls_back_to_command_root(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    source = repo_root / "src" / "module.py"
    _write(source, "def value() -> int:\n    return 1\n")

    resolved = resolve_lsp_provider_root(
        language="python",
        command_root=repo_root,
        file_path=source,
    )

    assert resolved == repo_root.resolve()
