"""Shell completion helpers for the CQ CLI."""

from __future__ import annotations

from pathlib import Path

from cyclopts import App

SUPPORTED_SHELLS = ("bash", "zsh", "fish")


def render_completion_scripts(app: App) -> dict[str, str]:
    """Render completion script text for supported shells.

    Returns:
        dict[str, str]: Mapping of shell name to completion script text.
    """
    return {shell: app.generate_completion(shell=shell) for shell in SUPPORTED_SHELLS}


def generate_completion_scripts(
    app: App,
    output_dir: Path,
    *,
    program_name: str = "cq",
) -> dict[str, Path]:
    """Generate completion scripts for supported shells.

    Returns:
        dict[str, Path]: Mapping of shell name to written completion file path.
    """
    scripts = render_completion_scripts(app)
    output_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for shell, script in scripts.items():
        output_path = _completion_output_path(output_dir, program_name=program_name, shell=shell)
        output_path.write_text(script, encoding="utf-8")
        written[shell] = output_path
    return written


def completion_scripts_need_update(
    app: App,
    output_dir: Path,
    *,
    program_name: str = "cq",
) -> list[Path]:
    """Return completion files that are missing or stale."""
    scripts = render_completion_scripts(app)
    stale: list[Path] = []
    for shell, expected in scripts.items():
        path = _completion_output_path(output_dir, program_name=program_name, shell=shell)
        if not path.exists():
            stale.append(path)
            continue
        if path.read_text(encoding="utf-8") != expected:
            stale.append(path)
    return stale


def _completion_output_path(output_dir: Path, *, program_name: str, shell: str) -> Path:
    return output_dir / f"{program_name}.{shell}"


__all__ = [
    "completion_scripts_need_update",
    "generate_completion_scripts",
    "render_completion_scripts",
]
