"""Shell completion helpers for the CLI."""

from __future__ import annotations

from pathlib import Path

from cyclopts import App


def generate_completion_scripts(app: App, output_dir: Path) -> None:
    """Generate completion scripts for supported shells.

    Parameters
    ----------
    app
        Cyclopts application.
    output_dir
        Directory to write completion scripts into.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    for shell in ("bash", "zsh", "fish"):
        script = app.generate_completion(shell=shell)
        output_path = output_dir / f"codeanatomy.{shell}"
        output_path.write_text(script, encoding="utf-8")


__all__ = ["generate_completion_scripts"]
