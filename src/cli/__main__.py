"""Module entrypoint for CodeAnatomy CLI."""

from __future__ import annotations

from cli.app import app


def main() -> None:
    """Run the CodeAnatomy CLI."""
    app.meta()


if __name__ == "__main__":
    main()
