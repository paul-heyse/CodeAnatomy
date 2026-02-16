"""Macro scope filter regression tests."""

from __future__ import annotations

from pathlib import Path

from tools.cq.macros.exceptions import _scan_exceptions
from tools.cq.macros.imports import _collect_imports
from tools.cq.macros.side_effects import _scan_side_effects


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_imports_scope_filters_include_only_selected_files(tmp_path: Path) -> None:
    """Test imports scope filters include only selected files."""
    _write(
        tmp_path / "pkg" / "a.py",
        "import os\nfrom pathlib import Path\n",
    )
    _write(
        tmp_path / "pkg" / "b.py",
        "import json\n",
    )

    _deps, imports = _collect_imports(
        tmp_path,
        include=["pkg/a.py"],
        exclude=None,
    )
    assert imports
    assert {item.file for item in imports} == {"pkg/a.py"}


def test_exceptions_scope_filters_exclude_files(tmp_path: Path) -> None:
    """Test exceptions scope filters exclude files."""
    _write(
        tmp_path / "pkg" / "a.py",
        "def alpha():\n    raise ValueError('a')\n",
    )
    _write(
        tmp_path / "pkg" / "b.py",
        "def beta():\n    raise RuntimeError('b')\n",
    )

    raises, catches, files_scanned = _scan_exceptions(
        tmp_path,
        include=None,
        exclude=["pkg/a.py"],
    )
    assert files_scanned == 1
    assert not catches
    assert raises
    assert {item.file for item in raises} == {"pkg/b.py"}


def test_side_effects_scope_filters_include_files(tmp_path: Path) -> None:
    """Test side effects scope filters include files."""
    _write(
        tmp_path / "pkg" / "a.py",
        "import os\nENV = os.environ.get('HOME')\n",
    )
    _write(
        tmp_path / "pkg" / "b.py",
        "import os\nVALUE = os.getcwd()\n",
    )

    effects, files_scanned = _scan_side_effects(
        tmp_path,
        max_files=20,
        include=["pkg/a.py"],
        exclude=None,
    )
    assert files_scanned == 1
    assert effects
    assert {item.file for item in effects} == {"pkg/a.py"}
