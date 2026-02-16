# ruff: noqa: D100, D103, INP001
from __future__ import annotations

from datafusion_engine.udf.constants import (
    ABI_LOAD_FAILURE_MSG,
    ABI_MISMATCH_PREFIX,
    ABI_VERSION_MISMATCH_MSG,
    EXTENSION_ENTRY_POINT,
    EXTENSION_MODULE_LABEL,
    EXTENSION_MODULE_PATH,
    EXTENSION_MODULE_PREFIX,
    REBUILD_WHEELS_HINT,
)


def test_extension_module_constants_are_stable() -> None:
    assert EXTENSION_MODULE_PATH == "datafusion_engine.extensions.datafusion_ext"
    assert EXTENSION_MODULE_LABEL == "datafusion_ext"
    assert EXTENSION_MODULE_PREFIX == "datafusion_engine.extensions."
    assert EXTENSION_ENTRY_POINT == "register"


def test_user_facing_messages_include_expected_markers() -> None:
    assert "Extension ABI mismatch" in ABI_MISMATCH_PREFIX
    assert "{expected}" in ABI_VERSION_MISMATCH_MSG
    assert "{actual}" in ABI_VERSION_MISMATCH_MSG
    assert "{module" in ABI_LOAD_FAILURE_MSG
    assert "build_datafusion_wheels.sh" in REBUILD_WHEELS_HINT
