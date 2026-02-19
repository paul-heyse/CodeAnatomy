"""Cache invalidation tests for extract coordination spec helper caches."""

from __future__ import annotations

import pytest

from extract.coordination import spec_helpers

_EXPECTED_REBUILDS = 2


def test_clear_spec_caches_resets_lru_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clearing caches should force recomputation on next helper invocation."""
    calls = {"count": 0}

    def _rows_for_template(_template_name: str) -> tuple[object, ...]:
        calls["count"] += 1
        return ()

    monkeypatch.setattr(spec_helpers, "_rows_for_template", _rows_for_template)

    spec_helpers.clear_spec_caches()
    _ = spec_helpers._feature_flag_rows("ast")  # noqa: SLF001
    _ = spec_helpers._feature_flag_rows("ast")  # noqa: SLF001
    assert calls["count"] == 1

    spec_helpers.clear_spec_caches()
    _ = spec_helpers._feature_flag_rows("ast")  # noqa: SLF001
    assert calls["count"] == _EXPECTED_REBUILDS
