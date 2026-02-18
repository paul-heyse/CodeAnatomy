"""Tests for interval-align kernel bridge routing."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine import kernels
from datafusion_engine.extensions import datafusion_ext


def test_interval_align_kernel_uses_bridge_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kernel should return bridge output when native bridge responds."""
    left = pa.table({"path": ["a.py"], "bstart": [0], "bend": [4], "value": [1]})
    right = pa.table({"path": ["a.py"], "bstart": [0], "bend": [4], "score": [9]})
    expected = pa.table({"path": ["a.py"], "bstart": [0], "bend": [4], "score": [9]})

    captured: dict[str, object] = {}

    def _bridge(left_table: object, right_table: object, payload: dict[str, object]) -> pa.Table:
        captured["left"] = left_table
        captured["right"] = right_table
        captured["payload"] = payload
        return expected

    monkeypatch.setattr(datafusion_ext, "interval_align_table", _bridge, raising=False)

    result = kernels.interval_align_kernel(
        left,
        right,
        cfg=kernels.IntervalAlignOptions(),
        runtime_profile=None,
    )

    assert isinstance(result, pa.Table)
    assert result.equals(expected)
    assert isinstance(captured["payload"], dict)
    assert captured["payload"]["mode"] == "CONTAINED_BEST"


def test_interval_align_kernel_raises_when_bridge_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hard-cutover path should fail when the bridge produces no payload."""
    left = pa.table({"path": ["a.py"], "bstart": [0], "bend": [4], "value": [1]})
    right = pa.table({"path": ["a.py"], "bstart": [0], "bend": [4], "score": [9]})

    monkeypatch.setattr(datafusion_ext, "interval_align_table", lambda *_args: None, raising=False)

    with pytest.raises(RuntimeError, match="interval_align_table bridge"):
        _ = kernels.interval_align_kernel(
            left,
            right,
            cfg=kernels.IntervalAlignOptions(),
            runtime_profile=None,
        )
