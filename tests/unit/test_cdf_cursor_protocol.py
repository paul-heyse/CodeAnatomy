"""Protocol compliance tests for shared CDF cursor contracts."""

from __future__ import annotations

from pathlib import Path

from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore
from storage.cdf_cursor_protocol import CdfCursorStoreLike

EXPECTED_LAST_VERSION = 7


def test_cdf_cursor_store_satisfies_protocol(tmp_path: Path) -> None:
    """CdfCursorStore implements shared CdfCursorStoreLike protocol."""
    store: CdfCursorStoreLike = CdfCursorStore(cursors_path=tmp_path / "cursors")
    cursor = CdfCursor(dataset_name="dataset", last_version=EXPECTED_LAST_VERSION)

    store.save_cursor(cursor)
    loaded = store.load_cursor("dataset")

    assert loaded is not None
    assert loaded.last_version == EXPECTED_LAST_VERSION
