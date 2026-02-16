"""Tests for semantics.incremental.cdf_cursors module."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from semantics.incremental.cdf_cursors import CdfCursor, CdfCursorStore

START_VERSION = 5
UPDATED_VERSION = 10
OVERWRITE_VERSION = 15
CURSOR_LIST_COUNT = 3
NEXT_VERSION_AFTER_FIVE = 6


class TestCdfCursor:
    """Tests for CdfCursor dataclass."""

    @staticmethod
    @pytest.mark.smoke
    def test_create_factory_method() -> None:
        """CdfCursor.create factory method creates cursor."""
        cursor = CdfCursor.create("my_dataset", START_VERSION)
        assert cursor.dataset_name == "my_dataset"
        assert cursor.last_version == START_VERSION
        assert cursor.last_timestamp is None

    @staticmethod
    def test_create_with_zero_version() -> None:
        """CdfCursor.create works with version 0."""
        cursor = CdfCursor.create("dataset", 0)
        assert cursor.last_version == 0

    @staticmethod
    def test_direct_construction() -> None:
        """CdfCursor can be constructed directly."""
        cursor = CdfCursor(
            dataset_name="direct_dataset",
            last_version=UPDATED_VERSION,
            last_timestamp="2024-01-01T00:00:00Z",
        )
        assert cursor.dataset_name == "direct_dataset"
        assert cursor.last_version == UPDATED_VERSION
        assert cursor.last_timestamp == "2024-01-01T00:00:00Z"

    @staticmethod
    def test_cursor_is_frozen() -> None:
        """CdfCursor is immutable."""
        cursor = CdfCursor.create("dataset", START_VERSION)
        attr_name = "last_version"
        with pytest.raises(FrozenInstanceError):
            setattr(cursor, attr_name, UPDATED_VERSION)

    @staticmethod
    def test_cursor_with_timestamp() -> None:
        """CdfCursor can store timestamp."""
        cursor = CdfCursor(
            dataset_name="dataset",
            last_version=1,
            last_timestamp="2024-06-15T10:30:00Z",
        )
        assert cursor.last_timestamp == "2024-06-15T10:30:00Z"


class TestCdfCursorStore:
    """Tests for CdfCursorStore class."""

    @staticmethod
    @pytest.mark.smoke
    def test_store_creation(tmp_path: Path) -> None:
        """CdfCursorStore can be created with path."""
        cursors_path = tmp_path / "cursors"
        store = CdfCursorStore(cursors_path=cursors_path)
        assert store.cursors_path == cursors_path

    @staticmethod
    def test_ensure_dir_creates_directory(tmp_path: Path) -> None:
        """ensure_dir creates the cursors directory."""
        cursors_path = tmp_path / "new_cursors"
        store = CdfCursorStore(cursors_path=cursors_path)

        assert not cursors_path.exists()
        store.ensure_dir()
        assert cursors_path.exists()
        assert cursors_path.is_dir()

    @staticmethod
    def test_ensure_dir_idempotent(tmp_path: Path) -> None:
        """ensure_dir is idempotent."""
        cursors_path = tmp_path / "cursors"
        store = CdfCursorStore(cursors_path=cursors_path)

        store.ensure_dir()
        store.ensure_dir()  # Should not raise
        assert cursors_path.exists()


class TestCdfCursorStoreSaveLoad:
    """Tests for CdfCursorStore save/load operations."""

    @staticmethod
    @pytest.mark.smoke
    def test_save_and_load_cursor(tmp_path: Path) -> None:
        """save_cursor and load_cursor work correctly."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")
        cursor = CdfCursor.create("test_dataset", UPDATED_VERSION)

        store.save_cursor(cursor)
        loaded = store.load_cursor("test_dataset")

        assert loaded is not None
        assert loaded.dataset_name == "test_dataset"
        assert loaded.last_version == UPDATED_VERSION

    @staticmethod
    def test_load_nonexistent_cursor(tmp_path: Path) -> None:
        """load_cursor returns None for nonexistent cursor."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")
        store.ensure_dir()

        loaded = store.load_cursor("nonexistent_dataset")
        assert loaded is None

    @staticmethod
    def test_save_overwrites_existing(tmp_path: Path) -> None:
        """save_cursor overwrites existing cursor."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")

        cursor1 = CdfCursor.create("dataset", START_VERSION)
        store.save_cursor(cursor1)

        cursor2 = CdfCursor.create("dataset", OVERWRITE_VERSION)
        store.save_cursor(cursor2)

        loaded = store.load_cursor("dataset")
        assert loaded is not None
        assert loaded.last_version == OVERWRITE_VERSION

    @staticmethod
    def test_save_multiple_cursors(tmp_path: Path) -> None:
        """Multiple cursors can be saved independently."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")

        store.save_cursor(CdfCursor.create("dataset_a", START_VERSION))
        store.save_cursor(CdfCursor.create("dataset_b", UPDATED_VERSION))

        loaded_a = store.load_cursor("dataset_a")
        loaded_b = store.load_cursor("dataset_b")

        assert loaded_a is not None
        assert loaded_a.last_version == START_VERSION
        assert loaded_b is not None
        assert loaded_b.last_version == UPDATED_VERSION


class TestCdfCursorStoreDelete:
    """Tests for CdfCursorStore delete operations."""

    @staticmethod
    @pytest.mark.smoke
    def test_delete_cursor(tmp_path: Path) -> None:
        """delete_cursor removes cursor file."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")
        cursor = CdfCursor.create("to_delete", START_VERSION)
        store.save_cursor(cursor)

        assert store.load_cursor("to_delete") is not None

        store.delete_cursor("to_delete")
        assert store.load_cursor("to_delete") is None

    @staticmethod
    def test_delete_nonexistent_cursor(tmp_path: Path) -> None:
        """delete_cursor does nothing for nonexistent cursor."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")
        store.ensure_dir()

        # Should not raise
        store.delete_cursor("nonexistent")

    @staticmethod
    def test_has_cursor(tmp_path: Path) -> None:
        """has_cursor returns correct status."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")

        assert not store.has_cursor("dataset")

        store.save_cursor(CdfCursor.create("dataset", START_VERSION))
        assert store.has_cursor("dataset")

        store.delete_cursor("dataset")
        assert not store.has_cursor("dataset")


class TestCdfCursorStoreList:
    """Tests for CdfCursorStore list operations."""

    @staticmethod
    def test_list_empty_store(tmp_path: Path) -> None:
        """list_cursors returns empty list for empty store."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")
        store.ensure_dir()

        result = store.list_cursors()
        assert result == []

    @staticmethod
    def test_list_all_cursors(tmp_path: Path) -> None:
        """list_cursors returns all saved cursors."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")

        store.save_cursor(CdfCursor.create("dataset_a", START_VERSION))
        store.save_cursor(CdfCursor.create("dataset_b", UPDATED_VERSION))
        store.save_cursor(CdfCursor.create("dataset_c", OVERWRITE_VERSION))

        result = store.list_cursors()
        assert len(result) == CURSOR_LIST_COUNT

        names = {c.dataset_name for c in result}
        assert names == {"dataset_a", "dataset_b", "dataset_c"}

    @staticmethod
    def test_list_cursors_nonexistent_directory(tmp_path: Path) -> None:
        """list_cursors returns empty for nonexistent directory."""
        store = CdfCursorStore(cursors_path=tmp_path / "nonexistent")

        result = store.list_cursors()
        assert result == []


class TestCdfCursorStoreUpdateVersion:
    """Tests for CdfCursorStore update_version method."""

    @staticmethod
    @pytest.mark.smoke
    def test_update_version(tmp_path: Path) -> None:
        """update_version creates and saves cursor."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")

        cursor = store.update_version("dataset", UPDATED_VERSION)
        assert cursor.dataset_name == "dataset"
        assert cursor.last_version == UPDATED_VERSION

        # Verify it was persisted
        loaded = store.load_cursor("dataset")
        assert loaded is not None
        assert loaded.last_version == UPDATED_VERSION

    @staticmethod
    def test_update_version_overwrites(tmp_path: Path) -> None:
        """update_version overwrites existing cursor."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")

        store.update_version("dataset", START_VERSION)
        store.update_version("dataset", OVERWRITE_VERSION)

        loaded = store.load_cursor("dataset")
        assert loaded is not None
        assert loaded.last_version == OVERWRITE_VERSION


class TestCdfCursorStoreGetStartVersion:
    """Tests for CdfCursorStore get_start_version method."""

    @staticmethod
    @pytest.mark.smoke
    def test_get_start_version_returns_next_version(tmp_path: Path) -> None:
        """get_start_version returns last_version + 1."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")
        store.update_version("dataset", START_VERSION)

        start = store.get_start_version("dataset")
        assert start == NEXT_VERSION_AFTER_FIVE

    @staticmethod
    def test_get_start_version_returns_none_for_missing(tmp_path: Path) -> None:
        """get_start_version returns None for missing cursor."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")
        store.ensure_dir()

        start = store.get_start_version("nonexistent")
        assert start is None

    @staticmethod
    def test_get_start_version_zero_based(tmp_path: Path) -> None:
        """get_start_version handles version 0 correctly."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")
        store.update_version("dataset", 0)

        start = store.get_start_version("dataset")
        assert start == 1


class TestCdfCursorStoreSanitization:
    """Tests for dataset name sanitization."""

    @staticmethod
    def test_sanitizes_slashes_in_name(tmp_path: Path) -> None:
        """Dataset names with slashes are sanitized."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")

        # Names with slashes should be saved safely
        store.save_cursor(CdfCursor.create("path/to/dataset", START_VERSION))

        loaded = store.load_cursor("path/to/dataset")
        assert loaded is not None
        assert loaded.dataset_name == "path/to/dataset"

    @staticmethod
    def test_sanitizes_backslashes_in_name(tmp_path: Path) -> None:
        """Dataset names with backslashes are sanitized."""
        store = CdfCursorStore(cursors_path=tmp_path / "cursors")

        store.save_cursor(CdfCursor.create("path\\to\\dataset", START_VERSION))

        loaded = store.load_cursor("path\\to\\dataset")
        assert loaded is not None
        assert loaded.dataset_name == "path\\to\\dataset"
