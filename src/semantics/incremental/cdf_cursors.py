"""CDF cursor persistence for incremental semantic pipeline reads.

This module provides cursor management for tracking the last processed Delta
table version during incremental CDF (Change Data Feed) reads in the semantic
pipeline. Cursors enable efficient incremental processing by recording version
checkpoints.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import msgspec

from serde_msgspec import StructBaseCompat, StructBaseStrict, dumps_json, loads_json

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]


class CdfCursor(StructBaseCompat, frozen=True):
    """Cursor tracking the last processed Delta table version for CDF reads.

    Immutable cursor that records version checkpoints for incremental
    processing. Uses ``StructBaseCompat`` for forward compatibility with
    future schema additions.

    Attributes
    ----------
    dataset_name : str
        Name of the dataset this cursor tracks.
    last_version : int
        Last Delta table version that was processed (non-negative).
    last_timestamp : str | None
        Optional ISO 8601 timestamp of when the cursor was last updated.
        Useful for debugging and audit trails.
    """

    dataset_name: str
    last_version: NonNegInt
    last_timestamp: str | None = None

    @classmethod
    def create(cls, dataset_name: str, version: int) -> CdfCursor:
        """Create a new CDF cursor for a dataset.

        Factory method that creates a cursor with the given dataset name
        and version. The timestamp is not set by this factory; callers
        may use struct replacement to add a timestamp if needed.

        Parameters
        ----------
        dataset_name
            Name of the dataset to track.
        version
            Delta table version to record.

        Returns
        -------
        CdfCursor
            New cursor instance.

        Examples
        --------
        >>> cursor = CdfCursor.create("my_dataset", 5)
        >>> cursor.dataset_name
        'my_dataset'
        >>> cursor.last_version
        5
        """
        return cls(dataset_name=dataset_name, last_version=version)


class CdfCursorStore(StructBaseStrict, frozen=True):
    """Persistent storage for CDF cursors.

    Manages per-dataset cursors that track the last processed Delta table
    version for incremental CDF reads. Cursors are stored as individual
    JSON files in the configured directory.

    Uses ``StructBaseStrict`` to enforce strict field validation, ensuring
    cursor file paths are always valid.

    Attributes
    ----------
    cursors_path : Path
        Path to the directory containing cursor files.
    """

    cursors_path: Path

    def ensure_dir(self) -> None:
        """Ensure the cursors directory exists.

        Creates the cursor directory and any necessary parent directories
        if they do not already exist.
        """
        self.cursors_path.mkdir(parents=True, exist_ok=True)

    def _cursor_file(self, dataset_name: str) -> Path:
        """Return the file path for a dataset's cursor.

        Parameters
        ----------
        dataset_name
            Name of the dataset.

        Returns
        -------
        Path
            Path to the cursor file.
        """
        # Sanitize dataset name for filesystem
        safe_name = dataset_name.replace("/", "_").replace("\\", "_")
        return self.cursors_path / f"{safe_name}.cursor.json"

    def save_cursor(self, cursor: CdfCursor) -> None:
        """Save a cursor to persistent storage.

        Serializes the cursor to JSON and writes it to the cursor file.
        Creates the cursor directory if it does not exist.

        Parameters
        ----------
        cursor
            Cursor to save.
        """
        self.ensure_dir()
        cursor_file = self._cursor_file(cursor.dataset_name)
        cursor_file.write_bytes(dumps_json(cursor, pretty=True))

    def load_cursor(self, dataset_name: str) -> CdfCursor | None:
        """Load a cursor from persistent storage.

        Attempts to read and deserialize a cursor from its file. Returns
        ``None`` if the cursor file does not exist or cannot be decoded.

        Parameters
        ----------
        dataset_name
            Name of the dataset.

        Returns
        -------
        CdfCursor | None
            Cursor if it exists and is valid, ``None`` otherwise.
        """
        cursor_file = self._cursor_file(dataset_name)
        if not cursor_file.exists():
            return None
        try:
            return loads_json(cursor_file.read_bytes(), target_type=CdfCursor, strict=False)
        except (msgspec.DecodeError, OSError):
            return None

    def has_cursor(self, dataset_name: str) -> bool:
        """Check if a cursor exists for a dataset.

        Parameters
        ----------
        dataset_name
            Name of the dataset.

        Returns
        -------
        bool
            ``True`` if cursor file exists, ``False`` otherwise.
        """
        return self._cursor_file(dataset_name).exists()

    def delete_cursor(self, dataset_name: str) -> None:
        """Delete a cursor from persistent storage.

        Removes the cursor file if it exists. Does nothing if the cursor
        file does not exist.

        Parameters
        ----------
        dataset_name
            Name of the dataset.
        """
        cursor_file = self._cursor_file(dataset_name)
        if cursor_file.exists():
            cursor_file.unlink()

    def list_cursors(self) -> list[CdfCursor]:
        """List all cursors in the store.

        Scans the cursor directory for all cursor files and attempts to
        load each one. Invalid cursor files are silently skipped.

        Returns
        -------
        list[CdfCursor]
            List of all successfully loaded cursors.
        """
        if not self.cursors_path.exists():
            return []
        cursors: list[CdfCursor] = []
        for cursor_file in self.cursors_path.glob("*.cursor.json"):
            try:
                cursor = loads_json(
                    cursor_file.read_bytes(),
                    target_type=CdfCursor,
                    strict=False,
                )
                cursors.append(cursor)
            except (msgspec.DecodeError, OSError):
                continue
        return cursors

    def update_version(self, dataset_name: str, version: int) -> CdfCursor:
        """Update a dataset's cursor to a new version.

        Creates a new cursor with the specified version and saves it to
        persistent storage. If a cursor already exists for the dataset,
        it is replaced with the new version.

        Parameters
        ----------
        dataset_name
            Name of the dataset.
        version
            New Delta table version to record.

        Returns
        -------
        CdfCursor
            The updated cursor instance.

        Examples
        --------
        >>> store = CdfCursorStore(cursors_path=Path("/tmp/cursors"))
        >>> cursor = store.update_version("my_dataset", 10)
        >>> cursor.last_version
        10
        """
        timestamp = datetime.now(tz=UTC).isoformat()
        cursor = CdfCursor(
            dataset_name=dataset_name,
            last_version=version,
            last_timestamp=timestamp,
        )
        self.save_cursor(cursor)
        return cursor

    def get_start_version(self, dataset_name: str) -> int | None:
        """Return the starting version for the next CDF read.

        Loads the existing cursor for the dataset and returns the next
        version to read from (last_version + 1). Returns ``None`` if no
        cursor exists, indicating a full refresh is needed.

        Parameters
        ----------
        dataset_name
            Name of the dataset.

        Returns
        -------
        int | None
            Next version to read from, or ``None`` if no cursor exists.

        Examples
        --------
        >>> store = CdfCursorStore(cursors_path=Path("/tmp/cursors"))
        >>> store.update_version("my_dataset", 5)
        CdfCursor(dataset_name='my_dataset', last_version=5)
        >>> store.get_start_version("my_dataset")
        6
        >>> store.get_start_version("unknown_dataset") is None
        True
        """
        cursor = self.load_cursor(dataset_name)
        if cursor is None:
            return None
        return cursor.last_version + 1


__all__ = ["CdfCursor", "CdfCursorStore"]
