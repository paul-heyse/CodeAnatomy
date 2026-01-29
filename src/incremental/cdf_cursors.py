"""CDF cursor persistence for incremental Delta table reads."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import msgspec

from serde_msgspec import StructBaseCompat, StructBaseStrict, dumps_json, loads_json

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]


class CdfCursor(StructBaseCompat, frozen=True):
    """Cursor tracking the last processed Delta table version for CDF reads.

    Attributes
    ----------
    dataset_name : str
        Name of the dataset this cursor tracks.
    last_version : int
        Last Delta table version that was processed.
    """

    dataset_name: str
    last_version: NonNegInt


class CdfCursorStore(StructBaseStrict, frozen=True):
    """Persistent storage for CDF cursors.

    This store manages per-dataset cursors that track the last processed
    Delta table version for incremental CDF reads.

    Attributes
    ----------
    cursors_path : Path
        Path to the directory containing cursor files.
    """

    cursors_path: Path

    def ensure_dir(self) -> None:
        """Ensure the cursors directory exists."""
        self.cursors_path.mkdir(parents=True, exist_ok=True)

    def _cursor_file(self, dataset_name: str) -> Path:
        """Return the file path for a dataset's cursor.

        Parameters
        ----------
        dataset_name : str
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

        Parameters
        ----------
        cursor : CdfCursor
            Cursor to save.
        """
        self.ensure_dir()
        cursor_file = self._cursor_file(cursor.dataset_name)
        cursor_file.write_bytes(dumps_json(cursor, pretty=True))

    def load_cursor(self, dataset_name: str) -> CdfCursor | None:
        """Load a cursor from persistent storage.

        Parameters
        ----------
        dataset_name : str
            Name of the dataset.

        Returns
        -------
        CdfCursor | None
            Cursor if it exists, None otherwise.
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
        dataset_name : str
            Name of the dataset.

        Returns
        -------
        bool
            True if cursor exists, False otherwise.
        """
        return self._cursor_file(dataset_name).exists()

    def delete_cursor(self, dataset_name: str) -> None:
        """Delete a cursor from persistent storage.

        Parameters
        ----------
        dataset_name : str
            Name of the dataset.
        """
        cursor_file = self._cursor_file(dataset_name)
        if cursor_file.exists():
            cursor_file.unlink()

    def list_cursors(self) -> list[CdfCursor]:
        """List all cursors in the store.

        Returns
        -------
        list[CdfCursor]
            List of all cursors.
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


__all__ = ["CdfCursor", "CdfCursorStore"]
