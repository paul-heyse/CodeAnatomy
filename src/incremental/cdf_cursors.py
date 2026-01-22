"""CDF cursor persistence for incremental Delta table reads."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class CdfCursor:
    """Cursor tracking the last processed Delta table version for CDF reads.

    Attributes
    ----------
    dataset_name : str
        Name of the dataset this cursor tracks.
    last_version : int
        Last Delta table version that was processed.
    """

    dataset_name: str
    last_version: int

    def to_dict(self) -> dict[str, object]:
        """Convert cursor to dictionary representation.

        Returns
        -------
        dict[str, object]
            Dictionary representation of the cursor.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> CdfCursor:
        """Create cursor from dictionary representation.

        Parameters
        ----------
        data : dict[str, object]
            Dictionary containing cursor data.

        Returns
        -------
        CdfCursor
            Cursor instance.
        """
        return cls(
            dataset_name=str(data["dataset_name"]),
            last_version=_coerce_int(data.get("last_version")),
        )


def _coerce_int(value: object) -> int:
    if isinstance(value, bool) or value is None:
        msg = "CDF cursor last_version must be an int."
        raise ValueError(msg)
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError) as exc:
        msg = "CDF cursor last_version must be an int."
        raise ValueError(msg) from exc


@dataclass(frozen=True)
class CdfCursorStore:
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
        cursor_file.write_text(json.dumps(cursor.to_dict(), indent=2))

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
            data = json.loads(cursor_file.read_text())
            return CdfCursor.from_dict(data)
        except (json.JSONDecodeError, KeyError, ValueError):
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
                data = json.loads(cursor_file.read_text())
                cursors.append(CdfCursor.from_dict(data))
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
        return cursors


__all__ = ["CdfCursor", "CdfCursorStore"]
