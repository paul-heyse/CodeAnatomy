"""Persistent state store paths for incremental runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StateStore:
    """Filesystem layout for incremental datasets and snapshots."""

    root: Path

    def ensure_dirs(self) -> None:
        """Ensure the state store directories exist."""
        self.snapshots_dir().mkdir(parents=True, exist_ok=True)
        self.datasets_dir().mkdir(parents=True, exist_ok=True)
        self.metadata_dir().mkdir(parents=True, exist_ok=True)

    def snapshots_dir(self) -> Path:
        """Return the snapshots directory.

        Returns
        -------
        Path
            Snapshots directory path.
        """
        return self.root / "snapshots"

    def datasets_dir(self) -> Path:
        """Return the datasets directory.

        Returns
        -------
        Path
            Datasets directory path.
        """
        return self.root / "datasets"

    def latest_snapshot_dir(self) -> Path:
        """Return the directory for the latest snapshot.

        Returns
        -------
        Path
            Latest snapshot directory path.
        """
        return self.snapshots_dir() / "latest"

    def repo_snapshot_path(self) -> Path:
        """Return the path to the current repo snapshot Delta table.

        Returns
        -------
        Path
            Repo snapshot Delta table path.
        """
        return self.latest_snapshot_dir() / "repo_snapshot"

    def incremental_diff_path(self) -> Path:
        """Return the path to the incremental diff Delta table.

        Returns
        -------
        Path
            Incremental diff Delta table path.
        """
        return self.latest_snapshot_dir() / "incremental_diff"

    def scip_snapshot_path(self) -> Path:
        """Return the path to the SCIP snapshot Delta table.

        Returns
        -------
        Path
            SCIP snapshot Delta table path.
        """
        return self.latest_snapshot_dir() / "scip_snapshot"

    def scip_diff_path(self) -> Path:
        """Return the path to the SCIP diff Delta table.

        Returns
        -------
        Path
            SCIP diff Delta table path.
        """
        return self.latest_snapshot_dir() / "scip_diff"

    def dataset_dir(self, dataset_name: str) -> Path:
        """Return the dataset directory for a named dataset.

        Returns
        -------
        Path
            Dataset directory path.
        """
        return self.datasets_dir() / Path(dataset_name)

    def metadata_dir(self) -> Path:
        """Return the metadata directory.

        Returns
        -------
        Path
            Metadata directory path.
        """
        return self.root / "metadata"

    def invalidation_snapshot_path(self) -> Path:
        """Return the invalidation snapshot Delta path.

        Returns
        -------
        Path
            Invalidation snapshot Delta table path.
        """
        return self.metadata_dir() / "invalidation_snapshot"

    def scip_fingerprint_path(self) -> Path:
        """Return the SCIP index fingerprint path.

        Returns
        -------
        Path
            SCIP fingerprint path.
        """
        return self.metadata_dir() / "scip_index_fingerprint.txt"

    def cdf_cursors_path(self) -> Path:
        """Return the directory for CDF cursors.

        Returns
        -------
        Path
            CDF cursors directory path.
        """
        return self.metadata_dir() / "cdf_cursors"

    def incremental_metadata_path(self) -> Path:
        """Return the Delta path for incremental runtime metadata.

        Returns
        -------
        Path
            Metadata Delta table path.
        """
        return self.metadata_dir() / "incremental_metadata"

    def cdf_cursor_snapshot_path(self) -> Path:
        """Return the Delta path for CDF cursor snapshots.

        Returns
        -------
        Path
            CDF cursor snapshot Delta table path.
        """
        return self.metadata_dir() / "cdf_cursor_snapshot"

    def pruning_metrics_path(self) -> Path:
        """Return the Delta path for pruning metrics.

        Returns
        -------
        Path
            Pruning metrics Delta table path.
        """
        return self.metadata_dir() / "incremental_pruning_metrics"

    def view_artifacts_path(self) -> Path:
        """Return the Delta path for view artifact snapshots.

        Returns
        -------
        Path
            View artifact Delta table path.
        """
        return self.metadata_dir() / "incremental_view_artifacts"


__all__ = ["StateStore"]
