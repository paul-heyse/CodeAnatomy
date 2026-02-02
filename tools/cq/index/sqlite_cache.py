from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

# Cache configuration constants
INDEX_DIR = ".cq"
INDEX_FILE = "index.sqlite"
SCHEMA_VERSION = "1"


@dataclass
class CacheStats:
    """Statistics about the index cache.

    Attributes
    ----------
    total_files : int
        Total number of cached files.
    total_records : int
        Total number of records across all files.
    rule_version : str
        Current rule version in use.
    database_size_bytes : int
        Size of the SQLite database file in bytes.
    """

    total_files: int
    total_records: int
    rule_version: str
    database_size_bytes: int


class IndexCache:
    """SQLite-backed cache for incremental code scanning.

    Manages a persistent index of scanned files with content hashes to enable
    incremental rescanning when files change.

    Parameters
    ----------
    repo_root : Path
        Root directory of the repository.
    rule_version : str
        Version identifier for the current rule set. Cache is invalidated
        when this changes.

    Attributes
    ----------
    db_path : Path
        Full path to the SQLite database file.
    rule_version : str
        Current rule version.
    """

    def __init__(self, repo_root: Path, rule_version: str) -> None:
        """Initialize the index cache.

        Parameters
        ----------
        repo_root : Path
            Root directory of the repository.
        rule_version : str
            Version identifier for the current rule set.
        """
        self.repo_root = repo_root
        self.rule_version = rule_version
        cache_dir = repo_root / INDEX_DIR
        cache_dir.mkdir(exist_ok=True)
        self.db_path = cache_dir / INDEX_FILE
        self._conn: sqlite3.Connection | None = None

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection.

        Returns
        -------
        sqlite3.Connection
            Active database connection.
        """
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def initialize(self) -> None:
        """Initialize the database schema.

        Creates the scan_cache table if it doesn't exist. Safe to call
        multiple times.
        """
        conn = self._get_connection()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scan_cache (
                file_path TEXT PRIMARY KEY,
                content_hash TEXT NOT NULL,
                mtime REAL NOT NULL,
                rule_version TEXT NOT NULL,
                records_json TEXT NOT NULL,
                schema_version TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_rule_version
            ON scan_cache(rule_version)
            """
        )
        conn.commit()

    def needs_rescan(self, file_path: Path) -> bool:
        """Check if a file needs to be rescanned.

        A file needs rescanning if:
        - It's not in the cache
        - Its content hash has changed
        - Its mtime has changed
        - The rule version has changed

        Parameters
        ----------
        file_path : Path
            Path to the file to check (relative to repo_root).

        Returns
        -------
        bool
            True if the file needs rescanning, False if cached results are valid.
        """
        if not file_path.exists():
            return False

        file_path_str = str(file_path.relative_to(self.repo_root))
        current_mtime = file_path.stat().st_mtime
        current_hash = compute_file_hash(file_path)

        conn = self._get_connection()
        cursor = conn.execute(
            """
            SELECT content_hash, mtime, rule_version
            FROM scan_cache
            WHERE file_path = ?
            """,
            (file_path_str,),
        )
        row = cursor.fetchone()

        if row is None:
            return True

        cached_hash = row["content_hash"]
        cached_mtime = row["mtime"]
        cached_rule_version = row["rule_version"]

        # Need rescan if hash, mtime, or rule version changed
        if cached_hash != current_hash:
            return True
        if cached_mtime != current_mtime:
            return True
        if cached_rule_version != self.rule_version:
            return True

        return False

    def store(self, file_path: Path, records: Sequence[dict[str, object]]) -> None:
        """Store scan results for a file.

        Parameters
        ----------
        file_path : Path
            Path to the file (relative to repo_root).
        records : Sequence[dict[str, object]]
            Scan records to cache (must be JSON-serializable).
        """
        file_path_str = str(file_path.relative_to(self.repo_root))
        content_hash = compute_file_hash(file_path)
        mtime = file_path.stat().st_mtime
        records_json = json.dumps(records)

        conn = self._get_connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO scan_cache
            (file_path, content_hash, mtime, rule_version, records_json, schema_version)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                file_path_str,
                content_hash,
                mtime,
                self.rule_version,
                records_json,
                SCHEMA_VERSION,
            ),
        )
        conn.commit()

    def retrieve(self, file_path: Path) -> list[dict[str, object]] | None:
        """Retrieve cached scan results for a file.

        Parameters
        ----------
        file_path : Path
            Path to the file (relative to repo_root).

        Returns
        -------
        list[dict[str, object]] | None
            Cached scan records if valid cache exists, None otherwise.
        """
        if self.needs_rescan(file_path):
            return None

        file_path_str = str(file_path.relative_to(self.repo_root))
        conn = self._get_connection()
        cursor = conn.execute(
            """
            SELECT records_json
            FROM scan_cache
            WHERE file_path = ?
            """,
            (file_path_str,),
        )
        row = cursor.fetchone()

        if row is None:
            return None

        return json.loads(row["records_json"])

    def get_stats(self) -> CacheStats:
        """Get statistics about the cache.

        Returns
        -------
        CacheStats
            Cache statistics including file count, record count, and size.
        """
        conn = self._get_connection()

        # Get total files
        cursor = conn.execute("SELECT COUNT(*) as count FROM scan_cache")
        total_files = cursor.fetchone()["count"]

        # Get total records by parsing JSON
        cursor = conn.execute("SELECT records_json FROM scan_cache")
        total_records = 0
        for row in cursor:
            records = json.loads(row["records_json"])
            total_records += len(records)

        # Get database size
        db_size = self.db_path.stat().st_size if self.db_path.exists() else 0

        return CacheStats(
            total_files=total_files,
            total_records=total_records,
            rule_version=self.rule_version,
            database_size_bytes=db_size,
        )

    def clear(self) -> None:
        """Clear all cached data.

        Removes all entries from the cache. Useful for forcing a full rescan.
        """
        conn = self._get_connection()
        conn.execute("DELETE FROM scan_cache")
        conn.commit()

    def close(self) -> None:
        """Close the database connection.

        Should be called when done with the cache to ensure proper cleanup.
        """
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> IndexCache:
        """Context manager entry.

        Returns
        -------
        IndexCache
            Self for context manager protocol.
        """
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit.

        Ensures database connection is closed.
        """
        self.close()


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA-256 hash of file contents.

    Parameters
    ----------
    file_path : Path
        Path to the file to hash.

    Returns
    -------
    str
        Hexadecimal SHA-256 hash of the file contents.
    """
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()
