"""Incremental query cache for cq.

Caches expensive computations with file-hash-based invalidation.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass
class CacheEntry:
    """A cached computation result.

    Attributes
    ----------
    key
        Cache key (query + file hash)
    value
        Cached result (JSON-serializable)
    file_hash
        Hash of source file when cached
    timestamp
        Cache time (Unix timestamp)
    """

    key: str
    value: Any
    file_hash: str
    timestamp: float


@dataclass
class CacheStats:
    """Statistics about the query cache.

    Attributes
    ----------
    total_entries
        Total number of cached entries.
    unique_files
        Number of unique files with cached data.
    database_size_bytes
        Size of the cache database file in bytes.
    oldest_entry
        Timestamp of oldest cache entry.
    newest_entry
        Timestamp of newest cache entry.
    """

    total_entries: int
    unique_files: int
    database_size_bytes: int
    oldest_entry: float | None
    newest_entry: float | None


class QueryCache:
    """SQLite-backed query cache with file hash invalidation.

    Caches expensive computations (ast-grep scans, symtable extraction) with
    automatic invalidation when source files change.

    Parameters
    ----------
    cache_dir
        Directory to store cache database.

    Attributes
    ----------
    db_path
        Full path to the SQLite database file.
    """

    def __init__(self, cache_dir: Path) -> None:
        """Initialize the query cache.

        Parameters
        ----------
        cache_dir
            Directory to store cache database.
        """
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = cache_dir / "query_cache.db"
        self._conn: sqlite3.Connection | None = None

    @property
    def db_path(self) -> Path:
        """Return the path to the cache database."""
        return self._db_path

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection.

        Returns
        -------
        sqlite3.Connection
            Active database connection.
        """
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._db_path))
            self._conn.row_factory = sqlite3.Row
            self._init_db()
        return self._conn

    def _init_db(self) -> None:
        """Initialize cache database schema."""
        conn = self._conn
        if conn is None:
            return

        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                file_path TEXT NOT NULL,
                timestamp REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_hash ON cache(file_hash)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_path ON cache(file_path)
        """)
        conn.commit()

    def get(self, key: str, file_path: Path) -> Any | None:
        """Get cached value if still valid.

        Validates cache by comparing stored file hash against current file hash.
        Automatically invalidates stale entries.

        Parameters
        ----------
        key
            Cache key
        file_path
            Source file to check hash against

        Returns
        -------
        Any | None
            Cached value if valid, None if miss or stale
        """
        current_hash = _compute_file_hash(file_path)

        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT value, file_hash FROM cache WHERE key = ?",
            (key,),
        )
        row = cursor.fetchone()

        if row is None:
            return None

        cached_value = row["value"]
        cached_hash = row["file_hash"]

        if cached_hash != current_hash:
            # File changed, invalidate this entry
            conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            conn.commit()
            return None

        return json.loads(cached_value)

    def set(self, key: str, value: Any, file_path: Path) -> None:
        """Store value in cache.

        Parameters
        ----------
        key
            Cache key
        value
            Value to cache (must be JSON-serializable)
        file_path
            Source file to compute hash from
        """
        file_hash = _compute_file_hash(file_path)
        value_json = json.dumps(value)
        file_path_str = str(file_path)

        conn = self._get_connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO cache (key, value, file_hash, file_path, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (key, value_json, file_hash, file_path_str, time.time()),
        )
        conn.commit()

    def invalidate_file(self, file_path: Path) -> int:
        """Invalidate all cache entries for a file.

        Parameters
        ----------
        file_path
            Path to file whose entries should be invalidated

        Returns
        -------
        int
            Number of entries invalidated
        """
        file_path_str = str(file_path)
        conn = self._get_connection()
        cursor = conn.execute(
            "DELETE FROM cache WHERE file_path = ?",
            (file_path_str,),
        )
        conn.commit()
        return cursor.rowcount

    def invalidate_by_hash(self, file_hash: str) -> int:
        """Invalidate all cache entries with a specific file hash.

        Parameters
        ----------
        file_hash
            Hash of file contents to invalidate

        Returns
        -------
        int
            Number of entries invalidated
        """
        conn = self._get_connection()
        cursor = conn.execute(
            "DELETE FROM cache WHERE file_hash = ?",
            (file_hash,),
        )
        conn.commit()
        return cursor.rowcount

    def clear(self) -> None:
        """Clear entire cache."""
        conn = self._get_connection()
        conn.execute("DELETE FROM cache")
        conn.commit()

    def stats(self) -> CacheStats:
        """Get cache statistics.

        Returns
        -------
        CacheStats
            Cache statistics including entry count, file count, and size.
        """
        conn = self._get_connection()

        # Total entries
        cursor = conn.execute("SELECT COUNT(*) as count FROM cache")
        total_entries = cursor.fetchone()["count"]

        # Unique files
        cursor = conn.execute(
            "SELECT COUNT(DISTINCT file_path) as count FROM cache"
        )
        unique_files = cursor.fetchone()["count"]

        # Timestamps
        cursor = conn.execute(
            "SELECT MIN(timestamp) as oldest, MAX(timestamp) as newest FROM cache"
        )
        row = cursor.fetchone()
        oldest_entry = row["oldest"]
        newest_entry = row["newest"]

        # Database size
        db_size = self._db_path.stat().st_size if self._db_path.exists() else 0

        return CacheStats(
            total_entries=total_entries,
            unique_files=unique_files,
            database_size_bytes=db_size,
            oldest_entry=oldest_entry,
            newest_entry=newest_entry,
        )

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> QueryCache:
        """Context manager entry.

        Returns
        -------
        QueryCache
            Self for context manager protocol.
        """
        self._get_connection()  # Initialize connection
        return self

    def __exit__(
        self, exc_type: object, exc_val: object, exc_tb: object  # noqa: ARG002
    ) -> None:
        """Context manager exit.

        Ensures database connection is closed.
        """
        self.close()


def _compute_file_hash(file_path: Path) -> str:
    """Compute hash of file contents.

    Parameters
    ----------
    file_path
        Path to file to hash

    Returns
    -------
    str
        Truncated SHA-256 hash (first 16 characters)
    """
    if not file_path.exists():
        return "missing"

    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()[:16]


def make_cache_key(query_type: str, file: str, params: Mapping[str, object]) -> str:
    """Generate cache key from query parameters.

    Parameters
    ----------
    query_type
        Type of query (e.g., "ast_grep", "symtable")
    file
        File path relative to repo root
    params
        Query parameters (must be JSON-serializable)

    Returns
    -------
    str
        32-character cache key hash
    """
    params_str = json.dumps(dict(params), sort_keys=True)
    key_input = f"{query_type}:{file}:{params_str}"
    return hashlib.sha256(key_input.encode()).hexdigest()[:32]
