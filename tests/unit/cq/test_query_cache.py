"""Tests for query cache.

Verifies:
1. Cache get/set operations
2. File hash invalidation
3. Statistics tracking
4. Cache key generation
"""

from __future__ import annotations

import time
from collections.abc import Generator
from pathlib import Path

import pytest
from tools.cq.index.query_cache import (
    CacheStats,
    QueryCache,
    _compute_file_hash,
    make_cache_key,
)


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    """Create a temporary cache directory.

    Returns
    -------
    Path
        Cache directory path.
    """
    return tmp_path / "cache"


@pytest.fixture
def test_file(tmp_path: Path) -> Path:
    """Create a test file for caching.

    Returns
    -------
    Path
        Path to the test file.
    """
    file_path = tmp_path / "test.py"
    file_path.write_text("def foo(): pass", encoding="utf-8")
    return file_path


@pytest.fixture
def cache(cache_dir: Path) -> Generator[QueryCache]:
    """Create a query cache instance.

    Yields
    ------
    QueryCache
        Cache instance for tests.
    """
    c = QueryCache(cache_dir)
    yield c
    c.close()


class TestQueryCache:
    """Tests for QueryCache class."""

    def test_creates_cache_dir(self, tmp_path: Path) -> None:
        """Cache directory is created if it doesn't exist."""
        cache_dir = tmp_path / "new" / "cache" / "dir"
        assert not cache_dir.exists()

        QueryCache(cache_dir)
        assert cache_dir.exists()

    def test_db_path(self, cache: QueryCache, cache_dir: Path) -> None:
        """Database path is in cache directory."""
        assert cache.db_path == cache_dir / "query_cache.db"

    def test_set_and_get(self, cache: QueryCache, test_file: Path) -> None:
        """Store and retrieve cached value."""
        cache.set("test_key", {"data": "value"}, test_file)

        result = cache.get("test_key", test_file)
        assert result == {"data": "value"}

    def test_get_missing_key(self, cache: QueryCache, test_file: Path) -> None:
        """Get returns None for missing key."""
        result = cache.get("nonexistent", test_file)
        assert result is None

    def test_get_invalidates_on_file_change(self, cache: QueryCache, test_file: Path) -> None:
        """Cache is invalidated when file content changes."""
        cache.set("test_key", {"data": "original"}, test_file)

        # Verify cache hit
        result = cache.get("test_key", test_file)
        assert result == {"data": "original"}

        # Modify file content
        test_file.write_text("def bar(): pass", encoding="utf-8")

        # Cache should be invalidated
        result = cache.get("test_key", test_file)
        assert result is None

    def test_set_overwrites_existing(self, cache: QueryCache, test_file: Path) -> None:
        """Set overwrites existing cache entry."""
        cache.set("test_key", {"version": 1}, test_file)
        cache.set("test_key", {"version": 2}, test_file)

        result = cache.get("test_key", test_file)
        assert result == {"version": 2}

    def test_multiple_keys_same_file(self, cache: QueryCache, test_file: Path) -> None:
        """Multiple cache entries for same file."""
        cache.set("key1", {"id": 1}, test_file)
        cache.set("key2", {"id": 2}, test_file)

        assert cache.get("key1", test_file) == {"id": 1}
        assert cache.get("key2", test_file) == {"id": 2}


class TestCacheInvalidation:
    """Tests for cache invalidation."""

    def test_invalidate_file(self, cache: QueryCache, test_file: Path) -> None:
        """Invalidate all entries for a file."""
        cache.set("key1", {"data": 1}, test_file)
        cache.set("key2", {"data": 2}, test_file)

        count = cache.invalidate_file(test_file)
        assert count == 2

        assert cache.get("key1", test_file) is None
        assert cache.get("key2", test_file) is None

    def test_invalidate_file_preserves_others(self, cache: QueryCache, tmp_path: Path) -> None:
        """Invalidating one file preserves other files."""
        file1 = tmp_path / "file1.py"
        file2 = tmp_path / "file2.py"
        file1.write_text("# file 1", encoding="utf-8")
        file2.write_text("# file 2", encoding="utf-8")

        cache.set("key1", {"file": 1}, file1)
        cache.set("key2", {"file": 2}, file2)

        cache.invalidate_file(file1)

        assert cache.get("key1", file1) is None
        assert cache.get("key2", file2) == {"file": 2}

    def test_clear_removes_all(self, cache: QueryCache, tmp_path: Path) -> None:
        """Clear removes all cache entries."""
        file1 = tmp_path / "file1.py"
        file2 = tmp_path / "file2.py"
        file1.write_text("# file 1", encoding="utf-8")
        file2.write_text("# file 2", encoding="utf-8")

        cache.set("key1", {"data": 1}, file1)
        cache.set("key2", {"data": 2}, file2)

        cache.clear()

        assert cache.get("key1", file1) is None
        assert cache.get("key2", file2) is None


class TestCacheStats:
    """Tests for cache statistics."""

    def test_empty_cache_stats(self, cache: QueryCache) -> None:
        """Empty cache returns zero stats."""
        stats = cache.stats()

        assert stats.total_entries == 0
        assert stats.unique_files == 0
        assert stats.oldest_entry is None
        assert stats.newest_entry is None

    def test_stats_count_entries(self, cache: QueryCache, tmp_path: Path) -> None:
        """Stats counts entries correctly."""
        file1 = tmp_path / "file1.py"
        file2 = tmp_path / "file2.py"
        file1.write_text("# 1", encoding="utf-8")
        file2.write_text("# 2", encoding="utf-8")

        cache.set("key1", {}, file1)
        cache.set("key2", {}, file1)
        cache.set("key3", {}, file2)

        stats = cache.stats()

        assert stats.total_entries == 3
        assert stats.unique_files == 2

    def test_stats_timestamps(self, cache: QueryCache, test_file: Path) -> None:
        """Stats tracks timestamp range."""
        before = time.time()
        cache.set("key1", {}, test_file)
        time.sleep(0.01)  # Small delay
        cache.set("key2", {}, test_file)
        after = time.time()

        stats = cache.stats()

        assert stats.oldest_entry is not None
        assert stats.newest_entry is not None
        assert before <= stats.oldest_entry <= stats.newest_entry <= after

    def test_stats_database_size(self, cache: QueryCache, test_file: Path) -> None:
        """Stats tracks database size."""
        # Add some data
        cache.set("key", {"large": "x" * 1000}, test_file)

        stats = cache.stats()
        assert stats.database_size_bytes > 0


class TestContextManager:
    """Tests for context manager protocol."""

    def test_context_manager_usage(self, cache_dir: Path, test_file: Path) -> None:
        """Cache works as context manager."""
        with QueryCache(cache_dir) as cache:
            cache.set("key", {"data": 1}, test_file)
            result = cache.get("key", test_file)
            assert result == {"data": 1}

    def test_context_manager_closes(self, cache_dir: Path) -> None:
        """Context manager closes connection on exit."""
        cache = QueryCache(cache_dir)
        cache.set("key", {"data": 1}, test_file)
        cache.__exit__(None, None, None)
        cache.set("key2", {"data": 2}, test_file)
        assert cache.get("key2", test_file) == {"data": 2}


class TestComputeFileHash:
    """Tests for file hash computation."""

    def test_hash_same_content(self, tmp_path: Path) -> None:
        """Same content produces same hash."""
        file1 = tmp_path / "file1.py"
        file2 = tmp_path / "file2.py"
        file1.write_text("identical content", encoding="utf-8")
        file2.write_text("identical content", encoding="utf-8")

        assert _compute_file_hash(file1) == _compute_file_hash(file2)

    def test_hash_different_content(self, tmp_path: Path) -> None:
        """Different content produces different hash."""
        file1 = tmp_path / "file1.py"
        file2 = tmp_path / "file2.py"
        file1.write_text("content A", encoding="utf-8")
        file2.write_text("content B", encoding="utf-8")

        assert _compute_file_hash(file1) != _compute_file_hash(file2)

    def test_hash_missing_file(self, tmp_path: Path) -> None:
        """Missing file returns 'missing' hash."""
        missing = tmp_path / "nonexistent.py"
        assert _compute_file_hash(missing) == "missing"

    def test_hash_is_truncated(self, tmp_path: Path) -> None:
        """Hash is truncated to 16 characters."""
        file = tmp_path / "test.py"
        file.write_text("content", encoding="utf-8")

        hash_val = _compute_file_hash(file)
        assert len(hash_val) == 16


class TestMakeCacheKey:
    """Tests for cache key generation."""

    def test_deterministic_key(self) -> None:
        """Same inputs produce same key."""
        key1 = make_cache_key("ast_grep", "path/to/file.py", {"rule": "test"})
        key2 = make_cache_key("ast_grep", "path/to/file.py", {"rule": "test"})

        assert key1 == key2

    def test_different_query_type(self) -> None:
        """Different query type produces different key."""
        key1 = make_cache_key("ast_grep", "file.py", {})
        key2 = make_cache_key("symtable", "file.py", {})

        assert key1 != key2

    def test_different_file(self) -> None:
        """Different file produces different key."""
        key1 = make_cache_key("ast_grep", "file1.py", {})
        key2 = make_cache_key("ast_grep", "file2.py", {})

        assert key1 != key2

    def test_different_params(self) -> None:
        """Different params produce different key."""
        key1 = make_cache_key("ast_grep", "file.py", {"rule": "A"})
        key2 = make_cache_key("ast_grep", "file.py", {"rule": "B"})

        assert key1 != key2

    def test_key_length(self) -> None:
        """Key is 32 characters."""
        key = make_cache_key("type", "file", {"param": "value"})
        assert len(key) == 32

    def test_param_order_independent(self) -> None:
        """Param order doesn't affect key."""
        key1 = make_cache_key("type", "file", {"a": 1, "b": 2})
        key2 = make_cache_key("type", "file", {"b": 2, "a": 1})

        assert key1 == key2


class TestCacheEntryDataclass:
    """Tests for CacheEntry dataclass."""

    def test_create_cache_entry(self) -> None:
        """Create CacheEntry instance."""
        from tools.cq.index.query_cache import CacheEntry

        entry = CacheEntry(
            key="test_key",
            value={"data": 123},
            file_hash="abc123",
            timestamp=1234567890.0,
        )

        assert entry.key == "test_key"
        assert entry.value == {"data": 123}
        assert entry.file_hash == "abc123"
        assert entry.timestamp == 1234567890.0


class TestCacheStatsDataclass:
    """Tests for CacheStats dataclass."""

    def test_create_cache_stats(self) -> None:
        """Create CacheStats instance."""
        stats = CacheStats(
            total_entries=100,
            unique_files=50,
            database_size_bytes=1024,
            oldest_entry=1000.0,
            newest_entry=2000.0,
        )

        assert stats.total_entries == 100
        assert stats.unique_files == 50
        assert stats.database_size_bytes == 1024
        assert stats.oldest_entry == 1000.0
        assert stats.newest_entry == 2000.0

    def test_nullable_timestamps(self) -> None:
        """Timestamps can be None."""
        stats = CacheStats(
            total_entries=0,
            unique_files=0,
            database_size_bytes=0,
            oldest_entry=None,
            newest_entry=None,
        )

        assert stats.oldest_entry is None
        assert stats.newest_entry is None
