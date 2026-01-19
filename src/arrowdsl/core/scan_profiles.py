"""Arrow scan profile helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

type ExecutionProfileName = Literal[
    "bulk",
    "default",
    "deterministic",
    "streaming",
    "dev_debug",
    "memory_tight",
    "prod_fast",
]

if TYPE_CHECKING:
    import pyarrow.dataset as ds


@dataclass(frozen=True)
class ScanProfile:
    """Dataset scan policy for Arrow scanners and Acero scans."""

    name: str
    batch_size: int | None = None
    batch_readahead: int | None = None
    fragment_readahead: int | None = None
    fragment_scan_options: object | None = None
    cache_metadata: bool = True
    use_threads: bool = True
    parquet_read_options: ds.ParquetReadOptions | None = None
    parquet_fragment_scan_options: ds.ParquetFragmentScanOptions | None = None

    require_sequenced_output: bool = False
    implicit_ordering: bool = False
    scan_provenance_columns: tuple[str, ...] = ()

    def scanner_kwargs(self) -> dict[str, object]:
        """Return kwargs for ``ds.Scanner.from_dataset``.

        Returns
        -------
        dict[str, object]
            Scanner keyword arguments without columns or filters.
        """
        kw: dict[str, object] = {"use_threads": self.use_threads}
        if self.batch_size is not None:
            kw["batch_size"] = self.batch_size
        if self.batch_readahead is not None:
            kw["batch_readahead"] = self.batch_readahead
        if self.fragment_readahead is not None:
            kw["fragment_readahead"] = self.fragment_readahead
        fragment_scan_options = self.parquet_fragment_scan_options or self.fragment_scan_options
        if fragment_scan_options is not None:
            kw["fragment_scan_options"] = fragment_scan_options
        if self.cache_metadata:
            kw["cache_metadata"] = True
        return kw

    def scan_node_kwargs(self) -> dict[str, object]:
        """Return kwargs for ``acero.ScanNodeOptions``.

        Returns
        -------
        dict[str, object]
            Scan node keyword arguments without dataset or filters.
        """
        kw: dict[str, object] = {}
        if self.require_sequenced_output:
            kw["require_sequenced_output"] = True
        if self.implicit_ordering:
            kw["implicit_ordering"] = True
        return kw

    def parquet_read_payload(self) -> dict[str, object] | None:
        """Return JSON-ready payload for Parquet read options.

        Returns
        -------
        dict[str, object] | None
            Payload for Parquet read options when configured.
        """
        options = self.parquet_read_options
        if options is None:
            return None
        return {
            "dictionary_columns": sorted(options.dictionary_columns),
            "coerce_int96_timestamp_unit": str(options.coerce_int96_timestamp_unit),
            "binary_type": str(options.binary_type),
            "list_type": str(options.list_type),
        }

    def parquet_fragment_scan_payload(self) -> dict[str, object] | None:
        """Return JSON-ready payload for Parquet fragment scan options.

        Returns
        -------
        dict[str, object] | None
            Payload for Parquet fragment scan options when configured.
        """
        options = self.parquet_fragment_scan_options
        if options is None:
            return None
        return {
            "buffer_size": options.buffer_size,
            "pre_buffer": options.pre_buffer,
            "use_buffered_stream": options.use_buffered_stream,
            "page_checksum_verification": options.page_checksum_verification,
            "thrift_string_size_limit": options.thrift_string_size_limit,
            "thrift_container_size_limit": options.thrift_container_size_limit,
            "arrow_extensions_enabled": options.arrow_extensions_enabled,
        }


def _normalize_profile(profile: str) -> ExecutionProfileName:
    """Normalize an execution profile name to the canonical token.

    Parameters
    ----------
    profile
        User-provided profile name.

    Returns
    -------
    ExecutionProfileName
        Canonical profile token.

    Raises
    ------
    ValueError
        Raised when the profile name is unknown.
    """
    key = profile.strip().lower()
    mapping: dict[str, ExecutionProfileName] = {
        "default": "default",
        "streaming": "streaming",
        "bulk": "bulk",
        "deterministic": "deterministic",
        "dev_debug": "dev_debug",
        "prod_fast": "prod_fast",
        "memory_tight": "memory_tight",
    }
    resolved = mapping.get(key)
    if resolved is None:
        msg = f"Unknown execution profile: {profile!r}."
        raise ValueError(msg)
    return resolved


def normalize_profile_name(profile: str) -> ExecutionProfileName:
    """Return the canonical profile name token.

    Returns
    -------
    ExecutionProfileName
        Canonical profile token.
    """
    return _normalize_profile(profile)


def scan_profile_factory(profile: str) -> ScanProfile:
    """Return a ScanProfile for the named execution profile.

    Returns
    -------
    ScanProfile
        Scan profile matching the named profile.
    """
    profile_key = _normalize_profile(profile)
    profiles: dict[ExecutionProfileName, ScanProfile] = {
        "streaming": ScanProfile(
            name="STREAM",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=True,
        ),
        "bulk": ScanProfile(
            name="BULK",
            batch_size=16384,
            batch_readahead=4,
            fragment_readahead=2,
            use_threads=True,
        ),
        "deterministic": ScanProfile(
            name="DETERMINISTIC",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=False,
            require_sequenced_output=True,
            implicit_ordering=True,
        ),
        "dev_debug": ScanProfile(
            name="DEV_DEBUG",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=False,
            require_sequenced_output=True,
            implicit_ordering=True,
        ),
        "prod_fast": ScanProfile(
            name="PROD_FAST",
            batch_size=16384,
            batch_readahead=4,
            fragment_readahead=2,
            use_threads=True,
        ),
        "memory_tight": ScanProfile(
            name="MEMORY_TIGHT",
            batch_size=4096,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=True,
        ),
        "default": ScanProfile(name="DEFAULT"),
    }
    return profiles[profile_key]


__all__ = [
    "ExecutionProfileName",
    "ScanProfile",
    "normalize_profile_name",
    "scan_profile_factory",
]
