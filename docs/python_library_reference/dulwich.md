Below is a **clustered feature catalog** of **Dulwich** (a pure-Python Git implementation) in the same “surface-area first” spirit as your PyArrow reference. 

---

## 0) Project shape, boundaries, and “what Dulwich is for”

* **Pure-Python Git implementation** for working with **local and remote repos** without shelling out to `git`. ([PyPI][1])
* Two user-facing strata:

  * **Low-level plumbing** (objects, packs, refs, protocol, repo/worktree/index).
  * **High-level “porcelain”** API that mirrors Git CLI subcommands as Python functions. ([Dulwich][2])
* Optional **Rust extensions** to accelerate hot low-level operations; you can force “pure python” installation mode. ([Dulwich][2])
* Tested Python versions: **CPython 3.10+ and PyPy** (per docs). ([Dulwich][2])
* License: **Apache-2.0 OR GPL-2.0-or-later** (dual-licensed). ([PyPI][1])

---

## A) High-level porcelain (Git-like commands as Python functions)

Dulwich’s porcelain layer is explicitly intended to behave like `git <subcommand>`; differences are treated as bugs, and path resolution tends to be relative to the current working directory (CLI-like). ([Dulwich][3])

### A1) Repo creation, cloning, remotes, sync

* `init`, `clone`
* `remote_add`
* `fetch`, `pull`, `push`
* Low-level endpoints exposed as porcelain calls: `upload_pack`, `receive_pack`, plus `daemon`, `update_server_info`. ([Dulwich][3])

### A2) Working tree + staging + commit lifecycle

* `add`, `rm`, `mv/move`
* `checkout`, `checkout_branch`, `reset`
* `commit`, `commit_tree`
* Change-management: `revert`, `cherry_pick` ([Dulwich][3])

### A3) Branches, tags, refs, history inspection

* `branch_create / branch_delete / branch_list`
* `tag_create / tag_delete / tag_list`
* `symbolic_ref`, `for_each_ref`
* `rev_list`, `describe`, `ls_remote`, `ls_files`, `ls_tree`, `status` ([Dulwich][3])

### A4) Merge/rebase/bisect and related workflows

* `merge`, `merge_tree`
* `bisect_start / bisect_good / bisect_bad / bisect_skip / bisect_reset / bisect_log / bisect_replay`
* `filter_branch` (as porcelain command wrapper)
* `prune`
* Sparse + submodules + worktrees:

  * `sparse_checkout`, `cone_mode_init / _set / _add`
  * `submodule_add / submodule_init / submodule_list`
  * `worktree_add / _list / _remove / _prune / _lock / _unlock / _move` ([Dulwich][3])

### A5) “Porcelain-specific” types + error surfaces

* Exceptions like `CheckoutError`, `DivergedBranches`, `RemoteExists`, plus porcelain’s base `Error`. ([Dulwich][3])

---

## B) Repository & identity primitives (local disk + in-memory repos)

### B1) Repository representations

* `Repo`: local-disk backed repository.
* `BaseRepo`: base class contract.
* `MemoryRepo`: in-memory repo that stores refs/objects/files in memory (useful for tests and in-memory transforms). ([Dulwich][4])

### B2) Identity discovery / validation (commit author/committer)

* `check_user_identity(identity)` validates `b"user <email>"` formatting.
* `get_user_identity(config, kind=None)` derives identity from env (`GIT_${KIND}_NAME/EMAIL`), then config (`user.name/user.email`), then OS defaults. ([Dulwich][4])

---

## C) Core Git object model (parse/serialize and type checks)

### C1) Object types

* `Blob`, `Tree`, `Commit`, `Tag` (plus helpers like `TreeEntry`). ([Dulwich][5])

### C2) Parsing/serialization + invariants

* Parse helpers: `parse_commit`, `parse_tree`, time/identity validation helpers (`check_hexsha`, `check_identity`, etc.).
* Serialization helpers: `serialize_tree`, `object_header`, plus “is_*” predicates (`is_blob/is_commit/is_tree/is_tag`). ([Dulwich][5])

---

## D) Object stores & storage backends (loose objects, packs, overlays, buckets)

### D1) Object store interfaces + implementations

* `BaseObjectStore` (interface), `DiskObjectStore`, `MemoryObjectStore`
* `PackBasedObjectStore` (object store backed by pack(s))
* `OverlayObjectStore` (compose object stores)
* `BucketBasedObjectStore` for bucket backends (S3-like concept), plus pack container abstractions. ([Dulwich][6])

### D2) Tree + commit reachability helpers at the store layer

* Functions like `iter_tree_contents`, `tree_lookup_path`, `peel_sha`, depth/shallow helpers (`get_depth`, `find_shallow`). ([Dulwich][6])

### D3) Cloud/back-end storage extensions

* **GCS**: `dulwich.cloud.gcs.GcsObjectStore` (“Storage of repositories on GCS”). ([Dulwich][7])
* **OpenStack Swift** contrib: `SwiftObjectStore`, `SwiftRepo`, `SwiftPack*`, etc. ([Dulwich][8])

---

## E) Packfiles, pack indexes, and delta mechanics

This is Dulwich’s “binary performance” core: reading/writing packfiles and indexes; deltification; streaming pack IO. ([Dulwich][9])

### E1) Pack & index structures

* Pack/index types and versions: `Pack`, `PackData`, `PackIndex`, and `PackIndex1/2/3`, plus file/memory index variants (`FilePackIndex`, `MemoryPackIndex`). ([Dulwich][9])

### E2) Pack streaming + verification + chunk IO

* Stream helpers: `PackStreamReader`, `PackStreamCopier`, `PackChunkGenerator`
* SHA wrappers: `SHA1Reader`, `SHA1Writer` ([Dulwich][9])

### E3) Delta creation/application & pack building

* Functions like `create_delta`, `apply_delta`, `deltify_pack_objects`, reusable delta detection, pack writing (`write_pack*`) and index writing (`write_pack_index_v1/v2/v3`). ([Dulwich][9])

---

## F) Refs, reflogs, notes, stash, and “ref storage formats”

### F1) Ref containers + disk formats

* Refs containers: `RefsContainer` + concrete `DiskRefsContainer`, `DictRefsContainer`, `InfoRefsContainer`, plus `locked_ref`. ([Dulwich][10])
* Parsers/emitters:

  * `read_info_refs`, `write_info_refs`
  * `read_packed_refs`, `write_packed_refs`, plus peeled-ref helpers (`split_peeled_refs`, `strip_peeled_refs`). ([Dulwich][10])

### F2) Reftable support (modern ref storage format)

* Reader/writer and record types: `ReftableReader`, `ReftableWriter`, `ReftableRefsContainer`, `RefRecord`, `RefValue`, etc. ([Dulwich][11])

### F3) Reflog utilities

* `read_reflog`, `iter_reflogs`, `parse_reflog_line`, `format_reflog_line`, `drop_reflog_entry`. ([Dulwich][12])

### F4) Notes & stash

* Notes: `Notes`, `NotesTree`, plus note path/fanout helpers. ([Dulwich][13])
* Stash: `Stash` + stash ref constant (`DEFAULT_STASH_REF`). ([Dulwich][14])

---

## G) History traversal + graph algorithms + commit acceleration

### G1) Walking commits and ordering

* `Walker`, `WalkEntry`, plus ordering constants (`ORDER_DATE`, `ORDER_TOPO`). ([Dulwich][15])

### G2) Graph algorithms (merge-base & fast-forward checks)

* `find_merge_base`, `find_octopus_base`, `can_fast_forward`. ([Dulwich][16])

### G3) Bisect + annotate/blame

* Bisect state (`BisectState`). ([Dulwich][17])
* Annotate/blame-like helpers: `annotate_lines`, `update_lines`. ([Dulwich][18])

### G4) Commit graph file format support (speeding ancestry queries)

* Commit graph support: `CommitGraph`, `CommitGraphEntry`, `read_commit_graph`, `write_commit_graph`, plus generators/reachability helpers. ([Dulwich][19])

---

## H) Working tree + index + diffs + patches + archives

### H1) Index file format + staging/checkout plumbing

The `dulwich.index` module is the “git index” parser/writer surface:

* Types: `Index`, `IndexEntry`, `locked_index`, plus extension types (e.g., `TreeExtension`, `ResolveUndoExtension`, `UntrackedExtension`) and merge-stage types (`Stage`, `UnmergedEntries`). ([Dulwich][20])
* Operations:

  * Read/write: `read_index`, `write_index`, plus dict-based helpers
  * Tree/index synthesis and working tree updates: `build_index_from_tree`, `commit_index`, `commit_tree`, `update_working_tree`
  * File/blob helpers: `blob_from_path_and_mode/stat`, `build_file_from_blob` ([Dulwich][20])

### H2) Worktrees (multiple checkouts)

* `WorkTree`, `WorkTreeContainer`, `WorkTreeInfo`
* Operations: `add_worktree`, `list_worktrees`, `move_worktree`, `lock_worktree`, `unlock_worktree`, `remove_worktree`, `prune_worktrees`, `temporary_worktree`. ([Dulwich][21])

### H3) Diffs & tree-diffs

* High-level diff entrypoints: `diff_working_tree_to_index`, `diff_working_tree_to_tree`, `diff_index_to_tree` and a `ColorizedDiffStream`. ([Dulwich][22])
* Tree change machinery: `tree_changes`, `tree_changes_for_merge`, `walk_trees`, plus rename detection (`RenameDetector`) and `TreeChange`. ([Dulwich][23])

### H4) Patches (“git am” style) and diff rendering

* Patch utilities: `unified_diff`, `git_am_patch_split`, `parse_patch_message`, `write_commit_patch`, `write_tree_diff`, `write_blob_diff`, `write_object_diff`, etc. ([Dulwich][24])

### H5) Archives

* Tarball generation: `archive.tar_stream` (+ chunked IO helper). ([Dulwich][25])

---

## I) Merge, rebase, sparse checkout, submodules

* Merge: `Merger`, `MergeConflict`, `three_way_merge`, `merge_blobs`. ([Dulwich][26])
* Rebase: `Rebaser`, state managers (`DiskRebaseStateManager`, `MemoryRebaseStateManager`), and rebase error types. ([Dulwich][27])
* Sparse checkout pattern engine: functions like `parse_sparse_patterns`, `determine_included_paths`, and cone/full path computations; error types for conflicts. ([Dulwich][28])
* Submodules (low-level): `ensure_submodule_placeholder`, `iter_cached_submodules`. ([Dulwich][29])

---

## J) Transports, clients, servers, and protocol support

### J1) Core protocol concepts

Dulwich documents the smart protocol concepts (pkt-line framing, sideband multiplexing) and the two key services:

* `git-upload-pack` (clone/fetch/pull) and
* `git-receive-pack` (push), including capability negotiation. ([Dulwich][30])

### J2) Client-side transport surface

The `dulwich.client` module provides a set of client implementations and transport utilities:

* Client classes include: `GitClient`, `LocalGitClient`, `SSHGitClient`, `TCPGitClient`, `Urllib3HttpGitClient`, `SubprocessGitClient`, `BundleClient`, etc.
* Protocol helpers: `get_transport_and_path` / `get_transport_and_path_from_url`, `negotiate_protocol_version`, pkt-refs readers (`read_pkt_refs_v1/v2`), capability readers, and credential helpers. ([Dulwich][31])

### J3) Server-side smart protocol

The `dulwich.server` module implements a smart protocol server:

* Handlers: `UploadPackHandler`, `ReceivePackHandler`, `UploadArchiveHandler`, plus TCP server/request handler types.
* Exposes `serve_command(...)` (useful for ssh-style “run a single git service”).
* Lists supported capabilities such as `thin-pack`, `multi_ack(_detailed)`, `side-band-64k`, `ofs-delta`, `report-status`, `delete-refs`, `shallow`, `symref`, etc. ([Dulwich][32])

### J4) HTTP serving modes

* **Smart HTTP** WSGI server/app: `HTTPGitApplication` and helpers for serving pack/idx/info refs/loose objects, etc. ([Dulwich][33])
* **Dumb HTTP(S)** support: `DumbHTTPObjectStore` + `DumbRemoteHTTPRepo` (classic static-file “dumb” transport). ([Dulwich][34])

### J5) Credentials + “vendor” swap-ins

* Credentials URL matching helpers: `match_urls`, `urlmatch_credential_sections`, etc. ([Dulwich][35])
* Pluggable SSH/HTTP implementations:

  * **Paramiko SSH vendor**: `ParamikoSSHVendor` and documented override hook (`dulwich.client.get_ssh_vendor = ParamikoSSHVendor`). ([Dulwich][36])
  * **Requests HTTP client**: `RequestsHttpGitClient` and override hook (`dulwich.client.HttpGitClient = RequestsHttpGitClient`) (noted as experimental). ([Dulwich][37])

---

## K) Git “policy” files and transformation filters

* `.gitignore` parsing and Git-accurate matching:

  * `IgnoreFilter`, `IgnoreFilterManager`, `IgnoreFilterStack`, `Pattern`, `translate(...)` (pattern → regex), and `read_ignore_patterns`. ([Dulwich][38])
* `.gitattributes` parsing:

  * `GitAttributes`, `parse_git_attributes`, `read_gitattributes`, etc. ([Dulwich][39])
* Filter drivers (clean/smudge framework):

  * `FilterDriver`, `ProcessFilterDriver`, `FilterRegistry`, `get_filter_for_path`, etc. ([Dulwich][40])
* Line-ending normalization helpers:

  * `convert_lf_to_crlf`, `convert_crlf_to_lf`, checkin/checkout filters, `normalize_blob`, and normalizer classes. ([Dulwich][41])

---

## L) Maintenance, rewriting, and import/export tooling

* Garbage collection:

  * `garbage_collect`, reachability/unreachability finders, prune helpers, `maybe_auto_gc`, plus stats type `GCStats`. ([Dulwich][42])
* History rewriting:

  * `filter_branch` module (`CommitFilter`, `filter_refs`). ([Dulwich][43])
* Bundle format:

  * `Bundle` + `read_bundle` / `write_bundle` / `create_bundle_from_repo`. ([Dulwich][44])
* Fast export/import:

  * `GitFastExporter`, `GitImportProcessor`. ([Dulwich][45])

---

## M) Developer ergonomics: CLI + logging

* Built-in CLI is “for testing Dulwich,” not a full Git CLI replacement. ([Dulwich][46])
* Logging helpers: `default_logging_config`, `remove_null_handler` and related utilities. ([Dulwich][47])
* Contrib diffstat helper: `dulwich.contrib.diffstat.diffstat(...)` parses patches to produce diffstat-style output. ([Dulwich][48])

---

## N) “Full module map” (for completeness / discoverability)

Dulwich’s published module index (API docs) enumerates the major functional areas (objects, pack, protocol, porcelain, repo, worktree, index, refs/reftable/reflog/notes, diff/diff_tree/patch/archive, client/server/web/dumb, merge/rebase/bisect/stash/submodule/sparse_patterns, gc/filter_branch/filters/ignore/attrs, LFS, cloud/contrib, etc.). ([Dulwich][49])

---

If you want the next increment like your PyArrow doc: I can expand each major cluster above into “deep dive pages” (power knobs + failure modes + minimal snippets), especially for **(a) pack/index performance & corruption handling**, **(b) smart HTTP/SSH client/server wiring and capability negotiation**, and **(c) worktree/index correctness (staging ↔ tree ↔ filesystem)**.

[1]: https://pypi.org/project/dulwich/?utm_source=chatgpt.com "dulwich"
[2]: https://dulwich.readthedocs.io/ "dulwich - Python implementation of Git — dulwich 0.25.0 documentation"
[3]: https://dulwich.io/api/dulwich.porcelain.html "dulwich.porcelain"
[4]: https://dulwich.io/api/dulwich.repo.html "dulwich.repo"
[5]: https://dulwich.io/api/dulwich.objects.html "dulwich.objects"
[6]: https://dulwich.io/api/dulwich.object_store.html "dulwich.object_store"
[7]: https://dulwich.io/api/dulwich.cloud.gcs.html "dulwich.cloud.gcs"
[8]: https://dulwich.io/api/dulwich.contrib.swift.html "dulwich.contrib.swift"
[9]: https://dulwich.io/api/dulwich.pack.html "dulwich.pack"
[10]: https://dulwich.io/api/dulwich.refs.html "dulwich.refs"
[11]: https://dulwich.io/api/dulwich.reftable.html "dulwich.reftable"
[12]: https://dulwich.io/api/dulwich.reflog.html "dulwich.reflog"
[13]: https://dulwich.io/api/dulwich.notes.html "dulwich.notes"
[14]: https://dulwich.io/api/dulwich.stash.html "dulwich.stash"
[15]: https://dulwich.io/api/dulwich.walk.html "dulwich.walk"
[16]: https://dulwich.io/api/dulwich.graph.html "dulwich.graph"
[17]: https://dulwich.io/api/dulwich.bisect.html "dulwich.bisect"
[18]: https://dulwich.io/api/dulwich.annotate.html "dulwich.annotate"
[19]: https://dulwich.io/api/dulwich.commit_graph.html "dulwich.commit_graph"
[20]: https://dulwich.io/api/dulwich.index.html "dulwich.index"
[21]: https://dulwich.io/api/dulwich.worktree.html "dulwich.worktree"
[22]: https://dulwich.io/api/dulwich.diff.html "dulwich.diff"
[23]: https://dulwich.io/api/dulwich.diff_tree.html "dulwich.diff_tree"
[24]: https://dulwich.io/api/dulwich.patch.html "dulwich.patch"
[25]: https://dulwich.io/api/dulwich.archive.html "dulwich.archive"
[26]: https://dulwich.io/api/dulwich.merge.html "dulwich.merge"
[27]: https://dulwich.io/api/dulwich.rebase.html "dulwich.rebase"
[28]: https://dulwich.io/api/dulwich.sparse_patterns.html "dulwich.sparse_patterns"
[29]: https://dulwich.io/api/dulwich.submodule.html "dulwich.submodule"
[30]: https://dulwich.readthedocs.io/en/latest/protocol.html "Git Server Protocol — dulwich 0.25.0 documentation"
[31]: https://dulwich.io/api/dulwich.client.html "dulwich.client"
[32]: https://dulwich.io/api/dulwich.server.html "dulwich.server"
[33]: https://dulwich.io/api/dulwich.web.html "dulwich.web"
[34]: https://dulwich.io/api/dulwich.dumb.html "dulwich.dumb"
[35]: https://dulwich.io/api/dulwich.credentials.html "dulwich.credentials"
[36]: https://dulwich.io/api/dulwich.contrib.paramiko_vendor.html "dulwich.contrib.paramiko_vendor"
[37]: https://dulwich.io/api/dulwich.contrib.requests_vendor.html "dulwich.contrib.requests_vendor"
[38]: https://dulwich.io/api/dulwich.ignore.html "dulwich.ignore"
[39]: https://dulwich.io/api/dulwich.attrs.html "dulwich.attrs"
[40]: https://dulwich.io/api/dulwich.filters.html "dulwich.filters"
[41]: https://dulwich.io/api/dulwich.line_ending.html "dulwich.line_ending"
[42]: https://dulwich.io/api/dulwich.gc.html "dulwich.gc"
[43]: https://dulwich.io/api/dulwich.filter_branch.html "dulwich.filter_branch"
[44]: https://dulwich.io/api/dulwich.bundle.html "dulwich.bundle"
[45]: https://dulwich.io/api/dulwich.fastexport.html "dulwich.fastexport"
[46]: https://dulwich.io/api/dulwich.cli.html "dulwich.cli"
[47]: https://dulwich.io/api/dulwich.log_utils.html "dulwich.log_utils"
[48]: https://dulwich.io/api/dulwich.contrib.diffstat.html "dulwich.contrib.diffstat"
[49]: https://dulwich.io/api/moduleIndex.html "Module Index"

Below are the three requested **“deep dive pages”** (power knobs + failure modes + minimal snippets) for Dulwich’s highest-leverage clusters.

---

# Deep Dive A — Pack/Index performance & corruption handling

## A0) Mental model (what you’re tuning)

* A **pack** is a compact stream of Git objects (often delta-compressed). Dulwich exposes:

  * **Pack stream readers** for *wire* / streaming integrity + inflation (`PackStreamReader`, `PackStreamCopier`). ([Dulwich][1])
  * **Delta-chain iterators** for efficient resolution where **memory is proportional to the longest delta chain** (not total objects). ([Dulwich][2])
  * **Index writers** (`write_pack_index*`) to build `.idx` from `(sha, offset, crc32)` tuples. ([Dulwich][3])

If you’re building “best-in-class” ingestion, the key is **(1) streaming verification**, **(2) bounded-memory delta resolution**, and **(3) deterministic index build + validation**.

---

## A1) Power knobs (what to turn, and why)

### A1.1 Streaming read / verify knobs

* **`PackStreamReader(..., zlib_bufsize=...)`**: controls decompression buffer size for pack streams. ([Dulwich][1])
* **`PackStreamReader.read_objects(compute_crc32=...)`**

  * `compute_crc32=True` adds CPU overhead but yields CRC32 per object for stronger corruption detection signals upstream. ([Dulwich][1])
  * Failure surfaces are explicit: checksum mismatch, zlib errors, and IO errors are called out. ([Dulwich][1])
* **`PackStreamCopier(..., delta_iter=...)` + `.verify(progress=...)`**

  * Verify while copying to an output file-like object; optionally **record deltas as you read** via a `DeltaChainIterator` instance. ([Dulwich][4])

### A1.2 Delta mechanics / bounded memory

* **`DeltaChainIterator`**: “inflate each object at most once,” with **memory proportional to longest delta chain**. If you see RAM spikes, you’re likely hitting pathological delta chains (or you’re caching too aggressively above Dulwich). ([Dulwich][2])
* **`deltify_pack_objects`** exists for pack building (delta creation), and the pack module exposes `create_delta` / `apply_delta`. ([Dulwich][3])

### A1.3 Pack writing knobs (through chunk generation + index versions)

* **Pack chunk generation**

  * `PackChunkGenerator._pack_data_chunks(..., compression_level=-1, reuse_compressed=True)` is the *core* knob set:

    * `compression_level`: CPU ↔ size tradeoff.
    * `reuse_compressed`: prefer reuse of existing compressed forms when possible. ([Dulwich][5])
* **Index writer knobs**

  * `write_pack_index(index_filename, entries, pack_checksum, version=None)`:

    * `version` can be **1, 2, or 3**; if `None`, defaults to `DEFAULT_PACK_INDEX_VERSION`. ([Dulwich][3])
  * `write_pack_index_v3(..., hash_algorithm=1)` enables v3 formatting with explicit hash algorithm selection. ([Dulwich][3])

### A1.4 “Operational” knobs you should treat as policy

* Pack module exposes constants like `DEFAULT_PACK_INDEX_VERSION`, `DEFAULT_PACK_DELTA_WINDOW_SIZE`, `PACK_SPOOL_FILE_MAX_SIZE`, and `_ZLIB_BUFSIZE` (useful for “policy docs” and guardrails). ([Dulwich][6])
* `compute_file_sha(...)` exists to hash file portions (useful for validating pack bytes or building audit tooling). ([Dulwich][3])

---

## A2) Failure modes (what breaks, how it presents, how you contain it)

### A2.1 Checksum / trailer mismatch (wire or file corruption)

* `PackStreamReader.read_objects(...)` can raise `ChecksumMismatch` if the computed checksum doesn’t match the pack trailer checksum. ([Dulwich][1])
  **Containment pattern:** treat as “hard corruption,” quarantine the pack, and re-fetch / re-copy from a trusted remote.

### A2.2 zlib decompression errors (truncation, bitrot, mid-stream cut)

* `PackStreamReader.read_objects(...)` explicitly calls out `zlib.error`. ([Dulwich][1])
  **Containment pattern:** retry only if you have reason to believe it’s transient IO; otherwise treat as corrupt bytes.

### A2.3 Delta application failure

* `ApplyDeltaError` exists specifically for delta application failures. ([Dulwich][7])
  **Root causes:** missing base, invalid delta instructions, corrupted base, or buggy producer.
  **Containment pattern:** in “agent-driven ingestion,” keep the raw pack bytes and emit a normalized “delta-failure” diagnostic with the object id(s) so you can repro deterministically.

### A2.4 Unresolved deltas / missing bases

* Pack module defines `UnresolvedDeltas` (“Delta objects could not be resolved”). ([Dulwich][3])
  **Typical cause:** you accepted a thin pack / partial set without having bases available, or you’re resolving against the wrong object store.

### A2.5 Index/pack mismatch (idx points at wrong pack)

* `PackIndex.get_pack_checksum()` returns the packfile checksum stored in the index (or `None`). ([Dulwich][6])
  **Containment pattern:** compare the index’s stored pack checksum with the pack trailer hash (or computed SHA) and rebuild the index if needed.

---

## A3) Minimal snippets

### A3.1 Verify a pack stream while writing it to disk (smart-protocol ingestion primitive)

```python
from dulwich.pack import PackStreamCopier

def copy_and_verify_pack(read_all, read_some, outfile):
    copier = PackStreamCopier(read_all=read_all, read_some=read_some, outfile=outfile)
    copier.verify()  # raises ChecksumMismatch / zlib.error on corruption
```

`PackStreamCopier` is explicitly intended to “verify a pack stream as it is being read,” writing to the provided output. ([Dulwich][4])

### A3.2 Read objects from a stream with optional CRC32s (stronger corruption signals)

```python
from dulwich.pack import PackStreamReader

def iter_unpacked(read_all, read_some=None, *, compute_crc32=False):
    r = PackStreamReader(read_all=read_all, read_some=read_some)
    yield from r.read_objects(compute_crc32=compute_crc32)
```

`read_objects(compute_crc32=...)` yields `UnpackedObject`s with offset/type/chunks and optionally CRC32, and documents the key exceptions. ([Dulwich][1])

### A3.3 Write a pack index deterministically (choose idx version)

```python
from dulwich.pack import write_pack_index

def write_idx(path_idx, entries, pack_checksum, *, version=None):
    # entries: iterable of (sha_bytes, offset_int, crc32_int)
    return write_pack_index(path_idx, entries, pack_checksum, version=version)
```

`write_pack_index(..., version=None)` supports v1/v2/v3 and defaults to `DEFAULT_PACK_INDEX_VERSION` when `None`. ([Dulwich][3])

### A3.4 Bounded-memory delta resolution (design note)

When you build “pack normalizers” or verification passes, prefer delta-chain iterators because inflation is bounded and memory tracks the longest chain. ([Dulwich][2])

---

# Deep Dive B — Smart HTTP/SSH client/server wiring & capability negotiation

## B0) Mental model (what you’re wiring)

Dulwich exposes both sides of the Git “smart” protocol:

* **Client**: `GitClient` and transport-specific implementations; convenience helpers like `get_transport_and_path(...)`. ([Dulwich][8])
* **Server**: `UploadPackHandler` (fetch/clone) and `ReceivePackHandler` (push) plus helpers like `serve_command`. ([Dulwich][9])
* **HTTP server**: `HTTPGitApplication` WSGI app + request/middleware utilities. ([Dulwich][10])

Capability negotiation matters because it changes:

* whether you accept/send **thin packs**,
* whether you get **side-band** progress multiplexing,
* and whether protocol-v2 features like **filter** are available. ([Git][11])

---

## B1) Power knobs

### B1.1 Transport resolution knob (your “front door”)

* `get_transport_and_path(location, config=None, operation=None, thin_packs=..., report_activity=...) -> (GitClient, str)`:

  * `operation` is documented as “pull” or “push”
  * `thin_packs` toggles thin pack retrieval
  * `report_activity` is a callback for transport activity ([Dulwich][8])

### B1.2 Fetch-pack knobs (protocol v1/v2 and advanced filters)

`fetch_pack(..., depth=None, ref_prefix=None, filter_spec=None, protocol_version=None)` is where the important knobs live:

* `depth`: shallow fetch depth
* `ref_prefix`: server-side filtering if supported; otherwise client-side filtering
* `filter_spec`: rev-list style object filter spec, **only used if server supports protocol-v2 “filter”**, ignored otherwise
* `protocol_version`: desired Git protocol version; by default highest mutually supported is used ([Dulwich][12])

### B1.3 Push knobs

* `send_pack(path, update_refs, generate_pack_data, progress=None) -> SendPackResult`

  * may raise `SendPackError` if the server rejects the pack data ([Dulwich][12])

### B1.4 Server capability surface (what Dulwich advertises/supports)

Dulwich server docs list supported capabilities including:
`include-tag`, `thin-pack`, `multi_ack(_detailed)`, `side-band-64k`, `ofs-delta`, `no-progress`, `report-status`, `delete-refs`, `shallow`, `symref`. ([samba.org][13])

(Upstream Git capability semantics are documented by Git itself; e.g. `ofs-delta` and `side-band-64k` are recognized by both upload-pack and receive-pack, while `report-status`/`delete-refs` are receive-pack/push features.) ([Git][11])

### B1.5 Smart HTTP wiring knobs

* `HTTPGitApplication(backend, dumb, handlers, fallback_app)` is the core WSGI app object. ([Dulwich][10])
* Middleware/utilities exist for HTTP correctness:

  * `GunzipFilter` to unzip gzip-encoded requests
  * `_LengthLimitedFile` ensures EOF after Content-Length bytes (WSGI compliance edge) ([Dulwich][14])

### B1.6 SSH wiring knobs

* `SSHVendor.run_command(host, command, username=None, port=None, password=None, key_filename=None, ssh_command=None, protocol_version=None)` is the explicit interface Dulwich uses for SSH subprocess/vendor implementations. ([Dulwich][15])

---

## B2) Failure modes & footguns

### B2.1 “Works over HTTP, fails over SSH” (hangups / auth / hostkey)

* Dulwich surfaces protocol-level failures via `GitProtocolError` subclasses like `HangupException`. ([Dulwich][16])
  **Typical causes:** host key verification, missing credentials, unsupported SSH configuration in the chosen SSH vendor.

### B2.2 Capability mismatch (thin-pack, side-band, shallow)

* If one side advertises features the other can’t honor, you’ll see failures ranging from “invalid wants” to mid-stream termination. Dulwich’s server enumerates the capability set it supports; keep your client expectations aligned (and log the negotiated set). ([samba.org][13])

### B2.3 Protocol-v2 filters silently ignored

* `filter_spec` is explicitly “only used if the server supports protocol-v2 ‘filter’ feature, and ignored otherwise.” If you depend on reduced transfer sizes, you must detect whether it was honored (e.g., via negotiated protocol/capabilities). ([Dulwich][12])

### B2.4 Smart HTTP request-body handling bugs (Content-Length / gzip)

* WSGI servers differ; Dulwich includes `_LengthLimitedFile` because some servers don’t enforce EOF after Content-Length as the WSGI spec requires. If you stream request bodies, keep this middleware in play. ([Dulwich][17])

---

## B3) Minimal snippets

### B3.1 “Open a remote transport” and fetch refs with a chosen protocol version

```python
from dulwich.client import get_transport_and_path

client, relpath = get_transport_and_path(
    "https://example.com/my/repo.git",
    operation="pull",
    thin_packs=True,
)

refs = client.get_refs(relpath, protocol_version=2)  # protocol_version is an explicit knob
```

`get_transport_and_path(...)` returns `(GitClient, path)` and documents `operation`, `thin_packs`, `report_activity`. ([Dulwich][8])

### B3.2 Fetch a pack with shallow depth + ref-prefix filtering + protocol v2 filter

```python
def determine_wants(remote_refs):
    # choose what you want based on refs dict; minimal example: want HEAD
    return [remote_refs[b"HEAD"]]

result = client.fetch_pack(
    relpath,
    determine_wants=determine_wants,
    graph_walker=None,      # you normally pass a graph walker from a target repo
    pack_data=lambda chunk: None,
    depth=50,
    ref_prefix=[b"refs/heads/"],
    filter_spec=b"blob:none",
    protocol_version=2,
)
```

Dulwich documents `depth`, `ref_prefix`, `filter_spec`, and `protocol_version` on `fetch_pack`, including how filtering and protocol-v2 behavior is conditional. ([Dulwich][12])

### B3.3 Minimal smart HTTP server (WSGI)

```python
from wsgiref.simple_server import make_server
from dulwich.web import HTTPGitApplication
from dulwich.server import Backend

class FSBackend(Backend):
    def open_repository(self, path):
        from dulwich.repo import Repo
        return Repo(path)

app = HTTPGitApplication(backend=FSBackend(), dumb=False, handlers=None, fallback_app=None)

httpd = make_server("0.0.0.0", 8000, app)
httpd.serve_forever()
```

`HTTPGitApplication` is the WSGI application object for Git smart HTTP. ([Dulwich][10])

---

# Deep Dive C — Worktree/Index correctness (staging ↔ tree ↔ filesystem)

## C0) One rule to tattoo on your codebase: bytes-first

Dulwich’s plumbing APIs intentionally operate on **byte strings** (paths, ids, etc.) because Git treats filenames as sequences of non-NUL bytes and does not enforce UTF-8. Higher-level porcelain converts unicode to UTF-8 bytes. ([Dulwich][18])

If you’re building a “contract harness,” treat “**accidental `str` paths**” as a correctness bug unless you are explicitly in porcelain land.

---

## C1) Power knobs (checkout/staging correctness levers)

### C1.1 Index access + locking

* `BaseRepo.open_index()` exists and raises `NoIndexPresent` when the repository has no index (e.g., bare). ([Dulwich][19])
* `dulwich.index.locked_index` exists as the “lock the index while making modifications” primitive. ([Dulwich][20])
* For file-level atomicity, Dulwich provides `GitFile(...)` which “obeys the git file locking protocol”; lock contention is represented by `FileLocked`. ([Dulwich][21])

### C1.2 Checkout primitives: “fresh clone” vs “incremental update”

#### (a) Fresh materialization: `build_index_from_tree(...)`

Signature highlights:

* `build_index_from_tree(root_path, index_path, object_store, tree_id, honor_filemode=True, validate_path_element=..., symlink_fn=None, blob_normalizer=None, tree_encoding='utf-8')` ([Dulwich][20])
  Behavioral landmine:
* “existing index is wiped and contents are not merged… Suitable only for fresh clones.” ([Dulwich][20])

#### (b) Incremental update: `update_working_tree(...)`

Signature highlights:

* `update_working_tree(repo, old_tree_id, new_tree_id, change_iterator, ... force_remove_untracked=False, blob_normalizer=None, tree_encoding='utf-8', allow_overwrite_modified=False)` ([Dulwich][20])
  Important knobs:
* `allow_overwrite_modified=False` means **error if you’d overwrite local modifications**. ([Dulwich][20])
* `force_remove_untracked=False` prevents accidental deletion of unknown files. ([Dulwich][20])
* `blob_normalizer` is explicitly for line-ending conversions on checkout. ([Dulwich][20])

### C1.3 Path validation & filesystem-specific behavior

* `validate_path(path, element_validator=validate_path_element_default)` is the default validator: “just checks for .git/.” ([Dulwich][20])
* `validate_path_element_hfs(...)` exists and is described as equivalent to Git’s HFS dotgit checks using **NFD normalization** and ignoring HFS+ ignorable chars. ([Dulwich][20])
* `get_path_element_normalizer(config)` returns the filesystem-appropriate normalizer function. ([Dulwich][20])

### C1.4 “Stage ↔ tree” conversion

* `commit_index(object_store, index)` exists but is **deprecated**; docs say to use `index.commit()` instead. ([Dulwich][20])
* `commit_tree(object_store, blobs=[(path, sha, mode), ...]) -> tree_sha` is the lower-level tree builder. ([Dulwich][20])
* `refresh_index(index, root_path)` is the equivalent of running **`git commit -a`**. ([Dulwich][20])

### C1.5 Sparse / skip-worktree plumbing

* `IndexEntry.set_skip_worktree(skip=True)` toggles the skip-worktree bit and ensures the extended-flag is set. ([Dulwich][22])
* The module exposes constants like `EXTENDED_FLAG_SKIP_WORKTREE` and `DEFAULT_VERSION=2` for the index format baseline. ([Dulwich][20])

### C1.6 Worktrees (multi-checkout correctness)

* `add_worktree(repo, path, branch=None, commit=None, force=False, detach=False, exist_ok=False) -> Repo` and raises `ValueError` for path conflicts or already-checked-out branch. ([Dulwich][23])
* `prune_worktrees(repo, expire=None, dry_run=False)` and `remove_worktree(repo, path, force=False)` have explicit correctness semantics (locked/local-changes cases yield errors). ([Dulwich][23])
* `temporary_worktree(repo, prefix='tmp-worktree-')` provides an auto-cleaned context manager. ([Dulwich][23])

---

## C2) Failure modes (and the “right” reaction)

### C2.1 Overwrite-protected checkout

* `WorkingTreeModifiedError` exists for “modifications that would be overwritten.” ([Dulwich][24])
  **Policy:** default to refusing overwrite (`allow_overwrite_modified=False`). Only allow overrides in explicit “force” flows and log the exact files impacted.

### C2.2 Invalid/untrusted paths (.git injection, filesystem reserved names)

* Always pass the strictest validator you can:

  * use HFS/NTFS validators when your deployment filesystem warrants it. ([Dulwich][20])
    **Policy:** treat any validator rejection as a hard failure during checkout/materialization (it’s usually a security boundary).

### C2.3 Case-only renames (cross-platform drift)

* `detect_case_only_renames(...)` exists and rewrites ADD/DELETE pairs into rename operations using config-driven normalization. ([Dulwich][20])
  **Policy:** if you diff trees across mixed-case filesystems (macOS vs Linux), run a normalization pass (or at least detect and surface) before applying changes.

### C2.4 Bare repos / missing index

* `open_index()` raises `NoIndexPresent`. ([Dulwich][19])
  **Policy:** never assume “repo has a working tree + index”; branch on `repo.bare` / catch `NoIndexPresent`.

### C2.5 Concurrent writers

* Use `locked_index` + git-style locking primitives (`GitFile`) to avoid races; handle `FileLocked` deterministically in your retry/backoff policy. ([Dulwich][20])

---

## C3) Minimal snippets

### C3.1 Fresh “checkout HEAD” (correct *only* for fresh clones)

```python
from dulwich.repo import Repo
from dulwich.index import build_index_from_tree

r = Repo("myrepo")
tree_id = r[b"HEAD"].tree  # bytes-level access

build_index_from_tree(
    root_path=r.path,
    index_path=r.index_path(),
    object_store=r.object_store,
    tree_id=tree_id,
)
```

`build_index_from_tree(...)` explicitly warns it wipes the existing index and is suitable only for fresh clones. ([Dulwich][20])

### C3.2 Enumerate unstaged changes (index ↔ filesystem drift)

```python
from dulwich.index import get_unstaged_changes
from dulwich.repo import Repo

r = Repo("myrepo")
idx = r.open_index()
for p in get_unstaged_changes(idx, r.path):
    print(p)  # path bytes
```

`get_unstaged_changes(...)` returns an iterator over paths with unstaged changes. ([Dulwich][20])

### C3.3 “git commit -a” equivalent (refresh tracked files into index)

```python
from dulwich.index import refresh_index
from dulwich.repo import Repo

r = Repo("myrepo")
idx = r.open_index()
refresh_index(idx, r.path)
```

`refresh_index(...)` is documented as equivalent to running `git commit -a`. ([Dulwich][20])

### C3.4 Create a linked worktree safely

```python
from dulwich.repo import Repo
from dulwich.worktree import add_worktree, list_worktrees

main = Repo("myrepo")
wt = add_worktree(main, path="myrepo-wt-feature", branch=b"feature", exist_ok=False)
print(list_worktrees(main))
```

`add_worktree(...)` includes `force/detach/exist_ok` knobs and raises `ValueError` on conflicts. ([Dulwich][23])

---

If you want the next increment after these pages (in the style you’ve been building): I can turn each deep dive into a **repo-ready “contract harness” pack** (pytest fixtures + golden failure snapshots + deterministic normalization rules) for (A) pack verification streams, (B) transport negotiation surfaces, and (C) checkout/index invariants (path validators + overwrite policy + worktree lifecycle).

[1]: https://dulwich.io/api/dulwich.pack.PackStreamReader.html?utm_source=chatgpt.com "dulwich.pack.PackStreamReader"
[2]: https://dulwich.io/api/dulwich.pack.DeltaChainIterator.html?utm_source=chatgpt.com "dulwich.pack.DeltaChainIterator"
[3]: https://dulwich.io/api/dulwich.pack.html?utm_source=chatgpt.com "dulwich.pack"
[4]: https://dulwich.io/api/dulwich.pack.PackStreamCopier.html?utm_source=chatgpt.com "dulwich.pack.PackStreamCopier"
[5]: https://dulwich.io/api/dulwich.pack.PackChunkGenerator.html?utm_source=chatgpt.com "dulwich.pack.PackChunkGenerator"
[6]: https://dulwich.io/api/dulwich.pack.PackIndex.html?utm_source=chatgpt.com "dulwich.pack.PackIndex"
[7]: https://dulwich.io/api/dulwich.errors.ApplyDeltaError.html?utm_source=chatgpt.com "dulwich.errors.ApplyDeltaError"
[8]: https://dulwich.io/api/dulwich.client.html "dulwich.client"
[9]: https://dulwich.io/api/dulwich.server.UploadPackHandler.html?utm_source=chatgpt.com "dulwich.server.UploadPackHandler"
[10]: https://dulwich.io/api/dulwich.web.HTTPGitApplication.html?utm_source=chatgpt.com "dulwich.web.HTTPGitApplication"
[11]: https://git-scm.com/docs/protocol-capabilities/2.17.0?utm_source=chatgpt.com "Git - protocol-capabilities Documentation"
[12]: https://dulwich.io/api/dulwich.client.TraditionalGitClient.html?utm_source=chatgpt.com "dulwich.client.TraditionalGitClient"
[13]: https://www.samba.org/~jelmer/dulwich/docs/api/dulwich.server.html?utm_source=chatgpt.com "dulwich.server module"
[14]: https://dulwich.io/api/dulwich.web.html?utm_source=chatgpt.com "dulwich.web"
[15]: https://dulwich.io/api/dulwich.client.SSHVendor.html?utm_source=chatgpt.com "dulwich.client.SSHVendor"
[16]: https://dulwich.io/api/dulwich.errors.GitProtocolError.html?utm_source=chatgpt.com "dulwich.errors.GitProtocolError"
[17]: https://dulwich.io/api/dulwich.web._LengthLimitedFile.html?utm_source=chatgpt.com "dulwich.web._LengthLimitedFile"
[18]: https://dulwich.readthedocs.io/_/downloads/en/stable/pdf/?utm_source=chatgpt.com "dulwich Documentation"
[19]: https://dulwich.io/api/dulwich.repo.BaseRepo.html?utm_source=chatgpt.com "dulwich.repo.BaseRepo"
[20]: https://dulwich.io/api/dulwich.index.html "dulwich.index"
[21]: https://dulwich.io/api/dulwich.file.html?utm_source=chatgpt.com "dulwich.file"
[22]: https://dulwich.io/api/dulwich.index.IndexEntry.html?utm_source=chatgpt.com "dulwich.index.IndexEntry"
[23]: https://www.dulwich.io/api/dulwich.worktree.html "dulwich.worktree"
[24]: https://dulwich.io/api/dulwich.errors.WorkingTreeModifiedError.html?utm_source=chatgpt.com "dulwich.errors.WorkingTreeModifiedError"

Below is a **repo-ready Dulwich “contract harness” pack**: **pytest fixtures + golden failure snapshots + deterministic normalization** for:

* **(A)** pack verification streams + corruption handling
* **(B)** transport negotiation probes (local always-on + optional smart HTTP integration)
* **(C)** checkout/index invariants + worktree lifecycle

You can drop this into your repo (adjusting the import root if needed).

---

## File tree

```
src/codeintel/interop/dulwich_contracts/
  __init__.py
  snapshot_norm.py
  pack_contract.py
  transport_contract.py
  worktree_contract.py

tests/contracts/
  _snapshot_json.py
  dulwich/
    __init__.py
    conftest.py
    test_pack_contract.py
    test_transport_contract.py
    test_worktree_contract.py

tests/golden/dulwich/
  pack/
    checksum_mismatch.json
  transport/
    local_ref_prefix_heads.json
  worktree/
    stage_absolute_path_error.json
    overwrite_modified.json

pytest.ini   (optional, to register markers)
```

---

## How to run

```bash
pip install dulwich pytest
pytest -q tests/contracts/dulwich
```

### Update goldens

```bash
UPDATE_GOLDENS=1 pytest -q tests/contracts/dulwich
```

### Optional smart-HTTP integration test

```bash
DULWICH_ENABLE_HTTP_INTEGRATION=1 pytest -q tests/contracts/dulwich -m dulwich_http_integration
```

---

## Deterministic normalization rules (what gets stabilized)

Implemented in `snapshot_norm.py` + `_snapshot_json.py`:

* **Paths**: replace absolute repo/temp roots with `<REPO>` / `<TMP>` / `<CWD>`
* **SHA1 hex**: redact `\b[0-9a-f]{40}\b` → `<SHA1>`
* **Ephemeral ports**: `:\d{2,5}` → `:<PORT>`
* **Slashes/whitespace**: normalize `\`→`/` and collapse whitespace
* **Golden placeholders**: JSON snapshots can use `"<ANY>"` for version-dependent/unstable fields

---

# 1) `src/codeintel/interop/dulwich_contracts/`

### `src/codeintel/interop/dulwich_contracts/__init__.py`

```python
# intentionally empty; namespace package for contract helpers
```

### `src/codeintel/interop/dulwich_contracts/snapshot_norm.py`

```python
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

_RE_SHA1_HEX = re.compile(r"\b[0-9a-f]{40}\b", re.IGNORECASE)
_RE_PORT = re.compile(r":\d{2,5}\b")
_RE_WIN_DRIVE = re.compile(r"\b[A-Za-z]:\\")
_RE_MULTI_WS = re.compile(r"[ \t]+")


@dataclass(frozen=True)
class RedactionPolicy:
    """Deterministic normalization rules for golden snapshots."""
    repo_root: Path | None = None
    tmp_root: Path | None = None


def _norm_slashes(s: str) -> str:
    return s.replace("\\", "/")


def redact_message(msg: str, policy: RedactionPolicy) -> str:
    """
    Normalize exception messages for deterministic golden comparisons:

    - Replace absolute repo/temp roots with <REPO> / <TMP> / <CWD>
    - Replace sha1 hex strings with <SHA1>
    - Replace ephemeral ports with :<PORT>
    - Normalize slashes + whitespace
    """
    s = msg

    if policy.repo_root is not None:
        s = s.replace(str(policy.repo_root), "<REPO>")
        s = s.replace(_norm_slashes(str(policy.repo_root)), "<REPO>")

    if policy.tmp_root is not None:
        s = s.replace(str(policy.tmp_root), "<TMP>")
        s = s.replace(_norm_slashes(str(policy.tmp_root)), "<TMP>")

    s = s.replace(os.getcwd(), "<CWD>")
    s = s.replace(_norm_slashes(os.getcwd()), "<CWD>")

    s = _RE_SHA1_HEX.sub("<SHA1>", s)
    s = _RE_PORT.sub(":<PORT>", s)

    # Windows drive letters can leak through in some messages
    s = _RE_WIN_DRIVE.sub("<DRIVE>:/", _norm_slashes(s))

    s = _RE_MULTI_WS.sub(" ", s).strip()
    return s


def bytes_to_hex(b: bytes) -> str:
    return b.hex()


def to_jsonable(x: Any) -> Any:
    """
    Convert common Dulwich-ish objects to JSON-safe representations.

    Contract policy:
    - bytes → {"__bytes__": "<hex>"} to avoid accidental UTF-8 decoding drift
    - Paths → str
    - mappings/sequences → recursive conversion
    """
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    if isinstance(x, bytes):
        return {"__bytes__": bytes_to_hex(x)}
    if isinstance(x, Path):
        return str(x)
    if isinstance(x, Mapping):
        return {str(k): to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set, frozenset)):
        return [to_jsonable(v) for v in x]
    if hasattr(x, "__dict__"):
        return {"__repr__": repr(x)}
    return {"__repr__": repr(x)}


def normalize_exception(exc: BaseException, *, policy: RedactionPolicy) -> dict[str, Any]:
    """Deterministic “error envelope” for golden snapshots."""
    return {
        "type": f"{exc.__class__.__module__}.{exc.__class__.__name__}",
        "message": redact_message(str(exc), policy),
    }
```

### `src/codeintel/interop/dulwich_contracts/pack_contract.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Sequence

from .snapshot_norm import bytes_to_hex


@dataclass(frozen=True)
class PackBuildOptions:
    deltify: bool = False
    delta_window_size: int | None = None
    compression_level: int = 0  # reduces zlib variability; don't pin pack bytes across environments


@dataclass(frozen=True)
class PackObjectInfo:
    oid_hex: str
    pack_type_num: int
    obj_type_num: int | None
    crc32: int | None


@dataclass(frozen=True)
class PackVerifyReport:
    object_count: int
    objects: list[PackObjectInfo]


def build_pack_bytes(objects: Sequence[object], *, opts: PackBuildOptions) -> bytes:
    """Write a pack into memory (bytes) from a list of ShaFile objects."""
    from dulwich.pack import write_pack_objects  # local import for importorskip friendliness

    buf = BytesIO()
    write_pack_objects(
        buf.write,
        objects,  # can be ShaFile or (ShaFile, path) tuples
        delta_window_size=opts.delta_window_size,
        deltify=opts.deltify,
        compression_level=opts.compression_level,
    )
    return buf.getvalue()


def _read_all_factory(bio: BytesIO):
    def read_all(size: int) -> bytes:
        data = bio.read(size)
        if len(data) != size:
            raise IOError(f"short read: wanted {size}, got {len(data)}")
        return data
    return read_all


def verify_pack_bytes(pack_bytes: bytes, *, compute_crc32: bool = True) -> PackVerifyReport:
    """Parse and checksum-verify a pack stream."""
    from dulwich.pack import PackStreamReader  # local import

    bio = BytesIO(pack_bytes)
    r = PackStreamReader(read_all=_read_all_factory(bio), read_some=bio.read)
    unpacked = list(r.read_objects(compute_crc32=compute_crc32))

    infos: list[PackObjectInfo] = []
    for u in unpacked:
        # UnpackedObject.sha() returns the binary SHA for the unpacked object.
        infos.append(
            PackObjectInfo(
                oid_hex=bytes_to_hex(u.sha()),
                pack_type_num=u.pack_type_num,
                obj_type_num=u.obj_type_num,
                crc32=u.crc32,
            )
        )
    return PackVerifyReport(object_count=len(infos), objects=infos)


def corrupt_bytes_flip_middle(pack_bytes: bytes) -> bytes:
    """Deterministic corruption: flip a bit in the middle of the byte stream."""
    if not pack_bytes:
        return pack_bytes
    b = bytearray(pack_bytes)
    b[len(b) // 2] ^= 0x01
    return bytes(b)


def stable_object_identity_set(report: PackVerifyReport) -> set[str]:
    """Stable set of object IDs (hex) from a verify report."""
    return {o.oid_hex for o in report.objects}


def stable_object_type_map(report: PackVerifyReport) -> dict[str, int]:
    """Map object ID → pack type (stable across compression output)."""
    return {o.oid_hex: o.pack_type_num for o in report.objects}
```

### `src/codeintel/interop/dulwich_contracts/transport_contract.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .snapshot_norm import bytes_to_hex


@dataclass(frozen=True)
class TransportProbeOptions:
    operation: str = "pull"  # "pull" or "push"
    protocol_version: int | None = None
    thin_packs: bool = True
    depth: int | None = None
    ref_prefix: list[bytes] | None = None
    filter_spec: bytes | None = None


@dataclass(frozen=True)
class TransportProbeReport:
    client_class: str
    path: str
    returned_ref_names: list[str]
    returned_ref_targets: dict[str, str]  # ref name → oid hex


def _default_determine_wants(remote_refs: dict[bytes, bytes], depth: int | None) -> list[bytes]:
    # conservative: ask for all advertised heads
    return list(remote_refs.values())


def probe_local_transport(
    *,
    source_repo_path: str,
    target_repo,
    opts: TransportProbeOptions,
    determine_wants: Callable[[dict[bytes, bytes], int | None], list[bytes]] | None = None,
) -> TransportProbeReport:
    """
    Local transport probe (always-on, deterministic).

    Uses LocalGitClient to:
      - get_refs (optionally filtered)
      - fetch objects into target repo

    Note: Dulwich versions differ in whether refs are updated by fetch().
    This probe reports *returned refs* rather than assuming target refs are updated.
    """
    from dulwich.client import LocalGitClient

    client = LocalGitClient(thin_packs=opts.thin_packs)
    path = source_repo_path

    remote_refs = client.get_refs(path, protocol_version=opts.protocol_version, ref_prefix=opts.ref_prefix)

    det = determine_wants or _default_determine_wants
    client.fetch(
        path,
        target_repo,
        determine_wants=det,
        depth=opts.depth,
        ref_prefix=opts.ref_prefix,
        filter_spec=opts.filter_spec,
        protocol_version=opts.protocol_version,
    )

    returned_ref_names = sorted([r.decode("utf-8", "replace") for r in remote_refs.keys()])
    returned_ref_targets = {k.decode("utf-8", "replace"): bytes_to_hex(v) for k, v in remote_refs.items()}

    return TransportProbeReport(
        client_class=client.__class__.__name__,
        path=path,
        returned_ref_names=returned_ref_names,
        returned_ref_targets=returned_ref_targets,
    )


def best_effort_update_refs(target_repo, remote_refs: dict[bytes, bytes], *, only_heads: bool = True) -> None:
    """Compatibility shim: update refs in the target repo to match remote refs."""
    for ref, oid in remote_refs.items():
        if only_heads and not ref.startswith(b"refs/heads/"):
            continue
        target_repo[ref] = oid
```

### `src/codeintel/interop/dulwich_contracts/worktree_contract.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class CommitIdentity:
    author: bytes = b"CI Bot <ci@example.com>"
    committer: bytes = b"CI Bot <ci@example.com>"
    timestamp: int = 1_700_000_000  # fixed epoch seconds
    timezone: int = 0


def init_repo(path: Path):
    """Initialize a non-bare repo at `path` (mkdir=True if supported)."""
    from dulwich.repo import Repo
    try:
        return Repo.init(str(path), mkdir=True)
    except Exception:
        from dulwich import porcelain
        return porcelain.init(str(path))


def write_file(repo_path: Path, rel: str, data: bytes) -> None:
    p = repo_path / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def stage(repo, rel_paths: Iterable[str]) -> None:
    """Stage paths (must be repo-relative; absolute paths are a contract violation)."""
    repo.stage(list(rel_paths))


def commit(repo, *, message: bytes, ident: CommitIdentity) -> bytes:
    """Create a deterministic commit (timestamps + identity fixed)."""
    return repo.do_commit(
        message=message,
        author=ident.author,
        committer=ident.committer,
        commit_timestamp=ident.timestamp,
        commit_timezone=ident.timezone,
        author_timestamp=ident.timestamp,
        author_timezone=ident.timezone,
    )


def checkout_tree(repo, tree_id: bytes) -> None:
    """Materialize a tree into working tree + index. Safe for tests (wipes index)."""
    from dulwich.index import build_index_from_tree
    build_index_from_tree(repo.path, repo.index_path(), repo.object_store, tree_id)


def compute_tree_changes(repo, old_tree: bytes | None, new_tree: bytes):
    from dulwich.diff_tree import tree_changes
    return list(tree_changes(repo.object_store, old_tree, new_tree))


def apply_tree_update(
    repo,
    *,
    old_tree: bytes | None,
    new_tree: bytes,
    allow_overwrite_modified: bool,
) -> None:
    """Apply a tree transition using the same primitive porcelain checkout uses underneath."""
    from dulwich.index import update_working_tree

    changes = compute_tree_changes(repo, old_tree, new_tree)
    update_working_tree(
        repo,
        old_tree_id=old_tree,
        new_tree_id=new_tree,
        change_iterator=iter(changes),
        allow_overwrite_modified=allow_overwrite_modified,
    )
```

---

# 2) `tests/contracts/`

### `tests/contracts/_snapshot_json.py`

```python
from __future__ import annotations

import difflib
import json
import os
from pathlib import Path
from typing import Any

import pytest

UPDATE_GOLDENS = os.getenv("UPDATE_GOLDENS") == "1"
ANY = "<ANY>"


def _match_any(expected: Any, actual: Any) -> bool:
    if expected == ANY:
        return True
    if type(expected) != type(actual):
        return False
    if isinstance(expected, dict):
        # expected keys must exist; extra keys in actual are allowed
        for k, v in expected.items():
            if k not in actual:
                return False
            if not _match_any(v, actual[k]):
                return False
        return True
    if isinstance(expected, list):
        if len(expected) != len(actual):
            return False
        return all(_match_any(e, a) for e, a in zip(expected, actual))
    return expected == actual


def assert_json_snapshot(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if UPDATE_GOLDENS:
        path.write_text(json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        return

    if not path.exists():
        path.write_text(json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
        pytest.fail(f"Snapshot did not exist; wrote {path}. Re-run with UPDATE_GOLDENS=1 to accept.")

    expected_obj = json.loads(path.read_text(encoding="utf-8"))
    if not _match_any(expected_obj, data):
        expected = json.dumps(expected_obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        actual = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        diff = "\n".join(
            difflib.unified_diff(
                expected.splitlines(),
                actual.splitlines(),
                fromfile="expected",
                tofile="actual",
                lineterm="",
            )
        )
        pytest.fail(f"Snapshot mismatch for {path}:\n{diff}")
```

---

# 3) `tests/contracts/dulwich/`

### `tests/contracts/dulwich/__init__.py`

```python
# package marker
```

### `tests/contracts/dulwich/conftest.py`

```python
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("dulwich")

from codeintel.interop.dulwich_contracts.worktree_contract import (
    CommitIdentity,
    commit,
    init_repo,
    stage,
    write_file,
)


@pytest.fixture()
def dulwich_ident() -> CommitIdentity:
    # Stable author/committer/timestamps for deterministic object IDs
    return CommitIdentity()


@pytest.fixture()
def source_repo(tmp_path: Path, dulwich_ident: CommitIdentity):
    repo_path = tmp_path / "source"
    repo = init_repo(repo_path)

    write_file(repo_path, "hello.txt", b"hello\n")
    stage(repo, ["hello.txt"])
    commit(repo, message=b"init", ident=dulwich_ident)

    return repo


@pytest.fixture()
def source_and_target_repos(tmp_path: Path, dulwich_ident: CommitIdentity):
    source_path = tmp_path / "source"
    target_path = tmp_path / "target"

    source = init_repo(source_path)
    write_file(source_path, "hello.txt", b"hello\n")
    stage(source, ["hello.txt"])
    commit(source, message=b"init", ident=dulwich_ident)

    target = init_repo(target_path)
    return source, target
```

---

## A) Pack contracts

### `tests/contracts/dulwich/test_pack_contract.py`

```python
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("dulwich")


def _golden_root() -> Path:
    # tests/contracts/dulwich/... → tests/golden/dulwich
    return Path(__file__).resolve().parents[2] / "golden" / "dulwich"


from codeintel.interop.dulwich_contracts.pack_contract import (
    PackBuildOptions,
    build_pack_bytes,
    corrupt_bytes_flip_middle,
    verify_pack_bytes,
)
from codeintel.interop.dulwich_contracts.snapshot_norm import RedactionPolicy, normalize_exception
from tests.contracts._snapshot_json import assert_json_snapshot


def _repo_objects_minimal(repo):
    # HEAD commit + its tree + root blobs
    commit = repo[b"HEAD"]
    tree = repo[commit.tree]
    objs = [commit, tree]

    for name, mode, oid in tree.items():
        obj = repo[oid]
        objs.append((obj, name))  # path hint for deltification heuristics
    return objs


def test_pack_roundtrip_smoke(source_repo):
    objs = _repo_objects_minimal(source_repo)
    pack_bytes = build_pack_bytes(objs, opts=PackBuildOptions(deltify=False, compression_level=0))

    report = verify_pack_bytes(pack_bytes, compute_crc32=True)

    # Semantic invariants: checksum verified, object count non-zero, CRCs populated when requested.
    assert report.object_count >= 2
    assert all(o.crc32 is not None for o in report.objects)


def test_pack_corruption_checksum_mismatch_golden(source_repo, tmp_path: Path):
    objs = _repo_objects_minimal(source_repo)
    pack_bytes = build_pack_bytes(objs, opts=PackBuildOptions(deltify=False, compression_level=0))
    bad = corrupt_bytes_flip_middle(pack_bytes)

    from dulwich.errors import ChecksumMismatch

    try:
        verify_pack_bytes(bad, compute_crc32=False)
    except ChecksumMismatch as e:
        payload = normalize_exception(e, policy=RedactionPolicy(tmp_root=tmp_path))
        assert_json_snapshot(_golden_root() / "pack" / "checksum_mismatch.json", payload)
    else:
        pytest.fail("Expected ChecksumMismatch when reading corrupted pack bytes")
```

---

## B) Transport contracts

### `tests/contracts/dulwich/test_transport_contract.py`

```python
from __future__ import annotations

import os
import threading
from pathlib import Path

import pytest

pytest.importorskip("dulwich")


def _golden_root() -> Path:
    # tests/contracts/dulwich/... → tests/golden/dulwich
    return Path(__file__).resolve().parents[2] / "golden" / "dulwich"


from codeintel.interop.dulwich_contracts.transport_contract import TransportProbeOptions, probe_local_transport
from tests.contracts._snapshot_json import assert_json_snapshot


def test_local_transport_get_refs_prefix_filter(source_and_target_repos):
    source, target = source_and_target_repos
    opts = TransportProbeOptions(protocol_version=None, ref_prefix=[b"refs/heads/"])

    report = probe_local_transport(
        source_repo_path=source.path,
        target_repo=target,
        opts=opts,
    )

    # Contract: ref_prefix filtering should not return non-head refs.
    assert all(r.startswith("refs/heads/") for r in report.returned_ref_names)

    # Snapshot the ref-name surface (branch names vary across configs; treat as <ANY>)
    assert_json_snapshot(
        _golden_root() / "transport" / "local_ref_prefix_heads.json",
        {"returned_ref_names": report.returned_ref_names},
    )


@pytest.mark.dulwich_http_integration
def test_http_transport_probe_smoke(source_repo):
    if os.getenv("DULWICH_ENABLE_HTTP_INTEGRATION") != "1":
        pytest.skip("Set DULWICH_ENABLE_HTTP_INTEGRATION=1 to run smart HTTP integration test")

    from wsgiref.simple_server import make_server
    from dulwich.server import Backend
    from dulwich.web import HTTPGitApplication
    from dulwich.client import get_transport_and_path

    class SingleRepoBackend(Backend):
        def open_repository(self, path):
            return source_repo

    app = HTTPGitApplication(backend=SingleRepoBackend(), dumb=False, handlers=None, fallback_app=None)
    httpd = make_server("127.0.0.1", 0, app)
    host, port = httpd.server_address

    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    try:
        url = f"http://{host}:{port}/repo"
        client, path = get_transport_and_path(url, operation="pull")
        refs = client.get_refs(path)
        assert refs
    finally:
        httpd.shutdown()
```

---

## C) Worktree / index contracts

### `tests/contracts/dulwich/test_worktree_contract.py`

```python
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("dulwich")


def _golden_root() -> Path:
    # tests/contracts/dulwich/... → tests/golden/dulwich
    return Path(__file__).resolve().parents[2] / "golden" / "dulwich"


from codeintel.interop.dulwich_contracts.snapshot_norm import RedactionPolicy, normalize_exception
from codeintel.interop.dulwich_contracts.worktree_contract import (
    CommitIdentity,
    apply_tree_update,
    checkout_tree,
    commit,
    init_repo,
    stage,
    write_file,
)
from tests.contracts._snapshot_json import assert_json_snapshot


def test_stage_rejects_absolute_path_golden(tmp_path: Path, dulwich_ident: CommitIdentity):
    repo_path = tmp_path / "repo"
    repo = init_repo(repo_path)

    write_file(repo_path, "hello.txt", b"hello\n")
    abs_path = str((repo_path / "hello.txt").resolve())

    try:
        repo.stage([abs_path])  # contract: absolute paths should error (version-dependent)
    except Exception as e:
        payload = normalize_exception(e, policy=RedactionPolicy(repo_root=repo_path, tmp_root=tmp_path))
        assert_json_snapshot(_golden_root() / "worktree" / "stage_absolute_path_error.json", payload)
    else:
        pytest.fail("Expected repo.stage([abs_path]) to raise")


def test_update_working_tree_refuses_overwrite_modified_golden(tmp_path: Path, dulwich_ident: CommitIdentity):
    from dulwich.errors import WorkingTreeModifiedError

    repo_path = tmp_path / "repo"
    repo = init_repo(repo_path)

    # Commit 1: hello=v1
    write_file(repo_path, "hello.txt", b"v1\n")
    stage(repo, ["hello.txt"])
    commit(repo, message=b"c1", ident=dulwich_ident)
    c1 = repo[b"HEAD"]
    t1 = c1.tree

    # Commit 2: hello=v2
    write_file(repo_path, "hello.txt", b"v2\n")
    stage(repo, ["hello.txt"])
    commit(repo, message=b"c2", ident=dulwich_ident)
    c2 = repo[b"HEAD"]
    t2 = c2.tree

    # Reset working tree to commit1 tree (safe in tests; wipes index)
    checkout_tree(repo, t1)

    # Local edit that would be overwritten by updating to t2
    write_file(repo_path, "hello.txt", b"LOCAL\n")

    try:
        apply_tree_update(repo, old_tree=t1, new_tree=t2, allow_overwrite_modified=False)
    except WorkingTreeModifiedError as e:
        payload = normalize_exception(e, policy=RedactionPolicy(repo_root=repo_path, tmp_root=tmp_path))
        assert_json_snapshot(_golden_root() / "worktree" / "overwrite_modified.json", payload)
    else:
        pytest.fail("Expected WorkingTreeModifiedError when overwrite would occur")


def test_worktree_lifecycle_smoke(tmp_path: Path, dulwich_ident: CommitIdentity):
    from dulwich.worktree import add_worktree, list_worktrees, remove_worktree

    repo_path = tmp_path / "repo"
    repo = init_repo(repo_path)

    write_file(repo_path, "hello.txt", b"hello\n")
    stage(repo, ["hello.txt"])
    commit(repo, message=b"init", ident=dulwich_ident)

    wt_path = tmp_path / "wt"
    add_worktree(repo, wt_path, branch=b"wt-branch", exist_ok=False)

    infos = list_worktrees(repo)
    assert any(Path(i.path) == wt_path for i in infos)

    remove_worktree(repo, wt_path, force=True)
```

---

# 4) Golden snapshots

### `tests/golden/dulwich/pack/checksum_mismatch.json`

```json
{
  "message": "<ANY>",
  "type": "dulwich.errors.ChecksumMismatch"
}
```

### `tests/golden/dulwich/transport/local_ref_prefix_heads.json`

```json
{
  "returned_ref_names": "<ANY>"
}
```

### `tests/golden/dulwich/worktree/stage_absolute_path_error.json`

```json
{
  "message": "<ANY>",
  "type": "<ANY>"
}
```

### `tests/golden/dulwich/worktree/overwrite_modified.json`

```json
{
  "message": "<ANY>",
  "type": "dulwich.errors.WorkingTreeModifiedError"
}
```

---

## Optional: register pytest markers (avoid warnings)

### `pytest.ini`

```ini
[pytest]
markers =
  dulwich_http_integration: dulwich smart HTTP integration tests (opt-in)
```

---

## What you get out of this immediately

* **Pack contract**: confirms your pack generator/reader pipeline detects corruption (ChecksumMismatch) and that CRC32 capture is wired when requested.
* **Transport contract**: ensures `ref_prefix` filtering behaves as expected and gives you an opt-in smart HTTP smoke check.
* **Worktree/index contract**: enforces “no absolute paths to stage”, and encodes the critical overwrite protection behavior into a golden snapshot, plus minimal worktree lifecycle coverage.

If you want the *next* increment after this pack: I can extend it into a **two-mode suite** (always-on unit-ish vs nightly integration) that also:

* exercises **delta packs** (`deltify=True`) and snapshots **ApplyDeltaError / UnresolvedDeltas** cases,
* adds a **ref-update shim** contract (since “fetch doesn’t update refs” has historically been a footgun),
* and produces a single consolidated **dulwich_compat_report.json** artifact per CI run.
