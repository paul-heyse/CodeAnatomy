Below is a **section map / feature-catalog** for a “pyarrow-advanced.md–style” pygit2 deep technical document (style reference: ). It’s grounded in the **current pygit2 1.19.1 docs + PyPI release** (Dec 29, 2025). ([PyPI][1])

---

## 0) Scope, versioning, and mental model

* **What pygit2 is**: Python bindings to **libgit2** (“Git plumbing” in-process), not a wrapper around the `git` CLI. ([PyPI][1])
* **Version & compatibility** (pygit2 ↔ libgit2), plus how to treat “compiled-in” capabilities as a runtime contract. ([PyPI][1])

---

## A) Installation and build/distribution boundary

Deep-dive topics:

* Wheels vs compiling (and what that implies for libgit2 feature set).
* Linking/licensing constraints and operational packaging posture.
* “Hello repo”: open/init/clone as validation steps. ([pygit2][2])

---

## B) General runtime primitives: version, exceptions, global options

Deep-dive topics:

* **Version constants** (libgit2 version info).
* **Exception model** (`GitError`, `AlreadyExistsError`, `InvalidSpecError`).
* **Global option() control surface** (cache/search path/SSL/owner-validation/etc.). ([pygit2][3])

---

## C) Library-wide Settings control plane

Deep-dive topics:

* Cache sizing + eviction semantics, object-type cache limits.
* TLS/SSL knobs (CA locations, ciphers), timeouts, user-agent.
* mmap window sizing, pack limits, owner validation, strictness toggles. ([pygit2][4])

---

## D) Feature detection

Deep-dive topics:

* The `pygit2.features` surface: how to gate optional functionality on what your build of libgit2 actually supports. ([pygit2][5])

---

## E) Repository core: lifecycle + identity + state machine

Deep-dive topics (the “central object” chapter):

* Open/init/clone flows; bare vs non-bare repos; discovery rules.
* Workdir/gitdir relationships; HEAD state; repository “in-progress” states (merge/cherrypick) and cleanup hooks.
* High-leverage repo entrypoints that other chapters depend on (config, remotes, references, index, etc.). ([pygit2][6])

---

## F) Object IDs (OIDs) and identity discipline

Deep-dive topics:

* OID representations (raw bytes vs hex vs `Oid` object).
* Equality/hashability, prefix resolution, and “stable identity” patterns. ([pygit2][7])

---

## G) Git object model (ODB): blobs, trees, commits, tags

Deep-dive topics:

* Object lookup (`Repository.get`, `__getitem__`, `__contains__`).
* Base `Object` API: peel/read_raw/type metadata.
* **Blobs**: data access, diffing blobs, streaming via BlobIO.
* **Trees**: traversal, path addressing, tree builders, tree↔workdir/index diffs.
* **Commits**: signatures, parents, message trailers, gpg signature surface.
* **Tags**: tag objects vs lightweight refs; tag creation flows. ([pygit2][8])

---

## H) Revision parsing (rev-parse / gitrevisions syntax)

Deep-dive topics:

* `revparse`, `revparse_single`, `revparse_ext` and how “ref-ish” resolution works.
* `RevSpec` flags (single/range/merge-base intent). ([pygit2][9])

---

## I) Commit traversal (“commit log” / walkers)

Deep-dive topics:

* `Repository.walk()` and Walker controls: hide/push/reset/sort/simplify-first-parent.
* Sort modes (time/topological/reverse) and reproducibility implications. ([pygit2][10])

---

## J) Blame

Deep-dive topics:

* `Repository.blame()` and options (range, oldest/newest bounds, whitespace/mailmap flags).
* `Blame`/`BlameHunk` access patterns and performance caveats. ([pygit2][11])

---

## K) References, reflogs, notes, and HEAD semantics

Deep-dive topics:

* `Repository.references` collection operations (create/delete/compress/iterator filters).
* `Reference` type: direct vs symbolic, resolve/peel/rename/set_target + reflog updates.
* HEAD semantics (detached/unborn).
* Reflog iteration + entries.
* Notes API (create/lookup/remove notes). ([pygit2][12])

---

## L) Reference Transactions (atomic multi-ref updates)

Deep-dive topics:

* Transaction context manager semantics (commit/rollback).
* Locking rules, symbolic target updates, deletion.
* Thread-safety constraints and usage patterns. ([pygit2][13])

---

## M) Branches

Deep-dive topics:

* Branch collections (local/remote filters, create/delete/get).
* Branch semantics: upstream tracking, rename, “is_head / is_checked_out” behaviors. ([pygit2][14])

---

## N) Index file and working copy operations

Deep-dive topics:

* Index read/write lifecycle; add/remove patterns; write_tree.
* Conflict objects in index; diff index↔tree/workdir.
* Status scanning controls and performance knobs (untracked recursion).
* Checkout surfaces (head/tree/index).
* Stash create/apply/drop/pop/list semantics. ([pygit2][15])

---

## O) Diff system (diff, patch, hunks, stats, rename/copy detection)

Deep-dive topics:

* Diff entrypoints (`Repository.diff`, `Tree.diff_to_*`, `Blob.diff*`).
* Diff model types: Diff, Patch, Delta, File, Hunk, Line, Stats.
* Similarity detection / rename tracking; parse/merge diffs. ([pygit2][16])

---

## P) Merge & cherrypick (and conflict resolution workflow)

Deep-dive topics:

* Merge base(s): pairwise + N-way (octopus support).
* Merge analysis (fast-forward/up-to-date/etc).
* High-level merge into workdir/index vs low-level `merge_commits` / `merge_trees` returning an in-memory Index.
* Cherrypick + required repository state cleanup. ([pygit2][17])

---

## Q) Remotes & networking (fetch/push/prune/listing) + callbacks + credentials

Deep-dive topics:

* Remote collection management (create/rename/delete/refspec edits).
* Remote operations: connect/list_heads/fetch/push/prune; shallow fetch/push options; proxy support.
* Callback model (progress, update tips, sideband, push status) and its correctness gotchas.
* Credential types: Username/UserPass/Keypair/Agent/Memory; cert validation callback. ([pygit2][18])

---

## R) Submodules

Deep-dive topics:

* Submodule collection ops (add/init/status/update).
* Submodule object surface (open/reload/update/branch/head_id).
* Cache mechanics + complexity notes (why bulk caching exists). ([pygit2][19])

---

## S) Worktrees

Deep-dive topics:

* Add/list/lookup worktrees.
* Prune semantics + “is_prunable” checks. ([pygit2][20])

---

## T) Mailmap (identity normalization)

Deep-dive topics:

* Constructing mailmaps (buffer/repo), resolving identities/signatures, precedence rules. ([pygit2][21])

---

## U) Filters (custom blob filters implemented in Python)

Deep-dive topics:

* Filter lifecycle (check/write/close), clean vs smudge directionality.
* Register/unregister and **thread-safety** constraints on the registry.
* Attribute-driven filter activation and chaining. ([pygit2][22])

---

## V) Packing (packbuilder + multithreading)

Deep-dive topics:

* Repository.pack and PackBuilder (add/add_recur/write).
* Thread controls and operational use cases (e.g., speeding network push pack creation). ([pygit2][23])

---

## W) Storage extensibility: ODB/RefDB backends

Deep-dive topics:

* Implementing custom `OdbBackend` and `RefdbBackend`.
* Built-in loose/pack backends and the filesystem refdb backend.
* “Virtual git” patterns (embedding Git objects/refs in non-filesystem stores). ([pygit2][24])

---

If you want a natural deep-dive order that matches “how you’d build a real system,” start with **B/C/D → E → F/G → K/L → N/J/O → P → Q → R/S → W/U/V**.

[1]: https://pypi.org/project/pygit2/?utm_source=chatgpt.com "pygit2"
[2]: https://www.pygit2.org/install.html "Installation — pygit2 1.19.1 documentation"
[3]: https://www.pygit2.org/general.html "General — pygit2 1.19.1 documentation"
[4]: https://www.pygit2.org/settings.html "Settings — pygit2 1.19.1 documentation"
[5]: https://www.pygit2.org/features.html "Feature detection — pygit2 1.19.1 documentation"
[6]: https://www.pygit2.org/repository.html "Repository — pygit2 1.19.1 documentation"
[7]: https://www.pygit2.org/oid.html "Object IDs — pygit2 1.19.1 documentation"
[8]: https://www.pygit2.org/objects.html "Objects — pygit2 1.19.1 documentation"
[9]: https://www.pygit2.org/revparse.html "Revision parsing — pygit2 1.19.1 documentation"
[10]: https://www.pygit2.org/commit_log.html "Commit log — pygit2 1.19.1 documentation"
[11]: https://www.pygit2.org/blame.html "Blame — pygit2 1.19.1 documentation"
[12]: https://www.pygit2.org/references.html "References — pygit2 1.19.1 documentation"
[13]: https://www.pygit2.org/transactions.html "Reference Transactions — pygit2 1.19.1 documentation"
[14]: https://www.pygit2.org/branches.html "Branches — pygit2 1.19.1 documentation"
[15]: https://www.pygit2.org/index_file.html "Index file & Working copy — pygit2 1.19.1 documentation"
[16]: https://www.pygit2.org/diff.html "Diff — pygit2 1.19.1 documentation"
[17]: https://www.pygit2.org/merge.html "Merge & Cherrypick — pygit2 1.19.1 documentation"
[18]: https://www.pygit2.org/remotes.html "Remotes — pygit2 1.19.1 documentation"
[19]: https://www.pygit2.org/submodule.html "Submodules — pygit2 1.19.1 documentation"
[20]: https://www.pygit2.org/worktree.html "Worktrees — pygit2 1.19.1 documentation"
[21]: https://www.pygit2.org/mailmap.html "Mailmap — pygit2 1.19.1 documentation"
[22]: https://www.pygit2.org/filters.html "Filters — pygit2 1.19.1 documentation"
[23]: https://www.pygit2.org/packing.html "Packing — pygit2 1.19.1 documentation"
[24]: https://www.pygit2.org/backends.html "Backends — pygit2 1.19.1 documentation"

## B) General runtime primitives: version, exceptions, global options

### B1) Version introspection: “what libgit2 am I actually running?”

pygit2 exposes **libgit2 version constants** so you can assert compatibility at runtime (useful when wheels bundle libgit2 vs when you link against a system libgit2). The documented constants are:

* `LIBGIT2_VER_MAJOR`, `LIBGIT2_VER_MINOR`, `LIBGIT2_VER_REVISION`
* `LIBGIT2_VER` (tuple)
* `LIBGIT2_VERSION` (string) ([pygit2][1])

Practical pattern (fail-fast on “known-bad” feature combos / ABI drift):

```python
import pygit2

print("libgit2:", pygit2.LIBGIT2_VERSION, pygit2.LIBGIT2_VER)
# gate behavior / diagnostics on version
if pygit2.LIBGIT2_VER < (1, 8, 0):
    raise RuntimeError("This app expects libgit2 >= 1.8.0")
```

Also keep in mind pygit2’s **own** version line moves with libgit2: the install docs show pygit2 1.19 targets libgit2 1.9.x and warns that minor releases may include breaking changes. ([pygit2][2])

---

### B2) Exceptions: what you should catch and what they mean

pygit2 documents three top-level exception types:

* `pygit2.GitError` (base: `Exception`)
* `pygit2.AlreadyExistsError` (base: `ValueError`) — creating something that already exists (e.g., a ref)
* `pygit2.InvalidSpecError` (base: `ValueError`) — invalid input specs like malformed reference names ([pygit2][1])

Operationally, most “Git went wrong” failures surface as `GitError`, so the pattern is:

* Catch **specific** exceptions where you can (AlreadyExists / InvalidSpec).
* Otherwise catch `GitError`, and treat it as a “libgit2 operation failed” envelope (clone/fetch/checkout/index writes, etc.). ([pygit2][1])

---

### B3) Global options: `pygit2.option(...)` as the low-level control plane

`pygit2.option()` is the **direct binding to libgit2 global options**, and it’s the most important “process-level knob surface” you have. The docs show a typed API that uses `Option.*` enums (and also describes the legacy-style `GIT_OPT_*` names in the parameter docs). ([pygit2][1])

The key option families you’ll actually care about:

**Memory mapping / pack access**

* `GET/SET_MWINDOW_SIZE`
* `GET/SET_MWINDOW_MAPPED_LIMIT`
* `GET/SET_MWINDOW_FILE_LIMIT` ([pygit2][1])

**Caching**

* `ENABLE_CACHING`
* `GET_CACHED_MEMORY`
* `SET_CACHE_MAX_SIZE`
* `SET_CACHE_OBJECT_LIMIT` ([pygit2][1])

**Security / repo ownership guard**

* `GET_OWNER_VALIDATION`
* `SET_OWNER_VALIDATION` (docs note default is to validate ownership) ([pygit2][1])

**Network identity + timeouts**

* `GET/SET_USER_AGENT`
* `GET/SET_USER_AGENT_PRODUCT`
* `GET/SET_SERVER_CONNECT_TIMEOUT`
* `GET/SET_SERVER_TIMEOUT` ([pygit2][1])

**TLS / HTTPS**

* `SET_SSL_CERT_LOCATIONS`
* `SET_SSL_CIPHERS`
* `ADD_SSL_X509_CERT` ([pygit2][1])

**Config discovery**

* `GET/SET_SEARCH_PATH` (by `ConfigLevel`)
* `GET/SET_HOMEDIR` ([pygit2][1])

**Repo initialization defaults**

* `GET/SET_TEMPLATE_PATH` ([pygit2][1])

**Pack / storage tuning**

* `GET/SET_PACK_MAX_OBJECTS`
* `SET_ODB_PACKED_PRIORITY`, `SET_ODB_LOOSE_PRIORITY` ([pygit2][1])

**Windows-specific**

* `GET/SET_WINDOWS_SHAREMODE` ([pygit2][1])

Minimal “early in process startup” tuning example:

```python
import pygit2
from pygit2 import Option  # depending on your build, this may also be pygit2.enums.Option

# Keep memory mapping under control on large repos / constrained containers
pygit2.option(Option.SET_MWINDOW_SIZE, 32 * 1024 * 1024)           # 32MB windows
pygit2.option(Option.SET_MWINDOW_FILE_LIMIT, 1024)                  # cap mapped files

# Tighten network behavior for services (milliseconds)
pygit2.option(Option.SET_SERVER_CONNECT_TIMEOUT, 5_000)
pygit2.option(Option.SET_SERVER_TIMEOUT, 30_000)

# Set a recognizable UA for outbound fetch/push
pygit2.option(Option.SET_USER_AGENT_PRODUCT, "myapp")
```

**Gotcha (ownership validation):** if you run on mounted volumes / CI agents where repo dirs are owned by another user, libgit2’s ownership validation can throw “repository path is not owned by current user”. That guard is exactly what `SET_OWNER_VALIDATION` / `GET_OWNER_VALIDATION` controls. ([pygit2][1])

---

## C) Configuration & tuning control plane: `pygit2.Settings`

`pygit2.Settings` is the “ergonomic wrapper” around the same global option surface: **library-wide settings interface** with method/property accessors. ([pygit2][3])

### C1) Caching (performance vs memory)

* `enable_caching(True/False)` toggles caching globally (note: caches are repo-specific, so disabling doesn’t instantly purge; they clear on next update attempt). ([pygit2][3])
* `cache_max_size(value)` sets the global soft cap; docs say default is **256MB** and eviction becomes aggressive when exceeded. ([pygit2][3])
* `cache_object_limit(object_type, value)` controls per-object-type eligibility; docs note defaults: **blobs not cached** (`0`) and **commit/tree/tag 4k**. ([pygit2][3])
* `cached_memory -> (current_bytes, max_bytes)` lets you introspect live cache usage. ([pygit2][3])

Practical pattern for large repo analytics: keep blob caching off (default) and raise commit/tree cache if you do lots of repeated history walks.

---

### C2) mmap windowing (big repo scalability knobs)

* `mwindow_size`, `mwindow_mapped_limit`, `mwindow_file_limit` let you bound how libgit2 mmaps packfiles (size per window, total mapped memory, number of mapped files). ([pygit2][3])

These are the first knobs to reach for when:

* you see high RSS from pack access,
* you hit file descriptor pressure,
* you’re scanning many repos concurrently.

---

### C3) Correctness / safety toggles (tradeoffs you should decide explicitly)

* `enable_strict_hash_verification`
* `enable_strict_object_creation`
* `enable_strict_symbolic_ref_creation`
* `enable_unsaved_index_safety`
* `enable_fsync_gitdir` ([pygit2][3])

These controls matter in “Git as a database” workloads (you’re creating commits/trees/refs programmatically, not just reading). For read-only scanning, you usually leave defaults alone; for mutation-heavy systems, decide whether you prioritize integrity checks and durability vs throughput.

---

### C4) Networking control surface (service-hardening)

* `server_connect_timeout` and `server_timeout` are in **milliseconds**. ([pygit2][3])
* `user_agent` and `user_agent_product` let you set consistent outbound identity. ([pygit2][3])
* `enable_http_expect_continue` is explicitly for “large pushes”. ([pygit2][3])

---

### C5) TLS/HTTPS configuration (CA + cipher policy)

* `set_ssl_cert_locations(cert_file, cert_dir)` sets CA locations (one can be null, not both). ([pygit2][3])
* `set_ssl_ciphers(ciphers)` sets cipher policy. ([pygit2][3])
* `ssl_cert_file` / `ssl_cert_dir` expose the effective paths. ([pygit2][3])

If you’re running in stripped containers, this is how you make HTTPS fetch/clone reliable without depending on host CA paths.

---

### C6) Repo ownership validation (security vs “real life CI”)

* `owner_validation` gets/sets repository directory ownership validation. ([pygit2][3])

If you see ownership-related `GitError`s on mounted volumes, this is the one knob you’d intentionally change (and document in your threat model). ([GitHub][4])

---

### C7) Config discovery: `homedir` + `search_path`

* `homedir` lets you override the home directory used for config discovery. ([pygit2][3])
* `search_path` exposes config search paths; docs note it behaves like an array indexed by `ConfigLevel`, and the local search path can’t be changed. ([pygit2][3])

This interacts with the config system: repo configs default to include global/system configs (when available), and you can also load global/system/xdg configs explicitly via `Config.get_global_config()`, `get_system_config()`, `get_xdg_config()`. ([pygit2][5])

---

### C8) Storage & pack behavior: ODB priorities, pack limits, remote FS

* `pack_max_objects` controls max objects per pack. ([pygit2][3])
* `set_odb_packed_priority` / `set_odb_loose_priority` set backend priorities (docs give defaults: packed=1, loose=2). ([pygit2][3])
* `disable_pack_keep_file_checks(True)` can help on remote filesystems by skipping `.keep` existence checks. ([pygit2][3])

---

## D) Feature detection: `pygit2.features` (capability gating)

### D1) What it is

`pygit2.features` is a bitmask of `enums.Feature` flags indicating what the **current libgit2 build supports**. ([pygit2][6])

In libgit2, those feature flags correspond to compile-time capabilities including:

* `GIT_FEATURE_THREADS` (thread-aware build)
* `GIT_FEATURE_HTTPS` (TLS implementation linked)
* `GIT_FEATURE_SSH` (libssh2 linked)
* `GIT_FEATURE_NSEC` (sub-second mtime support) ([libgit2][7])

### D2) Why you should gate on it (real failures you avoid)

**Transport support is the big one.** pygit2’s install docs explicitly warn: *Windows wheels don’t have SSH support*, and optional dependencies are needed to support HTTPS/SSH when building from source. ([pygit2][2])
So you can proactively decide:

* “If SSH isn’t supported, force HTTPS URLs (or fail with a clear message).”
* “If HTTPS isn’t supported, you can’t use `https://` remotes unless you rebuild / change distribution.”

### D3) Practical capability probe (drop into every CLI / service startup)

```python
import pygit2

def libgit2_caps():
    f = pygit2.features
    return {
        "threads": bool(f & pygit2.GIT_FEATURE_THREADS),
        "https":   bool(f & pygit2.GIT_FEATURE_HTTPS),
        "ssh":     bool(f & pygit2.GIT_FEATURE_SSH),
        "nsec":    bool(f & pygit2.GIT_FEATURE_NSEC),
    }

print("caps:", libgit2_caps())
```

(Those `GIT_FEATURE_*` names are the libgit2 feature flags described in the upstream docs. ([libgit2][7]))

### D4) Threading: “flag says yes” vs “should I rely on it?”

Even if `GIT_FEATURE_THREADS` is set, pygit2’s own install documentation warns that **threaded builds are experimental and not recommended for production**. ([pygit2][2])
For production systems, the conservative posture is:

* treat libgit2 as *best-effort threadsafe for some reads*,
* but prefer **process-level parallelism** (multi-process workers) for heavy scanning/mutation unless you’ve validated your exact libgit2 build + workload.

---

If you want the next deep dive after this trio, the most natural continuation is **E) Repository core lifecycle + state machine**, because it pulls in the knobs above (owner validation, search path, caching) and gives you the “center” object that everything else hangs off.

[1]: https://www.pygit2.org/general.html "General — pygit2 1.19.1 documentation"
[2]: https://www.pygit2.org/install.html "Installation — pygit2 1.19.1 documentation"
[3]: https://www.pygit2.org/settings.html "Settings — pygit2 1.19.1 documentation"
[4]: https://github.com/libgit2/pygit2/issues/1156?utm_source=chatgpt.com "pygit2 does not respect a global setting of safe.directory='*'"
[5]: https://www.pygit2.org/config.html "Configuration files — pygit2 1.19.1 documentation"
[6]: https://www.pygit2.org/features.html "Feature detection — pygit2 1.19.1 documentation"
[7]: https://libgit2.org/docs/reference/v1.4.5/common/git_feature_t.html "git_feature_t (libgit2 v1.4.5)"

## E) Repository core lifecycle and state machine

The `Repository` object is the **center of gravity** in pygit2: it’s the handle that binds together (a) object storage (ODB), (b) reference storage (RefDB), (c) the working directory (optional, for non-bare repos), and (d) “operation in progress” state (merge/rebase/cherrypick/etc). The top-level lifecycle entrypoints are `init_repository`, `clone_repository`, `discover_repository`, and `Repository(...)` itself.

---

### E1) Lifecycle entrypoints: create, clone, discover, open

#### 1) Create: `pygit2.init_repository(...)`

`init_repository` creates a repository on disk and returns a `Repository`. It supports:

* `bare` (no working copy),
* `flags` (e.g., `MKPATH` default; `NO_REINIT`, `NO_DOTGIT_DIR`, etc.),
* `mode` (shared repo modes),
* and “init-ext” fields like `workdir_path`, `description`, `template_path`, `initial_head`, `origin_url`.

One important footnote: if a repo already exists at `path`, it *may* open successfully, but the docs explicitly tell you **not to rely on that** and to use the `Repository` constructor when you intend “open existing”.

#### 2) Clone: `pygit2.clone_repository(url, path, ...)`

`clone_repository` clones from `url` into `path`, returning a `Repository`. Key lifecycle knobs:

* `bare` clone,
* `checkout_branch` (otherwise remote default branch),
* `callbacks` (`RemoteCallbacks` instance),
* `depth` for shallow clone (`>0` truncates history; `0` is full history),
* `proxy` (`None`, `True` for autodetect, or explicit proxy URL),
* plus **two “factory callbacks”**: `repository=(path,bare)->Repository` and `remote=(repo,name,url)->Remote` for customizing the created objects.

#### 3) Discover: `pygit2.discover_repository(path, across_fs=False, ceiling_dirs=...) -> str | None`

`discover_repository` walks upward from `path` looking for a git repository and returns the **repository path string** (not a `Repository`), or `None` if not found. Typical pattern is discover → open:

```python
repo_path = pygit2.discover_repository(os.getcwd())
repo = pygit2.Repository(repo_path)
```

#### 4) Open: `pygit2.Repository(path, flags=...)`

The constructor is the canonical “open existing repo” entrypoint. It accepts optional `RepositoryOpenFlag`s:

* `NO_SEARCH`, `CROSS_FS`, `BARE`, `NO_DOTGIT`, `FROM_ENV`.

Also: `Repository()` with **no arguments** creates a repo with **no backends** (no ODB/RefDB). That’s explicitly for building repos with custom backends; most operations are undefined until you attach an ODB and RefDB via `set_odb()` and `set_refdb()`.

---

### E2) Repository identity surface: “where am I?” and “what kind of repo is this?”

These are the minimal “repo classification” fields you’ll want to interrogate early in any tool:

* `repo.is_bare` → bare repository (no working directory)
* `repo.workdir` → normalized working dir path, or `None` if bare
* `repo.path` → normalized path to the git repository directory
* `repo.is_empty` → empty repository (no commits yet)
* `repo.is_shallow` → shallow clone (history truncated)

In practice:

* “scanner mode” often works fine with bare repos (faster / safer), but any working-tree operations require `workdir != None`.
* shallow repos are a correctness boundary for anything that assumes full history.

---

### E3) ODB/RefDB backends: the *actual* “database handles”

#### `repo.odb` and `pygit2.Odb`

`repo.odb` returns the object database handle.
`Odb` supports:

* `add_backend(backend, priority)` for custom object backends,
* `add_disk_alternate(path)` to add alternate object stores (read-only),
* `exists(oid)`, `read(oid)`, `write(type,data)`, and iterating `backends`.

This matters for:

* “virtual git” patterns (custom object storage),
* performance tuning (alternates / pack reuse),
* and aggressive read-heavy tooling where you might want to explicitly reason about storage layout.

#### `pygit2.Refdb`

The reference database (`Refdb`) controls how refs are stored and optimized. It exposes:

* `compress()` (“pack refs” / optimize refs, backend-dependent),
* `Refdb.new(repo)` for a refdb with no backend,
* `Refdb.open(repo)` to open the repo’s refdb.

#### Attaching backends explicitly

For custom repositories (`Repository()` with no path), the doc calls out `repo.set_odb(odb)` and `repo.set_refdb(refdb)` as the low-level way to make the repo usable.

---

### E4) Resource lifecycle: `free()` vs “GC eventually”

`repo.free()` releases handles to the Git database without deallocating the repository object. This is a lever when you’re scanning many repositories in one process and want to drop underlying resources deterministically.

(How much this matters in *your* workload is tied to the global caching + mmap knobs from sections B/C; `free()` helps you “let go” of repo internals, while Settings/options control how expensive those internals were to begin with.)

---

## E5) The repository state machine: “am I mid-operation?”

libgit2 tracks whether the repository is in the middle of an operation. pygit2 exposes the cleanup hook directly (`state_cleanup()`), and (in practice) you should treat “operation state” as part of correctness for any tool doing merges/cherrypicks/resets/checkouts.

### E5.1) What states exist? (libgit2 canonical list)

libgit2 enumerates these possible “ongoing operation” states:
`NONE`, `MERGE`, `REVERT`, `REVERT_SEQUENCE`, `CHERRYPICK`, `CHERRYPICK_SEQUENCE`, `BISECT`, `REBASE`, `REBASE_INTERACTIVE`, `REBASE_MERGE`, `APPLY_MAILBOX`, `APPLY_MAILBOX_OR_REBASE`.

libgit2 also provides a query (`git_repository_state`) described as: determining whether an operation (merge, cherry-pick, etc.) is in progress.

### E5.2) The one pygit2 method you must understand: `Repository.state_cleanup()`

pygit2’s `repo.state_cleanup()` removes metadata associated with an ongoing command (merge/revert/cherrypick/etc.), e.g., `MERGE_HEAD`, `MERGE_MSG`, and related files.

This maps directly onto libgit2’s `git_repository_state_cleanup(...)` contract.

### E5.3) Concrete pygit2 behavior you’ll trip over: cherrypick leaves “cherrypicking mode”

The merge/cherrypick docs call this out explicitly:

> After a successful cherrypick you have to run `Repository.state_cleanup()` to get the repository out of cherrypicking mode.

So any “porcelain-ish” workflow you build around `repo.cherrypick(...)` should end with:

1. inspect/resolve index conflicts (if any),
2. commit if appropriate,
3. **call `repo.state_cleanup()`**.

### E5.4) Why this is “state machine” and not “just a flag”

Because the state is backed by **on-disk metadata** (e.g., MERGE_* files), it affects:

* subsequent operations (some commands refuse to run mid-merge),
* user-facing tooling (`git status` equivalent reporting),
* and reproducibility (if your pipeline aborts mid-op, you may need cleanup / rollback logic).

---

## E6) Open/discovery flags as part of the “state machine boundary”

At the repo-open boundary, you can constrain behavior with `RepositoryOpenFlag`s like `NO_SEARCH`, `CROSS_FS`, `NO_DOTGIT`, and `FROM_ENV`.
And at the discovery boundary, you can decide whether discovery crosses filesystem boundaries (`across_fs`) and where to stop (`ceiling_dirs`).

For production tooling, these are correctness knobs:

* `CROSS_FS` / `across_fs` can change which repo you “find” when scanning mount points.
* `NO_SEARCH` is a safety rail when you require an explicit `.git` path and don’t want “walk up and accidentally open a parent repo”.

---

### The natural “next” deep dive after E

If we keep following the “repo is the center” path, the next chapter should be **K) References + reflogs + HEAD semantics**, because (1) state machine cleanup is ref-adjacent in real workflows and (2) refs are the canonical way you move the repository’s identity across time.

## F) Object IDs (OIDs) and identity discipline

### F1) The three representations (and why you should be strict about them)

In pygit2, an OID is the **SHA-1 hash** of a Git object and is **20 bytes**. pygit2 treats “OID” as the primary key into Git’s content-addressed store. It supports three representations: **raw bytes**, **hex string**, and the **`Oid` object**. ([PyGit2][1])

**Raw OID (`bytes`, length 20)**

* Python `bytes` of length 20.
* In pygit2 docs: *this form can only be used to create an `Oid` object*. ([PyGit2][1])

**Hex OID (`str`, length 40)**

* 40 hex chars.
* pygit2 APIs “directly accept hex oids everywhere” (but see the “prefix discipline” section below). ([PyGit2][1])

**`pygit2.Oid` object**

* Preferred internal representation: pygit2 “always returns, and accepts, `Oid` objects”, using hex mainly for user interaction. ([PyGit2][1])
* Constructed from raw or hex, but **not both**: `Oid(raw=...)` or `Oid(hex=...)`. ([PyGit2][1])
* Convert out via:

  * `oid.raw` → raw 20-byte form
  * `str(oid)` → hex form ([PyGit2][1])

```python
from binascii import unhexlify
from pygit2 import Oid

h = "cff3ceaefc955f0dbe1957017db181bc49913781"
oid_hex = Oid(hex=h)
oid_raw = Oid(raw=unhexlify(h))

assert oid_hex.raw == oid_raw.raw
assert str(oid_hex) == h
```

### F2) OID semantics: comparison, hashing, nullness, and constants

`Oid` supports:

* **Rich comparisons** (including ordering),
* `hash(oid)` so it can be used as a dict key,
* `bool(oid)` which is **False for the null SHA-1 (all zeros)**,
* `str(oid)` returning hex. ([PyGit2][1])

Key constants you’ll use for “contract enforcement”:

* `GIT_OID_RAWSZ`, `GIT_OID_HEXSZ`, `GIT_OID_HEX_ZERO`, `GIT_OID_MINPREFIXLEN`. ([PyGit2][1])

**Identity discipline note:** ordering comparisons are a byte/lexicographic artifact of the hash value. Treat `<` / `>` as “stable sort key” only, not “time order” or semantic order.

### F3) Prefix resolution, ambiguity, and why “short IDs” are *not stable*

There are two separate “short ID” concepts:

1. **A generated short id for display**: `Object.short_id`

* pygit2: `Object.short_id` returns an **unambiguous abbreviated hex OID** string. ([PyGit2][2])
* libgit2 clarifies how that abbreviation is computed: start at `core.abbrev` (default 7), extend until unambiguous — but *only unambiguous “until new objects are added”*. ([libgit2][3])
  → That last clause is the killer: **persisting short ids is a correctness bug** for long-lived caches / indexes.

2. **Lookup by prefix** (user input like `bbb78a9…`)

* libgit2’s prefix lookup requires `len >= GIT_OID_MINPREFIXLEN` and the prefix must identify a **unique** object, otherwise it fails. ([libgit2][4])
* pygit2 gives you a high-level resolver designed for “ref-ish” / shorthand inputs:
  `repo.resolve_refish(refish)` → `(Commit, Reference)` and examples explicitly include a short hex like `'bbb78a9'`. ([PyGit2][5])
* `repo.revparse_single(revision)` is the broader “gitrevisions” parser and can resolve many `<rev>` forms (eg `HEAD^`). ([PyGit2][6])

A robust “accept anything a human might type” resolver typically looks like:

```python
import pygit2

def resolve_to_commit(repo: pygit2.Repository, refish: str) -> pygit2.Commit:
    # Handles branch-ish, tag-ish, remote-tracking-ish, and short OID-ish forms.
    commit, ref = repo.resolve_refish(refish)
    return commit

commit = resolve_to_commit(repo, "bbb78a9")      # short id
commit = resolve_to_commit(repo, "origin/main")  # ref-ish
commit = resolve_to_commit(repo, "v1.2.3")       # tag-ish
```

### F4) “Stable identity” patterns you should standardize early

For “tooling that produces artifacts” (indexes, graphs, metadata tables), pick a canonical storage form and don’t deviate:

**Recommended canonical forms**

* **In-memory**: use `Oid` objects (they’re hashable and comparable). ([PyGit2][1])
* **On disk / wire**:

  * Store **hex** for readability (40 chars), or
  * Store **raw 20 bytes** for compactness (plus a clear encoding in your schema).

**Always disambiguate cross-repo keys**
OID is a content hash, so identical content can share OIDs across repos; but your *semantic key* in pipelines is almost always `(repo_id, oid)`.

**When you must accept prefixes**

* Enforce `len(prefix) >= GIT_OID_MINPREFIXLEN`. ([PyGit2][1])
* Prefer `resolve_refish` / `revparse_single` over guessing. ([PyGit2][5])
* Never persist “short_id” as durable identity. ([libgit2][3])

---

## G) Git object model (ODB): blobs, trees, commits, tags

### G0) Core invariants: objects are immutable, created via repo APIs

pygit2 exposes the four Git object types — `Blob`, `Tree`, `Commit`, `Tag` — all inheriting from base `Object`. Objects are **immutable** and you **cannot instantiate** them directly (no constructors); new objects are created via repository APIs (create_blob, TreeBuilder.write, create_commit, create_tag, etc.). ([PyGit2][2])

### G1) Object lookup: `Repository.get`, `__getitem__`, `__contains__`

`Repository` implements a subset of the mapping interface for object retrieval: ([PyGit2][2])

* `repo.get(key, default=None) -> Object | None`

  * returns `default` if not found
  * `key` can be `Oid` or hex `str` ([PyGit2][2])
* `repo[key] -> Object`

  * raises `KeyError` if not found
  * `key` can be `Oid` or hex `str` ([PyGit2][2])
* `key in repo -> bool`

  * membership test for object existence ([PyGit2][2])

```python
obj = repo.get(str(repo.head.target))   # safe: full hex
if obj is None:
    ...
commit = repo[repo.head.target]         # Oid
assert repo.head.target in repo
```

**Advanced:** you can also drop into the raw object database:

* `repo.odb.exists(oid)`
* `repo.odb.read(oid) -> (type, data, size)`
* `repo.odb.write(type, data) -> Oid` ([PyGit2][7])

### G2) The base `Object` API: identity, typing, raw bytes, peeling

Base `Object` exposes: ([PyGit2][2])

* `obj.id -> Oid`
* `obj.type -> enums.ObjectType` and `obj.type_str -> str`
* `obj.read_raw() -> bytes` (raw object content)
* `obj.short_id -> str` (unambiguous abbreviated hex)
* `obj.name` / `obj.filemode` **only set when the object is reached through a tree entry** (otherwise `None`) ([PyGit2][2])
* `__eq__`, `__hash__` (so objects can be used in sets / dicts) ([PyGit2][2])

**Peeling (`obj.peel(target_type)`)**
This is the core “object model navigation” primitive:

* If `target_type is None`: peel until the **type changes**. A tag peels until it’s no longer a tag; a commit peels to its tree; anything else raises `InvalidSpecError`. ([PyGit2][2])
* This mirrors libgit2’s behavior (including the “commit → tree” and “tag → non-tag” rules). ([libgit2][8])

### G3) Blobs: zero-copy access, diffing, creation, and streaming

#### Data access and memory behavior

A `Blob` is raw bytes (“Git file contents”). pygit2 emphasizes a performance detail:

* Blobs implement the **buffer interface**, so `memoryview(blob)` can access data without copying. ([PyGit2][2])
* `blob.data` is the full blob contents as `bytes` (same as `blob.read_raw()`). ([PyGit2][2])
* `blob.size`, `blob.is_binary` are exposed. ([PyGit2][2])

#### Diffing blobs directly

* `blob.diff(other_blob, flags=..., old_as_path=..., new_as_path=..., context_lines=..., interhunk_lines=...) -> Patch` ([PyGit2][2])
* `blob.diff_to_buffer(buffer, flags=..., old_as_path=..., buffer_as_path=...) -> Patch` ([PyGit2][2])

These produce a `Patch` directly (useful for “blob-to-blob” comparisons without trees/index/workdir). ([PyGit2][2])

#### Creating blobs and “hash without writing”

Repository methods for blob creation:

* `repo.create_blob(data: bytes) -> Oid`
* `repo.create_blob_fromdisk(path) -> Oid`
* `repo.create_blob_fromiobase(io.IOBase) -> Oid`
* `repo.create_blob_fromworkdir(path_relative_to_workdir) -> Oid` ([PyGit2][2])

And two “compute-only” helpers:

* `pygit2.hash(data: bytes) -> Oid` (does **not** write to ODB)
* `pygit2.hashfile(path) -> Oid` (does **not** write to ODB) ([PyGit2][2])

This matters for pipelines: you can precompute stable content IDs for *candidate* files without mutating the repo.

#### Streaming blob content with `BlobIO` (raw vs filtered)

`Blob.data` / `read_raw()` read the entire blob into memory and return raw bytes (no checkout filters). `BlobIO` gives you a streaming IO wrapper: ([PyGit2][2])

* `BlobIO(blob, as_path=None, flags=BlobFilter.CHECK_FOR_BINARY, commit_id=None)`
* Implements `io.BufferedReader`
* Can stream **raw** (default) or **filtered** content (by providing `as_path`, so the checkout filters that would apply to that path can be applied). ([PyGit2][2])

```python
from pygit2 import BlobIO

with BlobIO(blob) as f:
    for chunk in iter(lambda: f.read(1024), b""):
        ...

# filtered content (as if checked out to that filename)
with BlobIO(blob, as_path="my_file.ext") as f:
    filtered = f.read()
```

### G4) Trees: traversal, path addressing, tree builders, and diffs

#### Tree access model (mapping-ish + Pathlib-ish)

At the libgit2 level, a tree is a **sorted** set of entries; pygit2 makes entries feel like objects. Trees can be iterated and partially implement sequence/mapping interfaces. ([PyGit2][2])

* `tree[name]` → object by entry name (raises `KeyError` if absent) ([PyGit2][2])
* `tree / 'path' / 'deeper' / 'file'` → nested path navigation via `/` operator ([PyGit2][2])
* `name in tree`, `len(tree)`, `for obj in tree` ([PyGit2][2])

#### Tree ↔ index/workdir/tree diffs

Tree diffs are “snapshot diffs”:

* `tree.diff_to_index(index, flags=DiffOption.NORMAL, context_lines=3, interhunk_lines=0) -> Diff` ([PyGit2][2])
* `tree.diff_to_tree(tree2, flags=..., context_lines=..., interhunk_lines=..., swap=False) -> Diff` ([PyGit2][2])
* `tree.diff_to_workdir(...) -> Diff` (listed alongside the others) ([PyGit2][2])

This is your canonical primitive for:

* “What changed between commit X and the index?”
* “What changed between two commits?”
* “What changed between commit and working tree?” ([PyGit2][2])

#### Creating trees with `TreeBuilder` (write returns a tree OID)

Pygit2’s high-leverage constructor is `repo.TreeBuilder([tree])`, which returns a mutable builder you can write back to the ODB. ([PyGit2][2])

`TreeBuilder` methods:

* `clear()`
* `get(name) -> Object | None`
* `insert(name, oid, attr: FileMode)` (insert/replace)
* `remove(name)`
* `write() -> Oid` (writes a tree object into the repo) ([PyGit2][2])

Valid `FileMode` values include: `BLOB`, `BLOB_EXECUTABLE`, `TREE`, `LINK`, `COMMIT`. ([PyGit2][2])
That last one (`COMMIT`) is how Git stores **submodules** (tree entry points to a commit object).

```python
from pygit2 import FileMode

# start from empty tree
tb = repo.TreeBuilder()
readme_oid = repo.create_blob(b"# hello\n")
tb.insert("README.md", readme_oid, FileMode.BLOB)
tree_oid = tb.write()
```

#### Stable traversal ordering (when you care about deterministic output)

If you need “Git’s sort order” for tree entries, pygit2 exposes `tree_entry_cmp(a, b)` which wraps libgit2’s tree-entry comparison (only meaningful for objects obtained through a tree). ([PyGit2][7])

### G5) Commits: structure, signatures, trailers, parents, and signed commit flows

#### Commit surface (read path)

Key commit attributes: author/committer, time/offset, message/encoding/raw_message, trailers, parents, and the attached tree. ([PyGit2][2])

* `commit.author`, `commit.committer` are `Signature` objects ([PyGit2][2])
* `commit.parents` and `commit.parent_ids` give object references vs ids ([PyGit2][2])
* `commit.tree` / `commit.tree_id` give the snapshot tree ([PyGit2][2])
* `commit.message_trailers` returns trailers parsed into a dict (eg `Bug: 1234`) ([PyGit2][2])
  Under the hood, libgit2 defines trailers as key/value pairs in the **last paragraph** of a message, excluding patches/conflicts. ([libgit2][9])
* `commit.gpg_signature` returns a tuple: `(signature, signed_payload)` ([PyGit2][2])
  This aligns with libgit2’s `git_commit_extract_signature`: it extracts the signature block and the “signed data” as commit contents **minus the signature block**. ([libgit2][10])

#### Signature objects

`Signature` exposes name/email/time/offset (and raw byte forms) and can be compared for equality/inequality. ([PyGit2][2])

#### Creating commits (write path)

The low-level primitive is:
`repo.create_commit(reference_name, author, committer, message, tree, parents[, encoding]) -> Oid` ([PyGit2][2])

The official recipe shows the canonical flow:

* stage changes in the index,
* `tree = index.write_tree()`,
* `repo.create_commit(...)` with `ref="HEAD"` for the initial commit, then `ref=repo.head.name` with `parents=[repo.head.target]` for subsequent commits. ([PyGit2][11])

#### Creating *signed* commits (pygit2-native)

pygit2 exposes a two-step signed-commit workflow:

1. `commit_string = repo.create_commit_string(author, committer, message, tree, parents)`
2. sign that string externally (your GPG tool), then
3. `commit_oid = repo.create_commit_with_signature(commit_string, signature_text)` and update HEAD. ([PyGit2][11])

The recipe also shows the expected ASCII-armored PGP signature block format. ([PyGit2][11])

### G6) Tags: tag objects vs lightweight refs; creation flows

#### Two tag “shapes” in Git (and why your tooling must model both)

Git supports:

* **Lightweight tags**: “just a pointer to a specific commit” (a ref that doesn’t move)
* **Annotated tags**: “stored as full objects” with metadata (tagger, message, date, optional signature) ([Git][12])

libgit2 makes the implementation distinction explicit:

* Annotated tag creation writes a **tag object** *and* creates a ref under `/refs/tags/` pointing to it. ([libgit2][13])
* Lightweight tag creation creates a **direct reference** under `/refs/tags/` pointing to the target object. ([libgit2][14])

#### Tag objects in pygit2 (`Tag`)

A `Tag` object exposes:

* `tag.get_object() -> Object` (retrieve target object)
* `tag.target` (tagged object)
* `tag.name`, `tag.message`, plus raw byte forms
* `tag.tagger` (signature-like identity) ([PyGit2][2])

#### Creating annotated tags via `repo.create_tag`

`repo.create_tag(name, oid, type, tagger[, message]) -> Oid` creates a new tag object and returns its OID. ([PyGit2][2])
(Operationally, you should expect a `refs/tags/<name>` entry as well — that’s how libgit2 defines tag creation — and you can confirm by inspecting `repo.references`.) ([libgit2][13])

#### Creating lightweight tags via references (`refs/tags/...`)

In pygit2, lightweight tags are naturally expressed as **references under `refs/tags/`**. You can create them with `repo.create_reference` (or `repo.references.create`). The repository docs show examples including tags and even a short OID target. ([PyGit2][7])

```python
# lightweight tag: refs/tags/v1.0 -> commit oid
repo.create_reference("refs/tags/v1.0", repo.head.target)

# or equivalently:
repo.references.create("refs/tags/v1.0", repo.head.target)
```

#### Practical resolution logic: “give me the commit for any tag”

Your “normalize everything to a commit” function can be:

* `commit, ref = repo.resolve_refish("v1.0")` (works for tags and short ids) ([PyGit2][5])
* or: `ref = repo.lookup_reference_dwim("v1.0")` then `ref.peel()` (peels through annotated tag objects to first non-tag target) ([PyGit2][5])

---

If you want the next deep dive in the original sequence, the natural continuation after objects is **K) References + reflogs + notes + HEAD semantics**, because refs/tags vs tag objects (and `resolve_refish`, `lookup_reference_dwim`, `Reference.peel`) are where identity + object model “meet the world” operationally.

[1]: https://www.pygit2.org/oid.html "Object IDs — pygit2 1.19.1 documentation"
[2]: https://www.pygit2.org/objects.html "Objects — pygit2 1.19.1 documentation"
[3]: https://libgit2.org/docs/reference/v1.5.1/object/git_object_short_id.html "git_object_short_id (libgit2 v1.5.1)"
[4]: https://libgit2.org/docs/reference/main/object/git_object_lookup_prefix.html "git_object_lookup_prefix (libgit2 main)"
[5]: https://www.pygit2.org/references.html "References — pygit2 1.19.1 documentation"
[6]: https://www.pygit2.org/revparse.html "Revision parsing — pygit2 1.19.1 documentation"
[7]: https://www.pygit2.org/repository.html "Repository — pygit2 1.19.1 documentation"
[8]: https://libgit2.org/docs/reference/v1.8.4/object/git_object_peel.html?utm_source=chatgpt.com "git_object_peel (libgit2 v1.8.4)"
[9]: https://libgit2.org/docs/reference/main/message/git_message_trailers.html "git_message_trailers (libgit2 main)"
[10]: https://libgit2.org/docs/reference/main/commit/git_commit_extract_signature.html "git_commit_extract_signature (libgit2 main)"
[11]: https://www.pygit2.org/recipes/git-commit.html "git-commit — pygit2 1.19.1 documentation"
[12]: https://git-scm.com/book/en/v2/Git-Basics-Tagging?utm_source=chatgpt.com "Tagging"
[13]: https://libgit2.org/docs/reference/main/tag/git_tag_create.html "git_tag_create (libgit2 main)"
[14]: https://libgit2.org/docs/reference/main/tag/git_tag_create_lightweight.html "git_tag_create_lightweight (libgit2 main)"

## K) References, reflogs, notes, and HEAD semantics

### K0) Mental model: refs are the *mutable identity layer* over immutable objects

Git objects (commits/trees/blobs/tags) are immutable and addressed by OIDs; **references** are the mutable, named pointers that move over time (branches, tags, remotes, HEAD, notes namespaces). In pygit2 this all lives behind `Repository` + its `References` collection plus the `Reference` type itself. ([PyGit2][1])

---

### K1) `Repository` reference surfaces: lookup + DWIM + “ref-ish” resolution

#### `repo.references`: the collection interface

`repo.references` returns a `pygit2.repository.References` object (a repo-bound collection) with common collection operations:

* `__iter__() -> Iterator[str]` (iterate **names**)
* `__getitem__(name) -> Reference` and `get(name) -> Reference|None`
* `create(name, target, force=False) -> Reference`
* `delete(name) -> None`
* `compress() -> None` (packs loose references)
* `iterator(references_return_type=ReferenceFilter.ALL) -> Iterator[Reference]` with filters `ALL`, `BRANCHES`, `TAGS` (and a noted TODO for notes/remotes filtering)
* `objects` property returns a list of `Reference` objects ([PyGit2][1])

That split—**`__iter__` yields names**, while `iterator(...)` yields `Reference` objects—matters when you’re trying to minimize libgit2 object allocations vs doing richer per-ref analysis. ([PyGit2][1])

#### `repo.lookup_reference(name)` vs `repo.lookup_reference_dwim(short_name)`

* `lookup_reference(name)` expects an explicit name (e.g., `refs/heads/main`).
* `lookup_reference_dwim(name)` (“do what I mean”) resolves a short name into a real ref. ([PyGit2][1])

#### `repo.resolve_refish(refish) -> (Commit, Reference|None)`

`resolve_refish` is the high-leverage “accept what humans type” helper: it converts a ref-like short name (branch/tag/remote-tracking name or even a short OID prefix) into a `(Commit, Reference)` pair, and returns `None` for the reference when the input resolves directly to a commit. ([PyGit2][1])

#### Bulk listing as bytes: `raw_listall_references() -> list[bytes]`

If you need raw speed and don’t want `Reference` wrappers, `raw_listall_references()` returns all reference names as bytes. ([PyGit2][1])

#### Validating refnames: `reference_is_valid_name(refname) -> bool`

Use this before creating or renaming refs (especially for user-provided names), including special names like `HEAD`. ([PyGit2][1])

---

### K2) `Reference`: direct vs symbolic, resolve vs peel, rename/delete, and reflog-aware updates

#### Direct vs symbolic references

A `Reference` has a `type` of either `ReferenceType.OID` (direct) or `ReferenceType.SYMBOLIC` (symbolic). ([PyGit2][1])

* **Direct reference**: points directly to an object ID (`Oid`).
* **Symbolic reference**: points to another reference name (string).

You can see this directly in:

* `ref.target`: `Oid` if direct, full ref name string if symbolic
* `ref.raw_target`: `Oid` if direct, **bytes** with full target ref name if symbolic ([PyGit2][1])

Names are exposed in both str/bytes form:

* `ref.name` / `ref.raw_name` (full refname)
* `ref.shorthand` / `ref.raw_shorthand` (human-readable short name) ([PyGit2][1])

> Important discipline: **don’t cache `Reference` objects across mutations** (rename/delete/set_target). The docs explicitly warn that after `ref.delete()` “it will no longer be valid.” Re-lookup by name instead. ([PyGit2][1])

#### `resolve()` vs `peel()`: two different dereference operations

* `ref.resolve()` resolves a **symbolic reference** and returns a **direct reference**. ([PyGit2][1])
* `ref.peel(type=None)` peels the ref to an **object**, recursively dereferencing tags; with `type=None`, it returns the first non-tag object. ([PyGit2][1])

Practical meaning:

* Use `resolve()` when you need “the actual branch ref behind HEAD-like symbolic refs”.
* Use `peel()` when you need “the commit/tree object the ref ultimately refers to (through annotated tags).”

#### Rename and delete

* `ref.rename(new_name)` renames the reference. ([PyGit2][1])
* `ref.delete()` deletes *this* reference object; after deletion it’s invalid. ([PyGit2][1])

For bulk operations, prefer the collection methods (`repo.references.delete(name)` etc.) which keep your flow name-oriented and avoid stale objects. ([PyGit2][1])

#### Updating a ref target and reflog semantics

`ref.set_target(target, message=...)` updates the target and **creates a new reflog entry**; the optional `message` becomes the reflog message. ([PyGit2][1])

At repository level, `repo.create_reference(name, target, force=False, message=None)` will create either a direct or symbolic reference by guessing from the target, and also accepts an optional reflog message. The docs show examples for:

* direct: `refs/heads/foo` -> `repo.head.target`
* symbolic: `refs/tags/foo` -> `refs/heads/master`
* direct by hex/prefix-like string: `refs/tags/foo` -> `'bbb78a9cec580'` ([PyGit2][2])

That `message` hook is one of the big reasons to standardize on `repo.create_reference(...)` / `ref.set_target(..., message=...)` in any automation you want to be explainable later via reflog inspection. ([PyGit2][1])

---

### K3) HEAD semantics: symbolic, detached, and unborn

pygit2 treats `HEAD` as “just another reference,” but provides convenience and state probes:

* `repo.head` is equivalent to `repo.references['HEAD'].resolve()` ([PyGit2][1])
* `repo.head_is_detached`: HEAD points **directly to a commit** instead of a branch ([PyGit2][1])
* `repo.head_is_unborn`: HEAD names a branch that **doesn’t exist yet** (common in newly-initialized repos before the first commit) ([PyGit2][1])

A robust “get current commit if possible” pattern:

```python
import pygit2

def current_commit_or_none(repo: pygit2.Repository):
    if repo.head_is_unborn:
        return None  # no commits yet
    if repo.head_is_detached:
        # HEAD itself points to a commit; resolve_refish('HEAD') is also workable
        return repo[repo.references["HEAD"].target]
    # Normal case: HEAD -> refs/heads/<branch> -> commit
    return repo[repo.head.target]
```

Key correctness boundary: **unborn** is different from **empty**; unborn means HEAD names a branch with no target commit. So any tooling that assumes “there is a HEAD commit” must gate on `head_is_unborn`. ([PyGit2][1])

---

### K4) Reflogs: iteration, entry structure, and what they mean

#### Iterating reflog in pygit2

`Reference.log()` returns an iterator over reflog entries, and `RefLogEntry` exposes:

* `oid_old`, `oid_new`
* `message`
* `committer` ([PyGit2][1])

Example from the docs:

```python
head = repo.references.get('refs/heads/master')
for entry in head.log():
    print(entry.oid_old, entry.oid_new, entry.message)
```

([PyGit2][1])

#### What reflogs represent (Git semantics)

Git’s own docs describe reflogs as recording when the tips of branches and other refs were updated; they’re used for “where this ref used to point,” e.g., `HEAD@{2}` or `master@{one.week.ago}`. The `HEAD` reflog additionally records branch switching. ([Git][3])

This aligns exactly with pygit2’s `set_target(..., message=...)` behavior: it’s not just moving a pointer—it’s producing an auditable local history of pointer movement. ([PyGit2][1])

#### Under the hood (libgit2) and atomicity implications

libgit2 exposes reflog read/write/append/rename/delete operations; reflog writes are done “using an atomic file lock.” This matters operationally because reflog mutation is not “just another ref update”—it has its own locked file update path. ([libgit2][4])

---

### K5) Notes: attaching metadata to objects

#### pygit2 Notes API surface

pygit2 exposes Git notes as first-class operations:

* `repo.create_note(message, author, committer, annotated_id, ref='refs/notes/commits', force=False) -> Oid`
* `repo.lookup_note(annotated_id, ref='refs/notes/commits') -> Note`
* `Note.message`, `Note.id`, `Note.annotated_id`
* `Note.remove(author, committer, ref='refs/notes/commits')` ([PyGit2][1])

The default notes namespace is `refs/notes/commits`, which is the conventional “notes on commits” ref. ([PyGit2][1])

#### Under the hood (libgit2): notes are “metadata attached to an object”

libgit2 defines notes as metadata attached to an object and provides APIs to read, create, remove, and iterate notes, including discovering a repository’s default notes ref. That matches the pygit2 design (namespace ref + annotated object ID + note message). ([libgit2][5])

Practical identity discipline:

* Treat notes like a *separate metadata graph* keyed by `(notes_ref_namespace, annotated_oid)`.
* If you’re building a “code intelligence” system, notes are a convenient place to attach **local-only annotations** without rewriting commit history (but remember: notes can be pushed/fetched too, depending on refspec policy—so decide whether you want them to stay local).

---

---

## L) Reference Transactions (atomic multi-ref updates)

Reference transactions exist to guarantee: **either all ref updates apply, or none do**, keeping the repo consistent even if a multi-step ref update fails midway. ([PyGit2][6])

### L1) Context-manager semantics: automatic commit or automatic rollback

`Repository.transaction()` returns a context manager that:

* commits all queued ref updates when the context exits successfully
* performs no updates if an exception is raised ([PyGit2][6])

The docs also show manual usage:

* instantiate `ReferenceTransaction(repo)`
* call `commit()`
* ensure it’s freed (`del txn`) ([PyGit2][6])

### L2) Locking rules: you must lock before you touch

Transactions are explicit about locking:

* `txn.lock_ref(refname)` locks a ref “in preparation for updating it.”
* `txn.set_target(...)`, `txn.set_symbolic_target(...)`, and `txn.remove(refname)` require the ref to be locked first. ([PyGit2][6])

Two usage notes that drive “correct transaction style”:

* Transactions operate on **reference names**, not `Reference` objects.
* `remove()` deletes a reference inside the transaction. ([PyGit2][6])

### L3) Direct vs symbolic updates in a transaction

Transactions separate update APIs cleanly:

* `set_target(refname, target: Oid|str, signature=None, message=None)` updates a **direct** reference target.
* `set_symbolic_target(refname, target: str, signature=None, message=None)` updates a **symbolic** reference target. ([PyGit2][6])

Both accept:

* `signature`: signature for reflog (defaults to repo identity)
* `message`: reflog message ([PyGit2][6])

This is the transactional analogue of `Reference.set_target(..., message=...)` (non-transactional) and is the right primitive when you’re doing “move several pointers as one logical operation.” ([PyGit2][1])

### L4) Atomic multi-ref patterns (what transactions are really for)

The docs give a canonical example: swapping two branch tips atomically by locking both refs, reading their current targets, then setting targets crosswise—all within one transaction. ([PyGit2][6])

This is how you implement:

* branch swaps / promotions (e.g., “promote staging → prod” while moving “prod → rollback anchor”)
* consistent “two-pointer” updates (e.g., update a release tag and a release branch together)

### L5) Thread safety and deadlock discipline

Transactions are **thread-local**: they must be used from the thread that created them; using from another thread raises `RuntimeError`. Multiple threads can hold transactions simultaneously *as long as they don’t lock the same references.* ([PyGit2][6])

The docs also warn about deadlocks: if two threads try to lock the same refs in different orders, libgit2 will detect potential deadlocks and raise an error. ([PyGit2][6])

Practical production rule:

* When locking multiple refs in one transaction, **lock in a canonical order** (e.g., sorted by refname) across your entire process to avoid lock-order inversions.

---

If you want to keep following the original order, the next natural deep dive is **M) Branches**, because branches are mostly a higher-level façade over refs + upstream tracking—and all of the “correctness edges” (HEAD detached/unborn, reflog messages, atomic updates) still apply.

[1]: https://www.pygit2.org/references.html "References — pygit2 1.19.1 documentation"
[2]: https://www.pygit2.org/repository.html "Repository — pygit2 1.19.1 documentation"
[3]: https://git-scm.com/docs/git-reflog "Git - git-reflog Documentation"
[4]: https://libgit2.org/docs/reference/main/reflog/index.html "reflog APIs (libgit2 main)"
[5]: https://libgit2.org/docs/reference/main/notes/index.html "notes APIs (libgit2 main)"
[6]: https://www.pygit2.org/transactions.html "Reference Transactions — pygit2 1.19.1 documentation"
 
## M) Branches

### M0) Mental model: a branch is “a specific type of reference”

At the libgit2 layer, a **branch is a specific type of reference**; “being on a branch” just means Git will advance that branch reference when new commits are created, and the currently checked-out branch is indicated by the `HEAD` meta-ref. ([libgit2][1])

In pygit2 you should internalize this split:

* **Branch objects are convenience wrappers around references** (with extra branch-only affordances like upstream + remote-name derivation). ([PyGit2][2])
* **The canonical “identity” is still refs + OIDs**: you can always drop down to `repo.references["refs/heads/<name>"]` / `.target` / `.peel()` if needed. ([PyGit2][3])

---

### M1) Primary API surfaces: `repo.lookup_branch`, `repo.raw_listall_branches`, `repo.branches`

#### 1) Direct lookup: `repo.lookup_branch(branch_name, branch_type=BranchType.LOCAL) -> Branch`

This is the low-level “give me the Branch object” API. If you request a remote-tracking branch, you must include the remote name in the input like `origin/master`. ([PyGit2][2])

#### 2) Fast enumeration: `repo.raw_listall_branches(flag=BranchType.LOCAL) -> list[bytes]`

Returns branch names as **bytes**, with a flag controlling local vs remote-tracking vs both (`LOCAL`, `REMOTE`, `ALL`). Use this when you’re doing high-volume scanning and want to avoid allocating `Branch` wrappers unless necessary. ([PyGit2][2])

#### 3) The “collection façade”: `repo.branches` (`pygit2.repository.Branches`)

`repo.branches` behaves like a specialized `References` collection:

* mapping-ish: `__contains__`, `__getitem__`, `get()`
* iteration yields **branch names (strings)**
* mutators: `create(name, commit, force=False)`, `delete(name)`
* views: `.local`, `.remote`, and `.with_commit(commit|oid|str|None)` ([PyGit2][2])

Example patterns from the docs: list all / local-only / remote-only; get local and remote-tracking branches; create a local branch from a commit; delete it. ([PyGit2][2])

---

### M2) Naming discipline: short “branch names” vs full refnames

A *local branch* in `repo.branches` is typically addressed by short name (e.g., `"main"`), while a *remote-tracking branch* is addressed by `"origin/main"` (remote name included). ([PyGit2][2])

Underneath, remote-tracking branches live under the `refs/remotes/<remote>/...` namespace; Git’s remote docs describe the default as tracking branches under that namespace. ([Git][4])
pygit2’s own branch docs also use `refs/remotes/test/master` as the canonical example when discussing how `Branch.remote_name` extracts the remote. ([PyGit2][2])

**Practical rule:** keep a strict boundary in your code between:

* **UI-level names**: `"main"`, `"origin/main"` (what `repo.branches` uses)
* **storage-level refnames**: `"refs/heads/main"`, `"refs/remotes/origin/main"` (what `repo.references` uses) ([PyGit2][3])

This makes “branch vs ref” bugs obvious and prevents accidental `lookup_reference("main")` / `InvalidSpecError` type failures.

---

### M3) Creating, deleting, renaming branches: correctness + object-lifetime

#### Create: `repo.branches.local.create(name, commit, force=False) -> Branch`

Creation is commit-rooted: you pass a `Commit` object (not just an OID), and `force` controls overwrite behavior. ([PyGit2][2])

The documented pattern is:

```python
master = repo.branches["master"]
new = repo.branches.local.create("new-branch", repo[master.target])
```

This shows two important mechanics:

* A `Branch` exposes `.target` (because it is ultimately a reference pointer), and `repo[oid]` resolves an OID to an object (here, a `Commit`). ([PyGit2][2])

#### Delete: `repo.branches.delete(name)` or `branch.delete()`

Deletion is explicit—and **deletes invalidate branch objects**:

* `Branch.delete()` / `Branches.delete()` remove the branch; after that, the `Branch` object “will no longer be valid.” ([PyGit2][2])

So: never cache `Branch` objects across delete/rename/force-create; cache *names* (and re-lookup) instead.

#### Rename: `branch.rename(name, force=False) -> Branch`

Branch rename is a move/rename of the underlying local branch reference; pygit2 documents that the new name is validity-checked and the call returns the new branch. ([PyGit2][2])

---

### M4) “Is this *the* current branch?”: `is_head` vs detached/unborn HEAD

pygit2 gives you a branch-level predicate:

* `branch.is_head() -> bool`: True if `HEAD` points at that branch. ([PyGit2][2])

And the underlying conceptual anchor is: the checked-out branch is indicated by `HEAD`. ([libgit2][1])

This interacts sharply with two HEAD edge states (covered previously but important here):

* **Detached HEAD**: `HEAD` points directly to a commit, not a branch; in that state *no branch* will satisfy `is_head()`. ([PyGit2][3])
* **Unborn HEAD**: `HEAD` names a branch that doesn’t exist yet (no commits). Any “get current branch” logic must gate on `head_is_unborn`. ([PyGit2][3])

Operational pattern:

* Use `repo.head_is_detached` / `repo.head_is_unborn` to classify HEAD, then use branches as appropriate. ([PyGit2][3])

---

### M5) Multi-worktree safety: `is_checked_out()` is stronger than “is_head”

pygit2 exposes:

* `branch.is_checked_out() -> bool`: True if the branch is checked out by any repo “connected to the current one.” ([PyGit2][2])

This is not just “is it HEAD in *this* workdir?”
libgit2 clarifies that “checked out” is computed by iterating **all linked repositories (usually worktrees)** and seeing whether any `HEAD` points at the branch. ([libgit2][5])

So if your tooling supports worktrees (or runs on repos that might have them), **branch deletion/renaming should be guarded by `is_checked_out()`**, not just `is_head()`.

---

### M6) Upstream tracking: what it is, how it’s stored, and how pygit2 exposes it

#### M6.1) Git’s definition: upstream is config (`branch.<name>.remote` + `branch.<name>.merge`)

Git sets upstream tracking by writing config entries:

* `branch.<name>.remote`
* `branch.<name>.merge`

This is what makes `git status` / `git branch -v` show relationships and what makes `git pull` without args know what to fetch/merge. ([Git][6])

#### M6.2) Remote-tracking branches must exist

Git’s remote docs are explicit: `git fetch <name>` creates and updates remote-tracking branches `<name>/<branch>` under `refs/remotes/<name>/...` (default glob refspec). ([Git][4])

libgit2’s upstream setter has the same constraint: **the tracking reference must already exist** or the operation fails. ([libgit2][7])

That’s the core operational gotcha for programmatic workflows:

> you can’t “set upstream” to `origin/feature` until the repo actually has `refs/remotes/origin/feature` (usually created by a fetch).

#### M6.3) libgit2 upstream API shape (what pygit2 ultimately maps to)

* `git_branch_upstream_name(...)` resolves the upstream as a **full reference name** (or `GIT_ENOTFOUND` when none exists). ([libgit2][8])
* `git_branch_upstream(...)` returns a reference object corresponding to the remote-tracking branch (local branch required). ([libgit2][9])
* `git_branch_set_upstream(...)` updates configuration; passing NULL unsets; it fails if the named upstream ref doesn’t exist. ([libgit2][7])

#### M6.4) pygit2 surface: `Branch.upstream` and `Branch.upstream_name`

pygit2 surfaces upstream as:

* `branch.upstream` → upstream branch or `None` if not set; **documented as settable to `None` to unset**. ([PyGit2][2])
* `branch.upstream_name` → “the name of the reference set to be the upstream of this one.” ([PyGit2][2])

**Identity discipline recommendation:** treat `upstream_name` as the stable “contract” value (it’s a refname) and treat `upstream` as a convenience object for navigation.

#### M6.5) Computing “ahead/behind” correctly

Once you can get both tips (local tip OID and upstream tip OID), `Repository.ahead_behind(local, upstream)` computes divergence counts (“ahead” = commits in local ancestry not in upstream; “behind” vice-versa). ([PyGit2][10])

A typical branch status probe:

```python
def branch_divergence(repo, local_branch_name: str):
    br = repo.branches.local[local_branch_name]
    up = br.upstream  # None if not tracking
    if up is None:
        return None

    ahead, behind = repo.ahead_behind(br.target, up.target)
    return {"ahead": ahead, "behind": behind, "upstream": br.upstream_name}
```

(Here `.target` is the underlying reference target, i.e., an OID when direct; that’s the standard reference contract in pygit2.) ([PyGit2][3])

#### M6.6) Upstream is also addressable via revision syntax

Once upstream exists, pygit2’s revision parsing supports `<branchname>@{upstream}` forms and may return an intermediate reference. ([PyGit2][11])
That gives you an alternate route to “resolve upstream-ish strings” without manually reading config.

---

### M7) Remote-tracking branch → remote mapping: `Branch.remote_name` (and why it can throw)

For remote-tracking branches, pygit2 exposes `branch.remote_name`, which determines the remote by matching the branch against remotes’ fetch refspecs. If multiple remotes match, it raises `ValueError`. ([PyGit2][2])

This is subtle but important: don’t assume `origin/<x>` implies remote is `origin`—`remote_name` is derived from refspec matching, which can be ambiguous in heavily customized remote setups.

---

### M8) Branch-heavy “watchouts” you’ll hit in real systems

1. **Object invalidation after mutation**: delete/rename returns invalid/stale objects; re-lookup by name. ([PyGit2][2])
2. **Worktrees make “checked out” global**: `is_checked_out()` consults linked repos/worktrees; use it as the safety predicate before rename/delete. ([libgit2][5])
3. **Upstream requires the tracking ref to exist**: you typically must fetch first, or upstream set will fail (`ENOTFOUND`-style behavior). ([libgit2][7])
4. **Detached/unborn HEAD breaks “current branch” assumptions**: don’t infer branch from commits; treat branches as refs pointing to commits (not the other way around). ([libgit2][1])

---

If you want to continue in the original order, the next deep dive is **N) Index file and working copy operations** (status scanning, checkout surfaces, index conflicts, stash), because branch movement (refs) becomes “real” only when combined with index/workdir correctness.

[1]: https://libgit2.org/docs/reference/v1.9.0/branch/index.html "branch APIs (libgit2 v1.9.0)"
[2]: https://www.pygit2.org/branches.html "Branches — pygit2 1.19.1 documentation"
[3]: https://www.pygit2.org/references.html "References — pygit2 1.19.1 documentation"
[4]: https://git-scm.com/docs/git-remote "Git - git-remote Documentation"
[5]: https://libgit2.org/docs/reference/main/branch/git_branch_is_checked_out.html "git_branch_is_checked_out (libgit2 main)"
[6]: https://git-scm.com/docs/git-branch "Git - git-branch Documentation"
[7]: https://libgit2.org/docs/reference/main/branch/git_branch_set_upstream.html "git_branch_set_upstream (libgit2 main)"
[8]: https://libgit2.org/docs/reference/main/branch/git_branch_upstream_name.html "git_branch_upstream_name (libgit2 main)"
[9]: https://libgit2.org/docs/reference/main/branch/git_branch_upstream.html "git_branch_upstream (libgit2 main)"
[10]: https://www.pygit2.org/repository.html "Repository — pygit2 1.19.1 documentation"
[11]: https://www.pygit2.org/revparse.html "Revision parsing — pygit2 1.19.1 documentation"

Below is a **high-density technical deep dive** on the **N / O / J cluster** (Index+Working Copy, Diff system, Blame) in pygit2, with libgit2 + core Git semantics where that’s the real “source of truth”.

---

# N) Index file and working copy operations

## N0) The three-way model (HEAD tree ↔ index ↔ working directory)

Most of pygit2’s “porcelain-like” operations boil down to comparing and/or mutating three states:

* **HEAD**: an immutable snapshot (a commit’s tree)
* **Index** (“staging area”): the “contents of the next commit”; also stores *conflict state* and additional metadata such as a *tree cache* used to speed operations in libgit2’s implementation. ([libgit2][1])
* **Working directory**: the actual filesystem

That’s why **status** and **diff** naturally talk about *pairs* among these three, and why **checkout** and **stash** are fundamentally “state shapers” over workdir/index (with optional HEAD movement in pygit2’s high-level API).

---

## N1) `Repository.index` and the Index read/write lifecycle

### N1.1) Getting and reading the repository’s index

`repo.index` returns the `Index` representing the repository’s on-disk index file. The docs show the canonical “read it first” pattern, then access an entry by path and use its OID to retrieve the object: ([pygit2][2])

```python
index = repo.index
index.read()

entry = index["path/to/file"]   # IndexEntry
blob  = repo[entry.id]          # Blob (object lookup)
```

You can also iterate the whole index (`for entry in index:`) and get `(path, id)` quickly. ([pygit2][2])

### N1.2) Mutating the index: remember `index.write()`

Index mutation is *in-memory until you persist it*. The docs explicitly emphasize “don’t forget to save the changes”. ([pygit2][2])

```python
index.add("path/to/file")
index.remove("path/to/other")
index.write()
```

### N1.3) `Index.read(force=...)` and “reload discipline”

`Index.read(force=True)` always reloads; `force=False` reloads only if the on-disk file changed. This matters when you have multiple writers (or external `git` usage) and want to avoid doing unnecessary disk reads. ([pygit2][2])

---

## N2) Staging operations: `add`, `add_all`, `remove*`

### N2.1) `Index.add(path_or_entry)` correctness boundary

`Index.add()` accepts either:

* a **path**: must be **relative to the worktree root**, and the `Index` must be associated with a repository; or
* an **IndexEntry**: inserted without checking existence / OID validity. ([pygit2][2])

That’s the key invariant: *path-based add is “index from filesystem”*, IndexEntry add is “trust me, just stage this tuple”.

### N2.2) `Index.add_all(pathspecs=...)` vs `Index.add(...)`

`Index.add_all()` adds/updates entries matching files in the working directory, optionally filtered by `pathspecs`. ([pygit2][2])

If you’re thinking about the “real Git” semantics: libgit2’s underlying `git_index_add_all` notes that:

* it can fail for *bare* index instances,
* it takes pathspec patterns,
* it skips ignored files unless already tracked (unless you force),
* and it can clear conflict markers for files that were in conflict. ([libgit2][3])

That last point is a big deal operationally: **bulk add can implicitly advance conflict state**, which is often exactly what you want after resolving conflicts, but surprising if you’re trying to preserve conflict metadata while doing partial staging. ([libgit2][3])

### N2.3) Removing from the index

pygit2 provides:

* `Index.remove(path, level=0)`
* `Index.remove_all(pathspecs)`
* `Index.remove_directory(path, level=0)` ([pygit2][2])

Semantically:

* `remove(path)` is `git rm --cached`-like (index-only removal)
* `remove_directory` is the recursive removal surface
* `remove_all` is the pathspec-driven bulk removal

---

## N3) `IndexEntry`: the “staged file record”

`IndexEntry(path, object_id, mode)` is the minimal “this path maps to that object + that mode” record. It exposes:

* `.path`
* `.id` (OID)
* `.mode` (FileMode) ([pygit2][2])

This is where **filemode correctness** matters:

* Executable bit vs regular file (`BLOB_EXECUTABLE` vs `BLOB`)
* Symlink (`LINK`)
* Submodule entries use `COMMIT` filemode (tree entry points to a commit) (this shows up more in trees, but index entries can represent that intent too).

If you’re building automation that stages content without reading the filesystem, **IndexEntry is the lever**—but it’s also the point where you can produce subtly broken repos if you set filemode incorrectly.

---

## N4) Tree materialization: `read_tree` and `write_tree`

### N4.1) `Index.read_tree(tree|oid)`

`read_tree` replaces the contents of the index with a tree (recursively). This is the “reset index to match tree snapshot” primitive. ([pygit2][2])

### N4.2) `Index.write_tree(repo=None) -> Oid`

`write_tree` creates a tree object from the index and writes it to the object database (ODB), returning the new tree OID. It requires an associated repository (or one passed in). ([pygit2][2])

This is the core “index → tree snapshot” edge used by commit creation flows: stage → write_tree → create_commit.

---

## N5) Conflicts: representing, inspecting, clearing

### N5.1) Creating conflicts in the index: `add_conflict(...)`

`Index.add_conflict(ancestor, ours, theirs)` adds/updates index entries to represent a conflict and removes staged entries at the given paths. ([pygit2][2])

This maps directly to libgit2’s conflict model: any side can be null to represent add/delete scenarios (e.g., `ancestor=None` when a file was added independently in both branches). ([libgit2][4])

### N5.2) Inspecting conflicts: `Index.conflicts`

`Index.conflicts` returns:

* `None` if no conflicts, otherwise
* a mapping keyed by path, where each value is a 3-tuple `(ancestor, ours, theirs)` of `IndexEntry` (and any element may be `None`). ([pygit2][2])

Critically, pygit2 docs specify you can use **`del`** to remove a conflict from the index. ([pygit2][2])

This enables a “manual resolver” loop:

1. detect conflicts
2. write resolved file(s)
3. `index.add(resolved_path)` (or add_all)
4. clear conflict entries (often implicitly cleared by staging; libgit2 calls out that adding files that were in conflict removes the conflict marking) ([libgit2][3])

---

## N6) Status scanning: `repo.status()` / `repo.status_file()`

### N6.1) `repo.status(untracked_files=..., ignored=...) -> dict[path, FileStatus]`

Status returns a dict mapping file paths to `FileStatus` bitflags. ([pygit2][2])

Performance knobs matter here:

* `untracked_files="no"` or `"normal"` can be faster than `"all"` when you have many untracked files/dirs. ([pygit2][2])
* `ignored=True` includes ignored files *with untracked files* (and is ignored when `untracked_files == "no"`). ([pygit2][2])

### N6.2) The meaning of `INDEX_*` vs `WT_*`

pygit2 documents the key semantic split:

* `INDEX_*` flags describe **index vs HEAD**
* `WT_*` flags describe **working directory vs index** ([pygit2][2])

So “modified but unstaged” should show WT_MODIFIED; “staged but not committed” shows INDEX_MODIFIED, etc.

### N6.3) Under-the-hood caveat: rename detection + pathspec

If you ever drop below pygit2 into libgit2 status APIs (or if pygit2 exposes more knobs in future), note the libgit2 warning: when you filter status by pathspec, rename detection results may be inaccurate; proper rename detection wants the full file set. ([libgit2][5])

---

## N7) Checkout: `checkout`, `checkout_head`, `checkout_tree`, `checkout_index`

### N7.1) pygit2 surface

`repo.checkout(refname=None, strategy=..., directory=..., paths=..., callbacks=...)` checks out a reference and updates HEAD; if no reference is provided, it checks out from the index. Default strategy is `SAFE | RECREATE_MISSING`. ([pygit2][2])

Important correctness details:

* If `paths` is provided, **HEAD will not be set** to the reference (you’re doing a “path checkout” operation). ([pygit2][2])
* You can supply a `CheckoutCallbacks` object to observe conflicts/updates and abort early. ([pygit2][2])
* Lower-level entrypoints exist: `checkout_head`, `checkout_tree`, `checkout_index`—each takes the same kwargs. ([pygit2][2])

### N7.2) libgit2 mental model: checkout is a 4-way comparison engine

libgit2’s checkout docs are the best “explain it like you’re implementing it” description:

* Checkout updates working directory and index to match a *target tree*.
* It considers up to four things: **target**, **baseline**, **workdir**, **index**.
* In libgit2 itself, checkout **does not move HEAD** (you do that separately). ([libgit2][6])

This is why checkout has rich conflict and “dirty file” semantics: SAFE mode refuses to overwrite unexpected workdir changes, while FORCE overwrites to make the workdir match target. ([libgit2][6])

### N7.3) Strategy flags and high-risk toggles

libgit2 enumerates additional behavior modifiers beyond SAFE/FORCE, including:

* `ALLOW_CONFLICTS` (apply safe updates even if conflicts exist)
* removing untracked/ignored files
* `DONT_UPDATE_INDEX` (prevent writing updated file info to index)
* disabling filters, etc. ([libgit2][6])

pygit2’s docs show at least some of these are reachable in practice (e.g., stash apply example uses `CheckoutStrategy.ALLOW_CONFLICTS`). ([pygit2][2])

### N7.4) CheckoutOptions shape (what callbacks actually can observe)

libgit2’s `git_checkout_options` struct includes:

* strategy,
* `disable_filters` (skip filters like CRLF conversion),
* notify/progress callbacks + payloads,
* path filtering patterns,
* a baseline tree/index and a target directory override. ([libgit2][7])

So if you’re building tooling that needs:

* “only checkout these paths”
* performance measurement
* conflict labeling (“ours/theirs/ancestor” labels)
* filter control
  …those are the knobs you’re mapping onto. ([libgit2][7])

---

## N8) Stash: create/apply/drop/pop/list

### N8.1) pygit2 surface

pygit2 exposes:

* `repo.stash(stasher, message=None, keep_index=False, include_untracked=False, include_ignored=False, keep_all=False, paths=None) -> Oid` ([pygit2][2])
* `repo.stash_apply(index=0, reinstate_index=False, strategy=None, callbacks=None)`
* `repo.stash_drop(index=0)`
* `repo.stash_pop(index=0, reinstate_index=False, strategy=None, callbacks=None)`
* `repo.listall_stashes() -> list[Stash]` ([pygit2][2])

The `Stash` object exposes `commit_id` and `message`. ([pygit2][2])

Two important “plumbing truths” embedded in pygit2 docs:

* `stash()` returns the **OID of the stash merge commit**. ([pygit2][2])
* `stash_apply`’s callbacks inherit from `CheckoutCallbacks`, and you can customize checkout options via the same arguments accepted by `repo.checkout()`. ([pygit2][2])

### N8.2) Core Git semantics: stash is “save workdir+index, revert to HEAD”

Git’s own docs describe stash as recording the current state of the **working directory and the index**, then reverting the working directory to match HEAD. ([Git][8])

Git also clarifies the naming and storage model:

* the latest stash is in `refs/stash`
* older stashes live in the reflog of that ref (`stash@{0}`, `stash@{1}`, etc). ([Git][8])

And stash apply/pop can fail with conflicts; in that case it may not remove the stash entry automatically. ([Git][8])

### N8.3) libgit2 detail: `git_stash_save` output is the commit pointed to by `refs/stash`

libgit2 documents that the `out` oid from `git_stash_save` is the “commit containing the stashed state”, and that commit is the target of the direct reference `refs/stash`. ([libgit2][9])

So in pygit2, treating the returned OID as a first-class “stash object handle” is correct (you can store it, introspect it, diff it, etc.), but operationally remember the canonical list is the `refs/stash` reflog. ([libgit2][9])

---

# O) Diff system (Diff / Patch / Hunks / Lines / Stats / rename detection)

## O0) Entry points: `repo.diff` and the “pairwise comparators”

A **diff** is computed between trees, index, workdir, and/or blobs.

### `Repository.diff(a=None, b=None, cached=False, flags=..., context_lines=3, interhunk_lines=0) -> Diff | Patch`

Key semantics (as documented):

* `a=None, b=None` compares **working directory vs index**
* if `b=None`, by default compares **working directory vs a**
* if `b=None` and `cached=True`, compares **index vs a**
* supports `a`/`b` as revparse strings or References, etc. ([pygit2][10])

### Index-local diff helpers

The Index itself exposes:

* `index.diff_to_tree(tree)`
* `index.diff_to_workdir()` ([pygit2][2])

These are often cleaner than overloading `repo.diff` when you’re very explicit about what “side” you mean.

---

## O1) The `Diff` object: iteration model and high-leverage operations

### O1.1) Iteration and size

* `Diff.__iter__()` iterates over deltas/patches
* `Diff.__len__()` gives number of deltas/patches ([pygit2][10])

### O1.2) Transform and normalize: `find_similar(...)`

`Diff.find_similar(...)` is the rename/copy/rewrite detection pass. It **modifies the diff in place** and can:

* mark renames/copies
* replace entries with new rename/copy deltas
* optionally break “rewrites” into add/remove pairs above thresholds ([pygit2][10])

The parameter list includes thresholds and limits like `rename_threshold`, `copy_threshold`, and `rename_limit`. ([pygit2][10])

Under the hood, libgit2 has an explicit options struct (`git_diff_find_options`) dedicated to controlling rename/copy detection behavior. ([libgit2][11])

### O1.3) Patch materialization and identity

* `Diff.patch` is the unified patch string (can be `None` for empty commits). ([pygit2][10])
* `Diff.patchid` yields the corresponding patchid. ([pygit2][10])
* `Diff.stats` accumulates statistics for all patches. ([pygit2][10])
* `Diff.merge(other)` merges diffs (useful for composing). ([pygit2][10])
* `Diff.parse_diff(git_diff_bytes_or_str)` parses a unified diff into a Diff object without a repository. ([pygit2][10])

---

## O2) The `Patch` object: per-file diff payload + encoding discipline

`Patch` is the file-level unit you usually want for:

* raw patch content,
* iterating hunks/lines,
* computing insert/delete counts.

Key properties:

* `patch.data` gives raw bytes of the patch
* `patch.text` gives decoded Unicode **assuming UTF-8**, and docs warn this can be lossy/non-reversible for non-UTF-8 content (use `data` for fidelity). ([pygit2][10])
* `patch.delta` gives the associated `DiffDelta`
* `patch.hunks` and `patch.line_stats` provide structure and counts ([pygit2][10])
* `Patch.create_from()` can create patches from blobs/buffers without going through repo diff. ([pygit2][10])

**Practical rule**: if you’re building a deterministic artifact store (e.g., snapshotting diffs), store `patch.data` not `patch.text`.

---

## O3) Delta/File/Hunk/Line/Stats: the structural schema

### O3.1) `DiffDelta`: what changed at the file level

A `DiffDelta` contains:

* `status` (an `enums.DeltaStatus`)
* `status_char()` single-character abbreviation
* `old_file` / `new_file`
* `similarity` score for renamed/copied
* `is_binary` tri-state (True/False/None)
* `flags` (DiffFlag bitmask) ([pygit2][10])

This is the object you inspect after `find_similar()` to see RENAMED/COPIED behavior.

### O3.2) `DiffFile`: the per-side file metadata

`DiffFile` exposes:

* `id` (OID of blob-like item when applicable)
* `mode` (FileMode)
* `path` and `raw_path`
* `size`
* `flags` ([pygit2][10])

### O3.3) `DiffHunk`: location ranges and hunk header

`DiffHunk` provides:

* `header`
* `old_start`, `old_lines`
* `new_start`, `new_lines`
* `lines` (structured `DiffLine`s) ([pygit2][10])

### O3.4) `DiffLine`: the actual line-level stream

`DiffLine` fields include:

* `origin` (line type: context/add/del/etc)
* `content` (text)
* `raw_content` (bytes)
* `old_lineno`, `new_lineno` with `-1` sentinel for added/deleted lines
* `content_offset`
* `num_lines` (# newline chars) ([pygit2][10])

If you need a canonical “changed line spans” extractor, you build it from `DiffHunk` + `DiffLine` with `origin` and `old_lineno/new_lineno`.

### O3.5) `DiffStats`: totals + formatting

`DiffStats` gives:

* `files_changed`
* `insertions`
* `deletions`
* `format(format, width)` to produce a scaled stats string (bitmask `DiffStatsFormat`) ([pygit2][10])

---

## O4) Recipes that actually survive production

### O4.1) “Working tree not staged” vs “staged not committed”

Using `repo.diff`:

* not staged: `repo.diff()` (workdir vs index) ([pygit2][10])
* staged: `repo.diff(cached=True)` (index vs HEAD) ([pygit2][10])
* full since last commit: `repo.diff("HEAD")` (workdir vs HEAD) ([pygit2][10])

### O4.2) Reliable rename/copy detection

Compute your base diff, then run `find_similar()` explicitly before consuming deltas if rename/copy status matters:

```python
d = repo.diff("HEAD~1", "HEAD")
d.find_similar()      # marks renames/copies in-place
for delta in d.deltas:
    ...
```

The “in place rewrite” nature is intentional; treat it like a post-processing stage and don’t mix “pre” and “post” views. ([pygit2][10])

### O4.3) Deterministic patch capture

Use `patch.data` for lossless bytes; don’t rely on `patch.text` when encoding may vary. ([pygit2][10])

---

# J) Blame

## J0) What `repo.blame` actually computes (and default bounds)

`repo.blame(path, ...)` returns a `Blame` object for a single file, with optional constraints on commit range and line range. ([pygit2][12])

libgit2 clarifies defaults via `git_blame_options`:

* `newest_commit` default is **HEAD**
* `oldest_commit` default is the first commit encountered with a NULL parent
* `min_line` default is 1; `max_line` default is last line ([libgit2][13])

So: “blame(file)” is implicitly “blame file as of HEAD, walking history to root”.

---

## J1) `Repository.blame(...)` parameters and high-leverage flags

Signature highlights: ([pygit2][12])

* `flags: BlameFlag` controls algorithmic behavior
* `min_match_characters`: threshold for moved/copied line association
* `newest_commit`, `oldest_commit`: bound the history window
* `min_line`, `max_line`: bound the line window

Blame flags exposed by pygit2 include (not exhaustive):

* copy tracking variants (same file / same commit moves / same commit copies / any commit copies)
* `FIRST_PARENT`
* `USE_MAILMAP`
* `IGNORE_WHITESPACE` ([pygit2][12])

**Meaningful costs**:

* Any of the `TRACK_COPIES_*` flags can significantly increase work (more history + similarity checks).
* `IGNORE_WHITESPACE` can change attribution in whitespace-heavy refactors.

### `min_match_characters` isn’t just “accuracy”; it’s “search space”

libgit2 documents:

* default `min_match_characters` is **20**
* it only takes effect if any `TRACK_COPIES_*` flags are specified ([libgit2][13])

Lowering it tends to increase detected movements/copies (and compute), raising it makes copy detection stricter.

---

## J2) `Blame` and `BlameHunk` data model

### `Blame` behaviors

* iterable, indexable, and length-aware
* `for_line(line_no)` returns the hunk that covers a 1-based line number ([pygit2][12])

### `BlameHunk` fields you will actually use

A blame hunk is an interval mapping a contiguous set of lines to a commit:

* `lines_in_hunk`
* `final_commit_id`, `final_start_line_number`
* `orig_commit_id`, `orig_path`, `orig_start_line_number`
* `boundary` (tracked to a boundary commit)
* `final_committer`, `orig_committer` ([pygit2][12])

Two important subtleties:

* `orig_path` allows blame to represent renames / path movement across history. ([pygit2][12])
* `boundary=True` is exactly what you’d expect when you hit your `oldest_commit` bound or the root boundary. ([pygit2][12])

---

## J3) Practical “diff → blame” attribution (what you actually want for tooling)

A common production use-case is: **attribute the changed lines in a diff to the commit that last touched them**.

Pipeline:

1. compute diff (O section)
2. for each changed line range (old side), call `blame.for_line(old_lineno)` to get commit attribution

You can restrict blame to only the relevant part of the file by using `min_line/max_line`, which is often the single biggest performance win for “blame just what changed.” ([pygit2][12])

---

## J4) Performance guardrails

Blame is expensive; your knobs are:

* limit line range (`min_line`, `max_line`) ([pygit2][12])
* limit history window (`oldest_commit`, `newest_commit`) ([pygit2][12])
* avoid `TRACK_COPIES_*` unless you truly need moved/copied attribution (and if you do, tune `min_match_characters`) ([pygit2][12])
* use `FIRST_PARENT` if you want a mainline-only story for merge-heavy histories ([pygit2][12])

---

## If you want the “next” deep dive after N/J/O

The next chapter that naturally composes with this cluster is **P) Merge & cherrypick**, because:

* merge results often surface as **index conflicts**,
* checkout strategy controls the safe/force edges,
* diff and blame become the basis for conflict explanation and guided resolution.

[1]: https://libgit2.org/docs/reference/main/index/index.html "index APIs (libgit2 main)"
[2]: https://www.pygit2.org/index_file.html "Index file & Working copy — pygit2 1.19.1 documentation"
[3]: https://libgit2.org/docs/reference/main/index/git_index_add_all.html "git_index_add_all (libgit2 main)"
[4]: https://libgit2.org/docs/reference/main/index/git_index_conflict_add.html "git_index_conflict_add (libgit2 main)"
[5]: https://libgit2.org/docs/reference/main/status/git_status_list_new.html "git_status_list_new (libgit2 main)"
[6]: https://libgit2.org/docs/reference/main/checkout/git_checkout_strategy_t.html "git_checkout_strategy_t (libgit2 main)"
[7]: https://libgit2.org/docs/reference/main/checkout/git_checkout_options.html "git_checkout_options (libgit2 main)"
[8]: https://git-scm.com/docs/git-stash "Git - git-stash Documentation"
[9]: https://libgit2.org/docs/reference/main/stash/git_stash_save.html "git_stash_save (libgit2 main)"
[10]: https://www.pygit2.org/diff.html "Diff — pygit2 1.19.1 documentation"
[11]: https://libgit2.org/docs/reference/main/diff/index.html "diff APIs (libgit2 main)"
[12]: https://www.pygit2.org/blame.html "Blame — pygit2 1.19.1 documentation"
[13]: https://libgit2.org/docs/reference/main/blame/git_blame_options.html "git_blame_options (libgit2 main)"


## P) Merge & cherrypick (pygit2 / libgit2): the “real” mechanics

At the pygit2 layer, **merge** and **cherrypick** are intentionally “porcelain-adjacent but incomplete”: they set up the **index + working directory** (and write conflict metadata), but they **do not** finish the operation by committing and/or moving refs for you. That design is explicit in the docs for `Repository.merge(...)` (“does not commit nor update the branch reference in the case of a fastforward”) and in the requirement to inspect `repo.index` for conflicts and then create a commit yourself. ([pygit2][1])

Also: for compatibility with Git, libgit2 puts the repository into an **operation state** (“merging state” for `git_merge`), and you’re expected to clear that state when done (or when aborting) via `git_repository_state_cleanup()`—which is exposed in pygit2 as `Repository.state_cleanup()`. ([libgit2][2])

---

# P0) The core invariants you should standardize on

### 1) Three inputs: ancestor / ours / theirs

Any non-trivial merge is conceptually a **3-way merge**:

* **ancestor**: common base snapshot
* **ours**: current HEAD (or chosen “base” commit/tree)
* **theirs**: incoming commit/tree

pygit2 exposes the base computation (merge bases) and both **high-level “merge into HEAD”** and **low-level “merge two commits/trees into an in-memory index”** APIs. ([pygit2][1])

### 2) The index is the “result carrier”

Both libgit2 merge and cherry-pick produce results in the **index**, recording **conflicts** there when they exist, and then a checkout step materializes results into the working tree (including conflict markers when applicable). This is spelled out both in libgit2 merge behavior and Git’s own cherry-pick behavior description. ([libgit2][2])

### 3) Ref movement + commit creation are separate steps

* A **fast-forward** is fundamentally “move a ref” (no merge commit required).
* A **no-ff merge** is “create a merge commit” (even if a fast-forward was possible).
* A **normal merge** is “create a new tree (maybe with conflicts resolved), then create a merge commit with two parents”.

pygit2’s `merge(...)` explicitly does not auto-commit and does not auto-advance branch refs on fast-forward. ([pygit2][1])

---

# P1) Merge base computation: `merge_base`, `merge_base_many`, `merge_base_octopus`

### `Repository.merge_base(oid1, oid2) -> Oid | None`

Find the “best” common ancestor for two commits; returns `None` if no merge base exists. ([pygit2][1])

### N-way bases (when you’re doing octopus or repeated merging)

* `merge_base_many(oids: list[Oid]) -> Oid | None`
* `merge_base_octopus(oids: list[Oid]) -> Oid | None`

pygit2’s docs explicitly position these as “base calculators” for *n-way* merges, and then you repeatedly invoke `merge_commits` / `merge_trees` to fold multiple heads into a single resulting tree/index while handling conflicts along the way. ([pygit2][1])

**Identity discipline:** persist merge bases as full OIDs (not short IDs), and always treat them as *repo-scoped* even though they’re content hashes.

---

# P2) Merge planning: `merge_analysis(their_head, our_ref='HEAD') -> (MergeAnalysis, MergePreference)`

### What you get back

pygit2 returns:

* **analysis flags**: mixture of `NONE`, `NORMAL`, `UP_TO_DATE`, `FASTFORWARD`, `UNBORN`
* **preference**: user preference from `merge.ff` ([pygit2][1])

libgit2 defines the preference flags:

* `NO_FASTFORWARD` when `merge.ff=false`
* `FASTFORWARD_ONLY` when `merge.ff=only` ([libgit2][3])

…and the canonical analysis meanings:

* `UP_TO_DATE`: theirs is reachable from HEAD; nothing to do
* `FASTFORWARD`: HEAD can be advanced directly to theirs
* `NORMAL`: histories diverged; real merge required ([libgit2][4])

**UNBORN** is the “HEAD exists as a name but points to no commit yet” situation; libgit2 defines an unborn branch as named from HEAD but absent in refs namespace because it has no commit target. ([libgit2][5])

### A robust “plan then execute” skeleton

```python
import pygit2
from pygit2.enums import MergeAnalysis, MergePreference

def plan_merge(repo: pygit2.Repository, their_head: pygit2.Oid):
    analysis, pref = repo.merge_analysis(their_head)

    if analysis & MergeAnalysis.UP_TO_DATE:
        return ("noop", pref)

    if analysis & MergeAnalysis.UNBORN:
        return ("unborn_head", pref)

    if analysis & MergeAnalysis.FASTFORWARD:
        # pref may still request NO_FASTFORWARD (merge.ff=false)
        return ("fastforward", pref)

    if analysis & MergeAnalysis.NORMAL:
        # pref may request FASTFORWARD_ONLY, in which case you should abort
        return ("normal_merge", pref)

    return ("none", pref)
```

**Operational policy implications:**

* If preference includes `FASTFORWARD_ONLY` but analysis says `NORMAL`, fail (that’s exactly what the preference means). ([libgit2][3])
* If preference includes `NO_FASTFORWARD` but analysis says `FASTFORWARD`, you can still create a **no-ff merge commit** (tree equals theirs, parents are old HEAD and theirs). That’s how Git `--no-ff` behaves; pygit2 won’t do it automatically, but you can. ([pygit2][1])

---

# P3) High-level merge: `Repository.merge(source, favor, flags, file_flags)`

### Signature and “source” discipline

`merge(source=Reference|Commit|Oid|str, favor=MergeFavor.NORMAL, flags=MergeFlag.FIND_RENAMES, file_flags=MergeFileFlag.DEFAULT)` merges the given commit/reference into `HEAD`. Passing a **Reference** is preferred because it enriches merge metadata (e.g., `Repository.message` can name the merged branch). Passing partial commit hashes as strings is deprecated. ([pygit2][1])

### What `repo.merge(...)` does (and does not do)

It:

* writes results into the **working directory**
* stages cleanly-applied changes into the **index**
* writes conflicts into the **index**
* expects you to inspect the index, resolve conflicts, then create a commit ([pygit2][1])

It does **not**:

* create the merge commit
* advance branch refs for a fast-forward merge ([pygit2][1])

### `favor`: file-level conflict semantics (and why it’s more dangerous than it looks)

pygit2: for all values except `NORMAL`, “the index will not record a conflict.” ([pygit2][1])

libgit2 defines what those favor modes mean:

* `NORMAL`: record conflict in index so checkout can produce conflict markers
* `OURS`: take ours side for conflicting regions; no conflict recorded
* `THEIRS`: take theirs side; no conflict recorded
* `UNION`: combine unique lines; no conflict recorded ([libgit2][6])

**Implication:** `favor != NORMAL` trades away the *structured* conflict representation (`index.conflicts`) in exchange for auto-resolution. That’s fine for automation (“always take ours/theirs”), but it removes your ability to build a guided conflict-resolution UI later.

### `flags` / `file_flags`

pygit2’s merge flags default to `FIND_RENAMES` and can be combined (example includes `NO_RECURSIVE`); you can also set `flags=0` to disable the default rename detection behavior. ([pygit2][1])

### Post-merge: inspect conflicts, then commit

Conflicts will appear in `repo.index.conflicts` as a mapping keyed by path, where each value is `(ancestor, ours, theirs)` entries (any may be `None` for add/delete conflicts). ([pygit2][7])

If conflicts are resolved (or never existed), you create a merge commit with two parents (HEAD and theirs), using the index tree:

```python
user = repo.default_signature
tree = repo.index.write_tree()
msg  = "Merge ..."
new_commit = repo.create_commit('HEAD', user, user, msg, tree,
                                [repo.head.target, their_head])
```

That exact “write_tree then create_commit with two parents” flow is shown in the pygit2 merge docs. ([pygit2][1])

### Cleanup: end the merge state (or abort)

libgit2 puts the repo into a **merging state** and tells you to clear it with `git_repository_state_cleanup()` after commit or abort. ([libgit2][2])
In pygit2 that is `repo.state_cleanup()`, which removes metadata like `MERGE_HEAD`, `MERGE_MSG`, etc. ([pygit2][8])

---

# P4) “Safe vs force” edges: using checkout strategy with merge results

The high-level `repo.merge(...)` writes to the working directory directly, but the *real power move* is: use **lower-level merge → in-memory Index → checkout_index(strategy=...)** so you control the overwrite policy.

pygit2 exposes:

* `repo.checkout_index(index=None, **kwargs)` and all checkout methods accept `strategy=CheckoutStrategy...` ([pygit2][7])
  libgit2’s checkout strategy docs make clear that `FORCE` emulates `git checkout -f`, while SAFE modes avoid overwriting local modifications; `ALLOW_CONFLICTS` lets safe updates proceed even if conflicts exist. ([libgit2][9])

This becomes critical when you’re building automated pull/merge tooling in a service and must decide whether to:

* refuse dirty worktrees (SAFE)
* clobber changes (FORCE)
* partially update while leaving conflict artifacts (ALLOW_CONFLICTS)

---

# P5) Low-level merge APIs: deterministic merges + bare-repo support

pygit2 explicitly provides low-level merging that **does not touch the workdir** and instead returns an **in-memory Index**:

* `repo.merge_commits(ours, theirs, favor=..., flags=..., file_flags=...) -> Index`
* `repo.merge_trees(ancestor, ours, theirs, favor=..., flags=..., file_flags=...) -> Index` ([pygit2][1])

These are the primitives you want for:

* bare repositories
* “merge planning” without filesystem side effects
* multi-stage merges (n-way folding)
* building your own “checkout + ref movement + commit” policy layer

### Writing the result to a tree (and then to a commit)

Because the returned `Index` is in-memory and may not be associated with the repo, you typically do:

* resolve conflicts in `index.conflicts` (if any)
* `tree_id = index.write_tree(repo)`
* `repo.create_commit(...)` ([pygit2][10])

### Applying to the working directory with your chosen overwrite policy

```python
idx = repo.merge_commits(repo.head.target, their_head)
repo.checkout_index(idx, strategy=pygit2.CheckoutStrategy.SAFE)
```

`checkout_index` is explicitly designed for “checkout the given index (or repo index)” with the same kwargs as `checkout()`. ([pygit2][7])

---

# P6) Cherrypick: `Repository.cherrypick(oid)` and the “no workdir” variant

### High-level cherrypick (workdir + on-disk index)

`repo.cherrypick(oid)` “merges the given commit into HEAD as a cherrypick”, writing results to workdir and staging changes; conflicts go into the index and you must inspect/resolve and then create the new commit yourself. ([pygit2][1])

pygit2 also warns: after a successful cherrypick you must call `repo.state_cleanup()` to exit cherrypicking mode. ([pygit2][1])

This mirrors Git CLI semantics: on conflict, Git records up to three versions in the index and writes conflict markers into the working tree; it also sets `CHERRY_PICK_HEAD`. ([Git][11])

### Cherrypick recipe (pygit2’s documented “porcelain” flow)

The official recipe shows:

* `repo.checkout('basket')`
* `repo.cherrypick(cherry_id)`
* if no conflicts: `tree_id = repo.index.write_tree()` then `repo.create_commit(...)`
* `repo.state_cleanup()` ([pygit2][10])

### Cherrypick without a working copy (bare-safe, more control)

pygit2’s recipe uses `merge_trees` to simulate the 3-way operation:

* `base_tree = cherry.parents[0].tree`
* `index = repo.merge_trees(base_tree, basket, cherry)`
* `tree_id = index.write_tree(repo)`
* `create_commit(...)` ([pygit2][10])

This is also the pattern you use for “apply this patch-like commit onto that tip” in pipelines, without touching the filesystem until you decide to.

---

# P7) Merge commits in cherry-pick: the “mainline parent” problem

Git CLI requires `-m <parent-number>` when cherry-picking a **merge commit**, because you must choose which parent is the baseline (“mainline”). ([Git][11])

libgit2 encodes this explicitly in `git_cherrypick_commit(..., mainline, ...)`: “mainline is the parent of the cherrypick_commit, if it is a merge”. ([libgit2][12])

**Practical takeaway for pygit2:** the high-level `Repository.cherrypick(oid)` docs don’t expose a mainline selector; if you need correct cherry-pick semantics for merge commits (the equivalent of `git cherry-pick -m`), you should expect to implement a lower-level strategy (or avoid cherry-picking merge commits) guided by the libgit2 mainline parameter semantics. ([pygit2][1])

---

# P8) Building “guided conflict resolution” with Diff + Blame (the cluster payoff)

Once `merge(...)` or `cherrypick(...)` runs, the **index conflict tuples** are your structured input:

* `repo.index.conflicts[path] -> (ancestor_entry, ours_entry, theirs_entry)` ([pygit2][7])

From there you can build a high-quality explainer:

### 1) Rehydrate blobs

```python
anc, ours, theirs = repo.index.conflicts[path]
anc_blob   = repo[anc.id]   if anc   else None
ours_blob  = repo[ours.id]  if ours  else None
theirs_blob= repo[theirs.id]if theirs else None
```

### 2) Compute diffs that explain “why” it conflicted

* diff ancestor→ours and ancestor→theirs (to show competing edits)
* diff ours→theirs (to show the final divergence)

If you used `favor=NORMAL`, conflicts are recorded so checkout can generate conflict-marker files; if you used `favor=OURS/THEIRS/UNION`, you *won’t* get conflict records to explain later. ([libgit2][6])

### 3) Attribute competing edits with blame (line-bounded)

Git describes conflict cases as “index records up to three versions”; you can then blame the conflicting hunks in each side to point to the commit+author responsible. ([Git][11])

That’s the core of an “AI conflict resolver” that doesn’t guess: it can answer *what changed*, *who last touched the lines*, *and what the common ancestor looked like*, using only Git’s first-class data structures.

---

## Next after P (if we keep the original sequence)

The natural continuation is **Q) Remotes & networking (fetch/push/prune) + callbacks + credentials**, because a realistic “pull” is: fetch → merge_analysis → (fast-forward or merge/cherrypick) → commit/ref updates → cleanup, and the reliability hinges on your remote callbacks + credential policy.

[1]: https://www.pygit2.org/merge.html "Merge & Cherrypick — pygit2 1.19.1 documentation"
[2]: https://libgit2.org/docs/reference/main/merge/git_merge.html "git_merge (libgit2 main)"
[3]: https://libgit2.org/docs/reference/main/merge/git_merge_preference_t.html "git_merge_preference_t (libgit2 main)"
[4]: https://libgit2.org/docs/reference/main/merge/git_merge_analysis_t.html?utm_source=chatgpt.com "git_merge_analysis_t (libgit2 main)"
[5]: https://libgit2.org/docs/reference/main/repository/git_repository_head_unborn.html "git_repository_head_unborn (libgit2 main)"
[6]: https://libgit2.org/docs/reference/main/merge/git_merge_file_favor_t.html "git_merge_file_favor_t (libgit2 main)"
[7]: https://www.pygit2.org/index_file.html "Index file & Working copy — pygit2 1.19.1 documentation"
[8]: https://www.pygit2.org/repository.html "Repository — pygit2 1.19.1 documentation"
[9]: https://libgit2.org/docs/reference/v1.4.1/checkout/git_checkout_strategy_t.html?utm_source=chatgpt.com "git_checkout_strategy_t (libgit2 v1.4.1)"
[10]: https://www.pygit2.org/recipes/git-cherry-pick.html "git-cherry-pick — pygit2 1.19.1 documentation"
[11]: https://git-scm.com/docs/git-cherry-pick "Git - git-cherry-pick Documentation"
[12]: https://libgit2.org/docs/reference/main/cherrypick/git_cherrypick_commit.html "git_cherrypick_commit (libgit2 main)"

## Q) Remotes & networking (fetch/push/prune/listing) + callbacks + credentials

### Q0) Mental model: “Remote = configured transport endpoint + refspec mapping + network ops”

In pygit2, remotes live under `repo.remotes` (a `RemoteCollection`) and are backed by libgit2’s “remote” subsystem. A remote is not “pull”/“checkout” — it’s *connect/list refs/fetch/push/prune*, and it updates your **remote-tracking refs** according to **refspecs**. ([pygit2.org][1])

---

### Q1) Remote collection management: `repo.remotes` (`RemoteCollection`)

`RemoteCollection` is the configured-remote control plane:

* `create(name, url, fetch=None) -> Remote` (optional custom fetch refspec instead of default)
* `create_anonymous(url) -> Remote` (in-memory only; great for “ls-remote style” probes)
* `delete(name)` (removes remote from config; also removes remote-tracking branches + config settings for that remote)
* `rename(name, new_name) -> list[str]` (renames the remote and remaps *standard-format* refspecs; returns fetch refspecs that weren’t standard and couldn’t be remapped)
* `set_url(name, url)` and `set_push_url(name, url)`
* `add_fetch(name, refspec)` / `add_push(name, refspec)` to edit refspec lists
* `names()` iterator for configured remote names ([pygit2.org][1])

**Key correctness edge:** `rename()` returning “unremappable” refspecs is a signal that your repo has custom refspecs; if you ignore this list you can end up with a renamed remote whose fetch mapping is *partially* stale. ([pygit2.org][1])

---

### Q2) Remote object surface: identity + refspec introspection + refspec mechanics

A `Remote` exposes:

* identity: `name`, `url`, `push_url`
* refspecs: `fetch_refspecs`, `push_refspecs`, `refspec_count`, plus `get_refspec(n) -> Refspec` ([pygit2.org][1])

`Refspec` objects (returned by `get_refspec`) are not constructed directly; they model how remote refs map into your local namespace and include:

* `direction` (fetch/push)
* `src`, `dst`
* `force` (allows non-fast-forward updates)
* `src_matches(ref)`, `dst_matches(ref)`
* `transform(ref)` and `rtransform(ref)` for mapping lhs↔rhs ([pygit2.org][1])

**Practical identity discipline:** treat refspecs as part of your repo’s *data contract*. If you’re building automation (e.g., “mirror refs/* into refs/remotes/origin/*”), store and validate the refspec set explicitly and test it via `Refspec.transform()` / `src_matches()` in a harness. ([pygit2.org][1])

---

### Q3) Network operations: connect/list_heads/fetch/prune/push

#### Q3.1) `Remote.connect(callbacks=None, direction=0, proxy=None)`

`connect()` establishes the transport connection; it accepts a **proxy policy**:

* `None`: disable proxy usage
* `True`: auto-detect proxy
* `str`: explicit proxy URL (`http://proxy.example.org:3128/`) ([pygit2.org][1])

Use `connect()` explicitly if you’re doing a “probe” workflow (e.g., list remote refs) and want to control authentication/cert callbacks and proxy handling deterministically. ([pygit2.org][1])

#### Q3.2) Listing remote refs: `Remote.list_heads(..., connect=True)`

`list_heads()` returns the refs the server advertises for a new connection (the “ls-refs-ish” view). It can `connect` internally, or reuse an existing connection (`connect=False`). It also documents a subtle lifetime rule: the list remains available after disconnecting so long as you don’t initiate a new connection. ([pygit2.org][1])

(There’s also `ls_remotes` but it’s explicitly deprecated in favor of `list_heads`.) ([pygit2.org][1])

#### Q3.3) `Remote.fetch(refspecs=None, message=None, callbacks=None, prune=..., proxy=None, depth=0) -> TransferProgress`

Fetch is the canonical “download + update remote-tracking refs” operation:

* `prune`: `UNSPECIFIED` (use repo config), `PRUNE` (delete local remote branches not present on remote), `NO_PRUNE` ([pygit2.org][1])
* `proxy`: same policy as `connect()` ([pygit2.org][1])
* `depth`: shallow fetch (0 = full history; non-zero limits commits from tip per remote branch) ([pygit2.org][1])
* returns `TransferProgress` (counts of objects/deltas/received bytes, etc.) ([pygit2.org][1])

**Shallow semantics:** Git defines shallow clones/fetches via `--depth` (truncate history) and describes how to deepen with fetch. pygit2’s `depth` parameter is the direct analogue. ([Git SCM][2])

**Important operational boundary:** fetch updates *refs* and downloads objects; it does not “integrate into your current branch” automatically. Your “pull” pipeline is: fetch → choose remote-tracking target → merge_analysis/fast-forward/merge → checkout/cleanup. (This is why earlier we treated merge/cherrypick as separate from remote I/O.) ([pygit2.org][1])

#### Q3.4) `Remote.prune(callbacks=None)`

This runs a prune against the remote (separate from fetch). Use it when you want to prune without doing a full fetch. ([pygit2.org][1])

#### Q3.5) `Remote.push(specs, callbacks=None, proxy=None, push_options=None, threads=1) -> None`

Push sends ref updates to the remote:

* `specs`: push refspecs (like `refs/heads/main:refs/heads/main`)
* `proxy`: same policy as connect/fetch
* `push_options`: options passed to server hooks (pre-/post-receive)
* `threads`: packbuilder threads (0 = auto-detect; default is 1) ([pygit2.org][1])

**The big correctness gotcha (server hooks):** pygit2 documents that `push()` can return “successfully” even if a server-side hook denies a ref update; therefore it strongly recommends implementing `RemoteCallbacks.push_update_reference()` and checking the callback parameters to determine per-ref acceptance/rejection. ([pygit2.org][1])

---

### Q4) Callback model: `RemoteCallbacks` is your *correctness + observability surface*

`RemoteCallbacks` is passed into connect/fetch/push/list_heads and provides hooks:

* `credentials(url, username_from_url, allowed_types)`
* `certificate_check(certificate, valid, host)`
* `sideband_progress(string)` (“counting objects…” style progress output)
* `transfer_progress(stats: TransferProgress)`
* `update_tips(refname, old, new)` (ref update reporting)
* push-phase: `push_negotiation(updates)`, `push_transfer_progress(objects_pushed, total_objects, bytes_pushed)`, `push_update_reference(refname, message)` ([pygit2.org][1])

Two callback mechanics that matter in production:

1. **Performance context:** pygit2 warns that push transfer progress is called inline with pack building operations, so a slow callback can slow the push. libgit2 makes the same warning for pack-related callbacks. ([pygit2.org][1])
2. **Ref-update reporting:** libgit2’s remote callback struct includes both `update_tips` and a newer `update_refs` (and notes `update_refs` should be preferred over `update_tips`). pygit2 exposes `update_tips`, so treat it as “best available” at the pygit2 layer. ([libgit2][3])

---

### Q5) Credentials: selection, loops, and credential types

#### Q5.1) The authentication loop (libgit2 truth)

libgit2’s authentication guide describes the actual loop:

* libgit2 first tries credentials embedded in the URL
* then calls the caller-provided `credentials` callback
* if the server rejects the credentials, the callback is called again; the callback drives the loop until success or error
* for SSH, libgit2 may need a username first (username-only credential type) to discover available auth methods; SSH servers won’t allow changing the username mid-session ([libgit2][4])

This exactly explains why your pygit2 `credentials()` callback must:

* be prepared to be called multiple times,
* treat `allowed_types` as a negotiation signal, not a suggestion. ([pygit2.org][1])

#### Q5.2) pygit2 credential objects (callable convenience objects)

pygit2 documents several credential helper objects; they’re callable and can be passed directly as the `credentials` callback when credentials are constant:

* `Username(username)`
* `UserPass(username, password)`
* `Keypair(username, pubkey_path, privkey_path, passphrase)`
* `KeypairFromAgent(username)`
* `KeypairFromMemory(username, pubkey, privkey, passphrase)` ([pygit2.org][1])

#### Q5.3) Mapping to libgit2 credential types

libgit2 enumerates credential types like:

* `GIT_CREDENTIAL_USERPASS_PLAINTEXT`
* `GIT_CREDENTIAL_SSH_KEY`
* `GIT_CREDENTIAL_USERNAME`
* `GIT_CREDENTIAL_SSH_MEMORY` (with a warning that crypto backend differences may make it non-functional) ([libgit2][5])

In pygit2, your `allowed_types` argument corresponds to these kinds of capabilities, so your selector logic should branch on bitmasks (and degrade gracefully). ([pygit2.org][1])

#### Q5.4) Certificate validation callback (and the pygit2 portability caveat)

* pygit2’s `certificate_check(certificate, valid, host)` returns **True to connect / False to abort**, but notes the `certificate` is currently always `None` while representation is worked out cross-platform. ([pygit2.org][1])
* libgit2 describes `certificate_check` as the “final decision” callback when certificate verification fails. ([libgit2][3])

**Practical implication:** in pygit2 today, you mostly gate on `valid` and `host` (and your own allowlist/CA policy), rather than inspecting the certificate object itself. ([pygit2.org][1])

---

### Q6) Concrete “fetch/push with correct callbacks” skeleton

```python
import pygit2
from pygit2 import RemoteCallbacks, UserPass, KeypairFromAgent

class CB(RemoteCallbacks):
    def __init__(self, username=None, password=None):
        super().__init__()
        self._username = username
        self._password = password

    def credentials(self, url, username_from_url, allowed_types):
        # Example strategy:
        # 1) If SSH username is requested, supply it.
        # 2) If SSH agent auth is allowed, use it.
        # 3) If plaintext user/pass is allowed, use it.
        if allowed_types & pygit2.enums.CredentialType.USERNAME:
            return pygit2.Username(username_from_url or self._username or "git")
        if allowed_types & pygit2.enums.CredentialType.SSH_KEY:
            return KeypairFromAgent(username_from_url or self._username or "git")
        if allowed_types & pygit2.enums.CredentialType.USERPASS_PLAINTEXT:
            return UserPass(self._username, self._password)
        return None

    def push_update_reference(self, refname, message):
        # message is None if accepted; otherwise rejection reason
        if message:
            raise pygit2.GitError(f"push rejected for {refname}: {message}")

cb = CB(username="me", password="***")
remote = repo.remotes["origin"]

# Shallow-ish fetch example
tp = remote.fetch(callbacks=cb, prune=pygit2.enums.FetchPrune.PRUNE, depth=50)
print(tp.received_objects, tp.received_bytes)

# Push example (supply callback so hook rejections are surfaced)
remote.push(["refs/heads/main:refs/heads/main"], callbacks=cb)
```

Everything in that snippet is directly aligned with the documented RemoteCallbacks hooks, push hook gotcha, and the `depth`/`prune` controls. ([pygit2.org][1])

---

---

## R) Submodules

### R0) Mental model: “submodule = nested repo + superproject pins a commit”

A submodule is “a foreign repository embedded within a dedicated subdirectory.” In Git, the superproject typically pins a specific submodule commit (gitlink), and `git submodule update` brings the submodule working tree to what the superproject expects—usually checking out the recorded commit on a detached HEAD (unless you opt into rebase/merge update procedures). ([pygit2.org][6])

### R1) Repository-level access: `repo.submodules` + listing

* `repo.submodules` is a `SubmoduleCollection`
* `repo.listall_submodules()` returns all submodule paths in the repository ([pygit2.org][6])

### R2) `SubmoduleCollection`: add/init/status/update/get + caching

#### Add: `submodules.add(url, path, link=True, callbacks=None, depth=0) -> Submodule`

* automatically clones the submodule and adds it to the index
* `link` controls whether the workdir contains a gitlink to `.git/modules` vs the repo directly in the workdir
* supports `RemoteCallbacks`
* supports shallow clone via `depth` ([pygit2.org][6])

#### Init: `submodules.init(submodules=None, overwrite=False)`

Copies submodule info into `.git/config` (explicitly “just like git submodule init”). ([pygit2.org][6])

#### Status: `submodules.status(name, ignore=SubmoduleIgnore.UNSPECIFIED) -> SubmoduleStatus`

Returns a bitmask of status flags; `ignore` controls how deep to examine the working directory. ([pygit2.org][6])

#### Update (bulk): `submodules.update(submodules=None, init=False, callbacks=None, depth=0)`

The pygit2 semantics are explicit: update clones a missing submodule and checks out the subrepository to the **commit specified in the index** of the containing repo; if the target commit isn’t present, it will fetch. ([pygit2.org][6])

This lines up with Git’s own `git submodule update` documentation: “update registered submodules to match what the superproject expects”, and for the default “checkout” procedure, “the commit recorded in the superproject will be checked out … on a detached HEAD.” ([Git SCM][7])

**Critical gotcha (the one that breaks first implementations):** “update” does *not* mean “move submodule to its latest remote commit” — it means “move submodule to the pinned commit recorded by the superproject,” unless you explicitly do the `--remote` workflow (which changes the source of the target SHA-1 to the remote-tracking branch). ([Git SCM][7])

#### Lookup: `submodules.get(name_or_path) -> Submodule | None`

Returns `None` instead of raising if missing. ([pygit2.org][6])

#### Caching: `cache_all()` / `cache_clear()`

pygit2’s submodule cache guidance is unusually explicit and is worth treating as a design constraint:

* `.gitmodules` is unstructured; loading submodules is **O(N)**
* if you repeatedly look up each submodule individually while also needing “all submodules”, you can end up with **O(N²)** behavior
* `cache_all()` loads and caches all submodules so subsequent lookups are **O(1)**
* the cache includes repo config + worktree + index + HEAD state, so any changes there can invalidate it; `cache_clear()` resets it ([pygit2.org][6])

### R3) `Submodule` object surface

A `Submodule` exposes:

* `name`, `path`, `url`
* `branch` (tracked branch)
* `head_id` (HEAD commit id recorded in the superproject’s current HEAD tree; may be `None`)
* methods: `init(overwrite=False)`, `open() -> Repository`, `reload(force=False)`, `update(init=False, callbacks=None, depth=0)` ([pygit2.org][6])

`reload()` exists specifically because libgit2/pygit2 cache submodule info from config/index/HEAD; call it when you know `.gitmodules`/config/index/HEAD changed and you want a fresh view without rebuilding the whole collection. ([pygit2.org][6])

---

---

## S) Worktrees

### S0) Repository surface: add/list/lookup

pygit2’s worktree API is intentionally small:

* `repo.add_worktree(name, path, ref=None) -> Worktree`

  * if `ref` is specified, no new branch is created and the provided ref is checked out
* `repo.list_worktrees() -> list[str]`
* `repo.lookup_worktree(name) -> Worktree` ([pygit2.org][8])

### S1) Worktree object: name/path/prunability + prune

`Worktree` exposes:

* `name`
* `path`
* `is_prunable` (whether it can be pruned under given flags)
* `prune(force=False)` ([pygit2.org][8])

### S2) Prune semantics (libgit2 truth): what “prunable” actually means

libgit2 defines `git_worktree_is_prunable`:

* a worktree is **not prunable** if it links to a valid on-disk worktree (unless you set the `valid` override in prune options)
* a worktree is **not prunable** if it is locked (unless you set the `locked` override)
* returns 1 if prunable, 0 otherwise, and sets an error message describing why it isn’t prunable ([libgit2][9])

`git_worktree_prune` then removes the git data structures on disk, and it only proceeds if `git_worktree_is_prunable` succeeds (optionally with overrides). ([libgit2][10])

### S3) Git CLI mental model (for how users expect it to behave)

Git’s `git worktree prune` prunes worktree info in `$GIT_DIR/worktrees`, and `git worktree remove` only removes clean worktrees unless forced. This is useful when designing UX around pygit2 worktrees (e.g., explaining why prune/remove fails). ([Git SCM][11])

---

---

## T) Mailmap (identity normalization)

### T0) What mailmap does (and does not do)

Mailmap provides *canonicalization* for author/committer identity (name/email) in views and analytics; it does **not** rewrite commit history. Git describes it as mapping author/committer names/emails to canonical real names/emails based on `.mailmap` or configured mailmap sources. ([Git SCM][12])

### T1) pygit2 Mailmap API: construct + resolve

pygit2 exposes a `Mailmap` object with:

* `Mailmap.from_buffer(buffer: str) -> Mailmap` (parse a mailmap file from a string)
* `Mailmap.from_repository(repo) -> Mailmap` (load mailmap sources based on repo config)
* `add_entry(...)` (add mapping, overriding existing entries)
* `resolve(name, email) -> (real_name, real_email)`
* `resolve_signature(sig: Signature) -> Signature` ([pygit2.org][13])

### T2) Source loading order + precedence (repo mailmap “stack”)

Both pygit2 and libgit2 define the repository mailmap load order:

1. `.mailmap` in the root of the working directory (if present)
2. blob identified by `mailmap.blob` (defaults to `HEAD:.mailmap` in bare repos)
3. path identified by `mailmap.file` ([pygit2.org][13])

This ordering is intended to support precedence layering (later sources overriding earlier ones). You can see the same precedence concept in Git’s `check-mailmap` options: an explicitly provided mailmap file takes precedence over default/configured mailmap sources, and if both file and blob are specified, the file takes precedence. ([Git SCM][14])

### T3) Entry override semantics (“last mapping wins for identical keys”)

libgit2 documents that adding an entry replaces an existing entry if it already exists. pygit2’s `add_entry` mirrors that behavior (“overriding existing entries”). ([libgit2][15])

### T4) Mapping syntax and match specificity (Git spec)

Git’s `gitmailmap` defines the supported line formats:

* `Proper Name <commit@email>` (map email → name)
* `<proper@email> <commit@email>` (replace only email)
* `Proper Name <proper@email> <commit@email>` (replace name+email based on commit email)
* `Proper Name <proper@email> Commit Name <commit@email>` (replace name+email only when both commit name and commit email match)

Names and emails are matched case-insensitively. ([Git SCM][12])

### T5) “Best practice” usage in code-intel / analytics pipelines

If you’re materializing author-level metrics, the durable pattern is:

1. `mm = Mailmap.from_repository(repo)` once per repo (cache it)
2. normalize both `commit.author` and `commit.committer` via `resolve_signature`
3. write canonical fields into your tables (and keep raw fields too)

That keeps “identity normalization” as a pure view-layer transform, consistent with Git’s intent for mailmap. ([pygit2.org][13])

[1]: https://www.pygit2.org/remotes.html "Remotes — pygit2 1.19.1 documentation"
[2]: https://git-scm.com/docs/shallow?utm_source=chatgpt.com "Git - shallow Documentation"
[3]: https://libgit2.org/docs/reference/main/remote/git_remote_callbacks.html "git_remote_callbacks (libgit2 main)"
[4]: https://libgit2.org/docs/guides/authentication/ "Authenticating against a server"
[5]: https://libgit2.org/docs/reference/main/credential/git_credential_t.html "git_credential_t (libgit2 main)"
[6]: https://www.pygit2.org/submodule.html "Submodules — pygit2 1.19.1 documentation"
[7]: https://git-scm.com/docs/git-submodule "Git - git-submodule Documentation"
[8]: https://www.pygit2.org/worktree.html "Worktrees — pygit2 1.19.1 documentation"
[9]: https://libgit2.org/docs/reference/main/worktree/git_worktree_is_prunable.html "git_worktree_is_prunable (libgit2 main)"
[10]: https://libgit2.org/docs/reference/main/worktree/git_worktree_prune.html "git_worktree_prune (libgit2 main)"
[11]: https://git-scm.com/docs/git-worktree "Git - git-worktree Documentation"
[12]: https://git-scm.com/docs/gitmailmap "Git - gitmailmap Documentation"
[13]: https://www.pygit2.org/mailmap.html "Mailmap — pygit2 1.19.1 documentation"
[14]: https://git-scm.com/docs/git-check-mailmap "Git - git-check-mailmap Documentation"
[15]: https://libgit2.org/docs/reference/main/mailmap/git_mailmap_add_entry.html "git_mailmap_add_entry (libgit2 main)"


## T) Mailmap (identity normalization)

### T0) What mailmap *is* in Git terms

Mailmap is a **canonicalization layer** for author/committer names+emails: it maps the identities stored in commit signatures (often inconsistent across machines, time, or tooling) to a preferred “real name / real email” *without rewriting history*. Git looks for a `.mailmap` file in the repo (or mailmap config targets) and uses it to normalize identities in views/analytics. ([Git SCM][1])

---

### T1) pygit2 API surface: build + resolve

pygit2’s `Mailmap` object exposes the full set of primitives you need:

* `Mailmap.from_buffer(buffer: str) -> Mailmap` (parse mailmap text) ([pygit2.org][2])
* `Mailmap.from_repository(repo: Repository) -> Mailmap` (load from repo + config-defined mailmap targets) ([pygit2.org][2])
* `mailmap.add_entry(...)` (add / override mappings programmatically) ([pygit2.org][2])
* `mailmap.resolve(name, email) -> (real_name, real_email)` ([pygit2.org][2])
* `mailmap.resolve_signature(sig: Signature) -> Signature` ([pygit2.org][2])

**Identity discipline:** treat mailmap application as a *view-layer* transform. Store both raw `Signature` (as recorded) and canonicalized identity (mailmap-resolved) in downstream tables so you can audit/redo normalization when mailmap evolves.

---

### T2) Precedence / load-order rules (repo-based mailmap)

When you call `Mailmap.from_repository(repo)`, mailmaps are loaded in this order:

1. `.mailmap` at the root of the working directory (if present)
2. blob identified by `mailmap.blob` config (defaults to `HEAD:.mailmap` in bare repos)
3. path identified by `mailmap.file` config ([pygit2.org][2])

That ordering matters because it defines how multiple mailmap sources “stack” (e.g., working-tree `.mailmap` plus a blob-pinned `.mailmap` in a bare repo plus an external file configured in `mailmap.file`). ([pygit2.org][2])

**CLI precedence nuance (useful when validating):** `git check-mailmap` allows an explicit `--mailmap-file` and/or `--mailmap-blob`, and it states that if both are supplied, entries in `--mailmap-file` take precedence. That’s handy for “prove normalization” tests outside pygit2. ([Git SCM][3])

---

### T3) Mailmap syntax (what you can represent)

Git’s `gitmailmap` spec defines the line formats and matching rules:

* Simple: `Proper Name <commit@email>` maps commit email → canonical name
* Replace only email: `<proper@email> <commit@email>`
* Replace name+email when commit email matches: `Proper Name <proper@email> <commit@email>`
* Replace name+email only when both commit name and email match: `Proper Name <proper@email> Commit Name <commit@email>`
* Names/emails are matched **case-insensitively**; `#` starts comments; blank lines ignored ([Git SCM][1])

**Operational gotcha:** Git does **not follow symlinks** when reading `.mailmap` in the working tree (consistency when reading from index/tree vs filesystem). ([Git SCM][1])

---

### T4) Recommended “analytics-grade” usage pattern

A stable pattern for building commit tables with identity normalization:

```python
mm = pygit2.Mailmap.from_repository(repo)

commit = repo[oid]  # Commit
raw_author = commit.author
raw_committer = commit.committer

canon_author = mm.resolve_signature(raw_author)
canon_committer = mm.resolve_signature(raw_committer)

# Persist both: raw_author.name/email/time/offset + canon_* equivalents
```

This uses pygit2’s `from_repository` loading behavior + signature resolver directly. ([pygit2.org][2])

---

## U) Filters (custom blob filters implemented in Python)

### U0) Mental model: filters are an *in-process* transformation chain keyed by attributes

libgit2’s filter system applies transformations in two directions:

* **SMUDGE**: export ODB → working directory
* **CLEAN**: import working directory → ODB ([libgit2][4])

Filters are invoked based on **attributes** (from `.gitattributes` / attribute resolution). A filter can declare an attribute list with optional required values (including wildcards like `name=*`) that must match for the filter to run. ([libgit2][5])

---

### U1) pygit2’s filter class lifecycle: `check → write* → close`

pygit2 exposes a Python-first wrapper:

* Subclass `pygit2.Filter`
* Override `check()`, `write()`, `close()`
* Register via `pygit2.filter_register(name, filter_cls, priority=...)` ([pygit2.org][6])

**Per-stream instantiation:** a new Filter instance is created for each stream that needs filtering. The call order is explicitly: `check()` once, `write()` zero or many times, then `close()` once. ([pygit2.org][6])

**Chaining contract:** your filter must emit output by calling the provided `write_next(bytes)` callback during `write()` and/or `close()`, and **all output must be forwarded before `close()` returns**. ([pygit2.org][6])

**Pass-through control:** in pygit2, raising `Passthrough` in `check()` means “do not apply this filter”. ([pygit2.org][6])

---

### U2) Clean vs smudge directionality and ordering semantics

Both pygit2 and libgit2 define the same ordering rule:

* Filters run **in increasing priority order** on **smudge** (to workdir)
* Filters run **in reverse priority order** on **clean** (to ODB) ([pygit2.org][6])

This “reverse on clean” is critical: if your smudge does `A∘B`, then clean must do the logical inverse `B⁻¹∘A⁻¹` (or at least be compatible with it), otherwise your repo will not round-trip content stably.

---

### U3) Attribute-driven activation (and how to design it)

There are **two overlapping worlds** you should reconcile:

1. **Git’s “filter driver” attribute** (`filter=<driver>`)
   Git’s `gitattributes` defines `filter` as an attribute naming a driver configured via `filter.<driver>.clean` / `filter.<driver>.smudge` (or a long-running `process` filter). It also defines a `filter.<driver>.required` flag that upgrades missing/broken filters from “no-op” to “error”. ([Git SCM][7])

2. **libgit2’s filter registry + attribute list**
   libgit2’s `git_filter` struct includes an `attributes` string: a whitespace-separated list of attribute names, optionally with value constraints (`name=value` or `name=*`). Those values are evaluated, then your filter’s `check` / `stream` callbacks run. ([libgit2][5])

**Best practice in pygit2:** pick a *single* attribute convention and stick to it. Two common patterns:

* **Use `filter=<name>` as the activation attribute**, and in your filter class set:
  `attributes = "filter=<name>"` (or `filter=*` if you dispatch inside `check()`), then confirm driver value via `attr_values`. This mirrors how users already think about `.gitattributes filter=...`. ([Git SCM][7])
* **Use a dedicated boolean attribute**, e.g. `.gitattributes: *.secret mycrypt`, and set `attributes="mycrypt"` to activate whenever set (no value needed). This keeps the filter independent of Git’s external command driver concept, which libgit2 won’t shell out to on your behalf. ([libgit2][5])

---

### U4) Register/unregister + thread-safety constraints

pygit2 gives you:

* `pygit2.filter_register(name, filter_cls, priority=...)`
* `pygit2.filter_unregister(name)` ([pygit2.org][6])

**Registry thread-safety:** both pygit2 docs and libgit2 docs warn that the filter registry is **not thread safe**; registering/unregistering must be done when no filtering can occur (startup/shutdown). ([pygit2.org][6])

**Init/deinit behavior (libgit2 truth):** `initialize` is deferred until a filter is actually used; `shutdown` runs when removed/unregistered. ([libgit2][8])

---

### U5) Filter lifecycle mapping to libgit2 internals (so you know what’s really happening)

Under the hood, libgit2’s filter struct defines these callbacks:

* `initialize` / `shutdown`
* `check` (return pass-through to skip applying)
* `stream` (preferred; wraps a `git_writestream` chain)
* `apply` (legacy buffer-based; backward compat)
* `cleanup` (called when done filtering a file) ([libgit2][5])

pygit2’s `Filter.check/write/close` maps conceptually onto `check` + `stream` + `cleanup` in libgit2. The important bit is that **filtering is stream-oriented**, so your Python implementation should be careful about chunk boundaries (the pygit2 docs’ example buffers in a `BytesIO` explicitly to handle CRLF sequences spanning chunks). ([pygit2.org][6])

---

### U6) Built-ins + priorities: how to “fit in”

libgit2 preregisters:

* `GIT_FILTER_CRLF` (priority 0)
* `GIT_FILTER_IDENT` (priority 100) ([libgit2][8])

pygit2 defaults custom filter priority to `GIT_FILTER_DRIVER_PRIORITY`, intended to imitate a “core Git filter driver” that runs **last on checkout** and **first on check-in**. ([pygit2.org][6])

---

## V) Packing (PackBuilder + multithreading)

### V0) Why pack exists (Git semantics)

A pack is the compact storage/transfer format for Git objects:

* Objects stored compressed, often as deltas against other objects
* `.idx` enables fast random access
* Pack is the primary mechanism for efficient network transfer and efficient storage/access on disk ([Git SCM][9])

---

### V1) pygit2 high-level entrypoint: `Repository.pack(...)`

pygit2 exposes a convenience method:

`repo.pack(path=None, pack_delegate=None, n_threads=None) -> int`

It:

* builds a pack from objects selected by your `pack_delegate` (defaults to “all objects”)
* writes `.pack` and `.idx` to `path` (or default location if `None`)
* returns the number of objects written ([pygit2.org][10])

**Thread knob:** `n_threads=0` means libgit2 autodetects CPU count. ([pygit2.org][10])

---

### V2) `PackBuilder`: explicit selection + writing

`pygit2.PackBuilder(repo)` supports:

* `add(oid)` (insert one object)
* `add_recur(oid)` (recursively insert object + referenced objects)
* `set_threads(n_threads) -> int`
* `write(path=None)`
* `written_objects_count` property
* `__len__()` ([pygit2.org][10])

The recursive semantics are libgit2’s: “recursively insert an object and its referenced objects.” ([libgit2][11])

---

### V3) libgit2 packbuilder pipeline (the real algorithm boundary)

libgit2’s pack docs describe the canonical flow:

1. Insert objects into the packbuilder
2. Write out pack via `git_packbuilder_write` (or iterate via `git_packbuilder_foreach`)
3. libgit2 handles **delta ordering and generation**
4. `git_packbuilder_set_threads` controls threading for the process ([libgit2][11])

**Thread default gotcha:** libgit2 “won’t spawn any threads at all” by default; setting `0` autodetects CPUs. That means pack performance can change drastically depending on whether you explicitly set threads. ([libgit2][12])

---

### V4) Operational use cases you actually care about

#### 1) “Repack” after lots of loose objects

If you’re programmatically generating objects (blobs/trees/commits) you can end up with many loose objects. Packing consolidates them into `.pack/.idx` for better IO locality. (Git’s pack docs emphasize pack as an access-efficient archival format.) ([Git SCM][9])

#### 2) Speeding network push / replication workflows

Packs are *the* unit of efficient transfer. Creating packs explicitly can help you:

* precompute artifacts for distribution
* move computation earlier in a pipeline (especially if you set threads) ([Git SCM][9])

#### 3) “Selective pack export” for a subset of history

A `pack_delegate` can implement “include only reachable objects from these tips”:

```python
def pack_from_tip(pb: pygit2.PackBuilder):
    pb.add_recur(repo.head.target)  # include commit + trees + blobs it references

n = repo.pack(pack_delegate=pack_from_tip, n_threads=0)
```

This uses `add_recur` + thread autodetect semantics. ([pygit2.org][10])

*(If you want “all objects”, you don’t need a delegate—pygit2 defaults to all objects.)* ([pygit2.org][10])

---

## W) Storage extensibility: ODB/RefDB backends (“virtual git”)

### W0) The two databases you can swap

* **ODB** (object database): stores immutable objects keyed by OID
* **RefDB** (reference database): stores mutable refs (branches/tags/HEAD, etc.)

libgit2 explicitly supports pluggability via custom backends for ref storage and object database storage. ([Git SCM][13])

pygit2 supports custom backends for both ODB and RefDB. ([pygit2.org][14])

---

### W1) Attaching custom backends in pygit2: “frontend container + backend(s)”

#### W1.1) ODB “frontend”: `pygit2.Odb`

`Odb` supports:

* `add_backend(backend: OdbBackend, priority: int)`
* `add_disk_alternate(path)` (checked only after main backends; **writing disabled** on alternates)
* `backends` iterable
* `exists`, `read`, `write` ([pygit2.org][15])

The alternate-backend behavior matches libgit2: the alternate path must point to an `objects` directory; alternates are checked after main backends; writing is disabled. ([libgit2][16])

#### W1.2) RefDB “frontend”: `pygit2.Refdb`

`Refdb` supports:

* `Refdb.new(repo)` creates a refdb with no backend
* `Refdb.open(repo)` creates refdb with default backends
* `compress()`
* `set_backend(backend: RefdbBackend)` ([pygit2.org][15])

There’s also a libgit2 filesystem constructor: `git_refdb_backend_fs` is the default filesystem-based refdb backend that libgit2 normally constructs for you when opening a repo. ([libgit2][17])

---

### W2) “Repository with no backends” mode (the gateway to fully virtual repos)

If you construct `Repository()` with no arguments, pygit2 creates a repository with **no backends**, and warns that most operations are invalid until you provide ODB and RefDB via `set_odb()` and `set_refdb()`. ([pygit2.org][15])

That pattern is how you build “virtual git” repos that never touch `.git/objects` or `.git/refs` on disk, or that use hybrid storage.

---

### W3) Implementing a custom ODB backend in pygit2

`pygit2.OdbBackend` is subclassable and defines the minimal object-store interface:

* `exists(oid) -> bool`
* `exists_prefix(partial_oid) -> Oid` (KeyError if not found; ValueError if ambiguous)
* `read(oid)` (raw object data)
* `read_header(oid)`
* `read_prefix(prefix_oid) -> (type, data, full_oid)`
* `refresh()` ([pygit2.org][14])

**Built-in ODB backends you can compose with:**

* `OdbBackendLoose(objects_dir, compression_level, do_fsync, dir_mode=0, file_mode=0)`
* `OdbBackendPack` (packfiles) ([pygit2.org][14])

At the libgit2 level, there are corresponding constructors for loose + pack backends (`git_odb_backend_loose`, `git_odb_backend_pack`, etc.). ([libgit2][18])

**Priority ordering matters:** libgit2 checks ODB backends in relative ordering based on your `priority` parameter when adding a backend. ([libgit2][19])

---

### W4) Implementing a custom RefDB backend in pygit2

`pygit2.RefdbBackend` is subclassable and includes the core ref operations:

* `exists(refname) -> bool`
* `lookup(refname) -> Reference|None`
* `write(ref, force, who, message, old, old_target)` (write/update a ref)
* `delete(ref_name, old_id, old_target)`
* `rename(old_name, new_name, force, who, message)`
* reflog-related toggles: `ensure_log(ref_name)`, `has_log(ref_name)`
* `compress()` ([pygit2.org][14])

**Built-in RefDB backend:**

* `RefdbFsBackend(repo)` filesystem refdb backend ([pygit2.org][14])

**Reference construction for backends:** pygit2 documents that the `Reference(...)` constructor is intended for use in custom refdb backends, and references created that way “are unlikely to work as expected in other contexts.” ([pygit2.org][20])

---

### W5) “Virtual git” patterns that work well in practice

#### Pattern 1: Read-through ODB backend (CAS / object store)

Implement an `OdbBackend` that:

* answers `exists/read/read_header` from a content-addressed store keyed by OID
* supports `exists_prefix` / `read_prefix` for “human-ish” inputs and partials ([pygit2.org][14])

Then layer it behind a local pack/loose cache:

* Priority 1: `OdbBackendPack(local_objects_dir)`
* Priority 2: `OdbBackendLoose(local_objects_dir, ...)`
* Priority 3: `RemoteCasBackend(...)` (your custom backend)

…and optionally attach a shared on-disk alternate (read-only) for dedup across multiple repos. ([pygit2.org][15])

#### Pattern 2: Shared-object “fanout” via disk alternates

Use `odb.add_disk_alternate(path_to_objects)` to make multiple repos share a single object store directory for reads; alternates are checked after main backends and are read-only. ([pygit2.org][15])

This is a huge win for monorepo clones or CI fleets where many repos share most objects.

#### Pattern 3: RefDB in a transactional store (DB-backed refs)

A DB-backed `RefdbBackend` can store refs as rows (`name -> oid or symbolic target`) and implement atomic compare-and-swap semantics inside `write/delete/rename`. The method signatures explicitly include `old` / `old_target` inputs so you can enforce “expected old value” constraints. ([pygit2.org][14])

*(You’ll want to align this with the “Reference Transactions” chapter conceptually, even if the transaction mechanism is above your backend.)*

---

### W6) Pack/loose backend priority tuning (when you don’t go fully custom)

Even without writing custom backends, pygit2 exposes knobs to adjust priorities of packed vs loose ODB backends:

* `Settings.set_odb_packed_priority(priority)` (default 1)
* `Settings.set_odb_loose_priority(priority)` (default 2) ([pygit2.org][21])

This is the “soft” version of backend composition: you keep default backends but tune lookup preference (important when you’re trying to keep pack access hot and avoid loose-object churn).

---

If you want the next (final) wrap-up step for the document: we can turn these remaining chapters into a **“production recipes” appendix** (mailmap normalization pipeline, filter registration scaffolding + attribute conventions, pack/repack jobs, and a “virtual git” backend layering template with test harnesses).

[1]: https://git-scm.com/docs/gitmailmap "Git - gitmailmap Documentation"
[2]: https://www.pygit2.org/mailmap.html "Mailmap — pygit2 1.19.1 documentation"
[3]: https://git-scm.com/docs/git-check-mailmap "Git - git-check-mailmap Documentation"
[4]: https://libgit2.org/docs/reference/main/filter/index.html "filter APIs (libgit2 main)"
[5]: https://libgit2.org/docs/reference/main/sys/filter/git_filter.html "git_filter (libgit2 main)"
[6]: https://www.pygit2.org/filters.html "Filters — pygit2 1.19.1 documentation"
[7]: https://git-scm.com/docs/gitattributes/2.17.0 "Git - gitattributes Documentation"
[8]: https://libgit2.org/docs/reference/main/sys/filter/git_filter_register.html "git_filter_register (libgit2 main)"
[9]: https://git-scm.com/docs/git-pack-objects "Git - git-pack-objects Documentation"
[10]: https://www.pygit2.org/packing.html "Packing — pygit2 1.19.1 documentation"
[11]: https://libgit2.org/docs/reference/main/pack/index.html "pack APIs (libgit2 main)"
[12]: https://libgit2.org/docs/reference/main/pack/git_packbuilder_set_threads.html "git_packbuilder_set_threads (libgit2 main)"
[13]: https://git-scm.com/book/id/v2/Appendix-B%3A-Embedding-Git-in-your-Applications-Libgit2 "Git - Libgit2"
[14]: https://www.pygit2.org/backends.html "Backends — pygit2 1.19.1 documentation"
[15]: https://www.pygit2.org/repository.html "Repository — pygit2 1.19.1 documentation"
[16]: https://libgit2.org/docs/reference/main/odb/git_odb_add_disk_alternate.html?utm_source=chatgpt.com "git_odb_add_disk_alternate (libgit2 main)"
[17]: https://libgit2.org/docs/reference/main/sys/refdb_backend/git_refdb_backend_fs.html "git_refdb_backend_fs (libgit2 main)"
[18]: https://libgit2.org/docs/reference/main/odb_backend/index.html "odb_backend APIs (libgit2 main)"
[19]: https://libgit2.org/docs/reference/v1.6.3/odb/git_odb_add_backend.html?utm_source=chatgpt.com "git_odb_add_backend (libgit2 v1.6.3)"
[20]: https://www.pygit2.org/references.html "References — pygit2 1.19.1 documentation"
[21]: https://www.pygit2.org/settings.html "Settings — pygit2 1.19.1 documentation"
