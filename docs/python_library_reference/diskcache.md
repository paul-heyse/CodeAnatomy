
Below is a **section map / feature-category catalog** for a “DiskCache-advanced.md” style document (grounded in DiskCache **5.6.x** docs). The DiskCache tutorial itself already frames many of these top-level chapters (Cache, FanoutCache, DjangoCache, Deque, Index, Transactions, Recipes, Settings, Eviction Policies, Disk, Caveats, Implementation). ([Grant Jenks][1])

---

## DiskCache — exhaustive feature-category catalog (deep-dive chapters)

### 0) Scope, versioning, and mental model

* What DiskCache is: disk + file backed cache using SQLite + filesystem; “persistent cache” positioning and why it exists (disk is abundant, avoid running separate cache services). ([PyPI][2])
* Invariants you’ll rely on in later chapters:

  * dict-like API + “extra cache parameters”
  * thread-safe + process-safe, and “atomic operations” as a correctness primitive. ([Grant Jenks][1])

---

### A) Core object model and public surface

* **Primary types**: `Cache`, `FanoutCache`, `DjangoCache`, `Deque`, `Index`. ([Grant Jenks][1])
* **Supporting types / constants / errors**: `Disk`, `JSONDisk`, `Timeout`, warnings, default settings/eviction policy constants. ([Grant Jenks][3])

---

### B) `Cache`: mapping semantics + “cache-native” operations

(Everything you can do beyond a plain dict.)

* CRUD + mapping operators; open/close lifecycle, re-open behavior, and “directory as the identity”. ([Grant Jenks][1])
* Extended write/read semantics:

  * TTL/expiration (`expire=`), file-backed values (`read=True`), and **tag metadata** on keys. ([Grant Jenks][1])
  * “get with metadata” (`expire_time=True`, `tag=True`) result shapes. ([Grant Jenks][1])
* Atomic “counter-ish” ops: `incr`, `decr`, `pop` patterns (and what’s guaranteed atomic). ([Grant Jenks][1])
* Iteration ordering + sorted iteration constraints (`iterkeys`) and when sorted order is meaningful. ([Grant Jenks][1])
* Built-in queue-like primitives: `push`, `pull`, `peek` (prefix partitioning, front/back semantics). ([Grant Jenks][1])
* Introspection/ops hooks: `volume()`, `stats()`, `check()` (including “repair/reclaim” behavior). ([Grant Jenks][1])

---

### C) Expiration, tagging, and group eviction

* Expiration model:

  * lazy expiration vs explicit `expire()` calls (and why `len(cache)` can include expired items). ([Grant Jenks][1])
* Tagging:

  * `tag` metadata types, `evict(tag)` semantics, and tag-index acceleration (`tag_index=True`, create/drop tag index). ([Grant Jenks][1])

---

### D) Eviction + size management (“make disk bounded”)

* Size controls:

  * `size_limit`, `cull_limit`, and the “lazy culling vs cron job cull()” operational pattern. ([Grant Jenks][1])
* Eviction policy catalog:

  * least-recently-stored / least-recently-used / least-frequently-used / none (and their performance implications). ([Grant Jenks][1])
* Manual enforcement: `cull()` semantics (expire first, then evict until under limit). ([Grant Jenks][1])

---

### E) Concurrency & correctness primitives

* Thread/process safety and how DiskCache expects you to share a directory across processes. ([Grant Jenks][1])
* Timeout and retry model:

  * what `Timeout` means, where it can surface, and how retry is exposed. ([Grant Jenks][1])
* “Concurrent ops don’t block each other” claims and the practical boundaries (writers vs writers, etc.). ([Grant Jenks][1])

---

### F) Transactions & atomic batches

* `Cache.transact()` semantics: what it locks, what remains concurrent, nesting rules, and why transactions must be short. ([Grant Jenks][4])
* Performance patterns:

  * batching many writes into one transaction (2–5× claim in docs) and the tradeoff (blocking other writers). ([Grant Jenks][1])
* Sharding interaction:

  * why transactions are not implemented on `FanoutCache` / `DjangoCache`, and the “request a shard with transactions” pattern. ([Grant Jenks][1])

---

### G) `FanoutCache`: sharding for multi-writer scalability

* Sharding model:

  * why shards exist (writer contention), “shard per concurrent writer” heuristic, default shard count. ([Grant Jenks][1])
* Timeout semantics unique to FanoutCache:

  * `Timeout` is caught and operations abort; `set`/`delete` may silently fail; mapping operators auto-retry; “never raises Timeout”. ([Grant Jenks][1])
* Shard size-limit semantics:

  * total `size_limit` divided across shards; oversized items culled. ([Grant Jenks][1])
* Shard-scoped “sub-objects”:

  * `fanout_cache.cache(name)`, `.deque(name)`, `.index(name)` patterns (including why those matter for transactions). ([Grant Jenks][1])

---

### H) Persistent data structures on disk

* **`Deque`**:

  * deque-compatible API; persistent queue/stack; uses Cache push/pull/peek; never evicts/expires; fixed memory footprint and cross-process comms use cases. ([Grant Jenks][1])
* **`Index`**:

  * ordered mapping / ordered-dict-like semantics; inherits Cache benefits; never evicts/expires. ([Grant Jenks][1])

---

### I) Function memoization and cache-stampede controls

* `cache.memoize(...)` decorator:

  * lru_cache-like semantics + DiskCache-specific knobs (expire/tag/typed) and the “must call memoize()” footgun. ([Grant Jenks][1])
* Stampede protection:

  * `memoize_stampede` and how it differs from plain memoize. ([Grant Jenks][1])
* Rate limiting / call coordination:

  * `throttle` and `barrier` as function decorators (contract + typical use cases). ([Grant Jenks][1])

---

### J) Synchronization “recipes” (cross-thread + cross-process)

* `Lock`, `RLock`, `BoundedSemaphore` recipes (and when you use them vs Cache transactions). ([Grant Jenks][1])
* `Averager` running-average helper (common “shared stats” pattern). ([Grant Jenks][1])

---

### K) Django integration surface

* `DjangoCache` as a Django-compatible backend built on `FanoutCache`, key settings knobs (LOCATION, TIMEOUT, SHARDS, DATABASE_TIMEOUT, OPTIONS), and “never raises Timeout” semantics. ([Grant Jenks][1])
* File-handle workflows for web servers (X-Sendfile / X-Accel-Redirect patterns via `cache.read`). ([Grant Jenks][1])

---

### L) Configuration & tuning control plane (what you version)

* Settings model: stored in SQLite Settings table, loaded lazily, mutated via `reset()`. ([Grant Jenks][1])
* High-leverage knobs:

  * cache sizing/culling (`size_limit`, `cull_limit`), statistics enablement, tag index, eviction policy. ([Grant Jenks][1])
* Disk-level knobs (`disk_*`) and SQLite pragma knobs (`sqlite_*`)—journal mode (WAL), mmap sizing, synchronous mode, cache size, auto_vacuum, etc. ([Grant Jenks][1])

---

### M) Storage engine & serialization internals (`Disk`)

* Key vs value storage:

  * keys always in the metadata DB; values sometimes stored in separate files; `read=True` guarantees file storage. ([Grant Jenks][1])
* Type handling:

  * native DB types (int/float/str/bytes) vs pickle fallback. ([Grant Jenks][1])
* Custom serialization:

  * `Disk` subclassing; `JSONDisk` example and how/why to use it. ([Grant Jenks][1])

---

### N) Caveats, footguns, and operational hazards

* Key-equality is based on serialization, not Python hash/eq protocol; pickle can be inconsistent for container keys; mitigation strategies (e.g., JSONDisk for keys). ([Grant Jenks][1])
* FanoutCache silent-failure modes under timeout (and how to design around them). ([Grant Jenks][1])
* “Persistence means you delete directories yourself” lifecycle expectations. ([Grant Jenks][1])

---

### O) Implementation details (for the “how it works” appendix)

* SQLite schema/indexes, per-eviction-policy index tradeoffs, file layout, and how `check()` can reclaim space/fix inconsistencies. ([Grant Jenks][1])
* Memory-mapped / SQLite tuning rationale (how `sqlite_mmap_size`, WAL mode, etc. impact perf). ([Grant Jenks][1])

---

If you want, next step is: I can take **Chapter A (Core object model)** and turn it into your usual “full surface area” style: *API signatures → semantics → invariants/footguns → minimal runnable snippets → production patterns*, starting with `Cache` then `FanoutCache` (because it sets the concurrency posture).

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://pypi.org/project/diskcache/ "diskcache · PyPI"
[3]: https://grantjenks.com/docs/diskcache/genindex.html?utm_source=chatgpt.com "Index — DiskCache 5.6.1 documentation"
[4]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"

## Chapter A — Core object model (Cache → FanoutCache)

DiskCache’s “caching core” is built around **`Cache`** (a disk + file backed mapping), and **`FanoutCache`** (a sharded wrapper around multiple caches to reduce write contention). ([Grant Jenks][1])

---

# A.1 `diskcache.Cache`

## A.1.1 API signatures (surface area)

```python
class diskcache.Cache(
    directory: str | None = None,
    timeout: float = 60,
    disk: type[diskcache.Disk] = diskcache.Disk,
    **settings
)

# Mapping protocol (keys include expired items for iteration/len):
cache[key]          # __getitem__(key) -> value (KeyError if missing)
cache[key] = value  # __setitem__(key, value)
key in cache        # __contains__(key) -> bool
del cache[key]      # __delitem__(key, retry=True)  (KeyError if missing)

len(cache)          # includes expired
iter(cache)         # includes expired
reversed(cache)     # includes expired

# Core cache methods:
cache.set(key, value, expire=None, read=False, tag=None, retry=False) -> bool
cache.get(key, default=None, read=False, expire_time=False, tag=False, retry=False) -> value | tuple
cache.add(key, value, expire=None, read=False, tag=None, retry=False) -> bool  # atomic "insert-if-absent"
cache.delete(key, retry=False) -> bool   # missing ignored
cache.pop(key, default=None, expire_time=False, tag=False, retry=False) -> value | tuple

cache.incr(key, delta=1, default=0, retry=False) -> int
cache.decr(key, delta=1, default=0, retry=False) -> int

# Expiration / eviction:
cache.touch(key, expire=None, retry=False) -> bool
cache.expire(now=None, retry=False) -> int
cache.evict(tag, retry=False) -> int
cache.cull(retry=False) -> int
cache.clear(retry=False) -> int

# Iteration & queue primitives:
cache.iterkeys(reverse=False) -> iterator  # DB sort order; limited-type meaningfulness
cache.push(value, prefix=None, side="back", expire=None, read=False, tag=None, retry=False) -> key
cache.pull(prefix=None, default=(None, None), side="front", expire_time=False, tag=False, retry=False) -> (key, value)
cache.peek(prefix=None, default=(None, None), side="front", expire_time=False, tag=False, retry=False) -> (key, value)
cache.peekitem(last=True, expire_time=False, tag=False, retry=False) -> (key, value)

# File handle + settings + ops:
cache.read(key, retry=False) -> BinaryIO
cache.reset(key, value=ENOVAL, update=True) -> updated_value
cache.stats(enable=True, reset=False) -> (hits, misses)
cache.transact(retry=False) -> contextmanager
cache.check(fix=False, retry=False) -> list[warnings]
cache.volume() -> int

cache.directory  # property
cache.disk       # property
cache.timeout    # property
```

Signatures + many of the behavioral notes above are straight from the API reference and tutorial. ([Grant Jenks][2])

---

## A.1.2 Semantics (how `Cache` behaves)

### Identity + lifecycle

* A `Cache` is **identified by its directory**: if the directory doesn’t exist it’s created; if omitted, a **temporary directory** is used. ([Grant Jenks][3])
* `Cache` is **thread-safe** and can be shared between threads; **two Cache objects can reference the same directory from different threads or processes**, enabling cross-process communication. ([Grant Jenks][3])
* The cache maintains **one or more file handles**, but **operations are atomic** (in the DiskCache sense of “correct under concurrent access”), and caches can be **forked / pickled**. Each thread that accesses a cache should call `close()`, and a `with Cache(...)` block helps guarantee closure. ([Grant Jenks][3])
* Closed caches **re-open automatically** on access, but **opening is relatively slow**—so in long-lived processes, it’s usually fine (and intended) to leave them open. ([Grant Jenks][3])
* DiskCache is **persistent by design**: it does **not** automatically delete the directory; you remove the directory yourself to permanently delete the cache. ([Grant Jenks][3])

### Read/write semantics: `set/get/add/delete/pop`

* `set(..., expire=..., read=..., tag=...)` extends a normal mapping assignment with:

  * **expiration** (`expire` seconds),
  * **file-backed ingestion** (`read=True` means you pass a binary file-like object, and DiskCache reads bytes from it),
  * and **tag metadata** (`tag=...`). ([Grant Jenks][3])
* `get(..., read=True, expire_time=True, tag=True)` can return **a tuple** `(value_or_reader, expire_time_epoch, tag)`. With `read=True`, the “value” is a **file-like object** you read from. ([Grant Jenks][3])
* `add` is an **atomic insert-if-absent** (only one concurrent add for the same key wins). ([Grant Jenks][2])
* `delete` **ignores missing keys**, unlike `del cache[key]` which raises `KeyError` for a missing key. ([Grant Jenks][2])
* `pop` is **atomic** and **serializes concurrent pop operations** (important if you use it as a work-queue primitive). ([Grant Jenks][2])

### Ordering + “queue” built-ins

* Iteration (`for k in cache`) and `len(cache)` include **expired items** (because expiration is handled lazily unless you explicitly run `expire()`). ([Grant Jenks][2])
* `iterkeys()` iterates in **database sort order** and is only meaningfully sorted for `str`, `bytes`, `int`, and `float` keys; other key types are serialized, so “sorted order” becomes mostly meaningless. ([Grant Jenks][3])
* `push/pull/peek` implement a **queue-like structure** inside the cache by auto-assigning keys:

  * If `prefix is None`, it uses **integer keys** (starting at “500 trillion”).
  * Otherwise it uses string keys like `"prefix-integer"`. ([Grant Jenks][3])

### Transactions

* `cache.transact()` is a context manager that **locks out other writers** (reads may occur concurrently), so transactions should be short; they can be nested but **can’t be shared between threads**. ([Grant Jenks][2])

---

## A.1.3 Invariants & footguns (Cache)

1. **“Expired” doesn’t mean “gone.”**
   `len(cache)` and `list(cache)` include expired items until you call `cache.expire()` (or culling removes them). ([Grant Jenks][3])

2. **Sorted iteration is only meaningful for a subset of key types.**
   If you rely on `iterkeys()` for ordering, constrain key types to `str/bytes/int/float`. ([Grant Jenks][3])

3. **`check()` is intentionally heavyweight.**
   It holds a **writer lock** while checking the Cache table and filesystem consistency; for caches with many file references, that lock can be held long enough to matter (docs cite ~60ms for ~1,000 file references in a local benchmark). ([Grant Jenks][2])

4. **Timeout semantics are “raise by default.”**
   Many `Cache` methods document raising `diskcache.Timeout` on SQLite timeouts when `retry=False` (the default for many methods). ([Grant Jenks][2])

5. **`memoize` must be *called*.**
   `@cache.memoize(...)` is correct; `@cache.memoize` (no parentheses) raises a `TypeError`. ([Grant Jenks][3])

---

## A.1.4 Minimal runnable snippets (Cache)

### 1) Persistent cache + safe close (recommended pattern)

```python
from diskcache import Cache

with Cache("/tmp/mycache") as cache:
    cache["k"] = "v"
    assert cache["k"] == "v"
    assert ("k" in cache) is True
```

### 2) Large payloads: file-backed write + streaming read

```python
from io import BytesIO
from diskcache import Cache

cache = Cache("/tmp/mycache")
cache.set("blob", BytesIO(b"hello"), read=True, expire=60, tag="demo")

reader, expire_time, tag = cache.get("blob", read=True, expire_time=True, tag=True)
data = reader.read()
```

### 3) Atomic counters + “dump stats” pattern with `pop`

```python
from diskcache import Cache

cache = Cache("/tmp/mycache")
cache.incr("events")          # atomic increment
cache.incr("events", 10)
value = cache.pop("events")   # atomic delete+return
```

### 4) Transaction: atomically update multiple keys

```python
from diskcache import Cache

cache = Cache("/tmp/mycache")
with cache.transact():
    cache.incr("total", 123)
    cache.incr("count", 1)

with cache.transact():
    avg = cache["total"] / cache["count"]
```

(These are the same patterns the docs demonstrate, just normalized into “copy/paste” blocks.) ([Grant Jenks][3])

---

## A.1.5 Production patterns (Cache)

### Pattern 1 — “one directory = one cache contract”

Treat the directory as the durable identity. Prefer:

* a stable, app-scoped path for persistent caches,
* a temp directory only for “best-effort” caching. ([Grant Jenks][3])

### Pattern 2 — thread ownership of close()

If you share a `Cache` across threads, arrange for each thread to call `close()` (common: a thread entry/exit hook), or use `with Cache(...)` when the scope is naturally bounded. ([Grant Jenks][3])

### Pattern 3 — avoid `check()` in hot paths

Use `check()` for test/post-mortem workflows, not per-request maintenance, because it holds a writer lock and can block writers. ([Grant Jenks][2])

---

# A.2 `diskcache.FanoutCache`

FanoutCache exists because **SQLite writers block other writers**. Sharding spreads writes across multiple SQLite databases, reducing worst-case write latency under concurrency; the docs recommend roughly **one shard per concurrent writer** and default to **8 shards**. ([Grant Jenks][3])

## A.2.1 API signatures (surface area)

```python
class diskcache.FanoutCache(
    directory: str | None = None,
    shards: int = 8,
    timeout: float = 0.01,   # 10ms default
    disk: type[diskcache.Disk] = diskcache.Disk,
    **settings
)

# Mapping ops automatically call methods with retry=True:
cache[key]          # __getitem__(key) -> calls get(..., retry=True)
cache[key] = value  # __setitem__(key, value) -> calls set(..., retry=True)
del cache[key]      # __delitem__(key) -> calls delete(..., retry=True)

# Core methods mostly mirror Cache (but Timeout behavior differs):
cache.set(key, value, expire=None, read=False, tag=None, retry=False) -> bool
cache.get(key, default=None, read=False, expire_time=False, tag=False, retry=False) -> value | tuple
cache.add(key, value, expire=None, read=False, tag=None, retry=False) -> bool
cache.delete(key, retry=False) -> bool
cache.transact(retry=True) -> contextmanager   # locks all shards

# Fanout-only object model helpers:
cache.cache(name, timeout=60, disk=None, **settings) -> Cache
cache.deque(name, maxlen=None) -> Deque
cache.index(name) -> Index
```

Key points here are in the API reference (mapping ops → retry=True; `.cache(name, ...)` sub-cache) and the tutorial (default timeout 10ms; timeout swallowed). ([Grant Jenks][2])

---

## A.2.2 Semantics (what changes vs `Cache`)

### Sharding model

* FanoutCache “automatically shards the underlying database” to reduce writer blocking; the docs explicitly note **writers block other writers**, so sharding helps by distributing writes. ([Grant Jenks][3])

### Timeout + failure model (the big difference)

FanoutCache uses `timeout` to limit how long each DB transaction waits. When timeouts occur:

* The `Timeout` is raised internally, **caught by FanoutCache**, and the operation is **aborted**. ([Grant Jenks][3])
* As a result:

  * `set` and `delete` may **silently fail** unless you pass `retry=True` (or use mapping operators, which retry automatically). ([Grant Jenks][3])
  * `get` may simply return `default` on timeout (again, unless `retry=True`). ([Grant Jenks][2])
* The tutorial is explicit: **FanoutCache never raises `Timeout` to the caller**. ([Grant Jenks][3])

### Size limit is *total*, per-shard limit is divided

FanoutCache’s `size_limit` is treated as a **total** cache size; each shard’s effective size limit is `size_limit / shards`, and items larger than that per-shard limit are culled immediately. ([Grant Jenks][3])

### Transactions on FanoutCache

`fanout_cache.transact()` exists and will **lock the cache for writes**; the API reference notes it “blocks until transactions are held on all cache shards by retrying as necessary.” ([Grant Jenks][2])

---

## A.2.3 Invariants & footguns (FanoutCache)

1. **Timeouts turn into “misses” / “no-ops” unless you retry.**

* `get`: timeout → returns `default` unless `retry=True`. ([Grant Jenks][2])
* `set/delete`: timeout → can “fail silently” unless `retry=True`. ([Grant Jenks][3])

2. **Use mapping operators when you want auto-retry by default.**
   The API reference explicitly says `__getitem__/__setitem__/__delitem__` call underlying methods with `retry=True`. ([Grant Jenks][2])

3. **Oversized items can be culled immediately (per shard).**
   If you cache large blobs, you must budget per-shard `size_limit`. ([Grant Jenks][3])

4. **`transact()` on FanoutCache is “strong” but can reintroduce contention.**
   It must acquire write locks across shards; keep it short and rare in high-concurrency contexts. ([Grant Jenks][2])

---

## A.2.4 Minimal runnable snippets (FanoutCache)

### 1) Multi-process friendly counter (best “hello world” for concurrency)

```python
from diskcache import FanoutCache

cache = FanoutCache("/tmp/fanout", shards=8, timeout=0.01)

# atomic increments are safe under concurrency
cache.incr("jobs_started")
```

### 2) “I cannot tolerate silent failure” writes

```python
from diskcache import FanoutCache

cache = FanoutCache("/tmp/fanout", shards=8, timeout=0.01)

# Either:
cache["k"] = "v"                 # mapping operator uses retry=True
# Or:
ok = cache.set("k", "v", retry=True)
assert ok
```

### 3) “Best-effort cache” reads (timeouts become misses)

```python
from diskcache import FanoutCache

cache = FanoutCache("/tmp/fanout", shards=8, timeout=0.001)  # very aggressive
value = cache.get("maybe", default=None, retry=False)        # timeout => default
```

(These match the documented behavior: abort-on-timeout + retry knobs.) ([Grant Jenks][3])

---

## A.2.5 Production patterns (FanoutCache)

### Pattern 1 — choose shards from *writers*, not cores

Docs suggest a **shard per concurrent writer** (e.g., gunicorn workers or multiprocessing pool size). Start there, then validate with real load. ([Grant Jenks][3])

### Pattern 2 — treat `timeout` as an SLO lever

* Small timeout + `retry=False`: bounded latency, but timeouts manifest as misses/no-ops. ([Grant Jenks][3])
* Larger timeout and/or `retry=True`: stronger “eventually succeeds” behavior, but you may pay higher tail latency.

### Pattern 3 — isolate “critical” state in a named sub-cache

Use `fanout.cache(name, ...)` to carve out a dedicated `Cache` in a subdirectory (handy when you want different `timeout/settings`, or when you want to reason about its lifecycle independently). ([Grant Jenks][2])

### Pattern 4 — use benchmarks to sanity-check tail latency assumptions

DiskCache’s own benchmarks highlight why sharding exists: without sharding, max store/delete latency can be “unacceptably high”; FanoutCache reduces worst-case latency by distributing writes. ([Grant Jenks][4])

---

If you want the *next* chunk in the same “pyarrow-advanced” style, I’d do **Chapter B (settings + tuning control plane)** next: `DEFAULT_SETTINGS`, `reset()`, `sqlite_*` PRAGMA plumbing, and the “operational profiles” for Cache vs FanoutCache.

[1]: https://grantjenks.com/docs/diskcache/ "DiskCache: Disk Backed Cache — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[3]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[4]: https://grantjenks.com/docs/diskcache/cache-benchmarks.html "DiskCache Cache Benchmarks — DiskCache 5.6.1 documentation"

## Chapter B — `Cache`: mapping semantics + “cache-native” operations

This chapter is about **everything `diskcache.Cache` can do beyond a plain dict**: durability-by-directory, TTL, tags, file-backed values, atomic counters, ordered iteration, queue primitives, and operational introspection.

---

# B.1 CRUD + mapping operators (what’s “dict-like”, and what isn’t)

## Mapping operators and their semantics

**Read / membership**

* `cache[key]` → returns value, raises `KeyError` if missing. ([Grant Jenks][1])
* `key in cache` → `True/False` membership check. ([Grant Jenks][1])
* `cache.get(key, default=..., ...)` → returns `default` if missing; can optionally return file handles and metadata tuples. ([Grant Jenks][1])

**Write**

* `cache[key] = value` → stores value (no TTL/tag/file semantics unless you use `set`). ([Grant Jenks][2])
* `cache.set(key, value, expire=..., read=..., tag=...)` → the “cache-native” write with TTL, file-backed ingest, tags. ([Grant Jenks][1])

**Delete**

* `del cache[key]` → deletes item; raises `KeyError` if missing; `__delitem__` exposes a `retry` argument (default `True`) to mitigate SQLite timeout contention. ([Grant Jenks][1])
* `cache.delete(key)` → deletes item; **missing keys are ignored**; returns `True` if deleted; exposes `retry` (default `False`) and can raise `Timeout` when not retrying. ([Grant Jenks][1])

**Delete+return**

* `cache.pop(key, default=..., expire_time=..., tag=...)` → atomic remove+return; supports returning metadata (expire_time/tag). ([Grant Jenks][1])

### The two biggest “not-a-dict” differences

1. `len(cache)` and `for k in cache` **include expired keys** (expiration is lazy unless you call `expire()`). ([Grant Jenks][2])
2. Many methods accept `retry=` and may raise `diskcache.Timeout` if SQLite waits exceed the connection timeout and `retry=False`. ([Grant Jenks][1])

---

# B.2 Open/close lifecycle, re-open behavior, “directory = identity”

## Directory identity

* A `Cache` is **defined by its directory**: if the directory doesn’t exist, DiskCache creates it; if you omit it, DiskCache creates a temp directory. ([Grant Jenks][2])
* Two `Cache` objects can point at the **same directory** from different threads/processes (cross-process coordination is a first-class use case). ([Grant Jenks][2])

## Close semantics and re-open behavior

* Cache objects keep one or more file handles open; **each thread that accesses a cache should call `close()`**, or use a context manager. ([Grant Jenks][2])
* A closed cache will **automatically re-open on access**, but (per docs) opening is relatively slow, so long-lived processes often leave caches open. ([Grant Jenks][2])
* Caches are **persistent** and do **not** delete their directory automatically; you delete the directory yourself when you want to destroy the cache. ([Grant Jenks][2])

**Minimal pattern**

```python
from diskcache import Cache

with Cache("/var/tmp/mycache") as cache:
    cache["k"] = "v"
    assert cache["k"] == "v"
```

(Using `with` guarantees `close()` is called.) ([Grant Jenks][2])

---

# B.3 Extended write/read semantics: `expire=`, `read=True`, `tag=`

## `expire=` (TTL)

* `expire=<seconds>` sets **seconds until expiration**; `None` means no expiration. ([Grant Jenks][1])
* Expiration is *lazy* in normal reads/iteration; `cache.expire()` is the explicit “remove expired keys now” operation. ([Grant Jenks][2])

## `read=True` (file-backed ingest + file handle retrieval)

* On `set(..., read=True)` or `add(..., read=True)`, the `value` must be a **binary file-like object**, and DiskCache reads bytes from it for storage. ([Grant Jenks][1])
* On `get(..., read=True)`, DiskCache returns a **file handle** to the stored value instead of materializing it into memory. ([Grant Jenks][1])

**Minimal pattern**

```python
from io import BytesIO
from diskcache import Cache

cache = Cache("/tmp/mycache")
cache.set("blob", BytesIO(b"hello"), read=True, expire=60, tag="bin")

fh, exp_ts, tag = cache.get("blob", read=True, expire_time=True, tag=True)
data = fh.read()
```

This is exactly the intended “large payload” flow. ([Grant Jenks][2])

## `tag=` (metadata for bulk operations)

* `tag` is stored alongside the key and can be used later for `evict(tag)` (bulk removal). ([Grant Jenks][2])
* In the tutorial, tag values are described as being among primitive types (int/float/str/bytes/None). ([Grant Jenks][2])

---

# B.4 “Get with metadata”: `expire_time=True`, `tag=True` return shapes

## `get(...)` return shapes

* Default: `cache.get(key)` → value (or `default` if missing). ([Grant Jenks][1])
* With `expire_time=True` and/or `tag=True`, the return becomes a tuple that appends those fields. ([Grant Jenks][1])
* The tutorial explicitly shows: `(value_or_reader, timestamp_seconds_since_epoch, tag)` when both flags are `True` and `read=True`. ([Grant Jenks][2])

## `pop(...)` return shapes (similar, but no `read=`)

* `cache.pop(key, expire_time=True, tag=True)` returns `(value, expire_time, tag)` (where `expire_time` can be `None` for non-expiring entries). ([Grant Jenks][2])
* `pop` does **not** support `read=` (per tutorial). ([Grant Jenks][2])

---

# B.5 Atomic “counter-ish” ops: `add`, `incr`, `decr`, `pop`

DiskCache leans hard on **atomic single-key operations** so you can safely do coordination across threads/processes without rolling your own locks for common patterns.

## `add` (atomic “insert if absent”)

* `add` is like `set`, but only succeeds if the key is not present.
* Guarantee: **only one concurrent `add` for a given key can succeed**. ([Grant Jenks][1])

**Pattern: cross-process dedupe / idempotency**

```python
if cache.add(f"seen:{event_id}", True, expire=3600):
    process_event()
else:
    pass  # already seen
```

(That `add` “winner-takes-all” property is the whole trick.) ([Grant Jenks][1])

## `incr` / `decr` (atomic counters)

* Tutorial: increment/decrement are **atomic**, and are designed to use SQLite integer storage; SQLite supports 64-bit signed integers. ([Grant Jenks][2])
* API (decr): “All concurrent decrement operations will be counted individually”; negative values are supported (can decrement below zero). ([Grant Jenks][1])

**Patterns**

* Monotonic IDs: `job_id = cache.incr("job:id")`
* Rate-ish counters: `cache.incr(f"hits:{minute_bucket}")`

## `pop` (atomic “dump and reset”)

* `pop` is **atomic** and explicitly **serializes concurrent `pop` operations**. ([Grant Jenks][1])
* Tutorial calls out `incr` + `pop` as an “accurate method for counting and dumping statistics” in long-running systems. ([Grant Jenks][2])

**Pattern: exact “read-and-reset”**

```python
cache.incr("events")
# later…
events = cache.pop("events", default=0)  # atomic reset
```

([Grant Jenks][2])

---

# B.6 Iteration ordering + `iterkeys` (when “sorted” means anything)

## Insertion order vs DB sort order

* Default iteration (and `list(cache)`) is in insertion order (and includes expired items). ([Grant Jenks][2])
* `cache.iterkeys()` iterates keys in **database sort order**. ([Grant Jenks][1])

## When sorted order is meaningful

* Tutorial: DB sort order is only “valid” (i.e., sensible) for key types `str`, `bytes`, `int`, `float`; other key types are serialized, which makes the ordering effectively meaningless. ([Grant Jenks][2])

**Minimal demo**

```python
from diskcache import Cache
cache = Cache("/tmp/mycache2")
for k in "cab":
    cache[k] = None

print(list(cache))             # insertion order: c, a, b
print(list(cache.iterkeys()))  # db sort order: a, b, c
```

([Grant Jenks][2])

## `peekitem` for first/last by insertion order

If you just want “first” or “last” in insertion order, `peekitem(last=True/False)` is more efficient than iterating. ([Grant Jenks][2])

---

# B.7 Queue primitives: `push`, `pull`, `peek` (prefix partitioning + sides)

DiskCache implements a queue-like structure *inside the keyspace* using automatically assigned keys.

## Key assignment + prefix partitioning

* If `prefix=None`, queue items use **integer keys**; otherwise string keys of the form `"prefix-integer"`. ([Grant Jenks][1])
* The integer sequence begins at **500 trillion**. ([Grant Jenks][1])

## “Front/back” semantics

* `side='front'|'back'` controls which end you push/pull/peek. ([Grant Jenks][1])

## Atomicity + concurrency

* `pull` is **atomic** and explicitly **serializes concurrent `pull` operations**. ([Grant Jenks][1])
* (Same family as `pop`: these are designed for safe concurrent consumption.) ([Grant Jenks][1])

## Metadata

* Tutorial notes `push/pull/peek` support cache metadata like expiration time and tags (similar to `set/get`). ([Grant Jenks][2])

**Minimal “multi-queue in one cache”**

```python
from diskcache import Cache
cache = Cache("/tmp/qcache")

cache.push({"job": 1}, prefix="jobs", tag="work")
cache.push({"job": 2}, prefix="jobs", tag="work")
cache.push("audit-event", prefix="events", expire=60)

key, job = cache.pull(prefix="jobs")   # consumes from jobs queue
```

([Grant Jenks][1])

**Production pattern:** use `prefix` as your “queue name” (e.g., `jobs:high`, `jobs:low`) and treat the returned key as the durable handle for acknowledgements/logging.

---

# B.8 Introspection/ops hooks: `volume()`, `stats()`, `check()`

## `volume()`

* Returns an **estimated total size in bytes** of the cache directory on disk. ([Grant Jenks][2])

## `stats(enable=..., reset=...)`

* Returns `(hits, misses)`; statistics must be enabled first. ([Grant Jenks][1])
* Docs: stats are disabled by default because they add overhead; increment/decrement operations are not counted in hit/miss stats. ([Grant Jenks][2])

**Pattern: temporary instrumentation**

```python
cache.stats(enable=True)
# run workload
hits, misses = cache.stats(enable=False, reset=True)
```

([Grant Jenks][2])

## `check(fix=False)`

* Tutorial: verifies cache consistency; can fix inconsistencies and reclaim unused space; returns a list of warnings. ([Grant Jenks][2])
* API: intended for testing/post-mortem; holds a **writer lock** while checking and may be slow for caches with many file references (docs give ~60ms for ~1,000 file references in a local benchmark). ([Grant Jenks][1])

**Production posture:** treat `check()` like `fsck`—run it offline or during maintenance windows, not in hot paths.

---

# B.9 Extra operational nuance worth baking into your “advanced” doc

## Bulk removals are iterative (and timeouts carry partial progress)

Several bulk operations remove items in chunks; concurrent writes may happen between iterations. The API notes this explicitly for operations like `clear`, `expire`, and `evict`, and also notes that if a `Timeout` occurs, the first element of `Timeout.args` contains the number of items removed so far. ([Grant Jenks][1])

---

# B.10 Footguns checklist (the ones you’ll want to call out in bold in your doc)

* **Expired items still show up in `len(cache)` and iteration** until you call `expire()`. ([Grant Jenks][2])
* **Sorted iteration is only meaningful for primitive key types** (`str/bytes/int/float`); otherwise key serialization makes ordering meaningless. ([Grant Jenks][2])
* **`check()` holds a writer lock and can be slow** on caches with many file-backed entries. ([Grant Jenks][1])
* **Cache directories persist until you delete them manually.** ([Grant Jenks][2])

---

If you want to continue strictly “per the outline”, the next deep dive would be **Chapter C (Expiration, tagging, and group eviction)** — i.e., `expire/evict/touch`, tag indexes, and the operational patterns around lazy culling vs cron-driven maintenance.

[1]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"

## Chapter C — Expiration, tagging, and group eviction

This chapter is the “lifecycle control plane” for DiskCache entries:

* **Expiration**: per-key TTL → stored as an absolute `expire_time` (epoch seconds), removed lazily unless you force cleanup. ([Grant Jenks][1])
* **Tagging**: per-key *group label* → bulk invalidation via `evict(tag)`, optionally accelerated by a **tag index**. ([Grant Jenks][1])
* **Maintenance**: “lazy” culling during writes (`cull_limit`) vs a cron/systemd timer that periodically calls `expire()`/`cull()`. ([Grant Jenks][1])

---

# C.1 API surface (what you’ll use)

Core lifecycle methods on `Cache`:

```python
cache.set(key, value, expire=None, tag=None, read=False, retry=False) -> bool
cache.get(key, default=None, expire_time=False, tag=False, read=False, retry=False) -> value | tuple

cache.touch(key, expire=None, retry=False) -> bool
cache.expire(now=None, retry=False) -> int
cache.evict(tag, retry=False) -> int

cache.cull(retry=False) -> int              # size enforcement (expire + evict-by-policy)
cache.create_tag_index() -> None
cache.drop_tag_index() -> None
```

Semantics that matter in production:

* `expire()` / `evict()` / `cull()` remove items **iteratively** (batch-by-batch), allowing concurrent writes between iterations; on `Timeout`, the first element of `Timeout.args` contains “items removed so far.” ([Grant Jenks][2])
* `touch()` updates the stored expiration of an existing key (and returns `False` if the key doesn’t exist). ([Grant Jenks][1])

---

# C.2 Expiration model

## C.2.1 TTL is set at write time (`expire=`) and stored as an absolute timestamp

* `cache.set(..., expire=5)` means “expire in 5 seconds.”
* When you ask for metadata, `get(..., expire_time=True)` returns the **absolute expire time** as **seconds since epoch** (`float`). ([Grant Jenks][1])

Example (mirrors the tutorial’s return shape):

```python
from diskcache import Cache
from io import BytesIO

cache = Cache("/tmp/dc-expire")
cache.set("k", BytesIO(b"v"), expire=5, read=True, tag="data")

reader, expire_ts, tag = cache.get("k", read=True, expire_time=True, tag=True)
assert isinstance(expire_ts, float)
assert tag == "data"
```

([Grant Jenks][1])

## C.2.2 Expiration is **lazy** unless you force cleanup

DiskCache intentionally does not guarantee that “expired == physically removed” without cleanup:

* If you disable automatic culling (`cull_limit=0`), set many keys that expire immediately, then `len(cache)` and iteration still show them.
* Calling `cache.expire()` removes expired keys and returns the count removed.
* This works **regardless** of `cull_limit`. ([Grant Jenks][1])

The tutorial demonstrates this exact behavior (expired keys still counted/iterated, then removed by `expire()`). ([Grant Jenks][1])

## C.2.3 `expire(now=...)` is “purge everything older than now”

From the API:

* `expire(now=None)` uses `time.time()` if `now` is `None`.
* Removal is iterative and may interleave with concurrent writes.
* On `Timeout`, you can still inspect partial progress via `Timeout.args[0]`. ([Grant Jenks][2])

Operationally, `now=` is mainly useful if you want multiple expiration operations to share a single “cutoff time” (e.g., in tests or structured maintenance runs). ([Grant Jenks][2])

---

# C.3 `touch`: update expiry (including “remove expiry”)

`touch(key, expire=...)`:

* returns `True` if it updated the key,
* returns `False` if the key doesn’t exist, and
* `expire=None` means “no expiry” (make it persistent). ([Grant Jenks][1])

Minimal pattern:

```python
from diskcache import Cache
cache = Cache("/tmp/dc-touch")

cache.set("session", {"u": 1}, expire=60)
cache.touch("session", expire=60)   # extend TTL
cache.touch("session", expire=None) # make non-expiring
```

([Grant Jenks][1])

### Sliding-expiration (common production recipe)

“Sliding TTL” = refresh expiry when the key is accessed.

Best-effort version (simple, usually fine):

```python
val = cache.get(k)
if val is not None:
    cache.touch(k, expire=300)
```

Stronger version (avoid races between `get` and `touch` by using a transaction; this blocks other writers briefly):

```python
with cache.transact():
    val = cache.get(k)
    if val is not None:
        cache.touch(k, expire=300)
```

Transactions are “no other writes may occur while locked,” so keep this short. ([Grant Jenks][2])

---

# C.4 Tagging & group eviction (`tag=...` + `evict(tag)`)

## C.4.1 Tag semantics

* `tag` is metadata stored with the key.
* Default tag is `None`.
* Tags may be **int, float, str, bytes, or None**. ([Grant Jenks][1])

## C.4.2 `evict(tag)` removes *all* keys with that tag

From the tutorial:

* `evict('even')` removes all “even” tagged keys (example removes 50 out of 100). ([Grant Jenks][1])

From the API:

* `evict()` is iterative (batch deletes), allows concurrent writes between iterations, and exposes partial progress on `Timeout` via `Timeout.args[0]`. ([Grant Jenks][2])

### Practical “group invalidation” patterns you’ll likely want

**Pattern A — deploy/version busting**
Tag everything with the current “schema/model version”:

```python
TAG = "v7"  # build id, schema hash, model rev
cache.set(key, value, expire=3600, tag=TAG)
# on rollout:
cache.evict("v6")
```

(Exactly what `evict` is for.) ([Grant Jenks][2])

**Pattern B — “namespace” tags (per dataset / per customer)**
Use a structured tag like `f"cust:{cust_id}"` and evict per customer when their upstream data changes.

**Pattern C — hybrid: TTL + tag**
TTL handles “eventual staleness,” tags handle “immediate invalidation” when you *know* the upstream changed.

---

# C.5 Tag indexes (`tag_index=True`, `create_tag_index`, `drop_tag_index`)

## C.5.1 What it does

The tutorial: *“To accelerate eviction of items by tag, initialize the cache with `tag_index=True`.”* ([Grant Jenks][1])

Settings docs: `tag_index` defaults to `False` and is described as “create a database tag index for evict.” ([Grant Jenks][1])

## C.5.2 How to enable it (recommended: at initialization)

```python
cache = Cache("/tmp/dc-tags", tag_index=True)
```

This is the preferred approach vs toggling later. ([Grant Jenks][1])

## C.5.3 Managing tag indexes after-the-fact

DiskCache exposes:

* `cache.create_tag_index()`
* `cache.drop_tag_index()`

…but the tutorial explicitly says to prefer initialization with a tag index. ([Grant Jenks][1])

## C.5.4 Under the hood (useful for “advanced” documentation)

In the library source, `create_tag_index()` executes:

* `CREATE INDEX IF NOT EXISTS Cache_tag_rowid ON Cache(tag, rowid)`
* and updates the `tag_index` setting. ([Arcovid19][3])

This explains *why* `evict(tag)` becomes much cheaper on large caches: without an index, tag scans are inherently more expensive.

**Tradeoff note (inference):** maintaining an extra SQLite index typically adds overhead to writes/updates. Enable `tag_index` when you actually use `evict` at scale; otherwise keep it off. (Docs state the performance motivation; the write-overhead point is the usual SQLite index tradeoff.) ([Grant Jenks][1])

---

# C.6 Maintenance: lazy culling vs cron-driven cleanup

DiskCache separates **“removing expired items”** from **“keeping disk usage bounded”**:

## C.6.1 Lazy culling during writes (`cull_limit`)

In Settings:

* `cull_limit` (default **10**) = “max number of keys to cull when adding a new item.”
* Set `cull_limit=0` to disable automatic culling.
* Some systems do this and run a cron-like job that calls `cull()` in a separate process. ([Grant Jenks][1])

Key operational implication:

* With lazy culling, you amortize deletion cost across normal writes (good for request/response systems). ([Grant Jenks][1])

## C.6.2 “Hard enforce” size limits (`cull()`)

The tutorial is explicit:

* `cull()` starts by removing expired items,
* then uses the eviction policy to remove items until `volume() < size_limit`. ([Grant Jenks][1])

And, like `expire()`/`evict()`, it still works even if `cull_limit=0`. ([Grant Jenks][1])

## C.6.3 Cron/systemd timer pattern (recommended for very large caches)

When you want tighter control over tail latency on writes:

1. Disable automatic culling:

```python
cache.reset("cull_limit", 0)  # or initialize with cull_limit=0
```

([Grant Jenks][1])

2. Periodically run maintenance (separate process):

```python
from diskcache import Cache

cache = Cache("/var/cache/myapp", cull_limit=0)
cache.expire()  # remove expired
cache.cull()    # enforce size_limit (also expires first)
cache.close()
```

Concurrency note: the tutorial says these removal methods are designed to work concurrently with other cache usage across threads/processes. ([Grant Jenks][1])

---

# C.7 Footguns checklist (boldworthy in your “advanced” doc)

* **Expired entries can remain visible** in `len(cache)` / iteration until `expire()` (or `cull()`) runs. ([Grant Jenks][1])
* **`touch` does not create missing keys** (`False` for missing). ([Grant Jenks][1])
* **Bulk removals are iterative**, and **timeouts can produce partial completion** (check `Timeout.args[0]`). ([Grant Jenks][2])
* **Tag eviction at scale wants `tag_index=True`**; otherwise it can be much slower. ([Grant Jenks][1])

---

If you want to continue per the outline, the next chapter is **D) Eviction + size management** (size_limit/cull_limit in more depth, eviction policy internals, and how to choose between “least-recently-stored / used / frequently-used / none” with measurable hit-rate and write-amplification tradeoffs).

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[3]: https://arcovid19.readthedocs.io/en/latest/_modules/diskcache/core.html "diskcache.core — arcovid19 0.6b documentation"

## Chapter D — Eviction + size management (`size_limit`, `cull_limit`, `eviction_policy`)

DiskCache’s size control is **not “magic background GC”**. It’s a set of **durable settings** plus a **culling pipeline**:

* `size_limit` sets the **target maximum on-disk size** (approximate). ([Grant Jenks][1])
* `cull_limit` controls how much “maintenance work” DiskCache is allowed to do **during normal `set/add` operations**. ([Grant Jenks][1])
* `eviction_policy` determines **which *non-expired* keys get removed** when DiskCache must free space. ([Grant Jenks][1])

---

# D.1 The knobs (and what is actually enforced)

### `size_limit` (bytes): the target cap

* Default: **~1 GiB**. ([Grant Jenks][1])
* Meaning: “maximum on-disk size of the cache” (approximate). ([Grant Jenks][1])
* Important: DiskCache uses `volume()` as an **estimated** on-disk size metric. ([Grant Jenks][1])

### `cull_limit`: how aggressive *automatic* cleanup is

* Default: **10**. ([Grant Jenks][1])
* Meaning: “maximum number of keys to cull when adding a new item” / “maximum number of items culled during set or add operations.” ([Grant Jenks][1])
* Set `cull_limit=0` to disable automatic culling; a common pattern is to run a cron/systemd-timer job that calls `cull()` separately. ([Grant Jenks][1])

### `eviction_policy`: how “free space” decisions are made

Default is **`"least-recently-stored"`**. ([Grant Jenks][1])

DiskCache expects **all clients** sharing the same cache directory to use the **same** eviction policy. ([Grant Jenks][1])

---

# D.2 The culling pipeline (when/why size drops)

DiskCache reduces size through **culling** in two ways:

## D.2.1 Automatic, “lazy” culling (during `set/add`)

When you write (`set` / `add`), DiskCache may cull **up to `cull_limit` items** as part of the write path. ([Grant Jenks][1])

This is why the defaults exist: DiskCache was designed to avoid the classic “file cache cleanup takes forever in request/response” trap by doing bounded work per operation (instead of huge scans/deletes). ([Grant Jenks][1])

**Implication:** if your workload can tolerate a bit of extra work on writes, leave `cull_limit` nonzero (default 10). If write-tail latency is sacred, disable it and move culling to a scheduled job.

## D.2.2 Manual culling (`cache.cull()`)

`cull()` is the “make it fit now” operation:

> It **begins by removing expired items**, then uses the **eviction policy** to remove items until `volume() < size_limit`. ([Grant Jenks][1])

And critically: `cull()` still works even if `cull_limit=0`. ([Grant Jenks][1])

**Iterative + concurrent:** `cull` (and `expire/evict/clear`) removes items iteratively; concurrent writes may occur between iterations, and on `Timeout` the exception includes “how many items were removed so far” in `Timeout.args[0]`. ([Grant Jenks][2])
The tutorial also emphasizes these methods are intended to operate concurrently and “do not block readers or writers in other threads or processes.” ([Grant Jenks][1])

---

# D.3 Eviction policy internals (what metadata changes, and why it matters)

DiskCache’s eviction policies are implemented by tracking per-key metadata in SQLite and relying on indexes for the selection queries (“always use a SQLite index for queries” is explicitly listed as a performance technique). ([Grant Jenks][1])

## D.3.1 `"least-recently-stored"` (LRS) — default

* Metadata: each item records **time stored**.
* Internals: DiskCache “adds an index to that field.”
* Access behavior: **no update on access** (so reads stay reads).
* Eviction order: evict “oldest stored keys” first. ([Grant Jenks][1])

**Why it exists:** DiskCache is intended for large (GB-scale) caches; LRS is “usually good enough” while keeping gets lightweight. ([Grant Jenks][1])

**Write amplification profile:**

* `get` → no DB write for policy bookkeeping
* `set/delete` → normal writes only

## D.3.2 `"least-recently-used"` (LRU)

* Metadata: **access time** stored per key, indexed.
* Access behavior: “On every access, the field is updated” → “every access into a read and write.” ([Grant Jenks][1])

**Tradeoff:** better “true recency” eviction, but heavier write load (and therefore more contention potential in SQLite). The docs call out the slowdown explicitly. ([Grant Jenks][1])

**Write amplification profile:**

* `get` → DB write (update access time)
* `set/delete` → DB writes

## D.3.3 `"least-frequently-used"` (LFU)

* Metadata: **access count** stored per key, indexed.
* Access behavior: “On every access, the field is incremented” → access requires writing, slowing accesses. ([Grant Jenks][1])

**Tradeoff:** can be excellent when a stable “heavy hitters” set exists, but it is also write-heavy on reads. ([Grant Jenks][1])

**Write amplification profile:**

* `get` → DB write (increment count)
* `set/delete` → DB writes

## D.3.4 `"none"` (no eviction)

* Evictions are disabled; cache can grow without bound.
* Expired items are still removed lazily.
* DiskCache’s persistent structures `Deque` and `Index` use `"none"`. ([Grant Jenks][1])
* The docs explicitly point to `cull_limit` for “lazy culling” behavior in this mode. ([Grant Jenks][1])

**Operational note:** DiskCache recipes (Lock/RLock/Semaphores/Throttle/Barrier/Averager) assume their keys won’t be evicted; the API docs recommend setting eviction policy to `"none"` to guarantee that. ([Grant Jenks][2])

---

# D.4 Policy selection (hit-rate vs write amplification vs contention)

A practical decision rule (based on DiskCache’s own policy descriptions):

### Pick **LRS** when…

* You want the fastest reads and are OK with “recency-ish” eviction based on store time.
* Your workload is read-heavy and/or highly concurrent.
* You’re operating at GB scale where “good enough” eviction is preferable to write-heavy gets. ([Grant Jenks][1])

### Pick **LRU** when…

* True recency matters (hot set shifts quickly), and the extra writes per read are acceptable.
* You’re willing to pay: each `get` becomes a write transaction. ([Grant Jenks][1])

### Pick **LFU** when…

* Your access distribution is skewed and stable (“heavy hitters”), and you can afford write-heavy reads.
* You accept: each `get` increments a counter (DB write). ([Grant Jenks][1])

### Pick **NONE** when…

* You’re using DiskCache as a durable coordination substrate (locks/semaphores) or durable collections (`Deque`/`Index`).
* You’ll manage growth via TTL + periodic maintenance or external lifecycle controls. ([Grant Jenks][1])

---

# D.5 “Measurable tradeoffs”: how to evaluate policies in your workload

DiskCache explicitly calls out two built-in measurement hooks:

## D.5.1 Hit/miss rate via `stats()`

* `stats()` returns hits/misses, but **statistics are disabled by default** due to overhead.
* The tutorial says stats are “useful when evaluating different eviction policies.” ([Grant Jenks][1])

Minimal harness:

```python
import time
from diskcache import Cache

def run(policy: str, seconds: float = 2.0) -> dict:
    cache = Cache("/tmp/dc-policy", eviction_policy=policy, size_limit=200_000_000)  # 200MB-ish
    cache.clear()
    cache.stats(enable=True)

    # crude mixed workload: hot set + cold churn
    start = time.perf_counter()
    i = 0
    while time.perf_counter() - start < seconds:
        k = f"hot:{i % 1000}"
        cache.get(k)
        cache.set(k, i)
        cache.get(f"cold:{i}")
        i += 1

    hits, misses = cache.stats(enable=False, reset=True)
    return {"policy": policy, "hits": hits, "misses": misses, "miss_rate": misses / max(1, hits + misses)}

for p in ["least-recently-stored", "least-recently-used", "least-frequently-used"]:
    print(run(p))
```

(You’ll usually add `volume()` sampling before/after to confirm it’s actually evicting under your `size_limit`.) ([Grant Jenks][1])

## D.5.2 Latency percentiles (your own, DiskCache-style)

DiskCache’s benchmark pages emphasize percentile-based reporting (median / p90 / p99 / max) and warn synthetic benchmarks differ from production. ([Grant Jenks][3])
Do the same with your policy harness: record durations of `get/set` and compute percentiles.

---

# D.6 Operational patterns (how you keep size under control reliably)

## Pattern 1 — “write path must be clean”: disable lazy culling + schedule maintenance

* Set `cull_limit=0` to disable automatic culling. ([Grant Jenks][1])
* Run a scheduled job that calls `expire()` then `cull()` (or just `cull()`, which starts by removing expired keys). ([Grant Jenks][1])

## Pattern 2 — set eviction policy at initialization (don’t flip it later)

The tutorial is explicit:

* you *can* change `eviction_policy` via `reset()`, **but old indexes are not dropped**;
* so you should prefer setting `eviction_policy` at initialization. ([Grant Jenks][1])

## Pattern 3 — FanoutCache: remember per-shard limits

If you later move to `FanoutCache`, its `size_limit` is treated as a **total**, divided across shards; items larger than a shard’s limit are “immediately culled.” ([Grant Jenks][1])

---

If you want to keep following the outline, the next deep dive is **E) Concurrency & correctness primitives** (Timeout/retry semantics, atomicity boundaries, and how eviction + LRU/LFU writes interact with writer contention—plus when FanoutCache is mandatory).

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[3]: https://grantjenks.com/docs/diskcache/cache-benchmarks.html "DiskCache Cache Benchmarks — DiskCache 5.6.1 documentation"

## Chapter E — Concurrency & correctness primitives

DiskCache’s concurrency story is mostly “SQLite + filesystem, packaged into a cache API”:

* **Thread-safe + process-safe**: multiple threads/processes can share a single cache directory. ([Grant Jenks][1])
* **Atomic operations**: many single-key and queue primitives are explicitly atomic/serialized, making them safe building blocks for cross-process coordination. ([Grant Jenks][1])
* **SQLite concurrency reality**: with WAL, readers and writers can run concurrently, but **writers still serialize** (only one writer at a time). ([Grant Jenks][1])
* **FanoutCache exists** to reduce *writer-writer* contention by sharding writes across multiple SQLite DBs. ([Grant Jenks][1])

---

# E.1 Concurrency model (what actually blocks what)

### E.1.1 Cache directory sharing (threads + processes)

DiskCache `Cache` objects are explicitly described as:

* **thread-safe**, shareable between threads,
* **process-safe**, multiple `Cache` objects can reference the same directory across processes,
* and “all Cache operations are atomic.” ([Grant Jenks][1])

### E.1.2 SQLite journaling mode & what it implies

DiskCache’s docs call out that it uses SQLite and (as a performance technique) uses **SQLite write-ahead logging (WAL)** “so reads and writes don’t block each other.” ([Grant Jenks][1])
SQLite’s own WAL documentation clarifies the shape of that concurrency:

* WAL allows readers and writers to proceed concurrently (“readers do not block writers and a writer does not block readers”). ([SQLite][2])
* **But there can only be one writer at a time** because there is only one WAL file. ([SQLite][2])

That single-writer property is the root cause of most “database is locked / timeout” behaviors under load.

### E.1.3 “Same host only” and network filesystem footgun

SQLite (and DiskCache by extension) is a **local-filesystem** design:

* WAL requires a shared-memory wal-index; SQLite explicitly states WAL “does not work over a network filesystem” and all processes must be on the same host. ([SQLite][2])
* SQLite warns that network filesystem sync and locking can be unreliable and can lead to corruption because SQLite relies on exclusive locks for writes. ([SQLite][3])
* DiskCache repeats this at the library level: SQLite is “not recommended” for NFS mounts and users see poor behavior on some shared-folder environments. ([Grant Jenks][1])

**Practical invariant:** share a DiskCache directory across processes **on the same machine** (local disk/SSD), not across machines via NFS.

---

# E.2 Timeout & retry semantics (Cache vs FanoutCache)

## E.2.1 What `timeout` means

DiskCache uses SQLite transactions for every operation that writes to the database. The `timeout` parameter sets “how long to wait for database transactions”; on timeout, `diskcache.Timeout` is raised internally. ([Grant Jenks][1])

## E.2.2 `Cache` behavior: timeouts may propagate

For `Cache`, many methods explicitly state:

* **If a database timeout occurs and `retry=False`, a `Timeout` may be raised** to the caller. ([Grant Jenks][4])

Example: `get`, `set`, `delete`, `expire`, etc. all document raising `Timeout` when `retry=False`. ([Grant Jenks][4])

### When to use `Cache` semantics

Use `Cache` when:

* a timeout should be **visible** (you want to fail fast or alert),
* you have **low/moderate** concurrent writers,
* or you’re building “correctness-first” coordination flows where silent failure is unacceptable.

## E.2.3 `FanoutCache` behavior: timeouts are caught and operations abort

For `FanoutCache`:

* it catches all timeout errors and **aborts the operation**; it will “never raise a Timeout exception.” ([Grant Jenks][1])

This implies a critical semantic shift:

* **`set` and `delete` may silently fail** on timeout. ([Grant Jenks][1])
* Many methods take `retry=` (default `False`) to repeat attempts; and the mapping operators automatically retry on `Timeout`. ([Grant Jenks][1])

The API reference is explicit about the mapping operators:

* `FanoutCache.__getitem__` calls `get(..., retry=True)` internally. ([Grant Jenks][4])
* `FanoutCache.__delitem__` calls `delete(..., retry=True)` internally. ([Grant Jenks][4])

### Practical profiles (choose intentionally)

**Profile A — “best-effort cache” (lowest tail latency)**

* `FanoutCache(timeout=0.01, retry=False)` (defaults)
* Reads that time out become misses; writes that time out become no-ops.
* You must treat cache as advisory.

**Profile B — “I still want FanoutCache, but writes must stick”**

* Use FanoutCache *methods* with `retry=True` **and check return values**, or use mapping operators where appropriate. ([Grant Jenks][1])

```python
from diskcache import FanoutCache

fc = FanoutCache("/tmp/fc", shards=8, timeout=0.01)

# Stronger write intent:
ok = fc.set("k", "v", retry=True)
assert ok is True
```

---

# E.3 Atomicity boundaries (what is guaranteed “atomic”)

DiskCache uses “atomic operations” in two distinct senses:

## E.3.1 Single-operation atomicity (the library guarantees it)

Many operations are explicitly documented as atomic and/or serialized under concurrency:

* `add`: “Operation is atomic. Only one concurrent add operation for a given key will succeed.” ([Grant Jenks][4])
* `incr` / `decr`: atomic; concurrent increments/decrements are counted individually. ([Grant Jenks][4])
* `pop`: atomic; concurrent operations are serialized. ([Grant Jenks][4])
* Queue primitives: `push`, `pull`, `peek` are atomic/serialized. ([Grant Jenks][4])
* `peekitem`: atomic/serialized (and deletes expired items as part of the operation). ([Grant Jenks][4])

**Implication:** you can safely use these methods as coordination primitives across processes *without* wrapping them in your own locks.

## E.3.2 Multi-operation / multi-key atomicity (you must opt in)

Any multi-step logic like:

```python
if cache.get(k) is None:
    cache.set(k, v)
```

is **not** atomic across threads/processes (classic race) unless you use:

* an atomic method (`add`) or
* a `transact()` section (when you need multi-key invariants). ([Grant Jenks][4])

---

# E.4 Transactions: strong correctness, bounded by writer serialization

## E.4.1 `Cache.transact()`: what it guarantees

The API definition is crisp:

* While locked, **no other write operation is permitted**.
* Reads may occur concurrently to a transaction.
* “Read and write operations performed in a transaction are atomic.”
* Transactions may be nested and may not be shared between threads. ([Grant Jenks][4])

The tutorial emphasizes:

* Keep transactions short because they block other writers.
* Grouping many operations inside one transaction can improve performance “two to five times,” but large transactions block concurrent writers. ([Grant Jenks][1])

## E.4.2 FanoutCache: no global transactions (sharding makes it ambiguous)

Transactions are **not implemented** by `FanoutCache` due to key sharding. Instead:

* request a shard-backed `Cache` via `fanout_cache.cache(name)` (and use transactions there), and use `fanout_cache.deque/index` for persistent DS that also support their own semantics. ([Grant Jenks][1])

---

# E.5 How LRU/LFU + stats change contention (the “read becomes write” trap)

In high-concurrency environments, the difference between “pure reads” and “reads that write metadata” is enormous.

## E.5.1 LRU and LFU turn *every access* into a database write

DiskCache’s eviction policy docs spell out the exact problem:

* **LRU** updates access time “on every access,” making every access a **read + write**, slowing access. ([Grant Jenks][1])
* **LFU** increments access count “on every access,” so every access requires **writing** the database, slowing access. ([Grant Jenks][1])

Now combine that with SQLite WAL’s “one writer at a time” property: those metadata writes get serialized, so read-heavy workloads can become writer-bound under LRU/LFU. ([SQLite][2])

**Guidance:** in a highly concurrent, read-heavy workload, prefer **least-recently-stored** (LRS) if it meets your functional needs, because it avoids the “writes-on-read” behavior that drives contention. ([Grant Jenks][1])

## E.5.2 Statistics can also introduce writes on read paths

DiskCache notes:

* stats are disabled by default because they “incur an extra overhead on cache lookups.” ([Grant Jenks][1])
  And in caveats:
* when disk/database is full, write methods raise `sqlite3.OperationalError`; reads succeed only so long as they do not cause a write “as might occur if cache statistics are being recorded.” ([Grant Jenks][1])

**Takeaway:** if you need “reads that never write,” keep `statistics=False` and avoid LRU/LFU.

---

# E.6 When `FanoutCache` is mandatory (and how to decide)

DiskCache’s own guidance for FanoutCache is blunt:

* Writers block other writers; sharding is used “to decrease blocking writes.”
* “A shard for every concurrent writer is suggested.” ([Grant Jenks][1])

## Use plain `Cache` when…

* You have a single writer (or very low concurrent writers).
* You prefer explicit `Timeout` exceptions rather than silent abort behavior. ([Grant Jenks][1])

## Use `FanoutCache` when…

* You have *many* concurrent writers (threads/processes) and see lock contention / timeouts.
* You need better p95/p99 write latency by distributing writes across shard DBs. ([Grant Jenks][1])

## Practical decision rule (quick)

1. Start with `Cache(directory=..., timeout=...)`.
2. If you observe frequent timeouts/lock contention under realistic concurrency, move to `FanoutCache` and set:

   * `shards ≈ concurrent writers` (or a small multiple). ([Grant Jenks][1])
3. Decide whether your cache is:

   * **best-effort** (timeouts become misses/no-ops; default Fanout semantics), or
   * **must-stick** (use `retry=True`, check return values, and/or isolate critical state in a single shard cache via `.cache(name)` for transactions). ([Grant Jenks][1])

---

# E.7 Minimal runnable snippets (the patterns you’ll reuse)

### 1) Race-free “initialize once” with `add`

```python
from diskcache import Cache
cache = Cache("/tmp/dc")

if cache.add("init:done", True):
    # exactly one process will execute this block
    cache.set("heavy:thing", compute())
```

([Grant Jenks][4])

### 2) Multi-key invariant with `transact`

```python
from diskcache import Cache
cache = Cache("/tmp/dc")

with cache.transact():
    total = cache.incr("total", 123.45)
    count = cache.incr("count")
```

(Transactions block other writers; keep them short.) ([Grant Jenks][1])

### 3) High-writer concurrency: FanoutCache with “stronger” write intent

```python
from diskcache import FanoutCache
fc = FanoutCache("/tmp/fc", shards=8, timeout=0.01)

ok = fc.set("k", "v", retry=True)
assert ok
```

(FanoutCache aborts on timeout; `retry=True` makes it repeat attempts.) ([Grant Jenks][1])

---

If you want to keep following the outline, the next deep dive is **F) Transactions & atomic batches** — going deeper on `transact()` vs atomic primitives, how to batch writes for throughput (2–5× claims), and how to structure “short transactions” to avoid writer tail-latency regressions. ([Grant Jenks][1])

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://sqlite.org/wal.html "Write-Ahead Logging"
[3]: https://sqlite.org/useovernet.html "SQLite Over a Network, Caveats and Considerations"
[4]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"

## Chapter F — Transactions & atomic batches (`transact()` vs atomic primitives)

DiskCache gives you two “correctness tools” that look similar but behave very differently under concurrency:

* **Atomic primitives** (single call → single atomic action) like `add`, `incr/decr`, `pop`, `pull`, etc.
* **`transact()`** (many operations → one atomic unit) that locks out other writers while the context is open. ([Grant Jenks][1])

---

# F.1 `transact()` deep semantics (what it really guarantees)

### Implemented by Cache/Deque/Index

The tutorial is explicit: transactions are implemented by **`Cache`, `Deque`, and `Index`** and are used for consistency *and* performance. ([Grant Jenks][2])

### Contract of `Cache.transact(retry=False)`

From the API reference:

* It is a **context manager** that “performs a transaction by **locking the cache**.”
* While locked, **no other write operation is permitted**.
* Reads **may occur concurrently** with a transaction.
* Reads/writes inside the transaction are **atomic** as a group.
* Transactions may be **nested** and may **not be shared between threads**.
* Can raise `diskcache.Timeout` if a database timeout occurs and `retry=False`. ([Grant Jenks][1])

### `retry=` on `transact()`

`retry` is the “don’t fail on lock contention” switch:

* `retry=False`: you may see `Timeout` on acquisition/contention.
* `retry=True`: DiskCache retries acquisition rather than failing immediately. ([Grant Jenks][1])

---

# F.2 `transact()` vs atomic primitives: how to choose

### Prefer atomic primitives when you can express the logic as one operation

Atomic primitives are the best concurrency building blocks because they:

* hold locks for the shortest time possible, and
* avoid blocking unrelated writers with a long transaction.

Examples of “use primitives, not `transact()`”:

* **One-time initialization**: `add(key, value)` is atomic (“only one concurrent add succeeds”). ([Grant Jenks][1])
* **Counters**: `incr/decr` are atomic and safe under concurrency. ([Grant Jenks][1])
* **Work-queue / pop semantics**: `pop` and `pull` serialize concurrent operations, so you can build safe consumer patterns without `transact()`. ([Grant Jenks][1])

### Use `transact()` when you need *multi-key* invariants or a consistent multi-read snapshot

Canonical examples from the tutorial:

* Update related keys together (e.g., total + count).
* Read related keys together to compute a derived value. ([Grant Jenks][2])

---

# F.3 Atomic batching for throughput (why it can be 2–5× faster)

DiskCache’s tutorial states two key facts:

1. **Every write operation uses a transaction**, and transactions may be nested to improve performance. ([Grant Jenks][2])
2. Grouping many operations inside a single `with cache.transact():` can improve performance **“two to five times”**, but a large batch will block other concurrent writers. ([Grant Jenks][2])

### What’s going on (mechanically)

Without batching:

* each `cache[key] = value` incurs its own begin/commit cycle and its own “writer slot” time.

With batching:

* you pay writer-lock/commit overhead once for the whole batch.

SQLite WAL helps readers proceed while a writer exists, but **there is still only one writer at a time**—so the biggest thing you’re reducing is “writer-turnover overhead.” ([SQLite][3])

---

# F.4 “Short transactions” to avoid writer tail-latency regressions

DiskCache’s docs are blunt: **keep transactions as short as possible**, because “within a transaction, no other writes may occur.” ([Grant Jenks][2])

That single sentence is the whole tail-latency story: long transactions turn your cache into a single-writer bottleneck.

## F.4.1 Structure your transaction like a database engineer

**Rule 1 — Precompute outside the lock**
Do slow Python work before entering `transact()`; inside, do only the minimal `get/set/incr` calls.

**Rule 2 — Don’t do I/O inside the lock**
No network calls, no filesystem scans, no heavyweight serialization work.

**Rule 3 — Batch in chunks to create “writer gaps”**
The tutorial’s `set_many` example is intentionally tiny (and should be chunked when the mapping is large). ([Grant Jenks][2])

Here’s a production-safe chunked variant:

```python
import itertools
from diskcache import Cache

def set_many_chunked(cache: Cache, items, chunk_size: int = 1000):
    it = iter(items)
    while True:
        batch = list(itertools.islice(it, chunk_size))
        if not batch:
            return
        with cache.transact():
            for k, v in batch:
                cache[k] = v
```

This keeps each lock hold bounded even if you’re writing millions of keys.

## F.4.2 Why chunking matters even in WAL mode

SQLite WAL allows concurrent readers, but:

* **only one writer can append to the WAL at a time**. ([SQLite][3])
* **very large write transactions** can cause a large WAL file (cannot be reset mid-transaction), and large WAL files can degrade read performance. ([SQLite][3])

So chunking helps both:

* other writers (they get turns),
* and read performance stability (WAL growth is bounded by each chunk).

---

# F.5 FanoutCache and transactions (the “don’t undo sharding” rule)

The tutorial says transactions are not implemented by `FanoutCache` due to sharding and suggests requesting a shard cache (`fanout_cache.cache('name')`) for transaction support. ([Grant Jenks][2])

However, the API reference *does* document `FanoutCache.transact()` and notes it “blocks until transactions are held on all cache shards by retrying as necessary.” ([Grant Jenks][1])

### Practical interpretation

* **Global Fanout transaction** (`fanout_cache.transact()`): possible, but it effectively **locks all shards**, which can negate the concurrency benefit you chose FanoutCache for. ([Grant Jenks][1])
* **Shard-scoped transaction** (`fanout_cache.cache(name).transact()`): keeps the lock scope smaller and aligns with the tutorial’s recommendation. ([Grant Jenks][2])

---

# F.6 Minimal runnable patterns

## 1) Multi-key invariant: running average (docs pattern)

```python
from diskcache import Cache

cache = Cache("/tmp/dc-tx")

with cache.transact():
    total = cache.incr("total", 123.45)
    count = cache.incr("count", 1)

with cache.transact():
    total = cache.get("total")
    count = cache.get("count")

average = None if count == 0 else total / count
```

(“No other writes may occur” while the transaction is held; keep it short.) ([Grant Jenks][2])

## 2) Bulk write speedup: single transaction

```python
from time import perf_counter
from diskcache import Cache

cache = Cache("/tmp/dc-batch")
data = {f"k{i}": i for i in range(50_000)}

cache.clear()
t0 = perf_counter()
for k, v in data.items():
    cache[k] = v
t1 = perf_counter()

cache.clear()
t2 = perf_counter()
with cache.transact():
    for k, v in data.items():
        cache[k] = v
t3 = perf_counter()

print("no tx:", t1 - t0, "with tx:", t3 - t2)
```

DiskCache documents a typical **2–5×** improvement from grouping operations in one transaction (workload-dependent). ([Grant Jenks][2])

## 3) Concurrency-first alternative: atomic `add` for one-time init

```python
from diskcache import Cache
cache = Cache("/tmp/dc-once")

if cache.add("init:done", True):
    cache["heavy"] = build_once()
```

Only one concurrent `add` succeeds. ([Grant Jenks][1])

---

# F.7 Checklist for “safe and fast” transactions

* Keep the `with cache.transact():` body **tiny** (docs: “transactions should therefore be as short as possible”). ([Grant Jenks][1])
* Prefer atomic primitives (`add/incr/pop/pull`) whenever they express your intent; reserve `transact()` for true multi-key invariants. ([Grant Jenks][1])
* For large batches: **chunk transactions** to avoid starving other writers (docs warn large batches block concurrent writers). ([Grant Jenks][2])
* In high-writer environments: avoid global FanoutCache transactions unless absolutely necessary; prefer shard cache transactions via `fanout_cache.cache(name)`. ([Grant Jenks][2])

---

If you want to keep following the outline, the next deep dive is **G) FanoutCache: sharding for multi-writer scalability** (shard sizing, abort-on-timeout behavior, silent failure modes, and “critical state” isolation patterns).

[1]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[3]: https://sqlite.org/wal.html "Write-Ahead Logging"

## Chapter G — `FanoutCache`: sharding for multi-writer scalability

`FanoutCache` is `Cache` + **automatic sharding** across multiple underlying SQLite-backed cache directories to reduce *writer–writer* blocking (SQLite writers block other writers even when readers/writers can overlap). DiskCache recommends **~one shard per concurrent writer**, defaulting to **8**. ([Grant Jenks][1])

---

# G.1 Sharding model + shard sizing (what “shards” really buy you)

### What is sharded?

DiskCache frames FanoutCache as horizontally partitioning (“sharding”) the underlying cache database to reduce write contention; it’s “built atop Cache” and “automatically shards the underlying database.” ([Grant Jenks][1])

### How many shards should you use?

DiskCache’s own guidance:

* **“a shard for every concurrent writer is suggested”**
* **default shards = 8** ([Grant Jenks][1])

A practical sizing ladder that matches how DiskCache is benchmarked:

* **Web servers / job workers**: start with `shards = number_of_worker_processes` (or the max number of concurrent writers you expect).
* Increase shards when you see tail latency from write contention (timeouts / aborted ops), but remember: more shards = more per-shard overhead and *more places to maintain*. (The docs don’t quantify overhead; this is the standard “more partitions = more metadata” tradeoff.)

### What improvement looks like (bench evidence)

The DiskCache benchmarks explicitly show why sharding exists:

* Without sharding, “cache writers block each other” and max store/delete latency can be “unacceptably high.” ([Grant Jenks][2])
* With FanoutCache using multiple shards, max latency drops substantially (example: “reduces the maximum latency by a factor of ten”). ([Grant Jenks][2])

---

# G.2 Timeout model: abort-on-timeout + `retry=...` (core semantic shift)

### `timeout=` is a *per-operation* DB transaction deadline

DiskCache uses transactions for every operation that writes; when the configured timeout expires, a `diskcache.Timeout` is raised **internally**. ([Grant Jenks][1])

### Cache vs FanoutCache behavior

* **`Cache`**: Timeout may be raised to the caller (unless you use retry behavior). ([Grant Jenks][1])
* **`FanoutCache`**: catches timeout errors and **aborts the operation**; it “will never raise a Timeout exception.” ([Grant Jenks][1])

DiskCache also highlights:

* default FanoutCache timeout is **0.010s (10ms)** ([Grant Jenks][1])
* many methods accept `retry` (default `False`) to automatically repeat attempts that time out ([Grant Jenks][1])

### Mapping operators auto-retry

For FanoutCache, the mapping operators:

* `cache[key]`
* `cache[key] = value`
* `del cache[key]`

automatically call the underlying methods with `retry=True`. ([Grant Jenks][1])

---

# G.3 Silent failure modes (what can “fail silently” and how it manifests)

DiskCache is explicit: because FanoutCache aborts on internal timeouts, **`set` and `delete` may silently fail**. ([Grant Jenks][1])

In the API reference, this is repeated method-by-method:

* `FanoutCache.set(...)`: “If database timeout occurs then fails silently unless retry is set to True.” ([Grant Jenks][3])
* `FanoutCache.delete(...)`: same “fails silently unless retry=True.” ([Grant Jenks][3])
* many others (`add`, `touch`, `decr`, `evict`, `expire`, etc.) document the same pattern. ([Grant Jenks][3])

### Detection strategy (don’t guess)

Use return values as your “write acknowledgement”:

* `set` returns `True` on success. ([Grant Jenks][3])
* `delete` returns `True` if an item was deleted (missing keys are ignored). ([Grant Jenks][3])
* `decr` returns the new value “on success else None” in FanoutCache docs. ([Grant Jenks][3])

So for correctness-sensitive writes, you typically do:

* `retry=True`, and
* assert/check the return value.

---

# G.4 Size management nuance: total `size_limit` is divided across shards

FanoutCache treats `size_limit` as the **total** size of the fanout cache; each shard’s effective limit is:

> `per_shard_limit = total_size_limit / shards` ([Grant Jenks][1])

And DiskCache calls out an important edge case:

> Items larger than a shard’s size limit are **immediately culled**. ([Grant Jenks][1])

This is a classic “surprise” when you start caching multi-hundred-MB objects:

* With 4 shards and 1 GiB total, each shard is ~256 MiB → a 500 MiB object can be “written” and then immediately culled. ([Grant Jenks][1])

---

# G.5 Throughput vs reliability dial: `timeout`, `retry`, and the “misses are acceptable” contract

DiskCache’s benchmarks show how the knobs interact:

* With “one shard allocated per worker and a low timeout,” the maximum latency corresponds to the timeout.
* Some `set`/`delete` operations were canceled and recorded as misses.
* The miss rate due to timeout in that benchmark is ~0.01% (≈ 99.99% success). ([Grant Jenks][2])

This is the intended *best-effort cache* posture:

* bound tail latency (timeouts abort),
* accept tiny miss/no-op rate.

If your cache writes must not drop:

* increase `timeout`, and/or
* use `retry=True` and validate returns (at the cost of potentially higher tail latency). ([Grant Jenks][1])

---

# G.6 “Critical state isolation” patterns (don’t let best-effort semantics leak into correctness)

### Pattern 1 — Keep FanoutCache “best-effort”; isolate correctness-critical state into a named `Cache`

DiskCache recommends requesting a “cache shard with transaction support” via:

```python
fanout_cache = FanoutCache()
critical = fanout_cache.cache("critical")  # lives in a subdirectory
```

…and notes the named cache exists in a **subdirectory**. ([Grant Jenks][1])

This is the cleanest separation:

* FanoutCache: high-throughput, bounded latency, occasional aborts acceptable.
* `critical` (plain Cache): strong semantics, transactions, explicit Timeout surface if you want it.

### Pattern 2 — Durable cross-process structures: `fanout_cache.deque(name)` / `.index(name)`

FanoutCache can vend durable structures in named subdirectories:

* `deque(name, ...)` returns a `Deque` ([Grant Jenks][3])
* `index(name)` returns an `Index` (see API list) ([Grant Jenks][3])

Use these when you want “persistent DS” semantics rather than key/value caching.

### Pattern 3 — Transactions: global vs scoped

Here’s the subtlety in the official docs:

* The **tutorial** says transactions “are not implemented by FanoutCache … due to key sharding” and recommends requesting a named cache instead. ([Grant Jenks][1])
* The **API reference** *does* document `FanoutCache.transact()` and says it “blocks until transactions are held on all cache shards by retrying as necessary.” ([Grant Jenks][3])

In practice: treat `FanoutCache.transact()` as a **global barrier** (it locks *all shards*, undoing the scalability win), and prefer `fanout_cache.cache(name).transact()` for correctness-critical multi-key invariants.

---

# G.7 Minimal runnable snippets (canonical production postures)

### 1) Best-effort, high-throughput cache (bounded latency)

```python
from diskcache import FanoutCache

cache = FanoutCache("/tmp/fc", shards=8, timeout=0.010)  # default-style posture
cache.set("k", "v")  # may abort on timeout unless retry=True
```

(Timeout is caught/aborted; default timeout is 10ms; set/delete may silently fail.) ([Grant Jenks][1])

### 2) “Must stick” writes: `retry=True` + ack checks

```python
from diskcache import FanoutCache

cache = FanoutCache("/tmp/fc", shards=8, timeout=0.010)

ok = cache.set("k", "v", retry=True)
assert ok is True
```

(`set` returns True on success; otherwise timeouts can silently fail without `retry=True`.) ([Grant Jenks][3])

### 3) Isolate correctness-critical state in a named Cache (recommended)

```python
from diskcache import FanoutCache

fc = FanoutCache("/tmp/fc", shards=8, timeout=0.010)

critical = fc.cache("critical", timeout=5.0)  # separate subdirectory Cache
with critical.transact():
    critical.incr("total", 123.4)
    critical.incr("count", 1)
```

(named Cache in a subdirectory; Cache transactions are the “strong” tool.) ([Grant Jenks][3])

---

If you want to continue per the outline, next is **H) Persistent data structures** (`Deque` + `Index`)—deep semantics, durability invariants (no eviction/expiration), and how to use them as cross-process queues and ordered maps.

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/cache-benchmarks.html "DiskCache Cache Benchmarks — DiskCache 5.6.1 documentation"
[3]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"

## Chapter H — Persistent data structures: `Deque` + `Index`

DiskCache ships two “persistent containers” that behave like familiar stdlib types but store their contents on disk and can be shared across threads/processes:

* **`diskcache.Deque`**: `collections.deque`-compatible persistent double-ended queue, built on `Cache.push/pull/peek`, and it **never evicts or expires items**. ([Grant Jenks][1])
* **`diskcache.Index`**: persistent ordered mapping / ordered-dict-like interface that also **never evicts or expires items**. ([Grant Jenks][1])

Both are positioned as **safe cross-thread / cross-process communication primitives** that keep **fixed in-process memory** regardless of dataset size (disk usage still grows). ([Grant Jenks][1])

---

# H.1 `diskcache.Deque` (“deck”)

## H.1.1 API signatures (surface area)

**Constructor + reopen**

```python
Deque(iterable=(), directory=None, maxlen=None)
# Items are serialized to disk; may be initialized from a directory path. :contentReference[oaicite:3]{index=3}
```

**Attach to an existing Cache**

```python
Deque.fromcache(cache, iterable=(), maxlen=None) -> Deque
```

([Grant Jenks][2])

**Core methods (deque-like)**

* `append(value)` / `appendleft(value)` ([Grant Jenks][2])
* `extend(iterable)` / `extendleft(iterable)` ([Grant Jenks][2])
* `pop()` / `popleft()` ([Grant Jenks][2])
* `peek()` / `peekleft()` (faster than indexing `-1` / `0`) ([Grant Jenks][2])
* `rotate(steps=1)` / `reverse()` ([Grant Jenks][2])
* `remove(value)` / `count(value)` ([Grant Jenks][2])
* `clear()` / `copy()` ([Grant Jenks][2])
* `transact()` (locks deque for atomic multi-op batches) ([Grant Jenks][2])

**Properties**

* `.directory` (the durable identity) ([Grant Jenks][1])
* `.maxlen` ([Grant Jenks][2])
* `.cache` (underlying Cache used by the deque) ([Grant Jenks][2])

**Sequence protocol**
Deque supports iteration, `len()`, membership, indexing and assignment (e.g., `deque[i] = ...`) consistent with being a persistent sequence. ([Grant Jenks][2])

---

## H.1.2 Semantics (what Deque *is*)

### “Persistent deque” backed by a Cache

The tutorial defines `diskcache.Deque` as a `collections.deque`-compatible wrapper that uses `Cache.push/pull/peek` under the hood but **never evicts or expires items**. ([Grant Jenks][1])

### Directory = identity, reopen anywhere

You can create a deque, then open another handle to the same persistent deque using its directory (even in another process): ([Grant Jenks][1])

```python
from diskcache import Deque

d = Deque(range(5, 10))
other = Deque(directory=d.directory)
assert len(other) == len(d)
```

### Fixed in-process memory footprint

DiskCache documents Deque as using a **fixed amount of memory regardless of the size/number of items stored** and as “efficient and safe” for cross-thread/cross-process communication. ([Grant Jenks][1])

### `maxlen` behaves like stdlib `collections.deque`

DiskCache shows `maxlen` behaving like a bounded deque (e.g., initializing with `maxlen=3` keeps only the last 3 items). ([Grant Jenks][1])
Python’s `collections.deque` definition clarifies the rule: when a bounded deque is full, adding new items discards items from the opposite end. ([Python documentation][3])

---

## H.1.3 Durability invariants & footguns

### Invariants

* **No eviction / no expiration**: Deque items do not TTL-out and are not subject to eviction policy; the only way items leave is by your code (`pop/popleft/clear/...`). ([Grant Jenks][1])
* **Crash survivability**: because items are stored on disk, the deque persists across restarts; the case study uses this to make a BFS crawler resumable. ([Grant Jenks][4])
* **Cross-process safe**: the case study demonstrates multiple processes sharing the same Deque to divide work. ([Grant Jenks][4])

### Footguns (practical)

* **Unbounded growth unless you bound it**: with no eviction/expiration, you must manage growth via `maxlen`, consumption (`popleft/pop`), or periodic cleanup (`clear`). ([Grant Jenks][1])
* **Empty operations raise**: `peek/peekleft/pop/popleft` raise `IndexError` on empty. ([Grant Jenks][2])
* **“Work item loss” semantics (design consideration)**: if you `popleft()` a work item and the worker crashes before completing it, the item is gone from the queue. If you need at-least-once processing, use an “inflight” structure (see patterns below). (This is an architectural implication of dequeue-on-read, not a DiskCache-specific guarantee.)

---

## H.1.4 Minimal runnable snippets

### 1) Persistent producer/consumer queue (single process)

```python
from diskcache import Deque

q = Deque(directory="data/jobs")
q.append({"job_id": 1})
q.append({"job_id": 2})

job = q.popleft()  # {"job_id": 1}
```

Deque is persistent and reopenable by directory. ([Grant Jenks][1])

### 2) Multi-process work queue (pattern from the web-crawler case study)

The DiskCache case study uses Deque as the shared queue and shows multiple processes dividing work by popping from it. ([Grant Jenks][4])

### 3) Rolling “tail buffer” with `maxlen`

```python
from diskcache import Deque

events = Deque("abcde", directory="data/events", maxlen=3)
assert list(events) == ["c", "d", "e"]
```

This mirrors both DiskCache’s example and stdlib bounded deque behavior. ([Grant Jenks][1])

### 4) Atomic “rotate” / round-robin step

DiskCache explicitly demonstrates using `transact()` to rotate elements atomically: ([Grant Jenks][2])

```python
from diskcache import Deque

d = Deque(range(5), directory="data/rr")
with d.transact():
    x = d.pop()
    d.appendleft(x)
```

---

## H.1.5 Production patterns (Deque)

### Pattern A — Use Deque as a durable job queue + Index as an “inflight ledger”

If you need at-least-once processing:

* `Deque`: pending work
* `Index`: inflight + status (started_at, attempts, worker_id)
* On `popleft()`: write inflight record first (ideally in a transaction on the Index), then process; on success, delete inflight record; on crash recovery, requeue inflight items.

The case study already pairs Deque + Index (queue + results mapping) as a general pattern. ([Grant Jenks][4])

### Pattern B — Use `FanoutCache.deque(name)` for “named queues” in a shared cache root

FanoutCache can vend a Deque in a subdirectory by name. ([Grant Jenks][2])
This is useful when you already have a `FanoutCache` root and want consistent placement/layout for multiple queues.

---

# H.2 `diskcache.Index` (persistent ordered mapping / ordered dict)

## H.2.1 API signatures (surface area)

**Constructor + reopen**

```python
Index(*args, **kwargs)
# Items serialized to disk; may be initialized from directory path. :contentReference[oaicite:33]{index=33}
```

The API specifies: the first argument may be a directory string; if omitted/None, a temp directory is created. ([Grant Jenks][2])

**Attach to an existing Cache**

```python
Index.fromcache(cache, *args, **kwargs) -> Index
```

([Grant Jenks][2])

**Mapping + ordered-dict operations**

* `index[key]`, `index[key] = value`, `del index[key]` ([Grant Jenks][2])
* Iteration in **insertion order** + `reversed(index)` ([Grant Jenks][2])
* `pop(key, default=...)` ([Grant Jenks][2])
* `peekitem(last=True/False)` ([Grant Jenks][2])
* `popitem(last=True/False)` (LIFO if last=True, FIFO if last=False) ([Grant Jenks][2])
* `setdefault(key, default=None)` ([Grant Jenks][2])
* `keys() / values() / items()` views ([Grant Jenks][2])
* `clear()` ([Grant Jenks][2])

**Queue-style primitives (built into Index)**

* `push(value, prefix=None, side='back') -> key`
* `pull(prefix=None, default=(None, None), side='front') -> (key, value)` ([Grant Jenks][2])

**Transactions**

* `transact()` locks the index for atomic multi-op batches. ([Grant Jenks][2])

**Key correctness note**

* *“Hashing protocol is not used. Keys are looked up by their serialized format.”* ([Grant Jenks][2])

---

## H.2.2 Semantics (what Index *is*)

### Persistent ordered mapping

The tutorial describes Index as a persistent mapping with an “ordered dictionary interface,” inheriting cache benefits but **never evicting or expiring items**, with a fixed memory footprint and safe cross-thread/process use. ([Grant Jenks][1])

### Reopenable by directory (durable identity)

Index can be opened by directory and observed across processes (tutorial example uses `other = Index(index.directory)`). ([Grant Jenks][1])

### Ordered behaviors you can lean on

* Iteration order is insertion order; `popitem(last=False)` gives FIFO behavior, and `popitem(last=True)` gives LIFO behavior (the API explicitly frames this as “queue” vs “stack” imitation). ([Grant Jenks][2])
* Equality comparison is order-sensitive vs another Index/OrderedDict, but order-insensitive vs ordinary mappings. ([Grant Jenks][2])

### Built-in queue inside the Index keyspace (`push/pull`)

Index’s `push/pull` mirrors Cache’s queue system:

* integer keys by default, or `"prefix-integer"` if prefix is provided
* integer starts at **500 trillion**
* `side='front'/'back'` controls the endpoint ([Grant Jenks][2])

---

## H.2.3 Durability invariants & footguns

### Invariants

* **No eviction / no expiration**: Index does not evict/expire items; you remove them explicitly (`del`, `pop`, `popitem`, `clear`). ([Grant Jenks][1])
* **Cross-process safe**: the web-crawler case study uses an Index as the shared results dictionary across multiple processes. ([Grant Jenks][4])
* **Transactional batches exist**: `Index.transact()` provides the same “no other writes while locked; reads may occur; atomic batch” contract DiskCache documents for transactions. ([Grant Jenks][2])

### Footguns

* **Key equality is serialization-based** (not hash/eq): keys are looked up by serialized format; this can surprise you if you use complex/unreliable-to-serialize keys. ([Grant Jenks][2])
* **Unbounded growth unless you manage it**: because nothing expires, you must define retention (manual cleanup, popitem FIFO, etc.). ([Grant Jenks][1])
* **Queue keys can collide with your own key scheme**: `push()` uses integer keys when `prefix=None`. If your Index also uses integer keys for “real” mapping entries, separate them with `prefix=` (recommended).

---

## H.2.4 Minimal runnable snippets

### 1) Persistent ordered mapping + reopen

```python
from diskcache import Index

idx = Index([("a", 1), ("b", 2)], directory="data/index")
idx["c"] = 3

other = Index("data/index")
assert list(other) == ["a", "b", "c"]  # insertion-order iteration
```

Index is a persistent ordered mapping. ([Grant Jenks][1])

### 2) FIFO/LIFO removal via `popitem`

```python
from diskcache import Index

idx = Index([("a", 1), ("b", 2), ("c", 3)])
idx.popitem()            # ("c", 3)  LIFO default
idx.popitem(last=False)  # ("a", 1)  FIFO
```

([Grant Jenks][2])

### 3) Use Index as a queue (push/pull)

```python
from diskcache import Index

q = Index(directory="data/q")
k1 = q.push("apples")             # integer key
k2 = q.push("dates", prefix="f")  # "f-500000000000000"

k, v = q.pull()                   # (k1, "apples")
```

([Grant Jenks][2])

### 4) Atomic multi-key updates with `transact()`

DiskCache documents Index transactions with an “atomic average” example. ([Grant Jenks][2])

```python
from diskcache import Index
m = Index(directory="data/metrics")

with m.transact():
    m["total"] = m.get("total", 0) + 123.4
    m["count"] = m.get("count", 0) + 1
```

---

## H.2.5 Production patterns (Index)

### Pattern A — Results store / dedupe map paired with a Deque work queue

This is exactly the DiskCache web crawler case study:

* `urls = Deque([...], 'data/urls')`
* `results = Index('data/results')`
* multiple processes pop from the deque and write to the index, dividing work automatically ([Grant Jenks][4])

### Pattern B — Ordered retention / “bounded map” via FIFO `popitem(last=False)`

If you want a bounded results store without relying on expiration:

* after each insert, while `len(index) > N`: `index.popitem(last=False)` to drop oldest.

### Pattern C — Use Index transactions as your “multi-key invariant tool”

When you’re treating Index as a tiny persistent state store (counters, manifests, checkpoints), wrap read-modify-write groups in `with index.transact():` to avoid races. ([Grant Jenks][2])

---

If you want to keep following your outline, next is **I) Function memoization and cache-stampede controls** (`memoize`, `memoize_stampede`, plus how these interact with Deque/Index keys and persistence).

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[3]: https://docs.python.org/3/library/collections.html "collections — Container datatypes — Python 3.14.2 documentation"
[4]: https://grantjenks.com/docs/diskcache/case-study-web-crawler.html "Case Study: Web Crawler — DiskCache 5.6.1 documentation"

([Past chat][1])([Past chat][1])([Past chat][2])([Past chat][3])([Past chat][4])

## Chapter I — Function memoization + cache-stampede controls (`memoize`, `memoize_stampede`)

DiskCache gives you two layers:

1. **Persistent memoization**: “decorate a function, cache args → result” (`Cache.memoize`, `FanoutCache.memoize`, etc.). ([Grant Jenks][5])
2. **Memoization with stampede protection**: probabilistic early recomputation in a background thread (`diskcache.memoize_stampede`). ([Grant Jenks][6])

---

# I.1 Mental model: what DiskCache memoization *is* (vs `functools.lru_cache`)

### `functools.lru_cache` (baseline)

* In-memory dict-based cache: requires hashable args; in multithreaded scenarios the wrapped function *can be called more than once* if a second thread enters before the first finishes and stores the result. ([Python documentation][7])

### DiskCache `memoize`

* Same basic “remember results” semantics, but **stored in a disk-backed cache**, so results can persist across process restarts (because the underlying cache is durable-by-directory). ([Grant Jenks][5])
* Still conceptually a cache, not a strict “single-flight barrier” — i.e., without stampede controls, a hot key can be recomputed concurrently when missing/expired (classic dogpile risk). 

---

# I.2 `memoize` deep dive (API → semantics → invariants → patterns)

## I.2.1 API surface + parameters

For `Cache` / `FanoutCache` (core signature):

```python
@cache.memoize(
    name=None,
    typed=False,
    expire=None,
    tag=None,
    ignore=(),
)
def f(...): ...
```

* **`name`**: overrides the callable name used to form the cache key (default: automatic). ([Grant Jenks][6])
* **`typed`**: distinguishes `f(3)` vs `f(3.0)` (and other type-different args) as different cache entries. ([Grant Jenks][6])
* **`expire`**: TTL in seconds; `None` = no expiry. Special case: **`expire=0`** means *lookups still occur* but results are **not** written back to the cache. ([Grant Jenks][6])
* **`tag`**: associate a text tag with each memoized entry (so you can group-invalidate via `evict(tag)`). ([Grant Jenks][6])
* **`ignore`**: positional or keyword args to ignore when building the cache key. ([Grant Jenks][6])

The tutorial explicitly frames memoize args as “like `functools.lru_cache` and `Cache.set`.” ([Grant Jenks][5])

## I.2.2 Semantics (what happens on call)

**High-level behavior**

* Call `f(*args, **kwargs)`:

  1. compute a deterministic cache key derived from the function identity + arguments,
  2. `get(key)` from the cache,
  3. on miss, compute result, then `set(key, result, expire=..., tag=...)`. ([Grant Jenks][6])

**Implementation detail (useful intuition)**

* The DiskCache source shows the wrapper does a “get; if miss compute; if expire is None or >0 then set” flow, and exposes a `__cache_key__` helper for the same args. ([Arcovid19][8])

## I.2.3 Introspection hooks you should treat as part of the public contract

DiskCache’s API docs explicitly guarantee:

* `wrapper.__wrapped__` points at the original function (useful to bypass cache or rewrap). ([Grant Jenks][6])
* `wrapper.__cache_key__(*args, **kwargs)` returns the actual cache key for those args, so you can inspect/delete specific entries. ([Grant Jenks][6])

**Example: delete one memoized entry**

```python
key = fibonacci.__cache_key__(100)
del cache[key]
```

This pattern is shown in the docs for both `memoize` and `memoize_stampede`. ([Grant Jenks][6])

## I.2.4 Invariants + footguns

1. **You must call `memoize()`**
   If you write `@cache.memoize` (no parentheses), DiskCache raises `TypeError`. ([Grant Jenks][5])

2. **`expire=0` is “read-only memoization”**
   Lookups still happen but results are not stored — this is easy to set accidentally and wonder why nothing is cached. ([Grant Jenks][6])

3. **`ignore` is the escape hatch for uncacheable args**
   If one argument can’t be serialized (e.g., a DB session/connection), you typically ignore it and ensure it doesn’t affect results. The docs define `ignore` as positional or keyword args to ignore when building keys. ([Grant Jenks][6])

---

# I.3 What “cache stampede” means (and why plain memoize can dogpile)

The VLDB paper DiskCache cites defines the problem:

* When a frequently accessed cached value expires, many concurrent requests can observe the miss and all regenerate the value simultaneously, wasting work and potentially overloading the backend (“cache stampede”). 

DiskCache uses the same terminology (“dog-piling”, “thundering herd”, etc.) in its memoize-stampede docs. ([Grant Jenks][6])

---

# I.4 `memoize_stampede`: early recomputation + background refresh

## I.4.1 API surface

```python
from diskcache import memoize_stampede

@memoize_stampede(cache, expire, name=None, typed=False, tag=None, beta=1, ignore=())
def f(...): ...
```

* **`expire`** is required (base TTL). ([Grant Jenks][6])
* **`beta`** tunes “eagerness” of early recomputation. ([Grant Jenks][9])
* `name/typed/tag/ignore` parallel `memoize`. ([Grant Jenks][6])

## I.4.2 Semantics (the core behavior)

DiskCache’s docs describe it as:

* It implements stampede protection via **early recomputation**: before expiration, a call may *probabilistically* trigger a refresh in a **background thread** while still returning the currently cached value to the caller. ([Grant Jenks][6])
* If the item is truly missing, recomputation occurs synchronously and the result is cached. ([Grant Jenks][9])
* The approach is based on Vattani/Chierichetti/Lowenstein (2015) “Optimal Probabilistic Cache Stampede Prevention”. ([Grant Jenks][6])

### Why this works (intuition)

The paper’s key idea is: *spread regeneration out in time* by letting requests “pretend to be in the future” and decide early refresh probabilistically, which reduces the probability that many requests regenerate at the same instant. 

## I.4.3 `beta` tuning (what the case study shows)

DiskCache’s landing-page case study is the best “operational” guide:

* It explicitly describes early recomputation as: *use randomness to simulate a miss before expiry; recompute in a background thread while returning cached value*. ([Grant Jenks][9])
* **Lowering `beta` reduces eagerness**; default `beta=1` is “reasonable,” but tuning is empirical. ([Grant Jenks][9])
* If `beta` is **too low**, recomputation may not happen before the real expiration → workers see a real miss and recompute synchronously, causing an actual stampede. ([Grant Jenks][9])

## I.4.4 Invariants / footguns

1. **Your function must be safe to run in a background thread**
   `memoize_stampede` explicitly recomputes in a background thread. If the function touches thread-local request context, global mutable state, or non-thread-safe clients, you can create subtle bugs. ([Grant Jenks][6])

2. **Stampede protection is probabilistic, not a hard barrier**
   It reduces synchronized recomputation, but does not claim “exactly one compute globally” (that’s what explicit barriers/locks are for). DiskCache’s docs position `memoize_stampede` as mitigation via early recomputation, not strict single-flight. ([Grant Jenks][6])

---

# I.5 Interactions with `Deque` / `Index` and persistence

This is where DiskCache’s data model matters:

### Deque / Index are “persistent containers” with **no eviction/expiration**

* The tutorial states that `Deque` and `Index` “never evict or expire items.” ([Grant Jenks][5])

**Implication for memoization:**

* If you rely on TTL-based invalidation, **use `Cache`/`FanoutCache` memoize** with `expire=...`. ([Grant Jenks][6])
* If you want “results stick until I explicitly remove them,” then an **Index-backed memoization** style (or a Cache configured in a similar “never-evict” posture) can be appropriate — but you must define your own invalidation strategy (manual deletion via `__cache_key__`, or tagging + `evict(tag)` where applicable). ([Grant Jenks][6])

### Directory hygiene: isolate concerns

Since everything is durable-by-directory, a practical pattern is:

* one cache directory (or tag namespace) per “memoized function family,”
* separate directories/subdirectories for durable queues/maps (`Deque`, `Index`),
  so a size policy or cleanup job for memoized results can’t accidentally interfere with your persistent coordination structures. ([Grant Jenks][5])

---

# I.6 Minimal runnable snippets (canonical patterns)

### 1) Plain persistent memoize (TTL + typed + tag)

```python
from diskcache import Cache

cache = Cache("data/memo")

@cache.memoize(expire=300, typed=True, tag="api:v1")
def fetch_user(user_id: int):
    return expensive_fetch(user_id)
```

(Parameters and semantics are from the API + tutorial.) ([Grant Jenks][5])

### 2) Ignore an uncacheable arg (e.g., session)

```python
@cache.memoize(expire=60, ignore={"session", 1})
def read_row(row_id, session):
    return session.query(...)
```

(Why: `ignore` exists specifically to omit args from keying.) ([Grant Jenks][6])

### 3) Stampede-protected memoize (background refresh)

```python
from diskcache import Cache, memoize_stampede

cache = Cache("data/memo")

@memoize_stampede(cache, expire=60, beta=1.0)
def render_landing_page():
    return expensive_render()
```

(Background-thread probabilistic recomputation is the defining behavior.) ([Grant Jenks][6])

---

If you want to continue per the outline, the next deep dive is **J) Synchronization “recipes”** (Lock/RLock/BoundedSemaphore, plus the `barrier` decorator, which is the “hard single-flight” complement to probabilistic stampede mitigation). ([Grant Jenks][5])

[1]: https://chatgpt.com/c/6973d8e0-38c0-8333-af23-0e457779195a "DiskCache Features Catalog"
[2]: https://chatgpt.com/c/696c7a34-669c-8329-a985-4e87e0a574e0 "Delta Lake Python Features"
[3]: https://chatgpt.com/c/69639f3d-06e8-8330-a502-77849d1f6591 "Pydantic Documentation Outline"
[4]: https://chatgpt.com/c/696bdaa8-5a20-832d-ae02-8d946edf62e9 "Codebase Improvement Review"
[5]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[6]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[7]: https://docs.python.org/3/library/functools.html?utm_source=chatgpt.com "functools — Higher-order functions and operations on callable ..."
[8]: https://arcovid19.readthedocs.io/en/latest/_modules/diskcache/core.html "diskcache.core — arcovid19 0.6b documentation"
[9]: https://grantjenks.com/docs/diskcache/case-study-landing-page-caching.html "Case Study: Landing Page Caching — DiskCache 5.6.1 documentation"

## Chapter J — Synchronization “recipes” (`Lock` / `RLock` / `BoundedSemaphore` + `barrier`)

DiskCache ships **cache-backed coordination primitives** designed to work across **threads and processes** (same-host, same cache directory), built out of `Cache` operations + short transactions. The docs group these under “Recipes” and explicitly list `Lock`, `RLock`, `BoundedSemaphore`, and the `barrier` decorator. ([Grant Jenks][1])

A critical design constraint repeats everywhere:

> These recipes **assume their keys will not be evicted**; you should set the cache eviction policy to **`'none'`** to guarantee the coordination key remains present. ([Grant Jenks][2])

---

# J.1 API surface (signatures + what you get)

### `Lock`

```python
Lock(cache, key, expire=None, tag=None)
lock.acquire()
lock.release()
lock.locked() -> bool
with lock: ...
```

Docs: “Acquire lock using spin-lock algorithm”; “Release lock by deleting key.” ([Grant Jenks][2])

### `RLock` (re-entrant, owner-aware)

```python
RLock(cache, key, expire=None, tag=None)
rlock.acquire()
rlock.release()   # asserts if not owned
with rlock: ...
```

Docs: acquire increments a count (spin-lock); release decrements. ([Grant Jenks][2])

### `BoundedSemaphore`

```python
BoundedSemaphore(cache, key, value=1, expire=None, tag=None)
sem.acquire()
sem.release()     # asserts if release would exceed initial bound
with sem: ...
```

Docs: acquire decrements; release increments. ([Grant Jenks][2])

### `barrier` decorator

```python
@diskcache.barrier(cache, lock_factory, name=None, expire=None, tag=None)
def f(...): ...
```

Docs: “Barrier to calling decorated function”; supports `Lock`, `RLock`, `BoundedSemaphore`. ([Grant Jenks][2])

---

# J.2 `Lock` deep semantics (what it actually does)

DiskCache’s `Lock` is a **lease-like “presence key”**:

* **Acquire** spins until it can **atomically `add()` the key** (insert-if-absent), sleeping 1ms between attempts. ([Hugging Face][3])
* **Release** deletes the key. ([Hugging Face][3])
* **locked()** is just membership: `key in cache`. ([Hugging Face][3])

**Key implementation details (source):**

* Uses `cache.add(key, None, expire=..., tag=..., retry=True)` in a loop, then `cache.delete(key, retry=True)` on release. ([Hugging Face][3])

### Correctness invariants / footguns (Lock)

1. **No ownership token.**
   `Lock` doesn’t encode “who owns it”; it’s just a key’s existence. If you set `expire` and your critical section runs longer than the lease, another worker may acquire the lock while you still think you own it.

2. **If you *don’t* set `expire`, crashes can deadlock forever.**
   If the holder dies without calling `release()`, the key remains indefinitely. (That’s why the constructor supports `expire=` and the recipe is phrased as a spin-lock/lease pattern.) ([Hugging Face][3])

3. **Spin-lock contention cost.**
   Under heavy contention, every waiter is polling the cache ~every 1ms (transaction traffic + CPU wakeups). That’s fine for short critical sections, but you’ll feel it if many workers pile up.

---

# J.3 `RLock` deep semantics (owner-aware, re-entrant)

`RLock` upgrades `Lock` by storing an **owner id + recursion count** in the cache entry.

### How it identifies ownership

* Owner is `"{pid}-{thread_id}"` using `os.getpid()` and `threading.get_ident()`. ([Hugging Face][3])
* Re-entrancy is therefore **per-process AND per-thread** (matches Python’s thread-based RLock expectations).

### Acquire / release algorithm (source-level)

* Acquire spins; each attempt runs a short transaction:

  * reads `(owner, count)` default `(None, 0)`
  * if `count == 0` (unheld) **or** `owner == me` (re-enter), it sets `(me, count+1)` and returns. ([Hugging Face][3])
* Release is a transaction:

  * asserts you are the owner and `count > 0`
  * sets `(owner, count-1)` (note: it does **not** delete the key; it leaves a record with `count==0`). ([Hugging Face][3])

### Footguns (RLock)

1. **Release asserts on misuse.**
   Releasing from the wrong thread/process (or releasing too many times) throws an assertion: “cannot release un-acquired lock.” ([Hugging Face][3])

2. **Forking changes PID.**
   Because ownership includes PID, if you acquire in a parent and fork, the child cannot “release” the parent’s lock (ownership mismatch). Treat these locks as “acquire/release in the same process”.

3. **The key persists at count=0.**
   That’s fine functionally (acquire checks `count==0`), but it reinforces the “keys must not be evicted” rule: if eviction deletes the key at the wrong moment you can violate your intended invariants. ([Hugging Face][3])

---

# J.4 `BoundedSemaphore` deep semantics (distributed concurrency cap)

A `BoundedSemaphore` stores a single integer “remaining permits” in the cache key.

### Acquire / release algorithm (source-level)

* Acquire spins; inside a short transaction:

  * reads current value (default initial `_value`)
  * if `value > 0`, it writes back `value-1` and returns. ([Hugging Face][3])
* Release is a transaction:

  * reads current value (default initial `_value`)
  * asserts you aren’t releasing beyond the initial bound (`assert self._value > value`)
  * increments and writes back. ([Hugging Face][3])

### Footguns (Semaphore)

1. **It’s “bounded” and enforces it by assertion.**
   Extra releases raise an assertion (“cannot release un-acquired semaphore”). ([Hugging Face][3])

2. **State loss can break accounting.**
   If the semaphore key is deleted/expired while permits are held, the next `get(..., default=self._value)` can “reset” the counter. That’s why the docs insist keys must not be evicted and why you should be careful with `expire=`. ([Grant Jenks][2])

---

# J.5 `barrier` decorator = “hard single-flight” (or “N-flight”)

`barrier(cache, lock_factory, ...)` is intentionally simple:

* It chooses a **single key per decorated function**: `full_name(func)` unless you pass `name=`. ([Hugging Face][3])
* It constructs **one lock instance at decoration time**: `lock_factory(cache, key, expire=..., tag=...)`. ([Hugging Face][3])
* Every call is wrapped with `with lock: return func(...)`. ([Hugging Face][3])

So:

* With `Lock` → **strict serialization** (only one call runs at a time). ([Grant Jenks][2])
* With `BoundedSemaphore(value=N)` → **N concurrent calls max** (a “distributed concurrency limit”). (Mechanically, barrier just uses `with semaphore:`.) ([Hugging Face][3])

### Barrier footguns (important)

1. **Barrier is global across arguments.**
   Because the key is derived from the function name (unless overridden), `work(1)` blocks `work(2)`—as shown in the docs example. ([Grant Jenks][2])
   If you need “per-resource” barriers (e.g., per `user_id`), you must generate a key from args yourself (see patterns below).

2. **The lock is created once.**
   If you need a dynamic `value=` for a semaphore, you must supply a custom `lock_factory` that sets it.

---

# J.6 Minimal runnable snippets

### 1) Cross-process critical section (lock with a lease)

```python
import time
import diskcache
from diskcache import Lock

cache = diskcache.Cache("/tmp/dc-sync", eviction_policy="none")
lock = Lock(cache, "lock:rebuild-index", expire=120)

with lock:
    # do something you really don't want running concurrently
    time.sleep(5)
```

Lock semantics: spin on `cache.add`, release by `delete`, optional `expire` lease. ([Hugging Face][3])

### 2) Re-entrant lock for recursive / re-entrant code paths

```python
import diskcache
from diskcache import RLock

cache = diskcache.Cache("/tmp/dc-sync", eviction_policy="none")
rlock = RLock(cache, "rlock:planner")

def recurse(n):
    with rlock:
        if n:
            return recurse(n - 1)
        return "done"

print(recurse(3))
```

RLock is owner-aware and increments/decrements a count in a transaction. ([Hugging Face][3])

### 3) Distributed concurrency cap (semaphore)

```python
import time
import diskcache
from diskcache import BoundedSemaphore

cache = diskcache.Cache("/tmp/dc-sync", eviction_policy="none")
sem = BoundedSemaphore(cache, "sem:db-conns", value=3)

with sem:
    # only 3 workers across the machine can be here at once
    time.sleep(1)
```

Acquire decrements; release increments with bounds checking. ([Hugging Face][3])

### 4) “Hard single-flight” barrier on a function

```python
import time
import diskcache
from diskcache import barrier, Lock

cache = diskcache.Cache("/tmp/dc-sync", eviction_policy="none")

@barrier(cache, Lock, expire=60)
def expensive_step(x):
    time.sleep(2)
    return x * 2
```

Barrier derives the lock key from the function name (unless `name=`). ([Grant Jenks][2])

### 5) Barrier with a semaphore = “N-flight” decorator

```python
import functools
import diskcache
from diskcache import barrier, BoundedSemaphore

cache = diskcache.Cache("/tmp/dc-sync", eviction_policy="none")

def sem_factory(cache, key, expire=None, tag=None):
    return BoundedSemaphore(cache, key, value=5, expire=expire, tag=tag)

@barrier(cache, sem_factory, name="barrier:api", expire=30)
def call_api():
    ...
```

Barrier supports `BoundedSemaphore` as the lock type. ([Grant Jenks][2])

---

# J.7 Production patterns (the stuff you’ll actually want)

### Pattern A — Dedicated “coordination cache” with `eviction_policy='none'`

Make a separate cache directory used only for locks/semaphores/barriers:

```python
coord = Cache("/var/tmp/myapp_coord", eviction_policy="none", size_limit=int(1e9))
```

This enforces the “keys must not be evicted” contract for recipes. ([Grant Jenks][2])

### Pattern B — Always use leases for crash safety (and size them conservatively)

* For `Lock`/`barrier(..., Lock, expire=...)`, set `expire` > worst-case runtime + buffer.
* If you can’t bound runtime, a simple lease is risky (because of the “lease expires while still running” hazard). In that case, either:

  * restructure work into smaller chunks each under its own lock, or
  * implement your own token-based lock (recipe-level extension).

### Pattern C — Per-resource barriers (key includes args)

The built-in `barrier` is one key per function. For per-resource keys, do:

```python
import functools
from diskcache import Lock

def per_key_barrier(cache, key_fn, expire=None, tag=None):
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = key_fn(*args, **kwargs)
            lock = Lock(cache, key, expire=expire, tag=tag)
            with lock:
                return func(*args, **kwargs)
        return wrapper
    return deco
```

This mirrors DiskCache’s own barrier implementation, except you compute a key per call. ([Hugging Face][3])

### Pattern D — “True single-flight memoization” (barrier + cache value)

If you want “only one recompute on miss” (hard dogpile prevention), you often combine:

* a per-key barrier lock, then
* “check cache → compute → set cache” inside the lock.

That’s the strict complement to DiskCache’s probabilistic `memoize_stampede`. ([Grant Jenks][2])

---

If you want to keep following the outline, next is **K) Django integration surface** (`DjangoCache` behavior, key settings, and how its Fanout/timeout semantics affect correctness).

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[3]: https://huggingface.co/koichi12/llm_tutorial/blame/3eb4a7039b8fc1194fc39189b6b335495e31a0ef/.venv/lib/python3.11/site-packages/diskcache/recipes.py ".venv/lib/python3.11/site-packages/diskcache/recipes.py · koichi12/llm_tutorial at 3eb4a7039b8fc1194fc39189b6b335495e31a0ef"

## Chapter K — Django integration surface (`diskcache.DjangoCache`)

### K.1 What `DjangoCache` is (and what it wraps)

`diskcache.DjangoCache` is a **Django cache backend** implemented on top of DiskCache’s **`FanoutCache`** (i.e., sharded SQLite-backed caches) to reduce writer contention while keeping Django’s cache API. ([Grant Jenks][1])

A key behavioral promise (important for production error handling):

* `DjangoCache` **never raises `diskcache.Timeout`** to your Django app. ([Grant Jenks][1])
* Unlike `FanoutCache`, `DjangoCache` defaults `retry=True` for *most* mutating methods (`set/add/delete/incr/decr/touch/pop`), so it will retry lock-timeouts by default. ([Grant Jenks][1])

---

## K.2 Django settings mapping (what to put in `CACHES`)

DiskCache’s tutorial gives the canonical Django configuration (with DiskCache-specific knobs called out): ([Grant Jenks][1])

```python
CACHES = {
    "default": {
        "BACKEND": "diskcache.DjangoCache",
        "LOCATION": "/path/to/cache/directory",

        # Django-standard:
        "TIMEOUT": 300,                 # default TTL per key

        # DiskCache-specific:
        "SHARDS": 8,                    # FanoutCache shard count
        "DATABASE_TIMEOUT": 0.010,      # per SQLite transaction timeout (seconds)

        # Django-standard “pass-through”:
        "OPTIONS": {
            "size_limit": 2 ** 30,      # 1 GiB (DiskCache setting)
        },
    }
}
```

**Only `BACKEND` and `LOCATION` are required**; the rest are defaults shown explicitly. ([Grant Jenks][1])

### How these keys relate to Django’s cache framework

Django defines the core `CACHES` options:

* `LOCATION` is “the location of the cache to use” (directory, hostname, etc. depending on backend). ([Django Project][2])
* `TIMEOUT` is the default TTL (seconds); `None` means “never expire”; `0` means “immediately expire / don’t cache.” ([Django Project][2])
* `OPTIONS` is a backend-specific mapping for extra parameters (exact contents vary per backend). ([Django Project][2])
* Optional but commonly used: `KEY_PREFIX`, `VERSION`, and `KEY_FUNCTION` determine how Django composes final keys. ([Django Project][2])

### DiskCache-specific settings (DjangoCache → FanoutCache)

From the tutorial:

* `SHARDS` controls the Fanout sharding degree.
* `DATABASE_TIMEOUT` is the timeout for each backend database transaction.
* You can pass additional DiskCache settings (e.g., `size_limit`) via `OPTIONS`. ([Grant Jenks][1])

---

## K.3 API surface: “Django cache backend + DiskCache superset”

DiskCache explicitly says the `DjangoCache` API is a **superset** of Django’s caching docs and includes many FanoutCache features. ([Grant Jenks][1])

### K.3.1 Standard Django-ish methods (plus versioning)

The API reference shows `DjangoCache` implements typical backend operations **with Django-style parameters**:

* `get(key, default=None, version=None, ...)`
* `set(key, value, timeout=DEFAULT, version=None, ...)`
* `add(key, value, timeout=DEFAULT, version=None, ...)`
* `delete(key, version=None, ...)`
* `touch(key, timeout=DEFAULT, version=None, ...)`
* `incr/decr(key, delta=..., version=None, default=...)`
* `has_key(key, version=None)` ([Grant Jenks][3])

Notable details:

* `delete(...)` is described as “failing silently” (consistent with Django backend norms). ([Grant Jenks][3])
* `incr/decr(...)` return “new value on success else `None`” and raise `ValueError` if missing and `default` is `None` (DiskCache’s documented behavior). ([Grant Jenks][3])

### K.3.2 DiskCache-only extensions available through the Django cache object

These are *not* part of Django’s minimal backend contract, but are exposed by `diskcache.DjangoCache`:

**DiskCache metadata and bulk ops**

* Tagging + tag eviction: `set(..., tag=...)`, `evict(tag)`, `create_tag_index()`, `drop_tag_index()` ([Grant Jenks][3])
* Manual maintenance: `expire()`, `cull()`, `stats()` ([Grant Jenks][3])

**Fanout-style named substructures**

* `cache(name)` → returns a `Cache` in a subdirectory
* `deque(name, maxlen=None)` → returns a persistent `Deque` in a subdirectory
* `index(name)` → returns a persistent `Index` in a subdirectory ([Grant Jenks][3])

**File-handle read path**

* `read(key, version=None)` returns a file handle to a stored value. ([Grant Jenks][3])

---

## K.4 Timeout/retry semantics and “what correctness looks like” in Django

### K.4.1 Two different “timeouts” exist

1. **Django `TIMEOUT`**: per-entry TTL semantics (staleness). ([Django Project][2])
2. **DiskCache `DATABASE_TIMEOUT`**: how long a single SQLite transaction waits before it times out (contention control). ([Grant Jenks][1])

### K.4.2 `DjangoCache` never raises `diskcache.Timeout` → failures present as misses/no-ops

DiskCache is explicit that `DjangoCache` will never raise a `Timeout` exception. ([Grant Jenks][1])

So under heavy contention, the “symptom surface” becomes:

* reads returning `default` (a miss),
* writes returning `False` (or `None` for incr/decr) rather than raising. ([Grant Jenks][3])

### K.4.3 Default `retry` posture (and the subtle exception)

DiskCache’s tutorial states that unlike `FanoutCache`, `DjangoCache` methods default `retry=True`. ([Grant Jenks][1])
The API reference shows the concrete defaults:

* `set/add/delete/incr/decr/touch/pop` default `retry=True` ([Grant Jenks][3])
* `get(..., retry=False)` defaults to **no retry** (so in contention scenarios it can preferentially “fail fast into default/miss”). ([Grant Jenks][3])

**Practical interpretation (for correctness):**

* Treat DjangoCache as a **best-effort performance layer**, not as the only source of truth.
* If you truly need “must write,” you can explicitly check return values on `set/add/delete` (and consider raising/logging when they’re false). ([Grant Jenks][3])

---

## K.5 High-leverage DjangoCache patterns

### Pattern 1 — Use `OPTIONS` to set DiskCache sizing + eviction policy

Django’s `OPTIONS` is the official place for backend-specific knobs. ([Django Project][2])
DiskCache’s tutorial shows passing `size_limit` via `OPTIONS`. ([Grant Jenks][1])

Common “baseline” options you’ll likely set (DiskCache settings):

* `size_limit` (bytes cap)
* `cull_limit` (how much cleanup to do during writes)
* `eviction_policy` (e.g., least-recently-stored)
* `tag_index=True` if you plan to use tags/evict at scale

### Pattern 2 — Group invalidation via tags (backend-specific superpower)

Because `DjangoCache.set/add` accept `tag=...` and `DjangoCache.evict(tag)` exists, you can implement “soft namespaces” or deploy busting without relying solely on `KEY_PREFIX/VERSION`. ([Grant Jenks][3])

### Pattern 3 — Large media via `read()` + X-Accel-Redirect / X-Sendfile

DiskCache’s tutorial gives a Django view pattern:

* `cache.read(path)` returns a file handle, and when values are stored with `read=True` they’re guaranteed to be stored in files; the filesystem path is available as `reader.name`. ([Grant Jenks][1])

This enables efficient “store once, serve via web server” flows for cached media.

### Pattern 4 — “Critical state isolation” using `cache(name)` / `deque(name)` / `index(name)`

Since `DjangoCache` can vend:

* a plain `Cache` in a subdirectory (`cache(name)`),
* a persistent queue (`deque(name)`),
* and a persistent ordered map (`index(name)`),
  you can keep your Django page/data cache separate from durable coordination state (locks, work queues, resumable indexes). ([Grant Jenks][3])

---

If you want to keep following the outline, next is **L) Configuration & tuning control plane** — how DiskCache settings are stored (Settings table), `reset()`, and the `sqlite_*` / `disk_*` knobs (WAL, mmap, synchronous, cache_size) that matter most when Django workers scale up.

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://docs.djangoproject.com/en/6.0/ref/settings/ "Settings | Django documentation | Django"
[3]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"

## Chapter L — Configuration & tuning control plane (Settings table, `reset()`, `sqlite_*` / `disk_*`)

DiskCache’s “control plane” is intentionally **durable and shared**:

* Settings are **stored in the cache’s SQLite database (the `Settings` table)** “for durability and to communicate between processes.” Each setting is also exposed as a same-named **attribute** on the cache object. ([Grant Jenks][1])
* Settings are **lazy-loaded / can be stale** (e.g., `cache.count`), and are intended to be **set at initialization** (keyword args) or updated via `reset()`. ([Grant Jenks][1])
* Settings with a `disk_` prefix map to **Disk serializer attributes**, and settings with a `sqlite_` prefix map to **SQLite PRAGMAs** (and changing them executes the PRAGMA). ([Grant Jenks][1])

---

# L.1 Settings architecture: “durable config + lazy attributes”

### Where settings live

* DiskCache stores settings in the **SQLite `Settings` table** and caches them as attributes with the same name (e.g., `cache.size_limit`, `cache.sqlite_journal_mode`). ([Grant Jenks][1])

### How values propagate across many Django workers

* Because settings are stored in the DB, they can be used as a coordination channel across processes. ([Grant Jenks][1])
* But **per-process cache attributes can be stale**; the tutorial explicitly recommends preferring “idioms like `len(cache)`, `volume()`, and keyword arguments” over repeatedly poking `reset()` to observe live state. ([Grant Jenks][1])

---

# L.2 `reset()` deep semantics (the “only correct way” to mutate settings)

### API and behavioral contract

`Cache.reset(key, value=ENOVAL, update=True)` (and analogous methods on FanoutCache/DjangoCache) is the sanctioned mutation mechanism:

* If `value` is omitted, it **reloads** from the Settings table.
* If `value` is provided, it **updates** the Settings table (unless `update=False`). ([Grant Jenks][2])
* **`disk_…` settings:** updating the value updates the **unprefixed attribute** on the associated `Disk` instance. ([Grant Jenks][2])
* **`sqlite_…` settings:** updating the value **executes the corresponding SQLite PRAGMA**. ([Grant Jenks][2])
* You can execute PRAGMAs before the Settings table exists by calling `reset(..., update=False)` (useful for PRAGMAs like `auto_vacuum` that must be set before tables are created). ([Grant Jenks][2])

### Lazy attribute footgun (explicit in docs)

The tutorial demonstrates “stale attribute” behavior and then a forced reload using `reset('count')`. ([Grant Jenks][1])

---

# L.3 Settings catalog that matters for scale (with defaults)

These defaults are straight from DiskCache’s Settings chapter:

| Category | Setting                |                 Default | What it controls                                                                 |
| -------- | ---------------------- | ----------------------: | -------------------------------------------------------------------------------- |
| Cache    | `size_limit`           |                   1 GiB | Target maximum on-disk cache size ([Grant Jenks][1])                             |
| Cache    | `cull_limit`           |                      10 | Max keys culled during `set/add`; `0` disables lazy culling ([Grant Jenks][1])   |
| Cache    | `statistics`           |                   False | Hit/miss recording (adds overhead) ([Grant Jenks][1])                            |
| Cache    | `tag_index`            |                   False | Creates tag index to speed `evict(tag)` ([Grant Jenks][1])                       |
| Cache    | `eviction_policy`      | `least-recently-stored` | Which non-expired keys are evicted under size pressure ([Grant Jenks][1])        |
| Disk     | `disk_min_file_size`   |                  32 KiB | Minimum value size to store as a separate file (vs in SQLite) ([Grant Jenks][1]) |
| Disk     | `disk_pickle_protocol` |                 highest | Pickle protocol for non-native types ([Grant Jenks][1])                          |
| SQLite   | `sqlite_auto_vacuum`   |              `FULL` (1) | File truncation / free-page handling strategy ([Grant Jenks][1])                 |
| SQLite   | `sqlite_cache_size`    |              8192 pages | Per-connection page cache target ([Grant Jenks][1])                              |
| SQLite   | `sqlite_journal_mode`  |                   `wal` | WAL journaling mode (concurrency posture) ([Grant Jenks][1])                     |
| SQLite   | `sqlite_mmap_size`     |                  64 MiB | Max bytes for memory-mapped I/O per DB ([Grant Jenks][1])                        |
| SQLite   | `sqlite_synchronous`   |            `NORMAL` (1) | Sync frequency vs durability tradeoff (esp. in WAL) ([Grant Jenks][1])           |

DiskCache also notes these settings can be passed to **DjangoCache** via the Django `OPTIONS` mapping. ([Grant Jenks][1])

---

# L.4 The `sqlite_*` knobs: what they mean under Django worker scale

When Django workers scale up, you’re effectively multiplying “SQLite connection footprints” across processes—so each SQLite PRAGMA has an **aggregate** cost.

## L.4.1 `sqlite_journal_mode = "wal"`: concurrency posture + filesystem constraints

DiskCache defaults to WAL and explicitly lists WAL as a performance technique. ([Grant Jenks][1])

From SQLite:

* WAL uses a **write-ahead log** instead of a rollback journal and is **persistent across connections / reopen**. ([SQLite][3])
* WAL improves concurrency (readers don’t block writers and writers don’t block readers), but **all processes must be on the same host** and WAL **does not work over network filesystems** (shared-memory wal-index requirement). ([SQLite][4])

**Django scaling implication:** WAL is a good default for “many workers on one host,” but it’s a hard stop for “share one cache directory over NFS between hosts.” ([SQLite][4])

## L.4.2 `sqlite_synchronous = NORMAL`: safe from corruption in WAL, but not fully durable

SQLite’s synchronous modes define how aggressively SQLite uses `fsync`:

* `FULL` ensures content is written safely before continuing; in WAL mode it’s ACID-safe against corruption. ([SQLite][3])
* `NORMAL` syncs less often; **WAL mode is safe from corruption with `synchronous=NORMAL`**, but durability can be lost: a transaction committed in WAL+NORMAL might roll back after a power loss/system crash (application crashes are still handled). ([SQLite][3])

DiskCache chooses `NORMAL` by default, consistent with “cache semantics”: it’s OK if the cache loses the last few milliseconds of writes in a power-loss event. ([Grant Jenks][1])

**Django scaling implication:** keeping `NORMAL` reduces sync overhead under high write concurrency. If you’re using DiskCache as a correctness-critical store (not recommended), consider `FULL`—but expect higher write latency. ([SQLite][3])

## L.4.3 `sqlite_cache_size`: per-connection memory multiplier

SQLite’s `cache_size` is a **per-open-database-file / per-connection** page cache suggestion. ([SQLite][3])
Critically:

* Changing `PRAGMA cache_size` only endures for the **current session**; it reverts on close/reopen. ([SQLite][3])

DiskCache stores `sqlite_cache_size` in its Settings table and executes the PRAGMA when you update it via `reset()`. ([Grant Jenks][2])

**Django scaling implication:** if you have N worker processes and each ends up with its own SQLite connection(s), your effective page-cache memory consumption can be “(cache_size pages) × (page_size) × (connections).” (Page size depends on the DB; measure before changing.)

## L.4.4 `sqlite_mmap_size`: faster reads, but also a per-connection resource

SQLite `mmap_size` sets the maximum bytes used for memory-mapped I/O on a database. ([SQLite][3])
Notable nuance:

* It may be a **no-op** if you try to change it while statements are running on the same connection. ([SQLite][3])

DiskCache explicitly lists “memory-mapped pages to accelerate reads” as a performance technique and defaults `sqlite_mmap_size` to 64 MiB. ([Grant Jenks][1])

**Django scaling implication:** raising `mmap_size` can help read-heavy workloads, but like `cache_size`, it becomes a “per-connection multiplier.” Keep it modest unless you’ve measured a read bottleneck. ([SQLite][3])

## L.4.5 `sqlite_auto_vacuum = FULL`: shrink-on-delete tradeoffs

SQLite:

* `auto_vacuum=FULL` moves free pages to the end of the DB and truncates at each commit; it reclaims disk size but **can increase fragmentation** and adds per-commit work. ([SQLite][3])
* You generally cannot flip `auto_vacuum` on/off after tables exist without running `VACUUM`, and enabling it requires extra bookkeeping. ([SQLite][3])

DiskCache chooses `FULL` by default—consistent with cache churn where you’d prefer the DB file not to grow forever. ([Grant Jenks][1])

---

# L.5 The `disk_*` knobs: file-vs-SQLite boundary (very relevant to ops)

DiskCache’s implementation explicitly mixes “store small values in SQLite database and large values in files.” ([Grant Jenks][1])

## L.5.1 `disk_min_file_size` (default 32 KiB): tune for file-count vs DB bloat

* If you **lower** it: more values go to files → more filesystem entries (inode pressure, directory traversal overhead), but smaller SQLite DB.
* If you **raise** it: more values stay inline in SQLite → fewer files, but bigger SQLite DB pages / more DB I/O.

DiskCache documents the setting as “minimum size to store a value in a file.” ([Grant Jenks][1])

**Django scaling implication:** lots of small cached fragments (template fragments, rendered JSON blobs) often benefit from keeping small values inline (raise `disk_min_file_size` slightly), but only after measuring `volume()`, file counts, and latency. ([Grant Jenks][1])

## L.5.2 `disk_pickle_protocol`: cross-version compatibility knob

DiskCache uses pickle for non-native types; the protocol default is “highest.” ([Grant Jenks][1])
If you ever run mixed Python versions against the same cache directory (not typical for Django, but possible during rolling deploys), pinning this can reduce surprises.

---

# L.6 Recommended “Django workers are scaling up” tuning workflow

### 1) Prefer configuration via init / Django `OPTIONS`, not ad-hoc `reset()`

DiskCache recommends setting attributes during initialization and prefers keyword args over using `reset()` directly for routine usage. ([Grant Jenks][1])

### 2) Keep DiskCache’s SQLite defaults unless you have a measured bottleneck

DiskCache already defaults to WAL + mmap + NORMAL synchronous + a nontrivial cache size because those are the main levers. ([Grant Jenks][1])

### 3) Measure impact safely

* Temporarily enable `statistics` (expect lookup overhead) to see hit/miss patterns while comparing configurations. ([Grant Jenks][1])
* Track p95/p99 latency of cache operations (especially writes) because SQLite still has a single-writer constraint even with WAL; DiskCache’s sharding (`FanoutCache`/`DjangoCache`) exists specifically to reduce writer contention. ([Grant Jenks][1])

---

# L.7 Concrete configuration examples

### L.7.1 Django `CACHES` with DiskCache tuning knobs

Put worker-scale choices here (so every worker starts consistent): ([Grant Jenks][1])

```python
CACHES = {
  "default": {
    "BACKEND": "diskcache.DjangoCache",
    "LOCATION": "/var/cache/myapp",
    "TIMEOUT": 300,
    "SHARDS": 16,               # ~ concurrent writers
    "DATABASE_TIMEOUT": 0.010,  # contention budget
    "OPTIONS": {
      "size_limit": 2**30,             # bytes
      "cull_limit": 10,
      "tag_index": True,
      "sqlite_mmap_size": 256 * 2**20, # bytes
      "sqlite_cache_size": 8192,       # pages
      "sqlite_synchronous": 1,         # NORMAL
      "sqlite_journal_mode": "wal",
      "disk_min_file_size": 64 * 1024,
    },
  }
}
```

### L.7.2 Programmatic adjustment (maintenance window / one-off)

Use `reset()` so the change is durable and PRAGMAs are executed. ([Grant Jenks][2])

```python
from diskcache import Cache
cache = Cache("/var/cache/myapp")

cache.reset("sqlite_mmap_size", 256 * 2**20)
cache.reset("sqlite_cache_size", 16384)
cache.reset("disk_min_file_size", 64 * 1024)
```

---

If you want to keep following the outline, next is **M) Storage engine & serialization internals (`Disk`)** — how keys/values are stored (SQLite native types vs pickled blobs vs file-backed values), what `disk_min_file_size` actually changes on disk, and how to design a “stable-key” serialization strategy for long-lived caches.

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[3]: https://www.sqlite.org/pragma.html "Pragma statements supported by SQLite"
[4]: https://sqlite.org/wal.html "Write-Ahead Logging"

## Chapter M — Storage engine & serialization internals (`Disk`)

At the bottom of DiskCache’s stack is the `Disk` object: a **serialization/IO engine** that turns Python keys/values into a mix of **SQLite metadata rows** + optional **filesystem `.val` blobs**. DiskCache’s own tutorial summarizes the key boundary:

* **Keys are always stored in the cache metadata database** (SQLite).
* **Values are sometimes stored in SQLite and sometimes stored in files**, depending on type/size and `read=True`. ([Grant Jenks][1])

---

# M.1 Public API surface (`Disk` / `JSONDisk`)

### `Disk` (base engine)

`Disk(directory, min_file_size=0, pickle_protocol=0)` and its core methods: `put/get` for keys, `store/fetch` for values, plus `filename/remove/hash`. ([Grant Jenks][2])

* `put(key) -> (db_key, raw_bool)` and `get(db_key, raw_bool) -> key` ([Grant Jenks][2])
* `store(value, read, key=UNKNOWN) -> (size, mode, filename, value)` and `fetch(mode, filename, value, read) -> value_or_reader` ([Grant Jenks][2])
* `filename(...)` uses **random 28-hex + `.val`** and **two-level subdirectories** to avoid huge directories. ([Grant Jenks][2])
* `hash(key)` is a **portable hash** (important because Python’s built-in hash is not stable across processes). ([Grant Jenks][2])
* `remove(file_path)` is cross-thread/process safe and suppresses `OSError`. ([Grant Jenks][2])

### `JSONDisk` (convenience subclass)

`JSONDisk(directory, compress_level=1, **kwargs)` serializes keys/values as JSON and compresses with zlib. ([Grant Jenks][2])

---

# M.2 Physical storage model: SQLite tables + optional blob files

## M.2.1 The Cache table is explicitly “metadata + pointers”

DiskCache’s core SQLite table includes (key pieces only):

* `key BLOB`, `raw INTEGER`
* `tag BLOB`
* `size INTEGER`, `mode INTEGER`, `filename TEXT`, `value BLOB`
* plus timestamps and counters (`store_time`, `expire_time`, `access_time`, `access_count`) ([Debian Sources][3])

That layout matches Disk’s contract:

* **`key/raw`** come from `Disk.put(key)`
* **`mode/filename/value`** come from `Disk.store(value, ...)`
* and `Disk.fetch(...)` reverses it.

## M.2.2 Settings drive Disk construction

On cache initialization, DiskCache:

1. loads/merges settings
2. creates the Settings table
3. constructs the Disk instance using **all `disk_*` settings (prefix stripped)** ([Debian Sources][3])

So `disk_min_file_size` and `disk_pickle_protocol` are not “just config”; they literally parameterize the `Disk` object used for encoding/decoding. ([Grant Jenks][1])

---

# M.3 Key serialization (`put/get`) and the “stable key” problem

## M.3.1 The `raw` flag: “native SQLite type” vs “pickled bytes”

The default `Disk.put(key)` logic is (in effect):

* `bytes` → store as `sqlite3.Binary(...)`, `raw=True`
* `str`, `float`, and **64-bit-range `int`** → store directly, `raw=True`
* everything else → `pickle.dumps(..., protocol=pickle_protocol)`, `pickletools.optimize`, store as `sqlite3.Binary(...)`, `raw=False` ([Debian Sources][3])

`Disk.get(db_key, raw)` reverses this:

* if `raw=True`: return the DB value as `bytes`/`str`/`int`/`float`
* else: unpickle the stored bytes ([Debian Sources][3])

## M.3.2 Equality is by serialization, not by Python hashing

DiskCache explicitly does **not** use Python’s `__hash__` / `__eq__` for lookups. Lookups depend on the serialization defined by the Disk implementation. ([Grant Jenks][1])

**Concrete implications DiskCache calls out:**

* `1` and `1.0` compare equal as keys (as they do in Python) — so they collide. ([Grant Jenks][1])
* “Large integers and all other types” become bytes via pickle and the bytes representation defines equality. ([Grant Jenks][1])

## M.3.3 Footgun: pickle is not reliably stable for some container keys

DiskCache warns that pickling can produce inconsistencies for container keys like tuples: two equal tuples can serialize to different bytes, making lookups fail. It recommends using an alternative Disk type like `JSONDisk` for consistent key serialization. ([Grant Jenks][1])

## M.3.4 Portable hashing (why FanoutCache can shard deterministically)

`Disk.hash(key)` computes a portable hash from the **serialized key** (e.g., Adler-32 over binary/utf-8/packed float, integer mod). ([Grant Jenks][2])
This avoids Python’s per-process hash randomization for strings/bytes, which would break sharding if you used `hash(key)` directly.

---

# M.4 Value storage (`store/fetch`): SQLite inline vs `.val` files

DiskCache stores values in one of several “modes,” selected by **type**, **size**, and the **`read` flag**.

## M.4.1 The decision tree (default `Disk.store`)

From the implementation:

### 1) Inline (“MODE_RAW”) for native SQLite scalars and small-ish payloads

* `int` (within 64-bit signed range) and `float` → inline raw
* `str` with `len(str) < min_file_size` → inline raw
* `bytes` with `len(bytes) < min_file_size` → inline raw as `sqlite3.Binary(bytes)` ([Debian Sources][3])

### 2) File-backed (“MODE_BINARY” / “MODE_TEXT”) for larger byte/string payloads

* `bytes` with `len(bytes) >= min_file_size` → write to a `.val` file, store `(mode=MODE_BINARY, filename=...)` with DB `value=None` ([Debian Sources][3])
* `str` with `len(str) >= min_file_size` → write UTF-8 text file (`MODE_TEXT`) with DB `value=None` ([Debian Sources][3])

### 3) `read=True` forces file-backed write for file-like inputs

If you call `cache.set(key, fileobj, read=True)`, Disk writes the stream to a file (`MODE_BINARY`) in chunks and stores a filename pointer. ([Debian Sources][3])

### 4) Pickled objects (“MODE_PICKLE”) go inline or file depending on pickle size

For “everything else” (non-native types), Disk pickles the value:

* if `len(pickle_bytes) < min_file_size` → store inline in SQLite (`MODE_PICKLE`, `value=sqlite3.Binary(pickle_bytes)`)
* else → store pickle bytes in a file and store `(MODE_PICKLE, filename=...)` with DB `value=None` ([Debian Sources][3])

## M.4.2 Reading values back (`fetch`)

`Disk.fetch(mode, filename, value, read)` reverses the above:

* `MODE_RAW`: returns raw scalar/bytes from the DB
* `MODE_BINARY`: returns `open(..., 'rb')` if `read=True`, else reads bytes
* `MODE_TEXT`: reads UTF-8 text from file
* `MODE_PICKLE`: unpickles from file if DB `value is None`, else unpickles from the inline DB bytes ([Debian Sources][3])

---

# M.5 `disk_min_file_size` and what it *actually* changes on disk

## M.5.1 Definition and defaults

* DiskCache exposes `disk_min_file_size` (default **32 KiB**) — “values with greater size are stored in files.” ([Grant Jenks][1])
* The API uses the same concept as `Disk(min_file_size=...)`. ([Grant Jenks][2])

## M.5.2 What moves when you change it

Raising/lowering `disk_min_file_size` shifts the boundary for:

* raw bytes inline vs `.val` file (`bytes`)
* raw string inline vs `.val` file (`str`)
* pickled blob inline vs `.val` file (non-native types) ([Debian Sources][3])

**Operational tradeoff (practical):**

* higher threshold → fewer files, larger SQLite DB blobs
* lower threshold → more `.val` files, smaller DB blobs, more filesystem metadata churn

DiskCache also notes that `volume()` accounts for DB size + file sizes but not directory metadata; if directory count/size matters, consider a custom Disk. ([Grant Jenks][1])

---

# M.6 File naming and directory sharding (`filename` / `remove`)

## M.6.1 Filename scheme

The default scheme:

* generates 16 random bytes → 32 hex chars
* uses first 2 and next 2 chars as a two-level subdirectory
* uses the remaining 28 hex chars + `.val` as the filename ([Debian Sources][3])

This is explicitly to prevent directories with enormous file counts (slow on older filesystems). ([Grant Jenks][2])

## M.6.2 Deletion is race-tolerant

`Disk.remove(file_path)` suppresses `OSError` and attempts to remove now-empty parent directories, making it safe under concurrent deletion attempts. ([Grant Jenks][2])

---

# M.7 Designing a “stable-key” serialization strategy (recommended patterns)

DiskCache’s own caveat is the guiding principle:

> Keys are looked up by their serialized representation; pickling can be inconsistent for container keys; consider JSONDisk or a custom Disk. ([Grant Jenks][1])

### Strategy 1 — Use primitive key types only

Prefer `str` / `bytes` / small `int` keys and keep a single canonical representation:

* don’t mix `1` and `1.0` as keys (they collide) ([Grant Jenks][1])
* don’t use large ints outside 64-bit range unless you intentionally want “pickle-bytes identity” behavior ([Debian Sources][3])

### Strategy 2 — If you need structured keys, switch Disk for deterministic encoding

Use `JSONDisk` (or your own Disk subclass) so “equal structure → identical bytes,” avoiding pickle instability. DiskCache explicitly recommends JSONDisk for consistent key serialization. ([Grant Jenks][1])

### Strategy 3 — Version your key schema explicitly

For long-lived caches, make key strings include a schema/version prefix (or use tags + `evict(tag)` on version changes). (This is a practical pattern built on DiskCache’s tag + eviction mechanics; it keeps you from “migrating serialized keys in place.”) ([Grant Jenks][1])

---

# M.8 Minimal runnable snippets

## 1) Use `JSONDisk` for consistent structured keys + compressed values

```python
import json, zlib
from diskcache import Cache, JSONDisk

# JSONDisk is built-in; compress_level controls zlib compression.
with Cache("data/cache", disk=JSONDisk, disk_compress_level=6) as cache:
    cache.set({"k": [1, 2]}, {"v": "payload"})
    assert cache[{"k": [1, 2]}] == {"v": "payload"}
```

DiskCache’s tutorial shows this exact pattern (Disk subclass + `disk_*` kwargs). ([Grant Jenks][1])

## 2) Force large values to live as files (lower `disk_min_file_size`)

```python
from diskcache import Cache

cache = Cache("data/cache", disk_min_file_size=1)  # almost everything becomes file-backed
cache["blob"] = b"x" * 10_000
```

This works because the file-vs-inline boundary is controlled by `min_file_size`. ([Grant Jenks][1])

---

If you want to keep following the outline, the next chapter is **N) Caveats, footguns, and operational hazards**—and we can turn these Disk-layer issues (serialization equality, NFS/SQLite caveats, disk-full behavior, file/directory overhead) into a crisp “production checklist + mitigations.” ([Grant Jenks][1])

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[3]: https://sources.debian.org/src/diskcache/5.4.0-1/diskcache/core.py "File: core.py
\| Debian Sources
"

## Chapter N — Caveats, footguns, and operational hazards (production checklist + mitigations)

### N.1 Key identity & serialization hazards (the #1 “surprise”)

**What can go wrong**

* DiskCache does **not** use Python’s `__hash__`/`__eq__` for lookups; it uses the **serialized representation** produced by the configured `Disk`. That means “key equality” is really “serialized-bytes equality” for many types. ([Grant Jenks][1])
* Only `int/float/str/bytes` are stored natively; other key types are pickled, and the **bytes representation defines equality**. Also note that `1` and `1.0` compare equal as keys (Python semantics), so they can collide. ([Grant Jenks][1])
* Default `Disk` uses **pickle** for keys/values, and DiskCache explicitly warns that pickling can be **inconsistent for container keys** like tuples (equal tuples can serialize to different bytes), which can make “I set it but can’t get it back” failures. DiskCache recommends using `JSONDisk` (or another `Disk`) for consistent key serialization. ([Grant Jenks][1])

**Security footgun**

* Pickle is **not secure**: unpickling data from an untrusted or tampered cache directory can execute arbitrary code. If multiple users/processes can write into the cache directory (or it sits on untrusted storage), treat this as a real RCE risk. ([Python documentation][2])

**Mitigations (pick one, don’t “half do” it)**

1. **Use primitive, canonical string keys** (most robust)

   * e.g., `key = f"v1:user:{user_id}:region:{region}"`
   * Explicitly encode types to avoid `1` vs `1.0` surprises.
2. **Switch to a stable `Disk` for structured keys**

   * Start with `JSONDisk` if it fits, but for long-lived caches prefer a *canonical* JSON encoding (e.g., `sort_keys=True`, fixed separators) in a custom `Disk` subclass so semantically-equal dicts serialize identically. (DiskCache’s docs explicitly encourage alternative `Disk` types for consistent key serialization.) ([Grant Jenks][1])
3. **Treat the cache directory as trusted data** (permissions + ownership)

   * If you stick with pickle, lock down the directory (no shared write access outside your service user).

---

### N.2 SQLite + filesystem hazards (NFS/network shares/VM shared folders = danger)

**What can go wrong**

* DiskCache uses SQLite for synchronization and therefore “inherits all SQLite caveats.” DiskCache calls out that SQLite is **not recommended on NFS mounts**, and notes users have reported problems on PythonAnywhere and Parallels shared folders. ([Grant Jenks][1])
* If you’re using WAL (DiskCache defaults to it), SQLite is explicit: **WAL does not work over a network filesystem**, because WAL requires shared memory for the wal-index. ([SQLite][3])
* SQLite’s locking documentation bluntly warns that POSIX advisory locks are buggy/unimplemented on many NFS implementations, and that “your best defense is to not use SQLite for files on a network filesystem.” ([SQLite][4])

**Mitigations**

* **Do not** place a DiskCache directory on NFS/Samba/iSCSI/“shared folders”. Keep it on **local disk** (SSD) per host. ([Grant Jenks][1])
* If you need a **distributed cache** (shared across hosts), use a network-native cache (Redis/Memcached/etc.) and keep DiskCache as “node-local”. (This is the practical implication of the WAL + locking constraints.)

---

### N.3 “Disk full” and write failure modes (and the stats trap)

**What can go wrong**

* DiskCache explicitly states: when the **disk or database is full**, any method that attempts to write raises `sqlite3.OperationalError`. Reads still succeed **only if they don’t cause a write**, which *can happen* if cache statistics are enabled/recorded. ([Grant Jenks][1])

**Mitigations**

* Treat disk-full as a **service health incident**, not an “expected error.”
* Keep `statistics=False` in production unless actively measuring, because it can turn some “reads” into “writes” and fail under disk pressure. ([Grant Jenks][1])
* Enforce bounded growth:

  * set `size_limit`,
  * decide between lazy culling (`cull_limit>0`) vs scheduled maintenance (`cull_limit=0` + periodic `cull()`), and
  * monitor `volume()` plus OS-level disk headroom. ([Grant Jenks][1])
* Wrap writes with a graceful fallback:

  * if you hit `OperationalError`, degrade to “cache disabled” mode (don’t crash the request path).

---

### N.4 File/directory overhead (inode pressure, slow directory ops, misleading `volume()`)

**What can go wrong**

* DiskCache stores values partly in SQLite and partly as files (depending on type/size/`read=True`). This can create **many `.val` files** and substantial directory metadata churn at scale. ([Grant Jenks][1])
* DiskCache warns that `volume()` measures DB size + value files but **does not account for directory size itself or other filesystem metadata**, and suggests using an alternative `Disk` if directory count/size is a concern. ([Grant Jenks][1])

**Mitigations**

* Tune `disk_min_file_size` upward if you’re generating too many small files; use `read=True` only when you genuinely want file-backed streaming semantics. ([Grant Jenks][1])
* Put the cache directory on a filesystem suited for lots of small files (local SSD, sane inode provisioning).
* Monitor:

  * file count (inodes),
  * directory size,
  * and latency spikes during heavy churn.

---

### N.5 Maintenance & repair hazards (`check()` locks writers; bulk ops are iterative)

**What can go wrong**

* `cache.check()` holds a **writer lock** on the DB while checking consistency; DiskCache notes that for caches with many file references the lock can be held “for a long time” (example: ~60ms for 1,000 file references). ([Grant Jenks][5])
* Bulk removals (`clear/expire/evict/cull`) are **iterative**; concurrent writes may occur between iterations, and timeouts can yield partial completion (count in `Timeout.args[0]`). ([Grant Jenks][5])

**Mitigations**

* Treat `check()` like `fsck`: run it **offline / maintenance window**, not per-request. ([Grant Jenks][5])
* For scheduled maintenance jobs, log partial progress on `Timeout` and retry later.

---

### N.6 FanoutCache/DjangoCache timeout semantics (silent failure if you’re not explicit)

**What can go wrong**

* FanoutCache catches internal `Timeout` errors and **aborts operations**; as a result, `set` and `delete` may **silently fail**. ([Grant Jenks][1])
* This is a feature for bounding tail latency, but it’s a correctness hazard if you accidentally treat the cache as authoritative.

**Mitigations**

* Make an explicit choice:

  * **Best-effort cache**: accept occasional no-ops/misses.
  * **Must-stick cache writes**: pass `retry=True` and check return values, or isolate critical state into a non-fanout `Cache` shard. ([Grant Jenks][1])

---

## Production checklist (copy/paste into your “DiskCache-advanced.md”)

* **Storage placement**

  * ✅ Local disk only; ❌ NFS/network shares/VM shared folders. ([Grant Jenks][1])
* **Key strategy**

  * ✅ Primitive string keys (typed prefixes) *or* stable Disk serialization; ❌ tuple/dict keys with default pickle. ([Grant Jenks][1])
  * ✅ Treat cache dir as trusted; ❌ untrusted write access when using pickle. ([Python documentation][2])
* **Size + disk pressure**

  * ✅ Set `size_limit`; choose lazy vs scheduled `cull`; keep stats off unless measuring. ([Grant Jenks][1])
  * ✅ Monitor disk headroom + file counts; remember `volume()` ignores directory metadata. ([Grant Jenks][1])
* **Maintenance**

  * ✅ Run `check()` only in maintenance; it takes a writer lock. ([Grant Jenks][5])
* **Fanout correctness**

  * ✅ Treat FanoutCache as best-effort unless you opt into `retry=True` + return checks; be aware of silent failure modes. ([Grant Jenks][1])
* **Durability expectations**

  * ✅ If you’re using DiskCache as a cache, WAL+`synchronous=NORMAL` durability tradeoffs are usually fine;
  * ⚠️ If you’re (mis)using it as durable state, note SQLite: WAL+`synchronous=NORMAL` can roll back recent commits after power loss/system crash. ([SQLite][6])

If you want to keep following the outline, the next chapter is **O) Implementation details** (SQLite schema/indexes, file layout, triggers, and how those design choices explain performance characteristics like “read doesn’t write under LRS”).

[1]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[2]: https://docs.python.org/3/library/pickle.html?utm_source=chatgpt.com "pickle — Python object serialization"
[3]: https://sqlite.org/wal.html?utm_source=chatgpt.com "Write-Ahead Logging"
[4]: https://sqlite.org/lockingv3.html?utm_source=chatgpt.com "File Locking And Concurrency In SQLite Version 3"
[5]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[6]: https://www.sqlite.org/pragma.html?utm_source=chatgpt.com "Pragma statements supported by SQLite"

## Chapter O — Implementation details (schema, indexes, triggers, file layout, and why LRS reads don’t write)

DiskCache (latest PyPI release **5.6.3**, Aug 31 2023) is fundamentally **SQLite + filesystem**, with a small set of deliberate implementation choices: SQLite native types, WAL, mmap, sharding (FanoutCache), “small values in DB / large values in files”, and “always use a SQLite index for queries.” ([PyPI][1])

---

# O.1 On-disk layout (what actually appears in the cache directory)

### O.1.1 Database files

DiskCache’s SQLite database file name is literally:

* `cache.db` ([arcovid19.readthedocs.io][2])

Because DiskCache defaults to SQLite **WAL** journaling (`sqlite_journal_mode="wal"`), you should expect *additional* files alongside the main DB during active use:

* `cache.db-wal` (write-ahead log)
* `cache.db-shm` (shared-memory / wal-index)

SQLite documents that WAL mode creates a `-wal` file and a `-shm` file “in the same directory as the database” using the base database filename + suffix. ([SQLite][3])

### O.1.2 Value files (`.val`) and directory sharding

When values are stored as files (large bytes/strings, pickles over threshold, or `read=True`), DiskCache generates filenames like:

* 28 hex characters + `.val`
* inside **two levels of hex subdirectories** (e.g., `ab/cd/<28hex>.val`)

The `Disk.filename()` docstring and implementation explain the purpose: avoid huge directories (directory lookup performance on older filesystems) by using two levels of subdirectories; it also shows the random hex and `.val` suffix construction. ([arcovid19.readthedocs.io][2])

Deletion is made race-tolerant: `Disk.remove()` suppresses “no entry” errors so two processes trying to remove the same value file won’t explode. ([arcovid19.readthedocs.io][2])

---

# O.2 SQLite schema: `Settings` and `Cache` tables

DiskCache uses two “core” tables:

### O.2.1 `Settings` table (durable config + cross-process metadata)

Created at init:

```sql
CREATE TABLE IF NOT EXISTS Settings (
  key TEXT NOT NULL UNIQUE,
  value
);
```

That DDL is executed during initialization. ([arcovid19.readthedocs.io][2])

DiskCache also seeds defaults and metadata keys. In code, you can see defaults like `statistics`, `tag_index`, `eviction_policy`, `size_limit`, `cull_limit`, plus the `sqlite_*` and `disk_*` settings, and separate “metadata” keys `count`, `size`, `hits`, `misses`. ([arcovid19.readthedocs.io][2])

### O.2.2 `Cache` table (metadata + pointers to values)

Created at init:

```sql
CREATE TABLE IF NOT EXISTS Cache (
  rowid INTEGER PRIMARY KEY,
  key BLOB,
  raw INTEGER,
  store_time REAL,
  expire_time REAL,
  access_time REAL,
  access_count INTEGER DEFAULT 0,
  tag BLOB,
  size INTEGER DEFAULT 0,
  mode INTEGER DEFAULT 0,
  filename TEXT,
  value BLOB
);
```

This is the central record format: key identity, TTL/eviction bookkeeping fields, tag, and “how to fetch the value” (mode/filename/value + size). ([arcovid19.readthedocs.io][2])

**Column intent (practical reading):**

* `key`, `raw`: serialized key and a “raw/native vs pickled” marker.
* `store_time`: used by **least-recently-stored** eviction.
* `expire_time`: TTL, used by expiry scans and read filtering.
* `access_time` / `access_count`: used by **LRU** and **LFU** eviction, respectively.
* `tag`: group label for `evict(tag)`.
* `size`: used for “volume/size-limit” enforcement and accounting.
* `mode` / `filename` / `value`: how the value is stored (inline vs file, etc.).

---

# O.3 Indexing strategy (why operations stay fast at scale)

DiskCache’s docs call out “always use a SQLite index for queries” as a core performance technique. ([Grant Jenks][4])
Concretely, the implementation creates several indexes:

### O.3.1 Key lookup index (hot path)

```sql
CREATE UNIQUE INDEX IF NOT EXISTS Cache_key_raw ON Cache(key, raw);
```

This is why `get(key)` is basically “one indexed lookup by (key, raw)”. ([arcovid19.readthedocs.io][2])

### O.3.2 Expiration scan index

```sql
CREATE INDEX IF NOT EXISTS Cache_expire_time ON Cache(expire_time);
```

So “find expired items” is an indexed range scan, not a full table scan. ([arcovid19.readthedocs.io][2])

### O.3.3 Eviction-policy index (selected at init)

DiskCache has an eviction policy registry that specifies, per policy:

* which index to create (`init`)
* what (if anything) to update on `get` (`get`)
* what query to use to find cull candidates (`cull`)

In code:

* LRS: create index on `store_time`, **no get-update**, cull by `ORDER BY store_time`. ([arcovid19.readthedocs.io][2])
* LRU: create index on `access_time`, update `access_time` on every get, cull by `ORDER BY access_time`. ([arcovid19.readthedocs.io][2])
* LFU: create index on `access_count`, increment `access_count` on every get, cull by `ORDER BY access_count`. ([arcovid19.readthedocs.io][2])

The official docs describe the key consequence: **LRS doesn’t update on access**, while **LRU/LFU turn every access into a read+write**, slowing accesses. ([Grant Jenks][4])

### O.3.4 Tag eviction index (optional)

When you enable `tag_index=True`, DiskCache creates:

```sql
CREATE INDEX IF NOT EXISTS Cache_tag_rowid ON Cache(tag, rowid);
```

This is explicitly implemented by `create_tag_index()` and is recommended over toggling later. ([arcovid19.readthedocs.io][2])

---

# O.4 Triggers (why `count` and `size` exist as durable metadata)

DiskCache maintains two metadata numbers in `Settings` via SQLite triggers:

* `Settings.count`: number of rows (items)
* `Settings.size`: sum of `Cache.size`

Triggers installed at initialization:

* AFTER INSERT / DELETE on `Cache`: `Settings.count += 1` / `-= 1`
* AFTER INSERT / UPDATE / DELETE on `Cache`: adjust `Settings.size` using `NEW.size` / `OLD.size`

You can see the explicit `CREATE TRIGGER ... UPDATE Settings ...` statements for count and size in the implementation. ([arcovid19.readthedocs.io][2])

Why this exists (operationally): it avoids doing `COUNT(*)` and `SUM(size)` repeatedly on large tables in hot paths; the triggers keep those values incrementally updated.

---

# O.5 Connection + transaction model (how concurrency is handled)

### O.5.1 One SQLite connection per thread, fork-aware

DiskCache stores a connection in thread-local storage and checks PID changes to handle process forks; when a new connection is created it’s opened as:

* `sqlite3.connect(<dir>/cache.db, timeout=<timeout>, isolation_level=None)` ([arcovid19.readthedocs.io][2])

`isolation_level=None` means autocommit is on; DiskCache explicitly issues `BEGIN ...` when it wants a transaction.

### O.5.2 “Re-apply sqlite_* pragmas per connection”

Some SQLite PRAGMAs are per-connection. DiskCache responds by reading `Settings` and re-running `reset(key, value, update=False)` for keys starting with `sqlite_` when a connection is established. ([arcovid19.readthedocs.io][2])

### O.5.3 Manual retry for “database is locked” during init/reset

DiskCache includes a `_sql_retry` wrapper that retries statements for up to 60 seconds (sleeping 1ms) when it sees “database is locked”, with a comment referencing real-world variability in SQLite busy handling. ([arcovid19.readthedocs.io][2])

### O.5.4 Transaction lock mode

DiskCache transactions use `BEGIN IMMEDIATE` (grabs a write-reserved lock early) and track a transaction id to allow nesting inside a thread. ([arcovid19.readthedocs.io][2])

---

# O.6 The read path: why “LRS reads don’t write” (and when they *do*)

DiskCache’s `Cache.get()` has an explicit **fast path** vs **slow path** split:

1. Determine whether the eviction policy needs a “get update”:

   * `update_column = EVICTION_POLICY[eviction_policy]['get']` ([arcovid19.readthedocs.io][2])

2. If **statistics are disabled** *and* `update_column is None`, DiskCache takes a **no-transaction fast path**:

   * does a plain `SELECT ... FROM Cache WHERE key=? AND raw=? AND not expired`
   * returns value, no metadata updates ([arcovid19.readthedocs.io][2])

3. Otherwise it takes a **transactional slow path**:

   * optionally increments `Settings.hits` / `Settings.misses`
   * and if `update_column` is not None, it executes `UPDATE Cache SET <access_time or access_count update> WHERE rowid=?` ([arcovid19.readthedocs.io][2])

This matches the official policy description:

* **least-recently-stored**: “On access, no update is required” → reads can stay read-only. ([Grant Jenks][4])
* **least-recently-used / least-frequently-used**: access time/count is updated on every access → reads become writes. ([Grant Jenks][4])

**Important corollary:** even under LRS, if you enable `statistics`, `get()` can enter the slow path and perform `hits/misses` updates, so reads can still become writes. The docs warn about this in disk-full scenarios: reads only succeed “so long as they do not cause any write (as might occur if cache statistics are being recorded).” ([Grant Jenks][4])

---

# O.7 Culling/eviction and expiration (how deletions are chosen)

### O.7.1 Cull candidate selection

The eviction policy registry includes a cull query like:

* LRS: `ORDER BY store_time LIMIT ?`
* LRU: `ORDER BY access_time LIMIT ?`
* LFU: `ORDER BY access_count LIMIT ?` ([arcovid19.readthedocs.io][2])

That’s why the corresponding index exists: selecting eviction victims is an index-backed ordered scan.

### O.7.2 Expiration filtering is “in-query”

The `get` query includes:

* `(expire_time IS NULL OR expire_time > now)`
  so expired keys are ignored at read time, even if they haven’t been physically deleted yet. ([arcovid19.readthedocs.io][2])

---

# O.8 `check(fix=...)`: repair primitives and what they validate

DiskCache’s `check()` does several “DB + filesystem consistency” passes:

* runs `PRAGMA integrity_check`
* optionally runs `VACUUM` when `fix=True` ([arcovid19.readthedocs.io][2])
* checks that each `Cache.filename` exists on disk and has the expected `size`
* removes DB rows whose files are missing (when fixing)
* finds “unknown files” on disk not referenced by `Cache.filename` and removes them (when fixing)
* warns/removes empty directories (when fixing) ([arcovid19.readthedocs.io][2])
* recomputes and compares `Settings.count` vs `COUNT(Cache.key)` and `Settings.size` vs `SUM(Cache.size)`; fixes mismatches by updating `Settings` when `fix=True`. ([arcovid19.readthedocs.io][2])

This is the “ground truth validator” for the trigger-maintained metadata and file-backed value pointers.

---

# O.9 Practical takeaways (implementation → tuning knobs)

* If you want **read-heavy** performance with minimal write contention: prefer **LRS** and keep `statistics=False` so the fast path stays transaction-free. ([Grant Jenks][4])
* If you need LRU/LFU semantics, accept that “reads write” (updates on every access) and plan for higher write contention; FanoutCache exists largely to mitigate writer-writer blocking. ([Grant Jenks][4])
* If you use tags for invalidation at scale: enable `tag_index=True` up front so evictions are index-backed. ([Grant Jenks][4])
* If you’re debugging “mystery disk usage”: remember there are **three layers** (SQLite DB + WAL/SHM + `.val` files), and `.db-wal`/`.db-shm` lifecycles are governed by SQLite’s WAL behavior. ([SQLite][3])

If you want to keep following the outline, the next step is to convert this into the “**Implementation appendix**” style you used in `pyarrow-advanced.md`: a compact “DDL + index map + hot-path SQL” reference section plus a “performance consequences” checklist for each knob (`statistics`, `eviction_policy`, `tag_index`, `disk_min_file_size`, `sqlite_*`).

[1]: https://pypi.org/project/diskcache/ "diskcache · PyPI"
[2]: https://arcovid19.readthedocs.io/en/latest/_modules/diskcache/core.html "diskcache.core — arcovid19 0.6b documentation"
[3]: https://www.sqlite.org/tempfiles.html "Temporary Files Used By SQLite"
[4]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"

## Implementation appendix — DiskCache internals reference

This is a **compact, “DDL + index map + hot-path SQL”** reference, plus a **knob → performance consequences** checklist (aligned to DiskCache 5.x implementation and docs). ([Hugging Face][1])

---

# O.A Constants, defaults, and the “control plane” model

**Core constants (from `diskcache/core.py`)**

* DB file name: `DBNAME = 'cache.db'`
* Storage modes: `MODE_RAW=1`, `MODE_BINARY=2`, `MODE_TEXT=3`, `MODE_PICKLE=4`
* Default settings include: `statistics=0`, `tag_index=0`, `eviction_policy='least-recently-stored'`, `size_limit=2**30`, `cull_limit=10`, plus `sqlite_*` and `disk_*` defaults.
* Metadata keys in Settings: `count`, `size`, `hits`, `misses`. ([Hugging Face][1])

These defaults are also documented as programmatically accessible via `diskcache.DEFAULT_SETTINGS`. ([Grant Jenks][2])

---

# O.B DDL reference

## O.B1 `Settings` table

```sql
CREATE TABLE IF NOT EXISTS Settings (
  key TEXT NOT NULL UNIQUE,
  value
);
```

([Hugging Face][1])

## O.B2 `Cache` table

```sql
CREATE TABLE IF NOT EXISTS Cache (
  rowid INTEGER PRIMARY KEY,
  key BLOB,
  raw INTEGER,
  store_time REAL,
  expire_time REAL,
  access_time REAL,
  access_count INTEGER DEFAULT 0,
  tag BLOB,
  size INTEGER DEFAULT 0,
  mode INTEGER DEFAULT 0,
  filename TEXT,
  value BLOB
);
```

([Hugging Face][1])

**Column intent (operationally)**

* `(key, raw)` is the identity for lookups (unique index below).
* `expire_time` is TTL (lazy deletion).
* `store_time / access_time / access_count` are eviction-policy book-keeping.
* `mode/filename/value` encode where/how the value is stored (inline vs file). ([Hugging Face][1])

---

# O.C Index map (what exists, when, and why)

## Always-created indexes

1. **Key lookup**

```sql
CREATE UNIQUE INDEX IF NOT EXISTS Cache_key_raw ON Cache(key, raw);
```

([Hugging Face][1])

2. **Expiration scans**

```sql
CREATE INDEX IF NOT EXISTS Cache_expire_time ON Cache(expire_time);
```

([Hugging Face][1])

## Eviction-policy indexes (created depending on `eviction_policy`)

The policy registry specifies the “init” index and the “cull” query:

* **LRS** (`least-recently-stored`): index on `store_time`, no get-update
* **LRU** (`least-recently-used`): index on `access_time`, get-updates `access_time`
* **LFU** (`least-frequently-used`): index on `access_count`, get-updates `access_count` ([Hugging Face][1])

**Important footgun:** changing `eviction_policy` later does **not** drop previously created indexes; docs explicitly recommend setting it at initialization. ([Grant Jenks][3])

## Optional tag eviction index (`tag_index=True`)

```sql
CREATE INDEX IF NOT EXISTS Cache_tag_rowid ON Cache(tag, rowid);
```

Created by `create_tag_index()` and recommended to enable at init. ([Hugging Face][1])

---

# O.D Trigger map (incremental metadata maintenance)

DiskCache maintains two Settings values with triggers:

## `Settings.count` (row count)

```sql
CREATE TRIGGER IF NOT EXISTS Settings_count_insert
AFTER INSERT ON Cache FOR EACH ROW BEGIN
  UPDATE Settings SET value = value + 1 WHERE key = "count";
END;

CREATE TRIGGER IF NOT EXISTS Settings_count_delete
AFTER DELETE ON Cache FOR EACH ROW BEGIN
  UPDATE Settings SET value = value - 1 WHERE key = "count";
END;
```

([Hugging Face][1])

## `Settings.size` (sum of `Cache.size`)

```sql
CREATE TRIGGER IF NOT EXISTS Settings_size_insert
AFTER INSERT ON Cache FOR EACH ROW BEGIN
  UPDATE Settings SET value = value + NEW.size WHERE key = "size";
END;

CREATE TRIGGER IF NOT EXISTS Settings_size_update
AFTER UPDATE ON Cache FOR EACH ROW BEGIN
  UPDATE Settings SET value = value + NEW.size - OLD.size WHERE key = "size";
END;

CREATE TRIGGER IF NOT EXISTS Settings_size_delete
AFTER DELETE ON Cache FOR EACH ROW BEGIN
  UPDATE Settings SET value = value - OLD.size WHERE key = "size";
END;
```

([Hugging Face][1])

`hits/misses` are **not** trigger-maintained; they’re updated explicitly on the `get()` slow path when `statistics` is enabled. ([Hugging Face][1])

---

# O.E Hot-path SQL reference

Below are the “shape” queries that explain most performance behavior.

## O.E1 `get(key)` — fast path vs slow path

### Shared select (both paths)

```sql
SELECT rowid, expire_time, tag, mode, filename, value
FROM Cache
WHERE key = ? AND raw = ?
  AND (expire_time IS NULL OR expire_time > ?);
```

([Hugging Face][1])

### Fast path (“read doesn’t write”)

Condition:

* `statistics == 0` **and**
* `EVICTION_POLICY[eviction_policy]['get'] is None` (true for LRS and NONE)

Then DiskCache runs the select **without a transaction** and does **no updates**. ([Hugging Face][1])

### Slow path (transaction + optional writes)

If either statistics are enabled or the policy requires per-get updates, DiskCache enters a transaction and may execute:

**Hit/miss counters**

```sql
UPDATE Settings SET value = value + 1 WHERE key = "hits";
UPDATE Settings SET value = value + 1 WHERE key = "misses";
```

([Hugging Face][1])

**Policy “get update”**

```sql
UPDATE Cache SET <update_column> WHERE rowid = ?;
```

Where `<update_column>` is:

* `access_time = {now}` for LRU
* `access_count = access_count + 1` for LFU
* `None` for LRS / NONE ([Hugging Face][1])

This is the concrete mechanism behind the docs note that LRU/LFU “turn every access into a read and write.” ([Grant Jenks][3])

---

## O.E2 `set(key, value)` — update-or-insert + lazy cull

DiskCache intentionally prefers **UPDATE** on an existing row (even if expired) and only INSERT when absent:

* It explicitly avoids evicting expired keys during `get()` “to avoid writes during lookups,” so expired-but-hot keys commonly remain present, making UPDATE the common path. ([Hugging Face][1])

### Existence check (inside transaction)

```sql
SELECT rowid, filename
FROM Cache
WHERE key = ? AND raw = ?;
```

([Hugging Face][1])

### Update path

```sql
UPDATE Cache SET
  store_time = ?,
  expire_time = ?,
  access_time = ?,
  access_count = ?,
  tag = ?,
  size = ?,
  mode = ?,
  filename = ?,
  value = ?
WHERE rowid = ?;
```

(Also performs cleanup of the old filename after commit.) ([Hugging Face][1])

### Insert path

```sql
INSERT INTO Cache(
  key, raw, store_time, expire_time, access_time,
  access_count, tag, size, mode, filename, value
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
```

([Hugging Face][1])

### Lazy cull (`_cull`) invoked at end of `set/add` (bounded by `cull_limit`)

**Expire-first step**

```sql
SELECT filename FROM Cache
WHERE expire_time IS NOT NULL AND expire_time < ?
ORDER BY expire_time LIMIT ?;

DELETE FROM Cache WHERE rowid IN (
  SELECT rowid FROM Cache
  WHERE expire_time IS NOT NULL AND expire_time < ?
  ORDER BY expire_time LIMIT ?
);
```

([Hugging Face][1])

**Then policy eviction if `volume() >= size_limit`**
Victims are selected by the policy’s `cull` query and deleted by rowid-in-subquery (also limited). ([Hugging Face][1])

---

## O.E3 `expire(now)` — bulk expiry purge (iterative)

```sql
SELECT rowid, expire_time, filename
FROM Cache
WHERE ? < expire_time AND expire_time < ?
ORDER BY expire_time LIMIT ?;
```

Then delete selected rowids and cleanup files, repeating in batches. ([Hugging Face][1])

---

## O.E4 `evict(tag)` — tag-based bulk invalidation (iterative)

```sql
SELECT rowid, filename
FROM Cache
WHERE tag = ? AND rowid > ?
ORDER BY rowid LIMIT ?;
```

Then delete selected rowids and cleanup files, repeating in batches. ([Hugging Face][1])

This is why `Cache_tag_rowid (tag, rowid)` helps: it matches the predicate and cursor pattern. ([Hugging Face][1])

---

## O.E5 `cull()` — enforce `size_limit` “right now”

* First runs `expire(now)`.
* Then loops while `volume() > size_limit`, selecting 10 victims by policy order and deleting them in a transaction. ([Hugging Face][1])

---

# O.F File layout + value storage modes (`Disk`)

## O.F1 Physical files you’ll see

* `cache.db` (main SQLite file). ([Hugging Face][1])
* In WAL mode, SQLite uses `-wal` and `-shm` sidecars in the same directory; WAL requires permission to create those files. ([SQLite][4])

## O.F2 Value file naming (`.val` + 2-level dirs)

DiskCache generates a random hex name and stores file-backed values under two subdirectories:

* `hex_name = os.urandom(16) → 32 hex chars`
* subdir = `xx/yy/`
* filename = remaining 28 chars + `.val` ([Hugging Face][1])

## O.F3 Key/value encoding boundaries

**Keys (`Disk.put`)**

* Native: `bytes`, `str`, `float`, and 64-bit-range `int` are stored “raw”; others are pickled bytes. ([Hugging Face][1])

**Values (`Disk.store`)**

* `disk_min_file_size` is the threshold (`Disk.min_file_size`) controlling when strings/bytes/pickles become files. ([Hugging Face][1])
* Mode mapping is explicit: RAW / BINARY / TEXT / PICKLE, and `read=True` forces file-backed binary storage for stream ingest. ([Hugging Face][1])

---

# O.G Knob → performance consequences checklists

## 1) `statistics` (0/1)

**Changes in hot-path SQL**

* Off: enables `get()` fast path for LRS/NONE (no transaction, no writes). ([Hugging Face][1])
* On: `get()` uses slow path and executes `UPDATE Settings hits/misses ...` (writes on read). ([Hugging Face][1])

**Consequences**

* ✅ You can measure hit/miss precisely.
* ❌ Higher read latency and **more writer contention** (especially with many Django workers).
* ❌ Under “disk full” scenarios, reads that attempt stats updates can fail (docs call this out as a caveat). ([Grant Jenks][3])

**Guidance**

* Enable temporarily for benchmarking policy choices; keep off for latency-sensitive, read-heavy production. ([Grant Jenks][3])

---

## 2) `eviction_policy` (`least-recently-stored` / `least-recently-used` / `least-frequently-used` / `none`)

**Changes in index map**

* LRS: index on `store_time`.
* LRU: index on `access_time`.
* LFU: index on `access_count`. ([Hugging Face][1])

**Changes in get-path writes**

* LRS/NONE: policy `get` column is `None` → can be read-only (fast path) when stats off. ([Hugging Face][1])
* LRU: per-get `UPDATE Cache SET access_time = now WHERE rowid = ?`. ([Hugging Face][1])
* LFU: per-get `UPDATE Cache SET access_count = access_count + 1 WHERE rowid = ?`. ([Hugging Face][1])

**Consequences**

* ✅ LRU/LFU can improve hit-rate for “hot set” workloads.
* ❌ LRU/LFU **turn reads into writes**, amplifying SQLite single-writer contention. ([Grant Jenks][3])
* ❌ Changing the policy later **doesn’t drop old indexes**, so treat policy as an initialization-time choice. ([Grant Jenks][3])

**Guidance**

* High-concurrency, read-heavy → start with **LRS** (and stats off). ([Grant Jenks][3])
* Stable heavy-hitters → consider **LFU** but expect higher write pressure. ([Grant Jenks][3])
* Coordination primitives / persistent structures → **NONE** (avoid accidental eviction). ([Grant Jenks][3])

---

## 3) `tag_index` (0/1)

**Changes in index map**

* On: `CREATE INDEX Cache_tag_rowid ON Cache(tag, rowid)`; recommended at initialization. ([Hugging Face][1])

**Changes in eviction performance**

* `evict(tag)` is a cursoring query on `(tag, rowid)`; the index matches that shape and can massively reduce scan cost at scale. ([Hugging Face][1])

**Consequences**

* ✅ Much faster bulk invalidation by tag.
* ❌ Extra index maintenance on inserts/updates (standard SQLite tradeoff).

**Guidance**

* If you use tags for deploy busting / group invalidation at all, enable `tag_index=True` up front. ([Grant Jenks][3])

---

## 4) `disk_min_file_size` (bytes)

**What it changes**

* Moves the boundary between **inline SQLite blobs** vs **file-backed `.val` values** for bytes/strings/pickles. ([Hugging Face][1])

**Consequences**

* Lower threshold → more `.val` files (inode churn), smaller DB blobs.
* Higher threshold → fewer files, larger DB file and potentially more page churn.

**Guidance**

* Lots of medium-sized payloads (tens of KB): tune based on whether you’d rather pay filesystem overhead or SQLite blob/page overhead; measure with realistic concurrency. ([Hugging Face][1])

---

## 5) `sqlite_*` pragmas (journal/wal, synchronous, cache_size, mmap_size, auto_vacuum)

DiskCache stores these in Settings and re-applies them per connection; docs emphasize “measure before and after.” ([Grant Jenks][3])

### `sqlite_journal_mode = wal`

**Why it matters**

* WAL improves reader/writer concurrency, but creates `-wal` and `-shm` files and requires directory write permission to create them. ([SQLite][4])

**Operational hazard**

* WAL + shared storage (NFS) is a known footgun; SQLite discourages it and DiskCache calls this out as a caveat. ([Grant Jenks][3])

### `sqlite_synchronous = NORMAL (1)`

**Why it matters**

* SQLite documents that WAL + `synchronous=NORMAL` is consistent but loses some durability: commits might roll back after power loss/system crash (application crashes are still safe). ([SQLite][5])

**Guidance**

* Cache workloads usually accept this durability tradeoff; correctness-critical state generally should not. ([SQLite][5])

### `sqlite_cache_size` (pages)

**Why it matters**

* Page cache is per connection; changing it affects memory footprint and can improve DB read/write performance.
* Many SQLite environments treat it as session-scoped; DiskCache persists the setting and re-applies it. ([Stack Overflow][6])

### `sqlite_mmap_size` (bytes)

**Why it matters**

* SQLite’s mmap size applies **per open DB file**; total virtual address reservation can be `mmap_size × number_of_open_db_files`. ([SQLite][7])

### `sqlite_auto_vacuum = FULL (1)`

**Why it matters**

* FULL mode moves freelist pages to the end and truncates the DB file at each commit; this can reclaim disk but adds per-commit work and can worsen fragmentation. ([SQLite][8])

---

If you want the next step in “pyarrow-advanced” style: I can turn **this appendix into a single copy/paste block** for your `diskcache-advanced.md` (with a “knob decision matrix” and a “Django workers scaling” profile: *LRS + stats off + tag_index on + cautious mmap/cache_size*).

[1]: https://huggingface.co/koichi12/llm_tutorial/blob/c1012a522ebb6ce15f6fa9decfc35c1e1ccdaed7/.venv/lib/python3.11/site-packages/diskcache/core.py ".venv/lib/python3.11/site-packages/diskcache/core.py · koichi12/llm_tutorial at c1012a522ebb6ce15f6fa9decfc35c1e1ccdaed7"
[2]: https://grantjenks.com/docs/diskcache/api.html "DiskCache API Reference — DiskCache 5.6.1 documentation"
[3]: https://grantjenks.com/docs/diskcache/tutorial.html "DiskCache Tutorial — DiskCache 5.6.1 documentation"
[4]: https://sqlite.org/wal.html?utm_source=chatgpt.com "Write-Ahead Logging"
[5]: https://www.sqlite.org/pragma.html?utm_source=chatgpt.com "Pragma statements supported by SQLite"
[6]: https://stackoverflow.com/questions/28577577/does-sqlite-connection-needs-to-remain-openconnected-to-take-advantage-of-prag?utm_source=chatgpt.com "Does SQLite Connection needs to remain open ..."
[7]: https://www.sqlite.org/mmap.html?utm_source=chatgpt.com "Memory-Mapped I/O"
[8]: https://www3.sqlite.org/matrix/pragma.html?utm_source=chatgpt.com "Pragma statements supported by SQLite"
