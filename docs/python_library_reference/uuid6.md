## `uuid6` (Python) — comprehensive feature-category catalog (deep-dive outline)

> **Positioning:** `uuid6` is a small, focused library that generates the newer **time-ordered UUID formats** **v6/v7/v8** (and a v1→v6 converter) as described in **RFC 9562**, returning standard `uuid.UUID` objects. ([PyPI][1])

---

### 0) Scope, standards, and “why this library exists”

* **RFC alignment:** what “v6/v7/v8” mean, and which parts of RFC 9562 matter operationally (ordering, timestamp sources, entropy layout). ([PyPI][1])
* **Backport vs stdlib:** Python **3.14+** adds `uuid6()`, `uuid7()`, `uuid8()` in the standard library; this doc should include “when to use `uuid6` anyway” (older Python, pinned behavior, etc.). ([GitHub][2])
* **Versioning / release discipline:** the project’s versioning scheme shift (calendar → semver) and what to pin in production. ([PyPI][1])

---

### 1) Distribution, compatibility, and typing posture

* **Supported Python versions** / packaging metadata (minimum Python). ([PyPI][1])
* **Type-checking:** whether the package is typed (e.g., `py.typed`) and what’s actually annotated (helpful for agent-written code). ([Repowatch][3])
* **Runtime dependencies:** “pure-Python, no native deps” posture (implications for portability).

---

### 2) Public API surface (what you can call)

This section is a **complete API index** (names, signatures, return types, errors):

* `uuid6.uuid6(node=None, clock_seq=None)` ([PyPI][1])
* `uuid6.uuid7()` ([PyPI][1])
* `uuid6.uuid8()` ([PyPI][1])
* `uuid6.uuid1_to_uuid6(uuid1)` ([PyPI][1])

---

### 3) UUIDv6 deep dive (time-based, v1-compatible, DB-locality oriented)

* **Signature & parameters:** `node`, `clock_seq` semantics; sizing; defaults; privacy posture (node source). ([PyPI][1])
* **Bit/field layout** and how it differs from v1 (why it sorts better). ([PyPI][1])
* **Timestamp semantics:** what “time” means for v6 and how to extract it from the returned `uuid.UUID`. ([Python documentation][4])
* **Collision behavior & monotonicity model** (especially within the same timestamp). ([PyPI][1])

---

### 4) v1 → v6 conversion (`uuid1_to_uuid6`)

* **Exact transformation contract:** what is preserved, what is reordered, how to validate round-trips. ([PyPI][1])
* **Interoperability goal:** migrating legacy v1 primary keys to v6 while preserving time/node/seq information.

---

### 5) UUIDv7 deep dive (Unix-epoch ms + randomness; “default recommendation”)

* **Why v7 is the recommended default** (when you don’t need v1-compat). ([PyPI][1])
* **Bit layout & timestamp source** (Unix epoch milliseconds), and what ordering guarantees you actually get. ([PyPI][1])
* **Extracting time from `uuid.UUID`** (`UUID.time` behavior for v7). ([Python documentation][4])
* **Throughput behavior:** what happens under high ID generation rates (same-ms behavior).

---

### 6) UUIDv8 deep dive (vendor/experimental format; higher time granularity tradeoffs)

* **Why v8 exists** (RFC-compatible “custom” space) vs how this library chooses to implement it. ([PyPI][1])
* **Layout used here** (ms + sub-second fields + random tail) and the entropy vs granularity trade. ([PyPI][1])

---

### 7) Ordering, sorting, and database-key behavior (the “why you care” section)

* **Comparison semantics:** `uuid.UUID` ordering is by integer value (`UUID.int`), so “time-ordered UUIDs” become practically sortable keys. ([Python documentation][4])
* **Binary vs text storage tradeoffs** in common databases (index size, collation pitfalls, byte-order expectations).
* **Migration patterns:** replacing `uuid4()` PKs with v7 ensuring minimal index churn.

---

### 8) Concurrency, safety, and determinism boundaries

* **Thread-safety:** these generators are documented as **not thread-safe**; you’ll want a “safe wrapper” pattern (lock, per-thread generator state, etc.). ([PyPI][1])
* **Multi-process behavior:** what you can assume (and what you can’t) when many workers generate IDs concurrently.
* **Clock behavior:** monotonic counters, clock rollback scenarios, and what that means for ordering and uniqueness.

---

### 9) Interop with Python’s `uuid` ecosystem (stdlib + third parties)

* **Using returned objects everywhere `uuid.UUID` is accepted** (pydantic, ORMs, serializers).
* **Field access & decoding:** `UUID.time`, `UUID.clock_seq`, etc., and version-specific meaning (v6/v7 differences). ([Python documentation][4])
* **Stdlib parity / migration guide:** when on Python 3.14+ you may prefer the built-in `uuid` functions and how to switch cleanly. ([Python documentation][4])

---

### 10) Testing, validation, and conformance harness

* **Spec-conformance checks:** correct version/variant bits, ordering properties, boundary conditions. ([Python documentation][4])
* **Cross-language compatibility:** verifying IDs match other RFC 9562 implementations (important for distributed systems).

---

### 11) Operational footguns and “production checklist”

* When **not** to use time-ordered UUIDs (privacy/security constraints, correlation concerns).
* When **not** to use v6 (no legacy v1; prefer v7). ([PyPI][1])
* Safe defaults, seeding/entropy considerations, and logging/observability practices.

---


[1]: https://pypi.org/project/uuid6/ "uuid6 · PyPI"
[2]: https://github.com/oittaa/uuid6-python "GitHub - oittaa/uuid6-python: New time-based UUID formats which are suited for use as a database key"
[3]: https://repowatch.ulni.us-ashburn-1.oci.oraclecloud.com/repowatch/oraclelinux8.x86_64/ol8_automation2.3/5b00efc7a48e637d95241980557bf09867656aa69332e50335f4cd58055fce78?display=filelist "[ol8_automation2.3] python3.11-uuid6"
[4]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"

## 0) Scope, standards, and “why this library exists”

### 0.1 What `uuid6` is (and is not)

`uuid6` exists to generate **time-ordered RFC 9562 UUIDs** — **v6/v7/v8** — as **standard `uuid.UUID`** instances, with an explicit emphasis on **database-key locality** and **sort-by-time behavior** (e.g., the project’s own usage examples assert monotone ordering under sequential calls). ([PyPI][1])

It is **not** trying to be:

* a general UUID framework (it only targets v6/v7/v8 + a v1→v6 converter),
* a “secure token” generator (time-ordered formats leak timing; v8 can embed application data; use appropriate threat modeling),
* a replacement for all of `uuid` stdlib (especially now that Python 3.14+ ships these generators). ([PyPI][1])

---

### 0.2 The standards baseline: RFC 9562 supersedes RFC 4122

**RFC 9562 (May 2024)** is the current IETF spec for UUIDs and **obsoletes RFC 4122**, adding (among other things) versions **6, 7, and 8** and best-practice guidance. ([RFC Editor][2])

Operationally relevant RFC 9562 “shape facts” for implementers/agents:

* **UUIDs are 128-bit (16-octet) values**; **variant bits + version bits** define the layout. ([RFC Editor][2])
* **Bit numbering** is defined and fields are assumed **network byte order** (MSB-first) absent other protocol rules. This matters for **binary storage**, **lexicographic sort**, and “what does ‘time-ordered’ even mean” across languages and databases. ([RFC Editor][2])
* v6/v7/v8 are **variant-1 (RFC-style) UUIDs** with fixed version/variant bit placements; the rest of the bit budget is algorithm-defined (v6/v7) or application-defined (v8). ([RFC Editor][2])

---

### 0.3 RFC alignment: what v6 / v7 / v8 *mean* (the parts that matter in production)

#### UUIDv6 (time-ordered *v1-compatible* layout)

RFC 9562 defines v6 as **field-compatible with UUIDv1**, but **reordered for improved DB locality**; it’s expected mostly for ecosystems already holding **legacy v1** values. Concretely:

* v6 reorders the **same 60-bit timestamp** used by v1 so that timestamp bytes go **most-significant → least-significant** in the leading bytes (improving index locality). ([RFC Editor][2])
* The **clock sequence** and **node** bits keep the v1 positions (and thus share many of v1’s operational/privacy considerations). ([RFC Editor][2])
* RFC guidance: systems without legacy v1 “**SHOULD use UUIDv7 instead**.” ([RFC Editor][2])

**Agent takeaway:** choose v6 when you need *layout continuity* with v1 (migration, mixed-keyspace sorting, forensic extraction of v1-like fields) and accept the associated node/sequence semantics. ([RFC Editor][2])

#### UUIDv7 (Unix-epoch ms + randomness; “default recommendation”)

RFC 9562 positions v7 as the **recommended general-purpose time-ordered UUID**:

* v7’s time source is a **48-bit big-endian Unix epoch timestamp in milliseconds** (ms since 1970-01-01 UTC, leap seconds excluded) in the **most significant 48 bits**. ([RFC Editor][2])
* The remaining **74 bits** (excluding version/variant bits) are used for **randomness** and optionally for monotonicity constructs (sub-ms fraction and/or counter) to improve ordering within the same millisecond. ([RFC Editor][2])
* RFC guidance: implementations **SHOULD utilize v7 instead of v1/v6 if possible** (better entropy characteristics, widely understood time base). ([RFC Editor][2])

The `uuid6` project mirrors this stance in its “Which UUID version should I use?” guidance. ([PyPI][1])

**Agent takeaway:** v7 is your default for “sortable IDs” unless you have a v1-compat reason (v6) or a tightly specified custom payload (v8). ([RFC Editor][2])

#### UUIDv8 (explicitly *custom*/vendor space)

RFC 9562 makes v8 deliberately minimal:

* v8 is for **experimental/vendor-specific** layouts.
* Only requirement: **variant + version bits MUST be set** correctly; **uniqueness is implementation-specific and MUST NOT be assumed**.
* v8 leaves **122 bits** for application-defined meaning, and it is *not* a replacement for v4’s “all-random payload” model. ([RFC Editor][2])

`uuid6` **chooses a specific v8 algorithm** (the project describes v8 as an option when you “require greater granularity than v7” and mentions a nanosecond-precision timestamp usage), which is **policy beyond RFC 9562** (RFC defines the bit budget, not your semantics). ([GitHub][3])

**Agent takeaway:** treat v8 IDs as “format contract required.” If you don’t control all producers/consumers, v8 is an interoperability hazard by default. ([RFC Editor][2])

---

### 0.4 Ordering semantics: what “time-ordered” means for Python consumers

In Python, `uuid6` is intentionally framed around “DB-key friendliness” and “sortable generation.” The project examples literally assert:

* `uuid6.uuid6()` calls are comparable (`<`) and expected to increase across sequential calls
* same for `uuid6.uuid7()` and `uuid6.uuid8()` ([PyPI][1])

**Agent note:** those comparisons are only meaningful because UUIDs have a stable total order in Python (by 128-bit integer value). “Time-ordered UUID” is therefore equivalent to “the **lexicographically / integer-sorted** order matches creation time order (modulo clock resolution + concurrency).” (The RFC also calls out sorting as a best-practice topic.) ([RFC Editor][2])

---

### 0.5 Backport vs stdlib: Python 3.14+ makes this “optional,” not “obsolete”

Python **3.14**’s standard library `uuid` module explicitly moved to RFC 9562 and **added UUID versions 6, 7, and 8** (and even the `python -m uuid` CLI accepts `-u uuid6|uuid7|uuid8`). ([Python documentation][4])

The `uuid6` project itself highlights this as a NOTICE: Python 3.14 added `uuid6()`, `uuid7()`, `uuid8()` in stdlib. ([GitHub][3])

**When to use `uuid6` anyway (agent decision rules):**

1. **You are on Python < 3.14** and want RFC 9562 v6/v7/v8 generators. (Primary backport use case.) ([GitHub][3])
2. **You want a single import path** across mixed runtimes (prod on 3.11/3.12, dev on 3.14), without branching your code. ([PyPI][1])
3. **You want pinned third-party behavior** (e.g., you’re explicitly depending on this project’s chosen v8 algorithm/semantics, which is not something RFC 9562 standardizes). ([GitHub][3])

**Minimal compatibility shim (agent-ready):**

```python
try:
    # Python 3.14+
    from uuid import uuid6 as uuid6_stdlib, uuid7 as uuid7_stdlib, uuid8 as uuid8_stdlib
    uuid6 = uuid6_stdlib
    uuid7 = uuid7_stdlib
    uuid8 = uuid8_stdlib
except Exception:
    import uuid6 as _uuid6
    uuid6 = _uuid6.uuid6
    uuid7 = _uuid6.uuid7
    uuid8 = _uuid6.uuid8
```

(If you do this, document **v8 semantics** explicitly; stdlib v8 is “custom bits,” while `uuid6`’s v8 is an opinionated generator.) ([RFC Editor][2])

---

### 0.6 Versioning / release discipline (calendar → semver)

Starting at **`2025.0.0`**, the project **transitioned from calendar versioning** (`YYYY.MM.DD`) to **semantic versioning** (`MAJOR.MINOR.PATCH`) and documents how to pin accordingly. ([PyPI][1])

Key policy points you can encode into agent tooling:

* **Pre-2025.0.0** tags are CalVer-like (e.g., `2024.07.10`).
* **2025.0.0+** uses SemVer semantics (MAJOR=breaking, MINOR=features, PATCH=fixes).
* They explicitly claim constraints like `>=2024.07.10` should continue to work, and recommend `>=2025.0.0` for “latest features.” ([PyPI][1])

**Practical pin guidance (Python packaging idioms):**

* If you want “any bugfix/feature within 2025 major”: `uuid6~=2025.0` (allows `2025.x.y`, blocks `2026.0.0`).
* If you want “no surprises”: pin exact patch `uuid6==2025.0.1` (current PyPI line as of July 2025). ([PyPI][1])

---

## 1) Distribution, compatibility, and typing posture

### 1.1 Supported Python versions / packaging metadata

On PyPI, `uuid6` declares:

* **Requires: Python >= 3.9**
* classifiers for Python **3.9–3.13**
* **OS Independent** (pure-Python distribution posture). ([PyPI][1])

This cleanly matches the library’s role as a **small, pure-python generator** that returns stdlib `uuid.UUID` objects and should therefore behave consistently across platforms. ([PyPI][1])

---

### 1.2 Type-checking posture (PEP 561 / `py.typed`)

For agent-written codebases, “is this library typed?” is a real operational question because it controls whether your type checker will trust inline annotations.

**What to look for:** A `py.typed` marker file at the top-level package.

* PEP 561: package maintainers who want to support type checking **MUST add a marker file named `py.typed`**, which applies recursively to subpackages. ([Python Enhancement Proposals (PEPs)][5])
* Independent packaging evidence: the `uuid6` distribution is shipped with `uuid6/py.typed` alongside `uuid6/__init__.py`. ([Repowatch][6])

**Agent implication:** treat `uuid6` as **PEP 561 typed**; your tooling (mypy/pyright) should consume its inline types without external `types-*` stub packages. ([Python Enhancement Proposals (PEPs)][5])

---

### 1.3 Runtime dependencies / portability posture

From a deployment perspective, the library is positioned as:

* **OS independent** (pure-Python wheel) ([PyPI][1])
* conceptually a thin extension over stdlib UUID primitives (“extends immutable UUID objects … with `uuid6()`, `uuid7()`, `uuid8()`”) ([PyPI][1])

**Agent note (dependency hygiene):**

* Expect “stdlib-only” runtime behavior; treat any extras as dev/test-only (benchmarks, linting) rather than production dependencies.
* For strict verification in CI, agents can enforce this by checking installed metadata (`pip show uuid6` / reading `Requires-Dist`) in the build pipeline.

---

If you want, next we can proceed directly into **Section 2 (Public API surface)** in the same style, including **exact signatures, behavior contracts, monotonicity + concurrency caveats, and a “stdlib parity shim” recipe** for Python 3.9–3.14+.

[1]: https://pypi.org/project/uuid6/ "uuid6 · PyPI"
[2]: https://www.rfc-editor.org/rfc/rfc9562.html "RFC 9562: Universally Unique IDentifiers (UUIDs)"
[3]: https://github.com/oittaa/uuid6-python "https://github.com/oittaa/uuid6-python"
[4]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"
[5]: https://peps.python.org/pep-0561/ "https://peps.python.org/pep-0561/"
[6]: https://repowatch.ulni.us-ashburn-1.oci.oraclecloud.com/repowatch/oraclelinux9.x86_64/ol9_automation2.3/d5915d9b17370504dd5c4d57f598b5e0bddb300b9b639b486656f439101b1ba6?display=filelist "https://repowatch.ulni.us-ashburn-1.oci.oraclecloud.com/repowatch/oraclelinux9.x86_64/ol9_automation2.3/d5915d9b17370504dd5c4d57f598b5e0bddb300b9b639b486656f439101b1ba6?display=filelist"

## 2) Public API surface (`uuid6` package)

### 2.1 Namespace + import patterns (avoid self-footguns)

The distribution installs a top-level module named `uuid6`. The canonical usage is:

```python
import uuid6
u = uuid6.uuid7()
```

…and the project’s own examples rely on ordering comparisons like `assert my_uuid < uuid6.uuid7()` / `uuid6.uuid8()` to demonstrate “DB-key friendly” monotone-ish generation behavior in a single-threaded sequence. ([PyPI][1])

**Name-shadowing hazard (common in agent-written code):**

* `from uuid6 import uuid6` shadows the module name with the function name.
* Prefer either `import uuid6 as uuid6_pkg` or `from uuid6 import uuid7 as uuid7_fn` style imports.

---

### 2.2 API index (exported callable surface)

`uuid6` is intentionally small: four public call sites.

#### `uuid6.uuid6(node=None, clock_seq=None) -> uuid.UUID`

* “Generate a UUID from a host ID, sequence number, and the current time.”
* Defaults: **random 48-bit node**, **random 14-bit clock sequence**.
* **Not thread-safe**. ([PyPI][1])

#### `uuid6.uuid7() -> uuid.UUID`

* “Generate a UUID from a random number, and the current time.”
* Ordering: `unix_ts_ms` field is documented to guarantee ordering within the same millisecond via a monotonic increment mechanism.
* **Not thread-safe**. ([PyPI][1])

#### `uuid6.uuid8() -> uuid.UUID`

* “Generate a UUID from a random number, and the current time.”
* This package’s v8 is an **opinionated time-derived layout** (ms timestamp + subsecond fields + random tail), intended to trade entropy for greater time granularity vs v7.
* **Not thread-safe**. ([PyPI][1])

#### `uuid6.uuid1_to_uuid6(uuid1) -> uuid.UUID`

* “Generate a UUID version 6 object from a UUID version 1 object.”
* Deterministic transform (no randomness implied); see example in docs. ([PyPI][1])

**Return type contract:** the package “extends immutable UUID objects (the UUID class) …” and examples interoperate directly with `uuid.UUID(...)`, so treat returns as standard `uuid.UUID` instances. ([PyPI][1])

---

### 2.3 `uuid6.uuid6(node=None, clock_seq=None)` — behavior contract (v6 generator)

#### Signature + parameter semantics

* `node`: expected to fit **48 bits** (host identifier). If omitted, **this package chooses a random 48-bit node**. ([PyPI][1])
* `clock_seq`: expected to fit **14 bits**. If omitted, **random 14-bit** is chosen. ([PyPI][1])
* **Thread-safety:** explicitly “not thread-safe.” ([PyPI][1])

> **Notable divergence vs stdlib default privacy posture:** Python 3.14’s stdlib `uuid.uuid6(node=None, clock_seq=None)` defaults `node` to `uuid.getnode()` (MAC-derived when possible), while this package defaults to a random 48-bit node. ([Python documentation][2])

#### Ordering / monotonicity model (single-threaded assumption)

The package documents a v6 field layout and states:

* `time_high`, `time_mid`, `time_low` “guarantee the order of UUIDs generated within the same timestamp by monotonically incrementing the timer.” ([PyPI][1])

**Agent inference (safe):** this implies **mutable process-local state** used to ensure monotonicity across calls when time resolution is insufficient (i.e., you can’t assume “system time strictly increases per call”). Because the function is **not thread-safe**, that state is not protected from races. ([PyPI][1])

#### Field extraction (stdlib `uuid.UUID` semantics)

In Python’s `uuid.UUID`, the `.time` attribute for versions **1 and 6** is a 60-bit timestamp in **100-ns intervals since the Gregorian epoch (1582-10-15)**. ([Python documentation][2])

So your downstream logic can safely do:

```python
u = uuid6.uuid6()
assert u.version == 6
t_100ns = u.time
```

…but note: that timestamp basis is **not** Unix epoch.

---

### 2.4 `uuid6.uuid7()` — behavior contract (v7 generator)

#### Signature + documented ordering guarantee

* Signature: `uuid6.uuid7()` (no args). ([PyPI][1])
* Ordering: “`unix_ts_ms` field guarantees the order of UUIDs generated within the same millisecond by monotonically incrementing the timer.” ([PyPI][1])
* **Not thread-safe.** ([PyPI][1])

#### Timestamp semantics in Python

`uuid.UUID.time` for **version 7** is a **48-bit timestamp in milliseconds since Unix epoch (1970-01-01)**. ([Python documentation][2])

```python
u = uuid6.uuid7()
assert u.version == 7
unix_ms = u.time
```

#### Contrast: stdlib v7 monotonic strategy (useful for parity decisions)

Python 3.14’s `uuid.uuid7()` explicitly states it embeds a 48-bit timestamp and uses a **42-bit counter to guarantee monotonicity within a millisecond** for portability across platforms lacking sub-millisecond precision. ([Python documentation][2])

So, even if both “sort by creation time,” the *mechanism* and concurrency posture may differ; `uuid6` package additionally flags lack of thread-safety.

---

### 2.5 `uuid6.uuid8()` — behavior contract (this package’s opinionated v8)

#### Signature + high-level behavior

* Signature: `uuid6.uuid8()` (no args). ([PyPI][1])
* “Generate a UUID from a random number, and the current time.” ([PyPI][1])
* **Not thread-safe.** ([PyPI][1])

#### Layout / semantics (time-derived v8)

This implementation documents a v8 layout that includes:

* `unix_ts_ms` (timestamp ms),
* `subsec_a`, `subsec_b` (sub-ms / sub-second fields),
* `rand` (random tail),
  and states it “sacrifices some entropy for granularity compared to `uuid7()`.” ([PyPI][1])

> **Important:** this is *not* the same contract as Python’s stdlib `uuid.uuid8(...)` (next section). Treat v8 as “format agreement required.”

---

### 2.6 `uuid6.uuid1_to_uuid6(uuid1)` — behavior contract (deterministic transform)

* The docs define it as converting “a UUID version 1 object” to “a UUID version 6 object,” and show an explicit example using `uuid.UUID(hex=...)`. ([PyPI][1])

**Precondition:** caller should enforce `u.version == 1` before conversion (don’t feed arbitrary UUIDs and hope). The contract is described specifically as v1→v6. ([PyPI][1])

---

### 2.7 Monotonicity + concurrency caveats (agent-ready operational rules)

#### The explicit caveat

All three generators (`uuid6`, `uuid7`, `uuid8`) are documented as **not thread-safe**. ([PyPI][1])

#### What “not thread-safe” *means* operationally

You should assume:

* internal mutable state is used to maintain monotonicity within a timestamp / millisecond (explicitly stated for v6 and v7 fields), and
* concurrent calls can race that state → breaking monotonic ordering guarantees and (in worst cases) increasing collision risk. ([PyPI][1])

#### Minimal hardening pattern (single-process, multithreaded)

If you need “one generator shared across threads,” add an external lock:

```python
import threading
import uuid6

_lock = threading.Lock()

def uuid7_threadsafe():
    with _lock:
        return uuid6.uuid7()
```

If you need *very high throughput*, prefer per-worker generation (process-local) and avoid shared global state, or move to stdlib v7 (Python 3.14+) where monotonicity strategy is documented (42-bit counter). ([Python documentation][2])

---

## 2.8 “Stdlib parity shim” recipes (Python 3.9–3.14+)

Python 3.14 adds:

* `uuid.uuid6(node=None, clock_seq=None)`
* `uuid.uuid7()`
* `uuid.uuid8(a=None, b=None, c=None)` ([Python documentation][2])

…but there are two key parity pitfalls:

1. **Default `node` behavior differs for v6**

   * stdlib defaults to `getnode()` (MAC-derived when possible). ([Python documentation][2])
   * `uuid6` package defaults to random 48-bit node. ([PyPI][1])

2. **v8 semantics differ dramatically**

   * stdlib v8 is “pseudo-random UUID with customizable blocks (a,b,c)” and warns the default is not CSPRNG. ([Python documentation][2])
   * `uuid6` package v8 is time-derived (ms + subsec + random tail). ([PyPI][1])

### Shim A — “prefer stdlib semantics” (recommended if you want RFC9562 stdlib behavior)

* Use stdlib when available; use `uuid6` package only as a backport for v6/v7 and for v1→v6 conversion.
* Treat v8 as **stdlib-only** (or explicitly split into `uuid8_rfc()` vs `uuid8_time()`).

```python
# compat_uuid.py
from __future__ import annotations

import secrets
import uuid
from typing import Optional

try:
    import uuid6 as uuid6_pkg  # pip install uuid6
except Exception:  # pragma: no cover
    uuid6_pkg = None

_HAS_314 = hasattr(uuid, "uuid6") and hasattr(uuid, "uuid7") and hasattr(uuid, "uuid8")

def uuid6_time(node: Optional[int] = None, clock_seq: Optional[int] = None) -> uuid.UUID:
    # Optional: match uuid6-package privacy posture by using a random node when omitted.
    if node is None:
        node = secrets.randbits(48)
    if hasattr(uuid, "uuid6"):
        return uuid.uuid6(node=node, clock_seq=clock_seq)
    if uuid6_pkg is None:
        raise RuntimeError("Need Python 3.14+ or `pip install uuid6`.")
    return uuid6_pkg.uuid6(node=node, clock_seq=clock_seq)

def uuid7_time() -> uuid.UUID:
    if hasattr(uuid, "uuid7"):
        return uuid.uuid7()
    if uuid6_pkg is None:
        raise RuntimeError("Need Python 3.14+ or `pip install uuid6`.")
    return uuid6_pkg.uuid7()

def uuid8_rfc(a: Optional[int] = None, b: Optional[int] = None, c: Optional[int] = None) -> uuid.UUID:
    # RFC9562 stdlib v8 is only present in 3.14+.
    if not hasattr(uuid, "uuid8"):
        raise RuntimeError("uuid8_rfc requires Python 3.14+ stdlib uuid.uuid8().")
    # Supply CSPRNG blocks if caller didn't specify them.
    if a is None: a = secrets.randbits(48)
    if b is None: b = secrets.randbits(12)
    if c is None: c = secrets.randbits(62)
    return uuid.uuid8(a, b, c)

def uuid1_to_uuid6(u: uuid.UUID) -> uuid.UUID:
    if uuid6_pkg is None:
        raise RuntimeError("uuid1_to_uuid6 requires `pip install uuid6` (no stdlib helper).")
    return uuid6_pkg.uuid1_to_uuid6(u)
```

### Shim B — “preserve uuid6-package v8 behavior across all Pythons”

If you have existing data where v8 is time-derived **as implemented by this package**, keep it stable:

```python
import uuid
import uuid6 as uuid6_pkg

def uuid8_time() -> uuid.UUID:
    return uuid6_pkg.uuid8()  # time-derived v8 implementation
```

…and **do not** silently swap to stdlib v8 on Python 3.14+, because stdlib v8 is a different construct (custom blocks; not time-derived). ([Python documentation][2])

[1]: https://pypi.org/project/uuid6/ "uuid6 · PyPI"
[2]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"

## 3) UUIDv6 deep dive (time-based, v1-compatible, DB-locality oriented)

### 3.1 What UUIDv6 *is* (RFC contract + intended use)

**UUIDv6** is *field-compatible* with **UUIDv1** (same conceptual ingredients: **60-bit Gregorian timestamp**, **14-bit clock sequence**, **48-bit node**), but **reorders the timestamp bits** so that the *most significant* timestamp bytes appear first. The RFC frames v6 as primarily for environments with **existing v1 UUIDs**, and recommends **v7 instead** when you don’t need v1 continuity. ([RFC Editor][1])

Operationally: v6 exists so you can keep the “v1-style semantics” (clock_seq / node / Gregorian epoch time), but make the resulting 128-bit value **index-friendly** and **lexicographically sortable** by creation time without parsing. ([RFC Editor][1])

---

### 3.2 Generator API: signature, sizing, defaults, privacy posture

#### 3.2.1 Signature (uuid6 package)

```python
uuid6.uuid6(node: int | None = None, clock_seq: int | None = None) -> uuid.UUID
```

* `node`: **48-bit** value (bits 80–127) intended to be “spatially unique.” ([RFC Editor][1])
* `clock_seq`: **14-bit** sequence value (bits 66–79). ([RFC Editor][1])
* `timestamp`: implied from “current time” (the 60-bit v1/v6 timestamp domain). ([RFC Editor][1])

#### 3.2.2 Defaults + thread-safety (uuid6 package policy)

The `uuid6` *third-party* package explicitly documents:

* If `node` is not provided: it chooses a **random 48-bit** value.
* If `clock_seq` is not provided: it chooses a **random 14-bit** value.
* **Not thread-safe.** ([PyPI][2])

That default posture aligns with RFC 9562 guidance for v6: the RFC states that the **clock sequence and node bits SHOULD be reset to a pseudorandom value for each new UUIDv6** (though implementations *may* retain “old MAC + stable clock_seq” v1 behavior). ([RFC Editor][1])

#### 3.2.3 Privacy: why “random node” matters

`uuid.uuid1()` is explicitly called out in Python docs as potentially compromising privacy because it embeds the machine’s network address. UUIDv6 *can* embed MAC-like node data if an implementation chooses, but `uuid6` (package) defaults to random node, which is the “privacy-friendlier” posture. ([Python documentation][3])

---

### 3.3 Bit / field layout: v6 vs v1 (and why v6 sorts better)

#### 3.3.1 UUIDv1 layout (timestamp is *not* lexicographically ordered)

RFC v1 layout places **time_low** (the *least significant* 32 bits of the 60-bit timestamp) first, then `time_mid`, then `time_high` (12 bits) near the version nibble. ([RFC Editor][1])

Consequence: sorting v1 UUIDs as raw bytes / strings does **not** correspond to chronological order (because the low timestamp bits dominate the prefix).

#### 3.3.2 UUIDv6 layout (timestamp bytes stored MSB→LSB)

RFC v6 changes the timestamp encoding:

* store the **first 48 most significant** bits of the timestamp first,
* then the **version (6)** bits,
* then the remaining **12 least significant** timestamp bits,
* with `clock_seq` and `node` in the same positions as v1. ([RFC Editor][1])

Consequence: **lexicographic order ≈ time order** (given monotonic behavior within a tick). RFC explicitly says v6/v7 are designed so that systems can sort “as opaque raw bytes” and that textual representations are intended to be lexicographically sortable; it also calls out DB locality benefits (often large). ([RFC Editor][1])

#### 3.3.3 “DB locality” nuance (what you get and what you don’t)

* You get clustered inserts because new IDs fall near each other in index order. ([RFC Editor][1])
* You do **not** get a perfect per-process counter unless your generator enforces monotonicity when the clock granularity is too coarse (next sections).

---

### 3.4 Timestamp semantics in Python: what `.time` means for v6 and how to extract it

#### 3.4.1 UUIDv6 time domain (Gregorian epoch, 100ns ticks)

In Python, `uuid.UUID.time` is:

* for **versions 1 and 6**: the **60-bit timestamp** as a count of **100-nanosecond intervals since 1582-10-15 00:00:00 (Gregorian epoch)**. ([Python documentation][3])

So for `uuid6.uuid6()` results:

```python
import uuid6
from datetime import datetime, timedelta, timezone

GREGORIAN_EPOCH = datetime(1582, 10, 15, tzinfo=timezone.utc)

u = uuid6.uuid6()
assert u.version == 6

t_100ns = u.time  # 60-bit count of 100ns intervals since 1582-10-15
dt = GREGORIAN_EPOCH + timedelta(microseconds=t_100ns // 10)  # 100ns -> µs
sub_us_100ns = t_100ns % 10  # residual 100ns ticks within the µs
```

Notes:

* `timedelta` is microsecond-resolution, so you must carry the remainder (`t_100ns % 10`) separately if you care about sub-microsecond fidelity.
* This is *not* Unix time; v6 shares the v1 Gregorian epoch domain. ([Python documentation][3])

#### 3.4.2 Extracting `clock_seq` and `node` robustly

Python explicitly documents `UUID.clock_seq` as relevant for v1 and v6. ([Python documentation][3])

`UUID.node` is historically described as “only relevant to version 1” in the Python docs snippet, so if you want a version-agnostic extraction for v6, use bit masking of the final 48 bits:

```python
node_48 = u.int & ((1 << 48) - 1)
clock_seq_14 = u.clock_seq
```

(You’re extracting the RFC-defined field placement, independent of any Python doc phrasing.) ([RFC Editor][1])

---

### 3.5 Collision behavior + monotonicity model (esp. “same timestamp”)

#### 3.5.1 What has to repeat for a collision?

A UUIDv6 collision occurs only if **all three** repeat:

1. the **60-bit timestamp**
2. the **14-bit clock_seq**
3. the **48-bit node** ([RFC Editor][1])

So your collision story is about how your implementation sources / mutates `(timestamp, clock_seq, node)` across:

* high-rate calls within one clock tick,
* clock rollback / reboot,
* multi-process and distributed generation,
* and whether node/clock_seq are stable vs randomized.

#### 3.5.2 RFC requirements around clock rollback and node changes

RFC 9562 is explicit for the v1/v6 family:

* If the clock is set backwards (or might have been), and you can’t prove you didn’t already generate larger timestamps, **the clock sequence MUST change** (increment if known; otherwise set to random/high-quality pseudorandom). ([RFC Editor][1])
* If the node ID changes, setting clock_seq randomly minimizes duplicates across hosts with slightly different clocks. ([RFC Editor][1])
* The clock sequence **must be initially randomized** (not correlated with node). ([RFC Editor][1])

These rules exist because (timestamp-only) is insufficient if time isn’t strictly monotone or if identity moves.

#### 3.5.3 RFC guidance for v6 specifically: randomize node + clock_seq per UUID

For v6 the RFC goes further: **clock sequence and node bits SHOULD be reset to a pseudorandom value for each new UUIDv6 generated**, though v1-style behavior is permitted. ([RFC Editor][1])

The `uuid6` package follows this spirit by defaulting both `node` and `clock_seq` to fresh random values when not provided. ([PyPI][2])

**Practical implication:** if you accept per-UUID random node/seq, collisions become dominated by random collision probability rather than “stable identity + stateful clock_seq” correctness.

#### 3.5.4 Random node correctness: the “multicast bit” rule

If you generate a random 48-bit node ID, RFC requires setting **the least significant bit of the first octet** to 1 (the unicast/multicast bit) so random-node UUIDs cannot conflict with IEEE 802 MAC-derived node IDs. ([RFC Editor][1])

Agents generating their own `node` should implement this:

```python
import secrets

node = secrets.randbits(48)
node |= 1 << 40  # set LSB of first octet (bit 40 of the 48-bit node)
```

#### 3.5.5 “Same timestamp” monotonicity: what uuid6 (package) claims

The `uuid6` package explicitly states that the v6 timestamp fields “guarantee the order of UUIDs generated within the same timestamp by **monotonically incrementing the timer**,” and also flags the generator as **not thread-safe**. ([GitHub][4])

Interpretation (agent mental model):

* The implementation is almost certainly tracking “last timestamp used” in process memory and doing something like:

  * `ts = max(now_ts, last_ts + 1)` when time resolution stalls.
* This ensures that even if the system clock doesn’t advance at 100ns granularity, successive calls get strictly increasing timestamp bits, which preserves sort order and avoids duplicates in a single-threaded stream.

**Concurrency caveat:** because it’s “not thread-safe,” do not assume these monotonicity guarantees hold under concurrent calls. ([PyPI][2])

#### 3.5.6 Thread-safe monotone wrapper (minimal “make guarantees true” adapter)

If you need a *strong* per-process invariant (“each call returns a UUID whose `.int` is greater than the previous”), serialize generation:

```python
import threading
import uuid6

_lock = threading.Lock()

def uuid6_monotone() -> "uuid.UUID":
    with _lock:
        return uuid6.uuid6()
```

This wrapper is the simplest way to make the package’s stated “monotonically incrementing timer” property apply to your program, rather than to a single thread.

---

### 3.6 “When should an agent choose to pass `node` / `clock_seq` explicitly?”

Use explicit parameters when you need a *repeatable provenance* or a *sharded namespace*:

* **Stable per-process node, random per-process clock_seq:** improves intra-process correlation and makes it easier to reason about uniqueness, but leaks an identifier unless you treat it as opaque.
* **Random per-process node (multicast bit set), stable clock_seq:** can be okay if you also guarantee monotonic timestamp and never reuse the same node across processes.
* **Fully default (uuid6 package):** best privacy posture, simplest operationally, but ordering guarantees rely on non-thread-safe state unless you lock. ([PyPI][2])

If you’re not migrating from v1, the RFC itself says to prefer v7; v6 exists primarily for v1 continuity. ([RFC Editor][1])

[1]: https://www.rfc-editor.org/rfc/rfc9562.html "RFC 9562: Universally Unique IDentifiers (UUIDs)"
[2]: https://pypi.org/project/uuid6/?utm_source=chatgpt.com "uuid6"
[3]: https://docs.python.org/3/library/uuid.html?utm_source=chatgpt.com "uuid — UUID objects according to RFC 9562 — Python 3.14.2 ..."
[4]: https://github.com/oittaa/uuid6-python "GitHub - oittaa/uuid6-python: New time-based UUID formats which are suited for use as a database key"

## 4) v1 → v6 conversion (`uuid1_to_uuid6`)

### 4.1 Why this exists (interoperability + locality)

UUIDv6 is explicitly defined as **field-compatible** with UUIDv1, but with the timestamp bytes reordered to improve **DB locality / sort-by-time** behavior. The RFC positions v6 primarily as a “legacy v1 migration / coexistence” format. ([RFC Editor][1])

`uuid6.uuid1_to_uuid6()` exists to let you **mechanically migrate existing UUIDv1 identifiers** into UUIDv6 while preserving the *semantic payload* (timestamp + clock sequence + node). The project README includes a concrete v1→v6 test vector that demonstrates this is a *deterministic*, reversible transform. ([GitHub][2])

---

### 4.2 Exact transformation contract (what is preserved vs reordered)

#### 4.2.1 Preserved (bit-identical payload)

Per RFC 9562 §5.6, conversion preserves:

* **60-bit v1/v6 timestamp value** (the same timestamp, re-encoded),
* **clock sequence** bits,
* **node** bits,
* **variant** bits (assuming RFC 4122/9562 variant-1 input). ([RFC Editor][1])

> RFC wording is blunt: “The clock sequence and node bits remain unchanged” across v1/v6; only the timestamp byte ordering changes. ([RFC Editor][1])

#### 4.2.2 Reordered (layout-only changes)

For v6, the timestamp is not split as v1’s `{time_low, time_mid, time_high}`. Instead:

* Take the **60-bit timestamp** `T` (as defined for v1),
* Store the **first 48 most significant bits** of `T` first,
* then the **4-bit version** (6),
* then the **remaining 12 least significant bits** of `T`. ([RFC Editor][1])

That “timestamp MSB→LSB” ordering is what makes v6 “sortable by creation time” as an opaque 128-bit value. ([RFC Editor][1])

---

### 4.3 The canonical v1→v6 mapping (bit math you can implement / test)

Let `T` be the v1/v6 60-bit Gregorian timestamp (100ns ticks since 1582-10-15), and let `CS` be the 14-bit clock sequence, and `N` the 48-bit node.

Then v6 fields are derived from `T` as:

```text
time_high (32 bits) = (T >> 28) & 0xffffffff     # bits 59..28
time_mid  (16 bits) = (T >> 12) & 0xffff         # bits 27..12
time_low  (12 bits) =  T        & 0x0fff         # bits 11..0

time_low_and_version (16 bits) = (0x6 << 12) | time_low
```

And you pack:

```text
[ time_high | time_mid | time_low_and_version | clock_seq_hi_variant | clock_seq_low | node ]
```

This matches the README’s published conversion example:

* v1: `C232AB00-9414-11EC-B3C8-9E6BDECED846`
* v6: `1EC9414C-232A-6B00-B3C8-9E6BDECED846` ([GitHub][2])

---

### 4.4 Validating conversion correctness (invariants + “round-trip” discipline)

Python gives you several “must match” invariants:

#### 4.4.1 Timestamp + clock sequence invariants (high-signal)

Python’s `uuid.UUID.time` is **the 60-bit Gregorian timestamp** for **versions 1 and 6**; `uuid.UUID.clock_seq` is the **14-bit clock sequence** for **versions 1 and 6**. ([Python documentation][3])

So a correct conversion must satisfy:

```python
import uuid
import uuid6

u1 = uuid.UUID("c232ab00-9414-11ec-b3c8-9e6bdeced846")
u6 = uuid6.uuid1_to_uuid6(u1)

assert u1.version == 1
assert u6.version == 6

# semantic payload preserved:
assert u6.time == u1.time
assert u6.clock_seq == u1.clock_seq
```

These two asserts catch most broken implementations immediately. ([Python documentation][3])

#### 4.4.2 Node preservation (don’t rely on doc labels; use bits)

Python’s docs label `UUID.node` as “only relevant to version 1”, but node is still “the last 48 bits” of the UUID as a value. ([Python documentation][3])

Use a bitmask for version-agnostic validation:

```python
MASK_48 = (1 << 48) - 1
assert (u6.int & MASK_48) == (u1.int & MASK_48)
```

`UUID.int` is the canonical 128-bit integer representation, and UUID comparisons/sorting use it. ([Python documentation][3])

#### 4.4.3 Variant sanity check (reject garbage inputs)

Before converting, enforce the RFC variant:

```python
import uuid
assert u1.variant == uuid.RFC_4122
```

If you skip this, you can manufacture “v6-looking” outputs from non-RFC UUIDs that have undefined semantics. ([Python documentation][3])

---

### 4.5 Round-tripping: v1 ↔ v6 is bijective (and why you still need tests)

Because v6 retains the full `{timestamp, clock_seq, node}` payload, the transform is **reversible**: you can convert back-and-forth by re-encoding the same fields (only the timestamp byte ordering + version nibble differ). Libraries in other ecosystems explicitly document this “convert back-and-forth” property. ([Ramsey UUID][4])

A minimal **reverse** implementation (useful for test harnesses even if the library doesn’t ship it):

```python
import uuid

def uuid6_to_uuid1(u6: uuid.UUID) -> uuid.UUID:
    if u6.version != 6:
        raise ValueError("expected UUIDv6")

    # semantic payload
    T = u6.time                 # 60-bit Gregorian timestamp for v1/v6
    cs = u6.clock_seq           # 14-bit

    # split v1 timestamp fields
    time_low = T & 0xffffffff
    time_mid = (T >> 32) & 0xffff
    time_hi  = (T >> 48) & 0x0fff
    time_hi_version = (0x1 << 12) | time_hi

    # clock sequence fields (preserve variant bits already present in u6)
    clock_seq_hi_variant = u6.clock_seq_hi_variant
    clock_seq_low = u6.clock_seq_low

    node = u6.int & ((1 << 48) - 1)

    return uuid.UUID(fields=(
        time_low, time_mid, time_hi_version,
        clock_seq_hi_variant, clock_seq_low, node
    ))
```

Note: `uuid.UUID(fields=...)` is a supported constructor path, and the tuple structure is documented. ([Python documentation][3])

**Round-trip test**:

```python
u1_rt = uuid6_to_uuid1(uuid6.uuid1_to_uuid6(u1))
assert u1_rt == u1
```

This is the most powerful regression test for agents modifying conversion code.

---

### 4.6 Migration goal: “make v1 keys sortable” without losing v1 semantics

#### 4.6.1 What you gain

* **Index locality / insertion clustering**: v6’s leading bytes are timestamp MSBs, so new records tend to land near each other in B-trees when IDs are generated roughly in time order. ([RFC Editor][1])
* **Sort-by-time as opaque bytes**: applications can sort UUIDs directly (via `UUID.int` / 16-byte big-endian) and get chronological ordering properties that v1 does not provide. ([RFC Editor][1])

#### 4.6.2 What you *don’t* gain

* **Privacy improvement**: conversion preserves node/clock_seq; if your v1 node encodes a MAC address, your v6 will still leak it. Python explicitly warns about v1 privacy concerns. ([Python documentation][3])
* **“more uniqueness”**: it’s a bijection; collisions remain collisions.

#### 4.6.3 Practical DB migration pattern (agent-ready)

1. Add `id_v6` column (same type as existing UUID storage: `BINARY(16)` preferred).
2. Backfill: `id_v6 = uuid1_to_uuid6(id_v1)` in application or ETL.
3. Create new PK/index on `id_v6` (concurrently if supported).
4. Migrate FKs / references in dependent tables.
5. Optionally keep `id_v1` as a legacy column for external references, or build a mapping table if the ID is externally visible.

**Hard warning:** the UUID bytes and string representation change, so any software treating identifiers as immutable strings must be migrated in lockstep. (Other ecosystems’ docs flag the same caveat when converting encodings.) ([Ramsey UUID][4])

[1]: https://www.rfc-editor.org/rfc/rfc9562.html "RFC 9562: Universally Unique IDentifiers (UUIDs)"
[2]: https://github.com/oittaa/uuid6-python "GitHub - oittaa/uuid6-python: New time-based UUID formats which are suited for use as a database key"
[3]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"
[4]: https://uuid.ramsey.dev/en/stable/rfc4122/version6.html?utm_source=chatgpt.com "Version 6: Reordered Gregorian Time - ramsey/uuid"

## 5) UUIDv7 deep dive (Unix-epoch ms + randomness; “default recommendation”)

### 5.1 Why v7 is the recommended default (when you don’t need v1-compat)

RFC 9562 positions UUIDv7 as the *default* time-ordered UUID: it uses a **widely understood Unix epoch millisecond timestamp** and (generally) offers **improved entropy characteristics** compared to v1/v6, while still being sortable for DB/index locality. The RFC is explicit: implementations **SHOULD utilize UUIDv7 instead of UUIDv1 and UUIDv6 if possible**. ([RFC Editor][1])

Practically, UUIDv7 is “cleaner” than v6 for most modern systems because it avoids the v1/v6 legacy bundle of:

* Gregorian epoch timestamp semantics,
* clock sequence statefulness,
* node identifier semantics (and the privacy/host-identifiability concerns that come with stable node identifiers). ([RFC Editor][1])

The `uuid6` package mirrors this recommendation in its own guidance: v7 is the go-to unless you have a v1 continuity requirement (→ v6) or a custom format contract (→ v8). ([PyPI][2])

---

### 5.2 Bit layout & timestamp source (Unix epoch milliseconds)

RFC 9562 defines UUIDv7 as:

* **`unix_ts_ms` (48 bits, big-endian):** milliseconds since Unix epoch (1970-01-01 00:00:00 UTC), leap seconds excluded
* **`ver` (4 bits):** `0b0111` (7)
* **`rand_a` (12 bits):** pseudo-random data *and/or* sub-ms precision bits (optional monotonicity constructs)
* **`var` (2 bits):** `0b10` (RFC variant)
* **`rand_b` (62 bits):** pseudo-random data *and/or* counter bits (optional monotonicity constructs) ([RFC Editor][1])

The canonical field diagram (as raw bits) is in RFC Figure 11: ([RFC Editor][1])

**Why the layout matters for sorting:** UUIDv7 (like v6) is designed so that systems can sort UUIDs **as opaque raw bytes** and get time locality benefits; the spec also intends the **text form** to be lexicographically sortable and notes big-endian byte order as the baseline. ([RFC Editor][1])

---

### 5.3 Ordering guarantees (what you *actually* get)

#### 5.3.1 The baseline guarantee: millisecond buckets sort by time

UUIDv7 places `unix_ts_ms` in the **most significant 48 bits**, so comparisons by the 128-bit integer value (or big-endian bytes) will sort by timestamp at millisecond granularity. ([RFC Editor][1])

#### 5.3.2 Within the same millisecond: “random bits” do not imply monotonic order

RFC’s default construction is: put the 48-bit ms timestamp in front, then fill the remaining **74 bits (excluding version+variant)** with random bits for uniqueness. This yields:

* strong collision resistance,
* *but* ordering among UUIDs generated inside the **same millisecond** is effectively arbitrary (because the tie-breaker bits are random). ([RFC Editor][1])

#### 5.3.3 RFC-sanctioned monotonicity options (for high-frequency generation)

RFC 9562 explicitly allows replacing part of the 74-bit “random region” with monotonic constructs **in order**:

1. optional **sub-ms fraction** (up to 12 bits) (Method 3),
2. optional **seeded counter** (Method 1 or 2),
3. random remainder. ([RFC Editor][1])

Key details agents should internalize:

* **Method 3 (sub-ms precision):** you can repurpose up to the 12 `rand_a` bits for a fractional-millisecond value (scaled into 0..4095) so values remain time-ordered below 1ms. ([RFC Editor][1])
* **Method 2 (randomly seeded counter):** treat some random bits as a counter that is incremented (least-significant position) per UUID on the same timestamp tick, preserving “unguessability” better than a simple `+1`. ([RFC Editor][1])
* **Method 1 (dedicated counter):** allocate a fixed counter immediately after the timestamp; RFC guidance: counter **SHOULD be at least 12 bits and no longer than 42 bits**, leaving entropy after it. ([RFC Editor][1])

#### 5.3.4 Concrete implementations you will encounter (stdlib vs `uuid6`)

* **Python 3.14+ stdlib `uuid.uuid7()`**: embeds a 48-bit timestamp and uses a **42-bit counter** to guarantee monotonicity within a millisecond. ([Python documentation][3])
* **`uuid6` (third-party) `uuid6.uuid7()`**: documents that the `unix_ts_ms` field “guarantees the order” within a millisecond by **monotonically incrementing the timer**, and notes it is **not thread-safe**. ([PyPI][2])

These are not equivalent strategies:

* “42-bit counter” keeps the millisecond timestamp stable while ordering via counter bits.
* “monotonically incrementing the timer” can imply advancing the timestamp when the clock doesn’t tick fast enough (preserving sort order but potentially pushing embedded time into the future).

---

### 5.4 Extracting time from `uuid.UUID` (Python behavior for v7)

Python defines `UUID.time` for version 7 as:

> the **48-bit timestamp in milliseconds since Unix epoch**. ([Python documentation][3])

So extraction is trivial:

```python
import uuid6
from datetime import datetime, timedelta, timezone

u = uuid6.uuid7()
assert u.version == 7

unix_ms = u.time
dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=unix_ms)
```

One nuance the RFC explicitly permits: implementations **MAY alter the actual timestamp**, and the spec makes **no guarantee** that the embedded time equals wall-clock time (e.g., to handle clock corrections, leap-second handling strategies, or performance-oriented time scaling). Treat `UUID.time` as “time-ordered signal,” not a trusted clock. ([RFC Editor][1])

---

### 5.5 Throughput behavior (high ID rates; same-ms behavior)

UUIDv7 is ms-based by default. Under high generation rates, “what happens within one millisecond” is governed by which monotonicity strategy is used.

#### 5.5.1 If you use pure randomness in the 74-bit remainder

* **Uniqueness/collision resistance:** extremely high (in the usual birthday-paradox sense for large random spaces).
* **Ordering within the same ms:** not guaranteed (random tie-breaker). ([RFC Editor][1])

#### 5.5.2 If you use a counter (RFC Method 1/2; or stdlib’s approach)

* You can guarantee monotonic ordering within a millisecond up to the counter’s capacity.
* RFC guidance for counter length: **≥12 bits and ≤42 bits** (e.g., 12 bits → 4096 UUIDs/ms; 20 bits → ~1M UUIDs/ms; 42 bits → ~4.4e12 UUIDs/ms). ([RFC Editor][1])

RFC also warns about “overrun” behavior: if too many UUIDs are requested within one system-time interval, a service can **stall or error** and **MUST NOT knowingly return duplicates** due to counter rollover. ([RFC Editor][1])

#### 5.5.3 If you “advance the timestamp” to preserve monotonicity

This is the behavior the `uuid6` package documents (monotonic increment of `unix_ts_ms` within the same millisecond). It preserves strict ordering, but can cause embedded timestamps to drift ahead of wall time, especially under sustained same-ms generation or clock granularity limitations. ([PyPI][2])

#### 5.5.4 Concurrency caveat (important for agents)

The `uuid6` package explicitly marks `uuid7()` as **not thread-safe**, so any monotonic guarantees are best treated as **single-threaded** unless you serialize generation yourself. ([PyPI][2])

A minimal “make the guarantee real” wrapper:

```python
import threading
import uuid6

_lock = threading.Lock()

def uuid7_monotone_threadsafe():
    with _lock:
        return uuid6.uuid7()
```

(If you’re on Python 3.14+, and you want a well-specified monotonic strategy under the stdlib contract, prefer `uuid.uuid7()`.) ([Python documentation][3])

[1]: https://www.rfc-editor.org/rfc/rfc9562.html "RFC 9562: Universally Unique IDentifiers (UUIDs)"
[2]: https://pypi.org/project/uuid6/ "uuid6 · PyPI"
[3]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"

## 6) UUIDv8 deep dive (vendor/experimental format; higher time granularity tradeoffs)

### 6.1 Why v8 exists (RFC-compatible “custom space”)

**UUIDv8** is the “escape hatch” in RFC 9562: it provides a UUID that is still structurally a UUID (proper **variant** + **version** bits), but leaves the remaining bits to the implementation. Concretely:

* **Only requirement:** set **variant** and **version=8** bits correctly.
* **Uniqueness is implementation-specific and MUST NOT be assumed.**
* **122 bits are “free” custom bits** (48 + 12 + 62 around the fixed version/variant positions).
* v8 is **not** “v4 but newer”: RFC explicitly notes it is not a replacement for v4’s “fill all 122 bits with random.” ([RFC Editor][1])

RFC 9562’s best-practices section is also explicit about *why* you might need v8:

* If you need **other timestamp sources** or a **custom epoch**, “UUIDv8 MUST be used.”
* If you need **precision beyond v7’s default millisecond time**, v8 exists as the sanctioned space to do it. ([RFC Editor][1])

**Agent rule:** treat v8 as a *protocol*, not an algorithm. The standard only guarantees “this is a v8 UUID,” not what it *means*. ([RFC Editor][1])

---

### 6.2 “v8 in Python” is two different things (stdlib vs `uuid6` package)

#### 6.2.1 Python 3.14+ stdlib: `uuid.uuid8(a=None, b=None, c=None)`

The stdlib `uuid.uuid8()` is a **generic v8 constructor** that fills the RFC-defined custom blocks:

* `a` = 48 bits, `b` = 12 bits, `c` = 62 bits (with truncation to LSBs if oversized).
* By default, its pseudo-random blocks are **not guaranteed CSPRNG**; the docs explicitly recommend `uuid4()` for security-sensitive contexts. ([Python documentation][2])

So stdlib v8 is basically “give me a syntactically-correct v8 UUID with custom blocks,” with no implied time semantics. ([Python documentation][2])

#### 6.2.2 `uuid6` (third-party): `uuid6.uuid8()` is *opinionated* and time-derived

The `uuid6` package defines a specific v8 *meaning*: a **time-ordered** layout that looks like v7 but trades some randomness for sub-millisecond precision. It documents an explicit field layout and claims nanosecond-resolution ordering behavior. ([PyPI][3])

**Agent rule:** do not swap between stdlib `uuid.uuid8()` and `uuid6.uuid8()` and assume compatibility—the semantics are different even though the UUID “version” is the same. ([Python documentation][2])

---

### 6.3 Layout used by `uuid6.uuid8()` (ms + sub-second fields + random tail)

The project documents this exact layout (bits shown in network order), mapping directly onto RFC 9562’s `custom_a` / `custom_b` / `custom_c` structure: ([RFC Editor][1])

**`uuid6.uuid8()` field allocation**

* `unix_ts_ms` = **48 bits** (big-endian Unix epoch **milliseconds**)
* `ver` = **4 bits** (1000)
* `subsec_a` = **12 bits** (sub-second precision)
* `var` = **2 bits** (10)
* `subsec_b` = **8 bits** (sub-second precision)
* `rand` = **54 bits** (“cryptographically strong random data,” per project docs) ([PyPI][3])

So the “custom” space is partitioned as:

* `custom_a (48)` := `unix_ts_ms`
* `custom_b (12)` := `subsec_a`
* `custom_c (62)` := `subsec_b (8) || rand (54)`

The library’s intent statement is explicit:

* It “sacrifices some entropy for granularity compared to uuid7()”
* The `(unix_ts_ms, subsec_a, subsec_b)` triple is used to guarantee ordering “within the same nanosecond” by “monotonically incrementing the timer”
* The generator is **not thread-safe** (so treat those guarantees as single-threaded unless you serialize calls). ([PyPI][3])

---

### 6.4 Parsing / extracting fields (agent-ready bit slicing)

Python’s `uuid.UUID` doesn’t define `.time` semantics for v8 (only v1/v6/v7 have meaningful `UUID.time` in the docs). For `uuid6`’s **time-derived** v8, you must decode from bytes/ints using the library’s layout. (Use `u.bytes`, not `bytes_le`.)

```python
from __future__ import annotations

import uuid
from dataclasses import dataclass

@dataclass(frozen=True)
class UUID6V8Parts:
    unix_ts_ms: int     # 48 bits
    subsec_a: int       # 12 bits
    subsec_b: int       # 8 bits
    rand: int           # 54 bits

def parse_uuid6_uuid8(u: uuid.UUID) -> UUID6V8Parts:
    if u.version != 8:
        raise ValueError("expected UUIDv8")
    b = u.bytes  # network byte order

    # 48-bit unix_ts_ms (octets 0..5)
    unix_ts_ms = int.from_bytes(b[0:6], "big")

    # octet 6: high nibble is version, low nibble is top 4 bits of subsec_a
    ver = b[6] >> 4
    if ver != 0x8:
        raise ValueError("not uuid6-style uuid8 (version nibble mismatch)")

    subsec_a = ((b[6] & 0x0F) << 8) | b[7]  # 12 bits: low nibble of octet6 + octet7

    # octet 8: top 2 bits are variant (should be 0b10)
    var = b[8] >> 6
    if var != 0b10:
        raise ValueError("unexpected UUID variant")

    # subsec_b spans: remaining 6 bits of octet8 + top 2 bits of octet9 (8 bits total)
    subsec_b = ((b[8] & 0x3F) << 2) | (b[9] >> 6)

    # rand spans: lower 6 bits of octet9 + octets10..15 (6 + 48 = 54 bits)
    rand = ((b[9] & 0x3F) << 48) | int.from_bytes(b[10:16], "big")

    return UUID6V8Parts(unix_ts_ms=unix_ts_ms, subsec_a=subsec_a, subsec_b=subsec_b, rand=rand)
```

**Combining sub-second bits:**
`subsec_20 = (subsec_a << 8) | subsec_b` gives a **20-bit** value. The project claims this yields “nanosecond resolution,” but the exact mapping from those 20 bits to `[0, 1_000_000)` nanoseconds-within-millisecond is an implementation choice (not an RFC guarantee). ([PyPI][3])

---

### 6.5 Entropy vs granularity trade (what you’re really buying)

#### 6.5.1 Bit budget comparison vs v7

* RFC v7 defaults to **48 bits time (ms)** + **74 bits “random region”** (12+62) for uniqueness/optionally counters. ([RFC Editor][1])
* `uuid6`’s v8 chooses **48 bits time (ms)** + **20 bits sub-second** + **54 bits random tail**. ([PyPI][3])

So relative to a “pure-random remainder” v7, this v8 policy reduces the random budget by **20 bits** to encode sub-ms ordering. ([RFC Editor][1])

#### 6.5.2 Collision surface (where randomness still matters)

In this v8 design, collisions are only plausible among IDs that share the same `(unix_ts_ms, subsec_20)` (i.e., the same time bucket under the library’s sub-second mapping). Within such a bucket, the collision resistance is dominated by the **54-bit random tail** (birthday-style collision probability grows ~quadratically with the number of IDs per bucket).

**Agent interpretation:** the intent is “more buckets (ns-ish), fewer random bits,” which is usually fine for DB keys, but it is a *deliberate* step away from v7’s higher random budget.

#### 6.5.3 Monotonic ordering and “time drift”

The project claims ordering “within the same nanosecond” by “monotonically incrementing the timer.” That usually implies generator state that can:

* bump sub-second bits (and possibly ms) forward when multiple calls happen faster than the platform clock resolution,
* potentially create UUIDs whose embedded time is **slightly ahead** of wall time under extreme burst rates. ([PyPI][3])

And because it’s explicitly **not thread-safe**, you must serialize generation if you want those monotonic properties to hold globally within a process. ([PyPI][3])

---

### 6.6 “When to choose this v8” (decision discipline)

Use `uuid6.uuid8()` only if you can justify *both*:

1. **You control all producers/consumers** (or you’re storing opaque IDs with no cross-system decoding requirements), because v8 semantics are not standardized. ([RFC Editor][1])
2. You genuinely need **sub-millisecond ordering/precision** encoded into the ID (or a custom epoch/time source), which RFC explicitly reserves v8 to enable. ([RFC Editor][1])

Otherwise:

* choose **v7** for “sortable + high entropy + standardized meaning,”
* choose **v6** only for “v1 continuity/migration.” ([RFC Editor][1])

[1]: https://www.rfc-editor.org/rfc/rfc9562.html "RFC 9562: Universally Unique IDentifiers (UUIDs)"
[2]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"
[3]: https://pypi.org/project/uuid6/ "uuid6 · PyPI"

## 7) Ordering, sorting, and database-key behavior (the “why you care” section)

### 7.1 Comparison semantics (Python): `uuid.UUID` ordering == integer ordering

In Python, `uuid.UUID` instances are *totally ordered* by comparing their **128-bit integer value** (`UUID.int`). This is not “string compare,” and not “field-aware”—it’s literally integer order. ([Python documentation][1])

That matters because:

* If your UUID encoding is **time-ordered in its most-significant bits** (v6/v7), then plain `sorted(list_of_uuids)` yields “approximately chronological” order (subject to same-tick behavior). ([Python documentation][1])
* If you accidentally store/transport UUIDs in a **little-endian/mixed-endian** encoding and then reinterpret them as big-endian, your ordering will be wrong even if the *conceptual* UUID version is time-ordered. ([Python documentation][1])

**Agent invariant (Python-side):**

```python
import uuid6

u = uuid6.uuid7()
assert u.int == int.from_bytes(u.bytes, "big")  # big-endian network order
```

`UUID.bytes` is defined as the 16-byte representation in **big-endian** (network) byte order; `UUID.bytes_le` is a special little-endian encoding for the first three fields. ([Python documentation][1])

---

### 7.2 RFC 9562: “time-ordered UUIDs” are explicitly designed for DB/index sorting

RFC 9562 makes the DB story explicit in its **Sorting** guidance:

* **v6 and v7** are designed to be sortable “as opaque raw bytes” (no parsing required) for database indexes. ([IETF Datatracker][2])
* Time-ordered monotonic UUIDs improve index locality; RFC notes the real-world difference vs random inserts can be “one order of magnitude or more.” ([IETF Datatracker][2])
* Formats are intended to be **lexicographically sortable in textual form**, and are crafted with **big-endian (network byte order)** in mind. ([IETF Datatracker][2])

**Agent takeaway:** if you store UUIDs in a representation whose compare/sort follows big-endian byte order, v7 “just works” as a locality-friendly key. If your database compares UUIDs in a different byte significance order (not uncommon), you must account for that. ([IETF Datatracker][2])

---

### 7.3 Binary vs text storage: the high-leverage tradeoffs

#### 7.3.1 Size / index footprint

* UUIDs are **128-bit** identifiers; the compact representation is **16 bytes**. ([PostgreSQL][3])
* Text forms are **32 hex digits** plus separators (typically 36 chars with hyphens). PostgreSQL documents the canonical hyphenated form and that output is always standardized. ([PostgreSQL][3])
* MySQL explicitly provides `UUID_TO_BIN()`/`BIN_TO_UUID()` to convert between string UUIDs and **`VARBINARY(16)`** storage. ([MySQL Developer Zone][4])

**DB consequence:** larger keys → larger B-tree pages → fewer keys per page → more page reads/writes and more cache pressure, especially for PK + secondary indexes. (This is the *mechanical* reason “binary(16) beats char(36)” for hot tables.)

#### 7.3.2 Collation pitfalls (text UUIDs are “strings,” so string rules apply)

If you store UUIDs as `CHAR/VARCHAR/TEXT`, their comparison/sort/uniqueness depends on **collation and character set**:

* In MySQL, Unicode encodings like `utf8mb4` can require up to **4 bytes per character**, affecting row/index sizing and sometimes index limits. ([MySQL Developer Zone][5])
* Case-insensitive collations can treat `A` and `a` as equal; some collations can do non-binary comparisons (usually not a correctness problem for UUID *values*, but it can create surprising behaviors if you rely on bytewise ordering or store non-canonical forms).

**Best practice:** store as a **binary UUID type** (Postgres `uuid`, SQL Server `uniqueidentifier`, MySQL `BINARY(16)`/`VARBINARY(16)`), and canonicalize text only at the edges (APIs/logging). ([PostgreSQL][3])

#### 7.3.3 Byte-order expectations (the “silent killer” for ordered UUIDs)

RFC 9562 assumes **big-endian/network byte order** for the UUID formats it defines. ([IETF Datatracker][2])
Python’s `UUID.bytes` matches that (big-endian), while `UUID.bytes_le` exists for little-endian field encodings. ([Python documentation][1])

If you want v7 ordering to survive round-trips and DB ordering:

* persist **`u.bytes`** (big-endian),
* avoid persisting `bytes_le` unless you *explicitly* target a little-endian GUID convention and have a corresponding comparator/order normalization layer. ([Python documentation][1])

---

### 7.4 Database-specific behavior (what’s “common” in practice)

#### 7.4.1 PostgreSQL (native `uuid`, now native `uuidv7()`)

PostgreSQL’s `uuid` type stores UUIDs as RFC 9562 UUIDs (128-bit) and accepts/normalizes common textual forms; output is always the standard form. ([PostgreSQL][3])

As of PostgreSQL 18, you get:

* `uuidv7()` to generate **time-ordered v7 UUIDs** (with ms precision + sub-ms + random). ([PostgreSQL][6])
* `uuid_extract_timestamp(uuid)` to recover a timestamp for v1 or v7, with an explicit warning that extracted time may not exactly equal generation time (depends on producer implementation). ([PostgreSQL][6])
* Normal comparison operators for UUIDs (so B-tree indexing works as expected). ([PostgreSQL][6])

**Agent stance:** for Postgres, “do the obvious”: use `uuid` type, generate v7 either in-app (`uuid6.uuid7()` / `uuid.uuid7()`) or in-DB with `uuidv7()`, and rely on native comparisons. ([PostgreSQL][6])

#### 7.4.2 MySQL (no native UUID type; prefer `BINARY(16)` + conversion helpers)

MySQL’s `UUID()` produces string UUIDs; MySQL provides:

* `UUID_TO_BIN(str_uuid[, swap_flag])` returning **`VARBINARY(16)`**, and `BIN_TO_UUID()` for conversion. ([MySQL Developer Zone][4])
* `swap_flag` time-part swapping is explicitly a **UUIDv1-only** optimization; for non-v1 values it provides no benefit. ([MySQL Developer Zone][4])

**Implication for v7:**

* store v7 UUIDs as **raw 16 bytes** (big-endian), and **do not** use the v1 swap trick. ([MySQL Developer Zone][4])

#### 7.4.3 SQL Server (`uniqueidentifier` ordering is… not your mental model)

SQL Server’s `uniqueidentifier` is a **16-byte GUID** type. ([Microsoft Learn][7])
But Microsoft explicitly documents that ordering is **not implemented by comparing bit patterns**, and in practice GUID ordering can be unintuitive. ([Microsoft Learn][7])

For performance-sensitive clustered keys, SQL Server provides `NEWSEQUENTIALID()`, which is often faster than `NEWID()` because random GUID inserts cause more random activity and use fewer cached pages; it also helps fill data/index pages more completely. ([Microsoft Learn][8])

**Agent stance:** if you’re on SQL Server and your goal is “reduce fragmentation / churn,” prefer sequential GUID strategies (or a separate clustering key). Don’t assume RFC9562 v7 byte-order guarantees map cleanly onto SQL Server’s ordering semantics without verifying the exact comparator/encoding being used. ([Microsoft Learn][8])

#### 7.4.4 SQLite (no UUID type; you pick TEXT vs BLOB)

SQLite uses dynamic typing with affinities like TEXT and BLOB. ([sqlite.org][9])
For UUIDs you typically choose:

* `BLOB(16)` (compact, binary comparisons),
* or `TEXT(36)` (human-readable, but larger and collation-dependent).

---

### 7.5 Migration patterns: replacing `uuid4()` PKs with v7 while minimizing index churn

#### 7.5.1 The hard truth: “rewrite PK values” is maximal churn

You generally cannot “convert” v4 → v7 while preserving identifiers (v4 has no embedded time). A real PK swap rewrites:

* the PK column values,
* every referencing FK,
* every external reference,
* every cached identifier.

So “minimal churn” almost always means **introducing v7 alongside v4**, not replacing it overnight.

#### 7.5.2 Minimal-churn pattern A: dual-key (keep v4 as stable identity; add v7 as insertion-friendly ordering key)

**Goal:** keep external identity stable (v4), but make *physical locality* and common query patterns fast.

* Add `id_v7` column (uuid / binary16) with default generator for new rows.
* Backfill old rows with *some* ordering key (often `created_at`-derived v7 or just generate v7 at backfill time if you don’t need historical correctness).
* Index `id_v7` (or cluster/partition on it if your DB supports it).
* Keep v4 as PK (no FK rewrite), use v7 for locality, time-range queries, pagination.

PostgreSQL 18 sketch:

```sql
ALTER TABLE t ADD COLUMN id_v7 uuid;
ALTER TABLE t ALTER COLUMN id_v7 SET DEFAULT uuidv7();
CREATE INDEX CONCURRENTLY t_id_v7_idx ON t (id_v7);
```

Postgres provides `uuidv7()` natively. ([PostgreSQL][6])

MySQL sketch (app-generated v7, stored as binary):

* add `id_v7 VARBINARY(16) NOT NULL`
* populate from app with `uuid.UUID.bytes`
* index `(id_v7)`

(You can still use `BIN_TO_UUID()` for display; avoid swap tricks for v7.) ([MySQL Developer Zone][4])

#### 7.5.3 Minimal-churn pattern B: “surrogate clustering key” + UUID as logical key

If your only reason for switching is “B-tree pain,” the least invasive fix is:

* keep UUID (v4 or v7) as a **unique logical identifier**,
* add a monotonically increasing **BIGINT/IDENTITY** as the clustered/primary key (or at least as a leading index component),
* reference by bigint internally; expose UUID externally.

This is boring, extremely effective, and avoids rewriting external identifiers.

#### 7.5.4 Full replacement pattern (only if you truly need v7 as *the* PK)

Phased approach:

1. Add new `id_v7` + unique index.
2. Dual-write: new rows get both `id` (old) and `id_v7` (new).
3. Migrate FKs table-by-table to reference `id_v7`.
4. Flip PK constraint.
5. Keep old `id` as UNIQUE (or keep a mapping table) until all clients are migrated.

**Agent requirement:** build migration tooling that can:

* backfill deterministically,
* validate uniqueness,
* validate referential integrity at each phase,
* and (critically) validate that ordering/locality improvements are actually realized under your workload (insert pattern + fillfactor/page splits).

---

### 7.6 “Agent-ready” storage rules of thumb (portable defaults)

1. **Persist bytes, not strings** whenever you can:

   * Python: store `u.bytes` (big-endian). ([Python documentation][1])
2. Prefer DB-native UUID-ish types:

   * Postgres `uuid`, SQL Server `uniqueidentifier`, MySQL `BINARY(16)`/`VARBINARY(16)`. ([PostgreSQL][3])
3. If you must store text, normalize (lowercase, hyphenated) at write time to avoid collation/representation surprises; Postgres `uuid` type already enforces canonical output. ([PostgreSQL][3])
4. For MySQL, treat `UUID_TO_BIN(..., swap_flag)` as a **v1-only** legacy optimization; don’t apply it to v7. ([MySQL Developer Zone][4])

If you want, next section we can go straight into **8) Concurrency, safety, and determinism boundaries**, including concrete “thread-safe monotone generator” adapters for `uuid6.uuid6/7/8()` and process-safe patterns.

[1]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"
[2]: https://datatracker.ietf.org/doc/rfc9562/ "
            
        RFC 9562 - Universally Unique IDentifiers (UUIDs)

        "
[3]: https://www.postgresql.org/docs/current/datatype-uuid.html "PostgreSQL: Documentation: 18: 8.12. UUID Type"
[4]: https://dev.mysql.com/doc/refman/8.1/en/miscellaneous-functions.html "MySQL :: MySQL 8.4 Reference Manual :: 14.23 Miscellaneous Functions"
[5]: https://dev.mysql.com/doc/refman/9.3/en/charset-unicode-utf8mb4.html?utm_source=chatgpt.com "12.9.1 The utf8mb4 Character Set (4-Byte UTF-8 Unicode ..."
[6]: https://www.postgresql.org/docs/current/functions-uuid.html "PostgreSQL: Documentation: 18: 9.14. UUID Functions"
[7]: https://learn.microsoft.com/en-us/sql/t-sql/data-types/uniqueidentifier-transact-sql?view=sql-server-ver17 "uniqueidentifier (Transact-SQL) - SQL Server | Microsoft Learn"
[8]: https://learn.microsoft.com/en-us/sql/t-sql/functions/newsequentialid-transact-sql?view=sql-server-ver17 "NEWSEQUENTIALID (Transact-SQL) - SQL Server | Microsoft Learn"
[9]: https://www.sqlite.org/datatype3.html?utm_source=chatgpt.com "Datatypes In SQLite"

## 8) Concurrency, safety, and determinism boundaries

### 8.1 Core mental model: time-ordered UUIDs are *stateful generators*

RFC 9562 is explicit that **monotonicity** (“each subsequent value being greater than the last”) is the backbone of sortable UUIDs, and that batch generation within the *same timestamp* often requires **counter/state logic**. In other words: the generator is not just “timestamp + random”—it’s “timestamp + (maybe counter) + randomness + guardrails.” ([RFC Editor][1])

That state can be:

* **in-memory** (last timestamp / last counter) for fast per-process generation,
* optionally backed by **stable storage** for crash/reboot safety and cross-process coordination. ([RFC Editor][1])

---

### 8.2 Thread-safety: what `uuid6` promises (and what it doesn’t)

The `uuid6` package *explicitly* documents that all three generators are **not thread-safe**:

* `uuid6.uuid6(...): This function is not thread-safe.`
* `uuid6.uuid7(): This function is not thread-safe.`
* `uuid6.uuid8(): This function is not thread-safe.` ([PyPI][2])

It also documents that v6/v7 ordering within the same tick is achieved by “**monotonically incrementing the timer**” (i.e., there’s mutable “last time used” state). ([PyPI][2])

**Practical consequence:** you must treat `uuid6.uuid6/7/8()` as a **critical section** if you rely on any of:

* strict monotonic `UUID.int` ordering inside a process,
* “no duplicates ever” properties under concurrency,
* stable timestamp extraction (since “incrementing the timer” can drift forward under contention).

---

### 8.3 Safe wrapper patterns (single process)

#### Pattern A — global lock (max correctness, simplest)

Use this when you want the package’s monotone claims to actually hold “globally” in your process.

```python
import threading
import uuid
import uuid6

_lock = threading.Lock()

def uuid7_threadsafe() -> uuid.UUID:
    with _lock:
        return uuid6.uuid7()

def uuid6_threadsafe(node: int | None = None, clock_seq: int | None = None) -> uuid.UUID:
    with _lock:
        return uuid6.uuid6(node=node, clock_seq=clock_seq)

def uuid8_threadsafe() -> uuid.UUID:
    with _lock:
        return uuid6.uuid8()
```

This is the “make races impossible” adapter. It directly aligns with RFC 9562’s guidance that batch UUID creation may need monotonic counters and “monotonic error checking” logic; you’re enforcing single-writer semantics at the application boundary. ([RFC Editor][1])

#### Pattern B — “ID service” thread (high throughput, low lock contention)

If lock contention is high, put generation behind a dedicated thread and hand out IDs over a queue. This preserves strict monotonicity (single generator) while avoiding lock acquisition on every caller (callers do queue ops).

#### Pattern C — batch allocation (reduce lock frequency)

Generate N UUIDs under lock, return the slice to callers. This is equivalent to RFC’s “block allocation” idea (allocate a block of timestamps / IDs in one place, consume locally). ([RFC Editor][1])

---

### 8.4 Multi-process behavior: what you can assume (and what you can’t)

#### 8.4.1 What you *can* assume

* Each OS process has independent in-memory generator state, so “not thread-safe” does **not** automatically imply “not process-safe.”
* If the UUID version includes substantial randomness (v7 does by default; v8 depends on your design), **collisions across processes are typically dominated by entropy**, not shared state.

#### 8.4.2 What you *cannot* assume

* **Monotonic ordering across processes** is not a property of the UUID formats; it requires coordination.
* **Crash/reboot safety** (avoiding duplicates after restarts) is not guaranteed unless you persist generator state or include enough entropy. RFC 9562 calls out that stable storage *may* record last timestamp / counters / random data, and that skipping stable store increases duplicate probability and stresses entropy sources. ([RFC Editor][1])

#### 8.4.3 If you actually need “multi-process monotone”

RFC 9562 describes two relevant design levers:

* read/write generator state from **stable storage** (last timestamp, counters, etc.), or
* allocate **blocks** from a system-wide generator to per-process generators (a scalable coordination pattern). ([RFC Editor][1])

In Python, that can map to:

* a file-lock + small state file (timestamp/counter) on local disk,
* a lightweight local daemon (Unix socket) that issues monotone IDs,
* or an external coordinator (Redis atomic increments) if you need *global* ordering.

---

### 8.5 Clock behavior: rollback, NTP corrections, and “time drift”

#### 8.5.1 The environment is allowed to move the clock backwards

RFC 9562 explicitly warns that system clock changes (manual adjustment, time sync protocols) must be handled consistently with implementation requirements. ([RFC Editor][1])

#### 8.5.2 v1/v6 rollback rule: change the clock sequence

For v1 (and by extension v6’s compatible semantics), RFC 9562 states:

* the clock sequence exists to avoid duplicates when the clock is set backwards or node ID changes,
* if the clock is set backwards (or might have been), and you can’t be sure you didn’t already emit “future” timestamps, then the **clock sequence MUST be changed** (increment if known; otherwise random/high-quality pseudorandom). ([RFC Editor][1])

This is the “don’t repeat (timestamp, clock_seq, node)” rule in operational form.

#### 8.5.3 v7 rollback/counter rule: enforce monotonicity with counters (or timestamp bump)

RFC 9562’s monotonicity section provides concrete guidance for counter-based monotone generation:

* counters **SHOULD be at least 12 bits and no longer than 42 bits**,
* counter rollover handling: freeze and wait for timestamp advance, *or* increment timestamp ahead of actual time and reinitialize the counter,
* implementations **SHOULD check** that each new UUID is greater than the previous; if not, reuse previous timestamp and increment previous counter method. ([RFC Editor][1])

**Python 3.14+ stdlib** explicitly chooses the max guidance: `uuid.uuid7()` uses a **42-bit counter** to guarantee monotonicity within a millisecond. ([Python documentation][3])

**`uuid6` package** instead documents “monotonically incrementing the timer” to preserve within-ms ordering. That implies it may “push” timestamps forward under load (which is RFC-permitted as one rollover/monotonicity strategy). ([PyPI][2])

---

### 8.6 Distributed generation (multi-host): uniqueness vs global ordering

RFC 9562 notes that as you add nodes generating UUIDs independently, collision probability increases (still low with good entropy), and describes two “extra collision resistance” approaches:

* embedding a **pseudorandom node ID** (RFC suggests doing this with UUIDv8 if you choose to embed node IDs),
* centralized registries are **NOT RECOMMENDED** at scale (bottleneck). ([RFC Editor][1])

It also states distributed applications **MUST be willing to rely on the random number source at all hosts.** ([RFC Editor][1])

**Agent rule:** UUIDs solve uniqueness probabilistically. They do *not* solve “total order across machines” without additional coordination (logical clocks / consensus / sequencers).

---

### 8.7 Determinism boundaries (what to encode into agent “correctness expectations”)

**Safe to assume (with proper locking):**

* In-process, single-writer generation can preserve strict `UUID.int` monotonicity for v6/v7/v8-style time-ordered layouts. ([PyPI][2])

**Not safe to assume:**

* `UUID.time` equals wall-clock time (implementations may smear/alter timestamps; `uuid6` explicitly increments “the timer” under same-tick conditions). ([RFC Editor][1])
* Monotonicity across threads/processes/hosts without explicit coordination. ([RFC Editor][1])

---

### 8.8 Practical recommendations (for agent-generated production code)

* If you are on **Python 3.14+** and you want a clearly documented within-ms monotonicity strategy, prefer **stdlib `uuid.uuid7()`** (42-bit counter). ([Python documentation][3])
* If you must use `uuid6` (backport / pinned behavior), **wrap generation in a lock** whenever ordering or “never duplicate” is a correctness property. ([PyPI][2])
* If you need **cross-process monotonicity** on the same host, implement one of RFC’s coordination patterns (stable store state or block allocation). ([RFC Editor][1])
* If you need **global total ordering**, don’t pretend UUIDs provide it—introduce a sequencer/logical clock.

[1]: https://www.rfc-editor.org/rfc/rfc9562.html "RFC 9562: Universally Unique IDentifiers (UUIDs)"
[2]: https://pypi.org/project/uuid6/ "uuid6 · PyPI"
[3]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"

## 9) Interop with Python’s `uuid` ecosystem (stdlib + third parties)

### 9.1 The core interop guarantee: `uuid6` returns *stdlib* `uuid.UUID`

The `uuid6` package is deliberately “thin”: it **extends immutable UUID objects (the stdlib `UUID` class)** by adding generators `uuid6()`, `uuid7()`, `uuid8()` (and a v1→v6 converter). In other words, the values you get back are meant to be **drop-in `uuid.UUID` instances**, not a bespoke identifier type. ([PyPI][1])

**Agent implication:** any API that accepts a `uuid.UUID` (or can parse one) should accept `uuid6` outputs *without adapters*.

---

### 9.2 “UUID interoperability” in Python is really about **constructor + representations + ordering**

#### 9.2.1 Constructors: the stdlib `UUID(...)` is the universal parsing boundary

`uuid.UUID(...)` can be constructed from **hex strings**, **big-endian bytes**, **little-endian bytes**, **fields tuples**, or a **128-bit int**; and it can optionally force the variant/version bits via the `version=` parameter. ([Python documentation][2])

This means your canonical “edge normalization” patterns are:

```python
import uuid

u_from_str   = uuid.UUID("12345678-1234-5678-1234-567812345678")
u_from_bytes = uuid.UUID(bytes=b"\x12\x34\x56\x78" * 4)
u_from_int   = uuid.UUID(int=0x12345678123456781234567812345678)
```

#### 9.2.2 Ordering semantics: comparisons are by `UUID.int`

Python defines UUID comparisons by comparing their **128-bit integer value** (`UUID.int`). This is the key enabler for “time-ordered UUIDs behave like sortable keys” in Python. ([Python documentation][2])

```python
# Canonical ordering primitive
u = uuid6.uuid7()
k = u.int  # stable sortable key
```

#### 9.2.3 Byte order matters: `UUID.bytes` vs `UUID.bytes_le`

* `UUID.bytes` is the **16 bytes in big-endian order** (“network order” for the six integer fields).
* `UUID.bytes_le` is a **mixed little-endian** encoding for the first three fields. ([Python documentation][2])

**Agent rule:** if you care about “lexicographic bytes sort” matching “UUID.int sort”, persist/transport `UUID.bytes`, not `bytes_le`.

---

### 9.3 Field access & decoding (version-specific meaning you must not conflate)

#### 9.3.1 Stable, version-agnostic accessors (safe defaults)

These accessors remain meaningful across UUID versions:

* `u.int`, `u.bytes`, `u.hex`, `str(u)` (canonical string), `u.variant`, `u.version` ([Python documentation][2])
* `u.is_safe` indicates whether the platform generated the UUID in a multiprocessing-safe way (mostly relevant for platform-backed uuid1 paths). ([Python documentation][2])

#### 9.3.2 `UUID.time`: **v6 vs v7 have different epochs + units**

Python defines `UUID.time` as:

* for **v1 and v6**: the **60-bit timestamp** in **100ns ticks since the Gregorian epoch (1582-10-15)**
* for **v7**: the **48-bit timestamp in milliseconds since Unix epoch (1970-01-01)** ([Python documentation][2])

So a generic “extract time” routine must branch on `u.version`:

```python
from datetime import datetime, timedelta, timezone

GREGORIAN_EPOCH = datetime(1582, 10, 15, tzinfo=timezone.utc)
UNIX_EPOCH      = datetime(1970, 1, 1, tzinfo=timezone.utc)

def uuid_time_to_datetime(u):
    if u.version in (1, 6):
        t_100ns = u.time
        return GREGORIAN_EPOCH + timedelta(microseconds=t_100ns // 10)
    if u.version == 7:
        unix_ms = u.time
        return UNIX_EPOCH + timedelta(milliseconds=unix_ms)
    raise ValueError("UUID.time is not defined for this version in stdlib semantics")
```

#### 9.3.3 v6 “node/clock_seq” access: use what’s defined, and mask what isn’t

Stdlib docs define:

* `UUID.clock_seq_hi_variant`, `UUID.clock_seq_low`, `UUID.clock_seq` as relevant to **v1 and v6** ([Python documentation][2])
* `UUID.node` is documented as “only relevant to v1” ([Python documentation][2])

**Interop reality:** v6’s layout still carries the “node” in the last 48 bits (RFC-wise), but because the stdlib docs don’t commit to `.node` semantics for v6, the most robust extraction is a mask:

```python
NODE_48 = (1 << 48) - 1

def uuid_node_48(u) -> int:
    return u.int & NODE_48

def uuid_clock_seq_14(u) -> int:
    # Defined for v1/v6 per stdlib docs
    return u.clock_seq
```

---

### 9.4 Pydantic interop (validation + JSON serialization)

#### 9.4.1 Validation: `uuid.UUID` accepts strings and bytes

Pydantic’s UUID handling is intentionally aligned with stdlib parsing:

* Strings (and bytes coerced to strings) are passed to `UUID(v)`; for `bytes`/`bytearray` there’s a fallback to `UUID(bytes=v)`. ([Django Project][3])

So if your API receives `"f81d4fae-..."` strings, you can model as `uuid.UUID` and get real UUID objects internally.

#### 9.4.2 Constraining to “must be v7” (Pydantic doesn’t ship `UUID7` as a builtin)

Pydantic ships `UUID1/3/4/5` constrained types, but not `UUID6/7/8` in the documented standard set. ([Django Project][3])

For v7 you typically add a validator:

```python
import uuid
from pydantic import BaseModel, field_validator

class Item(BaseModel):
    id: uuid.UUID

    @field_validator("id")
    @classmethod
    def must_be_v7(cls, v: uuid.UUID) -> uuid.UUID:
        if v.version != 7:
            raise ValueError("id must be UUIDv7")
        return v
```

#### 9.4.3 Serialization: Pydantic supports UUIDs in JSON mode

Pydantic provides Python-mode dumps and JSON-mode dumps; it explicitly supports **UUID objects** beyond what stdlib `json` supports, and will raise if it can’t serialize. ([Pydantic][4])

```python
m = Item(id=uuid6.uuid7())
m.model_dump()              # python mode (UUID objects may remain)
m.model_dump(mode="json")   # JSON-compatible types
m.model_dump_json()         # JSON string
```

---

### 9.5 ORM interop (SQLAlchemy + Django patterns)

#### 9.5.1 SQLAlchemy: use `types.Uuid` for cross-backend UUID columns

SQLAlchemy 2.x provides a database-agnostic `Uuid` type:

* falls back to `CHAR(32)` on backends without native UUIDs
* uses native UUID/UNIQUEIDENTIFIER-like storage when available (`native_uuid=True` default)
* expects **Python `uuid.UUID` objects by default** (`as_uuid=True` default) ([SQLAlchemy][5])

```python
import uuid
from sqlalchemy import Column, Uuid

id_col = Column("id", Uuid, primary_key=True)  # expects uuid.UUID
```

**Annotated Declarative sweet spot:** SQLAlchemy’s default type map maps `uuid.UUID` → `types.Uuid()`, so `Mapped[uuid.UUID]` works naturally. ([SQLAlchemy][6])

#### 9.5.2 SQLAlchemy: backend-specific `UUID` vs generic `Uuid`

SQLAlchemy also has a SQL-native `UUID` type (uppercase) that represents exactly the backend’s `UUID` datatype; it defaults to `as_uuid=True`. ([SQLAlchemy][5])
Use this only when you explicitly want “emit UUID type name” behavior; otherwise prefer `Uuid` for portability.

#### 9.5.3 Django: `UUIDField` normalizes to `uuid.UUID`, storage varies by backend

* Django’s **form** `UUIDField` “normalizes to: a UUID object” and accepts any string format accepted by the `UUID(...)` constructor. ([Django Project][7])
* Historically, Django’s **model** `UUIDField` stored as native `uuid` on PostgreSQL and fixed-length character field on other backends. ([Django Project][8])
* Notably, Django 5.0 changed MariaDB 10.7+ to create `UUIDField` as a `UUID` column rather than `CHAR(32)`. ([Django Project][9])

**Agent implication:** don’t assume byte-level ordering/storage is uniform across DBs; verify backend-native type and collation rules when “sortable UUID” is part of the requirement.

---

### 9.6 Serializer interop (JSON / msgpack): what gets emitted on the wire

#### 9.6.1 orjson: serializes `uuid.UUID` to canonical RFC 4122 string

orjson serializes `uuid.UUID` instances into the standard hyphenated string form (RFC 4122 format). ([GitHub][10])

```python
import orjson, uuid6
orjson.dumps(uuid6.uuid7())  # -> b'"886313e1-...-..."'
```

orjson also explicitly does **not** deserialize JSON strings back into UUID objects; schema/validation is expected to live above it (e.g., Pydantic). ([GitHub][10])

#### 9.6.2 msgspec: explicit UUID formatting controls

msgspec supports UUIDs and lets you choose encoding format via `uuid_format`:

* `canonical`: `str(uuid)` (default)
* `hex`: `uuid.hex`
* `bytes`: `uuid.bytes` (**MessagePack only**) ([jcristharif.com][11])

**Agent implication:** msgspec is a good fit when you want *binary UUIDs* on-the-wire (msgpack) while keeping `uuid.UUID` in-memory.

---

### 9.7 Stdlib parity / migration guide (Python 3.9–3.14+)

#### 9.7.1 When to prefer stdlib (Python 3.14+)

Python 3.14 adds:

* `uuid.uuid6(node=None, clock_seq=None)` (defaults: `getnode()` for node, random 14-bit clock_seq)
* `uuid.uuid7()` (48-bit timestamp + **42-bit counter** for monotonicity within a millisecond)
* `uuid.uuid8(a,b,c)` (custom blocks; warns defaults are not CSPRNG) ([Python documentation][2])

If you’re on 3.14+, prefer stdlib when you want:

* a built-in, well-documented monotonic strategy for v7 (42-bit counter) ([Python documentation][2])
* the standard v8 “custom blocks” API ([Python documentation][2])

#### 9.7.2 Where `uuid6` (third-party) is *not* equivalent to stdlib

`uuid6` (package) differs in important policy choices:

* its `uuid6()` defaults to **random 48-bit node** (stdlib defaults to `getnode()` MAC-ish) ([PyPI][1])
* its generators are documented as **not thread-safe** ([PyPI][1])
* its `uuid8()` is a **time-derived, opinionated layout**, whereas stdlib `uuid8()` is “custom blocks pseudo-random.” ([PyPI][1])

**Agent rule:** don’t silently swap `uuid6.uuid8()` → `uuid.uuid8()`; that changes the meaning of your identifiers.

#### 9.7.3 “Clean switch” recipe: one import surface, explicit semantic knobs

If you want to opportunistically use stdlib on 3.14+ but preserve the `uuid6` package’s “random node” privacy posture for v6:

```python
# compat_uuid.py
from __future__ import annotations
import secrets, uuid

try:
    import uuid6 as uuid6_pkg
except Exception:
    uuid6_pkg = None

def _random_node_48() -> int:
    node = secrets.randbits(48)
    # optional: set multicast bit to avoid MAC collision conventions
    node |= 1 << 40
    return node

def uuid6_db(node: int | None = None, clock_seq: int | None = None) -> uuid.UUID:
    node = _random_node_48() if node is None else node
    if hasattr(uuid, "uuid6"):
        return uuid.uuid6(node=node, clock_seq=clock_seq)
    if uuid6_pkg is None:
        raise RuntimeError("Need Python 3.14+ or `pip install uuid6`.")
    return uuid6_pkg.uuid6(node=node, clock_seq=clock_seq)

def uuid7_db() -> uuid.UUID:
    if hasattr(uuid, "uuid7"):
        return uuid.uuid7()
    if uuid6_pkg is None:
        raise RuntimeError("Need Python 3.14+ or `pip install uuid6`.")
    return uuid6_pkg.uuid7()
```

This keeps the *call sites* stable while making the “policy” (node source) explicit. The validation surface stays identical because everything is still `uuid.UUID`. ([Python documentation][2])

[1]: https://pypi.org/project/uuid6/?utm_source=chatgpt.com "uuid6"
[2]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"
[3]: https://docs.djangoproject.com/en/5.0/_modules/django/db/models/fields/?utm_source=chatgpt.com "Source code for django.db.models.fields"
[4]: https://docs.pydantic.dev/latest/concepts/serialization/ "Serialization - Pydantic Validation"
[5]: https://docs.sqlalchemy.org/en/latest/core/type_basics.html "
        
        
    
    The Type Hierarchy
 —
    SQLAlchemy 2.1 Documentation

        
    "
[6]: https://docs.sqlalchemy.org/en/latest/orm/declarative_tables.html "
        
        
    
    Table Configuration with Declarative
 —
    SQLAlchemy 2.1 Documentation

        
    "
[7]: https://docs.djangoproject.com/en/6.0/ref/forms/fields/?utm_source=chatgpt.com "Form fields"
[8]: https://docs.djangoproject.com/en/6.0/releases/1.8/?utm_source=chatgpt.com "Django 1.8 release notes"
[9]: https://docs.djangoproject.com/en/6.0/releases/5.0/?utm_source=chatgpt.com "Django 5.0 release notes"
[10]: https://github.com/ijl/orjson "GitHub - ijl/orjson: Fast, correct Python JSON library supporting dataclasses, datetimes, and numpy"
[11]: https://jcristharif.com/msgspec/_sources/supported-types.rst.txt?utm_source=chatgpt.com "View this page"

## 10) Testing, validation, and conformance harness

### 10.1 Harness architecture (what you’re actually building)

For `uuid6` (the third-party backport), you want **three layers** of tests, each with different “brittleness vs assurance”:

1. **Wire-format conformance (RFC-level invariants)**
   Pure black-box checks against returned `uuid.UUID` objects: variant/version bits, field decode, byte order, and basic ordering properties. RFC 9562 explicitly frames monotonicity/counters and timestamp manipulation as implementation concerns you must test in practice. ([RFC Editor][1])

2. **Deterministic vectors (golden tests)**
   Freeze time + freeze randomness so you can assert exact UUID strings/ints/bytes. CPython’s own `test_uuid.py` shows exactly this style for v6 and v7: patch `time.time_ns`, patch randomness, reset internal “last timestamp/counter” state, then check full bit slices. ([Chromium Git Repositories][2])

3. **Cross-implementation / cross-language corpus tests**
   Validate decode equivalence (timestamp / counter / node / clock_seq where defined) across implementations, **not** byte-for-byte equality (especially for v7 and v8, where the RFC allows multiple valid strategies). The `uuid6/prototypes` repo exists specifically as a multi-language implementation study, and it explicitly notes that v8 prototypes will vary. ([GitHub][3])

---

### 10.2 Spec-conformance checks (variant/version bits, byte order, field legality)

#### 10.2.1 Baseline invariants (all UUIDs)

These are the “never regress” checks. For each generated UUID `u`:

* `isinstance(u, uuid.UUID)`
* `len(u.bytes) == 16`
* `0 <= u.int < 2**128`
* `u.variant == uuid.RFC_4122` (for v6/v7/v8 outputs)
* `u.version in {6,7,8}`

CPython’s tests do this immediately for uuid7/uuid8 (variant + version). ([Chromium Git Repositories][2])

**Agent-ready snippet (pytest):**

```python
import uuid, uuid6

def assert_rfc4122(u: uuid.UUID, version: int) -> None:
    assert isinstance(u, uuid.UUID)
    assert len(u.bytes) == 16
    assert 0 <= u.int < (1 << 128)
    assert u.variant == uuid.RFC_4122
    assert u.version == version

def test_uuid7_variant_version():
    assert_rfc4122(uuid6.uuid7(), 7)

def test_uuid6_variant_version():
    assert_rfc4122(uuid6.uuid6(), 6)

def test_uuid8_variant_version():
    assert_rfc4122(uuid6.uuid8(), 8)
```

#### 10.2.2 Byte-order expectations (the silent failure mode)

Your test harness should *assert your chosen persisted representation*:

* If you store UUIDs as bytes for DB keys / interop, require **big-endian `u.bytes`**, not `bytes_le`, unless you explicitly target GUID mixed-endian conventions. (Python exposes both; mixing them breaks ordering tests.)

You can add a “round-trip identity” check:

```python
import uuid
def test_bytes_roundtrip():
    u = uuid6.uuid7()
    assert uuid.UUID(bytes=u.bytes) == u
```

---

### 10.3 Field-level conformance (v6/v7 semantics + boundary conditions)

#### 10.3.1 v6 sizing + truncation: `node` (48 bits) and `clock_seq` (14 bits)

Even if you don’t freeze time, you can still test parameter boundary behavior.

CPython’s conformance tests for uuid6 explicitly check:

* node bit length ≤ 48 and larger nodes are truncated to 48 bits
* clock_seq is stored in a 14-bit region and larger values are truncated to 14 bits (they decode with `(u.int >> 48) & 0x3fff`) ([Chromium Git Repositories][2])

**Agent-ready tests mirroring those invariants:**

```python
import uuid6

NODE_MASK = (1 << 48) - 1
CS_MASK   = (1 << 14) - 1

def get_clock_seq_14(u) -> int:
    return (u.int >> 48) & CS_MASK

def test_uuid6_node_truncation():
    big = (1 << 52) - 1
    u = uuid6.uuid6(node=big)
    assert (u.int & NODE_MASK) == (big & NODE_MASK)

def test_uuid6_clock_seq_truncation():
    big = (1 << 20) - 1
    u = uuid6.uuid6(clock_seq=big)
    assert get_clock_seq_14(u) == (big & CS_MASK)
```

#### 10.3.2 Timestamp semantics: “time” is allowed to be smeared/fuzzed

RFC 9562 explicitly allows implementations to **alter/smear/fuzz** timestamps and states there is *no requirement* that embedded time match wall-clock time closely. ([RFC Editor][1])
So your tests should be written as:

* **Structural correctness** (decode shape)
* **Ordering correctness** (relative monotonicity in your process)
* **Optional wall-clock proximity**, but tolerant to drift

---

### 10.4 Ordering properties (monotonicity), including rollback + overflow behavior

#### 10.4.1 Sequential monotonicity (single-thread)

A basic property test for “DB-key friendliness” is:

```python
def test_uuid7_is_sorted_in_generation_order():
    us = [uuid6.uuid7() for _ in range(50_000)]
    assert us == sorted(us)
```

This is exactly the style CPython uses for uuid7 monotonicity at scale (10,000). ([Chromium Git Repositories][2])

**Important constraint:** the third-party `uuid6` project documents its generators as **not thread-safe** (so monotonicity claims are fundamentally single-thread unless you add locking). ([PyPI][4])

#### 10.4.2 Same-millisecond behavior (uuid7)

RFC 9562’s monotonicity section treats “many UUIDs within a single timestamp interval” as a first-class case and describes using counters, stalling, or timestamp-advance strategies, with explicit error-handling guidance for counter overrun. ([RFC Editor][1])
Python 3.14’s stdlib `uuid.uuid7()` documents a **42-bit counter** for monotonicity within a millisecond. ([Python documentation][5])
CPython tests verify that:

* within the same millisecond, a counter is advanced and ordering holds
* if timestamps go backwards, it advances state to preserve monotonicity
* if the counter overflows, it advances the timestamp and reseeds counter ([Chromium Git Repositories][2])

**Harness guidance for `uuid6` (third-party):**

* If you can’t patch internals, treat same-ms behavior as: “no duplicates; ordering stable within a tight loop” and validate with stress tests.
* If you *can* patch (or wrap) time/rng, add deterministic tests that emulate CPython’s rollback/overflow cases.

#### 10.4.3 Threaded monotonicity (only if you enforce a critical section)

Because `uuid6` documents “not thread-safe,” you should not write correctness tests that assume monotonicity across threads unless your application wrapper enforces serialization. ([PyPI][4])

Recommended test structure:

* “raw generator is not thread-safe” → only test uniqueness probabilistically under threads
* “locked wrapper” → test monotonicity across threads using a shared lock

---

### 10.5 Golden vectors (deterministic conformance)

Golden vectors are where you check exact layout down to individual bit slices.

#### 10.5.1 v1→v6 conversion vector (stable across languages)

The `uuid6` project publishes a concrete mapping example:

* v1: `C232AB00-9414-11EC-B3C8-9E6BDECED846`
* v6: `1EC9414C-232A-6B00-B3C8-9E6BDECED846` ([GitHub][6])

This is ideal as:

* a unit test
* a cross-language corpus seed (every language should match exactly because the mapping is deterministic)

#### 10.5.2 v6 generation test vector (freeze time + params)

CPython’s v6 tests reference RFC 9562 test vectors and show the full “freeze time, set node/clock_seq, reset last-state, assert exact UUID string + bit slices + `u.time`” approach. ([Chromium Git Repositories][2])

Even if you don’t want to couple to `uuid6`’s internals, you can implement a **reference v6 encoder** and compare `uuid6.uuid6(node=..., clock_seq=...)` output under frozen time.

---

### 10.6 Uniqueness & collision sanity checks (probabilistic, but still valuable)

For UUID generation, uniqueness tests are probabilistic but extremely useful as regression tests (e.g., accidentally fixing RNG seed, reusing state after fork, broken masking).

CPython does:

* uuid7 uniqueness: generate 1000 UUIDs, ensure set size is 1000; notes uuid7 uses `os.urandom()` and emphasizes randomness quality ([Chromium Git Repositories][2])
* uuid8 uniqueness: also 1000 UUIDs; explicitly calls out 122 bits of entropy and that uniqueness assumes the underlying PRNG is “sufficiently good” ([Chromium Git Repositories][2])

You can adopt the same thresholds for `uuid6`:

```python
def test_uuid7_uniqueness_smoke():
    N = 10_000
    s = {uuid6.uuid7() for _ in range(N)}
    assert len(s) == N
```

---

### 10.7 Cross-language compatibility (what to compare, and what *not* to compare)

#### 10.7.1 The compatibility contract differs by version

* **v6**: layout is fixed; if you freeze `(timestamp, node, clock_seq)` you can demand exact byte-for-byte match (goldens).
* **v7**: timestamp location is fixed, but RFC permits different strategies for sub-ms bits/counters/randomness. Do **not** expect identical UUIDs across libraries even at the same timestamp; instead compare decoded timestamp and invariant bits. ([RFC Editor][1])
* **v8**: semantics are application-defined; the prototypes repo explicitly warns v8 prototypes vary. ([GitHub][3])

#### 10.7.2 Practical cross-language harness pattern

Use a “decode-and-compare” approach:

1. In each language, generate N UUIDs and export:

   * `hex` (canonical string)
   * `bytes` (base64)
   * decoded fields (`version`, `variant`, plus timestamp where defined)
2. In Python, validate:

   * parsing works (`uuid.UUID(hex=...)`)
   * invariant bits match (version/variant)
   * timestamp extraction matches *within stated semantics* (exact for v7 ms tick, tolerant to smearing)

PostgreSQL 18 can be used as a “foreign oracle” for v7 generation/extraction:

* `uuidv7()` generates v7 using Unix ms + sub-ms + random
* `uuid_extract_timestamp()` extracts timestamp for v1 or v7, with an explicit warning that extracted time may differ depending on the generating implementation ([PostgreSQL][7])

#### 10.7.3 Leverage the existing multi-language ecosystem

`uuid6/prototypes` is effectively a curated list of UUIDv6/v7/v8 implementations across languages (Python, Go, Rust, Java, PHP, etc.) designed for comparative testing, and explicitly calls out RFC alignment tracking and v8 variability. ([GitHub][3])

---

### 10.8 Recommended file layout (agent-friendly)

If you’re building this as part of the `uuid6` technical doc + repo harness, a clean split is:

* `tests/test_rfc_invariants.py`
  Variant/version/bytes/int invariants; zero dependency on time-freezing.
* `tests/test_ordering_properties.py`
  Monotonicity loops; “locked wrapper” monotonicity if you require it.
* `tests/test_vectors_v1_to_v6.py`
  Deterministic mapping vectors (including the published example). ([GitHub][6])
* `tests/test_vectors_v6_v7_deterministic.py`
  Freeze time + RNG (if your generator supports injection) and assert exact bit slices (modeled after CPython’s own tests). ([Chromium Git Repositories][2])
* `tests/interop/`
  Cross-language corpus importer + decoder comparisons (v6 strict; v7/v8 decode-based).

If you want, the next deep dive can be **Section 11 (“Operational footguns + production checklist”)**, but written so it directly maps to “tests you must have” and “metrics/alerts you must log” for UUID generation in services.

[1]: https://www.rfc-editor.org/rfc/rfc9562.html "RFC 9562: Universally Unique IDentifiers (UUIDs)"
[2]: https://chromium.googlesource.com/external/github.com/python/cpython/%2B/refs/tags/v3.14.2/Lib/test/test_uuid.py "Lib/test/test_uuid.py - external/github.com/python/cpython - Git at Google"
[3]: https://github.com/uuid6/prototypes "GitHub - uuid6/prototypes: Draft Prototypes and Tests for UUIDv6 and beyond"
[4]: https://pypi.org/project/uuid6/?utm_source=chatgpt.com "uuid6"
[5]: https://docs.python.org/3/library/uuid.html?utm_source=chatgpt.com "uuid — UUID objects according to RFC 9562 — Python 3.14.2 ..."
[6]: https://github.com/oittaa/uuid6-python?utm_source=chatgpt.com "oittaa/uuid6-python: New time-based UUID formats which ..."
[7]: https://www.postgresql.org/docs/current/functions-uuid.html "PostgreSQL: Documentation: 18: 9.14. UUID Functions"

## 11) Operational footguns and “production checklist”

### 11.1 UUIDs are identifiers, not secrets (capability-token footgun)

RFC 9562 is explicit: implementations **SHOULD NOT assume UUIDs are hard to guess** and **MUST NOT use them as security capabilities** (i.e., “possession grants access”). It also calls out that predictability in the random source becomes a vulnerability. ([RFC Editor][1])

It further notes that embedded timestamps/counters leak creation ordering, and if UUIDs are involved in any **security operation**, then **UUIDv4 SHOULD be used**. ([RFC Editor][1])

**Agent rule of thumb**

* **Internal DB primary keys:** v7 is often a great default.
* **External/public IDs (URLs, invites, password resets, auth artifacts):** use a CSPRNG-backed token (often `uuid4()` at minimum; frequently longer tokens). Python documents `uuid4()` as cryptographically secure, while `uuid1()` may compromise privacy. ([Python documentation][2])

**Concrete split (Python)**

```python
import uuid

def public_id() -> str:
    # External-facing: treat as secret-adjacent; use CSPRNG UUIDv4 at minimum.
    return str(uuid.uuid4())

def db_id_v7() -> uuid.UUID:
    # Internal key: time-ordered, DB-locality-friendly (prefer stdlib uuid7 on 3.14+).
    return uuid.uuid7() if hasattr(uuid, "uuid7") else __import__("uuid6").uuid7()
```

(If you truly need “unguessable capability,” don’t stop at UUID length—use dedicated token patterns.)

---

### 11.2 Time-ordered UUIDs leak *time* and *order* (privacy + correlation footguns)

Time-ordered UUIDs (v6/v7 and time-derived v8 schemes) embed timestamps by design. RFC 9562 acknowledges timestamps/counters reveal creation order and are a (small) attack surface; it recommends v4 when security sensitivity is involved. ([RFC Editor][1])

**Where this bites in practice**

* **User correlation:** exposing IDs externally can leak approximate creation time and event rate (“how many objects per second”) and enable linkage across logs/requests.
* **Guessability drift:** even if not strictly guessable, “structure” can help attackers reduce search space or infer system behavior. RFC’s guidance is to *not treat UUIDs as hard-to-guess*. ([RFC Editor][1])

**Mitigations**

* Keep **time-ordered IDs internal**; issue a separate public identifier (uuid4 or other token).
* If you must expose time-ordered IDs, treat them as *metadata-bearing* and assume they leak ordering.

---

### 11.3 MAC/node leakage: v1/v6 can carry stable machine identifiers

RFC 9562 calls out privacy/network-security issues from embedding MAC addresses in v1’s node field and states MAC addresses **SHOULD NOT be used within a UUID**; instead prefer CSPRNG-derived data. ([RFC Editor][1])

Python echoes this: `uuid1()` may compromise privacy since it contains the machine network address. ([Python documentation][2])

**v6-specific operational caveat**

* **stdlib `uuid.uuid6()` (Python 3.14+) defaults node to `getnode()` (MAC when possible)**. ([Python documentation][2])
* The **third-party `uuid6` package defaults `node` to random 48-bit** and `clock_seq` to random 14-bit (more privacy-friendly by default). ([PyPI][3])

**Agent policy choice**

* If you generate v6 at all, decide explicitly whether `node` is:

  * stable host identity (often a privacy risk), or
  * random per-process / per-call (preferred if you’re avoiding linkability).

---

### 11.4 When *not* to use v6 (prefer v7 unless you have legacy v1)

Both RFC-aligned guidance and the `uuid6` package documentation are blunt: v6 is expected primarily for contexts with existing v1 UUIDs; systems without legacy v1 **SHOULD use v7 instead**. ([PyPI][3])

**Practical implications**

* If you don’t need v1 continuity (Gregorian epoch timestamp semantics, v1-like decoding, migration), v7 is the operational default.
* If you do need continuity, consider converting v1→v6 (`uuid1_to_uuid6`) and stop producing new v1s.

---

### 11.5 Hot partitions / hotspots: time-order helps B-trees but can hurt range-sharded stores

Time-ordered keys improve locality in **B-tree-like indexes**, but they can be disastrous as *leading keys* in distributed systems that partition by key ranges.

Authoritative examples:

* Bigtable warns that row keys **starting with a timestamp** cause sequential writes to hit a single node (“hotspot”); it recommends prefixing with a high-cardinality value (like user ID). ([Google Cloud Documentation][4])
* AWS DynamoDB scaling guidance warns that loading **sequential data** (sorted by partition key) can create a “rolling hot partition.” ([Amazon Web Services, Inc.][5])

**Agent design pattern**

* Use **(tenant_id, uuidv7)** or **hash_prefix(uuidv7) + uuidv7** depending on your datastore’s partitioning model:

  * *Range-partitioned / lexicographically sorted stores:* add a high-cardinality prefix.
  * *Single-node RDBMS B-tree indexes:* raw v7 often helps.

---

### 11.6 Entropy, seeding, and RNG posture (don’t accidentally downgrade security)

RFC 9562: avoid MAC-based node IDs and prefer **CSPRNG data with sufficient entropy**; don’t treat UUIDs as unguessable. ([RFC Editor][1])

Python’s uuid docs provide a crisp operational distinction:

* `uuid4()` is generated in a **cryptographically-secure method**. ([Python documentation][2])
* `uuid8()` (stdlib) is **pseudo-random by default and not CSPRNG**; Python explicitly says to use `uuid4()` in security-sensitive contexts. ([Python documentation][2])

**If you use stdlib `uuid.uuid8()`: always supply CSPRNG bits**

```python
import secrets, uuid

def uuid8_cs() -> uuid.UUID:
    a = secrets.randbits(48)
    b = secrets.randbits(12)
    c = secrets.randbits(62)
    return uuid.uuid8(a, b, c)  # avoids non-CSPRNG defaults
```

(Still: if the UUID participates in security decisions, RFC steers you toward v4 anyway.) ([RFC Editor][1])

---

### 11.7 Clock behavior, rollback, and “timestamp truthiness”

RFC 9562 explicitly allows implementations to **alter timestamps** (for clock correction, leap second handling, performance tricks, etc.) and makes **no guarantee** that embedded time equals wall-clock time. ([RFC Editor][1])

Databases are starting to surface this explicitly: PostgreSQL’s `uuid_extract_timestamp()` warns that the extracted timestamp is **not necessarily exactly equal** to the time the UUID was generated, depending on the generating implementation. ([PostgreSQL][6])

**Operational guidance**

* Treat embedded time as an **ordering signal**, not an audit clock.
* If you need reliable event time, store a separate `created_at` column.

---

### 11.8 Concurrency footguns: thread-safety and deterministic ordering

The `uuid6` (third-party) package documents its generators as **not thread-safe**. ([PyPI][3])

**Production checklist**

* If you rely on *monotonic ordering* (`u[i] < u[i+1]`) or you want a single global generator semantics inside a process: wrap calls with a lock.
* If you rely only on uniqueness: concurrency is usually fine, but still validate (stress tests, collision checks).

---

## 11.9 Production checklist (agent-ready)

**Version selection**

* ☐ Default to **v7** unless you have legacy v1 continuity needs. ([PyPI][3])
* ☐ Avoid v6 unless you’re migrating/interoping with v1. ([PyPI][3])
* ☐ Treat v8 as “custom protocol”; stdlib v8 defaults are not CSPRNG. ([Python documentation][2])

**Security/privacy**

* ☐ Do **not** use UUIDs as capability tokens; use v4 or dedicated tokens. ([RFC Editor][1])
* ☐ Do **not** embed MAC addresses (avoid `getnode()`-derived node for externally visible IDs). ([RFC Editor][1])

**Storage & ordering**

* ☐ Persist as **16-byte big-endian** (`UUID.bytes`) if you care about ordering portability. ([Python documentation][2])
* ☐ If using range-sharded stores (Bigtable/DynamoDB-like), avoid pure timestamp-leading keys—prefix/salt to prevent hotspots. ([Google Cloud Documentation][4])

**RNG/entropy**

* ☐ Use CSPRNG sources (`uuid4`, `secrets`) for any security-adjacent usage. ([RFC Editor][1])
* ☐ If you must use stdlib `uuid8`, provide your own CSPRNG blocks. ([Python documentation][2])

**Observability**

* ☐ Log: uuid version, generator (stdlib vs uuid6), whether generation is locked/serialized, and how you source `node` for v6. ([PyPI][3])
* ☐ Metrics: monotonicity violations (if required), duplicate detection (sampled), and drift between embedded timestamp and wall clock (for v7). ([RFC Editor][1])

[1]: https://www.rfc-editor.org/rfc/rfc9562.html "RFC 9562: Universally Unique IDentifiers (UUIDs)"
[2]: https://docs.python.org/3/library/uuid.html "uuid — UUID objects according to RFC 9562 — Python 3.14.2 documentation"
[3]: https://pypi.org/project/uuid6/?utm_source=chatgpt.com "uuid6"
[4]: https://docs.cloud.google.com/bigtable/docs/schema-design?utm_source=chatgpt.com "Schema design best practices | Bigtable"
[5]: https://aws.amazon.com/blogs/database/part-3-scaling-dynamodb-how-partitions-hot-keys-and-split-for-heat-impact-performance/?utm_source=chatgpt.com "Scaling DynamoDB: How partitions, hot keys, and split for ..."
[6]: https://www.postgresql.org/docs/current/functions-uuid.html?utm_source=chatgpt.com "Documentation: 18: 9.14. UUID Functions"
