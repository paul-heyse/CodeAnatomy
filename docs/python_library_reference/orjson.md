Below is a **clustered feature catalog** for **orjson**, organized in the same “surface area → knobs → failure modes” spirit as your PyArrow reference. 

---

# orjson — comprehensive feature catalog (clustered)

## 0) Mental model and explicit boundaries

* **Two core entrypoints**: `orjson.dumps(obj, default=..., option=...) -> bytes` and `orjson.loads(obj: bytes|bytearray|memoryview|str) -> Any`. ([GitHub][1])
* **Always UTF-8 bytes on output** (not `str`), and **strict UTF-8** on input/output (errors on invalid UTF-8 / surrogate abuse). ([GitHub][1])
* **No file IO / no JSONL helpers**: reading/writing files, line-delimited JSON, streaming helpers are intentionally *not* part of the library. ([GitHub][1])

---

## 1) Public API surface (what exists as importable surface area)

### 1.1 Functions

* `orjson.dumps(__obj, default: Callable | None = ..., option: int | None = ...) -> bytes` (serialization). ([GitHub][1])
* `orjson.loads(__obj: bytes | bytearray | memoryview | str) -> Any` (deserialization). ([PyPI][2])

### 1.2 Types

* `orjson.Fragment(bytes|str)` — splice already-serialized JSON into output without re-parsing. ([GitHub][1])

### 1.3 Exceptions (stdlib-compat semantics)

* `orjson.JSONEncodeError` (subclass of `TypeError`; can chain `default` exceptions via `__cause__`). ([GitHub][1])
* `orjson.JSONDecodeError` (subclass of `json.JSONDecodeError` and `ValueError`). ([PyPI][2])

### 1.4 Option flags (bitmask constants)

* All behavior switches live under `orjson.OPT_*` and are combined via bitwise-OR (e.g., `OPT_STRICT_INTEGER | OPT_NAIVE_UTC`). ([GitHub][1])

---

## 2) Serialization (`dumps`) — native type support + extension point

### 2.1 Native serialization matrix (built-ins + “batteries included” types)

* Natively serializes: `str`, `dict`, `list`, `tuple`, `int`, `float`, `bool`, `None`, `dataclasses.dataclass`, `typing.TypedDict`, `datetime.datetime/date/time`, `uuid.UUID`, `numpy.ndarray`, `orjson.Fragment`. ([GitHub][1])
* Also serializes **subclasses** of `str/int/dict/list/dataclass` and `enum.Enum` by default; **does not** serialize subclasses of `tuple` (avoids `namedtuple` being treated as array). ([GitHub][1])

### 2.2 Extensibility: `default=` hook (arbitrary type adapter)

* `default(obj)` must return a type that orjson already knows how to serialize (or something that eventually reduces to that); raise an exception (commonly `TypeError`) when you don’t handle a type. ([GitHub][1])
* Failure modes / guardrails:

  * non-serializable type → `JSONEncodeError`
  * circular refs → `JSONEncodeError`
  * `default` recursion > 254 levels → `JSONEncodeError`
  * invalid UTF-8 in a `str` → `JSONEncodeError`
  * unsupported `tzinfo` on datetime → `JSONEncodeError` ([GitHub][1])

### 2.3 Runtime behavior constraints

* The **GIL is held** for the duration of `dumps()`. ([GitHub][1])

---

## 3) Serialization options (`option=`) — clustered knobs

### 3.1 Output framing / formatting

* `OPT_APPEND_NEWLINE`: append `\n` without extra copying overhead. ([GitHub][1])
* `OPT_INDENT_2`: pretty-print with **indent=2 only** (no other indentation levels). ([GitHub][1])

### 3.2 Determinism / hashing / tests

* `OPT_SORT_KEYS`: deterministic key order for `dict` output (significant perf cost; not locale-aware). ([GitHub][1])

### 3.3 Dict key policy (non-string keys)

* `OPT_NON_STR_KEYS`: allow non-`str` keys (supports key types like `int/float/bool/None/datetime/date/time/enum/uuid`).

  * Footgun: can create **duplicate keys after stringification**; last key wins under typical parsers. ([GitHub][1])

### 3.4 Datetime policy knobs (representation control)

* `OPT_NAIVE_UTC`: treat naive `datetime` as UTC (adds `+00:00`). ([GitHub][1])
* `OPT_UTC_Z`: print UTC zone as `Z` instead of `+00:00`. ([GitHub][1])
* `OPT_OMIT_MICROSECONDS`: omit the `microsecond` field for `datetime` and `time`. ([GitHub][1])

### 3.5 “Passthrough” knobs (force `default` to run even for natively-supported objects)

Use these when you want **custom formatting**, at the cost of performance and with stricter “must be handled by default” semantics:

* `OPT_PASSTHROUGH_DATETIME`: route `datetime/date/time` into `default` (e.g., HTTP-date format). ([GitHub][1])
* `OPT_PASSTHROUGH_DATACLASS`: route dataclasses into `default` (explicitly documented as much slower). ([GitHub][1])
* `OPT_PASSTHROUGH_SUBCLASS`: route subclasses of builtin types into `default` (lets you mask/redact subclass payloads). ([GitHub][1])

### 3.6 Integer safety policy

* `OPT_STRICT_INTEGER`: enforce 53-bit integer limit (for JS/browser compatibility) instead of default 64-bit behavior. ([GitHub][1])

### 3.7 Numpy enablement (opt-in)

* `OPT_SERIALIZE_NUMPY`: enable numpy ndarray/scalar serialization. ([GitHub][1])

### 3.8 Deprecated/compat-only flags

* `OPT_SERIALIZE_DATACLASS` and `OPT_SERIALIZE_UUID`: deprecated/no effect in v3 (kept for historical compatibility). ([GitHub][1])

---

## 4) `Fragment` — embed pre-serialized JSON blobs

* Purpose: include JSON already encoded elsewhere (cache/JSONB/etc.) **without** `loads()` roundtrip. ([GitHub][1])
* Properties:

  * **No reformatting**: `OPT_INDENT_2` doesn’t rewrite fragment formatting. ([GitHub][1])
  * Input must be `bytes` or `str`. ([GitHub][1])
* Footguns (intentional power tool):

  * Fragment does **no JSON validation**; you can emit invalid JSON; it also **does not escape characters**. ([GitHub][1])

---

## 5) Type-specific semantics (high-leverage “specials”)

### 5.1 `dataclasses.dataclass`

* Native serialization; serializes as a JSON object with fields in class-definition order; supports `__slots__`, frozen dataclasses, optional/default attrs, subclasses (with perf note about `__slots__`). ([GitHub][1])
* Customization: requires `OPT_PASSTHROUGH_DATACLASS` + `default`. ([GitHub][1])

### 5.2 `datetime` / `date` / `time`

* Datetimes serialize to **RFC 3339** strings; timezone errors raise `JSONEncodeError`. ([PyPI][2])
* Controls: `OPT_PASSTHROUGH_DATETIME`, `OPT_NAIVE_UTC`, `OPT_UTC_Z`, `OPT_OMIT_MICROSECONDS`. ([PyPI][2])

### 5.3 `enum.Enum`

* Serialized natively; options apply to the underlying value; unsupported member values can be handled via `default`. ([PyPI][2])

### 5.4 `float`

* Double-precision round-tripping and consistent rounding; **NaN/±Infinity serialize to `null`** (note: that’s intentionally non-compliant JSON behavior vs emitting `NaN`). ([PyPI][2])

### 5.5 `int`

* Default: 64-bit integer round-trip range; `OPT_STRICT_INTEGER` enforces 53-bit maximum and raises `JSONEncodeError` outside that range. ([PyPI][2])

### 5.6 `numpy` (opt-in, deep surface)

* Native support for `numpy.ndarray` plus listed scalar dtypes (float16/32/64, signed/unsigned ints, `datetime64`, bool, etc.); requires `OPT_SERIALIZE_NUMPY`. ([PyPI][2])
* Array constraints + fallbacks:

  * array must be **C_CONTIGUOUS** and supported dtype; unsupported cases can “fall through” to `default` (where you might do `obj.tolist()`). ([PyPI][2])
  * endianness mismatch or malformed arrays raise `JSONEncodeError`. ([PyPI][2])
  * dtype nuance: native `numpy.float32` path can round differently than `tolist()` because `tolist()` converts to double first. ([PyPI][2])
  * `numpy.datetime64` serializes as RFC 3339; datetime options affect it. ([PyPI][2])

### 5.7 `str` (UTF-8 correctness)

* Strict UTF-8: errors on surrogate abuse; guidance suggests decoding bytes with replacement/lossy if you need “best-effort” recovery. ([PyPI][2])

### 5.8 `uuid.UUID`

* Serializes to RFC 4122 string form. ([PyPI][2])

---

## 6) Deserialization (`loads`) — strict JSON parsing + memory/perf knobs

### 6.1 Input types and output types

* Accepts `bytes`, `bytearray`, `memoryview`, `str`; recommends passing bytes-like objects directly to avoid extra decoding overhead. ([PyPI][2])
* Produces only: `dict`, `list`, `int`, `float`, `str`, `bool`, `None`. ([PyPI][2])

### 6.2 Strictness, limits, and exceptions

* Input must be valid UTF-8; invalid UTF-8 → `JSONDecodeError`. ([PyPI][2])
* Rejects invalid JSON values `NaN`, `Infinity`, `-Infinity` (stdlib `json` accepts them). ([PyPI][2])
* Recursion limit: errors if arrays/objects nest to 1024 levels. ([PyPI][2])
* Can raise `JSONDecodeError` on buffer allocation failure for parsing. ([PyPI][2])

### 6.3 Process-level key cache (memory optimization)

* Maintains a **map-key cache** for the duration of the process (keys ≤ 64 bytes; 2048 entries), reducing duplicate string allocation/memory. ([PyPI][2])

### 6.4 Runtime behavior constraint

* The **GIL is held** for the duration of `loads()`. ([PyPI][2])

---

## 7) Packaging, platform, and “ops” characteristics

* CPython-only support (no PyPy / no Android/iOS embedded builds / no PEP 554 subinterpreters); versions listed up through Python 3.15 in docs. ([GitHub][1])
* Wheels across common OS/arch; amd64 wheels can use AVX-512 at runtime for performance. ([GitHub][1])
* SemVer stance: “serializing a new object type without an opt-in flag is considered a breaking change” (important for long-lived contract systems). ([GitHub][1])

---

## 8) Explicit non-features / “won’t do”

* No file IO helpers; no JSONL streaming utilities. ([GitHub][1])
* No `indent` levels besides 2; no `ensure_ascii`-style ASCII escaping (UTF-8 output is the model). ([GitHub][1])

---

If you want the next increment (analogous to the PyArrow doc): I can expand each cluster into a **“deep dive page”** (power knobs + failure modes + minimal snippets) for (1) option composition recipes, (2) numpy edge cases + fallbacks, and (3) strictness/compatibility guidance for “drop-in replacement” migrations.

[1]: https://github.com/ijl/orjson "GitHub - ijl/orjson: Fast, correct Python JSON library supporting dataclasses, datetimes, and numpy"
[2]: https://pypi.org/project/orjson/ "orjson · PyPI"

## B) Option composition recipes — deep dive (orjson v3.x)

### Mental model: `option` is a **bitmask-encoded serialization policy**, not “pretty print on/off”

`orjson.dumps(obj, option=...)` takes an integer bitmask; combine flags with `|` (e.g. `OPT_STRICT_INTEGER | OPT_NAIVE_UTC`). ([GitHub][1])
Treat this bitmask as part of your *contract boundary*: once you ship bytes to another system, **policy drift** (ints, datetimes, key handling) becomes an interoperability incident.

---

# B1) Option surface area (knobs) + composition semantics

## B1.1 Base behaviors to remember (before any flags)

* `dumps()` returns **UTF-8 `bytes`** (not `str`). ([GitHub][1])
* `dumps()` holds the **GIL** for the duration. ([GitHub][1])
* Default native types include dataclasses/datetime/numpy/UUID/etc.; `default=` is only for the remainder (unless you force passthrough). ([GitHub][1])
* Failure modes are explicit: unsupported type, invalid UTF-8 in `str`, non-str dict keys (unless enabled), circular refs, unsupported tzinfo → `JSONEncodeError`. ([GitHub][1])

## B1.2 Formatting / framing knobs

* `OPT_APPEND_NEWLINE`: append `\n` efficiently (avoid `dumps(x) + b"\n"` copying). ([GitHub][1])
* `OPT_INDENT_2`: pretty-print with indent=2 (only indent level supported). Compatible with all other options. ([GitHub][1])

## B1.3 Determinism knobs

* `OPT_SORT_KEYS`: sorted dict keys for deterministic output (hashing/tests), **substantial perf penalty**, not locale-aware. ([GitHub][1])

## B1.4 Key policy knobs

* `OPT_NON_STR_KEYS`: allow certain non-`str` keys; warns about **duplicate-key collisions** after stringification (last wins) and unstable sorting with duplicates. ([GitHub][1])

## B1.5 Datetime representation knobs

* `OPT_NAIVE_UTC`: treat naïve `datetime` as UTC (add `+00:00`). ([GitHub][1])
* `OPT_UTC_Z`: serialize UTC timezone as `Z` instead of `+00:00`. ([GitHub][1])
* `OPT_OMIT_MICROSECONDS`: drop microseconds on `datetime`/`time`. ([GitHub][1])

## B1.6 Pass-through knobs (force `default=` to run)

These are “turn off native support so I can own formatting/redaction”.

* `OPT_PASSTHROUGH_DATACLASS`: dataclasses go to `default` (explicitly slower). ([GitHub][1])
* `OPT_PASSTHROUGH_DATETIME`: datetime/date/time go to `default` (e.g., HTTP-date formatting). ([GitHub][1])
* `OPT_PASSTHROUGH_SUBCLASS`: subclasses of builtins go to `default` (e.g., redact `Secret(str)`). ([GitHub][1])

## B1.7 Integer safety knob (interop with JS/browsers)

* `OPT_STRICT_INTEGER`: enforce 53-bit integer max; otherwise orjson supports 64-bit range. ([GitHub][1])

## B1.8 Numpy knob (opt-in)

* `OPT_SERIALIZE_NUMPY`: required to serialize numpy arrays/scalars natively. ([GitHub][1])

---

# B2) “Recipe” presets (copy/paste bitmasks)

## B2.1 Compact API payloads (default-ish, but explicit)

When you want a stable “default contract” across services:

* Keep compact output (default), no sorting, no indentation.
* Add only contract-critical datetime/int controls.

```python
API_OPTS = (
    orjson.OPT_NAIVE_UTC     # if you want naive datetimes normalized
    | orjson.OPT_UTC_Z       # if you prefer "Z"
    | orjson.OPT_OMIT_MICROSECONDS  # if you want second-granularity
)
out = orjson.dumps(obj, option=API_OPTS)
```

Datetime knobs are separate (you can choose none / some). ([GitHub][1])

## B2.2 JSONL / log lines (zero-copy newline append)

```python
LINE_OPTS = orjson.OPT_APPEND_NEWLINE
fp.write(orjson.dumps(event, option=LINE_OPTS))   # open file in "wb"
```

`OPT_APPEND_NEWLINE` exists specifically to avoid the copy you’d get from `dumps()+b"\n"`. ([GitHub][1])

## B2.3 Deterministic bytes for hashing + golden tests

```python
TEST_OPTS = orjson.OPT_SORT_KEYS
blob = orjson.dumps(obj, option=TEST_OPTS)
```

`OPT_SORT_KEYS` is the supported “stable order” mechanism, with an explicit performance penalty. ([GitHub][1])

If you also allow non-string keys:

```python
TEST_OPTS = orjson.OPT_NON_STR_KEYS | orjson.OPT_SORT_KEYS
```

…but beware: non-str keys can collide after stringification; sorting becomes unpredictable with duplicates. ([GitHub][1])

## B2.4 Browser-safe numeric contract (JS 53-bit)

```python
WEB_OPTS = orjson.OPT_STRICT_INTEGER
payload = orjson.dumps(obj, option=WEB_OPTS)
```

Raises if an integer exceeds 53-bit range (instead of silently emitting an unrepresentable integer for JS). ([GitHub][1])

## B2.5 “Own the formatting” (passthrough + default)

### Dataclasses: selective field export / redaction

```python
OPTS = orjson.OPT_PASSTHROUGH_DATACLASS

def default(o):
    if isinstance(o, User):
        return {"id": o.id, "name": o.name}  # omit password, etc.
    raise TypeError

out = orjson.dumps(user, option=OPTS, default=default)
```

Without `OPT_PASSTHROUGH_DATACLASS`, dataclasses serialize natively and your `default` won’t run for them; with it, serialization requires `default` and is slower. ([GitHub][1])

### Datetimes: custom format (HTTP-date, epoch millis, etc.)

```python
OPTS = orjson.OPT_PASSTHROUGH_DATETIME

def default(o):
    if isinstance(o, datetime.datetime):
        return o.strftime("%a, %d %b %Y %H:%M:%S GMT")
    raise TypeError

out = orjson.dumps({"created_at": dt}, option=OPTS, default=default)
```

`OPT_PASSTHROUGH_DATETIME` forces datetime/date/time to go through `default`. ([GitHub][1])

### Subclasses: safe redaction boundary

```python
class Secret(str): ...

OPTS = orjson.OPT_PASSTHROUGH_SUBCLASS

def default(o):
    if isinstance(o, Secret):
        return "******"
    raise TypeError
```

Subclasses of `str/int/dict/list` serialize by default in v3; `OPT_PASSTHROUGH_SUBCLASS` disables that and routes subclasses to `default`. ([GitHub][1])

## B2.6 Numpy payloads: explicit opt-in

```python
OPTS = orjson.OPT_SERIALIZE_NUMPY
out = orjson.dumps(arr, option=OPTS)
```

Numpy serialization is native but gated behind `OPT_SERIALIZE_NUMPY`. ([GitHub][1])

---

# B3) Failure modes (the ones that bite in production)

## B3.1 “default forgot to raise” ⇒ silent `null`

If your `default()` returns `None` implicitly, it serializes as JSON `null` (same pitfall exists with stdlib `json`). ([GitHub][1])

## B3.2 Non-str keys without OPT_NON_STR_KEYS ⇒ hard failure

`dict` keys must be `str` unless you enable `OPT_NON_STR_KEYS`. ([GitHub][1])

## B3.3 Recursion and circular refs

* circular references raise `JSONEncodeError`. ([GitHub][1])
* `default` recursion is bounded (254). ([GitHub][1])

## B3.4 UTF-8 strictness

Invalid UTF-8 / surrogate abuse raises on both dumps and loads. ([GitHub][1])

---

---

## C) Numpy edge cases + fallbacks — deep dive (orjson v3.x)

### Mental model: **fast path** is “C-contiguous + supported dtype + native endian”

orjson natively serializes `numpy.ndarray` and a specific set of numpy scalar types, but you must opt in (`OPT_SERIALIZE_NUMPY`). ([GitHub][1])
The ndarray fast-path requires:

* C-contiguous (`C_CONTIGUOUS`)
* supported dtype
* supported `numpy.datetime64` unit (unsupported examples include picoseconds)
* native endianness (otherwise hard error). ([GitHub][1])

---

# C1) Supported numpy surface area (what “just works”)

## C1.1 Native scalar dtypes and ndarray

orjson documents native support for:

* `numpy.ndarray`
* scalars: `float64/32/16`, `int64/32/16/8`, `uint64/32/16/8`, `uintp/intp`, `datetime64`, `bool` ([GitHub][1])

## C1.2 Compatibility promise

* Compatible with numpy v1 and v2. ([GitHub][1])
* No install-time dependency on numpy; it reads ndarrays via `PyArrayInterface`. ([GitHub][1])

---

# C2) Fast-path requirements (and how they fail)

## C2.1 Non-contiguous arrays

If the array isn’t C-contiguous, or has unsupported dtype, or unsupported `datetime64` representation, **orjson falls through to `default`** (i.e., you can decide what to do). ([GitHub][1])

### Minimal fallback snippet

```python
OPTS = orjson.OPT_SERIALIZE_NUMPY

def default(o):
    if isinstance(o, np.ndarray):
        return o.tolist()  # slow, but universal
    raise TypeError

out = orjson.dumps(arr, option=OPTS, default=default)
```

The recommended fallback inside `default` is `obj.tolist()`. ([GitHub][1])

## C2.2 Non-native endianness

If an array isn’t in native endianness, `JSONEncodeError` is raised. ([GitHub][1])

**Practical fix pattern:** normalize to native-endian before dumping (exact mechanics depend on dtype; in numpy this is typically done by casting to a native-byteorder dtype).

## C2.3 Malformed arrays

Malformed ndarray input can raise `JSONEncodeError`. ([GitHub][1])

---

# C3) A “normalize-for-orjson” pipeline (robust producer pattern)

Use this when arrays come from arbitrary upstream transformations (views, slices, dtype churn).

### C3.1 Ensure contiguous memory

* Prefer converting once upstream (to avoid repeated copies per `dumps()` call).
* Typical approach: materialize `np.ascontiguousarray(arr)` when `arr.flags["C_CONTIGUOUS"]` is false.

### C3.2 Ensure supported dtype

* If your dtype is object/structured/complex/decimal/etc., decide:

  * **semantic conversion** (e.g., decimals to strings), or
  * **lossy numeric cast**, or
  * **tolist fallback** via `default`.
    The “fall through to default” mechanism is the intended escape hatch. ([GitHub][1])

### C3.3 Handle datetime64 units

Unsupported `datetime64` units fall through to `default`. ([GitHub][1])
If you need stable datetime JSON, consider converting to `datetime64[ms]` (or to Python `datetime`) upstream as a contract decision.

---

# C4) Performance/memory tradeoffs (why you should avoid `tolist()` unless forced)

The docs benchmark native ndarray serialization vs `json` doing `ndarray.tolist()` via `default`, showing large latency and RSS deltas in favor of native serialization. ([GitHub][1])
So the typical best practice is:

* Use `OPT_SERIALIZE_NUMPY` for the common supported path.
* Add `default` only as a fallback for edge cases (non-contig, unsupported dtype/unit), rather than always forcing tolist.

---

---

## D) Strictness & compatibility guidance — deep dive (drop-in migrations)

### Mental model: orjson is *intentionally stricter* than stdlib `json` in three places

1. **output type** (`bytes` vs `str`), 2) **character correctness** (UTF-8/surrogates), 3) **numbers/JSON compliance** (NaN/Infinity tokens). ([GitHub][1])

---

# D1) API delta map (`json` → `orjson`)

## D1.1 Output type: bytes vs str

* `json.dumps(...) -> str`
* `orjson.dumps(...) -> bytes` ([GitHub][1])

**Drop-in wrapper (for code that expects `str`):**

```python
def dumps_str(obj, *, default=None, option=0) -> str:
    return orjson.dumps(obj, default=default, option=option).decode("utf-8")
```

## D1.2 “file-like object” helpers are absent

stdlib supports `json.dump(fp=...)` / `json.load(fp=...)` patterns; orjson explicitly does not provide file IO helpers. ([GitHub][1])
Migration pattern: `fp.write(orjson.dumps(obj))` with binary file handles.

---

# D2) Parameter mapping (when replacing stdlib knobs)

## D2.1 `sort_keys=True` → `OPT_SORT_KEYS`

orjson explicitly maps `sort_keys` to `OPT_SORT_KEYS`. ([GitHub][1])

## D2.2 `indent=2` → `OPT_INDENT_2` (and only 2)

orjson supports indent only via `OPT_INDENT_2`; other indentation levels aren’t supported. ([GitHub][1])

## D2.3 `ensure_ascii=True/False` has no analogue

stdlib escapes non-ASCII by default (`ensure_ascii=True`). ([Python documentation][2])
orjson outputs UTF-8 bytes and “ASCII-escaping” is not available; its migration notes call `ensure_ascii` largely irrelevant and say UTF-8 chars cannot be escaped to ASCII. ([GitHub][1])

## D2.4 `default=` exists, but be strict about raising

Both expect `default` to raise `TypeError` (or similar) when unhandled; orjson warns that implicit `None` becomes `null`. ([GitHub][1])

## D2.5 `skipkeys=True` has no orjson equivalent

stdlib can skip bad keys; by default it raises if keys aren’t scalar types. ([Python documentation][2])
orjson: either keep str keys, or explicitly enable `OPT_NON_STR_KEYS` (with collision caveats). ([GitHub][1])

---

# D3) Behavioral deltas you must audit

## D3.1 UTF / surrogate handling (very common migration break)

stdlib will serialize/deserialize UTF-16 surrogate code points in strings; orjson rejects them as invalid UTF-8 and raises. ([GitHub][1])

**Best-effort ingest strategy (when you don’t control input):**

* decode bytes with `errors="replace"` before `orjson.loads`, as documented. ([GitHub][1])

## D3.2 NaN/Infinity tokens (strict JSON)

stdlib decoder “understands” `NaN/Infinity/-Infinity` tokens; encoder defaults `allow_nan=True`. ([Python documentation][2])
orjson:

* `loads()` rejects those tokens as invalid JSON. ([GitHub][1])
* `dumps()` will serialize IEEE NaN/Inf float values as `null` (semantic loss, but produces valid JSON). ([GitHub][1])

## D3.3 Byte encodings in input

stdlib can accept bytes in UTF-8/16/32; orjson requires valid UTF-8. ([Python documentation][2])

## D3.4 Dict keys

stdlib encoder accepts scalar non-str keys (`int/float/bool/None`) by default (and can skip others); orjson default requires str keys unless you opt into `OPT_NON_STR_KEYS`. ([Python documentation][2])

## D3.5 Subclass serialization (v3 behavior)

orjson v3 serializes subclasses of common builtins by default; migration notes highlight this and provide `OPT_PASSTHROUGH_SUBCLASS` to disable. ([GitHub][1])

This matters for “string wrapper” types used for secrets/redaction.

---

# D4) Practical “drop-in” adapters (migration scaffolding)

## D4.1 `json.dumps`-like façade (return str, accept common knobs)

```python
def dumps_like_json(obj, *, default=None, sort_keys=False, indent=None) -> str:
    opt = 0
    if sort_keys:
        opt |= orjson.OPT_SORT_KEYS
    if indent == 2:
        opt |= orjson.OPT_INDENT_2
    elif indent not in (None, 2):
        raise ValueError("orjson only supports indent=2")
    return orjson.dumps(obj, default=default, option=opt).decode("utf-8")
```

Mapping is exactly what orjson documents for migrating. ([GitHub][1])

## D4.2 Strict ingest façade (reject bad UTF-8 early, keep bytes)

```python
def loads_utf8_bytes(data: bytes):
    # If you *need* best-effort recovery, decode with errors="replace" first.
    return orjson.loads(data)
```

orjson recommends passing bytes/memoryview directly for lower latency/memory, and requires valid UTF-8. ([GitHub][1])

---

# D5) Migration test playbook (what to assert)

## D5.1 Roundtrip invariants (where semantics should match)

* For “ordinary” JSON-compatible Python objects: `orjson.loads(orjson.dumps(x))` should be structurally equivalent (modulo bytes/str boundary). ([GitHub][1])

## D5.2 Differential tests (where you *expect* differences)

Write explicit tests for:

* surrogate-containing strings (stdlib passes; orjson raises). ([GitHub][1])
* `NaN/Infinity` tokens (stdlib may accept; orjson rejects). ([Python documentation][2])
* non-str keys (stdlib accepts some; orjson requires opt-in + collision policy). ([Python documentation][2])
* integer overflow >53-bit (if you enable `OPT_STRICT_INTEGER`). ([GitHub][1])

---

If you want the next increment after this: I can turn these into a **repo-ready “orjson contract harness”** (pytest helpers) that bakes in (a) option presets as named policies, (b) golden snapshot utilities using `OPT_SORT_KEYS`, and (c) negative tests for UTF/surrogates + NaN tokens + non-str key collisions.

[1]: https://github.com/ijl/orjson "GitHub - ijl/orjson: Fast, correct Python JSON library supporting dataclasses, datetimes, and numpy"
[2]: https://docs.python.org/3/library/json.html "json — JSON encoder and decoder — Python 3.14.2 documentation"

Below is a **drop-in, repo-ready orjson contract harness** (pytest-friendly) with:

* **Named policies** (option presets)
* **Golden snapshot utilities** built on **`OPT_SORT_KEYS`**
* **Negative tests** for **UTF/surrogates**, **NaN tokens**, and **non-str key collisions** (guarded preflight)

> Paths are suggestions—rename `myproj` to your package name.

---

## 1) Harness module

### `src/myproj/orjson_contract.py`

```python
"""
orjson contract harness (pytest-friendly).

Drop-in utilities for:
  - named option presets (policies)
  - stable canonical serialization for goldens
  - guardrails for OPT_NON_STR_KEYS key-collision footguns
"""

from __future__ import annotations

from dataclasses import dataclass
import dataclasses
from pathlib import Path
import difflib
import math
from typing import Any, Callable, Mapping, Optional

import orjson


JSONDefault = Callable[[Any], Any]


class OrjsonContractError(Exception):
    """Base class for harness-level contract violations."""


class OrjsonNonStrKeyCollisionError(OrjsonContractError):
    """Raised when OPT_NON_STR_KEYS would create duplicate JSON object keys."""


@dataclass(frozen=True, slots=True)
class OrjsonPolicy:
    """Named serialization policy.

    Args:
        name: A stable identifier for the policy.
        option: Bitmask of orjson.OPT_* flags.
        default: Optional default handler for non-native types.
        validate_non_str_key_collisions: If True, preflight dict key collisions when OPT_NON_STR_KEYS is set.
    """

    name: str
    option: int = 0
    default: Optional[JSONDefault] = None
    validate_non_str_key_collisions: bool = True


# --- Preset policies -------------------------------------------------------

POLICY_COMPACT = OrjsonPolicy(name="compact", option=0)

POLICY_JSONL = OrjsonPolicy(
    name="jsonl",
    option=orjson.OPT_APPEND_NEWLINE,
)

POLICY_CANONICAL = OrjsonPolicy(
    name="canonical",
    option=orjson.OPT_SORT_KEYS,
)

# Human-diffable goldens: deterministic + pretty + newline
POLICY_GOLDEN = OrjsonPolicy(
    name="golden",
    option=orjson.OPT_SORT_KEYS | orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE,
)

POLICY_WEB_SAFE = OrjsonPolicy(
    name="web_safe",
    option=orjson.OPT_STRICT_INTEGER,
)

POLICY_NON_STR_KEYS = OrjsonPolicy(
    name="non_str_keys",
    option=orjson.OPT_NON_STR_KEYS,
)

POLICY_NON_STR_KEYS_CANONICAL = OrjsonPolicy(
    name="non_str_keys_canonical",
    option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SORT_KEYS,
)

POLICY_NUMPY = OrjsonPolicy(
    name="numpy",
    option=orjson.OPT_SERIALIZE_NUMPY,
)

POLICIES: Mapping[str, OrjsonPolicy] = {
    p.name: p
    for p in (
        POLICY_COMPACT,
        POLICY_JSONL,
        POLICY_CANONICAL,
        POLICY_GOLDEN,
        POLICY_WEB_SAFE,
        POLICY_NON_STR_KEYS,
        POLICY_NON_STR_KEYS_CANONICAL,
        POLICY_NUMPY,
    )
}


# --- Core wrappers ---------------------------------------------------------

def dumps_bytes(
    obj: Any,
    *,
    policy: OrjsonPolicy = POLICY_COMPACT,
    default: Optional[JSONDefault] = None,
    option: Optional[int] = None,
    validate: bool = True,
) -> bytes:
    """Serialize to UTF-8 bytes under a named policy."""
    opt = policy.option if option is None else int(option)
    dflt = default if default is not None else policy.default

    if validate and policy.validate_non_str_key_collisions and (opt & orjson.OPT_NON_STR_KEYS):
        validate_no_non_str_key_collisions(obj, option=opt)

    return orjson.dumps(obj, default=dflt, option=opt)


def dumps_str(
    obj: Any,
    *,
    policy: OrjsonPolicy = POLICY_COMPACT,
    default: Optional[JSONDefault] = None,
    option: Optional[int] = None,
    validate: bool = True,
) -> str:
    """Serialize and decode to str (UTF-8)."""
    return dumps_bytes(
        obj, policy=policy, default=default, option=option, validate=validate
    ).decode("utf-8")


def loads(data: bytes | bytearray | memoryview | str) -> Any:
    """Deserialize using orjson.loads."""
    return orjson.loads(data)


# --- Canonicalization + goldens -------------------------------------------

def canonical_json_bytes(
    obj: Any,
    *,
    pretty: bool = False,
    default: Optional[JSONDefault] = None,
    validate: bool = True,
) -> bytes:
    """Deterministic JSON bytes for goldens/hashes.

    This ALWAYS applies OPT_SORT_KEYS. If `pretty=True`, also applies OPT_INDENT_2 and OPT_APPEND_NEWLINE.
    """
    opt = orjson.OPT_SORT_KEYS
    if pretty:
        opt |= orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE
    return dumps_bytes(obj, option=opt, default=default, validate=validate)


def assert_json_golden(
    name: str,
    obj: Any,
    *,
    golden_dir: Path,
    update: bool = False,
    pretty: bool = True,
    default: Optional[JSONDefault] = None,
    validate: bool = True,
    suffix: str = ".json",
) -> Path:
    """Assert obj matches a golden snapshot (and optionally update it)."""
    golden_dir.mkdir(parents=True, exist_ok=True)
    path = golden_dir / f"{name}{suffix}"
    actual = canonical_json_bytes(obj, pretty=pretty, default=default, validate=validate)

    if update or not path.exists():
        path.write_bytes(actual)
        return path

    expected = path.read_bytes()
    if expected != actual:
        exp_s = _safe_utf8(expected)
        act_s = _safe_utf8(actual)
        diff = "\n".join(
            difflib.unified_diff(
                exp_s.splitlines(),
                act_s.splitlines(),
                fromfile=f"expected:{path.name}",
                tofile=f"actual:{path.name}",
                lineterm="",
            )
        )
        raise AssertionError(f"Golden mismatch for {path}\n{diff}")

    return path


def _safe_utf8(b: bytes) -> str:
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return b.decode("utf-8", errors="replace")


# --- Non-str key collision guard ------------------------------------------

def validate_no_non_str_key_collisions(obj: Any, *, option: int) -> None:
    """Preflight collision detection for OPT_NON_STR_KEYS.

    orjson can emit duplicate JSON keys when non-str keys stringify to the same value
    (e.g., {"1": "a", 1: "b"} -> {"1":"a","1":"b"}). This helper raises early with a
    precise path to the offending dict.
    """
    _validate_no_non_str_key_collisions(obj, option=option, path="$")


def _validate_no_non_str_key_collisions(obj: Any, *, option: int, path: str) -> None:
    # dataclasses: walk field values without materializing asdict (avoid copies)
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        for f in dataclasses.fields(obj):
            try:
                v = getattr(obj, f.name)
            except Exception:
                continue
            _validate_no_non_str_key_collisions(v, option=option, path=f"{path}.{f.name}")
        return

    if isinstance(obj, dict):
        seen: dict[str, Any] = {}
        for k, v in obj.items():
            key_str = _json_object_key_string(k, option=option)
            if key_str in seen:
                prev = seen[key_str]
                raise OrjsonNonStrKeyCollisionError(
                    f"OPT_NON_STR_KEYS collision at {path}: keys {prev!r} and {k!r} "
                    f"both map to JSON key {key_str!r}"
                )
            seen[key_str] = k
            _validate_no_non_str_key_collisions(v, option=option, path=f"{path}.{key_str}")
        return

    if isinstance(obj, (list, tuple)):
        for i, item in enumerate(obj):
            _validate_no_non_str_key_collisions(item, option=option, path=f"{path}[{i}]")
        return

    # leave other types alone; collisions only occur in dict keys


def _json_object_key_string(key: Any, *, option: int) -> str:
    """Compute the JSON object key string content produced by OPT_NON_STR_KEYS.

    - str keys: key itself
    - non-str keys: stringify the JSON token or string content of the scalar encoding.
    """
    if isinstance(key, str):
        return key

    # Non-finite floats are serialized as `null` as values; as keys they'd become "null" and collide with None.
    if isinstance(key, float) and not math.isfinite(key):
        raise OrjsonNonStrKeyCollisionError(
            f"Non-finite float dict key {key!r} is not allowed under OPT_NON_STR_KEYS; "
            "it serializes as null and can collide."
        )

    # Use orjson's own scalar formatting (including datetime flags, STRICT_INTEGER, etc.)
    b = orjson.dumps(key, option=option & _SCALAR_REPR_MASK)

    # If scalar encodes as a JSON string, decode its content.
    if b[:1] == b'"':
        s = orjson.loads(b)
        if not isinstance(s, str):
            return str(s)
        return s

    # Otherwise, the key becomes the token text (number/true/false/null).
    return b.decode("utf-8")


# Options that can affect scalar formatting for keys.
_SCALAR_REPR_MASK = (
    orjson.OPT_NAIVE_UTC
    | orjson.OPT_UTC_Z
    | orjson.OPT_OMIT_MICROSECONDS
    | orjson.OPT_STRICT_INTEGER
)
```

---

## 2) pytest integration for snapshot updates

### `tests/conftest.py`

```python
from __future__ import annotations

import os
from pathlib import Path

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-goldens",
        action="store_true",
        default=False,
        help="Overwrite golden snapshot files with current output.",
    )


@pytest.fixture
def update_goldens(request: pytest.FixtureRequest) -> bool:
    env = os.getenv("UPDATE_GOLDENS", "0")
    env_flag = env not in ("", "0", "false", "False", "no", "No")
    return bool(request.config.getoption("--update-goldens") or env_flag)


@pytest.fixture
def golden_dir() -> Path:
    return Path(__file__).parent / "goldens"
```

Run once to create/update goldens:

```bash
pytest --update-goldens
# or
UPDATE_GOLDENS=1 pytest
```

---

## 3) Golden snapshot example test

### `tests/test_orjson_contract_goldens.py`

```python
from __future__ import annotations

from myproj.orjson_contract import assert_json_golden


def test_golden_snapshot_roundtrip(golden_dir, update_goldens) -> None:
    obj = {"b": [2, 1], "a": 1}
    # Stable output uses OPT_SORT_KEYS (and pretty formatting by default).
    assert_json_golden("example", obj, golden_dir=golden_dir, update=update_goldens)
```

This will create `tests/goldens/example.json` on the first run with `--update-goldens`.

---

## 4) Negative tests (UTF/surrogates, NaN tokens, non-str key collisions)

### `tests/test_orjson_contract_negative.py`

```python
from __future__ import annotations

import pytest
import orjson

from myproj.orjson_contract import (
    dumps_bytes,
    loads,
    POLICY_NON_STR_KEYS,
    OrjsonNonStrKeyCollisionError,
)


def test_dumps_rejects_surrogates() -> None:
    # Lone surrogate code points are not valid UTF-8.
    with pytest.raises(orjson.JSONEncodeError):
        dumps_bytes("\ud800")


def test_loads_rejects_invalid_utf8() -> None:
    with pytest.raises(orjson.JSONDecodeError):
        loads(b"\xff")


def test_loads_rejects_nan_tokens() -> None:
    with pytest.raises(orjson.JSONDecodeError):
        loads(b'{"x": NaN}')


def test_non_str_key_collision_is_detected() -> None:
    obj = {"1": "a", 1: "b"}
    with pytest.raises(OrjsonNonStrKeyCollisionError):
        dumps_bytes(obj, policy=POLICY_NON_STR_KEYS)
```

---

### Notes you’ll care about in real repos

* The collision guard is only invoked when `OPT_NON_STR_KEYS` is set (and you can disable it with `validate=False` or by setting `validate_non_str_key_collisions=False` on a policy).
* Goldens are **deterministic** via `OPT_SORT_KEYS`; the helper defaults to `pretty=True` for diffability.

If you want, I can extend this harness with a **single “policy registry + contract boundary” object** that standardizes: `(policy_name, bytes/str, parse/roundtrip expectations, and json-equivalence comparators)` so you can enforce consistent behavior across CLI, HTTP responses, and persisted artifacts.

Below is a **repo-ready extension** that turns your existing harness into a **single “policy registry + contract boundary” system** with:

* **Standardized boundary specs**: `(policy_name, output bytes/str, parse expectation, roundtrip expectation)`
* **A single JSON-equivalence comparator** (object↔object, output↔output, object↔output)
* **Boundary helpers** you can use everywhere: CLI, FastAPI/HTTP, persisted artifacts, tests

I’m treating your prior `src/myproj/orjson_contract.py` as already present (the file from the previous message).

---

## 1) Add the “policy registry + contract boundary” layer

### Append to `src/myproj/orjson_contract.py` (below your existing code)

```python
# --- Policy registry + contract boundaries ---------------------------------
from typing import Literal, MutableMapping


OutputKind = Literal["bytes", "str"]
RoundtripExpectation = Literal["none", "json_equivalent"]
ParseExpectation = Literal["strict"]


_FORMAT_MASK = (orjson.OPT_APPEND_NEWLINE | orjson.OPT_INDENT_2)


@dataclass(frozen=True, slots=True)
class OrjsonBoundarySpec:
    """A standardized JSON contract at a boundary (HTTP, CLI, artifact, etc.).

    policy_name:
      - selects a policy from the policy registry (option/default/validation)
    output:
      - whether dumps() returns bytes or str at this boundary
    parse:
      - whether outputs are expected to be parseable JSON (strict)
    roundtrip:
      - whether dumps->loads is expected to be JSON-equivalent to the input object
    """

    name: str
    policy_name: str
    output: OutputKind = "bytes"
    parse: ParseExpectation = "strict"
    roundtrip: RoundtripExpectation = "json_equivalent"


class OrjsonPolicyRegistry:
    """Mutable registry so your repo has one source of truth for orjson policies."""

    def __init__(self, initial: Mapping[str, OrjsonPolicy] | None = None) -> None:
        self._policies: MutableMapping[str, OrjsonPolicy] = dict(initial or {})

    def register(self, policy: OrjsonPolicy) -> None:
        if policy.name in self._policies:
            raise OrjsonContractError(f"Policy already registered: {policy.name}")
        self._policies[policy.name] = policy

    def get(self, name: str) -> OrjsonPolicy:
        try:
            return self._policies[name]
        except KeyError as e:
            raise OrjsonContractError(f"Unknown policy: {name}") from e

    def names(self) -> tuple[str, ...]:
        return tuple(self._policies.keys())


class OrjsonContractBoundary:
    """A boundary object that enforces consistent JSON behavior across your repo."""

    def __init__(self, *, spec: OrjsonBoundarySpec, policies: OrjsonPolicyRegistry) -> None:
        self._spec = spec
        self._policies = policies

    @property
    def name(self) -> str:
        return self._spec.name

    @property
    def policy(self) -> OrjsonPolicy:
        return self._policies.get(self._spec.policy_name)

    @property
    def output_kind(self) -> OutputKind:
        return self._spec.output

    def dumps(self, obj: Any, *, validate: bool = True) -> bytes | str:
        """Serialize according to boundary policy and output kind."""
        b = dumps_bytes(obj, policy=self.policy, validate=validate)
        if self._spec.output == "bytes":
            return b
        return b.decode("utf-8")

    def loads(self, data: bytes | bytearray | memoryview | str) -> Any:
        """Parse JSON output at this boundary (strict)."""
        return orjson.loads(data)

    # --------- Equivalence / normalization ---------------------------------

    def normalize_option(self) -> int:
        """Option mask used for boundary-consistent normalization.

        - Starts from the boundary's option set
        - Strips formatting-only flags (indent/newline)
        - Adds OPT_SORT_KEYS for stable bytes (useful for diffs/goldens)
        """
        opt = self.policy.option
        opt &= ~_FORMAT_MASK
        opt |= orjson.OPT_SORT_KEYS
        return opt

    def to_json_value(self, x: Any, *, validate: bool = True) -> Any:
        """Convert Python object OR JSON bytes/str -> parsed JSON value.

        - If x is bytes/str: parse as JSON
        - Else: serialize under boundary semantics (normalize_option), then parse
        """
        if isinstance(x, (bytes, bytearray, memoryview, str)):
            return orjson.loads(x)

        # Serialize using boundary semantics, then parse into JSON domain.
        b = dumps_bytes(
            x,
            option=self.normalize_option(),
            default=self.policy.default,
            validate=validate,
        )
        return orjson.loads(b)

    def json_equivalent(self, a: Any, b: Any, *, validate: bool = True) -> bool:
        """JSON semantic equivalence under this boundary's semantics."""
        return self.to_json_value(a, validate=validate) == self.to_json_value(b, validate=validate)

    def assert_json_equivalent(self, a: Any, b: Any, *, validate: bool = True) -> None:
        """Assert JSON semantic equivalence with a stable unified diff."""
        av = self.to_json_value(a, validate=validate)
        bv = self.to_json_value(b, validate=validate)
        if av == bv:
            return

        a_pretty = canonical_json_bytes(av, pretty=True, validate=validate)
        b_pretty = canonical_json_bytes(bv, pretty=True, validate=validate)

        a_s = _safe_utf8(a_pretty)
        b_s = _safe_utf8(b_pretty)

        diff = "\n".join(
            difflib.unified_diff(
                a_s.splitlines(),
                b_s.splitlines(),
                fromfile=f"{self.name}:a",
                tofile=f"{self.name}:b",
                lineterm="",
            )
        )
        raise AssertionError(f"JSON not equivalent under boundary '{self.name}'.\n{diff}")

    # --------- Parse + roundtrip expectations -------------------------------

    def assert_parseable(self, data: bytes | str) -> Any:
        """Assert boundary output parses as JSON; return parsed value."""
        try:
            return orjson.loads(data)
        except orjson.JSONDecodeError as e:
            raise AssertionError(f"Boundary '{self.name}' produced non-parseable JSON: {e}") from e

    def assert_roundtrip(self, obj: Any, *, validate: bool = True) -> Any:
        """Assert dumps->loads is JSON-equivalent to obj (in JSON domain)."""
        if self._spec.roundtrip == "none":
            return self.loads(self.dumps(obj, validate=validate))  # best-effort parse anyway

        dumped = self.dumps(obj, validate=validate)
        parsed = self.loads(dumped)

        expected = self.to_json_value(obj, validate=validate)
        if parsed != expected:
            self.assert_json_equivalent(parsed, expected, validate=validate)  # raises with diff
        return parsed

    # --------- Golden convenience (optional, but high leverage) --------------

    def assert_golden(
        self,
        name: str,
        obj: Any,
        *,
        golden_dir: Path,
        update: bool = False,
        suffix: str = ".json",
        validate: bool = True,
    ) -> Path:
        """Golden snapshot using boundary semantics + stable formatting."""
        # Snapshot the JSON-domain value under boundary semantics.
        json_value = self.to_json_value(obj, validate=validate)
        return assert_json_golden(
            name,
            json_value,
            golden_dir=golden_dir,
            update=update,
            pretty=True,
            validate=validate,
            suffix=suffix,
        )


class OrjsonContract:
    """Single entrypoint: policy registry + boundary registry."""

    def __init__(
        self,
        *,
        policies: OrjsonPolicyRegistry,
        boundaries: Mapping[str, OrjsonBoundarySpec],
    ) -> None:
        self.policies = policies
        self._boundary_specs = dict(boundaries)

    def boundary(self, name: str) -> OrjsonContractBoundary:
        try:
            spec = self._boundary_specs[name]
        except KeyError as e:
            raise OrjsonContractError(f"Unknown boundary: {name}") from e
        return OrjsonContractBoundary(spec=spec, policies=self.policies)

    def boundary_names(self) -> tuple[str, ...]:
        return tuple(self._boundary_specs.keys())


# --- Default contract (the “one thing” the whole repo imports) -------------

DEFAULT_POLICY_REGISTRY = OrjsonPolicyRegistry(POLICIES)

DEFAULT_BOUNDARIES: Mapping[str, OrjsonBoundarySpec] = {
    # Compact bytes for HTTP response bodies; roundtrip expected in JSON domain.
    "http": OrjsonBoundarySpec(name="http", policy_name="compact", output="bytes"),

    # Human-readable JSON for CLI output (string), deterministic.
    "cli": OrjsonBoundarySpec(name="cli", policy_name="golden", output="str"),

    # Persisted artifacts: deterministic bytes (stable across runs).
    "artifact": OrjsonBoundarySpec(name="artifact", policy_name="canonical", output="bytes"),

    # If you explicitly allow non-str keys, do so via a dedicated boundary.
    "artifact_non_str_keys": OrjsonBoundarySpec(
        name="artifact_non_str_keys",
        policy_name="non_str_keys_canonical",
        output="bytes",
    ),
}

DEFAULT_CONTRACT = OrjsonContract(policies=DEFAULT_POLICY_REGISTRY, boundaries=DEFAULT_BOUNDARIES)
```

What this gives you:

* **One import** (`DEFAULT_CONTRACT`) that the whole repo uses
* **Boundary objects** with:

  * `.dumps()` returning bytes/str consistently
  * `.assert_parseable()` and `.assert_roundtrip()`
  * `.json_equivalent()` / `.assert_json_equivalent()` for cross-system comparisons
  * `.assert_golden()` convenience

---

## 2) “Use everywhere” examples (CLI, FastAPI, persisted artifacts)

### CLI (string output, already pretty + deterministic)

```python
import sys
from myproj.orjson_contract import DEFAULT_CONTRACT

cli = DEFAULT_CONTRACT.boundary("cli")
sys.stdout.write(cli.dumps({"ok": True}))
```

### FastAPI / Starlette response (bytes output)

```python
from fastapi import FastAPI
from starlette.responses import Response
from myproj.orjson_contract import DEFAULT_CONTRACT

app = FastAPI()
http = DEFAULT_CONTRACT.boundary("http")

@app.get("/healthz")
def healthz():
    payload = {"ok": True}
    return Response(content=http.dumps(payload), media_type="application/json")
```

### Persisted artifacts (stable bytes)

```python
from pathlib import Path
from myproj.orjson_contract import DEFAULT_CONTRACT

artifact = DEFAULT_CONTRACT.boundary("artifact")
Path("out.json").write_bytes(artifact.dumps({"a": 1, "b": 2}))
```

---

## 3) pytest: enforce consistency across boundaries

### `tests/test_orjson_boundaries.py`

```python
from __future__ import annotations

from myproj.orjson_contract import DEFAULT_CONTRACT


def test_http_and_cli_are_json_equivalent() -> None:
    http = DEFAULT_CONTRACT.boundary("http")
    cli = DEFAULT_CONTRACT.boundary("cli")

    obj = {"b": [2, 1], "a": 1}

    http_out = http.dumps(obj)  # bytes
    cli_out = cli.dumps(obj)    # str

    # Both parse as JSON:
    http.assert_parseable(http_out)
    cli.assert_parseable(cli_out)

    # And are JSON-equivalent under each boundary’s semantics.
    # (Use one boundary as the comparator; pick the one whose semantics you want.)
    http.assert_json_equivalent(http_out, cli_out)


def test_artifact_roundtrip_contract() -> None:
    artifact = DEFAULT_CONTRACT.boundary("artifact")
    obj = {"b": [2, 1], "a": 1}
    artifact.assert_roundtrip(obj)


def test_cli_roundtrip_contract() -> None:
    cli = DEFAULT_CONTRACT.boundary("cli")
    obj = {"b": [2, 1], "a": 1}
    cli.assert_roundtrip(obj)
```

### Optional: boundary-aware goldens (if you like the ergonomics)

```python
def test_http_golden(golden_dir, update_goldens) -> None:
    http = DEFAULT_CONTRACT.boundary("http")
    http.assert_golden("healthz", {"ok": True}, golden_dir=golden_dir, update=update_goldens)
```

---

## 4) Why this structure works (practically)

* **Policy drift becomes impossible to miss**: options/default behavior are centralized and referenced by name.
* **You get “JSON-domain invariants”**: roundtrip/equivalence are defined in the parsed JSON domain, not the original Python type domain (which avoids false failures with datetimes, dataclasses, numpy, etc.).
* **You can compare across systems**: e.g., HTTP bytes from service A vs artifact bytes from pipeline B, and assert they are equivalent.

---

If you want the next increment: I can add a tiny **“BoundarySelector”** that picks `http|cli|artifact` based on context (env var / CLI flag / test mode), plus a `ContractBoundaryMiddleware` pattern for FastAPI so every endpoint automatically uses the same boundary and you can assert roundtrip/equivalence in integration tests with one helper.

Below is a **repo-ready “next increment”** that adds:

1. a tiny **`BoundarySelector`** (env var / CLI flag / test mode)
2. a **FastAPI contract wiring pattern**: `ContractBoundaryMiddleware` + `OrjsonContractJSONResponse` so *all* JSON endpoints use the same boundary
3. a **single integration-test helper** that asserts parseability + JSON-equivalence (and optionally roundtrip)

I’m assuming you already have `src/myproj/orjson_contract.py` from the previous steps (with `DEFAULT_CONTRACT`, `OrjsonContract`, etc.).

---

# 1) Runtime selector + FastAPI wiring

## `src/myproj/orjson_contract_runtime.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Literal, Mapping, Optional

import contextvars

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from myproj.orjson_contract import (
    DEFAULT_CONTRACT,
    OrjsonContract,
    OrjsonContractBoundary,
)


BoundaryContext = Literal["http", "cli", "artifact"]


_CURRENT_BOUNDARY_NAME: contextvars.ContextVar[str] = contextvars.ContextVar(
    "orjson_contract_boundary_name",
    default="http",
)


def current_boundary_name() -> str:
    return _CURRENT_BOUNDARY_NAME.get()


@dataclass(frozen=True, slots=True)
class BoundarySelector:
    """Select a boundary name based on override > CLI flag > env var > defaults.

    - env var is the most common knob in deploy (e.g. CODEINTEL_JSON_BOUNDARY=artifact)
    - CLI flag supports local usage (e.g. --json-boundary cli)
    - test mode defaults to deterministic boundaries unless overridden
    """

    env_var: str = "CODEINTEL_JSON_BOUNDARY"
    test_env_var: str = "PYTEST_CURRENT_TEST"
    default_http: str = "http"
    default_cli: str = "cli"
    default_artifact: str = "artifact"
    default_test: str = "artifact"  # deterministic-by-default in tests

    def is_test_mode(self, env: Optional[Mapping[str, str]] = None) -> bool:
        e = os.environ if env is None else env
        return self.test_env_var in e

    def select(
        self,
        *,
        context: BoundaryContext,
        override: str | None = None,
        cli_flag: str | None = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> str:
        e = os.environ if env is None else env
        if override:
            return override
        if cli_flag:
            return cli_flag
        if (v := e.get(self.env_var)):
            return v

        if self.is_test_mode(e):
            return self.default_test

        if context == "http":
            return self.default_http
        if context == "cli":
            return self.default_cli
        return self.default_artifact


class OrjsonContractJSONResponse(Response):
    """FastAPI/Starlette Response that renders JSON using the *current* contract boundary.

    Intended usage:
      app = FastAPI(default_response_class=OrjsonContractJSONResponse)
      app.add_middleware(ContractBoundaryMiddleware, contract=..., selector=..., context="http")
    """

    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        # Try to use the boundary set by middleware; fall back to "http".
        boundary_name = current_boundary_name()
        boundary = DEFAULT_CONTRACT.boundary(boundary_name)

        out = boundary.dumps(content)

        # FastAPI/Starlette requires bytes for the response body.
        if isinstance(out, bytes):
            return out
        return out.encode("utf-8")


class ContractBoundaryMiddleware(BaseHTTPMiddleware):
    """Sets the boundary for the lifetime of the request via a ContextVar.

    Also:
      - attaches boundary to request.state.contract_boundary for endpoint access
      - adds an X-Contract-Boundary header for debugging & tests
    """

    def __init__(
        self,
        app,
        *,
        contract: OrjsonContract = DEFAULT_CONTRACT,
        selector: BoundarySelector | None = None,
        context: BoundaryContext = "http",
        cli_flag: str | None = None,
        override: str | None = None,
        header_name: str = "X-Contract-Boundary",
    ) -> None:
        super().__init__(app)
        self._contract = contract
        self._selector = selector or BoundarySelector()
        self._context = context
        self._cli_flag = cli_flag
        self._override = override
        self._header_name = header_name

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        name = self._selector.select(
            context=self._context,
            override=self._override,
            cli_flag=self._cli_flag,
        )

        # Validate early so boundary mistakes crash loudly.
        boundary = self._contract.boundary(name)

        token = _CURRENT_BOUNDARY_NAME.set(name)
        try:
            request.state.contract_boundary = boundary  # type: ignore[attr-defined]
            response = await call_next(request)
        finally:
            _CURRENT_BOUNDARY_NAME.reset(token)

        response.headers.setdefault(self._header_name, name)
        return response


# --- Convenience glue -------------------------------------------------------

def install_fastapi_contract(
    app,
    *,
    contract: OrjsonContract = DEFAULT_CONTRACT,
    selector: BoundarySelector | None = None,
    context: BoundaryContext = "http",
    cli_flag: str | None = None,
    override: str | None = None,
) -> None:
    """One-liner to wire middleware + response class defaults.

    If you use this, instantiate FastAPI with:
      FastAPI(default_response_class=OrjsonContractJSONResponse)
    """
    app.add_middleware(
        ContractBoundaryMiddleware,
        contract=contract,
        selector=selector,
        context=context,
        cli_flag=cli_flag,
        override=override,
    )


def get_request_boundary(request: Request) -> OrjsonContractBoundary:
    """Endpoint helper (when you want to access boundary explicitly)."""
    b = getattr(request.state, "contract_boundary", None)
    if isinstance(b, OrjsonContractBoundary):
        return b
    return DEFAULT_CONTRACT.boundary(current_boundary_name())
```

### What this achieves

* **Middleware sets the boundary once per request**
* **All JSON rendering** (for endpoints returning dict/list/etc.) goes through `OrjsonContractJSONResponse`, which reads the boundary via a `ContextVar`
* You can run the *same app* under different contract regimes using:

  * `CODEINTEL_JSON_BOUNDARY=http|artifact|cli` (env)
  * a CLI flag passed into middleware at startup
  * a default deterministic test mode (`artifact`) unless overridden

---

# 2) FastAPI pattern: app factory

## `src/myproj/app.py`

```python
from __future__ import annotations

from fastapi import FastAPI

from myproj.orjson_contract_runtime import (
    OrjsonContractJSONResponse,
    install_fastapi_contract,
)

def create_app() -> FastAPI:
    app = FastAPI(default_response_class=OrjsonContractJSONResponse)

    # One line: ensures every JSON response uses the boundary selected for HTTP.
    install_fastapi_contract(app, context="http")

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    return app
```

> If an endpoint returns a `Response` explicitly, FastAPI will not use the default response class (by design). Your “everything uses the boundary” rule is: **return plain dict/list/models**, not manual JSONResponses.

---

# 3) BoundarySelector for CLI and artifacts

## CLI selection example (Cyclopts/Typer style)

Use the selector once at program start and pass it into the middleware (or just use the boundary directly for CLI printing).

```python
import os
import sys

from myproj.orjson_contract_runtime import BoundarySelector
from myproj.orjson_contract import DEFAULT_CONTRACT

def main(json_boundary: str | None = None) -> None:
    selector = BoundarySelector()
    name = selector.select(context="cli", cli_flag=json_boundary, env=os.environ)
    boundary = DEFAULT_CONTRACT.boundary(name)

    sys.stdout.write(boundary.dumps({"mode": name, "ok": True}))
```

## Artifact writing selection

```python
import os
from pathlib import Path

from myproj.orjson_contract_runtime import BoundarySelector
from myproj.orjson_contract import DEFAULT_CONTRACT

selector = BoundarySelector()
name = selector.select(context="artifact", env=os.environ)
artifact = DEFAULT_CONTRACT.boundary(name)

Path("out.json").write_bytes(artifact.dumps({"a": 1, "b": 2}))  # boundary may output bytes
```

---

# 4) One helper for integration tests (parse + equivalence + roundtrip)

## `tests/contract_http_assertions.py`

```python
from __future__ import annotations

from typing import Any, Optional

from myproj.orjson_contract import DEFAULT_CONTRACT, OrjsonContract
from myproj.orjson_contract_runtime import current_boundary_name


def assert_contract_response(
    response,
    expected: Any,
    *,
    contract: OrjsonContract = DEFAULT_CONTRACT,
    boundary_name: Optional[str] = None,
    header_name: str = "X-Contract-Boundary",
    roundtrip: bool = False,
) -> None:
    """Single helper for FastAPI TestClient/httpx responses.

    - asserts response body is parseable JSON
    - asserts JSON-equivalence to expected (object or JSON bytes/str)
    - optionally asserts boundary roundtrip semantics for expected
    """
    # Prefer boundary as reported by middleware; else caller override; else ContextVar; else "http".
    bname = (
        boundary_name
        or response.headers.get(header_name)
        or current_boundary_name()
        or "http"
    )
    boundary = contract.boundary(bname)

    body = response.content  # bytes
    boundary.assert_parseable(body)
    boundary.assert_json_equivalent(body, expected)

    if roundtrip:
        boundary.assert_roundtrip(expected)
```

---

# 5) FastAPI integration tests

## `tests/test_contract_middleware_fastapi.py`

```python
from __future__ import annotations

import os
from fastapi.testclient import TestClient

from myproj.app import create_app
from tests.contract_http_assertions import assert_contract_response


def test_contract_boundary_header_is_set(monkeypatch) -> None:
    # Override boundary for this test run.
    monkeypatch.setenv("CODEINTEL_JSON_BOUNDARY", "http")

    app = create_app()
    client = TestClient(app)

    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.headers.get("X-Contract-Boundary") == "http"
    assert_contract_response(r, {"ok": True})


def test_contract_boundary_can_switch(monkeypatch) -> None:
    # In tests, you might want deterministic artifacts (sorted keys, etc.)
    monkeypatch.setenv("CODEINTEL_JSON_BOUNDARY", "artifact")

    app = create_app()
    client = TestClient(app)

    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.headers.get("X-Contract-Boundary") == "artifact"

    # Still JSON-equivalent to expected.
    assert_contract_response(r, {"ok": True})
```

---

## (Optional) Make test mode deterministic without setting env

If you rely on `PYTEST_CURRENT_TEST` detection, your selector already defaults tests to `artifact`. If you want HTTP tests to default to `http` instead, just change `BoundarySelector.default_test`.

---

If you want the next increment after this, the most practical one is a **FastAPI dependency + response-model policy** layer:

* `Depends(get_request_boundary)` so handlers can explicitly call `boundary.assert_roundtrip(model)` in debug builds
* a `@contracted_route(boundary="http")` decorator to pin boundary per-router (useful when mixing “public HTTP” vs “internal debug endpoints”)

Below is the **FastAPI dependency + response-model policy layer** you asked for, built to snap onto what you already have:

* `Depends(get_request_boundary)` (as a clean `ContractBoundary` alias)
* `assert_roundtrip_if_debug(...)` that encodes response models consistently before roundtrip-checking
* a **router/route pinning system**:

  * `contracted_router(boundary="http")` pins a boundary for *all routes in that router*
  * `@contracted_route(boundary="cli")` overrides boundary for *one route*

This is designed to work with your existing:

* `DEFAULT_CONTRACT`
* `OrjsonContractJSONResponse` (default_response_class)
* middleware/ContextVar boundary mechanism

---

# 1) Small patch: expose a public boundary override context manager

### Append to `src/myproj/orjson_contract_runtime.py`

```python
from contextlib import contextmanager
from typing import Iterator

from starlette.requests import Request

from myproj.orjson_contract import OrjsonContract, OrjsonContractBoundary, DEFAULT_CONTRACT

# (uses the module-private _CURRENT_BOUNDARY_NAME already defined above)

@contextmanager
def override_contract_boundary(
    name: str,
    *,
    request: Request | None = None,
    contract: OrjsonContract = DEFAULT_CONTRACT,
) -> Iterator[OrjsonContractBoundary]:
    """Temporarily pin the current boundary (ContextVar + request.state) for the active scope."""
    boundary = contract.boundary(name)
    token = _CURRENT_BOUNDARY_NAME.set(name)
    try:
        if request is not None:
            request.state.contract_boundary = boundary  # type: ignore[attr-defined]
        yield boundary
    finally:
        _CURRENT_BOUNDARY_NAME.reset(token)
```

This is what the router/route pinning uses to ensure **dependencies** (including `get_request_boundary`) see the pinned boundary.

---

# 2) New module: dependency alias + response-model policy + route/router pinning

## `src/myproj/orjson_fastapi_contract.py`

```python
from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Annotated, Callable, Optional, Type

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute
from starlette.requests import Request
from starlette.responses import Response

from myproj.orjson_contract import DEFAULT_CONTRACT, OrjsonContractBoundary
from myproj.orjson_contract_runtime import get_request_boundary, override_contract_boundary


# ---------------------------------------------------------------------------
# 1) Dependency alias: make boundary injection ergonomic everywhere
# ---------------------------------------------------------------------------

ContractBoundary = Annotated[OrjsonContractBoundary, Depends(get_request_boundary)]


# ---------------------------------------------------------------------------
# 2) Response-model policy: standardize how “models” become JSON-serializable
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ResponseModelPolicy:
    """Defines how we encode handler returns for contract checks.

    Why this exists:
      - handlers often return Pydantic models, dataclasses, enums, etc.
      - orjson can’t serialize BaseModel directly (FastAPI typically runs jsonable_encoder)
      - we want debug assertions to check the *actual JSON-domain payload*
    """
    by_alias: bool = True
    exclude_none: bool = False
    exclude_unset: bool = False
    exclude_defaults: bool = False

    def encode(self, obj: Any) -> Any:
        return jsonable_encoder(
            obj,
            by_alias=self.by_alias,
            exclude_none=self.exclude_none,
            exclude_unset=self.exclude_unset,
            exclude_defaults=self.exclude_defaults,
        )


DEFAULT_RESPONSE_POLICY = ResponseModelPolicy(
    by_alias=True,
    exclude_none=False,
    exclude_unset=False,
    exclude_defaults=False,
)


def _env_truthy(name: str) -> bool:
    v = os.getenv(name, "")
    return v not in ("", "0", "false", "False", "no", "No")


def assert_roundtrip_if_debug(
    boundary: OrjsonContractBoundary,
    obj: Any,
    *,
    policy: ResponseModelPolicy = DEFAULT_RESPONSE_POLICY,
    env_var: str = "CODEINTEL_JSON_DEBUG_ROUNDTRIP",
    validate: bool = True,
) -> None:
    """In debug builds, assert boundary.dumps->loads matches JSON-domain semantics.

    Use this *inside handlers* on the value you intend to return.
    """
    if not _env_truthy(env_var):
        return

    encoded = policy.encode(obj)
    boundary.assert_roundtrip(encoded, validate=validate)


# ---------------------------------------------------------------------------
# 3) Route + router boundary pinning
# ---------------------------------------------------------------------------

_CONTRACT_BOUNDARY_ATTR = "__contract_boundary__"


def contracted_route(*, boundary: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to pin boundary per-route.

    IMPORTANT: apply this as the *inner* decorator (closest to the function),
    so FastAPI registers the annotated endpoint:

        @router.get("/x")
        @contracted_route(boundary="cli")
        def x(...): ...
    """
    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        setattr(fn, _CONTRACT_BOUNDARY_ATTR, boundary)
        return fn
    return deco


class ContractedAPIRoute(APIRoute):
    """APIRoute that pins an orjson boundary around dependency resolution + handler execution.

    Precedence:
      1) @contracted_route(boundary="...") on the endpoint
      2) router default boundary (from the route_class factory)
      3) do nothing (fall back to middleware/selector)
    """

    # Set by factory below
    default_boundary: str | None = None
    header_name: str = "X-Contract-Boundary"

    def _resolve_boundary_name(self) -> str | None:
        # Endpoint-level override first
        b = getattr(self.endpoint, _CONTRACT_BOUNDARY_ATTR, None)
        if isinstance(b, str) and b:
            return b
        return self.default_boundary

    def get_route_handler(self) -> Callable[[Request], Any]:
        original = super().get_route_handler()

        async def handler(request: Request) -> Response:
            boundary_name = self._resolve_boundary_name()
            if not boundary_name:
                return await original(request)

            # Pin boundary for the full route handler lifecycle (deps + endpoint + response build)
            with override_contract_boundary(boundary_name, request=request, contract=DEFAULT_CONTRACT) as boundary:
                resp = await original(request)

                # Ensure observability & testability even if global middleware sets a different boundary
                resp.headers[self.header_name] = boundary_name
                return resp

        return handler


def make_contracted_route_class(
    *,
    boundary: str | None,
    header_name: str = "X-Contract-Boundary",
) -> Type[APIRoute]:
    """Factory that returns an APIRoute class with a router-level default boundary."""
    class _R(ContractedAPIRoute):
        default_boundary = boundary
        header_name = header_name

    return _R


def contracted_router(
    *,
    boundary: str,
    header_name: str = "X-Contract-Boundary",
    **kwargs: Any,
) -> APIRouter:
    """Create an APIRouter where *all routes* default to the given boundary."""
    return APIRouter(
        route_class=make_contracted_route_class(boundary=boundary, header_name=header_name),
        **kwargs,
    )
```

---

# 3) How you use this in practice

## A) Per-router pinning (public vs internal routers)

```python
from fastapi import FastAPI
from myproj.orjson_contract_runtime import OrjsonContractJSONResponse, install_fastapi_contract
from myproj.orjson_fastapi_contract import contracted_router, ContractBoundary, assert_roundtrip_if_debug

app = FastAPI(default_response_class=OrjsonContractJSONResponse)
install_fastapi_contract(app, context="http")  # global default via env/selector

public = contracted_router(boundary="http", prefix="/v1")
internal = contracted_router(boundary="artifact", prefix="/_internal")

@public.get("/healthz")
def healthz(boundary: ContractBoundary):
    payload = {"ok": True}
    assert_roundtrip_if_debug(boundary, payload)  # no-op unless CODEINTEL_JSON_DEBUG_ROUNDTRIP=1
    return payload

@internal.get("/dump_state")
def dump_state(boundary: ContractBoundary):
    payload = {"state": {"a": 1, "b": 2}}
    assert_roundtrip_if_debug(boundary, payload)
    return payload

app.include_router(public)
app.include_router(internal)
```

## B) Per-route override inside a pinned router

```python
from myproj.orjson_fastapi_contract import contracted_route

@public.get("/human_debug")
@contracted_route(boundary="cli")  # override router default for this one endpoint
def human_debug(boundary: ContractBoundary):
    payload = {"note": "pretty + stable (cli boundary)"}
    assert_roundtrip_if_debug(boundary, payload)
    return payload
```

**Decorator ordering matters**: `@contracted_route` must be the inner decorator so FastAPI registers the modified endpoint.

---

# 4) Integration test ergonomics (it “just works” with your prior helper)

Because `ContractedAPIRoute` forces the `X-Contract-Boundary` header to match the pinned boundary, your earlier `assert_contract_response(...)` pattern can reliably infer which boundary semantics to compare against—even if the app-wide middleware selector is set differently.

Example:

```python
def test_internal_boundary_is_pinned(client):
    r = client.get("/_internal/dump_state")
    assert r.headers["X-Contract-Boundary"] == "artifact"
    assert_contract_response(r, {"state": {"a": 1, "b": 2}})
```

---

# 5) Recommended “debug build” knobs

Set one env var to turn on roundtrip assertions inside handlers:

```bash
CODEINTEL_JSON_DEBUG_ROUNDTRIP=1 pytest
# or
CODEINTEL_JSON_DEBUG_ROUNDTRIP=1 uvicorn myproj.app:create_app
```

Then inside handlers, the one-liner:

```python
assert_roundtrip_if_debug(boundary, model_or_payload)
```

gives you **fast failure when:**

* UTF/surrogate issues sneak into strings
* you accidentally return non-serializable shapes
* non-str-key policies would collide (if the boundary policy uses OPT_NON_STR_KEYS + validation)

---

If you want the next increment after this: I can add a **`@contracted_router_group(boundary=...)` helper** that builds `APIRouter`s with consistent tags/prefixes and exposes a “contract surface map” (router → boundary → endpoints) for automated documentation and test generation.

Below is a **repo-ready `@contracted_router_group(...)` helper** + a **contract surface map** that gives you:

* **Consistent tags/prefixes** for routers created within a group
* A **surface inventory**: *router → (default boundary) → endpoints (effective boundary per route)*
* Utilities to **render Markdown docs** and to **emit endpoint cases** for pytest param-generation

This builds on your existing `contracted_router(...)` / `@contracted_route(...)` / `ContractedAPIRoute` from `src/myproj/orjson_fastapi_contract.py`.

---

# 1) New module: router groups + surface map

## `src/myproj/orjson_contract_surface.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute

from myproj.orjson_fastapi_contract import contracted_router


# NOTE: this must match the attribute used by @contracted_route in orjson_fastapi_contract.py
_CONTRACT_BOUNDARY_ATTR = "__contract_boundary__"


def _join_prefix(a: str, b: str) -> str:
    """Join URL prefixes without double slashes."""
    if not a:
        return b or ""
    if not b:
        return a
    return a.rstrip("/") + "/" + b.lstrip("/")


def _effective_boundary_for_route(route: APIRoute) -> Optional[str]:
    """Compute the effective pinned boundary for a route (endpoint override > router default)."""
    endpoint_override = getattr(route.endpoint, _CONTRACT_BOUNDARY_ATTR, None)
    if isinstance(endpoint_override, str) and endpoint_override:
        return endpoint_override

    default_boundary = getattr(route, "default_boundary", None)  # set by ContractedAPIRoute subclasses
    if isinstance(default_boundary, str) and default_boundary:
        return default_boundary

    return None


@dataclass(frozen=True, slots=True)
class EndpointSurface:
    router_name: str
    endpoint_name: str
    methods: tuple[str, ...]
    path: str
    boundary: Optional[str]
    tags: tuple[str, ...]
    operation_id: Optional[str]
    include_in_schema: bool


@dataclass(frozen=True, slots=True)
class RouterSurface:
    router_name: str
    prefix: str
    tags: tuple[str, ...]
    default_boundary: Optional[str]
    endpoints: tuple[EndpointSurface, ...]


@dataclass(frozen=True, slots=True)
class EndpointCase:
    """A minimal, test-friendly endpoint inventory record."""
    method: str
    path: str
    expected_boundary: Optional[str]
    router_name: str
    endpoint_name: str

    @property
    def id(self) -> str:
        b = self.expected_boundary or "unbounded"
        return f"{self.method} {self.path} [{b}]"


@dataclass(frozen=True, slots=True)
class ContractSurfaceMap:
    routers: tuple[RouterSurface, ...]

    def by_boundary(self) -> dict[str, list[EndpointSurface]]:
        out: dict[str, list[EndpointSurface]] = {}
        for r in self.routers:
            for ep in r.endpoints:
                b = ep.boundary or "unbounded"
                out.setdefault(b, []).append(ep)
        return out

    def endpoint_cases(
        self,
        *,
        methods: Optional[set[str]] = None,
        include_in_schema_only: bool = True,
    ) -> list[EndpointCase]:
        cases: list[EndpointCase] = []
        for r in self.routers:
            for ep in r.endpoints:
                if include_in_schema_only and not ep.include_in_schema:
                    continue
                for m in ep.methods:
                    if methods and m not in methods:
                        continue
                    cases.append(
                        EndpointCase(
                            method=m,
                            path=ep.path,
                            expected_boundary=ep.boundary,
                            router_name=ep.router_name,
                            endpoint_name=ep.endpoint_name,
                        )
                    )
        return cases

    def to_markdown(self) -> str:
        """Human-facing contract surface doc (router sections + endpoint table)."""
        lines: list[str] = []
        lines.append("# Contract surface map")
        lines.append("")
        for r in self.routers:
            lines.append(f"## Router: `{r.router_name}`")
            lines.append("")
            lines.append(f"- **prefix**: `{r.prefix or '/'}'")
            lines.append(f"- **default boundary**: `{r.default_boundary or 'unbounded'}`")
            lines.append(f"- **tags**: `{', '.join(r.tags) if r.tags else ''}`")
            lines.append("")
            lines.append("| method | path | effective boundary | endpoint | tags | in schema |")
            lines.append("|---|---|---|---|---|---|")
            for ep in sorted(r.endpoints, key=lambda e: (e.path, ",".join(e.methods))):
                tags = ", ".join(ep.tags)
                methods = ", ".join(ep.methods)
                b = ep.boundary or "unbounded"
                ins = "yes" if ep.include_in_schema else "no"
                lines.append(f"| {methods} | `{ep.path}` | `{b}` | `{ep.endpoint_name}` | {tags} | {ins} |")
            lines.append("")
        return "\n".join(lines)


class ContractedRouterGroup:
    """Build APIRouters with consistent boundary/prefix/tags and emit a contract surface map."""

    def __init__(
        self,
        *,
        name: str,
        boundary: str,
        prefix: str = "",
        tags: Sequence[str] = (),
        header_name: str = "X-Contract-Boundary",
    ) -> None:
        self.name = name
        self.boundary = boundary
        self.prefix = prefix
        self.tags = tuple(tags)
        self.header_name = header_name
        self._routers: dict[str, APIRouter] = {}

    @property
    def routers(self) -> Mapping[str, APIRouter]:
        return dict(self._routers)

    def router(
        self,
        router_name: str,
        *,
        prefix: str = "",
        tags: Sequence[str] = (),
        boundary: Optional[str] = None,
        **kwargs: Any,
    ) -> APIRouter:
        """Create (and register) a router. Group prefix/tags are applied automatically."""
        if router_name in self._routers:
            raise ValueError(f"Router already exists in group '{self.name}': {router_name}")

        full_prefix = _join_prefix(self.prefix, prefix)
        full_tags = tuple(dict.fromkeys([*self.tags, *tags]))  # stable de-dupe

        r = contracted_router(
            boundary=(boundary or self.boundary),
            prefix=full_prefix,
            tags=list(full_tags) if full_tags else None,
            header_name=self.header_name,
            **kwargs,
        )
        self._routers[router_name] = r
        return r

    def include_in(self, app: FastAPI) -> None:
        """Include all routers into the app (using their built prefixes/tags)."""
        for _, r in self._routers.items():
            app.include_router(r)

    def surface_map(self) -> ContractSurfaceMap:
        """Router → default boundary → endpoints (effective boundary per-route)."""
        router_surfaces: list[RouterSurface] = []
        for router_name, r in self._routers.items():
            endpoints: list[EndpointSurface] = []
            for route in r.routes:
                if not isinstance(route, APIRoute):
                    continue
                boundary = _effective_boundary_for_route(route)
                methods = tuple(sorted(route.methods or []))
                tags = tuple(route.tags or [])
                operation_id = getattr(route, "operation_id", None)
                include_in_schema = bool(getattr(route, "include_in_schema", True))

                endpoints.append(
                    EndpointSurface(
                        router_name=router_name,
                        endpoint_name=route.name,
                        methods=methods,
                        path=_join_prefix(r.prefix or "", route.path),
                        boundary=boundary,
                        tags=tags,
                        operation_id=operation_id if isinstance(operation_id, str) else None,
                        include_in_schema=include_in_schema,
                    )
                )

            default_boundary = getattr(getattr(r, "route_class", None), "default_boundary", None)
            router_surfaces.append(
                RouterSurface(
                    router_name=router_name,
                    prefix=r.prefix or "",
                    tags=tuple(r.tags or []),
                    default_boundary=default_boundary if isinstance(default_boundary, str) else None,
                    endpoints=tuple(endpoints),
                )
            )

        return ContractSurfaceMap(routers=tuple(router_surfaces))


def contracted_router_group(
    *,
    name: str,
    boundary: str,
    prefix: str = "",
    tags: Sequence[str] = (),
    header_name: str = "X-Contract-Boundary",
) -> Callable[[Callable[[ContractedRouterGroup], Any]], ContractedRouterGroup]:
    """Decorator-style group builder.

    Usage:
        @contracted_router_group(name="public", boundary="http", prefix="/v1", tags=["v1"])
        def public(g):
            users = g.router("users", prefix="/users", tags=["users"])
            ...
        # `public` becomes the ContractedRouterGroup instance.
    """
    def deco(builder: Callable[[ContractedRouterGroup], Any]) -> ContractedRouterGroup:
        g = ContractedRouterGroup(
            name=name,
            boundary=boundary,
            prefix=prefix,
            tags=tags,
            header_name=header_name,
        )
        builder(g)
        return g
    return deco
```

---

# 2) How you use it (routers + per-route boundary override)

## Example: `src/myproj/routers/public.py`

```python
from __future__ import annotations

from myproj.orjson_contract_surface import contracted_router_group
from myproj.orjson_fastapi_contract import ContractBoundary, assert_roundtrip_if_debug, contracted_route


@contracted_router_group(name="public", boundary="http", prefix="/v1", tags=["v1"])
def public(g):
    users = g.router("users", prefix="/users", tags=["users"])

    @users.get("/{user_id}")
    def get_user(user_id: str, boundary: ContractBoundary):
        payload = {"id": user_id}
        assert_roundtrip_if_debug(boundary, payload)
        return payload

    @users.get("/_human", include_in_schema=False)
    @contracted_route(boundary="cli")  # endpoint-level override
    def human_debug(boundary: ContractBoundary):
        payload = {"note": "pretty boundary for humans"}
        assert_roundtrip_if_debug(boundary, payload)
        return payload
```

## App factory: include groups + emit surface map

```python
from fastapi import FastAPI
from myproj.orjson_contract_runtime import OrjsonContractJSONResponse, install_fastapi_contract
from myproj.routers.public import public

def create_app() -> FastAPI:
    app = FastAPI(default_response_class=OrjsonContractJSONResponse)
    install_fastapi_contract(app, context="http")  # global default selector/middleware

    public.include_in(app)

    # Optional: generate docs at startup or in a CLI command
    surface = public.surface_map()
    # print(surface.to_markdown())

    return app
```

---

# 3) Automated documentation generation (Markdown)

## Minimal CLI snippet (or script)

```python
from pathlib import Path
from myproj.routers.public import public

md = public.surface_map().to_markdown()
Path("docs/contract_surface_public.md").write_text(md, encoding="utf-8")
```

This produces a stable table per router with **method/path/effective boundary/tags/include_in_schema**.

---

# 4) Automated test generation (pytest parametrization)

You already have `assert_contract_response(...)` from earlier. Here’s how to generate endpoint cases from the surface map:

```python
import pytest
from fastapi.testclient import TestClient

from myproj.app import create_app
from myproj.routers.public import public
from tests.contract_http_assertions import assert_contract_response

SURFACE = public.surface_map()
CASES = SURFACE.endpoint_cases(methods={"GET"}, include_in_schema_only=True)

@pytest.fixture
def client():
    return TestClient(create_app())

@pytest.mark.parametrize("case", CASES, ids=lambda c: c.id)
def test_contract_boundary_header_and_equivalence(client: TestClient, case):
    # NOTE: This assumes GET routes are safe to call without extra auth/body.
    r = client.request(case.method, case.path)
    assert r.status_code < 500

    # This checks parseability + JSON-equivalence to itself (baseline sanity),
    # and uses the header to pick the boundary comparator.
    assert_contract_response(r, r.content)

    # If you want to assert the *expected* boundary is in the header:
    if case.expected_boundary:
        assert r.headers.get("X-Contract-Boundary") == case.expected_boundary
```

If you want deeper verification, you can maintain a small dict of “expected payloads” by `case.path` and pass that as `expected` to `assert_contract_response`.

---

# 5) “Contract surface map” across multiple groups

If you adopt router groups broadly (recommended), you can union them:

```python
from myproj.routers.public import public
from myproj.routers.internal import internal

maps = [public.surface_map(), internal.surface_map()]
all_endpoints = [ep for m in maps for r in m.routers for ep in r.endpoints]
# Or write a tiny merger if you want a single ContractSurfaceMap.
```

---

If you want one more increment after this: I can add a **surface-merger + OpenAPI cross-check** that asserts:

* every route has a pinned boundary (no “unbounded”)
* every OpenAPI path/method is represented in the surface map (and vice versa)
* and a generated `tests/test_contract_surface_generated.py` file that auto-parametrizes safe GET endpoints.

Below is the “one more increment” bundle:

1. **surface merger + OpenAPI bijection cross-check**
2. **assert every schema route is pinned to a boundary (no `unbounded`)**
3. **a generator that writes `tests/test_contract_surface_generated.py`** (parametrized “safe GET” probes)

---

## 0) Small bugfix you should apply first (path prefix doubling)

In your earlier `ContractedRouterGroup.surface_map()` implementation, `APIRoute.path` already includes the router prefix, so joining `router.prefix + route.path` **double-prefixes** paths and will break OpenAPI cross-checks.

### Patch `src/myproj/orjson_contract_surface.py`

```diff
- path=_join_prefix(r.prefix or "", route.path),
+ path=route.path,
```

(Everything else can remain the same.)

---

## 1) Surface merger + OpenAPI cross-check utilities

### `src/myproj/orjson_contract_openapi_audit.py`

```python
from __future__ import annotations

from dataclasses import replace
from typing import Iterable, Optional, Sequence, Tuple

from fastapi import FastAPI
from fastapi.routing import APIRoute

from myproj.orjson_contract_surface import (
    ContractSurfaceMap,
    RouterSurface,
    EndpointSurface,
    EndpointCase,
)

# must match @contracted_route attr name from orjson_fastapi_contract.py
_CONTRACT_BOUNDARY_ATTR = "__contract_boundary__"

_OPENAPI_METHODS = {"get", "post", "put", "delete", "patch", "options", "head", "trace"}


def merge_surface_maps(*maps: ContractSurfaceMap) -> ContractSurfaceMap:
    """Merge multiple ContractSurfaceMap objects into one (stable, name-conflict safe)."""
    if not maps:
        return ContractSurfaceMap(routers=())

    out: list[RouterSurface] = []
    seen: dict[str, int] = {}

    for m in maps:
        for r in m.routers:
            base = r.router_name
            n = seen.get(base, 0) + 1
            seen[base] = n
            router_name = base if n == 1 else f"{base}__{n}"

            # rewrite endpoint.router_name if we renamed this router
            endpoints = tuple(
                replace(ep, router_name=router_name) for ep in r.endpoints
            )
            out.append(
                replace(r, router_name=router_name, endpoints=endpoints)
            )

    return ContractSurfaceMap(routers=tuple(out))


def surface_map_from_fastapi(app: FastAPI, *, router_name: str = "app") -> ContractSurfaceMap:
    """Build a ContractSurfaceMap directly from FastAPI routes (whole-app view).

    This is useful when you want to avoid importing groups and just audit “what the app actually serves”.
    """
    endpoints: list[EndpointSurface] = []

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue

        override = getattr(route.endpoint, _CONTRACT_BOUNDARY_ATTR, None)
        boundary: Optional[str] = override if isinstance(override, str) and override else None
        if boundary is None:
            # ContractedAPIRoute subclasses expose default_boundary (class attribute)
            db = getattr(route, "default_boundary", None)
            boundary = db if isinstance(db, str) and db else None

        endpoints.append(
            EndpointSurface(
                router_name=router_name,
                endpoint_name=route.name,
                methods=tuple(sorted(route.methods or ())),
                path=route.path,
                boundary=boundary,
                tags=tuple(route.tags or ()),
                operation_id=route.operation_id if isinstance(route.operation_id, str) else None,
                include_in_schema=bool(route.include_in_schema),
            )
        )

    rs = RouterSurface(
        router_name=router_name,
        prefix="",
        tags=(),
        default_boundary=None,
        endpoints=tuple(endpoints),
    )
    return ContractSurfaceMap(routers=(rs,))


def surface_ops(
    surface: ContractSurfaceMap,
    *,
    include_in_schema_only: bool = True,
) -> set[Tuple[str, str]]:
    """(METHOD, /path) pairs from the surface map."""
    ops: set[Tuple[str, str]] = set()
    for r in surface.routers:
        for ep in r.endpoints:
            if include_in_schema_only and not ep.include_in_schema:
                continue
            for m in ep.methods:
                ops.add((m.upper(), ep.path))
    return ops


def openapi_ops(app: FastAPI) -> set[Tuple[str, str]]:
    """(METHOD, /path) pairs from app.openapi(). Only real HTTP methods."""
    schema = app.openapi()
    paths = schema.get("paths", {})
    ops: set[Tuple[str, str]] = set()

    if not isinstance(paths, dict):
        return ops

    for path, item in paths.items():
        if not isinstance(item, dict):
            continue
        for method, op in item.items():
            if method.lower() in _OPENAPI_METHODS:
                ops.add((method.upper(), str(path)))
    return ops


def assert_all_schema_routes_bounded(surface: ContractSurfaceMap) -> None:
    """Assert: every include_in_schema endpoint has an effective boundary (no 'unbounded')."""
    unbounded: list[str] = []

    for r in surface.routers:
        for ep in r.endpoints:
            if not ep.include_in_schema:
                continue
            if not ep.boundary:
                unbounded.append(f"{','.join(ep.methods)} {ep.path} (router={r.router_name}, endpoint={ep.endpoint_name})")

    if unbounded:
        details = "\n".join(f"- {x}" for x in sorted(unbounded))
        raise AssertionError(
            "Contract boundary audit failed: some schema routes are not pinned to a boundary.\n"
            "Fix by putting them under contracted_router(...) / contracted_router_group(...) or add @contracted_route(boundary=...).\n\n"
            f"{details}"
        )


def assert_openapi_surface_bijection(app: FastAPI, surface: ContractSurfaceMap) -> None:
    """Assert: OpenAPI (paths/methods) equals surface(include_in_schema routes) exactly."""
    o = openapi_ops(app)
    s = surface_ops(surface, include_in_schema_only=True)

    missing_in_surface = sorted(o - s)
    extra_in_surface = sorted(s - o)

    if missing_in_surface or extra_in_surface:
        msg = ["OpenAPI ↔ surface map mismatch detected."]

        if missing_in_surface:
            msg.append("\nPresent in OpenAPI but missing from surface map:")
            msg.extend([f"  - {m} {p}" for (m, p) in missing_in_surface])

        if extra_in_surface:
            msg.append("\nPresent in surface map but missing from OpenAPI:")
            msg.extend([f"  - {m} {p}" for (m, p) in extra_in_surface])

        msg.append(
            "\nNotes:\n"
            "- OpenAPI includes only include_in_schema routes.\n"
            "- If you build surfaces from router groups, ensure every router included in the app is represented.\n"
        )
        raise AssertionError("\n".join(msg))


def select_safe_get_cases(surface: ContractSurfaceMap) -> list[EndpointCase]:
    """Heuristic ‘safe GET’ list: include_in_schema + GET + no path params."""
    seen: set[Tuple[str, str]] = set()
    cases: list[EndpointCase] = []

    for r in surface.routers:
        for ep in r.endpoints:
            if not ep.include_in_schema:
                continue
            if "GET" not in ep.methods:
                continue
            if "{" in ep.path or "}" in ep.path:
                continue

            key = ("GET", ep.path)
            if key in seen:
                continue
            seen.add(key)

            cases.append(
                EndpointCase(
                    method="GET",
                    path=ep.path,
                    expected_boundary=ep.boundary,
                    router_name=ep.router_name,
                    endpoint_name=ep.endpoint_name,
                )
            )

    cases.sort(key=lambda c: c.path)
    return cases


def render_generated_safe_get_test(
    *,
    app_factory_import: str,
    cases: Sequence[EndpointCase],
    header_name: str = "X-Contract-Boundary",
) -> str:
    """Return file contents for tests/test_contract_surface_generated.py."""
    mod, fn = app_factory_import.split(":", 1)
    import_stmt = f"from {mod} import {fn}"

    lines: list[str] = []
    lines.append("# AUTO-GENERATED FILE. DO NOT EDIT BY HAND.")
    lines.append("# Generated by: tools/generate_contract_surface_tests.py")
    lines.append("")
    lines.append("from __future__ import annotations")
    lines.append("")
    lines.append("import pytest")
    lines.append("from fastapi.testclient import TestClient")
    lines.append("")
    lines.append(import_stmt)
    lines.append("from tests.contract_http_assertions import assert_contract_response")
    lines.append("")
    lines.append(f'HEADER_NAME = "{header_name}"')
    lines.append("")
    lines.append("CASES = [")
    for c in cases:
        b = c.expected_boundary or ""
        lines.append(f'    ("{c.method}", "{c.path}", "{b}"),')
    lines.append("]")
    lines.append("")
    lines.append("@pytest.fixture(scope='module')")
    lines.append("def client():")
    lines.append(f"    return TestClient({fn}())")
    lines.append("")
    lines.append("@pytest.mark.parametrize('method,path,expected_boundary', CASES, ids=lambda t: f\"{t[0]} {t[1]} [{t[2] or 'unbounded'}]\")")
    lines.append("def test_safe_get_contract_surface(client: TestClient, method: str, path: str, expected_boundary: str) -> None:")
    lines.append("    r = client.request(method, path)")
    lines.append("    # This probe is about contract invariants; allow auth/404/etc, but never 5xx.")
    lines.append("    assert r.status_code < 500")
    lines.append("")
    lines.append("    # Boundary pinning should always surface as a header (ContractedAPIRoute).")
    lines.append("    hdr = r.headers.get(HEADER_NAME)")
    lines.append("    if expected_boundary:")
    lines.append("        assert hdr == expected_boundary")
    lines.append("    else:")
    lines.append("        # If you ever see this: you have an 'unbounded' route that isn't pinned.")
    lines.append("        assert hdr is not None")
    lines.append("")
    lines.append("    # Only assert JSON parseability when the response declares JSON.")
    lines.append("    ct = (r.headers.get('content-type') or '').lower()")
    lines.append("    if 'application/json' in ct:")
    lines.append("        assert_contract_response(r, r.content)")
    lines.append("")

    return "\n".join(lines)
```

---

## 2) A hard CI-style test: pinned-boundary + OpenAPI bijection

### `tests/test_contract_surface_openapi_crosscheck.py`

```python
from __future__ import annotations

from myproj.app import create_app
from myproj.orjson_contract_openapi_audit import (
    surface_map_from_fastapi,
    assert_all_schema_routes_bounded,
    assert_openapi_surface_bijection,
)

def test_contract_surface_matches_openapi_and_is_bounded() -> None:
    app = create_app()
    surface = surface_map_from_fastapi(app)

    # 1) every include_in_schema route must be pinned (no “unbounded”)
    assert_all_schema_routes_bounded(surface)

    # 2) OpenAPI paths/methods must match the surface map exactly
    assert_openapi_surface_bijection(app, surface)
```

If you prefer group-based surfaces, replace `surface_map_from_fastapi(app)` with:

```python
# surface = merge_surface_maps(public.surface_map(), internal.surface_map(), ...)
```

…and keep the same assertions.

---

## 3) Generator that writes `tests/test_contract_surface_generated.py`

### `tools/generate_contract_surface_tests.py`

```python
from __future__ import annotations

import argparse
import importlib
from pathlib import Path
from typing import Callable

from fastapi import FastAPI

from myproj.orjson_contract_openapi_audit import (
    surface_map_from_fastapi,
    assert_all_schema_routes_bounded,
    assert_openapi_surface_bijection,
    select_safe_get_cases,
    render_generated_safe_get_test,
)


def _load_app_factory(spec: str) -> Callable[[], FastAPI]:
    if ":" not in spec:
        raise SystemExit("app factory must be in 'module:function' form, e.g. myproj.app:create_app")
    mod, fn = spec.split(":", 1)
    m = importlib.import_module(mod)
    f = getattr(m, fn, None)
    if not callable(f):
        raise SystemExit(f"factory '{spec}' is not callable")
    return f


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--app-factory", required=True, help="module:function, e.g. myproj.app:create_app")
    p.add_argument("--out", default="tests/test_contract_surface_generated.py")
    p.add_argument("--header-name", default="X-Contract-Boundary")
    args = p.parse_args()

    factory = _load_app_factory(args.app_factory)
    app = factory()

    # Use the whole-app surface (or swap in merged group surfaces if you prefer).
    surface = surface_map_from_fastapi(app)

    # Audit gates (fail fast):
    assert_all_schema_routes_bounded(surface)
    assert_openapi_surface_bijection(app, surface)

    cases = select_safe_get_cases(surface)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    content = render_generated_safe_get_test(
        app_factory_import=args.app_factory,
        cases=cases,
        header_name=args.header_name,
    )
    out_path.write_text(content, encoding="utf-8")
    print(f"Wrote {out_path} with {len(cases)} safe GET probes.")


if __name__ == "__main__":
    main()
```

Run it:

```bash
python tools/generate_contract_surface_tests.py --app-factory myproj.app:create_app
pytest -q
```

---

## 4) What the generated file looks like

The generator will produce **exactly**:

* `tests/test_contract_surface_generated.py`
* a static `CASES = [...]` list of safe GET paths
* a single parametrized probe that checks:

  * no 5xx
  * `X-Contract-Boundary` matches expected pinned boundary
  * if JSON response, parseability + self-equivalence via your existing `assert_contract_response(...)`

---

### Practical notes (so this doesn’t become flaky)

* “Safe GET” is intentionally conservative: **no path params**, **include_in_schema only**.
* If you still have GET endpoints that require auth and return non-JSON errors, the probe will still pass (it only enforces JSON assertions when `Content-Type` is JSON), while still enforcing boundary headers and “no 5xx”.

If you want one more increment after this, the most useful would be a **safe-endpoint annotation** (e.g., `@contract_probe_safe()`), so the generator uses explicit metadata instead of heuristics—making the generated test suite deterministic even as your API grows.
