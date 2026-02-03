## B) High-level loading surface (object construction)

### B1) The entrypoints and what they *guarantee*

PyYAML’s “high-level loading” functions fall into **single-document** vs **multi-document** APIs, and into **loader tiers** (safe/full/unsafe) that control which tags get resolved into Python objects.

#### API signatures (practical, version-safe)

**Always treat `Loader` as mandatory** when calling `yaml.load()` directly:

* PyYAML **6.0+**: `yaml.load(f)` raises `TypeError: load() missing 1 required positional argument: 'Loader'`. ([GitHub][1])
* PyYAML **5.1+**: calling `yaml.load()` without a loader is deprecated and warns; pass `Loader=` or use sugar functions. ([GitHub][2])

**Canonical calls you should standardize on:**

```python
import yaml

obj  = yaml.load(stream, Loader=yaml.SafeLoader)     # explicit tier
objs = yaml.load_all(stream, Loader=yaml.SafeLoader) # explicit tier, multi-doc
```

PyYAML’s own wrapper functions are literally thin calls into `load()` / `load_all()` with a specific loader. ([Chromium Git Repositories][3])

---

### B2) Single-document entrypoints (`load`, `safe_load`, `full_load`, `unsafe_load`)

#### `yaml.load(stream, Loader=...) -> Any`

**Contract:**

* Parses **exactly one** YAML document from the stream and constructs the corresponding Python object. ([Chromium Git Repositories][3])
* If the stream contains **multiple documents**, you’ll hit a `ComposerError` (“expected a single document in the stream”) in normal usage—use `load_all` instead. ([GitHub][4])
* If the stream has **no documents**, `load()` returns `None` (per the reference docs). ([pyyaml.org][5])

**Typical “explicit Loader” usage:**

```python
import yaml
from pathlib import Path

text = Path("config.yaml").read_text(encoding="utf-8")
cfg = yaml.load(text, Loader=yaml.SafeLoader)
```

#### `yaml.safe_load(stream) -> Any`

**Contract:**

* Single-doc parse + construct.
* Resolves **only basic YAML tags**; described as safe for untrusted input. ([Chromium Git Repositories][3])
* Equivalent to `yaml.load(stream, Loader=yaml.SafeLoader)`. ([Chromium Git Repositories][3])

**Default choice for config files / external input:**

```python
import yaml

cfg = yaml.safe_load(open("config.yaml", "r", encoding="utf-8"))
```

#### `yaml.full_load(stream) -> Any`

**Contract:**

* Single-doc parse + construct.
* Resolves **all tags except those known to be unsafe on untrusted input**. ([Chromium Git Repositories][3])
* Equivalent to `yaml.load(stream, Loader=yaml.FullLoader)`. ([Chromium Git Repositories][3])

**Use when you need “more YAML features / tags” than SafeLoader provides, but still want to avoid the truly dangerous tag set:**

```python
import yaml

obj = yaml.full_load(stream)
```

(You’ll usually document *why* SafeLoader is insufficient in the schema/tag chapter.)

#### `yaml.unsafe_load(stream) -> Any`

**Contract:**

* Single-doc parse + construct.
* Resolves **all tags**, including those known unsafe; only for fully trusted input. ([Chromium Git Repositories][3])
* Equivalent to `yaml.load(stream, Loader=yaml.UnsafeLoader)`. ([Chromium Git Repositories][3])

**Only for trusted artifacts you control end-to-end:**

```python
import yaml

obj = yaml.unsafe_load(stream)
```

---

### B3) Multi-document entrypoints (`load_all`, `safe_load_all`, `full_load_all`, `unsafe_load_all`)

A **YAML stream** can contain multiple documents separated by `---` markers; PyYAML exposes this via `_all` functions. 

#### `yaml.load_all(stream, Loader=...) -> Iterator[Any]`

**Contract:**

* Produces a **generator/iterator**: it `yield`s each document’s Python object one-by-one. ([Chromium Git Repositories][3])
* Internally loops `while loader.check_data(): yield loader.get_data()` and disposes the loader in a `finally:` block. ([Chromium Git Repositories][3])
* If there are no documents, iteration is empty (i.e., an empty generator). ([Chromium Git Repositories][3])

**Consume it streaming-style (preferred for large inputs):**

```python
import yaml

with open("multi.yaml", "r", encoding="utf-8") as f:
    for doc in yaml.load_all(f, Loader=yaml.SafeLoader):
        process(doc)
```

**Materialize explicitly (small inputs, need random access):**

```python
docs = list(yaml.load_all(stream, Loader=yaml.SafeLoader))
```

#### Tiered `_all` wrappers

These are just “tier defaults” calling `load_all(..., Loader=<tier>)`. ([Chromium Git Repositories][3])

* `yaml.safe_load_all(stream)` → `SafeLoader` ([Chromium Git Repositories][3])
* `yaml.full_load_all(stream)` → `FullLoader` ([Chromium Git Repositories][3])
* `yaml.unsafe_load_all(stream)` → `UnsafeLoader` ([Chromium Git Repositories][3])

---

### B4) Input types & stream handling (str / bytes / file objects)

#### Accepted “stream” types

PyYAML documents `yaml.load` as accepting:

* a **Unicode string** (`str`)
* a **byte string** (`bytes`)
* an **open binary file object**
* an **open text file object** 

That’s the practical rule: if it’s a `str`/`bytes`, it’s treated as the whole stream; if it’s file-like, PyYAML reads from it.

#### Encoding + BOM handling

For **bytes** and **binary streams**, PyYAML requires the bytes be encoded as **UTF-8, UTF-16-BE, or UTF-16-LE** and says it detects encoding via **BOM**, defaulting to UTF-8 when no BOM is present. 

**Implication:** if you have “unknown but likely UTF-*” YAML bytes, you can hand bytes directly:

```python
import yaml

data = yaml.safe_load(b"\xef\xbb\xbfkey: value\n")  # UTF-8 BOM example
```

For **text streams** (`open(..., "r")` returning `str`), *Python* performs decoding **before** PyYAML sees the data. If you don’t set `encoding=...`, you may accidentally decode using a platform default and get `UnicodeDecodeError`. A real-world report shows failure caused by `open(config, 'r')` decoding as CP-1251 on Windows. ([GitHub][6])

**Best-practice file opens (pick one):**

1. **Text mode, explicit encoding (most common):**

```python
with open("config.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)
```

2. **Binary mode, let PyYAML BOM-detect (useful if BOM might appear):**

```python
with open("config.yaml", "rb") as f:
    cfg = yaml.safe_load(f)
```

(Works because PyYAML supports open binary file objects + BOM detection. )

---

### B5) “First document vs all documents” semantics (and why it matters)

#### Single-doc APIs are “exactly one document”

If you point `safe_load` / `load(...get_single_data...)` at a multi-doc YAML stream, you’ll hit the classic composer failure (“expected a single document in the stream”). ([GitHub][4])

**Operational pattern:** when your inputs might be multi-doc, *always* use `_all`:

```python
docs = yaml.safe_load_all(stream)
for doc in docs:
    ...
```

#### `_all` APIs are generators: lifetime + large streams

Because `load_all` is a generator, the **source stream must remain open** while you iterate.

**Correct:**

```python
with open("multi.yaml", "r", encoding="utf-8") as f:
    for doc in yaml.safe_load_all(f):
        ...
```

**Common footgun:** returning the generator from inside a `with` block:

```python
def docs_from_path(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load_all(f)  # BUG: f closes, generator later breaks
```

If you need to return docs, return a **materialized list** (small), or return a generator that opens the file internally (bigger pattern; you’ll document it in your “streaming patterns” chapter).

#### Early-exit + cleanup (if you stop consuming)

PyYAML’s generator-based loaders dispose resources in a `finally:` when the generator closes. ([Chromium Git Repositories][3])
If you break early and keep the generator around, explicitly close it:

```python
import yaml

with open("multi.yaml", "r", encoding="utf-8") as f:
    it = yaml.safe_load_all(f)
    first = next(it, None)
    # ... decide you’re done ...
    it.close()  # ensures loader.dispose() runs promptly
```

---

### B6) Quick “command-style” recipes (copy/paste)

**Single doc, safe tier:**

```bash
python -c "import yaml, pathlib; print(yaml.safe_load(pathlib.Path('config.yaml').read_text(encoding='utf-8')))"
```

**Multi-doc, safe tier (streaming):**

```bash
python - <<'PY'
import yaml
with open("multi.yaml", "r", encoding="utf-8") as f:
    for i, doc in enumerate(yaml.safe_load_all(f), 1):
        print(i, type(doc), doc if i <= 2 else '...')
PY
```

---

If you want, the next subsection to do in the same dense style is **“B7) Loader selection patterns”** (SafeLoader vs FullLoader vs UnsafeLoader, plus `CLoader` fast-path), but I’ll stop here to keep this chapter strictly on the *high-level loading surface* you listed.

[1]: https://github.com/yaml/pyyaml/tags "Tags · yaml/pyyaml · GitHub"
[2]: https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load%28input%29-Deprecation "PyYAML yaml.load(input) Deprecation · yaml/pyyaml Wiki · GitHub"
[3]: https://chromium.googlesource.com/chromium/src.git/%2B/refs/tags/138.0.7167.0/third_party/pyyaml/__init__.py "third_party/pyyaml/__init__.py - chromium/src.git - Git at Google"
[4]: https://github.com/yaml/pyyaml/issues/689 "Why safe_load_all and load_all return empty generator? · Issue #689 · yaml/pyyaml · GitHub"
[5]: https://pyyaml.org/wiki/PyYAMLDocumentation?utm_source=chatgpt.com "PyYAML Documentation"
[6]: https://github.com/yaml/pyyaml/issues/123 "yaml.load does not support encodings different from current system encoding, cannot you add it? · Issue #123 · yaml/pyyaml · GitHub"

## B7) Loader selection patterns

### B7.0 What a “Loader” really controls

In PyYAML, a Loader is effectively **(parser pipeline) + (constructor policy)**:

* **Parser pipeline:** pure-Python vs LibYAML (C) scanner/parser (`CSafeLoader`, `CFullLoader`, … exist only if LibYAML bindings were built). 
* **Constructor policy:** which YAML tags are permitted to construct which Python objects.

  * `SafeLoader` uses `SafeConstructor` (standard YAML types; unknown tags raise). ([Chromium Git Repositories][1])
  * `FullLoader` uses `FullConstructor` (adds *some* Python tags like `python/tuple`, `python/name:` but **not** object instantiation tags). ([Chromium Git Repositories][1])
  * `Loader` / `UnsafeLoader` uses `Constructor` which is explicitly “same as `UnsafeConstructor`” (adds `python/object:*`, `python/object/new:*`, `python/object/apply:*`, `python/module:*`, etc.). ([Chromium Git Repositories][1])

PyYAML’s docs are blunt: `yaml.load` on untrusted data is “as powerful as pickle” and may call arbitrary Python functions; prefer `safe_load` for untrusted input. 

---

### B7.1 Decision matrix (pick by *trust boundary* + *tag requirements*)

**Rule 0 (default):** if any part of the YAML comes from outside your full control (users, network, “config file someone can edit”), start with **Safe**.

| Use case                                                         | Recommended API                                                   | Loader class                                            | Why                                                                                                                                                      |
| ---------------------------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Untrusted / external YAML                                        | `yaml.safe_load()` / `yaml.safe_load_all()`                       | `SafeLoader` / `CSafeLoader`                            | Only standard YAML tags; no arbitrary Python object construction.                                                                                        |
| Trusted YAML needing a few Python-y tags (e.g. `!!python/tuple`) | `yaml.load(..., Loader=yaml.FullLoader)`                          | `FullLoader` / `CFullLoader`                            | Adds selected Python tags like `python/tuple` and `python/name:` without enabling `python/object:*` instantiation tags. ([Chromium Git Repositories][2]) |
| “YAML as pickle” (round-trip arbitrary Python objects)           | `yaml.unsafe_load()` / `yaml.load(..., Loader=yaml.UnsafeLoader)` | `UnsafeLoader` / `Loader` / `CUnsafeLoader` / `CLoader` | Enables `python/object:*` / `python/object/new:*` / `python/module:*` etc. Use should be rare. ([Chromium Git Repositories][1])                          |

**Version policy note:** calling `yaml.load()` without `Loader=` is deprecated (and in newer versions effectively requires explicit Loader). ([GitHub][3])

---

### B7.2 Safe tier (`SafeLoader` / `CSafeLoader`) — what you get, what you *don’t*

#### What SafeLoader constructs

`SafeConstructor` registers the standard YAML tags (null/bool/int/float/binary/timestamp/seq/map/etc.). ([Chromium Git Repositories][2])
So this is “YAML as data”: dict/list/str/numbers/bools/None, and also YAML-native extras like timestamps. ([Chromium Git Repositories][2])

#### What SafeLoader rejects

Any unknown / Python-specific tag is a `ConstructorError` (“could not determine a constructor for the tag …”) because undefined tags route to `construct_undefined`. ([Chromium Git Repositories][2])

#### Safe loader selection (pure-Python vs C fast path)

Use **CSafeLoader if present**, otherwise SafeLoader:

```python
import yaml

Safe = getattr(yaml, "CSafeLoader", yaml.SafeLoader)

obj = yaml.load(stream, Loader=Safe)
# or: yaml.safe_load(stream)  # sugar
```

C-safe availability is documented as “only if you build LibYAML bindings,” and the C loader classes exist (`CSafeLoader`, `CFullLoader`, `CUnsafeLoader`, `CLoader`, …). ([PyYAML][4])

---

### B7.3 Full tier (`FullLoader` / `CFullLoader`) — the *actual* delta vs Safe

The difference is **not** “more YAML 1.1” so much as **additional Python tag constructors**.

From `FullConstructor` registration:

* adds `tag:yaml.org,2002:python/tuple` → `tuple(...)`
* adds `tag:yaml.org,2002:python/name:` → resolve an already-imported name
* adds python scalar tags (`python/bytes`, `python/complex`, …) ([Chromium Git Repositories][2])

But **FullConstructor does *not* register** `python/object:*`, `python/object/new:*`, `python/object/apply:*`, `python/module:*`. Those are only registered by `UnsafeConstructor`. ([Chromium Git Repositories][2])

So: **FullLoader is “SafeLoader + a curated set of Python tags”**, which is why it can parse `!!python/tuple` where SafeLoader fails. ([Chromium Git Repositories][2])

Practical selection pattern:

```python
import yaml

Full = getattr(yaml, "CFullLoader", yaml.FullLoader)  # if libyaml bindings exist
obj = yaml.load(stream, Loader=Full)
# or: yaml.full_load(stream)  # sugar
```

**Security posture:** even when CVEs are patched, FullLoader still crosses a trust boundary more than SafeLoader; distros have historically warned about processing untrusted input with FullLoader/full_load in vulnerable version ranges. ([Gentoo Security][5])

---

### B7.4 Unsafe tier (`UnsafeLoader` / `Loader` / `CUnsafeLoader` / `CLoader`) — what flips on

Two key facts that make this tier “deserialization-dangerous”:

1. `UnsafeLoader` is explicitly “the same as Loader” (kept for compatibility) and both wire in `Constructor`. ([Chromium Git Repositories][1])
2. `Constructor` is literally “same as `UnsafeConstructor`”. ([Chromium Git Repositories][2])

`UnsafeConstructor` registers multi-constructors for:

* `tag:yaml.org,2002:python/module:`
* `tag:yaml.org,2002:python/object:`
* `tag:yaml.org,2002:python/object/new:`
* `tag:yaml.org,2002:python/object/apply:` ([Chromium Git Repositories][2])

That is the “YAML as pickle” surface area.

**Hard rule:** only use if the YAML is an internal artifact and you already treat it like executable input (equivalent risk class to unpickling). PyYAML’s own docs repeatedly steer untrusted input to `safe_load`. 

---

### B7.5 LibYAML (C) fast-path — correct usage (and the common trap)

#### Build/runtime prerequisite

LibYAML bindings are optional; docs show building them and then using C-based parser/emitter. 

#### Correct “fast + safe” selector

C variants exist for each tier: `CSafeLoader`, `CFullLoader`, `CUnsafeLoader`, plus `CLoader`. ([Chromium Git Repositories][6])

**Trap:** many snippets import `CLoader` for speed. But `CLoader` is wired to `Constructor`, which is unsafe (same as `UnsafeConstructor`). ([Chromium Git Repositories][6])

So your “fast path” should usually be:

```python
import yaml

Safe = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
Full = getattr(yaml, "CFullLoader", yaml.FullLoader)
Unsafe = getattr(yaml, "CUnsafeLoader", getattr(yaml, "UnsafeLoader", yaml.Loader))
```

#### Parity warning

PyYAML notes “subtle (but not really significant) differences” between pure-Python and LibYAML-based parsers/emitters—treat switching to C as something you regression-test (golden fixtures for configs). 

---

### B7.6 “Safer than FullLoader” pattern: extend SafeLoader surgically

If you only need *one* extra tag (classic example: `!!python/tuple`) you can keep SafeLoader and add a constructor:

```python
import yaml

class SafePlusTuple(yaml.SafeLoader):
    pass

SafePlusTuple.add_constructor(
    "tag:yaml.org,2002:python/tuple",
    lambda loader, node: tuple(loader.construct_sequence(node)),
)

obj = yaml.load(stream, Loader=getattr(yaml, "CSafeLoader", SafePlusTuple))
```

This mirrors how `FullConstructor` implements tuples (`tuple(self.construct_sequence(node))`) and avoids enabling the broader unsafe tag surface. ([Chromium Git Repositories][2])

---

### B7.7 Ops/CLI snippets (quick checks)

**Check which C loaders exist in your runtime:**

```bash
python - <<'PY'
import yaml
for name in ["CSafeLoader","CFullLoader","CUnsafeLoader","CLoader"]:
    print(name, hasattr(yaml, name))
PY
```

**Run with safe loader explicitly (useful when suppressing YAMLLoadWarning policy-wide):**

```bash
PYTHONWARNINGS=ignore::yaml.YAMLLoadWarning \
python -c 'import yaml,sys; print(yaml.load(sys.stdin.read(), Loader=getattr(yaml,"CSafeLoader",yaml.SafeLoader)))' < config.yaml
```

(PyYAML documents suppressing `YAMLLoadWarning` via `PYTHONWARNINGS`.) ([GitHub][3])

---

If you want the next logical continuation after B7, the usual “dense doc” move is **B8) Loader/dumper subclassing patterns** (how to package your loader choice + constructors + resolvers into a reusable “schema policy” module, with zero global mutation).

[1]: https://chromium.googlesource.com/chromium/src/%2B/refs/tags/127.0.6512.1/third_party/pyyaml/loader.py "third_party/pyyaml/loader.py - chromium/src - Git at Google"
[2]: https://chromium.googlesource.com/chromium/src/%2B/refs/tags/123.0.6293.1/third_party/pyyaml/constructor.py "third_party/pyyaml/constructor.py - chromium/src - Git at Google"
[3]: https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load%28input%29-Deprecation "PyYAML yaml.load(input) Deprecation · yaml/pyyaml Wiki · GitHub"
[4]: https://pyyaml.org/wiki/PyYAMLDocumentation?utm_source=chatgpt.com "PyYAML Documentation"
[5]: https://security.gentoo.org/glsa/202402-33 "PyYAML: Arbitrary Code Execution (GLSA 202402-33) — Gentoo security"
[6]: https://chromium.googlesource.com/chromium/src/%2B/refs/tags/142.0.7432.1/third_party/pyyaml/cyaml.py "third_party/pyyaml/cyaml.py - chromium/src - Git at Google"

## B8) Loader/dumper subclassing patterns

### B8.1 Why subclass at all (the “zero global mutation” requirement)

PyYAML’s extensibility hooks (constructors / representers / resolvers) are stored in **class-level registries** on the Loader/Dumper classes and are mutated via **class methods** (e.g., `Loader.add_constructor`, `Dumper.add_representer`). 

That creates two failure modes you need to proactively design around:

1. **Process-wide side effects**
   The top-level helpers (`yaml.add_constructor(..., Loader=...)`, `yaml.add_representer(..., Dumper=...)`, etc.) default to mutating the library’s default `Loader`/`Dumper` unless you pass your own class explicitly. 
   In practice this is exactly what people trip on when multiple subsystems/plugins want different tag sets in the same process. ([GitHub][1])

2. **Hard-to-undo customizations**
   There are explicit requests for “deregister / restore representers” because once you `yaml.add_representer(...)` globally it’s difficult to revert safely. ([GitHub][2])

**Design target for B8:** package YAML behavior as a reusable *policy module* that:

* never mutates `yaml.SafeLoader`, `yaml.SafeDumper`, etc.
* can be instantiated more than once (multiple policies) without cross-talk
* can still use LibYAML C-accelerated classes when available 

---

### B8.2 The key internal mechanic that makes subclassing work

PyYAML’s registries are copied on first mutation at the subclass level. Concretely, `BaseConstructor.add_constructor` checks whether the dict is already defined on the class (`cls.__dict__`); if not, it copies the inherited registry before adding the new entry. ([GitHub][3])
The representer side does the same for `yaml_representers` / `yaml_multi_representers`. ([GitHub][4])

**Implication:** define a new class, then call `YourLoader.add_constructor(...)`. You now have a private constructor registry, without rewriting PyYAML internals.

---

### B8.3 Minimal “policy module” (single stable policy, C-fast-path, no globals)

Create `myproj/yaml_policy.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, IO, Iterable, Iterator, Type, TypeAlias

import yaml

YamlValue: TypeAlias = Any  # If you want, tighten later (dict[str, Any] | list[Any] | ...)

# ---- 1) Choose fastest SAFE base classes available ----
_BASE_SAFE_LOADER: Type[yaml.Loader] = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
_BASE_SAFE_DUMPER: Type[yaml.Dumper] = getattr(yaml, "CSafeDumper", yaml.SafeDumper)

class PolicyLoader(_BASE_SAFE_LOADER):
    """Safe loader with policy-specific constructors/resolvers."""

class PolicyDumper(_BASE_SAFE_DUMPER):
    """Safe dumper with policy-specific representers/resolvers."""


# ---- 2) Register policy-specific behavior on *Policy* classes (not global yaml.SafeLoader) ----
# Example placeholder tag (replace with your real tags)
def _construct_env(loader: PolicyLoader, node: yaml.Node) -> str:
    # node can be ScalarNode / SequenceNode / MappingNode; choose the right construct_* method
    value = loader.construct_scalar(node)  # expects scalar
    return value  # substitute env vars later

PolicyLoader.add_constructor("!ENV", _construct_env)


# ---- 3) High-level API wrappers ----
def load(stream: str | bytes | IO[str] | IO[bytes]) -> YamlValue:
    return yaml.load(stream, Loader=PolicyLoader)

def load_all(stream: str | bytes | IO[str] | IO[bytes]) -> Iterator[YamlValue]:
    return yaml.load_all(stream, Loader=PolicyLoader)

def dump(data: YamlValue, stream: IO[str] | None = None, **kw: Any) -> str:
    return yaml.dump(data, stream=stream, Dumper=PolicyDumper, **kw)
```

**Why this exact structure works:**

* `yaml.load(..., Loader=PolicyLoader)` keeps policy selection explicit (and avoids the `yaml.load()` default-loader footgun). 
* constructors/representers are added via Loader/Dumper **class methods** intended for this purpose. 
* subclass registries are copied on write, so you’re not mutating the shared base registries. ([GitHub][3])
* LibYAML usage is the *standard* fast-path pattern (“try C, else pure Python”), just pointed at safe C variants. 

---

### B8.4 Factory pattern (multiple isolated policies in one process)

If you have plugins that each want their own tag vocabulary, don’t define one global `PolicyLoader`. Instead, define a builder that returns a brand new class each time:

```python
from __future__ import annotations
from typing import Any, Callable, Type

import yaml

def build_safe_loader(
    *,
    register: Callable[[Type[yaml.Loader]], None],
) -> Type[yaml.Loader]:
    base = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    loader_cls = type("IsolatedSafeLoader", (base,), {})
    register(loader_cls)
    return loader_cls

def build_safe_dumper(
    *,
    register: Callable[[Type[yaml.Dumper]], None],
) -> Type[yaml.Dumper]:
    base = getattr(yaml, "CSafeDumper", yaml.SafeDumper)
    dumper_cls = type("IsolatedSafeDumper", (base,), {})
    register(dumper_cls)
    return dumper_cls
```

Then:

```python
def register_policy_a(loader: type[yaml.Loader]) -> None:
    loader.add_constructor("!ENV", construct_env)

LoaderA = build_safe_loader(register=register_policy_a)
obj = yaml.load(text, Loader=LoaderA)
```

This directly addresses the “in-process plugin system wants separate loaders/dumpers” pain point described by users of PyYAML. ([GitHub][1])

---

### B8.5 Resolver isolation + “don’t accidentally turn `on:` into `true:`”

Implicit resolvers are also class-level configuration. Historically there have been bugs where modifying a derived loader affected the base; the project explicitly notes a fix so derived-loader implicit resolvers shouldn’t affect the base loader. ([PyYAML][5])

**Why you still need a disciplined pattern:** many resolver edits are “surgical replace,” not “append more regex.” If you append without removing, you can:

* misparse keys like `on` / `off` / `yes` / `no` as booleans (classic YAML 1.1 behavior; see the real bug report where `on:` dumped as `true:`). ([GitHub][6])
* accumulate resolver entries repeatedly if you “register on every load,” which has been discussed as a performance trap. ([flutter.googlesource.com][7])

**Practical pattern: clone → remove/replace → add**

```python
import re
import yaml

class Bool12SafeLoader(getattr(yaml, "CSafeLoader", yaml.SafeLoader)):
    pass

# Shallow copy the outer dict.
Bool12SafeLoader.yaml_implicit_resolvers = Bool12SafeLoader.yaml_implicit_resolvers.copy()

# If you mutate per-key lists, copy those lists too (avoid aliasing).
for k, lst in list(Bool12SafeLoader.yaml_implicit_resolvers.items()):
    Bool12SafeLoader.yaml_implicit_resolvers[k] = list(lst)

# Remove existing bool resolvers (implementation detail: varies by key; simplest is filter).
BOOL_TAG = "tag:yaml.org,2002:bool"
for k, lst in Bool12SafeLoader.yaml_implicit_resolvers.items():
    Bool12SafeLoader.yaml_implicit_resolvers[k] = [
        (tag, rx) for (tag, rx) in lst if tag != BOOL_TAG
    ]

# Re-add YAML 1.2-ish bools (true/false only).
Bool12SafeLoader.add_implicit_resolver(
    BOOL_TAG,
    re.compile(r"^(?:true|false|True|False|TRUE|FALSE)$"),
    list("tTfF"),
)

data = yaml.load("on: 1\n", Loader=Bool12SafeLoader)  # now "on" stays a string key
```

> You’ll deep-dive resolver mechanics later; here the key is the *subclass + copy + replace* workflow to keep policies isolated and deterministic.

---

### B8.6 YAMLObject and policy alignment (avoid “constructor not found” surprises)

PyYAML’s docs explicitly note that to make an object safe-loadable you can derive from `yaml.YAMLObject` and set `yaml_loader` to `yaml.SafeLoader`. 
But there’s a real-world gotcha: loader defaults shifted over time, and `YAMLObject`-based tags can end up registered on a different loader than the one you use to load, leading to “could not determine a constructor” errors. ([GitHub][8])

**Policy-safe approach:** always bind YAMLObject types to your policy classes:

```python
import yaml
from myproj.yaml_policy import PolicyLoader, PolicyDumper

class Ref(yaml.YAMLObject):
    yaml_tag = "!Ref"
    yaml_loader = PolicyLoader
    yaml_dumper = PolicyDumper

    def __init__(self, val: str):
        self.val = val

    @classmethod
    def from_yaml(cls, loader: PolicyLoader, node: yaml.Node) -> "Ref":
        return cls(loader.construct_scalar(node))
```

This keeps tag registration + loading in the same policy universe.

---

### B8.7 Integrating third-party tag packages without global mutation

Many tag helper libraries are designed to “add a constructor to a loader you pass in” (good), or they show examples using `yaml.add_constructor(..., yaml.Loader)` (global by default; you should not do this).

**Good pattern (library returns loader with constructor added):** `pyyaml-env-tag` documents passing a loader into its `add_env_tag` and getting the modified loader back. Use it with your policy loader, not `yaml.Loader`. ([GitHub][9])

```python
from yaml_env_tag import add_env_tag
from myproj.yaml_policy import PolicyLoader

PolicyLoader2 = add_env_tag(PolicyLoader)
cfg = yaml.load(text, Loader=PolicyLoader2)
```

**Global-mutation example you should adapt:** pyyaml-include shows registering `!inc` using `yaml.add_constructor("!inc", ..., yaml.Loader)`. Instead, pass your policy loader class. ([pyyaml-include.readthedocs.io][10])

```python
import yaml
import yaml_include
from myproj.yaml_policy import PolicyLoader

yaml.add_constructor("!inc", yaml_include.Constructor(), Loader=PolicyLoader)
cfg = yaml.load(text, Loader=PolicyLoader)
```

---

### B8.8 Verification commands (prove to yourself you didn’t mutate globals)

**1) Constructor dict identity should differ (policy vs base):**

```bash
python - <<'PY'
import yaml
from myproj.yaml_policy import PolicyLoader

print("base is PolicyLoader base:", PolicyLoader.__mro__[1].__name__)
print("id(base.yaml_constructors) =", id(PolicyLoader.__mro__[1].yaml_constructors))
print("id(PolicyLoader.yaml_constructors) =", id(PolicyLoader.yaml_constructors))
PY
```

(After you add a constructor to `PolicyLoader`, PyYAML’s “copy-on-first-mutation” behavior should ensure those IDs differ.) ([GitHub][3])

**2) Representer undo problem disappears if you never touch globals**
If you only mutate `PolicyDumper`, you don’t need “restore representers” context managers that people request when they mutate global representers. ([GitHub][2])

---

If you want the next step after B8 in the same style, the natural continuation is:

**B9) “Policy invariants & safety rails”** — enforce *“never unsafe loader”* in code review/CI (Semgrep/Guardrails rules), and harden your wrappers so callers can’t accidentally pass `yaml.Loader`/`yaml.CLoader` (and get unsafe tag execution). 

[1]: https://github.com/yaml/pyyaml/issues/221?utm_source=chatgpt.com "No easy way to get an isolated, customized SafeLoader #221"
[2]: https://github.com/yaml/pyyaml/issues/435?utm_source=chatgpt.com "deregister representer · Issue #435 · yaml/pyyaml"
[3]: https://github.com/yaml/pyyaml/blob/master/lib/yaml/constructor.py?utm_source=chatgpt.com "pyyaml/lib/yaml/constructor.py at main"
[4]: https://github.com/yaml/pyyaml/blob/master/lib/yaml/representer.py?utm_source=chatgpt.com "pyyaml/lib/yaml/representer.py at main"
[5]: https://pyyaml.org/wiki/PyYAML?utm_source=chatgpt.com "PyYAML is a YAML parser and emitter for Python."
[6]: https://github.com/yaml/pyyaml/issues/696?utm_source=chatgpt.com "Handle top level yaml property `on` · Issue #696"
[7]: https://flutter.googlesource.com/third_party/pyyaml/%2B/ddf20330be1fae8813b8ce1789c48f244746d252%5E?utm_source=chatgpt.com "fc914d52c43f499224f7fb4c2d4c..."
[8]: https://github.com/yaml/pyyaml/issues/266?utm_source=chatgpt.com "could not determine a constructor for custom tag (5.1) ..."
[9]: https://github.com/waylan/pyyaml-env-tag?utm_source=chatgpt.com "waylan/pyyaml-env-tag: A custom YAML tag for referencing ..."
[10]: https://pyyaml-include.readthedocs.io/en/stable/apidocs/yaml_include.constructor.html?utm_source=chatgpt.com "yaml_include.constructor module - pyyaml-include"

## C) Loader classes & the security tier model

### C0) The “security tier model” in one table (what you should standardize on)

PyYAML’s loader choice is **the security boundary**: it determines whether YAML tags can become *arbitrary Python object construction* (and historically, arbitrary function invocation). PyYAML’s own docs explicitly warn that `yaml.load` on untrusted input is “as powerful as `pickle.load`”. 

| Tier       | Loader                                                              | What it constructs                                                                  | Trust boundary guidance                                        |
| ---------- | ------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| **Base**   | `yaml.BaseLoader`                                                   | **No tag resolution**; only `dict`/`list`/`str` (all scalars as strings)            | safest “raw ingest”; you do all typing                         |
| **Safe**   | `yaml.SafeLoader` / `yaml.safe_load`                                | “Standard YAML tags” only; no Python object instantiation                           | recommended for untrusted input                                |
| **Full**   | `yaml.FullLoader` / `yaml.full_load`                                | full YAML language + some “Python-ish” tags, but tries to avoid arbitrary code exec | **not** for untrusted input; keep PyYAML patched ([GitHub][1]) |
| **Unsafe** | `yaml.UnsafeLoader` (aka legacy `yaml.Loader`) / `yaml.unsafe_load` | all predefined tags incl. Python object construction tags                           | treat like unpickling; only internal artifacts ([GitHub][1])   |

---

### C1) Loader taxonomy is literally “pipeline mixins” (Reader→Scanner→Parser→Composer→Constructor→Resolver)

The built-in loaders are “multiple inheritance bundles” over the YAML pipeline pieces. Seeing this once makes the model click: **the big differences are which `Constructor` and which `Resolver` class they mix in.** ([docs.ajenti.org][2])

From the loader definitions:

* `BaseLoader(…, BaseConstructor, BaseResolver)`
* `SafeLoader(…, SafeConstructor, Resolver)`
* `FullLoader(…, FullConstructor, Resolver)`
* `Loader(…, Constructor, Resolver)`
* `UnsafeLoader(…, Constructor, Resolver)` and is “the same as Loader” for backwards compatibility ([docs.ajenti.org][2])

**Meaning:**

* **`Resolver`** = implicit/explicit tag resolution is active (e.g., unquoted `3` becomes int, `true` becomes bool, timestamps can become datetime-like objects).
* **`BaseResolver`** = *no* tag resolution; you get raw strings. PyYAML docs phrase this as “does not resolve or support any tags.” 

---

### C2) `BaseLoader` (raw ingest; everything is strings)

**Contract (PyYAML docs + deprecation wiki agree):**

* *No* tag resolution; constructs only basic Python objects (lists, dicts, unicode strings). 
* All scalars are strings. ([GitHub][1])

**Use BaseLoader when:**

* you want **schema-free** ingestion and will do conversion yourself
* you want to avoid YAML 1.1 implicit typing surprises (`on/off/yes/no`, timestamps, etc.)
* you want the *most restrictive* behavior without writing a custom resolver policy

**Syntax:**

```python
import yaml

raw = yaml.load(stream, Loader=yaml.BaseLoader)
# or: yaml.load_all(stream, Loader=yaml.BaseLoader)
```

**Quick “see the difference” test:**

```bash
python - <<'PY'
import yaml
doc = "a: 3\nb: true\nc: 3.14\n"
print("BaseLoader:", yaml.load(doc, Loader=yaml.BaseLoader))
print("SafeLoader:", yaml.load(doc, Loader=yaml.SafeLoader))
PY
```

(Expect BaseLoader values to remain strings; SafeLoader resolves scalars via `Resolver`.) 

---

### C3) `SafeLoader` (safe subset; recommended for untrusted input)

**Contract (PyYAML docs + deprecation wiki):**

* Supports **standard YAML tags**, and does **not** construct Python class instances (no arbitrary object instantiation). 
* This is the basis for `yaml.safe_load()` / `yaml.safe_load_all()`. 

**Syntax:**

```python
import yaml

cfg = yaml.safe_load(stream)         # sugar
cfg = yaml.load(stream, Loader=yaml.SafeLoader)

for doc in yaml.safe_load_all(stream):
    ...
```

**Practical pitfall:** implicit tag resolution

* Plain scalars are matched against resolver regexes; that’s how `true`, `3`, `3.14` become native types. PyYAML docs explicitly show implicit + explicit tag forms (`!!bool`, `!!int`, `!!float`). 
* If you *never* want implicit typing, use `BaseLoader` (or build a custom resolver policy as in B8).

---

### C4) `FullLoader` (full YAML; “safer than Unsafe”, still not for untrusted)

**What it is (PyYAML’s own deprecation guidance):**

* “Loads the full YAML language” and “avoids arbitrary code execution” — but PyYAML maintainers have historically warned about discovered exploits in some releases; their guidance is still: **don’t use on untrusted input.** ([GitHub][1])

**What changed materially vs older versions:**

* A major hardening step was to move “arbitrary python tags” to `UnsafeLoader` (documented in `CHANGES`, referencing the CVE fix). ([GitHub][3])
  Operationally, this is why `FullLoader` exists as a “more capable than SafeLoader” tier.

**Syntax:**

```python
import yaml

obj = yaml.full_load(stream)                       # sugar
obj = yaml.load(stream, Loader=yaml.FullLoader)
```

**Typical reason to use FullLoader:**

* you need select Python-related tags that SafeLoader rejects (e.g., `!!python/name:` lookups), but you still want to avoid enabling `python/object/apply` / object-instantiation surfaces (those belong in unsafe). The deprecation page is explicit that `UnsafeLoader` is the “original exploitable loader”, and positions `FullLoader` separately. ([GitHub][1])

---

### C5) `UnsafeLoader` / legacy `Loader` (the “YAML as pickle” tier)

**Canonical definition:**

* `UnsafeLoader` is also called `Loader` for backward compatibility; it’s the “original loader code that could be easily exploitable.” ([GitHub][1])
* PyYAML’s docs: `Loader` “supports all predefined tags” and “may construct an arbitrary Python object,” and is therefore not safe for untrusted input. 

**Syntax:**

```python
import yaml

obj = yaml.unsafe_load(stream)
obj = yaml.load(stream, Loader=yaml.UnsafeLoader)  # or yaml.Loader
```

**Exploit-shaped proof (from PyYAML maintainers):**

```bash
python -c 'import yaml; yaml.load("!!python/object/new:os.system [echo EXPLOIT!]")'
```

This exact example is in the deprecation page to demonstrate why “Loader must be explicit.” ([GitHub][1])

---

### C6) `yaml.load()` deprecation + warning model (and how to silence it correctly)

#### Behavior across versions (what breaks your code)

* PyYAML 5.1+: `yaml.load(input)` **warns** (`YAMLLoadWarning`) and still works, pushing you to specify `Loader=`. ([GitHub][1])
* PyYAML 6.0+: `yaml.load()` **requires** the `Loader` argument and will raise `TypeError` if missing (seen widely in downstream breakage reports). ([GitHub][4])

#### Fix patterns you should enforce in a codebase

1. **Default to safe:**

```python
data = yaml.safe_load(stream)
```

2. **Or explicitly choose a tier:**

```python
data = yaml.load(stream, Loader=yaml.SafeLoader)
```

#### Suppressing the warning (when you’re stuck with third-party code)

PyYAML documents two suppression knobs:

**A) Env var (preferred for “I don’t own the code”):**

```bash
PYTHONWARNINGS=ignore::yaml.YAMLLoadWarning your_command_here
```

([GitHub][1])

**B) Programmatic global toggle (use sparingly):**

```python
import yaml
yaml.warnings({'YAMLLoadWarning': False})
```

([GitHub][5])

(You should still audit whether the underlying call is safe before suppressing.)

---

### C7) C-accelerated variants (`CLoader` etc.) + parity implications

#### What “CLoader” means

If LibYAML bindings are installed, PyYAML exposes C-backed parser/emitter classes and loader variants. The docs show the classic fast-path pattern and explicitly warn there are “subtle (but not really significant) differences” between pure-Python and LibYAML-based implementations. 

They also list C loader variants (`CLoader`, `CSafeLoader`, `CBaseLoader`) as “versions of the above classes written in C using LibYAML.” 

#### Correct “fast + safe” selection pattern

PyYAML’s docs/examples often demonstrate `CLoader`, but note: **`CLoader` is the C version of the (unsafe) `Loader` tier**. The safe fast-path you usually want is `CSafeLoader`.

```python
import yaml

Safe = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
data = yaml.load(stream, Loader=Safe)
```

#### Build/install knobs that affect whether C loaders exist

* PyYAML docs show building with LibYAML via `python setup.py --with-libyaml install`. 
* The upstream repo README also describes `--with-libyaml` / `--without-libyaml` and shows using `yaml.CLoader`/`yaml.CDumper` when bindings exist. ([GitHub][6])

#### Parity discipline (what you should *do* about “subtle differences”)

If you ship YAML as a stable interface (configs, manifests), treat switching to C-loader as a **behavior change**:

* add golden fixtures: parse + re-dump + compare normalized structure
* run the same fixtures under both pure and C variants in CI (when possible)

This is directly motivated by the docs warning about subtle differences. 

---

### C8) Drop-in “tier guardrails” snippet (copy/paste)

Use this pattern to make loader choice explicit and centrally audited:

```python
import yaml
from typing import Any, IO, Iterator

SafeLoader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)

def load_yaml(stream: str | bytes | IO[str] | IO[bytes]) -> Any:
    return yaml.load(stream, Loader=SafeLoader)

def load_yaml_all(stream: str | bytes | IO[str] | IO[bytes]) -> Iterator[Any]:
    return yaml.load_all(stream, Loader=SafeLoader)
```

This aligns with PyYAML’s explicit “Loader must be specified” direction and avoids the `yaml.load()` version cliff in 6.x. ([GitHub][1])

---

If you want the next subsection after C, the highest-ROI continuation is **D) Dumper classes & emission safety tiers** (SafeDumper vs Dumper vs CDumper) *paired with* **“round-trip invariants”** (what changes between parse+dump with different dumpers).

[1]: https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load%28input%29-Deprecation "PyYAML yaml.load(input) Deprecation · yaml/pyyaml Wiki · GitHub"
[2]: https://docs.ajenti.org/en/ajenti-3-dev/_modules/yaml/loader.html "yaml.loader — Ajenti 2.2.1 documentation"
[3]: https://raw.githubusercontent.com/yaml/pyyaml/5.4.1/CHANGES?utm_source=chatgpt.com "https://raw.githubusercontent.com/yaml/pyyaml/5.4...."
[4]: https://github.com/yaml/pyyaml/issues/576?utm_source=chatgpt.com "PyYAML 6.0 load() function requires Loader argument #576"
[5]: https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load%28input%29-Deprecation/2515445036d58cfb23e41fd0917a4ffcfdfff550?utm_source=chatgpt.com "PyYAML yaml.load(input) Deprecation"
[6]: https://github.com/yaml/pyyaml "GitHub - yaml/pyyaml: Canonical source repository for PyYAML"

## D) Dumper classes & emission safety tiers

### D0) Mental model: dumping = **represent** (Python→YAML nodes) + **serialize/emit** (nodes→text)

PyYAML’s “safety tiers” on *dumping* are almost entirely about **which representer is in play**:

* `SafeDumper` uses `SafeRepresenter` → *only standard YAML tags*, errors on unknown Python objects. 
* `Dumper` uses `Representer` → can emit **Python-specific tags** (e.g. `!!python/object:*`, `!!python/object/apply:*`) that other YAML processors won’t load. 
* C variants (`CSafeDumper`, `CDumper`) primarily swap the **emitter** to LibYAML C for speed; the tier semantics remain (safe vs python-taggy). 

---

## D1) High-level API surface (what you actually call)

### Single-doc vs multi-doc emitters

* `yaml.dump(data, stream=None, Dumper=..., **kw)` → one document
* `yaml.dump_all(documents, stream=None, Dumper=..., **kw)` → N documents (each becomes its own YAML document) 

### “Safe emission” sugars

* `yaml.safe_dump(data, stream=None, **kw)` and `yaml.safe_dump_all(documents, stream=None, **kw)` always force `Dumper=SafeDumper` internally. ([Chromium Git Repositories][1])

**Key operational consequence:** you **cannot** select a custom dumper via `yaml.safe_dump`; you must use `yaml.dump(..., Dumper=YourDumper)` if you need subclass behavior (no aliases, custom representers, etc.). ([Chromium Git Repositories][1])

### Dump formatting knobs (the knobs you should standardize)

PyYAML documents these core kwargs on `dump()` / `dump_all()`:

* `default_style`, `default_flow_style`
* `canonical`
* `indent`, `width`
* `allow_unicode`, `line_break`
* `encoding`
* `explicit_start`, `explicit_end`
* `version`, `tags` 

**Modern releases also support `sort_keys`** (defaulting to `True`); you’ll see it in `help(yaml.Dumper)` in real installs. ([Stack Overflow][2])

---

## D2) Dumper class taxonomy (what each tier does)

PyYAML’s reference defines these classes explicitly: 

### `yaml.Dumper`

* “Most common … used in most cases” (per docs). 
* **Supports all predefined tags** and **may represent an arbitrary Python object**, which means:

  * You may get YAML containing Python-only tags.
  * That YAML may not be loadable by non-Python YAML processors. 

### `yaml.SafeDumper`

* “Produces only standard YAML tags” → best for configs / interop. 
* Cannot represent arbitrary Python objects; unsupported types raise `RepresenterError`. (This is literally implemented as `represent_undefined → raise RepresenterError`.) ([Fuchsia Git Repositories][3])

### `yaml.BaseDumper`

* “Does not support any tags” and is mainly for subclassing. 

---

## D3) What “unsafe emission” actually looks like (Python tags)

PyYAML’s tutorial shows that `yaml.dump()` can serialize a class instance using `!!python/object:...`: 

```python
import yaml

class Hero:
    def __init__(self, name): self.name = name

print(yaml.dump(Hero("Galain")))
# !!python/object:__main__.Hero {name: Galain}
```

Under the hood, the non-safe `Representer` layer uses Python tags like `tag:yaml.org,2002:python/object/apply:...` for some types (example: ordered dict representation). ([Fuchsia Git Repositories][3])

**Compatibility rule you should encode in your docs/CI:**

* If you emit with `Dumper` and Python tags appear, consumers need **PyYAML** + a loader that permits those tags (often `UnsafeLoader`).
* If you emit with `SafeDumper`, the YAML is intended to be standard-tag-only and broadly parseable. 

---

## D4) C-accelerated emission (`CDumper`, `CSafeDumper`) and parity discipline

### Availability & selection

If LibYAML bindings are built, PyYAML exposes C-based parser/emitter classes; docs show a typical try/except fallback for `CLoader` / `CDumper`. 

The reference lists C dumpers as “available only if you build LibYAML bindings”: `CDumper`, `CSafeDumper`, `CBaseDumper`. 

### Tier semantics remain: safe vs full-featured

You can see the difference directly in class composition:

* `CSafeDumper(CEmitter, SafeRepresenter, Resolver)`
* `CDumper(CEmitter, Serializer, Representer, Resolver)` ([Chromium Git Repositories][4])

So: **CSafeDumper ≈ SafeDumper, but faster emission**; **CDumper ≈ Dumper, but faster emission**. ([Chromium Git Repositories][4])

### Behavior parity warning

PyYAML docs explicitly warn there are “subtle (but not really significant) differences” between the pure-Python and LibYAML-based parsers/emitters. Treat swapping to C emitters as something you regression-test (golden fixtures). 

---

## D5) Round-trip invariants: what changes on `load → dump` and how to control it

### D5.1 The big non-invariant: **text preservation is not a PyYAML goal**

* **Comments:** PyYAML (and LibYAML) discard comments at the scanner level; they’re not available to re-emit. ([GitHub][5])

If you need “edit YAML then re-save with comments/format intact”, that’s *not* a PyYAML round-trip story.

---

### D5.2 Round-trip invariants you *can* enforce (data-level)

When you constrain yourself to **safe types** (dict/list/str/int/float/bool/None/bytes/datetime-ish), you can usually enforce:

* `obj == yaml.safe_load(yaml.safe_dump(obj, ...))` (modulo normalization of floats/dates/strings)

But beware type normalization edges:

* Example: explicit `!!timestamp` loaded to `datetime.date` may dump back as a plain scalar string (`2015-01-01`) rather than preserving the explicit tag spelling. ([GitHub][6])

---

### D5.3 Round-trip non-invariants you must expect (text-level) + the knobs to stabilize them

#### (1) Mapping key order

* Many installs default `sort_keys=True`, which will reorder keys alphabetically unless you pass `sort_keys=False`. ([Stack Overflow][2])

**Stabilize for human-edited configs:**

```python
yaml.safe_dump(data, sort_keys=False)
```

#### (2) Flow vs block style drift

PyYAML may choose flow style for “simple” nested collections; the docs show the exact phenomenon and the fix: set `default_flow_style=False` to force block style. 

```python
yaml.safe_dump(data, default_flow_style=False)
```

#### (3) Anchors/aliases (`&id001` / `*id001`) appearing “randomly”

The representer tracks `id(data)` and reuses already-represented nodes to create aliases. ([Fuchsia Git Repositories][3])

PyYAML doesn’t provide a `dump(..., aliases=False)` flag; the endorsed workaround is to override `ignore_aliases()` in a custom dumper. ([GitHub][7])

```python
import yaml

class NoAliasSafeDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True

text = yaml.dump(data, Dumper=NoAliasSafeDumper, sort_keys=False)
```

#### (4) Unicode escaping vs readable UTF-8

`allow_unicode` is an explicit knob on dumpers. 

```python
yaml.safe_dump(data, allow_unicode=True)
```

#### (5) Document markers (`---` / `...`)

Use:

* `explicit_start=True` to force `---` (docs show exact behavior) 
* `explicit_end=True` to force `...` (supported parameter) 

#### (6) “Canonical” output (maximally explicit, least human-friendly)

`canonical=True` produces verbose output with explicit tags (example in docs). This is useful for deterministic debugging artifacts, not configs. 

---

## D6) Standardized recipes (copy/paste)

### Recipe 1: “Config-safe, stable, human-ish YAML”

Use safe emission + stable formatting knobs:

```python
import yaml

def dump_config(data) -> str:
    return yaml.safe_dump(
        data,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        indent=2,
        width=88,
    )
```

Back it with a regression assertion:

```python
assert yaml.safe_load(dump_config(data)) == data
```

### Recipe 2: “Config-safe + no anchors/aliases”

Because `safe_dump` can’t take a custom Dumper, you must route through `yaml.dump`:

```python
import yaml

class NoAliasSafeDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True

def dump_config_no_aliases(data) -> str:
    return yaml.dump(
        data,
        Dumper=NoAliasSafeDumper,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        indent=2,
        width=88,
    )
```

This is the same workaround suggested by PyYAML users for disabling aliases. ([GitHub][7])

### Recipe 3: “Fast emission with LibYAML if available”

Select `CSafeDumper`/`CDumper` explicitly; PyYAML does not automatically switch `safe_dump` to the C dumper unless you add your own selection layer. 

```python
import yaml

SafeFastDumper = getattr(yaml, "CSafeDumper", yaml.SafeDumper)

text = yaml.dump(
    data,
    Dumper=SafeFastDumper,
    default_flow_style=False,
    sort_keys=False,
)
```

(And keep a golden test because C-vs-Python emitters can have subtle differences.) 

---

If you want the next subsection in this same style, the natural continuation is **E) Dumper/emitter formatting knobs in depth** (indentation edge cases, line wrapping, scalar styles `|` vs `>` vs quoting, explicit tag control via `tags=`/`version=`, and deterministic output profiles for “configs” vs “debug artifacts”).

[1]: https://chromium.googlesource.com/chromium/src.git/%2B/refs/tags/138.0.7167.0/third_party/pyyaml/__init__.py "third_party/pyyaml/__init__.py - chromium/src.git - Git at Google"
[2]: https://stackoverflow.com/questions/16782112/can-pyyaml-dump-dict-items-in-non-alphabetical-order "python - Can PyYAML dump dict items in non-alphabetical order? - Stack Overflow"
[3]: https://fuchsia.googlesource.com/third_party/pyyaml/%2B/4.1/lib3/yaml/representer.py?autodive=0%2F "lib3/yaml/representer.py - third_party/pyyaml - Git at Google"
[4]: https://chromium.googlesource.com/chromium/src/%2B/refs/tags/142.0.7432.1/third_party/pyyaml/cyaml.py "third_party/pyyaml/cyaml.py - chromium/src - Git at Google"
[5]: https://github.com/yaml/pyyaml/issues/90?utm_source=chatgpt.com "Load comments · Issue #90 · yaml/pyyaml"
[6]: https://github.com/yaml/pyyaml/issues/283 "Unable to dump with the same format loaded · Issue #283 · yaml/pyyaml · GitHub"
[7]: https://github.com/yaml/pyyaml/issues/103 "Disable Aliases/Anchors · Issue #103 · yaml/pyyaml · GitHub"

## E) Dumper/emitter formatting knobs in depth

### E0) Where each knob lands in the pipeline (so you know what you’re actually controlling)

`yaml.dump(..., **kw)` wires kwargs into **three separate subsystems**:

* **Emitter** (layout + whitespace): `canonical`, `indent`, `width`, `allow_unicode`, `line_break`
* **Serializer** (document framing + directives): `encoding`, `explicit_start`, `explicit_end`, `version`, `tags`
* **Representer** (Python→YAML-node choices): `default_style`, `default_flow_style`, `sort_keys`, plus per-type representers

You can see this explicitly in the `Dumper`/`SafeDumper` constructors: they forward kwargs into `Emitter.__init__`, `Serializer.__init__`, and `Representer.__init__`. ([Chromium Git Repositories][1])

Actionable consequence: if output surprises you, you debug the **subsystem** responsible (layout vs document framing vs representation), not just “PyYAML is weird”.

---

## E1) Indentation (`indent`) — what it *really* means + the list-indentation footgun

### E1.1 `indent` is a *step size* with a hard acceptance window

The emitter sets:

* default indent step: `best_indent = 2`
* it only accepts a user `indent` if `1 < indent < 10` (i.e., **2–9**); anything else silently falls back to 2 ([Chromium Git Repositories][2])

So:

```python
yaml.safe_dump(data, indent=2)   # ok
yaml.safe_dump(data, indent=4)   # ok
yaml.safe_dump(data, indent=10)  # ignored -> behaves like indent=2
```

### E1.2 `indent` is applied per nesting level via `increase_indent()`

When you nest structures, the emitter increments `self.indent += self.best_indent` unless `indentless=True`. ([Chromium Git Repositories][2])

### E1.3 The “sequence indentation” trap (and the clean override)

Block sequences decide whether they are *indentless* like this:

```python
indentless = (self.mapping_context and not self.indention)
self.increase_indent(flow=False, indentless=indentless)
```

So for some “sequence under mapping” contexts, PyYAML intentionally avoids extra indentation (common complaint: `indent=4` indents after `-` instead of aligning the sequence properly). ([Chromium Git Repositories][2])

**Workaround (pure-Python emitters only): override `increase_indent()` to ignore indentless.**

```python
import yaml

class IndentSeqSafeDumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        # force indentless=False always
        return super().increase_indent(flow, indentless=False)

print(yaml.dump(
    {"x": [{"b": 1}, 2]},
    Dumper=IndentSeqSafeDumper,
    default_flow_style=False,
    sort_keys=False,
))
```

Why this works: the emitter’s indentation behavior is entirely in `Emitter.increase_indent()` and the block-sequence handler passes `indentless` into it. ([Chromium Git Repositories][2])

**Important limitation:** this override does **not** apply to `CSafeDumper`/`CDumper` (they use a C emitter), so if you need this exact layout, prefer Python `SafeDumper`. (PyYAML itself notes there are subtle differences between Python and LibYAML emitters.) 

---

## E2) Line wrapping (`width`) — how to disable wrapping *reliably*

### E2.1 The emitter’s real rule

Emitter sets:

* `best_width = 80` by default
* applies user `width` only if `width > best_indent * 2` ([Chromium Git Repositories][2])

So `width=3` is ignored when `indent=2`, because `3` is not `> 4`.

### E2.2 Practical patterns

**Avoid wrapping in flow collections / long scalars:**

```python
yaml.safe_dump(data, width=10_000, default_flow_style=False)
```

PyYAML’s own docs demonstrate `width` changing line breaks in emitted sequences. 

**Prefer “explicit and stable” widths** (e.g., 88/100/120/10_000) rather than relying on default 80 to reduce diff churn.

---

## E3) Line endings (`line_break`) — determinism across OSes

Emitter default is `\n`, and it accepts only `'\r'`, `'\n'`, or `'\r\n'`; anything else falls back to `\n`. ([Chromium Git Repositories][2])

**Config file output (always LF):**

```python
yaml.safe_dump(data, line_break="\n")
```

**Windows-style files (CRLF):**

```python
yaml.safe_dump(data, line_break="\r\n")
```

---

## E4) Scalar styles: plain vs quotes vs `|` vs `>` — global default vs per-value control

### E4.1 What YAML scalar styles exist (and what `|`/`>` actually do)

PyYAML’s docs enumerate **5 scalar styles**: plain, single-quoted, double-quoted, literal (`|`), folded (`>`), and explain literal vs folded semantics. 

### E4.2 Global scalar style: `default_style=...`

Representers implement:

```python
def represent_scalar(tag, value, style=None):
    if style is None:
        style = self.default_style
    return ScalarNode(tag, value, style=style)
```

So `default_style` becomes the fallback style for *all* scalars unless a representer chooses otherwise. ([Chromium Git Repositories][3])

**Example (force double quotes everywhere + flow collections):**
PyYAML’s docs show this exact usage: `default_flow_style=True, default_style='"'`. 

```python
yaml.dump(range(5), default_flow_style=True, default_style='"')
```

### E4.3 Per-value scalar style (the “correct” way for mixed short/long strings)

You usually want: short strings quoted; long multi-line strings literal (`|`) or folded (`>`).

Pattern:

1. define `str` subclasses as markers
2. register representers that call `represent_scalar(..., style='|')` etc.

```python
import yaml

class LiteralStr(str): pass
class FoldedStr(str): pass
class SQStr(str): pass
class DQStr(str): pass

class StyleSafeDumper(yaml.SafeDumper):
    pass

def _repr_literal(dumper: yaml.Dumper, data: LiteralStr):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")

def _repr_folded(dumper: yaml.Dumper, data: FoldedStr):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=">")

def _repr_sq(dumper: yaml.Dumper, data: SQStr):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="'")

def _repr_dq(dumper: yaml.Dumper, data: DQStr):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')

StyleSafeDumper.add_representer(LiteralStr, _repr_literal)
StyleSafeDumper.add_representer(FoldedStr, _repr_folded)
StyleSafeDumper.add_representer(SQStr, _repr_sq)
StyleSafeDumper.add_representer(DQStr, _repr_dq)

data = {
  "short": SQStr("Hello"),
  "long": LiteralStr("Line1\nLine2\nLine3\n"),
}

print(yaml.dump(data, Dumper=StyleSafeDumper, sort_keys=False))
```

Why this is “the real mechanism”: `represent_scalar(..., style=...)` directly sets the YAML scalar node style, and the representer code is the canonical place where styles are chosen. ([Chromium Git Repositories][3])

---

## E5) Block scalars (`|` / `>`) edge cases: trailing spaces + “multiline strings are ugly”

### E5.1 Trailing spaces can defeat block style

There’s a long-standing behavior: if a string contains trailing spaces, PyYAML may refuse block scalar emission and fall back to quoted style (because YAML block scalars cannot safely preserve certain trailing-space scenarios without changing semantics). This is discussed explicitly as a problem/behavior in PyYAML’s issue tracker. ([GitHub][4])

**Operational guidance:**

* If you require literal `|`, normalize your content (e.g., strip trailing whitespace on each line) before dumping.
* If trailing whitespace is meaningful data, you’re in “quoted scalars only” territory for correctness.

### E5.2 PyYAML doesn’t aim for “pretty multiline output” automatically

The project acknowledges that multiline string formatting can be undesirable and that “style guessing” isn’t currently built-in (unless someone contributes it with tests). ([GitHub][5])

**Actionable consequence:** if you care, you *must* own the representer policy (E4.3).

---

## E6) Collections: flow vs block (`default_flow_style`) and what changed in newer releases

### E6.1 What `default_flow_style` does in representers

When representing a list/dict, PyYAML computes a `best_style` (true when all children are plain scalars) and picks flow vs block like this:

* if `default_flow_style is not None`: use it
* else: use `best_style`

You can see this in representer logic and docs. ([Chromium Git Repositories][3])

### E6.2 Modern default: `default_flow_style=False`

In current PyYAML source, dumper constructors default `default_flow_style=False`, which means “block style unless you override per-node”. ([Chromium Git Repositories][1])

**If you want deterministic output across environments / versions:** set it explicitly anyway:

```python
yaml.safe_dump(data, default_flow_style=False)
```

(Older PyYAML docs show why you’d do this—nested collections defaulting to flow in some versions.) 

---

## E7) Tag directives + YAML version directives (`tags=`, `version=`) + canonical mode

### E7.1 What `version=` and `tags=` actually trigger

`version` and `tags` are captured by the serializer and placed into the `DocumentStartEvent`. ([Chromium Git Repositories][6])
The emitter checks for `event.version` / `event.tags` and writes directives:

* `%YAML <version>`
* `%TAG <handle> <prefix>` (for each tag handle), iterating handles in sorted order ([Chromium Git Repositories][2])

### E7.2 Syntax you should use

**Version directive:**

```python
yaml.dump(data, version=(1, 1))
```

**Tag directives: `tags` must be a mapping `{handle: prefix}`**

```python
yaml.dump(
  data,
  tags={"!e!": "tag:example.com,2026:"},
  explicit_start=True,
)
```

Why: emitter treats `event.tags.keys()` as **handles** and `event.tags[handle]` as the **prefix** used to write `%TAG`. ([Chromium Git Repositories][2])

### E7.3 Canonical output (`canonical=True`) — deterministic but not human-friendly

Canonical mode forces explicit tagging and a highly normalized rendering; PyYAML docs show canonical output emitting explicit `!!seq`, `!!int`, quoted scalars, etc. 

Use case: debugging artifacts, golden fixtures, “diff-stable machine output”.

---

## E8) Key ordering (`sort_keys`) — how to stop PyYAML from re-sorting your configs

Representer sorts mapping items only if `self.sort_keys` is true. ([Chromium Git Repositories][3])
Dumper defaults `sort_keys=True` in current source. ([Chromium Git Repositories][1])

So for config emission, make this **non-negotiable**:

```python
yaml.safe_dump(data, sort_keys=False)
```

---

## E9) Unicode emission (`allow_unicode`) — and the C-emitter discrepancy

Emitter stores `allow_unicode` and uses it to decide whether to emit unicode characters directly vs escape sequences. ([Chromium Git Repositories][2])

**BUT:** there’s a documented behavior difference where `CSafeDumper` may ignore `allow_unicode=True` and still emit `\U....` escapes in some versions. ([GitHub][7])

Actionable policy:

* If readable UTF-8 output is a requirement, prefer Python `SafeDumper` in CI/production, or add snapshot tests that assert your unicode behavior under both dumpers.

---

## E10) Deterministic output profiles (copy/paste “config” vs “debug artifact”)

### Profile A: “Human config YAML” (stable diffs, interoperable)

Goals:

* standard tags only
* no key reordering
* block style
* no anchors/aliases (optional)
* consistent LF
* readable unicode (prefer Python emitter if you hit CSafeDumper issues)

```python
import yaml

class ConfigDumper(yaml.SafeDumper):
    # (Optional) nicer sequence indentation
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, indentless=False)

    # (Optional) avoid anchors/aliases entirely
    def ignore_aliases(self, data):
        return True

def dump_config(data) -> str:
    return yaml.dump(
        data,
        Dumper=ConfigDumper,
        default_flow_style=False,
        sort_keys=False,
        indent=2,
        width=88,
        allow_unicode=True,
        line_break="\n",
        explicit_start=False,
    )
```

Rationale is directly grounded in dumper/emitter/representer semantics above. ([Chromium Git Repositories][1])

### Profile B: “Debug artifact YAML” (maximally explicit, diff-stable)

Goals:

* canonical tagging
* explicit doc markers
* stable width, LF

```python
import yaml

def dump_debug(data) -> str:
    return yaml.dump(
        data,
        Dumper=yaml.SafeDumper,   # keep safe tags; canonical adds explicit typing
        canonical=True,
        explicit_start=True,
        explicit_end=True,
        sort_keys=True,
        indent=2,
        width=80,
        allow_unicode=False,      # makes escapes explicit/stable
        line_break="\n",
    )
```

Canonical behavior is shown in PyYAML’s own examples. 

---

## E11) CLI test harness (prove knobs do what you think)

Dump the same object under “config” vs “debug” profiles:

```bash
python - <<'PY'
import yaml

data = {
  "name": "Привет",
  "seq": [{"b": 1}, 2],
  "text": "Line1\nLine2\n",
}

class ConfigDumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, indentless=False)
    def ignore_aliases(self, data):
        return True

print("=== config ===")
print(yaml.dump(data, Dumper=ConfigDumper, default_flow_style=False, sort_keys=False,
                indent=2, width=88, allow_unicode=True, line_break="\n"))

print("=== debug ===")
print(yaml.dump(data, Dumper=yaml.SafeDumper, canonical=True, explicit_start=True, explicit_end=True,
                sort_keys=True, indent=2, width=80, allow_unicode=False, line_break="\n"))
PY
```

If you want the next chapter after E: the natural “agent-ready” continuation is **F) Tag system & Python-object interoperability** (standard tags vs python tags, representer/constructor registration strategy, and how to enforce safe-tag-only round trips).

[1]: https://chromium.googlesource.com/chromium/src/%2B/main/third_party/pyyaml/dumper.py "third_party/pyyaml/dumper.py - chromium/src - Git at Google"
[2]: https://chromium.googlesource.com/chromium/src/%2B/main/third_party/pyyaml/emitter.py "third_party/pyyaml/emitter.py - chromium/src - Git at Google"
[3]: https://chromium.googlesource.com/chromium/src/%2B/main/third_party/pyyaml/representer.py "third_party/pyyaml/representer.py - chromium/src - Git at Google"
[4]: https://github.com/yaml/pyyaml/issues/121?utm_source=chatgpt.com "Why does pyyaml disallow trailing spaces in block scalars?"
[5]: https://github.com/yaml/pyyaml/issues/240?utm_source=chatgpt.com "Multiline strings are ugly after dumping · Issue #240 · yaml ..."
[6]: https://chromium.googlesource.com/chromium/src/%2B/main/third_party/pyyaml/serializer.py "third_party/pyyaml/serializer.py - chromium/src - Git at Google"
[7]: https://github.com/yaml/pyyaml/issues/771?utm_source=chatgpt.com "CSafeDumper doesn't appear to respect `allow_unicode= ..."

## F) Tag system & Python-object interoperability

### F0) Tags are the *type system* of YAML (and PyYAML treats every node as “tag + value”)

A YAML node’s tag can be:

* **Implicit** (chosen by resolver regexes on plain scalars): `true` → `!!bool`, `3` → `!!int`, `3.14` → `!!float`. 
* **Explicit** (forced by syntax): `!!bool "true"`, `!!int "3"`, etc. 

In PyYAML, this tag is what drives **construction** (YAML → Python object) and **representation** (Python → YAML). 

---

## F1) Standard YAML tags vs Python tags (what you should treat as “interop surface”)

### F1.1 Standard YAML tags (safe, cross-language)

PyYAML’s docs give the canonical mapping for the standard tag set (safe, interoperable): 

* `!!null` → `None`
* `!!bool` → `bool`
* `!!int` → `int`
* `!!float` → `float`
* `!!binary` → `bytes` (Py3)
* `!!timestamp` → `datetime.datetime`
* `!!omap` / `!!pairs` → list of pairs
* `!!set` → `set`
* `!!str` → `str`
* `!!seq` → `list`
* `!!map` → `dict`

**Forcing a type with explicit tags (useful when implicit resolution is a footgun):**

```yaml
port: !!int "5432"
enabled: !!bool "true"
version: !!str 1.0
```

(Explicit tags override resolver guessing.) 

### F1.2 Python-specific tags (portable only within the PyYAML universe)

PyYAML also documents “Python-specific tags” (basic types + containers) and “complex Python tags” (object/name/module/apply). 
Examples:

* `!!python/tuple` → `tuple`
* `!!python/name:module.name` → `module.name`
* `!!python/object:module.cls` → instance
* `!!python/object/apply:module.f` → value of `f(...)` 

**Interop rule:** the moment these appear in your YAML, you’ve stopped producing “just YAML” and started producing “PyYAML-flavored YAML”.

---

## F2) The dangerous part: `!!python/object*` and why loaders matter

PyYAML explicitly shows that it can construct class instances using `!!python/object`, and warns this may be dangerous on untrusted input; `safe_load` limits construction to simple objects. 
The PyYAML deprecation note also includes a trivial exploit shape using `!!python/object/new:os.system` and explains why `yaml.load(...)` requires an explicit loader choice. ([GitHub][1])

### F2.1 The *three* Python object construction forms

PyYAML documents `!!python/object`, plus two “pickle protocol” compatible forms: `object/new` and `object/apply`, each with a structured mapping form and an “args-only” short form. 

**Full form:**

```yaml
!!python/object/new:pkg.ModClass
args: [a, b]
kwds: {k: v}
state: ...
listitems: [...]
dictitems: { ... }

!!python/object/apply:pkg.factory
args: [...]
kwds: { ... }
```

**Short form when only args exist:**

```yaml
!!python/object/new:pkg.ModClass [a, b]
!!python/object/apply:pkg.factory [a, b]
```



### F2.2 Loader policy in one sentence

* `yaml.safe_load()` **recognizes only standard YAML tags** and “cannot construct an arbitrary Python object.” 
* `yaml.load(..., Loader=...)` is where you choose whether any of the Python tag families are even allowed. The PyYAML maintainers’ deprecation page exists because the default loader historically enabled unsafe construction. ([GitHub][1])

---

## F3) Dumping tiers: “safe YAML” vs “Python object YAML”

PyYAML’s docs draw a bright line:

* `safe_dump` / `safe_dump_all` **produce only standard YAML tags** and “cannot represent an arbitrary Python object.” 
* `Dumper` / `dump` “may represent an arbitrary Python object” and may emit documents other YAML processors can’t load. 

### Practical implication for round-trip guarantees

If you want **safe-tag-only round trips**, the invariant you enforce is:

* **Emit** with SafeDumper (or SafeDumper subclass) so output never contains `tag:yaml.org,2002:python/...` tags. 
* **Load** with SafeLoader (or SafeLoader subclass) so inputs containing unknown / Python tags are rejected by default. 

---

## F4) Registration strategy: constructors + representers (the only correct way to make tags “mean something”)

PyYAML’s extension API is explicitly:

* `add_constructor(tag, constructor, Loader=...)`
* `add_multi_constructor(tag_prefix, multi_constructor, Loader=...)`
* `add_representer(type, representer, Dumper=...)`
* `add_multi_representer(base_type, multi_representer, Dumper=...)` 

### F4.1 Controlled custom tags (recommended for “configs with some structure”)

**Goal:** allow *your* tags (e.g., `!Ref`, `!Duration`) but keep the loader *safe* (no python/object execution), by registering constructors on a SafeLoader subclass.

```python
import yaml
from dataclasses import dataclass

# Loader/Dumper policy classes (no global mutation)
class PolicyLoader(getattr(yaml, "CSafeLoader", yaml.SafeLoader)):
    pass

class PolicyDumper(getattr(yaml, "CSafeDumper", yaml.SafeDumper)):
    pass

@dataclass(frozen=True)
class Ref:
    key: str

# Constructor: YAML node -> Python object
def construct_ref(loader: PolicyLoader, node: yaml.Node) -> Ref:
    return Ref(loader.construct_scalar(node))

# Representer: Python object -> YAML node
def represent_ref(dumper: PolicyDumper, data: Ref) -> yaml.Node:
    return dumper.represent_scalar("!Ref", data.key)

PolicyLoader.add_constructor("!Ref", construct_ref)
PolicyDumper.add_representer(Ref, represent_ref)

obj = yaml.load("x: !Ref abc\n", Loader=PolicyLoader)
txt = yaml.dump(obj, Dumper=PolicyDumper, sort_keys=False)
```

Why this is “the canonical mechanism”: PyYAML defines constructors as functions that convert a representation node into a Python object, and representers as functions that convert a Python object into a representation node. 

### F4.2 YAMLObject (convenience layer; still needs explicit loader/dumper binding)

PyYAML documents `YAMLObject` as a way to define tags + constructor + representer via `from_yaml` / `to_yaml`, and explicitly shows `yaml_loader` / `yaml_dumper` on the class. 

```python
import yaml
from dataclasses import dataclass

class PolicyLoader(getattr(yaml, "CSafeLoader", yaml.SafeLoader)): pass
class PolicyDumper(getattr(yaml, "CSafeDumper", yaml.SafeDumper)): pass

@dataclass
class Ref(yaml.YAMLObject):
    yaml_tag = "!Ref"
    yaml_loader = PolicyLoader
    yaml_dumper = PolicyDumper

    key: str

    @classmethod
    def from_yaml(cls, loader, node):
        return cls(loader.construct_scalar(node))

    @classmethod
    def to_yaml(cls, dumper, data):
        return dumper.represent_scalar(cls.yaml_tag, data.key)
```

This avoids “constructor not found” surprises by keeping tag registration aligned to the exact loader/dumper classes you will use. 

---

## F5) Enforcing safe-tag-only round trips (practical guardrails)

### F5.1 Output guardrail: **never emit Python tags**

Hard rule: use `safe_dump` when you can; otherwise use a **SafeDumper subclass** and `yaml.dump(..., Dumper=YourSafeDumper)` to support custom tags while staying out of python/object emission. PyYAML explicitly says safe_dump emits only standard tags and can’t represent arbitrary objects. 

**CI check (cheap, effective):**

```bash
python - <<'PY'
import re, sys, yaml
text = sys.stdin.read()
# deny python tags in emitted YAML
deny = re.compile(r"(?:!!python/|tag:yaml\.org,2002:python/)")
if deny.search(text):
    raise SystemExit("Found python tags in YAML output")
print("ok")
PY < artifact.yaml
```

(You’re enforcing a property the docs describe: safe output ≠ python tags.) 

### F5.2 Input guardrail: **two-pass “parse then construct”**

If you accept YAML from anywhere semi-trusted, do:

1. **Parse/compose to nodes** (no construction), walk tags, enforce allowlist
2. Then construct with your policy loader

PyYAML exposes tag strings and the tag families you need to allow/deny (standard + python tags). 

Allowlist example (standard tags only + your custom tags):

* allow: `tag:yaml.org,2002:*` (standard), plus `!Ref`, `!Duration`
* deny: anything with prefix `tag:yaml.org,2002:python/` (python tags), plus unknown tag handles

### F5.3 “I need custom objects but want SafeLoader semantics”

PyYAML documents the exact intended approach: `safe_load` limits construction; you can mark a Python object “safe” (recognized by safe_load) by deriving from `YAMLObject` and setting `yaml_loader` to `yaml.SafeLoader`. 
In practice, for production code you nearly always want “SafeLoader subclass + explicit constructors” (more auditable), but YAMLObject is the supported shortcut. 

---

## F6) “Operator-grade” default policy (copy/paste)

If your doc is steering teams toward the right baseline, the policy module usually looks like:

* Load: `PolicyLoader = CSafeLoader if present else SafeLoader`
* Dump: `PolicyDumper = CSafeDumper if present else SafeDumper`
* Allow custom tags only via explicit constructors/representers on those policy classes
* Reject python tags on input, and assert none are produced on output

This aligns exactly with PyYAML’s own framing: safe_load/safe_dump are the standard-tag-only surfaces; python/object tags exist but are dangerous if used on untrusted input. 

If you want to continue in the same “agent-ready” style, the next best chapter is **G) Extensibility API deep dive** (constructors vs multi-constructors, representers vs multi-representers, and resolver strategies to make tags + implicit typing behave like a schema).

[1]: https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load%28input%29-Deprecation "PyYAML yaml.load(input) Deprecation · yaml/pyyaml Wiki · GitHub"

## G) Extensibility API: constructors, representers, resolvers

### G0) Non-negotiable rule: **bind extensions to the exact Loader/Dumper you use**

PyYAML’s extensibility state lives on **class-level registries** (constructors, representers, resolvers). The module-level helpers (`yaml.add_constructor`, `yaml.add_implicit_resolver`, etc.) have defaults that **do not target `SafeLoader`** unless you explicitly pass `Loader=`—they register on `Loader`, `FullLoader`, and `UnsafeLoader` by default.([Chromium Git Repositories][1])

Actionable policy:

* App code: build a `PolicyLoader` / `PolicyDumper` (Safe-tier), register everything there, and always call `yaml.load(..., Loader=PolicyLoader)` / `yaml.dump(..., Dumper=PolicyDumper)`.
* Library code: never mutate global registries; accept a Loader/Dumper class as an argument and mutate only that.

---

## G1) Constructors (YAML → Python)

### G1.1 API surface + call signatures

**Module-level helpers (recommended only with explicit `Loader=`):**

* `yaml.add_constructor(tag, constructor, Loader=...)`
* `yaml.add_multi_constructor(tag_prefix, multi_constructor, Loader=...)`

**Constructor function signatures:**

```python
def constructor(loader, node): -> object
def multi_constructor(loader, tag_suffix, node): -> object
```

A constructor gets the loader instance + a YAML node (`ScalarNode`, `SequenceNode`, `MappingNode`). A multi-constructor also gets the suffix after the matched prefix.

### G1.2 Node-shape primitives you should always use inside constructors

Use the loader’s helpers to validate node kind and delegate recursive construction:

* `loader.construct_scalar(node)`
* `loader.construct_sequence(node)`
* `loader.construct_mapping(node)`

These enforce node type and (for mappings) validate hashable keys.([Chromium Git Repositories][2])

**Pattern (scalar-only tag):**

```python
import yaml

class PolicyLoader(getattr(yaml, "CSafeLoader", yaml.SafeLoader)):
    pass

def construct_env(loader: PolicyLoader, node: yaml.Node) -> str:
    raw = loader.construct_scalar(node)   # enforces scalar node
    # expand env etc...
    return raw

PolicyLoader.add_constructor("!ENV", construct_env)
```

(You can register via classmethod or module helper; classmethod avoids default-loader surprises.)([Chromium Git Repositories][2])

### G1.3 How constructor dispatch actually works (so you can design tag strategies)

PyYAML’s `construct_object()` picks a constructor in this order:([Chromium Git Repositories][2])

1. exact tag match: `yaml_constructors[node.tag]`
2. prefix match: first `tag_prefix` where `node.tag.startswith(tag_prefix)` from `yaml_multi_constructors`
3. default multi-constructor: `yaml_multi_constructors[None]` (if present)
4. default constructor: `yaml_constructors[None]` (if present)
5. fallback by node type: scalar/sequence/mapping default constructors

This matters because it enables three “industrial” patterns:

#### (A) Single explicit tag → single constructor

Best for config-like tags: `!Ref`, `!Duration`, `!Env`.

#### (B) Namespaced tags → one multi-constructor

Best for “typed union” namespaces:

```yaml
!<tag:example.com,2026:ref> abc
!<tag:example.com,2026:duration> 5s
```

```python
import yaml
from dataclasses import dataclass

class PolicyLoader(getattr(yaml, "CSafeLoader", yaml.SafeLoader)): pass

@dataclass(frozen=True)
class Ref: key: str

def multi(loader: PolicyLoader, suffix: str, node: yaml.Node):
    if suffix == "ref":
        return Ref(loader.construct_scalar(node))
    if suffix == "duration":
        return loader.construct_scalar(node)  # parse later
    raise ValueError(f"unknown suffix {suffix!r}")

PolicyLoader.add_multi_constructor("tag:example.com,2026:", multi)
```

Multi-constructor suffix is the part after the prefix (by `len(tag_prefix)` slicing).([Chromium Git Repositories][1])

#### (C) “Catch-all unknown tags” → default multi-constructor (`tag_prefix=None`)

You can install a default multi-constructor and preserve unknown tags (useful when ingesting YAML from tools that emit arbitrary tags). The engine explicitly checks for `yaml_multi_constructors[None]` as the fallback.([Chromium Git Repositories][2])

⚠️ Version footgun: older releases had bugs around `None` handling; the project history notes fixes like “allow calling add_multi_constructor with None” and related loader-default repairs.

### G1.4 Recursion/anchors: why constructors must be *pure* + re-entrant

`construct_object()` caches constructed objects and tracks recursive nodes; if a node appears again, it returns the cached Python object (anchor/alias behavior). If it encounters an unconstructable recursive node it raises `ConstructorError`.([Chromium Git Repositories][2])

Practical implications for custom constructors:

* Do **not** mutate global state while constructing.
* If you need “second pass” fixups (e.g., forward refs), store placeholders and resolve after `load()` returns.

### G1.5 CLI harness: validate constructor registration actually attaches to your Loader

```bash
python - <<'PY'
import yaml

class L(getattr(yaml, "CSafeLoader", yaml.SafeLoader)): pass
L.add_constructor("!X", lambda loader, node: ("X", loader.construct_scalar(node)))

print(yaml.load("a: !X hi\n", Loader=L))
PY
```

---

## G2) Representers (Python → YAML)

### G2.1 API surface + signatures

* `yaml.add_representer(py_type, representer, Dumper=...)`
* `yaml.add_multi_representer(base_type, multi_representer, Dumper=...)`

Representer signature:

```python
def representer(dumper, data) -> yaml.Node
```

You return a YAML node (scalar/sequence/mapping).

### G2.2 How representer dispatch works (MRO + exact-type precedence)

`represent_data()` does this:([Chromium Git Repositories][3])

1. compute `type(data).__mro__`
2. if exact type is in `yaml_representers`: use it
3. else walk the MRO; first match in `yaml_multi_representers` wins
4. else fallback to `yaml_multi_representers[None]`, then `yaml_representers[None]`, else emit `ScalarNode(None, str(data))`

**Why you care:**

* If you register a representer for `dict`, it **won’t automatically cover dict subclasses** unless you use `add_multi_representer`. This is a common gotcha and has caused real-world regressions and confusion.([GitHub][4])

### G2.3 Safe emission vs “Python object emission” (and what triggers python tags)

Safe-tier dumpers are wired so that “unknown object” becomes an error: `represent_undefined()` raises `RepresenterError`, and `None` is explicitly mapped to that undefined representer in `SafeRepresenter`.([Chromium Git Repositories][3])

Non-safe `Representer` (used by `Dumper` / `CDumper`) can serialize arbitrary objects using `__reduce__` / `copyreg`, and emits `!!python/object*` / `!!python/object/apply*` tags.([Chromium Git Repositories][3])

Actionable rule:

* If your YAML must be portable: base your dumper on `SafeDumper` / `CSafeDumper` and explicitly represent your custom types with standard tags or your own `!Tag`.

### G2.4 The core node builders you should use

Use these dumper methods (they also wire alias/anchor bookkeeping correctly):

* `dumper.represent_scalar(tag, value, style=...)`([Chromium Git Repositories][3])
* `dumper.represent_sequence(tag, seq, flow_style=...)`([Chromium Git Repositories][3])
* `dumper.represent_mapping(tag, mapping, flow_style=...)` (also applies `sort_keys` if enabled)([Chromium Git Repositories][3])

**Example: represent a dataclass as a tagged mapping (portable)**

```python
import yaml
from dataclasses import dataclass, asdict

class PolicyDumper(getattr(yaml, "CSafeDumper", yaml.SafeDumper)):
    pass

@dataclass(frozen=True)
class Ref:
    key: str

def repr_ref(dumper: PolicyDumper, obj: Ref) -> yaml.Node:
    return dumper.represent_mapping("!Ref", asdict(obj))

PolicyDumper.add_representer(Ref, repr_ref)

print(yaml.dump({"x": Ref("abc")}, Dumper=PolicyDumper, sort_keys=False))
```

This stays out of python/object emission while still round-tripping via your custom tag.([Chromium Git Repositories][1])

### G2.5 Aliases/anchors: controlling when `&id001` appears

Representation tracks `alias_key=id(data)` unless `ignore_aliases(data)` says otherwise; repeated objects reuse the same node.([Chromium Git Repositories][3])

To force “no anchors ever”:

```python
import yaml

class NoAliasSafeDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True
```

(PyYAML uses `ignore_aliases()` as the alias gate.)([Chromium Git Repositories][3])

### G2.6 CLI harness: verify multi_representer actually catches subclasses

```bash
python - <<'PY'
import yaml

class D(yaml.SafeDumper): pass

class Base: pass
class Child(Base): pass

D.add_multi_representer(Base, lambda dumper, obj: dumper.represent_scalar("!Base", obj.__class__.__name__))

print(yaml.dump({"x": Child()}, Dumper=D, sort_keys=False))
PY
```

---

## G3) Resolvers (implicit tagging + path-based “schema-ish” behavior)

### G3.1 add_implicit_resolver: regex-triggered tags for **plain scalars**

**API:** `add_implicit_resolver(tag, regexp, first, Loader=..., Dumper=...)`

Implementation detail you should design for:

* The resolver table is keyed by the **first character** of the scalar (performance index). If you pass `first=None`, it becomes `[None]` and is treated as a wildcard bucket.([Chromium Git Repositories][5])

**Example: implicit `!uuid` for `UUID(...)` strings**

```python
import re, uuid, yaml

regex = re.compile(r"^UUID\((.+)\)$")

class L(getattr(yaml, "CSafeLoader", yaml.SafeLoader)): pass
L.add_implicit_resolver("!uuid", regex, list("U"))

def construct_uuid(loader: L, node: yaml.Node) -> uuid.UUID:
    raw = loader.construct_scalar(node)
    return uuid.UUID(regex.match(raw).group(1))

L.add_constructor("!uuid", construct_uuid)

print(yaml.load("x: UUID(6a02171e-6482-11e9-ab43-f2189845f1cc)\n", Loader=L))
```

This pattern is exactly how implicit tagging and constructors are meant to compose.([Chromium Git Repositories][5])

#### Hard limitation: quoted scalars won’t match your implicit resolver

A real-world repro shows the implicit resolver matches the unquoted form, but not the quoted form; explicitly tagging still works.([GitHub][6])
This aligns with the documentation statement that `add_implicit_resolver` is for “plain scalars.”

### G3.2 Resolver precedence: implicit regex wins; path resolvers don’t override matches

`resolve()` checks implicit resolvers first and **returns immediately** if a regex matches; only then does it consult path resolvers.([Chromium Git Repositories][5])

Operational consequence:

* You cannot use `add_path_resolver()` to “force string” for values like `false` or `11.2` while leaving them unquoted, because built-in bool/float implicit resolvers will match first and return.([Chromium Git Repositories][5])

If you need “string at that path” anyway, your practical options are:

* enforce explicit tags (`!!str`) in the YAML
* enforce quoting (turns it into non-plain scalar)
* or load with `BaseLoader` and cast yourself

### G3.3 add_path_resolver: match by structural path (experimental)

**API:** `add_path_resolver(tag, path, kind, Loader=..., Dumper=...)`

Important: the implementation explicitly labels this API **experimental**.([Chromium Git Repositories][5])

Path matching model (compressed but precise):

* A path compiles into `(node_check, index_check)` pairs.
* `index_check` meaning:

  * `True` → mapping keys
  * `None` / `False` → any mapping/sequence value
  * `str` → mapping value whose scalar key equals that string
  * `int` → sequence index match([Chromium Git Repositories][5])
* `node_check` can be a node class (`ScalarNode`/`SequenceNode`/`MappingNode`), `None`, or a string tag that must equal `current_node.tag`.([Chromium Git Repositories][5])
* `kind` can be `str`/`list`/`dict` and gets mapped to the corresponding node class.([Chromium Git Repositories][5])

**Example: tag all scalars at `spec.id` as `!uuid` (works when values don’t match any built-in implicit resolver)**

```python
import re, uuid, yaml

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)

class L(getattr(yaml, "CSafeLoader", yaml.SafeLoader)): pass

# if value doesn't match bool/float/etc, path resolver can assign !uuid
L.add_path_resolver("!uuid", ["spec", "id"], kind=str)

def construct_uuid(loader: L, node: yaml.Node) -> uuid.UUID:
    raw = loader.construct_scalar(node)
    return uuid.UUID(raw)

L.add_constructor("!uuid", construct_uuid)

doc = "spec:\n  id: 6a02171e-6482-11e9-ab43-f2189845f1cc\n"
print(yaml.load(doc, Loader=L))
```

(If you need regex-based tagging, prefer `add_implicit_resolver`; if you need location-based tagging, use `add_path_resolver`.)([Chromium Git Repositories][5])

### G3.4 Overriding YAML 1.1 implicit typing (bool “yes/no/on/off”) is a resolver-edit job

PyYAML’s default bool resolver explicitly includes `yes/no/on/off` and seeds `first` with `yYnNtTfFoO`.([Chromium Git Repositories][5])
To change semantics, you typically:

1. subclass your Loader
2. copy/trim `yaml_implicit_resolvers`
3. re-add your own bool resolver

(You already saw this pattern in B8; it’s the canonical way because resolver tables are class-level and copied-on-write when modified.)([Chromium Git Repositories][5])

---

## G4) Isolation patterns (global mutation vs custom policy classes)

### G4.1 Why global mutation is toxic (and subtly broken for safe_load)

If you call `yaml.add_constructor(tag, ctor)` with no `Loader=`, it registers on `loader.Loader`, `loader.FullLoader`, and `loader.UnsafeLoader`—**not** `SafeLoader`. So `yaml.safe_load()` won’t see your tag.([Chromium Git Repositories][1])

Same for `add_implicit_resolver` / `add_path_resolver`: the module-level default attaches to those same non-safe loaders unless you pass `Loader=`.([Chromium Git Repositories][1])
This mismatch has been a long-standing source of “my resolver/constructor doesn’t fire” reports, especially around the 5.1 loader-default change.

### G4.2 Your “policy module” template (safe + isolated + C-fast-path)

```python
import yaml

BaseSafeLoader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
BaseSafeDumper = getattr(yaml, "CSafeDumper", yaml.SafeDumper)

class PolicyLoader(BaseSafeLoader): ...
class PolicyDumper(BaseSafeDumper): ...

# Register constructors/resolvers/representers on Policy* classes only.
# Always call yaml.load(..., Loader=PolicyLoader) / yaml.dump(..., Dumper=PolicyDumper).
```

Why this is correct mechanically:

* `add_constructor` / `add_multi_constructor` copy the constructor dict on first mutation for a subclass.([Chromium Git Repositories][2])
* representers do the same for representer dicts.([Chromium Git Repositories][3])
* implicit resolvers copy their tables on first mutation, including per-initial-char lists.([Chromium Git Repositories][5])

### G4.3 YAMLObject defaults are *not safe-tier*

`YAMLObject` registers its constructor on `yaml_loader = [Loader, FullLoader, UnsafeLoader]` by default.([Chromium Git Repositories][1])
If you want a YAMLObject to be recognized by safe load, docs say you must explicitly set `yaml_loader = yaml.SafeLoader` (or your safe policy loader).

### G4.4 Minimal regression tests you should standardize

1. **No python tags in emitted YAML** (if you promise portability)
2. **Round-trip property**: `safe_load(dump(obj)) == obj` for safe types
3. **Loader binding**: a fixture YAML containing your tags fails under plain `SafeLoader` and succeeds under `PolicyLoader`

---

If you want to keep going in the same “agent-ready” direction, the next chapter is **H) Low-level parsing & emission pipeline surfaces**—`scan`/`parse`/`compose` as debugging tools for tags/resolvers (especially when a resolver “should have matched” but didn’t).

[1]: https://chromium.googlesource.com/chromium/src.git/%2B/refs/tags/138.0.7167.0/third_party/pyyaml/__init__.py "third_party/pyyaml/__init__.py - chromium/src.git - Git at Google"
[2]: https://chromium.googlesource.com/chromium/src/%2B/refs/tags/123.0.6293.1/third_party/pyyaml/constructor.py "third_party/pyyaml/constructor.py - chromium/src - Git at Google"
[3]: https://chromium.googlesource.com/chromium/src/%2B/main/third_party/pyyaml/representer.py "third_party/pyyaml/representer.py - chromium/src - Git at Google"
[4]: https://github.com/yaml/pyyaml/issues/142 "Regression - RepresenterError on dict subclass · Issue #142 · yaml/pyyaml · GitHub"
[5]: https://chromium.googlesource.com/chromium/src/%2B/refs/tags/130.0.6707.2/third_party/pyyaml/resolver.py "third_party/pyyaml/resolver.py - chromium/src - Git at Google"
[6]: https://github.com/yaml/pyyaml/issues/457 "add_implicit_resolver does not match quoted strings · Issue #457 · yaml/pyyaml · GitHub"

## H) Low-level parsing & emission pipeline surfaces (scan / parse / compose / emit)

### H0) The three “inspection layers” and what each one gives you

PyYAML exposes three progressively higher-level, lower-level-than-`load()` views of the same YAML stream:

1. **Tokens** (scanner / lexer): `yaml.scan(stream, Loader=...) → Token`
2. **Events** (parser / emitter; SAX-like): `yaml.parse(stream, Loader=...) → Event` and `yaml.emit(events, ...) → YAML text`
3. **Nodes** (composer / serializer; DOM-like): `yaml.compose(...) → Node`, `yaml.compose_all(...) → Iterator[Node]`, and `yaml.serialize(node[s], ...) → YAML text`

These are *public* APIs; `yaml.scan`/`yaml.parse`/`yaml.compose`/`yaml.compose_all` are implemented directly as wrappers that instantiate a `Loader(stream)` and then repeatedly `check_*`/`get_*` until exhausted, finally calling `dispose()` for cleanup. ([Fuchsia][1])

---

## H1) Token stream (scanner layer): `yaml.scan(...) → token objects`

### H1.1 What `yaml.scan()` actually does (generator semantics + cleanup)

Implementation (shape you should remember):

* constructs a loader, then `while loader.check_token(): yield loader.get_token()`, then `loader.dispose()` in `finally`. ([Fuchsia][1])

**Why this matters operationally**

* It’s **streaming** (doesn’t build the full parse tree), so it’s the lowest-memory way to “peek” at YAML structure.
* It’s also **lifetime sensitive**: if you pass a file object, keep it open until iteration completes.

### H1.2 When tokens are worth using

PyYAML explicitly frames tokens as “not really useful except for low-level YAML applications such as syntax highlighting.” 
So: use tokens for **syntax highlighting**, **lexical linting**, **precise location reporting**, and **debugging “why does the parser think this is a key/value/etc.”**.

### H1.3 Token catalog (what you’ll see, and what fields exist)

PyYAML’s scanner produces the following token types (scanner’s own comment list). ([Fuchsia][2])

#### “Structural / punctuation tokens”

From PyYAML’s token class definitions (`id` is what the parser expects): ([Fuchsia][3])

* `StreamStartToken(encoding=...)` → `id='<stream start>'`
* `StreamEndToken` → `id='<stream end>'`
* `DirectiveToken(name,value)` → `id='<directive>'` (for `%YAML` and `%TAG`)
* `DocumentStartToken` → `id='<document start>'` (the `---` marker)
* `DocumentEndToken` → `id='<document end>'` (the `...` marker)
* Block collections:

  * `BlockSequenceStartToken` / `BlockMappingStartToken` / `BlockEndToken`
  * `BlockEntryToken` → `id='-'` (sequence entry)
* Flow collections:

  * `FlowSequenceStartToken` → `id='['`
  * `FlowSequenceEndToken` → `id=']'`
  * `FlowMappingStartToken` → `id='{'`
  * `FlowMappingEndToken` → `id='}'`
  * `FlowEntryToken` → `id=','`
* Mapping shape:

  * `KeyToken` → `id='?'` (explicit key *or* “simple key start”)
  * `ValueToken` → `id=':'`

#### “Reference & typing tokens”

Also defined directly in `tokens.py`: ([Fuchsia][3])

* `AliasToken(value=...)` → `id='<alias>'` (the `*A` form)
* `AnchorToken(value=...)` → `id='<anchor>'` (the `&A` form)
* `TagToken(value=...)` → `id='<tag>'` (tag handles / `!!int` / `!foo` etc.)
* `ScalarToken(value, plain, style=...)` → `id='<scalar>'`

#### Token positional data

All tokens carry `start_mark` and `end_mark` (and `__repr__` intentionally hides `*_mark` fields and prints the meaningful attributes). ([Fuchsia][3])
That makes tokens ideal for “point exactly to the bad lexeme”.

### H1.4 Practical “token debug harness” (CLI copy/paste)

```bash
python - <<'PY'
import sys, yaml
L = getattr(yaml, "CSafeLoader", yaml.SafeLoader)  # safer default tier for *scan/parse/compose*, too
doc = sys.stdin.read()
for t in yaml.scan(doc, Loader=L):
    mark = getattr(t, "start_mark", None)
    where = f"{mark.line+1}:{mark.column+1}" if mark else "?:?"
    payload = {k:v for k,v in t.__dict__.items() if not k.endswith("_mark")}
    print(f"{where}  {t.__class__.__name__}  {payload}")
PY < your.yaml
```

---

## H2) Event stream (parser/emitter layer): `yaml.parse(...) → events` and `yaml.emit(...) → YAML`

### H2.1 What `yaml.parse()` does (and what it *doesn’t*)

`yaml.parse(stream, Loader=...)` is also a generator: create loader → `while loader.check_event(): yield loader.get_event()` → dispose. ([Fuchsia][1])

**Crucial boundary:** events are the parser’s *structural* output; they’re not “constructed Python objects”. Events are explicitly described as the low-level “Parser and Emitter interfaces, similar to the SAX API.” 

### H2.2 Event catalog (the full grammar surface you must obey)

PyYAML defines the event types (docs list them; the code defines the base classes and concrete event classes). 

* Stream framing:

  * `StreamStartEvent(encoding=...)`
  * `StreamEndEvent`
* Document framing:

  * `DocumentStartEvent(explicit, version, tags, ...)`
  * `DocumentEndEvent(explicit, ...)`
* Node events:

  * `AliasEvent(anchor=...)`
  * `ScalarEvent(anchor, tag, implicit, value, style, ...)`
* Collection events:

  * `SequenceStartEvent(anchor, tag, implicit, flow_style, ...)`
  * `SequenceEndEvent`
  * `MappingStartEvent(anchor, tag, implicit, flow_style, ...)`
  * `MappingEndEvent`

**Event payload semantics you should actually use**

* `flow_style`: `None | True | False` (block vs flow decision) 
* scalar `style`: `None`, `"'"`, `'"'`, `"|"`, `">"` (and `_` is used in docs’ style list) 
* scalar `implicit`: a pair `(plain_ok, nonplain_ok)` controlling whether the tag may be omitted when emitting 

### H2.3 `yaml.emit(events, ...)` — the inverse of `parse()`

`yaml.emit(...)` constructs a `Dumper(stream, ...)` and then loops `dumper.emit(event)` for each event; if `stream is None`, it returns the produced string. ([Fuchsia][1])

This is your “surgical tool”:

* **parse → rewrite events → emit** (stream transformer)
* **build events from scratch** (rare; but useful for deterministic output generators)

### H2.4 Event grammar constraints (the things that cause “expected NodeEvent…”)

The emitter is strict about event ordering; its grammar is documented right in `emitter.py`: ([Fuchsia][4])

* `stream ::= STREAM-START document* STREAM-END`
* `document ::= DOCUMENT-START node DOCUMENT-END`
* (and then node/sequence/mapping expansions)

If you generate events, you must obey this grammar (or you’ll get runtime exceptions from the emitter).

### H2.5 Canonical “round-trip events” trick (fast sanity check)

* If you want to validate that a YAML input is parseable and can be re-emitted structurally, you can do:

  * `yaml.emit(yaml.parse(text, Loader=...), canonical=True)`
    because `emit()` accepts formatting knobs (`canonical`, `indent`, `width`, `allow_unicode`, `line_break`). ([Fuchsia][1])

---

## H3) Node graph (composer/serializer layer): `yaml.compose(...) → Node`

### H3.1 What nodes are (and why this is the “schema enforcement” sweet spot)

Nodes are the YAML informational model objects: `ScalarNode`, `SequenceNode`, `MappingNode`. PyYAML’s docs explicitly state nodes are produced by **Composer** and can be serialized by **Serializer**. 
And YAML spec-wise, a node has a **tag** and content of kind scalar/sequence/mapping. ([YAML][5])

**Why you’ll use nodes in real systems**

* Inspect tags **before** Python construction (safer than `load`)
* Enforce: “only these tags allowed” / “these paths must be strings” / “no anchors” / “no merges”
* Apply transformations at the representation level, then serialize back to YAML

### H3.2 `yaml.compose()` / `yaml.compose_all()` mechanics

* `compose(stream, Loader=...)` returns the **single root node** of the first document via `loader.get_single_node()` and disposes the loader. ([Fuchsia][1])
* `compose_all(stream, Loader=...)` yields nodes for all documents using `loader.check_node()` / `loader.get_node()`. ([Fuchsia][1])

### H3.3 Node shapes (exact invariants)

From docs (these invariants matter when you write visitors/rewriters): 

* `ScalarNode(tag, value, style, start_mark, end_mark)`

  * `value` is a unicode string
* `SequenceNode(tag, value, flow_style, start_mark, end_mark)`

  * `value` is a list of nodes
* `MappingNode(tag, value, flow_style, start_mark, end_mark)`

  * `value` is a list of `(key_node, value_node)` pairs

And the implementation shows the base `Node` carries `tag`, `value`, `start_mark`, `end_mark`. ([Fuchsia][6])

### H3.4 The critical internal step: **compose resolves implicit tags**

This is the point where untagged scalars acquire explicit canonical tags.

In the composer implementation:

* `compose_scalar_node()` takes a `ScalarEvent`; if the event tag is `None` or `'!'`, it assigns a tag via `self.resolve(ScalarNode, event.value, event.implicit)` before creating `ScalarNode(tag, event.value, ...)`. ([Searchfox][7])

So:

* `parse()` output can show `ScalarEvent(tag=None, value="yes")`
* `compose()` output can show `ScalarNode(tag="tag:yaml.org,2002:bool", value="yes")` (depending on resolver rules)

That’s the clean boundary to debug “implicit typing”.

### H3.5 `yaml.serialize(node)` / `yaml.serialize_all(nodes)` (nodes → YAML)

`serialize_all()` does:

* create `Dumper(...)`
* `dumper.open()` (emits `StreamStartEvent`)
* `dumper.serialize(node)` per node
* `dumper.close()` (emits `StreamEndEvent`)
* dispose; return string if stream is None ([Fuchsia][1])

So: **nodes are the “DOM”, serialize is the “DOM → text” pipe**.

---

## H4) Safety + default-class footguns (low-level APIs included)

PyYAML’s docs explicitly say: by default **`scan`, `parse`, `compose`, `construct`, … use `Loader`**, and `Loader` “may construct an arbitrary Python object” and is not safe for untrusted sources. 

Even if you’re “only scanning/parsing”, standardize your tooling to pass a safe tier anyway:

```python
L = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
tokens = yaml.scan(stream, Loader=L)
events = yaml.parse(stream, Loader=L)
node   = yaml.compose(stream, Loader=L)
```

C-based variants exist (`CSafeLoader`, etc.), and PyYAML notes there can be subtle differences between pure-Python and LibYAML-based parsers/emitters. 

---

## H5) High-value debugging workflows (what you actually do with these surfaces)

### Workflow 1: “Why is this YAML failing?” → tokens first

* Run `yaml.scan` and print token + `start_mark` line/column to pinpoint where lexical structure breaks. Tokens are positioned, and token types correspond closely to punctuation and indentation boundaries. 

### Workflow 2: “Why did this value get typed/tagged?” → parse vs compose diff

* `yaml.parse` shows events (often `ScalarEvent(tag=None, value="...")`)
* `yaml.compose` shows nodes after tag resolution (explicit tag strings) 

### Workflow 3: “I need to rewrite YAML but keep it YAML (not Python objects)” → compose → transform → serialize

* Compose nodes, walk the node graph, rewrite tags/values, then `yaml.serialize(node)` back out. 

---

If you want the next chapter after H in the same dense style, the natural continuation is **I) Scalar resolution & YAML 1.1 semantics** (exact resolver regexes, why `yes/no/on/off` become booleans, and how to override safely with resolver table edits).

[1]: https://fuchsia.googlesource.com/third_party/pyyaml.git/%2B/refs/tags/5.4/lib/yaml/__init__.py "lib/yaml/__init__.py - third_party/pyyaml.git - Git at Google"
[2]: https://fuchsia.googlesource.com/third_party/pyyaml.git/%2B/refs/tags/5.4/lib/yaml/scanner.py "lib/yaml/scanner.py - third_party/pyyaml.git - Git at Google"
[3]: https://fuchsia.googlesource.com/third_party/pyyaml.git/%2B/refs/tags/5.4/lib/yaml/tokens.py "lib/yaml/tokens.py - third_party/pyyaml.git - Git at Google"
[4]: https://fuchsia.googlesource.com/third_party/pyyaml.git/%2B/refs/tags/5.4/lib/yaml/emitter.py "lib/yaml/emitter.py - third_party/pyyaml.git - Git at Google"
[5]: https://yaml.org/spec/1.2.2/?utm_source=chatgpt.com "YAML Ain't Markup Language (YAML™) revision 1.2.2"
[6]: https://fuchsia.googlesource.com/third_party/pyyaml.git/%2B/refs/tags/5.4/lib/yaml/nodes.py "lib/yaml/nodes.py - third_party/pyyaml.git - Git at Google"
[7]: https://searchfox.org/firefox-main/source/third_party/python/pyyaml/lib/yaml/composer.py?utm_source=chatgpt.com "composer.py - mozsearch"

