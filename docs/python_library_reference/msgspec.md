
Below is a **feature-catalog + semantic clustering** for **`msgspec` (v0.20.0)**, structured in the same “surface area first” spirit as your attached PyArrow reference.  ([PyPI][1])

---

# msgspec comprehensive feature catalog

## 0) Mental model

`msgspec` is essentially:

1. **A schema/type system** expressed in standard Python type annotations (plus `Annotated[..., msgspec.Meta(...)]` constraints).
2. **A fast struct/object model** (`msgspec.Struct`) optimized for decode/encode + general use.
3. **A family of codecs** with a consistent interface across protocols: `msgspec.json`, `msgspec.msgpack`, `msgspec.yaml`, `msgspec.toml`. ([Jim Crist-Harif][2])

---

## 1) Core data modeling: Structs as the “schema object”

### 1.1 `msgspec.Struct` (C-backed “dataclass-like” schema object)

**What it gives you**

* Field definitions via type annotations; generated `__init__`, `__eq__`, `__repr__`, `__copy__`; `__struct_fields__` for field names; runtime config available via `__struct_config__`. ([Jim Crist-Harif][3])

**Config surface (class definition keywords)**

* **Value semantics & construction:** `kw_only`, `eq`, `order`
* **Encoding shape & compat:** `array_like`, `rename`, `omit_defaults`, `forbid_unknown_fields`
* **Mutability & identity:** `frozen` (hashable), `dict` (add `__dict__`), `weakref`
* **Tagged unions:** `tag` + `tag_field` (default tag-field `"type"` when tagged)
* **Runtime/GC knobs:** `gc` (disable to reduce GC pressure; you own cycle safety), `cache_hash` (cache hash for frozen structs) ([Jim Crist-Harif][3])

### 1.2 `msgspec.StructMeta`

* Metaclass for defining Structs; can be subclassed to set default struct config (and can be combined with `abc.ABCMeta`; other metaclass combos aren’t supported as public API). ([Jim Crist-Harif][3])

### 1.3 Field-level config: `msgspec.field(...)`

* Per-field default or `default_factory`, plus per-field encoded name override (`name=...`) that can supersede struct-level `rename`. ([Jim Crist-Harif][3])

### 1.4 Dynamic struct construction: `msgspec.defstruct(...)`

* Build a Struct class at runtime from `(name, type[, default|field(...)])` field specs + the same struct config knobs. ([Jim Crist-Harif][3])

### 1.5 Struct utilities (`msgspec.structs.*`)

* `replace(struct, **changes)` (dataclasses-like copy-with-changes) ([Jim Crist-Harif][3])
* `asdict(struct)` / `astuple(struct)` ([Jim Crist-Harif][3])
* `fields(type_or_instance) -> tuple[FieldInfo]` (runtime field inspection) ([Jim Crist-Harif][3])
* `force_setattr(struct, name, value)` (mutate even if frozen; explicitly “unsafe” and intended for `__post_init__`-style tweaks) ([Jim Crist-Harif][3])
* `FieldInfo(...)`, `StructConfig`, `NODEFAULT` sentinels for defaults/config reflection ([Jim Crist-Harif][3])

---

## 2) “Schema control” primitives: constraints, metadata, and sentinels

### 2.1 `msgspec.Meta(...)` (constraints + schema metadata)

`Meta` can be attached via `typing.Annotated[T, Meta(...)]` to express:

* **Numeric constraints:** `gt/ge/lt/le/multiple_of`
* **String constraints:** `pattern` (uses unanchored matching semantics), `min_length/max_length`
* **Collection/dict length constraints:** `min_length/max_length`
* **Datetime/time timezone requirement:** `tz=True|False|None`
* **Schema metadata for JSON Schema / OpenAPI:** `title`, `description`, `examples`, `extra_json_schema` (recursive merge), and `extra` for arbitrary user metadata ([Jim Crist-Harif][3])

### 2.2 Constraint catalog (what’s supported)

Constraints are documented by category: Numeric, String, Datetime (`tz`), Bytes, Sequence, Mapping. The docs also flag a float precision caveat for non-integral `multiple_of` on floats. ([Jim Crist-Harif][4])

### 2.3 `msgspec.UNSET` + `msgspec.UnsetType`

* A singleton to distinguish “unset” vs “explicit null”.
* Encoding omits fields whose value is `UNSET`; decoding populates missing fields with `UNSET` if it’s the default. Works for Structs and also dataclasses/attrs types. ([Jim Crist-Harif][3])

### 2.4 `msgspec.Raw`

* A buffer wrapper for **delayed decoding** (store raw slice of message for a field) and for **embedding pre-encoded submessages** during encoding.
* `Raw.copy()` detaches from a larger backing buffer to reduce retained-memory risk. ([Jim Crist-Harif][3])

---

## 3) Type system coverage (what schemas can describe)

### 3.1 Supported type universe (high level)

* **Builtins:** `None`, `bool`, `int`, `float`, `str`, `bytes`, `bytearray`, `tuple/list/dict/set/frozenset`
* **msgspec types:** `msgspec.msgpack.Ext`, `msgspec.Raw`, `msgspec.UNSET`, `msgspec.Struct`
* **stdlib:** `datetime.datetime/date/time/timedelta`, `uuid.UUID`, `decimal.Decimal`, `enum.Enum/IntEnum/StrEnum/Flag/IntFlag`, `dataclasses.dataclass`
* **typing constructs:** `Any`, `Optional`, `Union`, `Literal`, `NewType`, `Final`, `TypeAliasType` / `TypeAlias`, `NamedTuple`, `TypedDict`, `Generic`, `TypeVar`
* **abstract collections:** `Collection/Sequence/Mapping` variants decode into their common concrete forms (lists/sets/dicts)
* **third-party:** `attrs` types ([Jim Crist-Harif][5])

### 3.2 Union / Optional decoding rules (ambiguity-elimination)

Unions are supported with restrictions to guarantee a single unambiguous decoding target. Example restrictions include: at most one “int-like” member, at most one “string-like” member, at most one “object-like” member, at most one “array-like” member. ([Jim Crist-Harif][5])

### 3.3 Generics

User-defined generics built on Struct/dataclasses/attrs/TypedDict/NamedTuple are supported; unparameterized generics substitute `Any` (or a `TypeVar` bound if present). ([Jim Crist-Harif][5])

---

## 4) Serialization protocols (consistent interface across submodules)

`msgspec` exposes protocol submodules with a consistent surface: `encode`, `decode`, and reusable `Encoder`/`Decoder` objects. ([Jim Crist-Harif][2])

### 4.1 Typed decoding and “strict vs lax”

* All `decode(...)` APIs accept `type=...` for **decode+validate** against an annotation schema.
* Default is strict (no unsafe implicit coercions); `strict=False` enables a wider set of string→non-string coercions. ([Jim Crist-Harif][2])

### 4.2 `msgspec.json`

**Encoder**

* `msgspec.json.Encoder(enc_hook=..., decimal_format={'string'|'number'}, uuid_format={'canonical'|'hex'}, order={None|'deterministic'|'sorted'})`
* Methods: `encode`, `encode_into(bytearray, offset=0|-1)`, `encode_lines(iterable)->NDJSON` ([Jim Crist-Harif][3])

**Decoder**

* `msgspec.json.Decoder(type=Any, strict=True|False, dec_hook=..., float_hook=...)`
* Methods: `decode`, `decode_lines` (parse NDJSON into list) ([Jim Crist-Harif][3])

**Top-level helpers**

* `msgspec.json.encode(...)`, `msgspec.json.decode(..., type=..., strict=..., dec_hook=...)`
* `msgspec.json.format(buf, indent=2|0|negative)` to reformat or minify JSON ([Jim Crist-Harif][3])

### 4.3 `msgspec.msgpack`

**Encoder**

* `msgspec.msgpack.Encoder(enc_hook=..., decimal_format={'string'|'number'}, uuid_format={'canonical'|'hex'|'bytes'}, order=...)`
* Methods: `encode`, `encode_into` ([Jim Crist-Harif][3])

**Decoder**

* `msgspec.msgpack.Decoder(type=Any, strict=..., dec_hook=..., ext_hook=...)`

  * `ext_hook(code: int, data: memoryview)` intercepts MessagePack extension values; otherwise they decode as `msgspec.msgpack.Ext`. The docs warn about holding references to `data` (view into a larger buffer). ([Jim Crist-Harif][3])

**Extension type**

* `msgspec.msgpack.Ext(code, data)` with `.code` and `.data` fields. ([Jim Crist-Harif][3])

**Top-level helpers**

* `msgspec.msgpack.encode(...)`, `msgspec.msgpack.decode(..., type=..., strict=..., dec_hook=..., ext_hook=...)` ([Jim Crist-Harif][3])

### 4.4 `msgspec.yaml`

* `msgspec.yaml.encode(obj, enc_hook=..., order=...)` / `msgspec.yaml.decode(buf, type=..., strict=..., dec_hook=...)`
* Notes: requires third-party **PyYAML** installed. ([Jim Crist-Harif][3])

### 4.5 `msgspec.toml`

* `msgspec.toml.encode(obj, enc_hook=..., order=...)` / `msgspec.toml.decode(buf, type=..., strict=..., dec_hook=...)` ([Jim Crist-Harif][3])

---

## 5) JSON Schema generation (schema export)

* `msgspec.json.schema(type, schema_hook=...)` → JSON Schema with shared components extracted into `"$defs"`. ([Jim Crist-Harif][3])
* `msgspec.json.schema_components(types, schema_hook=..., ref_template='#/$defs/{name}')` → multiple schemas + a separate `components` mapping (useful for OpenAPI composition).
* Output is documented as compatible with **JSON Schema 2020-12** and **OpenAPI 3.1**. ([Jim Crist-Harif][6])

---

## 6) Conversion utilities (protocol bridging + post-parse coercion)

### 6.1 `msgspec.convert(...)`

Convert an already-decoded object into a target annotation type:

* Knobs: `strict`, `from_attributes` (ORM-ish objects → Struct/dataclass/attrs by reading attributes), `dec_hook`, `str_keys`, `builtin_types` (declare which “exotic” types your wrapped protocol natively supports to disable msgspec coercions for them). ([Jim Crist-Harif][3])

### 6.2 `msgspec.to_builtins(...)`

Convert complex objects → “simple builtins” commonly supported by other serializers:

* Knobs: `builtin_types` passthrough, `str_keys`, `enc_hook`, and `order` (`None` / `'deterministic'` / `'sorted'`). ([Jim Crist-Harif][3])

---

## 7) Type inspection and reflection (tooling / registries / metaprogramming)

### 7.1 Predicates

* `msgspec.inspect.is_struct(obj)` and `msgspec.inspect.is_struct_type(tp)` for runtime detection (and type-checker narrowing). ([Jim Crist-Harif][3])

### 7.2 Type IR (“explain this annotation”)

* `msgspec.inspect.type_info(type)` and `multi_type_info(types)` return structured `msgspec.inspect.Type` objects; `multi_type_info` is explicitly called out as the efficient batch form. ([Jim Crist-Harif][3])

### 7.3 Inspect type lattice (what exists)

Every supported schema construct has a corresponding `msgspec.inspect.*Type` node (e.g., `IntType`, `StrType`, `ListType`, `DictType`, `TypedDictType`, `DataclassType`, `StructType`, `UnionType`, etc.), and object-like fields are described by `msgspec.inspect.Field`. The API docs enumerate the complete list. ([Jim Crist-Harif][3])

---

## 8) Extensibility patterns (custom types)

### 8.1 Hook-based extension (all protocols)

* `enc_hook(obj) -> supported_obj` to map unknown/custom types into msgspec-native representations.
* `dec_hook(type, obj) -> custom_obj` to map native representations back into a custom type **when doing typed decoding**. The docs specify that `TypeError`/`ValueError` raised by `dec_hook` become user-facing `ValidationError` with context. ([Jim Crist-Harif][7])

### 8.2 MessagePack extension types (MessagePack-only)

* Use `msgspec.msgpack.Ext(code, data)` in `enc_hook`, and decode it via `ext_hook(code, data_view)` to map binary extension payloads to custom objects (and potentially support untyped decoding, since the extension “type code” is transmitted). ([Jim Crist-Harif][7])

---

## 9) Schema evolution support (forward/backward compatibility)

`msgspec` documents “schema evolution” guidance, including:

1. New Struct fields must have defaults.
2. For `array_like=True`, don’t reorder fields; only append new fields (with defaults).
3. Don’t change type annotations for existing fields/messages.
4. For MessagePack extensions, don’t change type codes or implementations. ([Jim Crist-Harif][8])

---

## 10) Performance / operational knobs worth treating as “first-class”

* Reuse `Encoder`/`Decoder` instances instead of repeated top-level calls. ([Jim Crist-Harif][9])
* `encode_into` (JSON/MsgPack) to reuse a `bytearray` buffer and reduce allocations. ([Jim Crist-Harif][3])
* `omit_defaults=True` and `repr_omit_defaults=True` to reduce message size / repr noise. ([Jim Crist-Harif][3])
* `forbid_unknown_fields=True` when you want strictness over “schema evolution friendliness.” ([Jim Crist-Harif][3])
* `Raw.copy()` to avoid retaining large backing buffers in long-lived objects. ([Jim Crist-Harif][3])
* `gc=False` and `cache_hash=True` for specialized performance tuning (with correctness tradeoffs). ([Jim Crist-Harif][3])

---

## 11) Exception taxonomy

* `msgspec.MsgspecError` (base)
* `msgspec.EncodeError`
* `msgspec.DecodeError`
* `msgspec.ValidationError` (subclass of `DecodeError`, used when data fails schema validation) ([Jim Crist-Harif][3])

---

If you want the **next increment** (like you did for PyArrow): I can expand each section into a “deep dive” page (power knobs + failure modes + minimal snippets), but the above is the complete **clustered feature inventory** for the public `msgspec` surface.

[1]: https://pypi.org/project/msgspec/?utm_source=chatgpt.com "msgspec"
[2]: https://jcristharif.com/msgspec/usage.html "Usage"
[3]: https://jcristharif.com/msgspec/api.html "API Docs"
[4]: https://jcristharif.com/msgspec/constraints.html "Constraints"
[5]: https://jcristharif.com/msgspec/supported-types.html "Supported Types"
[6]: https://jcristharif.com/msgspec/jsonschema.html "JSON Schema"
[7]: https://jcristharif.com/msgspec/extending.html "Extending"
[8]: https://jcristharif.com/msgspec/schema-evolution.html "Schema Evolution"
[9]: https://jcristharif.com/msgspec/perf-tips.html "Performance Tips"

Below is the “next increment” you asked for: **a deep-dive companion** that expands each catalog section into **power knobs + failure modes + minimal snippets** (dense, reference-style, like your PyArrow doc).

I’m assuming **msgspec 0.20.0** (latest on PyPI as of Nov 24, 2025). ([PyPI][1])

---

# msgspec deep-dive companion (v0.20.0)

## A) Structs as schema objects — deep dive

### A0) Mental model: `Struct` is your *schema carrier*; validation happens at decode/convert boundaries

* `msgspec.Struct` is a C-backed dataclass-like object model that auto-generates `__init__`, `__repr__`, `__eq__`, `__copy__`, etc. You can define additional methods, but **cannot override `__init__` / `__new__`**. Config is via class keywords and introspectable via `__struct_config__`; field names via `__struct_fields__`. ([Jim Crist-Harif][2])
* **Runtime construction does not type-check your own code**; type checks are applied during **typed decoding** (and `convert`). ([Jim Crist-Harif][3])

### A1) Struct configuration surface (what matters operationally)

**Core semantics**

* `kw_only`: make all *newly defined* fields keyword-only; helps avoid required/optional ordering constraints (see A2). ([Jim Crist-Harif][2])
* `eq`, `order`: control equality and lexicographic ordering generation (tuple-of-fields semantics in definition order). ([Jim Crist-Harif][2])
* `frozen`: disables attribute mutation post-init and adds `__hash__` (requires all fields hashable). ([Jim Crist-Harif][2])

**Encoding/decoding shape & compatibility**

* `array_like`: encode/decode as arrays (positional, no field names) for perf; major schema-evolution constraints (append-only; no reorder). ([Jim Crist-Harif][2])
* `rename`: field name transform at the encoding boundary (snake_case ↔ camelCase, mapping, callable). Field-level `field(name=...)` overrides rename. ([Jim Crist-Harif][2])
* `omit_defaults`: omit fields that match defaults; size + speed wins but matching is optimized (not deep-equality). ([Jim Crist-Harif][2])
* `repr_omit_defaults`: same idea but for `repr` only. ([Jim Crist-Harif][2])
* `forbid_unknown_fields`: fail fast on unexpected object keys (good for configs). ([Jim Crist-Harif][2])

**Runtime/object model knobs**

* `dict=True`: adds `__dict__` so you can attach extra runtime state; otherwise Structs are slot-like and only declared fields exist. ([Jim Crist-Harif][2])
* `weakref=True`: allow weak references. ([Jim Crist-Harif][2])
* `gc=False`: never GC-track instances (can reduce GC pauses but risks uncollectable cycles). ([Jim Crist-Harif][2])
* `cache_hash=True`: cache hash for frozen structs (speed vs memory). ([Jim Crist-Harif][2])

### A2) Field ordering, defaults, and `kw_only`

`Struct` follows the same positional/keyword rules as Python function signatures:

* You **cannot place required (no-default) fields after optional (defaulted) fields** unless you switch to `kw_only=True`. The error message explicitly points you there. ([Jim Crist-Harif][3])
* `kw_only` affects only fields defined on that class; base/subclass ordering has specific rules (kw-only fields reorder after positional fields). ([Jim Crist-Harif][3])

**Minimal snippet**

```python
import msgspec

class Bad(msgspec.Struct):
    a: str = ""
    b: int  # TypeError (required after optional)

class Good(msgspec.Struct, kw_only=True):
    a: str = ""
    b: int   # ok
```

([Jim Crist-Harif][3])

### A3) Default value semantics (and why `field(default_factory=...)` matters)

* Defaults can be static (`x: int = 1`) or dynamic (`field(default_factory=...)`), and msgspec provides “safe sugar” for empty mutable defaults (`[]`, `{}`, `set()`, `bytearray()`) so you don’t accidentally share instances. ([Jim Crist-Harif][3])

### A4) Post-init (`__post_init__`) and controlled mutation for `frozen=True`

* If you define `__post_init__`, it runs after initialization **and** after typed decoding into that Struct. ([Jim Crist-Harif][3])
* If you freeze structs but need to normalize values in `__post_init__`, `msgspec.structs.force_setattr` exists (explicitly unsafe). ([Jim Crist-Harif][2])

### A5) Type validation boundary (don’t expect runtime checks on constructors)

* `Point(x=1, y="oops")` is allowed at runtime; typed decoding rejects with a `ValidationError` including a JSONPath-like location (`$.y`). ([Jim Crist-Harif][3])

**Minimal snippet**

```python
import msgspec

class Point(msgspec.Struct):
    x: float
    y: float

msgspec.json.decode(b'{"x":1.0,"y":"oops"}', type=Point)
# -> ValidationError: Expected `float`, got `str` - at `$.y`
```

([Jim Crist-Harif][3])

### A6) Pattern matching and ergonomics helpers

* Structs provide `__match_args__` enabling Python 3.10+ structural pattern matching. ([Jim Crist-Harif][3])
* Structs also define `__rich_repr__` for rich pretty-printing. ([Jim Crist-Harif][3])

### A7) Tagged unions (sum types that decode deterministically)

* You can enable tagging with `tag=True` (default `tag_field="type"`, tag value is the class name), or provide explicit tag values / tag callables. ([Jim Crist-Harif][2])
* Tagged unions are the supported way to decode unions of multiple struct types (see C4). ([Jim Crist-Harif][4])
* Tagged unions also work with `array_like=True`: tag is the **first array element**, then positional fields follow. ([Jim Crist-Harif][3])

**Minimal snippet**

```python
import msgspec
from typing import Union

class Get(msgspec.Struct, tag=True):
    key: str

class Put(msgspec.Struct, tag=True):
    key: str
    val: str

Op = Union[Get, Put]
msgspec.json.decode(b'{"type":"Put","key":"k","val":"v"}', type=Op)
```

([Jim Crist-Harif][3])

### A8) Omitting defaults (`omit_defaults=True`) — the fast path and the “default matching” caveat

* Omitting defaults reduces payload size and often speeds encode/decode. ([Jim Crist-Harif][3])
* Matching is intentionally optimized: identity match (`is`), type equality, and empty list/set/dict are treated as matching defaults; it is **not** a deep equality check. ([Jim Crist-Harif][3])

**Failure mode**

* If you rely on “structural equality” of complex defaults, you may still see default values serialized because they don’t satisfy the optimized predicate. ([Jim Crist-Harif][3])

### A9) Unknown fields: permissive by default, strict when asked

* Default behavior: unknown object fields are ignored to support schema evolution. ([Jim Crist-Harif][3])
* `forbid_unknown_fields=True` turns that into a hard error (useful for configs where typos must fail). ([Jim Crist-Harif][3])

### A10) Array-like encoding (`array_like=True`): when you should and shouldn’t use it

**Why**

* Removes field names -> significantly faster decode/encode in many workloads. ([Jim Crist-Harif][3])

**Hard constraints**

* Schema evolution becomes “append-only”; you must not reorder fields; new fields must be appended and defaulted. ([Jim Crist-Harif][5])

**Semantic constraint**

* Positional encoding is hostile to partial messages and human debugging; reserve for stable internal protocols. ([Jim Crist-Harif][3])

### A11) `defstruct(...)` and `StructMeta` for policy-driven projects

* `defstruct` creates Struct types at runtime and supports the full config surface. ([Jim Crist-Harif][2])
* `StructMeta` can be subclassed to enforce project-wide defaults (e.g., default `kw_only=True`); only `ABCMeta` combinations are considered supported. ([Jim Crist-Harif][3])

---

## B) Constraints + schema metadata (`Annotated[..., Meta(...)]`) — deep dive

### B0) Mental model

`Meta` does two distinct jobs:

1. **Runtime validation constraints** during typed decoding/convert/inspect.
2. **JSON Schema/OpenAPI annotations** (`title`, `description`, `examples`, `extra_json_schema`, `extra`). ([Jim Crist-Harif][2])

### B1) Attaching constraints and reusing them

Use `typing.Annotated[T, Meta(...)]` as a reusable type alias:

```python
from typing import Annotated
from msgspec import Meta

PositiveFloat = Annotated[float, Meta(gt=0)]
```

([Jim Crist-Harif][6])

### B2) Constraint surface area (enforced during typed decode/convert)

* Numeric: `gt/ge/lt/le/multiple_of`
* String: `min_length/max_length/pattern` (pattern is treated as unanchored)
* Collections/Mappings: `min_length/max_length`
* Datetime/time: `tz` requirement (aware vs naive) ([Jim Crist-Harif][2])

### B3) Float `multiple_of` precision footgun

* Non-integral `multiple_of` constraints on floats (e.g. `0.1`) can fail due to floating-point precision issues; the docs explicitly caution against it. ([Jim Crist-Harif][7])

### B4) JSON Schema-specific metadata and merge semantics

* `extra_json_schema` is **recursively merged** into the generated schema and overrides conflicting autogenerated fields. ([Jim Crist-Harif][2])
* `extra` is arbitrary user-defined metadata (not necessarily schema). ([Jim Crist-Harif][2])
* In `msgspec.inspect`, schema-specific metadata gets merged into a single `extra_json_schema` dict and wrapped in `inspect.Metadata`. ([Jim Crist-Harif][8])

**Practical guidance**

* Use `description` for field-level schema docs (OpenAPI clients). Use `extra_json_schema` for dialect-specific knobs (e.g., `"format"`, `"deprecated"`, vendor extensions). ([Jim Crist-Harif][2])

---

## C) Supported type system + decoding semantics — deep dive

### C0) Mental model: msgspec is *typed* at the boundary; protocol mappings matter

* With no type (or `Any`), you get protocol-default python types (dict/list/str/int/float/None…).
* With a type, msgspec does fast validation and structured construction (Struct/dataclass/etc). ([Jim Crist-Harif][4])

### C1) “Strict vs Lax” (`strict=True|False`) is *coercion policy*

* `Decoder(..., strict=False)` enables broader coercions **from strings to non-string types** (and some numeric coercions), applied across values. ([Jim Crist-Harif][2])
* Examples include `"null"`→`None`, `"false"`/`"0"`→`False`, numeric strings→ints/floats, float strings `"nan"/"inf"`→nonfinite float, numeric epoch seconds→datetime when lax. ([Jim Crist-Harif][4])

**Operational warning**

* Lax mode is great for “loosely typed external inputs,” but it can hide upstream data quality problems. Prefer strict + explicit coercion in ingestion pipelines unless you *need* lax. ([Jim Crist-Harif][2])

### C2) Protocol constraints and “surprising” mappings

* **TOML has no null**: encoding `None` to TOML errors. ([Jim Crist-Harif][4])
* **JSON nonfinite floats** aren’t allowed by RFC8259; msgspec encodes NaN/±Inf as `null` for JSON. ([Jim Crist-Harif][4])
* **MessagePack int range**: msgpack supports integers only within `[-2**63, 2**64-1]`. ([Jim Crist-Harif][4])
* **bytes-like in JSON/YAML/TOML** are base64-encoded strings; msgpack uses `bin`. ([Jim Crist-Harif][4])

### C3) Zero-copy and retention footguns

* In msgpack decoding, `memoryview` results can be views into the original input buffer; holding them keeps the entire buffer alive. ([Jim Crist-Harif][4])

### C4) Union decoding restrictions (why some unions are rejected)

To guarantee unambiguous decoding, unions are constrained, e.g.:

* At most one type that encodes to an **object/map** (dict/TypedDict/dataclass/attrs/Struct with `array_like=False`)
* At most one type that encodes to an **array** (list/tuple/set/frozenset/NamedTuple/Struct with `array_like=True`)
* At most one **untagged** Struct; multiple Structs require tagged unions
* Custom types are unsupported beyond optionality (e.g. `Optional[CustomType]`) ([Jim Crist-Harif][4])

### C5) Dataclasses / attrs / TypedDict / NamedTuple integration

* TypedDict and dataclasses map to objects/maps; extra fields are ignored (TypedDict extra keys ignored; dataclasses extra ignored), missing required fields error; dataclass `__post_init__` runs after decode; `InitVar` not supported. ([Jim Crist-Harif][9])
* If you want strict unknown-field rejection, Struct supports `forbid_unknown_fields=True` (dataclasses currently don’t). ([Jim Crist-Harif][3])

---

## D) JSON module (`msgspec.json`) — deep dive

### D0) Core surfaces

* Reusable `Encoder`/`Decoder` objects for hot paths (avoid per-call setup). ([Jim Crist-Harif][10])
* Top-level `encode/decode` convenience wrappers. ([Jim Crist-Harif][2])

### D1) Encoder power knobs

* `decimal_format={'string'|'number'}`: string avoids precision loss; number encodes Decimal as float. ([Jim Crist-Harif][2])
* `uuid_format={'canonical'|'hex'}`. ([Jim Crist-Harif][2])
* `order=None|'deterministic'|'sorted'` controls ordering of unordered containers (and optionally object-like types).

  * `deterministic`: sort dict/set to stabilize output
  * `sorted`: also sort object-like fields by name (more readable, slower) ([Jim Crist-Harif][2])

**When to use `order`**

* Use `deterministic` when output bytes are hashed/compared (golden tests, content-addressed caching).
* Use `sorted` when humans diff outputs (debug, config snapshots). ([Jim Crist-Harif][2])

### D2) NDJSON / JSON-lines support

* `Encoder.encode_lines(iterable)` emits newline-delimited JSON with a trailing newline; `Decoder.decode_lines` parses. ([Jim Crist-Harif][2])

### D3) Allocation control: `encode_into`

* `encode_into(obj, bytearray, offset=0|-1)` writes into an existing buffer; can reduce allocations and copies; supports appends (offset `-1`) and framing tricks. ([Jim Crist-Harif][2])

**Failure mode**

* Buffers only grow; encountering a huge message can bloat capacity long-term (measure with `sys.getsizeof`). ([Jim Crist-Harif][10])

### D4) Formatting existing JSON: `json.format`

* `indent>0`: pretty-print
* `indent=0`: single line *with spaces* for readability
* `indent<0`: minify (strip unnecessary whitespace) ([Jim Crist-Harif][2])

### D5) Decoder knobs: `strict`, `dec_hook`, `float_hook`

* `strict=False` enables lax coercions (see C1). ([Jim Crist-Harif][2])
* `dec_hook(type, obj)` enables typed custom decoding hooks (see J). ([Jim Crist-Harif][2])

---

## E) MessagePack module (`msgspec.msgpack`) — deep dive

### E0) Why MessagePack in msgspec

* msgspec supports JSON and MessagePack with parallel APIs; MessagePack can be more performant for some workloads, and msgspec explicitly calls this out. ([Jim Crist-Harif][10])

### E1) Encoder knobs

* `uuid_format` supports `'bytes'` in MessagePack (big-endian 128-bit) in addition to string forms. ([Jim Crist-Harif][2])
* Same `decimal_format` and `order` options as JSON. ([Jim Crist-Harif][2])

### E2) Extension types (`Ext`) and `ext_hook`

* Extensions are exposed as `msgspec.msgpack.Ext(code, data)` by default. ([Jim Crist-Harif][11])
* `ext_hook(code: int, data: memoryview)` can decode them into custom objects; **`data` is a view into the larger message buffer**—copy if you need to retain. ([Jim Crist-Harif][2])

### E3) Allocation control

* `encode_into` exists for MessagePack too (same buffer-growth caveat). ([Jim Crist-Harif][2])

---

## F) YAML / TOML — deep dive

### F0) What these modules are (and aren’t)

* `msgspec.yaml.encode/decode` requires PyYAML installed (YAML integration is thin, msgspec still handles typed conversion/validation). ([Jim Crist-Harif][2])
* TOML integration is similar: msgspec provides typed encode/decode frontends. ([Jim Crist-Harif][2])

### F1) Key protocol limitations you need in contracts

* TOML: no `null`, so `None` cannot be encoded. ([Jim Crist-Harif][4])
* bytes-like objects: become base64 strings in JSON/YAML/TOML. ([Jim Crist-Harif][4])

---

## G) Converters (`convert`, `to_builtins`) — deep dive

### G0) Mental model: converters let you “wrap” any protocol

The converter pair is designed for “serialize/deserialize with *other* libs”:

* `to_builtins(obj)`: normalize complex objects → builtin graph
* `convert(obj, type)`: validate/coerce builtin graph → typed schema ([Jim Crist-Harif][12])

### G1) `convert(...)` power knobs

* `strict=False` is a *big* switch: it implies `str_keys=True` and `builtin_types=None` (broader coercion assumptions). ([Jim Crist-Harif][2])
* `from_attributes=True`: treat objects as attribute containers (ORM-ish) when converting into object-like targets. ([Jim Crist-Harif][2])
* `str_keys=True`: enable coercion rules for dict keys when wrapped protocols only support string keys. ([Jim Crist-Harif][2])
* `builtin_types=...`: declare extra “already-native” types for the wrapped protocol (turn off msgspec’s conversions for those). ([Jim Crist-Harif][2])

### G2) `to_builtins(...)` power knobs

* `builtin_types` passthrough supports bytes/bytearray/memoryview, datetime/time/date/timedelta, UUID, Decimal, and custom types (pass through unchanged). ([Jim Crist-Harif][2])
* `str_keys` converts keys to strings (for protocols that require it). ([Jim Crist-Harif][2])
* `order` mirrors encoder ordering: `deterministic` / `sorted`. ([Jim Crist-Harif][2])

**Minimal “wrap another protocol” sketch**

```python
import msgspec
import some_toml_lib

def loads_toml_typed(buf: str, tp):
    raw = some_toml_lib.loads(buf)       # produces builtins
    return msgspec.convert(raw, tp)      # validate/coerce -> typed

def dumps_toml(obj) -> str:
    raw = msgspec.to_builtins(obj, str_keys=True)
    return some_toml_lib.dumps(raw)
```

([Jim Crist-Harif][12])

---

## H) JSON Schema generation — deep dive

### H0) Mental model

* `msgspec.json.schema(type)` emits a JSON Schema with shared components extracted to `"$defs"`. ([Jim Crist-Harif][2])
* `schema_components(types, ref_template=...)` emits multiple schemas plus a separate `components` map; `ref_template` is how you align with OpenAPI component locations. ([Jim Crist-Harif][2])
* Output is stated to be compatible with JSON Schema 2020-12 and OpenAPI 3.1. ([Jim Crist-Harif][6])

### H1) Where descriptions come from

* Struct docstrings become schema `description` in the example output. ([Jim Crist-Harif][6])
* Field-level descriptions and other schema metadata flow from `Meta(...)` (and are merged into `extra_json_schema`). ([Jim Crist-Harif][2])

### H2) Extending schema generation for custom types

* `schema_hook(type)->dict` lets you define schemas for custom types. ([Jim Crist-Harif][2])

**Failure mode**

* If you rely on vendor-specific OpenAPI dialect features, use `extra_json_schema` to inject them; remember merges are recursive and override autogenerated keys. ([Jim Crist-Harif][2])

---

## I) `msgspec.inspect` (experimental type IR) — deep dive

### I0) What it’s for

* Experimental module intended for downstream tooling: OpenAPI generation, example instance synthesis, Hypothesis integration, etc. ([Jim Crist-Harif][8])

### I1) Core APIs

* `type_info(annotation)` returns a structured `Type` object; `multi_type_info([...])` is the efficient batch form. ([Jim Crist-Harif][8])

### I2) Metadata normalization

* Types with JSON-schema metadata are wrapped in `inspect.Metadata`, and schema-related fields are merged into `extra_json_schema`. ([Jim Crist-Harif][8])

### I3) Using inspect to build “schema-driven” tooling

Common patterns:

* “Walk the type graph” (`StructType.fields`, `UnionType.types`, etc.) to generate:

  * codegen / adapters
  * synthetic data for tests
  * domain-specific validators beyond what msgspec enforces
  * dataset contracts (schema checksum + constraints) ([Jim Crist-Harif][8])

---

## J) Extensibility hooks (custom types) — deep dive

### J0) Three hooks, three roles

* `enc_hook(obj)` transforms unsupported objects into msgspec-supported representations.
* `dec_hook(type, obj)` transforms decoded native representations into the requested custom type during typed decoding; **TypeError/ValueError are converted into ValidationError with context**. ([Jim Crist-Harif][11])
* `ext_hook(code, data)` is MessagePack-only and maps extension payloads to objects. ([Jim Crist-Harif][11])

### J1) Choosing a pattern

**Pattern 1: “map to native types”**

* Best when your custom type is structurally similar to supported types (e.g., `complex ↔ (real, imag)`), but requires typed decoding to roundtrip. ([Jim Crist-Harif][11])

**Pattern 2: “MessagePack extension type”**

* Best when you need custom type info in the wire format; can support untyped decoding if the extension codes are shared across implementations. ([Jim Crist-Harif][11])

### J2) Memory retention warning (MessagePack `ext_hook`)

* `data` is a `memoryview` into the full message buffer; retaining it retains the whole buffer unless you copy. ([Jim Crist-Harif][11])

---

## K) Schema evolution & compatibility discipline — deep dive

### K0) What msgspec means by “schema evolution”

* Old messages decode with new schema; new messages decode with old schema *if you follow rules*. ([Jim Crist-Harif][5])

### K1) The four rules (enforced by you, not magic)

1. New Struct fields must have defaults.
2. With `array_like=True`, never reorder; only append new defaulted fields.
3. Don’t change type annotations for existing fields/messages.
4. Don’t change MessagePack extension codes or implementations. ([Jim Crist-Harif][5])

### K2) Interactions that can surprise you

* If you enable `forbid_unknown_fields=True`, you are *explicitly* opting out of “ignore unknown keys” behavior that makes forwards-compat easy. ([Jim Crist-Harif][3])
* Tagged unions: tag values become part of your wire contract—treat them like stable IDs. ([Jim Crist-Harif][2])

---

## L) Performance, correctness, drift resistance — deep dive

### L0) Reuse encoders/decoders (baseline)

* Top-level `encode` allocates temporary state; reuse `Encoder`/`Decoder` objects for hot loops. ([Jim Crist-Harif][10])

### L1) Encode less, decode less

* Use `omit_defaults=True` when defaults are known on both ends. ([Jim Crist-Harif][10])
* Define “view structs” with only needed fields to avoid decoding unused data. ([Jim Crist-Harif][10])

### L2) Reduce allocations and copies with `encode_into`

* Reuse a shared `bytearray` buffer; use offsets for framing (length-prefix, NDJSON newline append). ([Jim Crist-Harif][10])

### L3) Use `order='deterministic'` for golden tests and stable hashing

* Deterministic ordering sorts unordered containers to stabilize output bytes; useful for snapshot testing and cache keys. ([Jim Crist-Harif][2])

### L4) `gc=False` only when you can prove “no cycles”

* `gc=False` can reduce GC pauses, but cycles composed only of `gc=False` structs are never collected (leak risk). ([Jim Crist-Harif][3])

### L5) “Known sharp edge” to be aware of: `UNSET` + `array_like=True`

* `UNSET` is documented as a field sentinel that omits fields during encoding for object-like schemas. ([Jim Crist-Harif][9])
* There is an open issue reporting that encoding `UNSET` inside `array_like=True` structs raises `TypeError` instead of omitting. ([GitHub][13])

  * Practically: if you need positional encoding, prefer explicit optional trailing fields (append-only) over `UNSET` semantics, until/if this behavior changes. ([Jim Crist-Harif][5])

### L6) Error taxonomy and handling contracts

* Base: `MsgspecError`; encoding errors: `EncodeError`; decoding errors: `DecodeError`; schema mismatch: `ValidationError` (subclass of `DecodeError`). ([Jim Crist-Harif][2])
* `ValidationError` commonly includes an “at `$.path`” suffix (JSONPath-like) for fast root-cause. ([Jim Crist-Harif][3])

---

If you want the *next* increment after this: I can convert this into a **repo-ready “msgspec contract harness”** (pytest fixtures + golden JSON/MsgPack snapshots + deterministic ordering policy + ValidationError normalization), in the same style you’ve been building for your other protocol catalogs.

[1]: https://pypi.org/project/msgspec/?utm_source=chatgpt.com "msgspec"
[2]: https://jcristharif.com/msgspec/api.html "API Docs"
[3]: https://jcristharif.com/msgspec/structs.html "Structs"
[4]: https://jcristharif.com/msgspec/supported-types.html "Supported Types"
[5]: https://jcristharif.com/msgspec/schema-evolution.html "Schema Evolution"
[6]: https://jcristharif.com/msgspec/jsonschema.html "JSON Schema"
[7]: https://jcristharif.com/msgspec/constraints.html?utm_source=chatgpt.com "Constraints - msgspec"
[8]: https://jcristharif.com/msgspec/inspect.html "Inspecting Types"
[9]: https://jcristharif.com/msgspec/supported-types.html?utm_source=chatgpt.com "Supported Types"
[10]: https://jcristharif.com/msgspec/perf-tips.html "Performance Tips"
[11]: https://jcristharif.com/msgspec/extending.html "Extending"
[12]: https://jcristharif.com/msgspec/converters.html?utm_source=chatgpt.com "Converters"
[13]: https://github.com/jcrist/msgspec/issues/723 "JSON and msgpack encoding fails to skip UNSET values in array_like Struct · Issue #723 · jcrist/msgspec · GitHub"

Below is a **drop-in, repo-ready msgspec contract harness**: pytest fixtures + golden JSON/MsgPack snapshots, a **deterministic ordering policy**, and **ValidationError normalization** (snapshot-friendly). It’s designed so you can **add new “contract cases” by appending to a list**, and you can **regenerate goldens** with a single flag.

Deterministic ordering is enforced by using `order="deterministic"` on the protocol Encoders. ([Jim Crist-Harif][1])

---

## 1) File layout

```
tests/
  msgspec_contract/
    __init__.py
    conftest.py
    _support/
      __init__.py
      codecs.py
      goldens.py
      models.py
      normalize.py
    goldens/
      .gitkeep
    test_contract_json.py
    test_contract_msgpack.py
    test_contract_schema.py
    test_contract_errors.py
```

> Notes
>
> * Goldens are **text-first**: JSON snapshots are **pretty-formatted** via `msgspec.json.format(..., indent=2)` (stable whitespace you control). ([Jim Crist-Harif][1])
> * MsgPack “wire bytes” are snapshotted as **base64** (diffable-enough + avoids binary blobs).
> * Error snapshots store **normalized dicts**, parsed from `str(ValidationError)` (msgspec commonly includes `- at `$.path``). ([Jim Crist-Harif][2])

---

## 2) `tests/msgspec_contract/conftest.py`

```python
from __future__ import annotations

import os
import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-goldens",
        action="store_true",
        default=False,
        help="Regenerate msgspec contract snapshots.",
    )


@pytest.fixture(scope="session")
def update_goldens(pytestconfig: pytest.Config) -> bool:
    # Also allow env-based updating for CI workflows
    return bool(pytestconfig.getoption("--update-goldens") or os.getenv("UPDATE_GOLDENS") == "1")
```

---

## 3) `tests/msgspec_contract/_support/goldens.py`

```python
from __future__ import annotations

import base64
from pathlib import Path


GOLDENS_DIR = Path(__file__).resolve().parents[1] / "goldens"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def assert_text_snapshot(*, path: Path, text: str, update: bool) -> None:
    """Compare `text` to a golden file. If update=True, write the file."""
    if not text.endswith("\n"):
        text += "\n"

    if not path.exists():
        if update:
            _ensure_parent(path)
            path.write_text(text, encoding="utf-8")
            return
        raise AssertionError(
            f"Golden missing: {path}\n"
            f"Run: pytest -q --update-goldens tests/msgspec_contract\n"
            f"or set UPDATE_GOLDENS=1"
        )

    if update:
        _ensure_parent(path)
        path.write_text(text, encoding="utf-8")
        return

    expected = path.read_text(encoding="utf-8")
    assert text == expected


def assert_bytes_snapshot_b64(*, path: Path, data: bytes, update: bool) -> None:
    """Snapshot binary bytes as base64 text (with trailing newline)."""
    b64 = base64.b64encode(data).decode("ascii")
    assert_text_snapshot(path=path, text=b64, update=update)


def read_bytes_snapshot_b64(path: Path) -> bytes:
    """Utility for consumers that want to rehydrate wire bytes."""
    raw = path.read_text(encoding="utf-8").strip()
    return base64.b64decode(raw)
```

---

## 4) `tests/msgspec_contract/_support/normalize.py`

```python
from __future__ import annotations

import re
from typing import Any, Mapping


_VALIDATION_RE = re.compile(
    # Typical msgspec errors look like: "Expected `int`, got `str` - at `$.x`"
    r"^(?P<summary>.*?)(?:\s+-\s+at\s+`(?P<path>[^`]+)`)?$"
)


def normalize_exception(exc: Exception) -> dict[str, Any]:
    """
    Convert msgspec errors into a stable, snapshot-friendly dict.

    We intentionally normalize from `str(exc)` rather than relying on private attrs.
    """
    s = str(exc).strip()
    m = _VALIDATION_RE.match(s)

    out: dict[str, Any] = {"type": exc.__class__.__name__}
    if m:
        out["summary"] = (m.group("summary") or "").strip()
        path = m.group("path")
        if path:
            out["path"] = path
    else:
        out["summary"] = s
    return out


def normalize_jsonable(obj: Any) -> Any:
    """
    Make sure objects can be snapshotted as JSON (paranoid fallback).
    If you keep snapshots as JSON text, this helps avoid accidental non-serializables.
    """
    if isinstance(obj, Mapping):
        return {str(k): normalize_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [normalize_jsonable(x) for x in obj]
    return obj
```

---

## 5) `tests/msgspec_contract/_support/codecs.py`

```python
from __future__ import annotations

from typing import Any

import msgspec


# Contract policy: stable ordering for unordered compound types.
# Encoder supports order={None, 'deterministic', 'sorted'}; we standardize on 'deterministic'.
# See msgspec API docs for json/msgpack Encoder + ordering semantics.  :contentReference[oaicite:3]{index=3}
JSON_ENCODER = msgspec.json.Encoder(
    order="deterministic",
    decimal_format="string",
    uuid_format="canonical",
)

MSGPACK_ENCODER = msgspec.msgpack.Encoder(
    order="deterministic",
    decimal_format="string",
    uuid_format="canonical",
)


def encode_json(obj: Any) -> bytes:
    return JSON_ENCODER.encode(obj)


def encode_json_pretty(obj: Any, *, indent: int = 2) -> str:
    raw = encode_json(obj)
    formatted = msgspec.json.format(raw, indent=indent)  # bytes in -> bytes out  :contentReference[oaicite:4]{index=4}
    return formatted.decode("utf-8")


def decode_json(buf: bytes | str, *, type: Any, strict: bool = True) -> Any:
    # Using top-level decode keeps the harness simple and explicit.  :contentReference[oaicite:5]{index=5}
    return msgspec.json.decode(buf, type=type, strict=strict)


def encode_msgpack(obj: Any) -> bytes:
    return MSGPACK_ENCODER.encode(obj)


def decode_msgpack(buf: bytes, *, type: Any, strict: bool = True) -> Any:
    return msgspec.msgpack.decode(buf, type=type, strict=strict)


def msgpack_semantics_for_snapshot(obj: Any) -> Any:
    """
    Convert a typed object into builtins for human-diffable semantic snapshots.
    `to_builtins` supports order='deterministic' as well.  :contentReference[oaicite:6]{index=6}
    """
    return msgspec.to_builtins(obj, order="deterministic", str_keys=True)
```

---

## 6) `tests/msgspec_contract/_support/models.py`

A compact “contract model set” that exercises: Structs, tagged unions, `Meta` constraints, `UNSET`, Decimal/UUID/datetime, omit-default behavior.

```python
from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import Annotated, Union

import msgspec


Email = Annotated[
    str,
    msgspec.Meta(
        # Keep constraints simple and stable for snapshot tests
        min_length=3,
        max_length=254,
        pattern=r"^[^@]+@[^@]+\.[^@]+$",
        description="RFC-lite email (contract-level)",
    ),
]


class User(msgspec.Struct, kw_only=True, omit_defaults=True):
    id: uuid.UUID
    email: Email
    created_at: Annotated[dt.datetime, msgspec.Meta(tz=True)]
    flags: set[str] = msgspec.field(default_factory=set)
    note: Union[str, msgspec.UnsetType] = msgspec.UNSET


class Click(msgspec.Struct, kw_only=True, tag="click"):
    url: str
    ts: Annotated[dt.datetime, msgspec.Meta(tz=True)]


class Purchase(msgspec.Struct, kw_only=True, tag="purchase"):
    sku: str
    amount: Decimal


Event = Union[Click, Purchase]


class Envelope(msgspec.Struct, kw_only=True, omit_defaults=True):
    user: User
    event: Event
    trace_id: Union[str, msgspec.UnsetType] = msgspec.UNSET
    metadata: dict[str, str] = msgspec.field(default_factory=dict)


class StrictUser(msgspec.Struct, kw_only=True, forbid_unknown_fields=True):
    id: uuid.UUID
    email: str


CANONICAL_ENVELOPE = Envelope(
    user=User(
        id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        email="alice@example.com",
        created_at=dt.datetime(2020, 1, 2, 3, 4, 5, 123456, tzinfo=dt.timezone.utc),
        flags={"internal", "beta"},
        note=msgspec.UNSET,
    ),
    event=Purchase(sku="SKU-123", amount=Decimal("12.34")),
    trace_id=msgspec.UNSET,
    metadata={"b": "2", "a": "1"},
)
```

---

## 7) JSON contract tests: `tests/msgspec_contract/test_contract_json.py`

```python
from __future__ import annotations

from pathlib import Path

from ._support import codecs, goldens, models


def test_json_contract_envelope_roundtrip(update_goldens: bool) -> None:
    # Encode -> snapshot (pretty) -> decode typed -> semantic equality
    pretty = codecs.encode_json_pretty(models.CANONICAL_ENVELOPE, indent=2)

    goldens.assert_text_snapshot(
        path=goldens.GOLDENS_DIR / "envelope.json",
        text=pretty,
        update=update_goldens,
    )

    raw = codecs.encode_json(models.CANONICAL_ENVELOPE)
    decoded = codecs.decode_json(raw, type=models.Envelope, strict=True)
    assert decoded == models.CANONICAL_ENVELOPE


def test_json_contract_encode_into_equivalence() -> None:
    # encode_into is an important perf primitive; we sanity-check equivalence.
    import msgspec

    enc = msgspec.json.Encoder(order="deterministic")
    buf = bytearray()
    enc.encode_into({"b": 2, "a": 1}, buf)
    assert bytes(buf) == enc.encode({"b": 2, "a": 1})
```

---

## 8) MsgPack contract tests: `tests/msgspec_contract/test_contract_msgpack.py`

```python
from __future__ import annotations

from ._support import codecs, goldens, models


def test_msgpack_contract_envelope_wire_bytes(update_goldens: bool) -> None:
    wire = codecs.encode_msgpack(models.CANONICAL_ENVELOPE)

    goldens.assert_bytes_snapshot_b64(
        path=goldens.GOLDENS_DIR / "envelope.msgpack.b64",
        data=wire,
        update=update_goldens,
    )

    decoded = codecs.decode_msgpack(wire, type=models.Envelope, strict=True)
    assert decoded == models.CANONICAL_ENVELOPE


def test_msgpack_contract_semantics_snapshot(update_goldens: bool) -> None:
    # Human-diffable semantic snapshot, complementary to wire bytes.
    wire = codecs.encode_msgpack(models.CANONICAL_ENVELOPE)
    decoded = codecs.decode_msgpack(wire, type=models.Envelope, strict=True)

    semantic = codecs.msgpack_semantics_for_snapshot(decoded)
    pretty = codecs.encode_json_pretty(semantic, indent=2)

    goldens.assert_text_snapshot(
        path=goldens.GOLDENS_DIR / "envelope.msgpack.semantics.json",
        text=pretty,
        update=update_goldens,
    )
```

---

## 9) Schema contract tests: `tests/msgspec_contract/test_contract_schema.py`

This snapshots the JSON Schema emitted for your contract types (useful to catch drift when bumping msgspec). msgspec exposes `msgspec.json.schema` and related helpers. ([Jim Crist-Harif][3])

```python
from __future__ import annotations

import msgspec

from ._support import codecs, goldens, models


def test_json_schema_contract(update_goldens: bool) -> None:
    schema = msgspec.json.schema(models.Envelope)
    pretty = codecs.encode_json_pretty(schema, indent=2)

    goldens.assert_text_snapshot(
        path=goldens.GOLDENS_DIR / "envelope.schema.json",
        text=pretty,
        update=update_goldens,
    )
```

---

## 10) Error contract tests: `tests/msgspec_contract/test_contract_errors.py`

These ensure your system gets **stable, snapshotted error shapes** for invalid payloads. msgspec’s ValidationError string format commonly includes a JSONPath-ish location like ``- at `$.x```; we normalize that into `{type, summary, path}`. ([Jim Crist-Harif][2])

```python
from __future__ import annotations

import msgspec

from ._support import codecs, goldens, models
from ._support.normalize import normalize_exception, normalize_jsonable


def test_validation_error_snapshot_bad_uuid(update_goldens: bool) -> None:
    # Bad UUID at $.user.id
    bad = {
        "user": {
            "id": "not-a-uuid",
            "email": "alice@example.com",
            "created_at": "2020-01-02T03:04:05.123456Z",
            "flags": ["beta"],
        },
        "event": {"type": "purchase", "sku": "SKU-123", "amount": "12.34"},
        "metadata": {"a": "1"},
    }

    try:
        _ = codecs.decode_json(codecs.encode_json(bad), type=models.Envelope, strict=True)
        raise AssertionError("Expected msgspec.ValidationError")
    except msgspec.ValidationError as e:
        normalized = normalize_jsonable(normalize_exception(e))
        pretty = codecs.encode_json_pretty(normalized, indent=2)

        goldens.assert_text_snapshot(
            path=goldens.GOLDENS_DIR / "error.bad_uuid.json",
            text=pretty,
            update=update_goldens,
        )


def test_validation_error_snapshot_unknown_field(update_goldens: bool) -> None:
    # StrictUser forbids unknown fields.
    bad = {
        "id": "00000000-0000-0000-0000-000000000001",
        "email": "alice@example.com",
        "typo_field": 1,
    }

    try:
        _ = codecs.decode_json(codecs.encode_json(bad), type=models.StrictUser, strict=True)
        raise AssertionError("Expected msgspec.ValidationError")
    except msgspec.ValidationError as e:
        normalized = normalize_jsonable(normalize_exception(e))
        pretty = codecs.encode_json_pretty(normalized, indent=2)

        goldens.assert_text_snapshot(
            path=goldens.GOLDENS_DIR / "error.unknown_field.json",
            text=pretty,
            update=update_goldens,
        )


def test_unset_omission_contract() -> None:
    # UNSET fields should be omitted on encode (when value is UNSET), and preserved on decode.
    obj = models.CANONICAL_ENVELOPE
    raw = codecs.encode_json(obj)
    assert b'"trace_id"' not in raw
    assert b'"note"' not in raw

    decoded = codecs.decode_json(raw, type=models.Envelope, strict=True)
    assert decoded.user.note is msgspec.UNSET
    assert decoded.trace_id is msgspec.UNSET
```

---

## 11) `tests/msgspec_contract/goldens/.gitkeep`

Empty file just to keep the directory checked in.

---

## 12) How you use it

* First run (generates snapshots):
  `pytest -q --update-goldens tests/msgspec_contract`
* Normal run (enforces contract):
  `pytest -q tests/msgspec_contract`

This gives you a **tight “wire contract” tripwire** for JSON + MessagePack (plus schema and error-shape) so bumping msgspec becomes a deliberate, reviewable change (diffs show exactly what changed).

[1]: https://jcristharif.com/msgspec/api.html "API Docs"
[2]: https://jcristharif.com/msgspec/converters.html "Converters"
[3]: https://jcristharif.com/msgspec/jsonschema.html?utm_source=chatgpt.com "JSON Schema - msgspec"
