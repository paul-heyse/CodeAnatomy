Below is a **feature-catalog / surface-area map** for **Protocol Buffers in Python** (the `protobuf` PyPI package + the Python code generator invoked by `protoc`). It’s organized as **semantically coherent “planes”** (toolchain → generated-code ABI → runtime APIs → reflection/dynamic → formats → WKT → services/plugins → performance backends), in the same spirit as your PyArrow reference doc. ([PyPI][1])

---

## Table of contents

A) Distribution + toolchain boundary
B) Code generation products + packaging rules
C) Generated message/field surface (what you get in `_pb2.py`)
D) Core runtime `Message` API (mutation, presence, parse/serialize, unknowns)
E) Alternate formats: JSON mapping + textproto
F) Descriptor/reflection plane (schema as data)
G) Dynamic types at runtime (DescriptorPool/MessageFactory/proto_builder)
H) Well-known types (WKT) + their “extra” methods
I) Services/RPC hooks + generic-services (deprecated)
J) Protoc plugin insertion points + schema/plugin protos
K) Runtime backends + performance knobs (upb/cpp/python)
L) “Gotchas” that belong in your later deep dives (but worth tagging now)

---

## A) Distribution + toolchain boundary (what “protobuf in Python” actually includes)

### A1) Two installs, two purposes

* **Python runtime library**: `pip install protobuf` (current PyPI releases are in the **6.x** line). ([PyPI][1])
* **Compiler (`protoc`)**: installed separately; it’s what turns `.proto` → generated Python modules. ([Protocol Buffers][2])

### A2) Runtime = stable API, multiple implementations

The Python runtime is a **switching layer** that can choose between **three implementations**:

* `upb` (default; shipped in PyPI wheels)
* `cpp` (deprecated; not shipped in PyPI wheels; legacy zero-copy sharing use-cases)
* `python` (pure Python fallback)
  Selected via `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION={upb|cpp|python}`; you can also introspect the active backend via `google.protobuf.internal.api_implementation.Type()` (explicitly described as not a stable API). ([Chromium Git Repositories][3])

---

## B) Code generation products + packaging rules (compiler plane)

### B1) Primary outputs

* `protoc --python_out=OUT_DIR …`: produces `*_pb2.py` per `.proto`. Naming/path mapping rules are explicitly defined (proto-path prefix replaced by output path; `.proto`→`_pb2.py`; invalid module-name characters become `_`). ([Protocol Buffers][4])
* Optional typing stub output: `protoc --pyi_out=OUT_DIR …` produces `*_pb2.pyi` stubs (explicitly called out in the Python repo docs and the generated-code guide). ([Chromium Git Repositories][3])

### B2) “Python package name” is directory structure, not `.proto package`

Python generated code is **unaffected** by the `.proto package` clause; the Python import path is determined by the filesystem layout of generated files. ([Protocol Buffers][4])

### B3) Output-to-zip is a first-class workflow

`protoc` can write Python outputs directly to a `.zip`, which Python can import if it’s on `PYTHONPATH`. ([Protocol Buffers][4])

---

## C) Generated message/field surface (what `_pb2.py` “contains” conceptually)

### C1) Message classes and the “metaclass does the work”

* Each message becomes a concrete class `Foo` that subclasses `google.protobuf.Message`.
* Python generation is explicitly described as: compiler outputs descriptor-building code, and a **metaclass** injects behavior. ([Protocol Buffers][4])
* Generated classes expose `Foo.FromString(s)` convenience. ([Protocol Buffers][4])

### C2) Field categories (important for indexing + contracts)

The generated-code guide explicitly distinguishes:

* **Singular scalar fields with explicit presence** vs **implicit presence** (proto2 vs proto3/editions semantics)
* **Repeated fields** (scalar/enum vs message)
* **Map fields**
* **Oneof** groups
  …and documents keyword-conflict handling (access via `getattr`) and nested types (`Foo.Bar`). ([Protocol Buffers][4])

### C3) Extensions (proto2 / editions)

For messages with extension ranges, generated classes expose:

* `foo.Extensions[ext_id] = value` mapping
* Presence operations are extension-specific (`HasExtension`, `ClearExtension`) rather than `HasField`/`ClearField`. ([Protocol Buffers][4])

---

## D) Core runtime `Message` API (the “operational contract surface”)

This is the feature cluster you lean on for 95% of real use.

### D1) Serialization / parsing

* `SerializeToString(deterministic=bool)` and `SerializePartialToString(deterministic=bool)` (determinism is explicitly documented as “predictable ordering of map keys”). ([Google Cloud][5])
* `ParseFromString(bytes)` (clear + parse) and `MergeFromString(bytes)` (merge semantics). ([Google Cloud][5])
* Size/introspection: `ByteSize()`. ([Google Cloud][5])

### D2) Structural mutation primitives

* `Clear()`, `ClearField(name)`, `CopyFrom(msg)`, `MergeFrom(msg)`. ([Google Cloud][5])
* Presence: `HasField(name)` (and oneof-group presence), `WhichOneof(oneof_name)`. ([Google Cloud][5])
* “Force presence” of an empty submessage: `SetInParent()` (documented as a design smell). ([Google Cloud][5])

### D3) Initialization & required fields (proto2/editions)

* `IsInitialized()` checks required fields. ([Google Cloud][5])

### D4) Unknown fields plane (forward-compat / partial decoding)

* `UnknownFields()` returns an `UnknownFieldSet`. ([Google Cloud][5])
* `DiscardUnknownFields()` recursively clears unknowns. ([Google Cloud][5])

### D5) Errors / exceptions

* `EncodeError`, `DecodeError` are the top-level runtime error classes for serialization/deserialization failures. ([Google Cloud][5])

---

## E) Alternate formats: JSON mapping + textproto (human/debug + interop)

### E1) Proto3 JSON mapping (`google.protobuf.json_format`)

Primary functions (all documented with knobs you should treat as part of the “contract surface”):

* Serialize: `MessageToJson(message, …)`, `MessageToDict(message, …)`
* Parse/merge: `Parse(text, message, ignore_unknown_fields=..., descriptor_pool=..., max_recursion_depth=...)`, `ParseDict(dict, message, ...)`
  Key knobs include `including_default_value_fields`, `preserving_proto_field_name`, `use_integers_for_enums`, `descriptor_pool`, plus recursion depth bounds. ([Google Cloud][6])

### E2) Text format / “textproto” (`google.protobuf.text_format`)

Primary functions:

* Serialize: `MessageToString(message, …)`, `PrintMessage(message, out, …)`, `MessageToBytes(message, …)`
* Parse: `Parse(text, message, …)`
  Important semantic note: `Parse` does **not** clear the message first (“merge-like” behavior), and will error on conflicts for non-repeated fields; you must clear explicitly if you want overwrite semantics. ([Google Cloud][7])

---

## F) Descriptor/reflection plane (schema as data; introspection; tooling)

### F1) Descriptors are first-class objects

* Each message class exposes `DESCRIPTOR` (a `google.protobuf.descriptor.Descriptor`). ([Google Cloud][5])
* The Python stack exposes modules for descriptors and descriptor protos: `google.protobuf.descriptor`, `google.protobuf.descriptor_pb2`, `google.protobuf.descriptor_pool`, etc. (enumerated directly in the API reference index / module table-of-contents). ([Google Cloud][5])

### F2) Descriptor pools as the “registry boundary”

* `DescriptorPool()` is described as the container for descriptors used for **dynamic** message construction (used with a descriptor database, can `Add()` file descriptor protos, then `FindMessageTypeByName(...)`, etc.). ([Protocol Buffers][8])

### F3) Reflection/metaclass layer

* `google.protobuf.reflection` provides the `GeneratedProtocolMessageType` metaclass and helpers used to create message classes from descriptors. ([Google Cloud][9])

### F4) Symbol database = registry for generated types

* `google.protobuf.symbol_database.SymbolDatabase` is explicitly “the MessageFactory for messages generated at compile time,” enabling lookups by fully-qualified symbol name. ([Google Cloud][10])

---

## G) Dynamic types at runtime (for schema-driven systems, plugins, DSLs)

### G1) `message_factory` (dynamic message generation)

* Module-level: `message_factory.GetMessages(iterable_of_FileDescriptorProto)` → `{full_name: message_cls}`
* Class-level: `MessageFactory(pool=...).GetPrototype(descriptor)` (cached) / `CreatePrototype(descriptor)` (always new). ([Google Cloud][11])

### G2) `proto_builder` (simple dynamic proto classes)

* `proto_builder.MakeSimpleProtoClass(fields, full_name=None, pool=None)` creates a message class from a dict/OrderedDict of field specs (explicitly warns it doesn’t validate field names). ([Google Cloud][12])

### G3) Descriptor-proto plumbing as the “universal IR”

If you’re building tooling, the “schema IR” is the descriptor messages in `google.protobuf.descriptor_pb2` (e.g., `FileDescriptorProto`, `FileDescriptorSet`, etc.), which can be stored/transported/compiled into pools (then turned into prototypes via `message_factory`). ([Protocol Buffers][8])

---

## H) Well-known types (WKT) + their Python-only convenience methods

### H1) Inventory (what exists)

The `google.protobuf` WKT package includes (among others): `Any`, `Timestamp`, `Duration`, `FieldMask`, `Struct`, `ListValue`, wrappers (`BoolValue`, `Int32Value`, …), plus schema-introspection types (`Type`, `Field`, `Enum`, …). ([Protocol Buffers][13])

### H2) WKT “extra methods” called out for Python generated code

From the Python generated-code guide:

* `Any`: `Pack(msg)`, `Unpack(msg)`, `Is(descriptor)`, `TypeName()`. ([Protocol Buffers][4])
* `Timestamp`: `ToJsonString()/FromJsonString()`, `GetCurrentTime()`, `To/From{Nanos,Micros,Millis,Seconds}()`, `ToDatetime()/FromDatetime()`. ([Protocol Buffers][4])
* `Duration`: analogous conversions + `ToTimedelta()/FromTimedelta()`. ([Protocol Buffers][4])

*(In later deep dives, you’ll likely want separate “WKT semantics” sections for FieldMask merge semantics, Struct/ListValue JSON interop, wrappers vs optional scalars, etc.)* ([Protocol Buffers][13])

---

## I) Services/RPC hooks + generic-services (deprecated)

### I1) “Generic services” generation is gated and deprecated

* Service stubs/interfaces in Python are only generated if `option py_generic_services = true;` is set; otherwise defaults to false, and generic services are described as deprecated. ([Protocol Buffers][4])

### I2) Protobuf runtime provides *hooks*, not an RPC implementation

The generated-code guide is explicit: Protobuf ships descriptors and the abstract service/channel/controller interfaces, but not an RPC system; RPC ecosystems should use **plugins** to generate appropriate code. ([Protocol Buffers][4])

---

## J) Protoc plugin insertion points + schema/plugin protos

### J1) Python generator insertion points

Plugins targeting Python generation can inject at:

* `imports`
* `module_scope`
  …and are warned not to rely on private generator internals. ([Protocol Buffers][4])

### J2) Plugin protocol (CodeGeneratorRequest/Response)

The plugin protocol itself is defined by the compiler plugin schema (i.e., the “protoc plugin” request/response messages), which is part of the broader protobuf ecosystem; in Python, this commonly surfaces as `google.protobuf.compiler.plugin_pb2` for plugin authors/users (availability historically varied by packaging/distribution). ([Google Groups][14])

---

## K) Runtime backends + performance knobs (what you can *choose*)

### K1) Selecting the backend

* `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=upb|cpp|python` selects backend. ([Chromium Git Repositories][3])

### K2) Backends and why they matter

* `upb` is described as the default, higher-performance backend shipped in PyPI wheels (since 4.21.0).
* `cpp` is deprecated/not shipped in PyPI wheels; exists for legacy “share messages between Python and C++” scenarios (zero-copy).
* `python` is pure-Python fallback. ([Chromium Git Repositories][3])

---

## L) Gotchas worth “tagging” now (you’ll likely expand later)

These are not fully expanded here, but they’re major “deep dive magnets” for your next iteration:

* **Version/toolchain compatibility discipline** (runtime `protobuf` vs `protoc` versioning; ABI behavior; pinned constraints in large ecosystems). ([PyPI][1])
* **Presence semantics & migrations** (proto2 required fields, proto3 optional presence, oneofs, wrappers, FieldMask update semantics). ([Protocol Buffers][4])
* **Deterministic serialization** as a “contract primitive” (esp. map ordering). ([Google Cloud][5])
* **Unknown fields retention vs discard** and how it interacts with forward-compat, text/json printers, and debug flows. ([Google Cloud][5])
* **Dynamic type loading** (DescriptorSet ingestion, pool isolation strategies, Any type resolution via descriptor pools). ([Google Cloud][11])

---

If you want the *next* increment in the same style as your PyArrow document: we can take **each section (D–H)** and expand it into a **function-by-function catalog** (like your “every feature includes concrete Python call syntax” docs), plus a “footguns + invariants” block per section.

Reference style target (your attached template): 

[1]: https://pypi.org/project/protobuf/?utm_source=chatgpt.com "protobuf · PyPI"
[2]: https://protobuf.dev/installation/?utm_source=chatgpt.com "Protocol Buffer Compiler Installation"
[3]: https://chromium.googlesource.com/external/github.com/protocolbuffers/protobuf/%2B/refs/tags/v27.2/python/README.md "Protocol Buffers Python"
[4]: https://protobuf.dev/reference/python/python-generated/ "Python Generated Code Guide | Protocol Buffers Documentation"
[5]: https://googleapis.dev/python/protobuf/latest/google/protobuf/message.html "google.protobuf.message — Protocol Buffers 4.21.1 documentation"
[6]: https://googleapis.dev/python/protobuf/latest/google/protobuf/json_format.html "google.protobuf.json_format — Protocol Buffers 4.21.1 documentation"
[7]: https://googleapis.dev/python/protobuf/latest/google/protobuf/text_format.html "google.protobuf.text_format — Protocol Buffers 4.21.1 documentation"
[8]: https://protobuf.readthedocs.io/en/latest/google/protobuf/descriptor_pool.html?utm_source=chatgpt.com "google.protobuf.descriptor_pool"
[9]: https://googleapis.dev/python/protobuf/latest/google/protobuf/reflection.html?utm_source=chatgpt.com "google.protobuf.reflection — Protocol Buffers 4.21. ..."
[10]: https://googleapis.dev/python/protobuf/latest/google/protobuf/symbol_database.html?utm_source=chatgpt.com "google.protobuf.symbol_database — Protocol Buffers 4.21. ..."
[11]: https://googleapis.dev/python/protobuf/latest/google/protobuf/message_factory.html "google.protobuf.message_factory — Protocol Buffers 4.21.1 documentation"
[12]: https://googleapis.dev/python/protobuf/latest/google/protobuf/proto_builder.html?utm_source=chatgpt.com "google.protobuf.proto_builder — Protocol Buffers 4.21.1 ..."
[13]: https://protobuf.dev/reference/protobuf/google.protobuf/?utm_source=chatgpt.com "Protocol Buffers Well-Known Types"
[14]: https://groups.google.com/g/protobuf/c/5Ywz8gwn9Kk?utm_source=chatgpt.com "Python installation does not build plugin_pb2"


Below are **Sections A–C** expanded to match the **“function/flag-by-flag + call syntax + footguns/invariants”** depth/style you now have for D–L.

---

# A) Distribution + toolchain boundary (what “protobuf in Python” actually is)

### Mental model

You’re dealing with **two deliverables** + an **IDL**:

1. **IDL**: `.proto` files (schema / service definitions)
2. **Compiler**: `protoc` (turns `.proto` → generated code + optional descriptor sets)
3. **Python runtime**: `protobuf` PyPI package (`google.protobuf.*`) that executes generated modules + provides reflection/JSON/text tooling

The sharp boundary: **`protoc` emits modules that *require* the runtime at import time** (descriptor bootstrap + class/materialization). The runtime is not optional. ([Protocol Buffers][1])

---

## A1) Install + introspect the Python runtime (`protobuf` PyPI)

### Installation (runtime)

```bash
pip install protobuf
```

PyPI is the canonical distribution point; it exposes current versions and wheel targets. ([PyPI][2])

### Runtime identity (version)

```python
import google.protobuf
print(google.protobuf.__version__)
```

(You’ll often treat this as a **contract knob** in CI to prevent “gencode/runtime drift”.) ([PyPI][2])

---

## A2) Install + verify the compiler (`protoc`)

### Installation (compiler)

The official install doc calls out three paths: prebuilt release zips, package managers, or building from source. ([Protocol Buffers][3])

**Package manager installs**

```bash
# Linux
apt install -y protobuf-compiler
protoc --version

# macOS
brew install protobuf
protoc --version

# Windows
winget install protobuf
protoc --version
```

The doc explicitly warns that package managers can be outdated and tells you to run `protoc --version` to verify recency. ([Protocol Buffers][3])

**Prebuilt protoc zip**

```bash
PB_REL="https://github.com/protocolbuffers/protobuf/releases"
curl -LO $PB_REL/download/v30.2/protoc-30.2-linux-x86_64.zip
unzip protoc-30.2-linux-x86_64.zip -d $HOME/.local
export PATH="$PATH:$HOME/.local/bin"
```

([Protocol Buffers][3])

---

## A3) Toolchain outputs beyond Python code: FileDescriptorSet (schema-as-data artifact)

If you’re doing **schema-driven tooling** (dynamic message construction, registry services, API gateways, doc-gen, etc.), you typically want a **self-contained descriptor set**.

### Compiler flags (general protoc surface)

* `--descriptor_set_out=FILE` : write a `FileDescriptorSet` containing descriptors for inputs
* `--include_imports` : include all imported dependencies so the set is loadable/self-contained
* `--include_source_info` : include `SourceCodeInfo` (locations/comments); makes output larger

These flags are commonly documented/used as a bundle and explicitly noted as important for “loadable” descriptor sets. ([GitHub][4])

### Typical call pattern

```bash
protoc \
  -I proto \
  --include_imports \
  --include_source_info \
  --descriptor_set_out=build/schema.desc \
  proto/**/*.proto
```

(Your Section F/G machinery then consumes this via `descriptor_pb2.FileDescriptorSet` → `DescriptorPool.Add(...)` → `MessageFactory`.) ([GitHub][4])

---

## A) Footguns + invariants (distribution/toolchain)

* **Compiler and runtime are separable installs**: `protoc` is not shipped by `pip install protobuf`; you must install/ship it separately for codegen. ([Protocol Buffers][3])
* **Package-manager protoc can be stale**: official docs warn to verify `protoc --version` post-install. ([Protocol Buffers][3])
* **Descriptor sets are not self-contained unless you pass `--include_imports`** (or you’ll later fail to load/resolve). ([Buf][5])
* **Modern Python gencode can enforce/complain about runtime drift** (warnings and/or import-time validation are now common), so “check in pb2.py files but float runtime” is brittle. ([GitHub][6])

---

# B) Code generation products + packaging rules (compiler invocation → importable modules)

### Mental model

Python codegen is **path-based**, not `.proto package`-based: importability is determined by **directory layout of generated files** and Python’s `sys.path`, not the proto `package` clause. ([Protocol Buffers][7])

---

## B1) Core Python codegen (`--python_out`) and path mapping

### Primary knob

* `protoc --python_out=OUT_DIR ...`

The generated-code guide defines the mapping rules:

* `.proto` → `_pb2.py`
* `--proto_path` / `-I` prefix is replaced by `--python_out` output path
* output subdirectories are created as needed, but the top-level output dir must exist ([Protocol Buffers][7])

### Canonical example (from docs)

```bash
protoc --proto_path=src --python_out=build/gen src/foo.proto src/bar/baz.proto
# -> build/gen/foo_pb2.py
# -> build/gen/bar/baz_pb2.py
```

Also: compiler may create `build/gen/bar` but will not create `build` or `build/gen`. ([Protocol Buffers][7])

### Module name sanitization

If the `.proto` filename/path contains characters invalid in Python identifiers (e.g., hyphens), they’re replaced with underscores:

* `foo-bar.proto` → `foo_bar_pb2.py` ([Protocol Buffers][7])

---

## B2) Python typing stubs (`--pyi_out`)

### Stub generation knob

* `protoc --pyi_out=OUT_DIR ...`

Both the tutorial and the generated-code guide explicitly call out `.pyi` stub generation via `--pyi_out`. ([Protocol Buffers][1])

### Typical call pattern

```bash
protoc -I proto --python_out=gen --pyi_out=gen proto/my/pkg/messages.proto
# -> gen/my/pkg/messages_pb2.py
# -> gen/my/pkg/messages_pb2.pyi
```

(The output path mapping mirrors `--python_out` semantics.) ([Protocol Buffers][7])

---

## B3) Output-to-zip as a first-class compiler feature

The generated-code guide explicitly recommends outputting Python gencode directly to a `.zip` because Python can import from zip archives when placed on `PYTHONPATH`. ([Protocol Buffers][7])

### Call pattern

```bash
protoc -I proto --python_out=build/gen.zip proto/my/pkg/messages.proto
# build/gen.zip contains my/pkg/messages_pb2.py
```

([Protocol Buffers][7])

---

## B4) Packaging invariants: `.proto package` vs Python package layout

### Hard rule

Python generated code is “completely unaffected” by `.proto package`; Python packages are determined by directory structure. ([Protocol Buffers][7])

### Still declare `package` anyway (for proto namespace + non-Python)

The Python tutorial explicitly says: even though it won’t affect Python import paths, you should still declare `package` to avoid name collisions in the Protobuf namespace and in other languages. ([Protocol Buffers][1])

### Practical packaging recipe (what actually works)

* Choose a stable `--proto_path` so generated files land under a consistent root (`gen/…`)
* Ensure that generated root is importable (e.g., add `gen/` to `PYTHONPATH` / `sys.path`)
* Ensure the directory structure matches how you want to import modules (`my/pkg/foo_pb2.py` → `import my.pkg.foo_pb2`) ([Protocol Buffers][7])

---

## B) Footguns + invariants (codegen + packaging)

* **OUT_DIR must exist**: protoc creates subdirectories *under* OUT_DIR but not OUT_DIR itself. ([Protocol Buffers][7])
* **Import path = filesystem path**: `.proto package` does not fix broken imports; only directory layout + `sys.path` does. ([Protocol Buffers][7])
* **Hyphens become underscores**: if you reference module names programmatically, normalize like protoc does or you’ll “mystery import” fail. ([Protocol Buffers][7])
* **Zip output works, but you’re now shipping an importable artifact**: treat the zip as a versioned build product (hash it, pin it) if you use it in production. ([Protocol Buffers][7])

---

# C) Generated module + symbol surface (`*_pb2.py` ABI you can treat as a contract)

### Mental model

A generated Python module is **not** “plain handwritten code.” It is a **descriptor bootstrap** that, at import-time, loads a `FileDescriptor` into a pool and asks internal builders/metaclasses to materialize message/enum classes.

The docs describe this as: protoc outputs descriptor-building code; a Python metaclass “does the real work,” and the guide documents the post-metaclass surface. ([Protocol Buffers][7])

---

## C1) Module bootstrap scaffolding (import-time behavior you must account for)

From the official tutorial’s representative generated output (high-signal because it shows what modern pb2 files *actually do*): ([Protocol Buffers][1])

### Runtime version validation (gencode/runtime discipline)

Generated code imports `runtime_version` and calls:

* `_runtime_version.ValidateProtobufRuntimeVersion(...)` ([Protocol Buffers][1])

This is the “import-time guardrail” that surfaces gencode/runtime drift as warnings/errors in newer ecosystems. ([GitHub][6])

### Descriptor pool load + module descriptor

* `DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b"...")` ([Protocol Buffers][1])

### Builder-based materialization

* `_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())`
* `_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'some.module_pb2', globals())` ([Protocol Buffers][1])

### Symbol database handle

* `_sym_db = _symbol_database.Default()` ([Protocol Buffers][1])

### Protoc insertion points (plugin + augmentation boundary)

Generated code includes markers like:

* `# @@protoc_insertion_point(imports)`
* `# @@protoc_insertion_point(module_scope)` ([Protocol Buffers][1])

### Descriptor implementation toggle (debug metadata)

Generated code may include:

* `if not _descriptor._USE_C_DESCRIPTORS: ... _serialized_start/_serialized_end ...` ([Protocol Buffers][1])

**Practical implication:** importing a pb2 module is a **side-effectful operation** (pool registration + class creation). Treat import order and version pinning as contract concerns. ([Protocol Buffers][1])

---

## C2) Generated message classes (`Foo`)

From the generated-code guide: given `message Foo {}`, protoc generates a concrete class `Foo` that subclasses `google.protobuf.Message` and has no abstract methods. ([Protocol Buffers][7])

### Class identity + stability rules

* **Do not subclass generated message classes** (“fragile base class” / not designed for inheritance). ([Protocol Buffers][7])
* Python generation is unaffected by `optimize_for`; in effect, Python is optimized for code size. ([Protocol Buffers][7])

### Extra generated convenience

* `Foo.FromString(s: bytes) -> Foo` (static) returns a new instance deserialized from wire bytes. ([Protocol Buffers][7])

### Nested types

If `Bar` is nested in `Foo`, it’s accessible as:

* `Foo.Bar` ([Protocol Buffers][7])

### Keyword collisions (message names)

If the message name is a Python keyword, you must access the class via `getattr(module, "keyword")`. ([Protocol Buffers][7])

---

## C3) Field surface in generated classes (properties + numeric constants)

### Property naming

For each field in the message, the class has a property with the same name as the field. ([Protocol Buffers][7])

### Field-number constants (compile-time numeric ABI)

For each field, protoc also generates an integer constant:

* `FOO_BAR_FIELD_NUMBER = 5` for `int32 foo_bar = 5;` ([Protocol Buffers][7])

These constants are extremely useful for “schema-as-contract” tooling (stable numbers, stable diffs). ([Protocol Buffers][7])

### Keyword collisions (field names)

If a field name is a Python keyword, it is only accessible via `getattr()` / `setattr()` (dot access is a syntax error). The generated-code guide includes an explicit example (`from`, `in`). ([Protocol Buffers][7])

**Call syntax**

```python
setattr(msg, "from", 99)
x = getattr(msg, "from")
getattr(msg, "in").append(42)
```

([Protocol Buffers][7])

---

## C4) Enums in generated modules (constants + `EnumTypeWrapper` utilities)

The generated-code guide describes Python enums as integer-valued constants that can be accessed multiple ways, and exposes utility methods via the generated enum wrapper. ([Protocol Buffers][7])

### Access patterns for an enum `SomeEnum { VALUE_A = 0; ... }`

* `myproto_pb2.SomeEnum.VALUE_A`
* `myproto_pb2.VALUE_A` (if enum is top-level)
* `myproto_pb2.SomeEnum.Value("VALUE_A")` (string → int) ([Protocol Buffers][7])

### Utility API surface (common)

* `SomeEnum.Name(number: int) -> str`
* `SomeEnum.Value(name: str) -> int`
  (Guide explicitly references `enum_type_wrapper.EnumTypeWrapper` as the base for generated enum classes.) ([Protocol Buffers][7])

### Keyword collisions (enum / enum value identifiers)

If an enum name or enum value is a Python keyword, access it via `getattr()`. ([Protocol Buffers][7])

---

## C5) `.proto package` vs “protobuf full_name” vs Python import name (three namespaces!)

You will see three “names” for the same concept:

* **Python import name**: derived from filesystem path (`my/pkg/foo_pb2.py`) ([Protocol Buffers][7])
* **Protobuf symbol full name**: derived from `.proto package` + message name (`tutorial.Person`) ([Protocol Buffers][1])
* **File descriptor name**: `.proto` filename (`main.proto`, `foo.proto`) and its descriptor pool entrypoint (`AddSerializedFile(...)`) ([Protocol Buffers][1])

Your contract harness should treat **protobuf full_name + field numbers** as the stable schema identity, not Python module names. ([Protocol Buffers][7])

---

## C) Footguns + invariants (generated ABI)

* **Import-time side effects are real**: pb2 import loads descriptors and builds classes into module globals (and into pools/registries). Treat it as an initialization step. ([Protocol Buffers][1])
* **Gencode/runtime drift is now enforced more aggressively**: modern gencode includes runtime version validation; users report warnings when gencode lags runtime. ([Protocol Buffers][1])
* **Do not subclass generated messages**: both the guide and tutorial explicitly recommend wrapping instead of inheritance. ([Protocol Buffers][7])
* **Keyword identifiers remain “as-is”**: the generator doesn’t rename them; you must use `getattr/setattr`. ([Protocol Buffers][7])
* **Python module name ≠ protobuf package**: `.proto package` does nothing for Python imports; your build layout must make generated modules importable. ([Protocol Buffers][7])

---

If you want the next micro-increment after A–C: I can add an **“A–C contract harness extension”** that snapshots *exactly* the gencode bootstrap metadata (the serialized `FileDescriptorProto` bytes / pool registration behavior) so you can detect toolchain drift even when the visible message APIs remain identical.

[1]: https://protobuf.dev/getting-started/pythontutorial/ "Protocol Buffer Basics: Python | Protocol Buffers Documentation"
[2]: https://pypi.org/project/protobuf/?utm_source=chatgpt.com "protobuf · PyPI"
[3]: https://protobuf.dev/installation/ "Protocol Buffer Compiler Installation | Protocol Buffers Documentation"
[4]: https://github.com/tokio-rs/prost/issues/880?utm_source=chatgpt.com "Use of --include_imports option should be optional #880"
[5]: https://buf.build/docs/reference/descriptors/?utm_source=chatgpt.com "Descriptors - Buf Docs"
[6]: https://github.com/protocolbuffers/protobuf/issues/18096?utm_source=chatgpt.com "Excessive new warning in Python protobuf runtime #18096"
[7]: https://protobuf.dev/reference/python/python-generated/ "Python Generated Code Guide | Protocol Buffers Documentation"

Below is the **D–H expansion** in the same “catalog + call-syntax” style: **every capability is anchored to the concrete Python entrypoint** (method/function/property), clustered by semantic plane, with a **“footguns + invariants”** block at the end of each plane.

> Scope note: this is the **Python runtime surface** (`protobuf` PyPI package; currently in the 6.x line). ([PyPI][1])
> The call signatures below are taken from the published Python API reference (plus WKT generated APIs) and are intended as a stable “contract surface.” ([Google Cloud][2])

---

# D) Core runtime `Message` API (binary wire, mutation, presence, containers, unknowns)

### Mental model: message instances are **typed records**; the runtime offers:

1. **structural ops** (clear/copy/merge), 2) **wire ops** (parse/serialize), 3) **presence** (where supported), 4) **container proxies** for repeated/map fields, 5) **unknown-field retention** for forward-compat.

---

## D1) Size & field enumeration

### Full surface area

* `msg.ByteSize() -> int`
  Returns serialized size; recursive over submessages. ([Google Cloud][2])
* `msg.ListFields() -> list[tuple[FieldDescriptor, Any]]`
  Returns **present / non-empty** fields ordered by field number. (Presence semantics differ proto2 vs proto3; see footguns.) ([Google Cloud][2])

### Minimal snippet

```python
pairs = msg.ListFields()
for fd, value in pairs:
    print(fd.full_name, value)
```

---

## D2) Clearing, copying, merging (object-graph semantics)

### Full surface area

* `msg.Clear() -> None`
  Clears all set data. ([Google Cloud][2])
* `msg.ClearField(field_name: str) -> None`
  Clears a named field; **also accepts oneof group name** (“clear whichever is set”). Raises `ValueError` if not a field/oneof. ([Google Cloud][2])
* `msg.CopyFrom(other_msg: Message) -> None`
  Clears then merges `other_msg` into `msg`. ([Google Cloud][2])
* `msg.MergeFrom(other_msg: Message) -> None`
  Merge semantics: singular overwrite; repeated append; submessages recursively merge. ([Google Cloud][2])
* `msg.MergeFromString(serialized: bytes|buffer) -> int`
  Parses and merges wire data into `msg`; returns bytes consumed (group nuance). Merge rules spelled out (repeated append, scalar overwrite, composite merge). ([Google Cloud][2])

### Minimal snippet

```python
msg.ClearField("foo")      # or ClearField("my_oneof")
msg.CopyFrom(other)
msg.MergeFrom(other)
n = msg.MergeFromString(blob)
```

---

## D3) Binary serialization / parsing

### Full surface area

* `msg.SerializeToString(deterministic: bool=False) -> bytes`
  Deterministic affects **map-key ordering**. Raises `EncodeError` if required fields missing (`IsInitialized()` false). ([Google Cloud][2])
* `msg.SerializePartialToString(deterministic: bool=False) -> bytes`
  Like SerializeToString, but **skips initialization check**. ([Google Cloud][2])
* `msg.ParseFromString(serialized: bytes|buffer) -> None`
  Like `MergeFromString`, but **clears first**. Raises `DecodeError` on parse failure. ([Google Cloud][2])

### Minimal snippet

```python
wire = msg.SerializeToString(deterministic=True)
msg.ParseFromString(wire)  # clears then parses
```

---

## D4) Presence & oneofs

### Full surface area

* `msg.HasField(field_name: str) -> bool`
  Checks presence for a field or oneof group; raises `ValueError` if not in descriptor. ([Google Cloud][2])
* `msg.WhichOneof(oneof_group: str) -> str|None`
  Returns which field name is set for a oneof; raises `ValueError` if no such group. ([Google Cloud][2])
* `msg.SetInParent() -> None`
  Forces an empty submessage to be considered present in parent (design smell). ([Google Cloud][2])

---

## D5) Extensions (proto2 / editions use-cases)

### Full surface area

* `msg.HasExtension(extension_handle) -> bool`
  Presence for singular extensions; raises `KeyError` for repeated extensions (presence notion = empty vs non-empty). ([Google Cloud][2])
* `msg.ClearExtension(extension_handle) -> None` ([Google Cloud][2])
* `Message.RegisterExtension(extension_handle) -> None` (static) ([Google Cloud][2])

### Call syntax you actually use (generated)

* `msg.Extensions[my_ext]` (read/write) — extension map accessor (documented by runtime as “Extensions mapping”). ([Google Cloud][2])

---

## D6) Unknown fields (forward-compat payload retention)

### Full surface area

* `msg.UnknownFields() -> UnknownFieldSet`
  Returns unknown-field container. ([Google Cloud][2])
* `msg.DiscardUnknownFields() -> None`
  Recursively clears UnknownFieldSet on message + nested messages. ([Google Cloud][2])

### Unknown field containers (semi-public but widely used for inspection)

From `google.protobuf.internal.containers`:

* `UnknownFieldSet` (iterable container)
* `UnknownFieldRef(field_number, wire_type, data)` (per-entry view) ([Google Cloud][3])

---

## D7) Repeated fields & map fields (container proxies)

These are the concrete “mutation surfaces” you use when updating repeated/map fields.

### Repeated scalars: `RepeatedScalarFieldContainer`

* `msg.repeated_scalar.append(x)`
* `msg.repeated_scalar.extend(iterable)`
* `msg.repeated_scalar.insert(i, x)`
* `msg.repeated_scalar.pop([i])`
* `msg.repeated_scalar.remove(x)`
* `msg.repeated_scalar.clear()`
* `msg.repeated_scalar.MergeFrom(other_container)`
* plus list ops: `count`, `index`, `reverse`, `sort` ([Google Cloud][3])

### Repeated messages: `RepeatedCompositeFieldContainer`

* `child = msg.repeated_msg.add(**kwargs)` (alloc + append + return child)
* `msg.repeated_msg.append(child_msg)` (copies message)
* `msg.repeated_msg.extend(seq_of_msgs)` (copies each)
* `msg.repeated_msg.insert(i, child_msg)` (copies)
* `msg.repeated_msg.pop([i])`, `remove`, `clear`
* `msg.repeated_msg.MergeFrom(other_container)` ([Google Cloud][3])

### Map fields: scalar values (`ScalarMap`) vs message values (`MessageMap`)

Both are dict-like with type checking:

* `m[key] = value` (scalar map)
* `m.get(key, default)`
* `m.items() / keys() / values()`
* `m.pop(key[, default]) / popitem() / clear() / update(...) / setdefault(...)`
* `m.MergeFrom(other_map)` ([Google Cloud][3])

Special for message-valued map: `MessageMap`

* `msg.my_msg_map.get_or_create(key)` (alias for `msg.my_msg_map[key]`, but makes mutation explicit for linters) ([Google Cloud][3])

---

## D) Footguns + invariants (core `Message`)

* **Parse vs merge**: `ParseFromString()` clears first; `MergeFromString()` merges into existing fields (repeated append, scalar overwrite, composite merge). ([Google Cloud][2])
* **Deterministic serialization** only promises **predictable map-key ordering**, not canonicalization of all wire-level details. ([Google Cloud][2])
* **Presence is not uniform**: proto3 **implicit presence scalars** do not support `HasField()`; rely on proto3 `optional`, oneofs, wrappers, or API-specific presence rules. ([Protocol Buffers][4])
* **`SetInParent()` is a smell**: if you need “empty but present,” consider schema redesign (oneof / explicit presence). ([Google Cloud][2])
* **Map/repeated are container proxies**: treat them as mutable “views” tied to parent message; copying semantics differ (repeated composite `.append` copies messages; `.add` constructs new). ([Google Cloud][3])

---

# E) Alternate formats: JSON mapping + textproto (debugging, interop, diffs)

---

## E1) JSON mapping: `google.protobuf.json_format`

### Full surface area (functions)

* `json_format.MessageToDict(message, including_default_value_fields=False, preserving_proto_field_name=False, use_integers_for_enums=False, descriptor_pool=None, float_precision=None) -> dict` ([Google Cloud][5])
* `json_format.MessageToJson(message, including_default_value_fields=False, preserving_proto_field_name=False, indent=2, sort_keys=False, use_integers_for_enums=False, descriptor_pool=None, float_precision=None, ensure_ascii=True) -> str` ([Google Cloud][5])
* `json_format.Parse(text: str, message: Message, ignore_unknown_fields=False, descriptor_pool=None, max_recursion_depth=100) -> Message` (merges into `message`) ([Google Cloud][5])
* `json_format.ParseDict(js_dict: dict, message: Message, ignore_unknown_fields=False, descriptor_pool=None, max_recursion_depth=100) -> Message` ([Google Cloud][5])

### Full surface area (exceptions)

* `json_format.Error` (module base error)
* `json_format.ParseError`
* `json_format.SerializeToJsonError` ([Google Cloud][5])

### Semantics knobs worth treating as “contract knobs”

* **Field naming**: `preserving_proto_field_name=True` preserves snake_case; default lowerCamelCase. ([Google Cloud][5])
* **Default/empty emission**: `including_default_value_fields=True` forces emission for singular primitives / repeated / maps; message fields and oneofs are special-cased. ([Google Cloud][5])
* **Enums**: `use_integers_for_enums=True` prints numeric value. ([Google Cloud][5])
* **Type resolution**: `descriptor_pool=` matters for `Any` unpack / resolution. ([Google Cloud][5])
* **Safety**: `ignore_unknown_fields=True` and `max_recursion_depth=` are correctness/robustness controls. ([Google Cloud][5])

---

## E2) Text format (“textproto”): `google.protobuf.text_format`

### Full surface area (printing)

* `text_format.MessageToString(message, as_utf8=False, as_one_line=False, use_short_repeated_primitives=False, pointy_brackets=False, use_index_order=False, float_format=None, double_format=None, use_field_number=False, descriptor_pool=None, indent=0, message_formatter=None, print_unknown_fields=False, force_colon=False) -> str` ([Google Cloud][6])
* `text_format.MessageToBytes(message, **kwargs) -> bytes` (encoded text; same kwargs as MessageToString) ([Google Cloud][6])
* `text_format.PrintMessage(message, out, ...) -> None` (stream writer) ([Google Cloud][6])
* `text_format.PrintField(field, value, out, ...) -> None` ([Google Cloud][6])
* `text_format.PrintFieldValue(field, value, out, ...) -> None` ([Google Cloud][6])

### Full surface area (parsing)

* `text_format.Parse(text, message, allow_unknown_extension=False, allow_field_number=False, descriptor_pool=None, allow_unknown_field=False) -> Message` ([Google Cloud][6])
* `text_format.Merge(text, message, allow_unknown_extension=False, allow_field_number=False, descriptor_pool=None, allow_unknown_field=False) -> Message`
  Like Parse, but **allows repeated values for non-repeated fields** and uses the last one (“replace top-level” semantics). ([Google Cloud][6])

### Parsing invariants baked into docs

* `Parse()` **does not clear** message (historic behavior), unlike binary parse; repeated fields accumulate; singular conflicts error; caller must clear if needed. ([Google Cloud][6])
* Floating formatting: `double_format='.17g'` is recommended for roundtrip identity. ([Google Cloud][6])

---

## E) Footguns + invariants (JSON + text)

* **Both JSON and text parse APIs “merge into” the provided message** (they do not implicitly clear). For text this is explicitly highlighted; for JSON it’s stated as “merge into.” ([Google Cloud][5])
* **Name mapping drift**: JSON defaults to lowerCamelCase field names; enforce `preserving_proto_field_name=True` if your system treats proto field names as a stable contract. ([Google Cloud][5])
* **Unknown-field handling differs by format**: JSON parse can ignore unknowns via `ignore_unknown_fields`; text parse can skip unknowns via `allow_unknown_field` (dangerous—can hide typos). ([Google Cloud][5])
* **Any resolution depends on descriptor pools** for both JSON and text printers/parsers when `Any` appears. ([Google Cloud][5])

---

# F) Descriptors & reflection plane (schema as data)

### Mental model: descriptors are the **runtime schema graph**. You use them to:

* inspect fields/types/options,
* build dynamic messages,
* implement schema-driven tooling (codegen, validation, contract enforcement).

---

## F1) Descriptor objects (`google.protobuf.descriptor`)

### Message descriptor: `Descriptor`

**High-value attributes (read-only)**

* `desc.name: str`, `desc.full_name: str`
* `desc.fields: list[FieldDescriptor]`
* `desc.fields_by_name: dict[str, FieldDescriptor]`
* `desc.fields_by_number: dict[int, FieldDescriptor]`
* `desc.nested_types`, `desc.nested_types_by_name`
* `desc.enum_types`, `desc.enum_types_by_name`, `desc.enum_values_by_name`
* `desc.extensions`, `desc.extensions_by_name`
* `desc.oneofs`, `desc.oneofs_by_name`
* `desc.file: FileDescriptor`
* `desc.fields_by_camelcase_name` (property) ([Google Cloud][7])

**Methods**

* `desc.CopyToProto(proto: descriptor_pb2.DescriptorProto) -> None` ([Google Cloud][7])
* `desc.GetOptions() -> <OptionsMsg>` ([Google Cloud][7])
* `desc.EnumValueName(enum: str, value: int) -> str` ([Google Cloud][7])

### Field descriptor: `FieldDescriptor`

High-value attributes you commonly need in schema tooling:

* `fd.name`, `fd.full_name`, `fd.number`, `fd.index`
* `fd.type` (TYPE_*), `fd.cpp_type` (CPPTYPE_*), `fd.label` (LABEL_*)
* `fd.default_value`, `fd.has_default_value`
* `fd.message_type` (Descriptor | None), `fd.enum_type` (EnumDescriptor | None)
* `fd.containing_type`, `fd.containing_oneof`
* `fd.is_extension`, `fd.extension_scope`, `fd.json_name` ([Google Cloud][7])

### Enum descriptors

* `enum_desc.values: list[EnumValueDescriptor]`
* `enum_desc.values_by_name`, `enum_desc.values_by_number`
* `enum_desc.CopyToProto(proto: descriptor_pb2.EnumDescriptorProto)`
* `enum_desc.GetOptions()` ([Google Cloud][7])

---

## F2) Descriptor protos (`google.protobuf.descriptor_pb2`) — schema IR

### The “transportable schema bundle”

* `descriptor_pb2.FileDescriptorProto` (one file)
* `descriptor_pb2.FileDescriptorSet` (many files, typically compiler output)
* `descriptor_pb2.DescriptorProto` (message definition)
* `descriptor_pb2.FieldDescriptorProto`, `EnumDescriptorProto`, `ServiceDescriptorProto`, etc. ([GitHub][8])

### Canonical operations (call syntax)

* Build/edit like any proto message:

  * `fdp = descriptor_pb2.FileDescriptorProto()`
  * `fdp.name = "x.proto"; fdp.package = "pkg"`
  * `wire = fdp.SerializeToString()`
* Embed in a set:

  * `fds = descriptor_pb2.FileDescriptorSet(); fds.file.append(fdp)` (container ops follow repeated semantics)

*(You’ll use these objects as inputs to DescriptorPool/MessageFactory in Section G.)*

---

## F3) Descriptor registry layers

### `google.protobuf.descriptor_database.DescriptorDatabase`

A container for `FileDescriptorProto` plus lookup operations: ([Google Cloud][9])

* `db.Add(file_desc_proto) -> None` (raises `DescriptorDatabaseConflictingDefinitionError` on conflicting redefinition) ([Google Cloud][9])
* `db.FindFileByName(name: str) -> FileDescriptorProto`
* `db.FindFileContainingSymbol(symbol: str) -> FileDescriptorProto`
* `db.FindFileContainingExtension(extendee_name: str, extension_number: int) -> FileDescriptorProto`
* `db.FindAllExtensionNumbers(extendee_name: str) -> Iterable[int]` ([Google Cloud][9])

### `google.protobuf.descriptor_pool.DescriptorPool`

A “compiled” container for descriptors (adds + resolves): ([Google Cloud][10])

* Construction:

  * `descriptor_pool.Default() -> DescriptorPool`
  * `pool = descriptor_pool.DescriptorPool(descriptor_db=None)`
* Add schema:

  * `pool.Add(file_desc_proto: FileDescriptorProto) -> None`
  * `pool.AddSerializedFile(serialized_file_desc_proto: bytes) -> FileDescriptor`
* Lookup:

  * `pool.FindMessageTypeByName(full_name) -> Descriptor`
  * `pool.FindEnumTypeByName(full_name) -> EnumDescriptor`
  * `pool.FindFieldByName(full_name) -> FieldDescriptor`
  * `pool.FindOneofByName(full_name) -> OneofDescriptor`
  * `pool.FindServiceByName(full_name) -> ServiceDescriptor`
  * `pool.FindMethodByName(full_name) -> MethodDescriptor`
  * `pool.FindExtensionByName(full_name) -> FieldDescriptor`
  * `pool.FindExtensionByNumber(message_descriptor, number) -> FieldDescriptor`
  * `pool.FindAllExtensions(message_descriptor) -> list[FieldDescriptor]`
  * `pool.FindFileByName(file_name) -> FileDescriptor`
  * `pool.FindFileContainingSymbol(symbol) -> FileDescriptor` ([Google Cloud][10])

### Reflection metaclass (`google.protobuf.reflection`)

* `reflection.GeneratedProtocolMessageType` is the metaclass used to inject generated behaviors at import/load time. ([Google Cloud][11])

---

## F) Footguns + invariants (descriptors)

* **Descriptor protos vs descriptors**: `descriptor_pb2.*` are *data*; `descriptor.*` objects are *runtime views*; `DescriptorPool.Add*` is the boundary converting the former into the latter. ([Google Cloud][9])
* **Conflicting schema definitions are fatal at the DB layer** (`DescriptorDatabaseConflictingDefinitionError`), which is exactly what you want for contract discipline. ([Google Cloud][9])
* **Pool lookup names are fully-qualified** (package-qualified); treat `full_name` as your stable schema key. ([Google Cloud][7])

---

# G) Dynamic types at runtime (DescriptorPool ⇄ MessageFactory ⇄ ProtoBuilder)

---

## G1) `google.protobuf.message_factory` (dynamic message classes from descriptors)

### Full surface area

* `message_factory.MessageFactory(pool=None)` ([Google Cloud][12])
* `factory.GetPrototype(descriptor) -> type[Message]`
  Returns cached class for the descriptor full name. ([Google Cloud][12])
* `factory.CreatePrototype(descriptor) -> type[Message]`
  Always creates new class; intended for override/hooking; “don’t call directly.” ([Google Cloud][12])
* `factory.GetMessages(files: list[str]) -> dict[str, type[Message]]`
  Builds all messages from specified file(s), resolving dependencies via the pool. ([Google Cloud][12])

**Module-level helper**

* `message_factory.GetMessages(file_protos: Iterable[FileDescriptorProto]) -> dict[str, type[Message]]` (builds from protos directly) ([Google Cloud][12])

### Minimal snippet (DescriptorPool → prototype class)

```python
from google.protobuf import descriptor_pool, message_factory

pool = descriptor_pool.DescriptorPool()
pool.Add(file_desc_proto)

md = pool.FindMessageTypeByName("pkg.MyMessage")
MyMessage = message_factory.MessageFactory(pool).GetPrototype(md)
msg = MyMessage()
```

---

## G2) `google.protobuf.proto_builder` (quick dynamic schemas)

### Full surface area

* `proto_builder.MakeSimpleProtoClass(fields: dict, full_name: str|None=None, pool: DescriptorPool|None=None) -> type[Message]`
  Notes: does **not validate field names**; field order follows OrderedDict else sorts by name. ([Google Cloud][13])

### Minimal snippet

```python
from collections import OrderedDict
from google.protobuf import descriptor, proto_builder

T = proto_builder.MakeSimpleProtoClass(
    OrderedDict([
        ("id", descriptor.FieldDescriptor.TYPE_INT64),
        ("name", descriptor.FieldDescriptor.TYPE_STRING),
    ]),
    full_name="pkg.Dynamic",
)
t = T(id=1, name="x")
```

*(Field type values are the `FieldDescriptor.TYPE_*` constants.)* ([Google Cloud][7])

---

## G3) `google.protobuf.symbol_database` (registry for compile-time generated code)

### Full surface area

* `symbol_database.Default() -> SymbolDatabase` (global default) ([Google Cloud][14])
* `symbol_database.SymbolDatabase(pool=None)` ([Google Cloud][14])

**Registry ops**

* `db.RegisterFileDescriptor(file_descriptor)` ([Google Cloud][14])
* `db.RegisterMessage(message_cls_or_instance)` ([Google Cloud][14])
* `db.RegisterMessageDescriptor(message_descriptor)` ([Google Cloud][14])
* `db.RegisterEnumDescriptor(enum_descriptor)` ([Google Cloud][14])
* `db.RegisterServiceDescriptor(service_descriptor)` ([Google Cloud][14])

**Lookup / factory ops**

* `db.GetSymbol(symbol: str) -> type[Message]` (raises `KeyError` if missing) ([Google Cloud][14])
* `db.GetMessages(files: list[str]) -> dict[str, type[Message]]` (only returns already-registered messages; differs from MessageFactory) ([Google Cloud][14])
* `db.GetPrototype(descriptor) -> type[Message]` / `db.CreatePrototype(descriptor) -> type[Message]` (inherited factory surface) ([Google Cloud][14])

---

## G) Footguns + invariants (dynamic)

* **Prefer `GetPrototype()` over `CreatePrototype()`** unless you’re intentionally bypassing caching / injecting custom class construction. ([Google Cloud][12])
* **DescriptorPool is your isolation boundary**: use separate pools when you need strict schema namespace isolation (e.g., multi-tenant schema ingestion). Pool lookups are by fully-qualified name, so collisions are real. ([Google Cloud][10])
* **`proto_builder.MakeSimpleProtoClass` is intentionally “sharp”** (no name validation, implicit ordering rules). Use it for tests, adapters, and lightweight dynamic protos—not as a primary schema system. ([Google Cloud][13])

---

# H) Well-known types (WKT) + their Python-only convenience APIs

### Mental model: WKTs are normal messages **plus** extra utility methods (conversion, packing, JSON helpers). ([Protocol Buffers][4])

---

## H1) `Any` (`google.protobuf.any_pb2.Any`)

### Full surface area (WKT extras)

* `any_msg.Pack(msg, type_url_prefix='type.googleapis.com/', deterministic=None) -> None` ([Google Cloud][15])
* `any_msg.Unpack(msg) -> bool|None` (unpacks into provided msg instance) ([Google Cloud][15])
* `any_msg.TypeName() -> str` (inner protobuf full type name) ([Google Cloud][15])

### Typical call patterns

```python
from google.protobuf import any_pb2
a = any_pb2.Any()
a.Pack(my_msg, deterministic=True)
ok = a.Unpack(dst_msg)
t = a.TypeName()
```

([Google Cloud][15])

---

## H2) `Timestamp` (`google.protobuf.timestamp_pb2.Timestamp`)

### Full surface area (WKT extras)

* `ts.FromDatetime(dt: datetime) -> None` (timezone-naive assumed UTC) ([Google Cloud][16])
* `ts.ToDatetime(tzinfo=None) -> datetime` (tz-aware if tzinfo provided; otherwise naive UTC) ([Google Cloud][16])
* `ts.FromJsonString(value: str) -> None` (RFC3339 parse; ValueError on failure) ([Google Cloud][16])
* `ts.ToJsonString() -> str` (RFC3339 Z-normalized, fractional digits as needed) ([Google Cloud][16])
* `ts.ToSeconds()/ToMilliseconds()/ToMicroseconds()/ToNanoseconds() -> int` ([Google Cloud][16])

*(If your codebase treats timestamps as contract-critical, standardize on the JSON string form for stable diffs and cross-language equivalence.)* ([Google Cloud][16])

---

## H3) `Duration` (`google.protobuf.duration_pb2.Duration`)

### Full surface area (WKT extras)

* `dur.FromTimedelta(td: datetime.timedelta) -> None` ([Google Cloud][17])
* `dur.ToTimedelta() -> datetime.timedelta` ([Google Cloud][17])
* `dur.FromJsonString(value: str) -> None` (must end with `'s'`, fractional digits allowed; ValueError on parse issues) ([Google Cloud][17])
* `dur.ToJsonString() -> str` (3/6/9 fractional digits as needed) ([Google Cloud][17])
* `dur.FromSeconds/FromMilliseconds/FromMicroseconds/FromNanoseconds(x: int) -> None` ([Google Cloud][17])
* `dur.ToSeconds/ToMilliseconds/ToMicroseconds/ToNanoseconds() -> int` ([Google Cloud][17])

---

## H4) `FieldMask` (`google.protobuf.field_mask_pb2.FieldMask`)

### Full surface area (WKT extras)

* JSON form conversion:

  * `mask.FromJsonString(value: str) -> None`
  * `mask.ToJsonString() -> str` ([Google Cloud][18])
* Set algebra / normalization:

  * `mask.Union(mask1, mask2) -> None`
  * `mask.Intersect(mask1, mask2) -> None`
  * `field_mask_pb2.FieldMask.CanonicalFormFromMask(mask) -> FieldMask` (removes covered paths, sorts) ([Google Cloud][18])
* Descriptor validation / derivation:

  * `mask.IsValidForDescriptor(message_descriptor) -> bool`
  * `field_mask_pb2.FieldMask.AllFieldsFromDescriptor(message_descriptor) -> FieldMask` ([Google Cloud][18])
* Apply mask to messages:

  * `mask.MergeMessage(source, destination, replace_message_field=False, replace_repeated_field=False) -> None` ([Google Cloud][18])

### Typical call patterns

```python
from google.protobuf import field_mask_pb2

m = field_mask_pb2.FieldMask()
m.FromJsonString("foo,bar.baz")
s = m.ToJsonString()

m2 = field_mask_pb2.FieldMask()
m2.Union(m, other)

m.MergeMessage(src, dst, replace_message_field=True, replace_repeated_field=True)
```

([Google Cloud][18])

---

## H5) `Struct` / `ListValue` / `Value` (`google.protobuf.struct_pb2`)

### `Struct` behaves like a dict-like wrapper (WKT extras)

* `struct.get_or_create_list(key) -> ListValue`
* `struct.get_or_create_struct(key) -> Struct`
* `struct.update(dictionary) -> None`
* `struct.items() / keys() / values()` ([Google Cloud][19])

### `ListValue` behaves like list-like wrapper (WKT extras)

* `lv.append(value)`
* `lv.extend(elem_seq)`
* `lv.add_list() -> ListValue` (append nested list)
* `lv.add_struct() -> Struct` (append nested object) ([Google Cloud][19])

### `Value` is a tagged union (oneof-like)

* properties: `bool_value`, `number_value`, `string_value`, `struct_value`, `list_value`, `null_value` ([Google Cloud][19])

---

## H6) Wrappers (`google.protobuf.wrappers_pb2.*Value`)

These are the classic “presence wrapper” messages for scalars.

* `wrappers_pb2.Int32Value(value=123)` or `w.value = 123`
* Same pattern for `BoolValue`, `StringValue`, `BytesValue`, `DoubleValue`, etc. ([Google Cloud][20])

---

## H) Footguns + invariants (WKT)

* **WKT extras are real API surface**: they exist *in addition* to normal Message methods. The Python generated-code guide explicitly calls this out. ([Protocol Buffers][4])
* **Timestamp timezone rules matter**: `FromDatetime` treats naive datetimes as UTC; `ToDatetime()` returns naive UTC if `tzinfo` not provided. Bake this into your contract rules. ([Google Cloud][16])
* **Duration JSON strings must end with `s`**; parsing failures raise `ValueError`; output precision uses 3/6/9 digits depending on required exactness. ([Google Cloud][17])
* **FieldMask canonicalization** removes paths covered by others and sorts; if you diff masks or use them as cache keys, canonicalize first. ([Google Cloud][18])
* **Struct/ListValue are *not* plain dict/list**; they’re wrappers around `Value`/`fields` that preserve protobuf semantics. Use `.update()` / `.get_or_create_*()` / `.append()` rather than trying to assign incompatible Python types directly. ([Google Cloud][19])

---

If you want the *next* increment after this D–H expansion: I can extend the same treatment to **(I–L)** (services/plugin hooks, generator insertion points, runtime backend toggles, and the “contract-hardening” checklist), so the whole protobuf catalog reads like your PyArrow advanced doc end-to-end.

[1]: https://pypi.org/project/protobuf/?utm_source=chatgpt.com "protobuf · PyPI"
[2]: https://googleapis.dev/python/protobuf/latest/google/protobuf/message.html "google.protobuf.message — Protocol Buffers 4.21.1 documentation"
[3]: https://googleapis.dev/python/protobuf/latest/google/protobuf/internal/containers.html "google.protobuf.internal.containers — Protocol Buffers 4.21.1 documentation"
[4]: https://protobuf.dev/reference/python/python-generated/?utm_source=chatgpt.com "Python Generated Code Guide"
[5]: https://googleapis.dev/python/protobuf/latest/google/protobuf/json_format.html "google.protobuf.json_format — Protocol Buffers 4.21.1 documentation"
[6]: https://googleapis.dev/python/protobuf/latest/google/protobuf/text_format.html "google.protobuf.text_format — Protocol Buffers 4.21.1 documentation"
[7]: https://googleapis.dev/python/protobuf/latest/google/protobuf/descriptor.html "google.protobuf.descriptor — Protocol Buffers 4.21.1 documentation"
[8]: https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/descriptor.proto?utm_source=chatgpt.com "protobuf/src/google/protobuf/descriptor.proto at main"
[9]: https://googleapis.dev/python/protobuf/latest/google/protobuf/descriptor_database.html "google.protobuf.descriptor_database — Protocol Buffers 4.21.1 documentation"
[10]: https://googleapis.dev/python/protobuf/latest/google/protobuf/descriptor_pool.html "google.protobuf.descriptor_pool — Protocol Buffers 4.21.1 documentation"
[11]: https://googleapis.dev/python/protobuf/latest/google/protobuf/reflection.html?utm_source=chatgpt.com "google.protobuf.reflection — Protocol Buffers 4.21. ..."
[12]: https://googleapis.dev/python/protobuf/latest/google/protobuf/message_factory.html "google.protobuf.message_factory — Protocol Buffers 4.21.1 documentation"
[13]: https://googleapis.dev/python/protobuf/latest/google/protobuf/proto_builder.html?utm_source=chatgpt.com "google.protobuf.proto_builder — Protocol Buffers 4.21.1 ..."
[14]: https://googleapis.dev/python/protobuf/latest/google/protobuf/symbol_database.html "google.protobuf.symbol_database — Protocol Buffers 4.21.1 documentation"
[15]: https://googleapis.dev/python/protobuf/latest/google/protobuf/any_pb2.html "google.protobuf.any_pb2 — Protocol Buffers 4.21.1 documentation"
[16]: https://googleapis.dev/python/protobuf/latest/google/protobuf/timestamp_pb2.html "google.protobuf.timestamp_pb2 — Protocol Buffers 4.21.1 documentation"
[17]: https://googleapis.dev/python/protobuf/latest/google/protobuf/duration_pb2.html "google.protobuf.duration_pb2 — Protocol Buffers 4.21.1 documentation"
[18]: https://googleapis.dev/python/protobuf/latest/google/protobuf/field_mask_pb2.html "google.protobuf.field_mask_pb2 — Protocol Buffers 4.21.1 documentation"
[19]: https://googleapis.dev/python/protobuf/latest/google/protobuf/struct_pb2.html "google.protobuf.struct_pb2 — Protocol Buffers 4.21.1 documentation"
[20]: https://googleapis.dev/python/protobuf/latest/google/protobuf/wrappers_pb2.html?utm_source=chatgpt.com "google.protobuf.wrappers_pb2 — Protocol Buffers 4.21.1 ..."

Below is the **I–L expansion** in the same style as D–H: **feature clusters → concrete entrypoints (method/function/option/env var) → footguns/invariants per cluster**.

---

# I) Services / RPC hooks (proto2 “generic services”, deprecated)

### I1) `.proto` switch + generated Python artifacts (when enabled)

**Compiler option**

* `option py_generic_services = true;` (otherwise defaults `false` because generic services are deprecated). ([Protocol Buffers][1])

**Generated code shape (per service `Foo` with rpc `Bar`)**

* Service base class generated: `class Foo(google.protobuf.service.Service)` ([Protocol Buffers][1])
* RPC methods you override in servers:

  * `def Bar(self, rpc_controller, request, done): ...` (method descriptor implied). ([Protocol Buffers][1])
* Stub class generated for clients: `class Foo_Stub(Foo)`

  * Constructor takes an `RpcChannel` and implements RPC methods by calling `channel.CallMethod(...)`. ([Protocol Buffers][1])

**Service interface methods autogenerated on `Foo`**

* `Foo.GetDescriptor() -> ServiceDescriptor` ([Protocol Buffers][1])
* `Foo.CallMethod(method_descriptor, rpc_controller, request, done)` ([Protocol Buffers][1])
* `Foo.GetRequestClass(method_descriptor)` / `Foo.GetResponseClass(method_descriptor)` ([Protocol Buffers][1])

---

### I2) Runtime service interfaces (`google.protobuf.service`) — “least common denominator” RPC ABI

> This module is explicitly **deprecated** and is meant as an abstract interface layer, not a complete RPC system. ([Google Cloud][2])

**Channel**

* `RpcChannel.CallMethod(method_descriptor, rpc_controller, request, response_class, done)` ([Google Cloud][2])

**Controller**

* `RpcController.ErrorText() -> str` ([Google Cloud][2])
* `RpcController.Failed() -> bool` ([Google Cloud][2])
* `RpcController.IsCanceled() -> bool` ([Google Cloud][2])
* `RpcController.NotifyOnCancel(callback) -> None` (at most once) ([Google Cloud][2])
* `RpcController.Reset() -> None` ([Google Cloud][2])
* `RpcController.SetFailed(reason) -> None` ([Google Cloud][2])
* `RpcController.StartCancel() -> None` ([Google Cloud][2])

**Service**

* `Service.CallMethod(method_descriptor, rpc_controller, request, done)`

  * If `done is None`, call is blocking and returns the response; raises `RpcException` on failure. ([Google Cloud][2])
* `Service.GetDescriptor()` ([Google Cloud][2])
* `Service.GetRequestClass(method_descriptor)` ([Google Cloud][2])
* `Service.GetResponseClass(method_descriptor)` ([Google Cloud][2])

**Exception**

* `google.protobuf.service.RpcException` raised for failed **blocking** calls. ([Google Cloud][2])

---

### I3) Service metaclass layer (`google.protobuf.service_reflection`)

These are the metaclasses the generated runtime uses to build service/stub classes from `ServiceDescriptor`s:

* `service_reflection.GeneratedServiceType(name, bases, dictionary)` ([Google Cloud][3])
* `service_reflection.GeneratedServiceStubType(name, bases, dictionary)` ([Google Cloud][3])

(Generated `_pb2.py` modules call internal builder logic that constructs these classes when services exist.) ([GitHub][4])

---

## I) Footguns + invariants (services)

* **Generic services are deprecated + default off**: if you rely on them you’re choosing a legacy ABI. Modern RPC stacks (e.g., gRPC) use protoc plugins to generate RPC-specific code. ([Protocol Buffers][1])
* **Protobuf does not ship an RPC implementation**: you must supply `RpcChannel` and `RpcController` implementations (or use a separate RPC ecosystem). ([Protocol Buffers][1])
* **Schema-driven call path**: `Service.CallMethod` preconditions include “method_descriptor belongs to the service” and “request is of the expected message class.” ([Google Cloud][2])

---

# J) Protoc plugins + generator insertion points (extensibility plane)

### J1) Python codegen insertion points exposed by the standard generator

Python generator allows third-party plugins to insert code at:

* `imports` (import statements)
* `module_scope` (top-level declarations) ([Protocol Buffers][1])

In generated Python files you will see markers like:

* `@@protoc_insertion_point(imports)` (commonly present near imports) ([Chromium Git Repositories][5])

---

### J2) Protoc plugin protocol (the “wire contract” between `protoc` and plugins)

**How protoc discovers plugins**

* Plugin must be named `protoc-gen-$NAME`, and is invoked when `--${NAME}_out=...` is passed. ([Pigweed][6])

**I/O contract**

* Plugin reads a serialized `CodeGeneratorRequest` from **stdin** and writes a serialized `CodeGeneratorResponse` to **stdout**. ([Protocol Buffers][7])

**Request message: `google.protobuf.compiler.CodeGeneratorRequest`**

* `file_to_generate: repeated string` — only generate for these files. ([Pigweed][6])
* `parameter: optional string` — raw parameter string passed on CLI. ([Pigweed][6])
* `proto_file: repeated FileDescriptorProto` — includes all transitive imports; in topological order. ([Pigweed][6])
* `source_file_descriptors: repeated FileDescriptorProto` — “all options” descriptors for files_to_generate. ([Pigweed][6])
* `compiler_version: optional Version` — major/minor/patch/suffix. ([Pigweed][6])

**Response message: `google.protobuf.compiler.CodeGeneratorResponse`**

* `error: optional string` — if non-empty, generation failed **but plugin should still exit 0**. ([Pigweed][6])
* `supported_features: optional uint64` — bitmask (see Feature enum). ([Pigweed][6])
* `minimum_edition / maximum_edition: optional int32` (effective only with editions feature flag). ([Pigweed][6])
* `file: repeated CodeGeneratorResponse.File` — generated output files. ([Pigweed][6])

---

### J3) CodeGeneratorResponse.Feature flags (capability negotiation)

* `FEATURE_PROTO3_OPTIONAL = 1` ([Pigweed][6])
* `FEATURE_SUPPORTS_EDITIONS = 2` ([Pigweed][6])

---

### J4) `CodeGeneratorResponse.File` (file emission + insertion semantics)

Fields:

* `name: optional string` — relative path, must not contain `.`/`..`, must not be absolute; `/` is separator. ([Pigweed][6])
* `insertion_point: optional string` — if set, inserts into an existing generated file at `@@protoc_insertion_point(NAME)` marker; requires `name` present. ([Pigweed][6])
* `content: optional string` — file contents (or inserted snippet). ([Pigweed][6])
* `generated_code_info: optional GeneratedCodeInfo` — metadata for tooling; offsetting works with insertion. ([Pigweed][6])

Insertion point invariants:

* Inserted text goes **immediately above** the marker line; multiple insertions preserve the order they were added. ([Pigweed][6])
* Leading whitespace on the insertion point line is copied onto each inserted line (Python indentation support). ([Pigweed][6])
* Code generators run in **command-line order**, and insertions must occur in the same `protoc` invocation as the original file generation. ([Pigweed][6])

---

### J5) Minimal plugin “call surface” (Python perspective)

If you have generated/bundled the plugin protocol messages as `plugin_pb2` (from `plugin.proto`), the operational calls look like:

```python
# req = plugin_pb2.CodeGeneratorRequest.FromString(sys.stdin.buffer.read())
# resp = plugin_pb2.CodeGeneratorResponse()
# resp.file.add(name="...", content="...")
# sys.stdout.buffer.write(resp.SerializeToString())
```

Everything above is mechanically implied by the stdin/stdout contract and the request/response schema. ([Protocol Buffers][7])

---

## J) Footguns + invariants (plugins + insertion points)

* **Use `response.error`, not non-zero exit, for “user/schema” errors**; reserve non-zero exit for protoc/plugin protocol failures (unparseable request, crashes). ([Pigweed][6])
* **Generate only for `file_to_generate`**; `proto_file` contains much more (imports + topological order). ([Pigweed][6])
* **Insertion requires name + same protoc invocation**; indentation behavior is deliberate—treat it as contract. ([Pigweed][6])
* **File path safety**: `name` must be relative and cannot escape the output dir. ([Pigweed][6])

---

# K) Runtime backends + performance knobs (upb/cpp/python)

### K0) Current packaging reality (why this matters)

The PyPI `protobuf` package is currently in the **6.x** line (e.g., 6.33.2 released Dec 6, 2025). ([PyPI][8])
The major runtime shift to the **upb-based** Python implementation began with Python API 4.21.0. ([Protocol Buffers][9])

---

### K1) Backend selection (`PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION`)

Runtime chooses an implementation at import time:

* Tries `upb` if `google._upb._message` is importable
* Else tries `cpp` if `google.protobuf.pyext._message` is importable
* Else falls back to pure `python` ([Chromium Git Repositories][10])

Override with environment variable:

* `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION = 'python' | 'cpp' | 'upb'`
* Any other value raises `ValueError` (and on PyPy, `cpp` falls back to `python`). ([Chromium Git Repositories][10])

**Call surface (process-level knob)**

```python
import os
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"  # must be set before importing google.protobuf
```

(Last clause is an inference from the fact the env var is read during module import.) ([Chromium Git Repositories][10])

---

### K2) Backend introspection (discouraged but available)

* `google.protobuf.internal.api_implementation.Type() -> str` returns `'upb'|'cpp'|'python'` ([Chromium Git Repositories][10])
  The module explicitly warns that clients shouldn’t care and there’s no guarantee differences remain stable. ([Chromium Git Repositories][10])

---

### K3) Behavioral constraints & migration facts you must treat as “ops invariants”

From the Python-upb migration notes:

* Python 4.21.0 is upb-based, faster parsing, includes Apple silicon binaries. ([Protocol Buffers][9])
* “Sharing messages between Python and C++” breaks; an env-var workaround exists for affected users. ([Protocol Buffers][9])
* upb requires generated code produced by `protoc >= 3.19.0`. ([Protocol Buffers][9])

---

## K) Footguns + invariants (runtime backends)

* **Backend is not part of your app’s stable contract**: treat it like an internal performance detail unless you *explicitly* test and pin it. ([Chromium Git Repositories][10])
* **Toolchain coupling**: if you upgrade the runtime, you may need to regenerate `_pb2.py` with a newer protoc (notably `>= 3.19.0` for upb). ([Protocol Buffers][9])

---

# L) Contract-hardening checklist (production-grade determinism, drift resistance, golden testing)

This is the “how to make protobuf a stable contract surface” bundle—each item points to the knob/API that enforces it.

---

## L1) Pin + assert the toolchain (runtime + protoc + generated code)

* Record and enforce:

  * Runtime package version (e.g., `protobuf==…`) — current latest is shown on PyPI. ([PyPI][8])
  * Protoc version (critical because upb requires `protoc >= 3.19.0`-generated code). ([Protocol Buffers][9])
* If you author protoc plugins, the request contains:

  * `CodeGeneratorRequest.compiler_version` (major/minor/patch/suffix) — use it to stamp generated artifacts / metadata for reproducibility. ([Pigweed][6])

**Hard rule**: runtime upgrades that cross major boundaries (4→5→6) should trigger “regen all protos + rerun golden tests.” ([Protocol Buffers][9])

---

## L2) Never treat wire bytes as canonical identity

* Protobuf explicitly documents that **serialization is not canonical**, and that using serialized bytes for equality/hashes/checksums can fail. ([Protocol Buffers][11])
* If you still need stable bytes for *intra-build test snapshots*, use:

  * `msg.SerializeToString(deterministic=True)` (predictable map-key ordering) ([Google Cloud][12])
    …but treat it as **best-effort determinism**, not a cross-version/cross-language canonical form. ([Protocol Buffers][11])

---

## L3) Define a canonical debug/snapshot representation (JSON or text) and freeze the knobs

### Canonical JSON snapshot recipe knobs

Use `google.protobuf.json_format.MessageToJson` / `MessageToDict` with a fixed policy:

* `preserving_proto_field_name=True` (stabilize field names)
* `including_default_value_fields=True` (optional; stabilizes “unset vs default” in output)
* `sort_keys=True` (stable map/object ordering in JSON text)
* optionally `use_integers_for_enums=True` to avoid enum name drift

### Canonical textproto snapshot recipe knobs

Use `google.protobuf.text_format.MessageToString` with a fixed policy:

* consider `as_one_line=True` for compact diffs
* configure float formatting to avoid spurious diffs (docs recommend `.17g` for doubles)

### Parsing invariant (avoid merge surprises)

Both JSON and text parsing APIs merge into the provided message; for text this is explicitly called out. If you want overwrite semantics:

* `msg.Clear(); json_format.Parse(text, msg, ...)`
* `msg.Clear(); text_format.Parse(text, msg, ...)`

---

## L4) Presence discipline (proto3 defaults are a trap unless you choose explicit presence)

* Protobuf presence comes in **implicit vs explicit** forms; recommended practice is to use `optional` for proto3 basic types to get explicit presence. ([Protocol Buffers][13])
* Python generated code guide: implicit-presence singular scalars **do not have** `HasField()`. ([Protocol Buffers][14])

**Contract rule**: if “unset vs set-to-default” matters anywhere in your system, enforce explicit presence (proto3 `optional`, oneof, or wrapper types) and test for it with `HasField()` / `WhichOneof()`. ([Protocol Buffers][14])

---

## L5) Unknown fields policy (forward compat vs normalization)

Decide intentionally:

* Preserve unknowns for forward compatibility and roundtripping (`UnknownFields()`), or
* Normalize by stripping them: `msg.DiscardUnknownFields()` before snapshotting/serving. ([Protocol Buffers][9])

The upb migration notes call out unknown-field API changes (explicit `UnknownFieldSet`). Treat unknown handling as part of your compatibility contract. ([Protocol Buffers][9])

---

## L6) Partial-update semantics: FieldMask as the contract primitive

If you use “patch/update” APIs, enforce:

* canonical masks before caching/diffing (FieldMask canonicalization exists)
* apply updates via `FieldMask.MergeMessage(source, destination, replace_message_field=..., replace_repeated_field=...)`

---

## L7) Dynamic schema ingestion: isolate descriptor pools; treat FQNs as keys

If you load descriptors dynamically (DescriptorSet / plugin workflows):

* Use a dedicated `DescriptorPool()` per logical schema universe to avoid collisions
* Use fully-qualified names as stable identifiers (plugin request notes type names are fully qualified in descriptors) ([Pigweed][6])

---

## L8) Protoc plugin contract checklist (if you generate code or metadata)

* Generate only for `file_to_generate`. ([Pigweed][6])
* Treat `proto_file` order as topological; don’t assume file-system order. ([Pigweed][6])
* Use `CodeGeneratorResponse.error` for schema-level failures (exit 0). ([Pigweed][6])
* Enforce path safety constraints on `CodeGeneratorResponse.File.name`. ([Pigweed][6])
* If extending generated output, use insertion points + indentation semantics as specified. ([Pigweed][6])

---

## L9) Backend neutrality: don’t couple correctness to `upb/cpp/python`

* Introspection API exists (`api_implementation.Type()`), but the runtime warns against relying on backend differences. ([Chromium Git Repositories][10])
  **Practical rule**: run your golden tests under your production backend choice; if you must support multiple backends, run CI in a matrix and treat differences as bugs unless explicitly tolerated.

---

If you want one more increment after this: I can convert **L** into a **repo-ready “protobuf contract harness”** (pytest-style) that bakes in: version assertions, canonical JSON/text snapshot utilities, deterministic-bytes helper, and a “presence/unknowns policy” validator that fails fast when schemas drift.

[1]: https://protobuf.dev/reference/python/python-generated/ "Python Generated Code Guide | Protocol Buffers Documentation"
[2]: https://googleapis.dev/python/protobuf/latest/google/protobuf/service.html "google.protobuf.service — Protocol Buffers 4.21.1 documentation"
[3]: https://googleapis.dev/python/protobuf/latest/google/protobuf/service_reflection.html "google.protobuf.service_reflection — Protocol Buffers 4.21.1 documentation"
[4]: https://raw.githubusercontent.com/protocolbuffers/protobuf/main/python/google/protobuf/internal/builder.py "raw.githubusercontent.com"
[5]: https://chromium.googlesource.com/chromium/dom-distiller/dist/%2B/refs/heads/main/python/plugin_pb2.py?utm_source=chatgpt.com "python/plugin_pb2.py - chromium/dom-distiller/dist"
[6]: https://pigweed.googlesource.com/third_party/github/protocolbuffers/protobuf/%2B/refs/heads/upstream/main-tmp-2/src/google/protobuf/compiler/plugin.proto "src/google/protobuf/compiler/plugin.proto - third_party/github/protocolbuffers/protobuf - Git at Google"
[7]: https://protobuf.dev/reference/other/ "Other Languages | Protocol Buffers Documentation"
[8]: https://pypi.org/project/protobuf/ "protobuf · PyPI"
[9]: https://protobuf.dev/news/2022-05-06/ "Changes announced May 6, 2022 | Protocol Buffers Documentation"
[10]: https://chromium.googlesource.com/chromium/src/third_party/%2B/master/protobuf/python/google/protobuf/internal/api_implementation.py "protobuf/python/google/protobuf/internal/api_implementation.py - chromium/src/third_party - Git at Google"
[11]: https://protobuf.dev/programming-guides/serialization-not-canonical/?utm_source=chatgpt.com "Proto Serialization Is Not Canonical"
[12]: https://googleapis.dev/python/protobuf/latest/google/protobuf/message.html?utm_source=chatgpt.com "google.protobuf.message — Protocol Buffers 4.21.1 ..."
[13]: https://protobuf.dev/programming-guides/field_presence/?utm_source=chatgpt.com "Application Note: Field Presence"
[14]: https://protobuf.dev/reference/python/python-generated/?utm_source=chatgpt.com "Python Generated Code Guide"

Below is a **repo-ready `pytest` harness** you can drop into `tests/protobuf/` that enforces a **stable Protobuf contract surface**:

* **Version assertions** (runtime `protobuf`, active backend, optional `protoc` min)
* **Canonical JSON / textproto snapshot utilities** (with version-adaptive knobs)
* **Deterministic-bytes helper** (`SerializeToString(deterministic=True)`)
* **Schema drift guard** (stable descriptor-shape fingerprint snapshots)
* **Presence + unknowns policy validator** that **fails fast** (and works even when `UnknownFields()` isn’t implemented by the active backend by using a wire-roundtrip probe)

You’ll edit a single target file (`contract_targets.py`) to point at your `*_pb2.py` modules and any fields you require explicit presence for.

---

## 1) Directory layout

Create:

```
tests/protobuf/
  contract_targets.py
  contract_config.py
  contract_harness.py
  golden/
    schema_fingerprints.json          # generated on first UPDATE_GOLDENS=1 run
    schema_shapes.json                # optional (for diff-friendly debugging)
  test_contract_versions.py
  test_contract_schema.py
  test_contract_presence_unknowns.py
```

---

## 2) Configuration target file (you edit this)

### `tests/protobuf/contract_targets.py`

```python
"""
Edit this file to point the contract harness at your generated protobuf modules.

You can either:
  - enumerate modules explicitly in PROTO_MODULE_IMPORTS, or
  - specify package roots in PROTO_PACKAGE_ROOTS to auto-discover *_pb2 modules.
"""

from __future__ import annotations

# Option A: explicit module import strings (recommended; fastest & deterministic)
PROTO_MODULE_IMPORTS: list[str] = [
    # "myproj.protos.foo_pb2",
    # "myproj.protos.bar_pb2",
]

# Option B: auto-discover *_pb2 modules under these package roots (slower)
PROTO_PACKAGE_ROOTS: list[str] = [
    # "myproj.protos",
]

# Fields that MUST support explicit presence in Python (proto3 optional / oneof / proto2).
# Key: fully-qualified message name (e.g., "my.pkg.MyMessage")
# Value: list of field names on that message.
EXPLICIT_PRESENCE_FIELDS: dict[str, list[str]] = {
    # "my.pkg.MyMessage": ["field_a", "field_b"],
}

# Unknown-field normalization policy at your "contract boundary" (snapshots/serving).
# - "preserve": keep unknown fields (forward-compat; roundtrip retains unknown bytes)
# - "strip":    drop unknown fields before snapshot/serve via DiscardUnknownFields()
UNKNOWN_FIELD_POLICY: str = "strip"
```

---

## 3) Contract config + env knobs

### `tests/protobuf/contract_config.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
import os


UnknownPolicy = Literal["preserve", "strip"]
Backend = Literal["upb", "python", "cpp"]


@dataclass(frozen=True)
class ContractConfig:
    # If set, assert runtime protobuf version equals this string (e.g. "6.33.2")
    expected_runtime_version: str | None = None

    # If set, assert runtime protobuf version starts with this prefix (e.g. "6.")
    expected_runtime_version_prefix: str | None = "6."

    # If set, assert active backend matches (upb/python/cpp)
    expected_backend: Backend | None = None

    # If set, assert protoc exists and version >= this (major, minor, patch)
    expected_protoc_min: tuple[int, int, int] | None = (3, 19, 0)

    # Contract boundary unknown-field policy
    unknown_field_policy: UnknownPolicy = "strip"

    # Goldens update switch (set to "1" to rewrite golden files)
    update_goldens_env: str = "UPDATE_GOLDENS"

    @property
    def update_goldens(self) -> bool:
        return os.getenv(self.update_goldens_env, "").strip() in {"1", "true", "TRUE", "yes", "YES"}

    @staticmethod
    def from_env() -> "ContractConfig":
        # Keep intentionally minimal: most users pin runtime via requirements/lockfile.
        expected_backend = os.getenv("PROTOBUF_CONTRACT_BACKEND")
        if expected_backend is not None:
            expected_backend = expected_backend.strip() or None

        expected_runtime_version = os.getenv("PROTOBUF_CONTRACT_VERSION")
        if expected_runtime_version is not None:
            expected_runtime_version = expected_runtime_version.strip() or None

        expected_runtime_prefix = os.getenv("PROTOBUF_CONTRACT_VERSION_PREFIX")
        if expected_runtime_prefix is not None:
            expected_runtime_prefix = expected_runtime_prefix.strip() or None

        unknown_policy = os.getenv("PROTOBUF_CONTRACT_UNKNOWN_POLICY")
        if unknown_policy is not None:
            unknown_policy = unknown_policy.strip().lower()
            if unknown_policy not in {"preserve", "strip"}:
                raise ValueError(f"Invalid PROTOBUF_CONTRACT_UNKNOWN_POLICY={unknown_policy!r}")

        protoc_min = os.getenv("PROTOBUF_CONTRACT_PROTOC_MIN")  # "3.19.0"
        protoc_min_parsed: tuple[int, int, int] | None = (3, 19, 0)
        if protoc_min is not None:
            s = protoc_min.strip()
            if s == "":
                protoc_min_parsed = None
            else:
                parts = s.split(".")
                if len(parts) < 2:
                    raise ValueError("PROTOBUF_CONTRACT_PROTOC_MIN must look like '3.19.0'")
                nums = [int(p) for p in parts[:3]]
                while len(nums) < 3:
                    nums.append(0)
                protoc_min_parsed = (nums[0], nums[1], nums[2])

        return ContractConfig(
            expected_runtime_version=expected_runtime_version,
            expected_runtime_version_prefix=expected_runtime_prefix if expected_runtime_prefix is not None else "6.",
            expected_backend=expected_backend,  # type: ignore[arg-type]
            expected_protoc_min=protoc_min_parsed,
            unknown_field_policy=(unknown_policy or "strip"),  # type: ignore[arg-type]
        )
```

---

## 4) Core harness utilities (snapshots, fingerprints, probes)

### `tests/protobuf/contract_harness.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Iterator, Literal, cast
import importlib
import inspect
import json
import pkgutil
import re
import subprocess

from google.protobuf.message import Message
from google.protobuf.descriptor import Descriptor, FieldDescriptor, FileDescriptor, EnumDescriptor
from google.protobuf import json_format, text_format
from google.protobuf.internal import api_implementation


UnknownPolicy = Literal["preserve", "strip"]


# ----------------------------
# Module discovery / loading
# ----------------------------

def load_pb2_modules(
    module_imports: list[str],
    package_roots: list[str],
) -> list[ModuleType]:
    modules: list[ModuleType] = []

    for mod in module_imports:
        modules.append(importlib.import_module(mod))

    for root in package_roots:
        pkg = importlib.import_module(root)
        if not hasattr(pkg, "__path__"):
            continue
        for m in _walk_pb2_modules(pkg):
            modules.append(m)

    # De-dupe by module name, keep stable order
    seen: set[str] = set()
    uniq: list[ModuleType] = []
    for m in modules:
        if m.__name__ in seen:
            continue
        seen.add(m.__name__)
        uniq.append(m)
    return uniq


def _walk_pb2_modules(pkg: ModuleType) -> Iterator[ModuleType]:
    prefix = pkg.__name__ + "."
    for _, name, _ in pkgutil.walk_packages(pkg.__path__, prefix):
        if name.endswith("_pb2"):
            yield importlib.import_module(name)


# ----------------------------
# Runtime / toolchain checks
# ----------------------------

def protobuf_runtime_version() -> str:
    import google.protobuf  # local import to keep dependency surface explicit
    return cast(str, google.protobuf.__version__)


def protobuf_backend() -> str:
    # internal, but useful as an operational invariant
    return cast(str, api_implementation.Type())


def protoc_version() -> tuple[int, int, int] | None:
    """
    Returns (major, minor, patch) from `protoc --version`, or None if protoc not found.
    Typical output: "libprotoc 3.21.12" or "libprotoc 25.1".
    """
    try:
        cp = subprocess.run(
            ["protoc", "--version"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        return None
    except subprocess.CalledProcessError as e:
        raise AssertionError(f"protoc failed: {e.stderr or e.stdout}") from e

    m = re.search(r"\blibprotoc\s+([0-9]+)\.([0-9]+)(?:\.([0-9]+))?", cp.stdout.strip())
    if not m:
        raise AssertionError(f"Unrecognized protoc version output: {cp.stdout!r}")
    major = int(m.group(1))
    minor = int(m.group(2))
    patch = int(m.group(3) or 0)
    return (major, minor, patch)


def assert_version_tuple_gte(actual: tuple[int, int, int], minimum: tuple[int, int, int]) -> None:
    if actual < minimum:
        raise AssertionError(f"Expected protoc >= {minimum}, got {actual}")


# ----------------------------
# Canonical representations
# ----------------------------

def canonical_json(
    message: Message,
    *,
    preserving_proto_field_name: bool = True,
    always_print_fields_with_no_presence: bool = True,
    use_integers_for_enums: bool = False,
    float_precision: int | None = None,
    indent: int = 2,
) -> str:
    """
    Version-adaptive canonical JSON for snapshotting/diffs.
    - Uses MessageToDict then json.dumps(sort_keys=True).
    - Adapts to protobuf versions where MessageToDict uses
      `including_default_value_fields` (older) vs
      `always_print_fields_with_no_presence` (newer).
    """
    d = _message_to_dict(
        message,
        preserving_proto_field_name=preserving_proto_field_name,
        always_print_fields_with_no_presence=always_print_fields_with_no_presence,
        use_integers_for_enums=use_integers_for_enums,
        float_precision=float_precision,
    )
    return json.dumps(d, sort_keys=True, indent=indent, ensure_ascii=True)


def canonical_textproto(
    message: Message,
    *,
    as_one_line: bool = False,
    use_short_repeated_primitives: bool = True,
    use_index_order: bool = False,
    double_format: str = ".17g",
    float_format: str | None = None,
    print_unknown_fields: bool = False,
) -> str:
    """
    Canonical-ish textproto (for debugging and optional snapshots).
    Note: textproto is not a formal contract format; treat it as a diff aid.
    """
    return text_format.MessageToString(
        message,
        as_utf8=True,
        as_one_line=as_one_line,
        use_short_repeated_primitives=use_short_repeated_primitives,
        pointy_brackets=False,
        use_index_order=use_index_order,
        float_format=float_format,
        double_format=double_format,
        use_field_number=False,
        descriptor_pool=None,
        indent=0,
        message_formatter=None,
        print_unknown_fields=print_unknown_fields,
        force_colon=False,
    )


def deterministic_wire(message: Message) -> bytes:
    # Deterministic only guarantees predictable map-key ordering.
    return message.SerializeToString(deterministic=True)


def _message_to_dict(
    message: Message,
    *,
    preserving_proto_field_name: bool,
    always_print_fields_with_no_presence: bool,
    use_integers_for_enums: bool,
    float_precision: int | None,
) -> dict[str, Any]:
    fn = json_format.MessageToDict
    sig = inspect.signature(fn)
    kwargs: dict[str, Any] = {}

    # Newer protobuf
    if "always_print_fields_with_no_presence" in sig.parameters:
        kwargs["always_print_fields_with_no_presence"] = always_print_fields_with_no_presence
    # Older protobuf
    if "including_default_value_fields" in sig.parameters:
        kwargs["including_default_value_fields"] = always_print_fields_with_no_presence

    if "preserving_proto_field_name" in sig.parameters:
        kwargs["preserving_proto_field_name"] = preserving_proto_field_name
    if "use_integers_for_enums" in sig.parameters:
        kwargs["use_integers_for_enums"] = use_integers_for_enums
    if "float_precision" in sig.parameters:
        kwargs["float_precision"] = float_precision

    # descriptor_pool exists on most modern versions; harmless if absent
    if "descriptor_pool" in sig.parameters:
        kwargs["descriptor_pool"] = None

    return cast(dict[str, Any], fn(message, **kwargs))


# ----------------------------
# Golden snapshot I/O
# ----------------------------

def golden_dir() -> Path:
    return Path(__file__).resolve().parent / "golden"


def assert_golden_text(path: Path, content: str, *, update: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if update or not path.exists():
        path.write_text(content, encoding="utf-8")
        return
    expected = path.read_text(encoding="utf-8")
    if expected != content:
        raise AssertionError(
            f"Golden mismatch: {path}\n"
            f"Run with UPDATE_GOLDENS=1 to update snapshots."
        )


def assert_golden_json(path: Path, obj: Any, *, update: bool) -> None:
    content = json.dumps(obj, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    assert_golden_text(path, content, update=update)


# ----------------------------
# Schema fingerprinting (drift guard)
# ----------------------------

@dataclass(frozen=True)
class MessageShape:
    full_name: str
    # normalized schema structure
    shape: dict[str, Any]
    sha256: str


def schema_fingerprints(mods: Iterable[ModuleType]) -> tuple[dict[str, str], dict[str, Any]]:
    """
    Returns:
      - fingerprints: { "pkg.Message": "sha256..." }
      - shapes:       { "pkg.Message": {...shape...} }  (diff-friendly)
    """
    fingerprints: dict[str, str] = {}
    shapes: dict[str, Any] = {}

    for m in mods:
        fd = cast(FileDescriptor, getattr(m, "DESCRIPTOR"))
        for desc in _iter_message_descriptors(fd):
            ms = message_shape(desc)
            fingerprints[ms.full_name] = ms.sha256
            shapes[ms.full_name] = ms.shape

    # stable ordering in outputs
    fingerprints = {k: fingerprints[k] for k in sorted(fingerprints)}
    shapes = {k: shapes[k] for k in sorted(shapes)}
    return fingerprints, shapes


def _iter_message_descriptors(file_desc: FileDescriptor) -> Iterator[Descriptor]:
    for _, top in sorted(file_desc.message_types_by_name.items(), key=lambda kv: kv[0]):
        yield top
        yield from _iter_nested(top)


def _iter_nested(desc: Descriptor) -> Iterator[Descriptor]:
    for nested in sorted(desc.nested_types, key=lambda d: d.full_name):
        yield nested
        yield from _iter_nested(nested)


def message_shape(desc: Descriptor) -> MessageShape:
    shape = _normalize_descriptor(desc)
    payload = json.dumps(shape, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    h = sha256(payload).hexdigest()
    return MessageShape(full_name=desc.full_name, shape=shape, sha256=h)


def _normalize_descriptor(desc: Descriptor) -> dict[str, Any]:
    out: dict[str, Any] = {
        "full_name": desc.full_name,
        "name": desc.name,
        "file": {
            "name": desc.file.name,
            "package": desc.file.package,
            "syntax": getattr(desc.file, "syntax", None),
        },
        "oneofs": [],
        "fields": [],
        "enums": [],
        "nested_types": [],
    }

    # oneofs
    for o in sorted(desc.oneofs, key=lambda o: o.name):
        out["oneofs"].append({
            "name": o.name,
            "fields": [f.name for f in sorted(o.fields, key=lambda f: f.number)],
        })

    # fields
    fields_sorted = sorted(desc.fields, key=lambda f: f.number)
    for f in fields_sorted:
        out["fields"].append(_normalize_field(f))

    # enums
    for e in sorted(desc.enum_types, key=lambda e: e.full_name):
        out["enums"].append(_normalize_enum(e))

    # nested types (names only; their own shapes appear independently via traversal)
    out["nested_types"] = [t.full_name for t in sorted(desc.nested_types, key=lambda t: t.full_name)]
    return out


def _normalize_field(f: FieldDescriptor) -> dict[str, Any]:
    # Map fields are represented as repeated message fields with map_entry option on the entry type.
    is_map = bool(f.message_type and f.message_type.GetOptions().map_entry)

    msg_type = f.message_type.full_name if f.message_type else None
    enum_type = f.enum_type.full_name if f.enum_type else None
    oneof = f.containing_oneof.name if f.containing_oneof else None

    # proto3 optional shows up on FieldDescriptor in modern versions
    proto3_optional = getattr(f, "proto3_optional", False)

    field_dict: dict[str, Any] = {
        "name": f.name,
        "number": f.number,
        "label": int(f.label),
        "type": int(f.type),
        "cpp_type": int(f.cpp_type),
        "json_name": getattr(f, "json_name", None),
        "is_extension": bool(f.is_extension),
        "is_map": is_map,
        "message_type": msg_type,
        "enum_type": enum_type,
        "oneof": oneof,
        "proto3_optional": bool(proto3_optional),
    }

    if is_map and f.message_type:
        # entry has fields "key" and "value"
        kf = f.message_type.fields_by_name.get("key")
        vf = f.message_type.fields_by_name.get("value")
        field_dict["map_key"] = _normalize_map_elem(kf) if kf else None
        field_dict["map_value"] = _normalize_map_elem(vf) if vf else None

    # Options: only include non-default fields to reduce noise
    opts = f.GetOptions()
    opt_fields = opts.ListFields()
    if opt_fields:
        field_dict["options"] = {fd.name: v for fd, v in opt_fields}
    return field_dict


def _normalize_map_elem(f: FieldDescriptor) -> dict[str, Any]:
    return {
        "type": int(f.type),
        "message_type": f.message_type.full_name if f.message_type else None,
        "enum_type": f.enum_type.full_name if f.enum_type else None,
    }


def _normalize_enum(e: EnumDescriptor) -> dict[str, Any]:
    return {
        "full_name": e.full_name,
        "name": e.name,
        "values": [{"name": v.name, "number": v.number} for v in sorted(e.values, key=lambda v: v.number)],
        "options": {fd.name: v for fd, v in e.GetOptions().ListFields()} if e.GetOptions().ListFields() else {},
    }


# ----------------------------
# Presence checks (explicit presence required fields)
# ----------------------------

def assert_explicit_presence(message_cls: type[Message], field_name: str) -> None:
    desc = message_cls.DESCRIPTOR
    if field_name not in desc.fields_by_name:
        raise AssertionError(f"{desc.full_name}: no such field {field_name!r}")

    fd = desc.fields_by_name[field_name]

    if fd.label == FieldDescriptor.LABEL_REPEATED:
        raise AssertionError(f"{desc.full_name}.{field_name}: repeated/map fields do not have HasField presence")

    if fd.message_type and fd.message_type.GetOptions().map_entry:
        raise AssertionError(f"{desc.full_name}.{field_name}: map fields do not have HasField presence")

    msg = message_cls()

    # Must not raise ValueError (implicit-presence scalars in proto3 do)
    try:
        before = msg.HasField(field_name)
    except ValueError as e:
        raise AssertionError(
            f"{desc.full_name}.{field_name}: does not support explicit presence in Python.\n"
            f"Likely proto3 implicit-presence scalar; prefer proto3 'optional', oneof, wrapper, or proto2.\n"
            f"Underlying error: {e}"
        ) from e

    if before is not False:
        raise AssertionError(f"{desc.full_name}.{field_name}: expected HasField False on new instance")

    _set_field_for_presence(msg, fd)

    after = msg.HasField(field_name)
    if after is not True:
        raise AssertionError(f"{desc.full_name}.{field_name}: expected HasField True after setting")

    msg.ClearField(field_name)
    cleared = msg.HasField(field_name)
    if cleared is not False:
        raise AssertionError(f"{desc.full_name}.{field_name}: expected HasField False after ClearField")


def _set_field_for_presence(msg: Message, fd: FieldDescriptor) -> None:
    name = fd.name
    if fd.cpp_type == FieldDescriptor.CPPTYPE_MESSAGE:
        # For submessage presence, mutate it in a minimal way.
        # SetInParent exists but is considered a smell; for tests, we mutate via SetInParent
        # because it is the only guaranteed way to mark an empty submessage present.
        sub = getattr(msg, name)
        sub.SetInParent()
        return

    # scalars/enums: set to a non-default sample
    setattr(msg, name, _sample_scalar_value(fd))


def _sample_scalar_value(fd: FieldDescriptor) -> Any:
    t = fd.type
    if t in (
        FieldDescriptor.TYPE_INT32,
        FieldDescriptor.TYPE_SINT32,
        FieldDescriptor.TYPE_SFIXED32,
        FieldDescriptor.TYPE_UINT32,
        FieldDescriptor.TYPE_FIXED32,
        FieldDescriptor.TYPE_INT64,
        FieldDescriptor.TYPE_SINT64,
        FieldDescriptor.TYPE_SFIXED64,
        FieldDescriptor.TYPE_UINT64,
        FieldDescriptor.TYPE_FIXED64,
    ):
        return 1
    if t in (FieldDescriptor.TYPE_FLOAT, FieldDescriptor.TYPE_DOUBLE):
        return 1.0
    if t == FieldDescriptor.TYPE_BOOL:
        return True
    if t == FieldDescriptor.TYPE_STRING:
        return "x"
    if t == FieldDescriptor.TYPE_BYTES:
        return b"x"
    if t == FieldDescriptor.TYPE_ENUM:
        # choose first non-zero if available, else first
        values = list(fd.enum_type.values) if fd.enum_type else []
        if not values:
            return 0
        for v in values:
            if v.number != 0:
                return v.number
        return values[0].number
    raise AssertionError(f"Unsupported scalar type for presence sample: {t}")


# ----------------------------
# Unknown fields policy (wire-probe based)
# ----------------------------

def normalize_for_contract(msg: Message, unknown_policy: UnknownPolicy) -> Message:
    """
    Apply contract-boundary normalization:
      - strip unknown fields if policy == "strip"
    Extend this function over time (FieldMask canonicalization, etc.).
    """
    if unknown_policy == "strip":
        msg.DiscardUnknownFields()
    return msg


def assert_unknown_field_policy_roundtrip(
    message_cls: type[Message],
    unknown_policy: UnknownPolicy,
) -> None:
    """
    Probes unknown-field behavior without relying on UnknownFields() (which may be unimplemented).
    Strategy:
      1) serialize empty msg
      2) append an unknown varint field (with an unused field number)
      3) parse
      4) serialize and assert unknown bytes present (baseline retention)
      5) normalize_for_contract and assert presence/absence per policy
    """
    desc = message_cls.DESCRIPTOR
    field_no = _pick_unused_field_number(desc)
    unknown_blob = _encode_unknown_varint(field_no, 123)

    m = message_cls()
    base = m.SerializeToString()
    injected = base + unknown_blob

    m2 = message_cls()
    m2.ParseFromString(injected)

    # Baseline: unknowns should survive parse->serialize in typical protobuf runtimes.
    out_before = m2.SerializeToString()
    if unknown_blob not in out_before:
        # If your runtime drops unknowns, you still get a meaningful "strip" policy, but
        # forward-compat roundtrip won't be possible. Treat as a hard failure by default.
        raise AssertionError(
            f"{desc.full_name}: unknown fields did not survive parse->serialize.\n"
            f"Expected injected unknown bytes to be present in output."
        )

    normalize_for_contract(m2, unknown_policy)
    out_after = m2.SerializeToString()

    if unknown_policy == "preserve":
        if unknown_blob not in out_after:
            raise AssertionError(f"{desc.full_name}: policy=preserve but unknown bytes were removed")
    else:  # strip
        if unknown_blob in out_after:
            raise AssertionError(f"{desc.full_name}: policy=strip but unknown bytes still present")


def _pick_unused_field_number(desc: Descriptor) -> int:
    used = set(desc.fields_by_number.keys())
    # Choose a small-ish number to keep varint tag bytes stable, but unlikely to collide.
    for candidate in range(1000, 20000):
        if candidate not in used:
            return candidate
    raise AssertionError(f"{desc.full_name}: unable to find unused field number for unknown-field probe")


def _encode_unknown_varint(field_number: int, value: int) -> bytes:
    # tag = (field_number << 3) | wire_type(0)
    tag = (field_number << 3) | 0
    return _encode_varint(tag) + _encode_varint(value)


def _encode_varint(x: int) -> bytes:
    if x < 0:
        raise ValueError("varint cannot encode negative numbers")
    out = bytearray()
    while True:
        b = x & 0x7F
        x >>= 7
        if x:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)
```

---

## 5) Tests

### `tests/protobuf/test_contract_versions.py`

```python
from __future__ import annotations

import pytest

from .contract_config import ContractConfig
from .contract_harness import (
    protobuf_runtime_version,
    protobuf_backend,
    protoc_version,
    assert_version_tuple_gte,
)


def test_protobuf_runtime_version_contract() -> None:
    cfg = ContractConfig.from_env()
    v = protobuf_runtime_version()

    if cfg.expected_runtime_version is not None:
        assert v == cfg.expected_runtime_version
    if cfg.expected_runtime_version_prefix is not None:
        assert v.startswith(cfg.expected_runtime_version_prefix), (v, cfg.expected_runtime_version_prefix)


def test_protobuf_backend_contract() -> None:
    cfg = ContractConfig.from_env()
    if cfg.expected_backend is None:
        pytest.skip("No expected backend configured (set PROTOBUF_CONTRACT_BACKEND=upb|python|cpp)")
    assert protobuf_backend() == cfg.expected_backend


def test_protoc_version_min_contract() -> None:
    cfg = ContractConfig.from_env()
    if cfg.expected_protoc_min is None:
        pytest.skip("No protoc minimum configured (PROTOBUF_CONTRACT_PROTOC_MIN is empty)")
    v = protoc_version()
    if v is None:
        pytest.skip("protoc not found on PATH")
    assert_version_tuple_gte(v, cfg.expected_protoc_min)
```

---

### `tests/protobuf/test_contract_schema.py`

```python
from __future__ import annotations

import pytest

from .contract_config import ContractConfig
from .contract_harness import (
    golden_dir,
    load_pb2_modules,
    schema_fingerprints,
    assert_golden_json,
)
from . import contract_targets as targets


def test_schema_fingerprints() -> None:
    cfg = ContractConfig.from_env()
    mods = load_pb2_modules(targets.PROTO_MODULE_IMPORTS, targets.PROTO_PACKAGE_ROOTS)

    if not mods:
        pytest.skip("No protobuf modules configured in tests/protobuf/contract_targets.py")

    fingerprints, shapes = schema_fingerprints(mods)

    # 1) minimal drift guard: stable hashes
    assert_golden_json(golden_dir() / "schema_fingerprints.json", fingerprints, update=cfg.update_goldens)

    # 2) optional diff-friendly shapes (handy when something changes)
    assert_golden_json(golden_dir() / "schema_shapes.json", shapes, update=cfg.update_goldens)
```

---

### `tests/protobuf/test_contract_presence_unknowns.py`

```python
from __future__ import annotations

import pytest

from google.protobuf.message import Message

from .contract_config import ContractConfig
from .contract_harness import (
    load_pb2_modules,
    assert_explicit_presence,
    assert_unknown_field_policy_roundtrip,
)
from . import contract_targets as targets


def _iter_message_classes(mods) -> list[type[Message]]:
    # Collect message classes via each module's DESCRIPTOR traversal.
    # Importing the module ensures generated classes are created and registered.
    classes: dict[str, type[Message]] = {}
    for m in mods:
        fd = m.DESCRIPTOR
        for _, top in sorted(fd.message_types_by_name.items(), key=lambda kv: kv[0]):
            _collect_cls_for_desc(top, classes)
    return [classes[k] for k in sorted(classes)]


def _collect_cls_for_desc(desc, classes: dict[str, type[Message]]) -> None:
    # Generated Python message classes are accessible via the symbol name from reflection.
    # Easiest path: resolve from the containing module attributes by name if present;
    # otherwise fall back to the default symbol database.
    import google.protobuf.symbol_database as symbol_database
    db = symbol_database.Default()
    cls = db.GetSymbol(desc.full_name)
    classes[desc.full_name] = cls

    for nested in sorted(desc.nested_types, key=lambda d: d.full_name):
        _collect_cls_for_desc(nested, classes)


def test_explicit_presence_requirements() -> None:
    cfg = ContractConfig.from_env()
    mods = load_pb2_modules(targets.PROTO_MODULE_IMPORTS, targets.PROTO_PACKAGE_ROOTS)

    if not targets.EXPLICIT_PRESENCE_FIELDS:
        pytest.skip("No EXPLICIT_PRESENCE_FIELDS configured in contract_targets.py")
    if not mods:
        pytest.skip("No protobuf modules configured in contract_targets.py")

    # Ensure all modules are imported before symbol resolution
    _ = _iter_message_classes(mods)

    import google.protobuf.symbol_database as symbol_database
    db = symbol_database.Default()

    for full_name, fields in sorted(targets.EXPLICIT_PRESENCE_FIELDS.items()):
        cls = db.GetSymbol(full_name)
        for f in fields:
            assert_explicit_presence(cls, f)


def test_unknown_field_policy_roundtrip() -> None:
    cfg = ContractConfig.from_env()
    mods = load_pb2_modules(targets.PROTO_MODULE_IMPORTS, targets.PROTO_PACKAGE_ROOTS)
    if not mods:
        pytest.skip("No protobuf modules configured in contract_targets.py")

    # Global policy can be overridden by env
    unknown_policy = cfg.unknown_field_policy
    if hasattr(targets, "UNKNOWN_FIELD_POLICY") and targets.UNKNOWN_FIELD_POLICY:
        unknown_policy = targets.UNKNOWN_FIELD_POLICY  # type: ignore[assignment]

    if unknown_policy not in {"preserve", "strip"}:
        raise AssertionError(f"UNKNOWN_FIELD_POLICY must be 'preserve' or 'strip', got {unknown_policy!r}")

    classes = _iter_message_classes(mods)

    # If you have a huge proto surface, you can cap via env var to keep runtime bounded.
    import os
    cap = os.getenv("PROTOBUF_CONTRACT_MAX_MESSAGES")
    if cap:
        n = int(cap)
        classes = classes[:n]

    for cls in classes:
        assert_unknown_field_policy_roundtrip(cls, unknown_policy)  # type: ignore[arg-type]
```

---

## 6) How to use it

* First run to generate goldens:

  ```bash
  UPDATE_GOLDENS=1 pytest -q tests/protobuf
  ```
* Normal CI run:

  ```bash
  pytest -q tests/protobuf
  ```

Optional env knobs:

* `PROTOBUF_CONTRACT_BACKEND=upb|python|cpp`
* `PROTOBUF_CONTRACT_VERSION=6.33.2`
* `PROTOBUF_CONTRACT_VERSION_PREFIX=6.`
* `PROTOBUF_CONTRACT_PROTOC_MIN=3.19.0`
* `PROTOBUF_CONTRACT_UNKNOWN_POLICY=strip|preserve`
* `PROTOBUF_CONTRACT_MAX_MESSAGES=200` (cap unknown-policy probe runtime)

---

## What this harness catches (fast)

* Any change in message structure (field numbers/types/labels, oneofs, map shapes, enum value changes, non-default options) → **fingerprint mismatch**
* Any accidental reliance on proto3 implicit-presence scalars where you need explicit presence → **presence test fails with a “fix schema” message**
* Any drift in unknown-field handling at your contract boundary → **unknown policy probe fails**

---

If you want the *next* increment: I can extend `normalize_for_contract()` into a full **“contract boundary normalizer”** that also:

* canonicalizes `FieldMask` paths,
* strips/keeps unknowns per message-type allowlist,
* enforces deterministic JSON emission policy,
* and provides a single `assert_contract_equivalent(msg_a, msg_b)` comparator used across your entire test suite.


Below is a **drop-in “next increment”** that turns your existing `normalize_for_contract()` into a real **contract-boundary normalizer**, with:

* **FieldMask canonicalization** (all `google.protobuf.FieldMask` anywhere in the tree)
* **Selective unknown handling** via **message full-name allowlists** (glob patterns)
* A single deterministic **contract JSON policy** (version-adaptive)
* A unified **`assert_contract_equivalent(a, b)`** comparator (JSON diff on failure)

I’m giving this as **repo-ready files/patches**. You can copy/paste as-is.

---

## 0) Update your targets config

### `tests/protobuf/contract_targets.py` (add this block)

```python
# ----------------------------
# Contract-boundary policy knobs
# ----------------------------

# Unknown-field policy is resolved per message full_name using fnmatch globs.
# Precedence: STRIP_UNKNOWN_FOR_MESSAGES > PRESERVE_UNKNOWN_FOR_MESSAGES > UNKNOWN_POLICY_DEFAULT
UNKNOWN_POLICY_DEFAULT: str = "strip"  # "strip" | "preserve"

# Examples:
#   ["my.pkg.*"]          (all messages in package)
#   ["my.pkg.Foo", "google.protobuf.Any"]
PRESERVE_UNKNOWN_FOR_MESSAGES: list[str] = []
STRIP_UNKNOWN_FOR_MESSAGES: list[str] = []

# Canonicalize FieldMask paths for stable diffs/cache keys.
CANONICALIZE_FIELDMASKS: bool = True

# Deterministic JSON emission policy for snapshots and contract equivalence.
# These values are applied via a version-adaptive wrapper around json_format.MessageToDict
# (older protobuf uses including_default_value_fields; newer uses always_print_fields_with_no_presence).
JSON_SNAPSHOT_POLICY: dict[str, object] = {
    "preserving_proto_field_name": True,
    "always_print_fields_with_no_presence": True,
    "use_integers_for_enums": False,
    "float_precision": None,  # or an int like 6/9/15 if you want to bound float noise
    "indent": 2,
}
```

(You can keep your existing `UNKNOWN_FIELD_POLICY` if you want; the loader below will fall back to it.)

---

## 1) Add a session fixture for options

### `tests/protobuf/conftest.py` (new)

```python
from __future__ import annotations

import pytest

from .contract_config import ContractConfig
from .contract_harness import load_contract_options
from . import contract_targets as targets


@pytest.fixture(scope="session")
def pb_contract_options():
    cfg = ContractConfig.from_env()
    return load_contract_options(targets, cfg)
```

Now every test can just accept `pb_contract_options`.

---

## 2) Extend the harness: options, normalization, deterministic JSON, comparator

### `tests/protobuf/contract_harness.py` (add/replace sections)

#### 2.1 Add imports (near top)

```python
import difflib
import fnmatch
from dataclasses import dataclass
from typing import Callable, Literal
```

#### 2.2 Add option dataclasses + loader (place near other config helpers)

```python
UnknownPolicy = Literal["preserve", "strip"]


@dataclass(frozen=True)
class ContractJsonPolicy:
    preserving_proto_field_name: bool = True
    always_print_fields_with_no_presence: bool = True
    use_integers_for_enums: bool = False
    float_precision: int | None = None
    indent: int = 2


@dataclass(frozen=True)
class ContractBoundaryOptions:
    unknown_default: UnknownPolicy = "strip"
    preserve_unknown_for: tuple[str, ...] = ()
    strip_unknown_for: tuple[str, ...] = ()
    canonicalize_field_masks: bool = True
    json_policy: ContractJsonPolicy = ContractJsonPolicy()


def load_contract_options(targets_module, cfg) -> ContractBoundaryOptions:
    """
    Merge ContractConfig (env-driven) + contract_targets.py (repo-driven).
    Precedence:
      - unknown_default: contract_targets.UNKNOWN_POLICY_DEFAULT
          else contract_targets.UNKNOWN_FIELD_POLICY
          else cfg.unknown_field_policy
      - patterns: contract_targets lists (default empty)
      - json policy / fieldmask toggle: contract_targets (defaulted)
    """
    # Unknown default
    unknown_default = getattr(targets_module, "UNKNOWN_POLICY_DEFAULT", None)
    if unknown_default is None:
        unknown_default = getattr(targets_module, "UNKNOWN_FIELD_POLICY", None)
    if unknown_default is None:
        unknown_default = getattr(cfg, "unknown_field_policy", "strip")

    unknown_default = str(unknown_default).strip().lower()
    if unknown_default not in {"strip", "preserve"}:
        raise ValueError(f"Unknown policy default must be 'strip'|'preserve', got {unknown_default!r}")

    preserve_patterns = tuple(getattr(targets_module, "PRESERVE_UNKNOWN_FOR_MESSAGES", []) or [])
    strip_patterns = tuple(getattr(targets_module, "STRIP_UNKNOWN_FOR_MESSAGES", []) or [])

    canonicalize_fm = bool(getattr(targets_module, "CANONICALIZE_FIELDMASKS", True))

    jp = getattr(targets_module, "JSON_SNAPSHOT_POLICY", {}) or {}
    json_policy = ContractJsonPolicy(
        preserving_proto_field_name=bool(jp.get("preserving_proto_field_name", True)),
        always_print_fields_with_no_presence=bool(jp.get("always_print_fields_with_no_presence", True)),
        use_integers_for_enums=bool(jp.get("use_integers_for_enums", False)),
        float_precision=jp.get("float_precision", None),
        indent=int(jp.get("indent", 2)),
    )

    return ContractBoundaryOptions(
        unknown_default=unknown_default,  # type: ignore[arg-type]
        preserve_unknown_for=preserve_patterns,
        strip_unknown_for=strip_patterns,
        canonicalize_field_masks=canonicalize_fm,
        json_policy=json_policy,
    )
```

#### 2.3 Add deterministic “contract JSON” wrapper (keep your existing canonical_json if you like)

```python
def contract_json(message: Message, opts: ContractBoundaryOptions) -> str:
    """
    Deterministic JSON emission under the configured contract policy.
    Uses your existing _message_to_dict() version-adaptive wrapper.
    """
    d = _message_to_dict(
        message,
        preserving_proto_field_name=opts.json_policy.preserving_proto_field_name,
        always_print_fields_with_no_presence=opts.json_policy.always_print_fields_with_no_presence,
        use_integers_for_enums=opts.json_policy.use_integers_for_enums,
        float_precision=opts.json_policy.float_precision,
    )
    return json.dumps(d, sort_keys=True, indent=opts.json_policy.indent, ensure_ascii=True)
```

#### 2.4 Replace your current `normalize_for_contract()` with a full boundary normalizer

Add these helpers (they avoid `UnknownFields()` entirely and work under upb):

```python
def clone_message(msg: Message) -> Message:
    c = msg.__class__()  # type: ignore[call-arg]
    c.CopyFrom(msg)
    return c


def resolve_unknown_policy(full_name: str, opts: ContractBoundaryOptions) -> UnknownPolicy:
    # Precedence: explicit strip patterns > preserve patterns > default
    for pat in opts.strip_unknown_for:
        if fnmatch.fnmatchcase(full_name, pat):
            return "strip"
    for pat in opts.preserve_unknown_for:
        if fnmatch.fnmatchcase(full_name, pat):
            return "preserve"
    return opts.unknown_default


def normalize_for_contract_inplace(msg: Message, opts: ContractBoundaryOptions) -> Message:
    """
    In-place normalization at the contract boundary:
      1) Apply selective unknown-field stripping/preservation based on message full_name allowlists
      2) Canonicalize FieldMask paths everywhere (optional)
    """
    resolver = lambda name: resolve_unknown_policy(name, opts)
    _apply_unknown_policy(msg, resolver)
    if opts.canonicalize_field_masks:
        _canonicalize_field_masks(msg)
    return msg


def normalize_for_contract(msg: Message, opts: ContractBoundaryOptions) -> Message:
    """
    Safe normalizer that returns a normalized clone (does not mutate caller-owned message).
    """
    c = clone_message(msg)
    return normalize_for_contract_inplace(c, opts)
```

Now add the internal traversal + selective unknown algorithm:

```python
def _iter_child_messages(msg: Message) -> Iterator[Message]:
    # Uses FieldDescriptor.is_repeated to avoid label deprecation warnings in protobuf 6.x
    for fd, value in msg.ListFields():
        if fd.cpp_type != FieldDescriptor.CPPTYPE_MESSAGE:
            continue

        if fd.is_repeated:
            # map fields are represented as repeated entry messages; the runtime exposes them as dict-like
            if fd.message_type and fd.message_type.GetOptions().map_entry:
                for _, v in value.items():
                    if isinstance(v, Message):
                        yield v
            else:
                for v in value:
                    yield v
        else:
            yield value


def _collect_topmost_preserve_descendants(root: Message, resolver: Callable[[str], UnknownPolicy]) -> list[Message]:
    """
    Collect preserve-policy nodes in the subtree, but STOP descending once we hit a preserve node.
    This avoids restoring nested preserve nodes whose references could be invalidated by restoring an ancestor.
    """
    out: list[Message] = []

    def walk(node: Message) -> None:
        if resolver(node.DESCRIPTOR.full_name) == "preserve":
            out.append(node)
            return
        for ch in _iter_child_messages(node):
            walk(ch)

    for ch in _iter_child_messages(root):
        walk(ch)
    return out


def _apply_unknown_policy(root: Message, resolver: Callable[[str], UnknownPolicy]) -> None:
    """
    Selective unknown-field stripping/preservation without UnknownFields() access:

    If a node's policy is 'strip':
      - snapshot bytes for all TOPMOST preserve descendants
      - call node.DiscardUnknownFields() (recursive strip)
      - restore each preserve descendant by ParseFromString(saved_bytes)
      - recurse to handle strip nodes inside restored preserve subtrees
    """
    def rec(node: Message) -> None:
        if resolver(node.DESCRIPTOR.full_name) == "strip":
            preserve_nodes = _collect_topmost_preserve_descendants(node, resolver)
            saved = [(p, p.SerializeToString()) for p in preserve_nodes]
            node.DiscardUnknownFields()
            for p, b in saved:
                p.ParseFromString(b)

        for ch in _iter_child_messages(node):
            rec(ch)

    rec(root)
```

And FieldMask canonicalization across the tree (preserves unknown bytes because it only mutates `paths`):

```python
def _canonicalize_field_masks(root: Message) -> None:
    from google.protobuf import field_mask_pb2

    fm_full_name = field_mask_pb2.FieldMask.DESCRIPTOR.full_name

    def rec(node: Message) -> None:
        if node.DESCRIPTOR.full_name == fm_full_name:
            tmp = field_mask_pb2.FieldMask()
            tmp.CanonicalFormFromMask(node)  # writes canonical paths into tmp; returns None

            paths = getattr(node, "paths", None)
            if paths is not None:
                del paths[:]          # mutate only the repeated field
                paths.extend(tmp.paths)

        for ch in _iter_child_messages(node):
            rec(ch)

    rec(root)
```

---

## 3) Add the single comparator: `assert_contract_equivalent(a, b)`

Add this to `contract_harness.py`:

```python
def assert_contract_equivalent(
    expected: Message,
    actual: Message,
    opts: ContractBoundaryOptions,
    *,
    context: str = "",
) -> None:
    """
    Contract equivalence:
      - descriptor must match
      - both sides are normalized under contract-boundary policy
      - compared via deterministic contract JSON
    On mismatch: raises with a unified diff.
    """
    if expected.DESCRIPTOR.full_name != actual.DESCRIPTOR.full_name:
        raise AssertionError(
            f"Descriptor mismatch: {expected.DESCRIPTOR.full_name} vs {actual.DESCRIPTOR.full_name}"
            + (f" ({context})" if context else "")
        )

    ne = normalize_for_contract(expected, opts)
    na = normalize_for_contract(actual, opts)

    je = contract_json(ne, opts)
    ja = contract_json(na, opts)

    if je != ja:
        diff = "\n".join(
            difflib.unified_diff(
                je.splitlines(),
                ja.splitlines(),
                fromfile="expected",
                tofile="actual",
                lineterm="",
            )
        )
        raise AssertionError(
            "Contract equivalence failed"
            + (f" ({context})" if context else "")
            + ":\n"
            + diff[:20000]  # cap giant diffs
        )
```

---

## 4) Update the unknown-policy test to use per-type allowlists

### `tests/protobuf/test_contract_presence_unknowns.py` (update only the unknown test)

Replace the body of `test_unknown_field_policy_roundtrip()` with this:

```python
def test_unknown_field_policy_roundtrip(pb_contract_options) -> None:
    mods = load_pb2_modules(targets.PROTO_MODULE_IMPORTS, targets.PROTO_PACKAGE_ROOTS)
    if not mods:
        pytest.skip("No protobuf modules configured in contract_targets.py")

    classes = _iter_message_classes(mods)

    import os
    cap = os.getenv("PROTOBUF_CONTRACT_MAX_MESSAGES")
    if cap:
        classes = classes[: int(cap)]

    for cls in classes:
        policy = resolve_unknown_policy(cls.DESCRIPTOR.full_name, pb_contract_options)
        assert_unknown_field_policy_roundtrip(cls, policy)  # existing probe test still works
```

This keeps your existing wire-probe roundtrip assertion but now drives it with the **same resolver** your normalizer uses.

---

## 5) How you use this everywhere (the point)

### Golden snapshot emission (deterministic)

```python
snap = contract_json(normalize_for_contract(msg, pb_contract_options), pb_contract_options)
assert_golden_text(path, snap, update=cfg.update_goldens)
```

### Comparing expected vs actual in any test

```python
assert_contract_equivalent(expected_msg, actual_msg, pb_contract_options, context="my feature X")
```

### Normalizing before serving / caching

```python
normalized = normalize_for_contract(msg, pb_contract_options)
cache_key = sha256(contract_json(normalized, pb_contract_options).encode("utf-8")).hexdigest()
```

---

### Why this design holds up

* **FieldMask canonicalization** prevents spurious diffs from path ordering / redundant covered paths.
* **Selective unknowns** lets you preserve forward-compat payload where you intend (e.g., `Any`, specific messages) while still stripping unknowns elsewhere to stabilize snapshots and cache keys.
* **Contract JSON policy** gives a single “source of truth” for diffs, golden tests, and equivalence.
* **Comparator** ensures every test asserts the *contract*, not incidental encoding.

If you want one more increment after this: I can add a small `contract_boundary.py` “public façade” module (thin wrappers + type exports) so production code can import the same normalizer without reaching into `tests/`, and so your test suite uses a single stable import path.
