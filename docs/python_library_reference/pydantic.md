Below is a **deep dive on Pydantic v2** (docs “latest” currently track **v2.12.5**) focused on:

* **B) Model authoring** (`BaseModel` + lifecycle)
* **C) Validation entrypoints** (`__init__`, `model_validate*` + modes)
* **I) Config as policy** (`ConfigDict` + inheritance + “trust boundaries”)

All citations point to the official Pydantic docs for the exact behaviors/knobs described. ([Pydantic][1])

---

# B) Models: authoring, lifecycle, and “schema as a class”

## B1) What a model *is* in v2

A Pydantic model is a Python class that inherits from `BaseModel` and declares fields via type annotations. ([Pydantic][2])

Under the hood, each model has:

* A **core schema** (pydantic-core “validator/serializer machinery”)
* A synthesized `__init__` signature
* Field metadata (`__pydantic_fields__`)
* Optional “extra fields” storage when enabled (`__pydantic_extra__`) ([Pydantic][3])

That mental model matters because:

* **Schema build time** is a thing (and can be deferred)
* **Validation modes** (Python vs JSON vs strings) reuse the same core schema but with different parsing rules
* **Config** is part of the schema boundary (important for nested models) ([Pydantic][4])

---

## B2) The “field contract”: required vs optional vs default

In Pydantic v2, “requiredness” is basically:

* **No default** → required
* **Default value** or **default factory** → optional at input-time (but still type-validated)
* `Optional[T]` only means `None` is allowed; it does **not** mean “not required” by itself (requiredness is about defaults)

Example:

```python
from __future__ import annotations
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class User(BaseModel):
    id: int                          # required
    name: str = "John Doe"           # default → not required
    signup_ts: Optional[datetime] = None  # default None → not required, None allowed
    tags: list[str] = Field(default_factory=list)  # safe default
```

(Defaults, `Field(...)`, and constraints are a huge topic, but we’ll keep it minimal here.)

---

## B3) Model lifecycle hooks: `model_post_init`

When you need “whole-object” initialization after validation/construct, override:

```python
def model_post_init(self, context: Any) -> None: ...
```

This runs **after** `__init__` and **after** `model_construct`, and is intended for work that requires the full model to exist (e.g., invariants or derived cached attributes). ([Pydantic][3])

**Policy implication:** `model_post_init` *will run even on* `model_construct`, so be careful not to assume inputs were validated if you allow construct paths.

---

## B4) Forward references + schema rebuilding: `model_rebuild`

When annotations reference types not yet defined (or recursive structures), Pydantic sometimes can’t fully resolve them at class creation. Pydantic can often rebuild automatically, but you can (and sometimes should) call:

```python
MyModel.model_rebuild()
```

In v2, `model_rebuild()` replaces v1’s `update_forward_refs()`. A key behavioral change: calling `model_rebuild()` on the outermost model builds a core schema for the *entire nested structure*, so “all types at all levels need to be ready” before you call it. ([Pydantic][4])

Also, `model_rebuild` exists specifically to handle cases where a `ForwardRef` couldn’t be resolved initially. ([Pydantic][3])

---

## B5) Constructing models without validation: `model_construct` (trust boundary)

`model_construct()` is explicitly for **trusted / pre-validated data**:

* It sets `__dict__` and `__pydantic_fields_set__`
* **Default values are respected**
* **No other validation is performed** ([Pydantic][3])

It also (subtly) interacts with `extra` config:

* If `extra='allow'`, extras go into `__dict__` / `__pydantic_extra__`
* If `extra='ignore'` (default), extras are ignored
* If `extra='forbid'`, *no validation happens*, so extras don’t raise—effectively ignored. ([Pydantic][3])

**Practical rule:** treat `model_construct` like “unsafe fast path.” Only use it after *your own* validation gates, schema snapshots, or when loading trusted persisted state.

---

# C) Validation entrypoints: modes, semantics, and parameter overrides

## C1) Three validation modes

Pydantic validates in three modes: **Python**, **JSON**, and **strings**. ([Pydantic][4])

* **Python mode** is used by:

  * `__init__(**kwargs)`
  * `model_validate(obj)` (dict / model instance / possibly objects)
* **JSON mode**:

  * `model_validate_json(json_data)`
* **Strings mode**:

  * `model_validate_strings(obj)` validates “stringy dicts” in JSON mode to coerce strings into proper types ([Pydantic][4])

Important nuance: **Python mode and JSON mode can behave differently** depending on types/config (especially strictness). If you have non-JSON data but want JSON-mode behavior, docs recommend JSON-dumping first or using `model_validate_strings` when your input is (nested) string dicts. ([Pydantic][4])

---

## C2) The canonical entrypoints and their signatures

### `__init__(**kwargs)` (Python mode)

* Only keyword args
* Uses the model’s config defaults (no per-call knobs except what you coded into the model) ([Pydantic][4])

### `model_validate(obj, *, strict=None, extra=None, from_attributes=None, context=None, by_alias=None, by_name=None)`

This is the most flexible entrypoint:

* Accepts dicts, model instances, and (optionally) arbitrary objects
* Can override strictness/extra/from_attributes/alias behavior per call ([Pydantic][3])

### `model_validate_json(json_data, *, strict=None, extra=None, context=None, by_alias=None, by_name=None)`

* Validates a JSON string/bytes directly (often faster than `json.loads` + `model_validate`) ([Pydantic][3])

### `model_validate_strings(obj, *, strict=None, extra=None, context=None, by_alias=None, by_name=None)`

* Validates a dict-like structure where keys/values are strings (possibly nested) *as if in JSON mode*, so strings can be coerced into target types ([Pydantic][3])

---

## C3) What to use when (practical “entrypoint routing”)

### Network/API boundary (JSON payload)

Use:

```python
m = MyModel.model_validate_json(raw_bytes_or_str)
```

You get JSON parsing + validation in one pass. ([Pydantic][4])

### Python dict boundary (already parsed / internal data)

Use:

```python
m = MyModel.model_validate(data)
```

This is the most general. ([Pydantic][4])

### Env-var / CLI / INI-ish boundary (string dicts)

Use:

```python
m = MyModel.model_validate_strings({"port": "5432", "debug": "true"})
```

You get “JSON-like coercion behavior” without manually JSON dumping. ([Pydantic][4])

### Trusted persisted state / fast internal rehydration

Use `model_construct` *only* if you truly trust the data and you understand the extra-handling behavior described above. ([Pydantic][3])

---

## C4) Instance revalidation: the “surprising default”

If you pass a **model instance** into `model_validate`, Pydantic can treat it as already valid (depending on config). The docs explicitly call out `revalidate_instances` as the control knob. ([Pydantic][4])

Why this matters: if someone mutates a model after creation (or if it was constructed via `model_construct`), you might accidentally “trust invalid data.”

---

# I) Config as policy: `ConfigDict` as the contract boundary

## I1) How config is specified (and what’s deprecated)

In v2, config is primarily via `model_config = ConfigDict(...)` (or even a plain dict). The v1-style inner `Config` class still works but is deprecated. ([Pydantic][5])

You can also use **class arguments**:

```python
class Model(BaseModel, frozen=True):
    ...
```

Type checkers understand class arguments better (e.g., `frozen=True` flags mutation as a type error). ([Pydantic][5])

---

## I2) Config inheritance + merging (and the multiple-inheritance footgun)

Config is **inherited**, and if a subclass defines its own `model_config`, it **merges** with the parent’s. ([Pydantic][5])

Multiple inheritance: Pydantic merges non-default settings across bases, and later bases override earlier ones (docs note this; plus there’s an explicit warning that Pydantic doesn’t currently follow Python MRO in this area). ([Pydantic][6])

**Design takeaway:** In “policy-driven” codebases, prefer a **single explicit base model** (or a small number of base models for different trust zones) rather than mixing many base classes.

---

## I3) Configuration does *not* propagate across nested model boundaries

This is a big one:

> For Pydantic models and dataclasses, configuration will not be propagated; each model has its own “configuration boundary.” ([Pydantic][5])

Meaning: if `Parent` sets `str_to_lower=True`, that does **not** affect the nested `User` model unless `User` itself has that config.

**Implication for “config as policy”:** If you want org-wide invariants (strictness, extra handling, alias behavior), you need to enforce them by:

* making all models inherit from your project base model, or
* using separate model classes per boundary (e.g., “InboundRequestModel” vs “InternalModel”), or
* validating nested values explicitly (less ideal).

---

## I4) The core policy knobs you’ll actually use a lot

### (1) Extra fields policy: `extra = 'ignore' | 'forbid' | 'allow'`

* Default is `'ignore'`
* `'forbid'` raises validation errors for unknown fields
* `'allow'` stores extras in `__pydantic_extra__`
* You can type-check extra values by annotating `__pydantic_extra__` ([Pydantic][7])

Also, **`extra` can be overridden per validation call** via the `extra=` argument to `model_validate*`. ([Pydantic][7])

**Policy pattern:**

* Inbound API request models: `extra='forbid'`
* “Blob-ish” metadata models: `extra='allow'` with typed `__pydantic_extra__`
* Internal models: either `ignore` or `forbid` depending on how defensive you want to be

---

### (2) Immutability and hashability: `frozen=True`

`frozen=True` makes models faux-immutable (blocks `__setattr__`) and generates `__hash__()` if fields are hashable. ([Pydantic][7])

**Tradeoff:** great for “value objects” / caching keys, but you must use `model_copy(update=...)` patterns instead of mutation.

---

### (3) Validate on mutation: `validate_assignment=True`

Default is `False`: mutations won’t be revalidated. Setting `validate_assignment=True` revalidates field updates. ([Pydantic][7])

**Pair it with:** `frozen=True` (no mutation at all) *or* strict mutation validation if you expect runtime updates.

---

### (4) Strictness: `strict=True` (model-wide) or per-call `strict=...`

* Model-wide strict mode: `model_config = ConfigDict(strict=True)` ([Pydantic][7])
* Per-validation strict override exists on `model_validate*` methods: `strict=` ([Pydantic][3])
* There’s also per-field strictness (via `Field(strict=...)`) but that’s more “Fields” territory. ([Pydantic][8])

**Policy pattern:**

* For external boundaries where “stringly typed” is common, keep model lax and selectively use strict.
* For internal boundaries where wrong types indicate bugs, use strict widely.

---

### (5) Revalidating instances: `revalidate_instances = 'never' | 'always' | 'subclass-instances'`

Default is `'never'` (no revalidation). ([Pydantic][7])

If you accept model instances as inputs to other models/services and you want to be defensive, set `'always'`. ([Pydantic][7])

---

### (6) Attribute-based loading (“ORM mode”): `from_attributes=True`

Controls whether Pydantic can build models by reading object attributes (and also affects discriminator lookup for tagged unions). ([Pydantic][7])

`model_validate` also has a per-call override: `from_attributes=`. ([Pydantic][3])

**Watchout:** attribute access can trigger properties/lazy loads/side effects in ORMs.

---

### (7) Alias population policy: `validate_by_alias` / `validate_by_name`

By default, validation uses aliases (`validate_by_alias=True`) and not field names (`validate_by_name=False`). ([Pydantic][9])

There’s a key compatibility note:

* `validate_by_name=True` and `validate_by_alias=True` together are equivalent to the old `populate_by_name=True` behavior. ([Pydantic][7])

And you cannot set both to `False` (it would make population impossible; Pydantic raises a usage error). ([Pydantic][10])

Also, the runtime entrypoints have `by_alias` / `by_name` overrides (`model_validate*`). ([Pydantic][3])

---

### (8) Error privacy: `hide_input_in_errors=True`

If you’re validating potentially sensitive inputs, you can prevent error messages from including the raw input value/type. ([Pydantic][7])

---

### (9) Namespace hygiene: `protected_namespaces`

Prevents fields from colliding with BaseModel members (prefix/regex-based). This is useful in “schema-from-external” contexts where keys might be weird. ([Pydantic][7])

---

### (10) Performance knobs: `defer_build` and `cache_strings`

Two config options that matter at scale:

* `defer_build=True` delays validator/serializer construction until first validation (useful if many nested models exist but are rarely instantiated) ([Pydantic][7])
* `cache_strings` can improve validation performance by caching strings at some memory cost ([Pydantic][7])

(We can do a dedicated “performance chapter” later if you want.)

---

## I5) A “best-in-class” policy layout (patterns, not prescriptions)

### Pattern 1: Explicit trust zones (recommended)

Define *two or three* base models and be very explicit about where they’re used:

* **Inbound** (untrusted): strict-ish, forbid extras, hide inputs in errors
* **Internal** (trusted): may allow extras, may be lax, may use `model_construct` in controlled places
* **Interop** (ORM/object sources): `from_attributes=True` and revalidation policies tuned

Example sketch:

```python
from pydantic import BaseModel, ConfigDict

class InboundModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=False,              # often keep lax at the boundary, tighten with validators
        hide_input_in_errors=True,
        revalidate_instances="always",
        validate_assignment=False, # usually treat inputs as immutable data blobs
    )

class InternalModel(BaseModel):
    model_config = ConfigDict(
        extra="ignore",
        strict=True,               # make bugs loud internally
        revalidate_instances="never",
    )
```

Why this aligns with Pydantic’s mechanics:

* Config is inherited + merged (base models work well) ([Pydantic][5])
* Config does **not** propagate into nested models unless they share the base class ([Pydantic][5])

---

### Pattern 2: Treat validation entrypoints as part of your API design

In a “best-in-class” system, you typically decide “one blessed way” to validate:

* At IO boundaries: `model_validate_json`
* For everything else: `model_validate`
* Avoid ad-hoc `__init__` calls in boundary code so you can set `strict/extra/context` explicitly when needed ([Pydantic][4])

---

### Pattern 3: Make “unsafe” paths loud

If you use `model_construct`, put it behind a named constructor:

```python
@classmethod
def from_trusted_storage(cls, row: dict) -> "MyModel":
    return cls.model_construct(**row)
```

…and document that this is not validation.

This aligns with `model_construct` semantics and its interaction with `extra`. ([Pydantic][3])

---



[1]: https://docs.pydantic.dev/latest/?utm_source=chatgpt.com "Welcome to Pydantic - Pydantic Validation"
[2]: https://docs.pydantic.dev/latest/concepts/models/?utm_source=chatgpt.com "Models - Pydantic Validation"
[3]: https://docs.pydantic.dev/latest/api/base_model/ "BaseModel - Pydantic Validation"
[4]: https://docs.pydantic.dev/latest/concepts/models/ "Models - Pydantic Validation"
[5]: https://docs.pydantic.dev/latest/concepts/config/ "Configuration - Pydantic Validation"
[6]: https://docs.pydantic.dev/latest/migration/?utm_source=chatgpt.com "Migration Guide - Pydantic Validation"
[7]: https://docs.pydantic.dev/latest/api/config/ "Configuration - Pydantic Validation"
[8]: https://docs.pydantic.dev/latest/concepts/fields/?utm_source=chatgpt.com "Fields - Pydantic Validation"
[9]: https://docs.pydantic.dev/latest/concepts/alias/?utm_source=chatgpt.com "Alias - Pydantic Validation"
[10]: https://docs.pydantic.dev/latest/errors/usage_errors/?utm_source=chatgpt.com "Usage Errors - Pydantic Validation"
[11]: https://docs.pydantic.dev/latest/concepts/validators/?utm_source=chatgpt.com "Validators"

## F) Validators in Pydantic v2

Pydantic v2 has two “placement levels” for custom validation:

* **Field validators**: operate on one field (but can be attached to multiple fields).
* **Model validators**: operate on the whole model input / whole validated instance.

Both levels support **modes** that control *where* your code runs relative to Pydantic’s internal parsing/coercion. ([Pydantic][1])

There are also two **authoring patterns**:

1. **Annotated pattern** (validators live “on the type” via `typing.Annotated`)
2. **Decorator pattern** (`@field_validator`, `@model_validator` on methods)

Both are first-class, and **decorator validators are internally converted into Annotated metadata** (affects ordering—more on that below). ([Pydantic][1])

---

# 1) Field validators: after / before / plain / wrap

In its simplest form, a field validator is a callable: **input → output**, where output is the “validated value” (possibly mutated/coerced). You *must return* the value you want to store. ([Pydantic][1])

Pydantic supports **four field validator modes**, each of which can be written either as:

* `Annotated[T, AfterValidator(func)]` etc, or
* `@field_validator('field', mode='...')` on a **classmethod** ([Pydantic][1])

---

## 1.1 After validators (default, most common)

**When they run:** after Pydantic has already done its internal parsing/validation of the target type.
**Why you like them:** the value has the annotated type (or very close), so code is safer/simpler. ([Pydantic][1])

### Decorator form

```python
from pydantic import BaseModel, field_validator

class Model(BaseModel):
    number: int

    @field_validator('number')  # mode='after' is the default
    @classmethod
    def is_even(cls, v: int) -> int:
        if v % 2:
            raise ValueError('must be even')
        return v
```

The docs explicitly call out that `'after'` is the default for the decorator and can be omitted. ([Pydantic][1])

### Annotated form (reusable)

```python
from typing import Annotated
from pydantic import AfterValidator

def is_even(v: int) -> int:
    if v % 2:
        raise ValueError('must be even')
    return v

EvenInt = Annotated[int, AfterValidator(is_even)]
```

The **big win** is that `EvenInt` can now be reused across models and nested inside containers (e.g. `list[EvenInt]`). ([Pydantic][1])

---

## 1.2 Before validators (shape normalization, “raw input”)

**When they run:** before Pydantic’s internal parsing/coercion.
**Input type:** could be *anything* (often `Any`).
**Contract:** whatever you return will then be validated against the field annotation by Pydantic. ([Pydantic][1])

Classic use: accept a scalar or a list and normalize to list:

```python
from typing import Any
from pydantic import BaseModel, field_validator

class Model(BaseModel):
    numbers: list[int]

    @field_validator('numbers', mode='before')
    @classmethod
    def ensure_list(cls, v: Any) -> Any:
        return v if isinstance(v, list) else [v]
```

Pydantic still validates the list items as `int` afterwards, so `"str"` will fail at `numbers.0`. ([Pydantic][1])

**Important footgun:** docs warn you should avoid mutating the raw input directly if you might raise a validation error later, because mutated values can be passed to other validators when unions are involved. ([Pydantic][1])

---

## 1.3 Plain validators (full override / short-circuit)

**When they run:** “like before”, but they **terminate validation immediately**:

* no further validators run
* Pydantic does **not** do its internal validation against the annotated field type ([Pydantic][1])

That means a plain validator can intentionally allow values that do *not* match the type annotation:

```python
from typing import Any
from pydantic import BaseModel, field_validator

class Model(BaseModel):
    number: int

    @field_validator('number', mode='plain')
    @classmethod
    def accept_anything(cls, v: Any) -> Any:
        # returning a string "works" because internal int validation is skipped
        return v
```

Docs illustrate this explicitly (a field annotated as `int` can accept `'invalid'` if a plain validator returns it). ([Pydantic][1])

**How to think about plain mode:** it’s an “escape hatch” for:

* validated-by-other-system data
* polymorphic “pass-through” blobs
* legacy compatibility shims

If you’re trying to *enforce* constraints, plain is usually the wrong tool.

---

## 1.4 Wrap validators (middleware: observe/transform/retry/fallback)

**When they run:** *around* inner validation.
**Signature requirement:** wrap validators must take an extra `handler` parameter (a callable that runs the inner validation). ([Pydantic][1])

Wrap is the most flexible mode:

* run code before calling `handler(value)`
* catch `ValidationError` and decide how to proceed
* skip `handler` entirely (accept/return early)
* re-run `handler` with transformed input ([Pydantic][1])

Example from docs: try validation; if it failed with `string_too_long`, truncate and revalidate:

```python
from typing import Any
from pydantic import BaseModel, ValidationError, ValidatorFunctionWrapHandler, field_validator

class Model(BaseModel):
    my_string: str

    @field_validator('my_string', mode='wrap')
    @classmethod
    def truncate(cls, v: Any, handler: ValidatorFunctionWrapHandler) -> str:
        try:
            return handler(v)
        except ValidationError as err:
            if err.errors()[0]['type'] == 'string_too_long':
                return handler(v[:5])
            raise
```

([Pydantic][1])

---

## 1.5 Choosing Annotated vs decorator for fields

### Prefer **Annotated** when:

* you want **reusable** validators across models
* you want validators to apply to **parts of a type**, like list items (`list[EvenInt]`) ([Pydantic][1])

### Prefer **decorators** when:

* you want one function applied to **multiple fields** at once
* you want class-centric grouping (everything in one model class)

Decorators can target multiple fields, or even all fields with `'*'`, and you can disable “field exists” checking via `check_fields=False` (helpful for base classes where subclasses define the fields). ([Pydantic][1])

---

## 1.6 Defaults: why your validators “didn’t run”

Pydantic **does not validate default values by default**, and therefore **custom validators won’t run** when a field uses its default unless you enable it. ([Pydantic][1])

Enable default validation via:

* field-level: `Field(validate_default=True)`
* or config-level: `validate_default` ([Pydantic][2])

---

# 2) Model validators: before / after / wrap

Model validators validate the entire model input or instance. Pydantic supports three modes:

* `mode='before'`: input-level (raw data)
* `mode='after'`: instance-level (post-validation, must return `self`)
* `mode='wrap'`: middleware around model validation with a handler ([Pydantic][1])

Also: `@root_validator` from v1 is deprecated; migrate to `@model_validator`. ([Pydantic][3])

---

## 2.1 After model validators (cross-field invariants)

These run after the whole model is validated, as **instance methods**, and **must return the validated instance**. ([Pydantic][1])

```python
from typing_extensions import Self
from pydantic import BaseModel, model_validator

class UserModel(BaseModel):
    password: str
    password_repeat: str

    @model_validator(mode='after')
    def passwords_match(self) -> Self:
        if self.password != self.password_repeat:
            raise ValueError("Passwords do not match")
        return self
```

Use this when the rule truly depends on multiple fields.

---

## 2.2 Before model validators (raw input gating / normalization)

These run before instantiation; they must handle raw input that could be “anything” (often `Any`). Docs note that if `from_attributes` is enabled, you may receive an arbitrary class instance rather than a dict. ([Pydantic][1])

```python
from typing import Any
from pydantic import BaseModel, model_validator

class UserModel(BaseModel):
    username: str

    @model_validator(mode='before')
    @classmethod
    def reject_card_number(cls, data: Any) -> Any:
        if isinstance(data, dict) and 'card_number' in data:
            raise ValueError("'card_number' should not be included")
        return data
```

Same union mutation warning applies here: avoid mutating raw inputs if you might raise later. ([Pydantic][1])

---

## 2.3 Wrap model validators (logging / observability / “policy envelope”)

Wrap model validators take a handler (`ModelWrapValidatorHandler`) and can surround validation, log failures, etc. Example from docs logs on validation failure and re-raises:

([Pydantic][1])

---

## 2.4 Inheritance behavior

* A model validator defined in a base class is called for subclasses.
* Overriding in a subclass overrides the base version (only subclass version runs). ([Pydantic][1])

This is a big deal for “policy base models”: you can centralize invariants, then selectively override.

---

## 2.5 `validate_assignment=True` caveat

Migration guide note: in some circumstances (e.g., assignment validation), `@model_validator` may receive an **instance** rather than a dict of values—so write before validators defensively. ([Pydantic][3])

---

# 3) `ValidationInfo`: context, already-validated data, and mode

Both field and model validators can optionally accept a `ValidationInfo` argument, which provides:

* already validated data (`info.data`, field validators only)
* user-defined context (`info.context`)
* validation mode (`'python' | 'json' | 'strings'`)
* field name (for field validators) ([Pydantic][1])

### Cross-field access inside a field validator

For field validators, `info.data` gives a dict of *already validated* values. This enables patterns like “confirm repeat password equals password” inside a field validator:

```python
from pydantic import BaseModel, ValidationInfo, field_validator

class UserModel(BaseModel):
    password: str
    password_repeat: str
    username: str

    @field_validator('password_repeat', mode='after')
    @classmethod
    def passwords_match(cls, v: str, info: ValidationInfo) -> str:
        if v != info.data['password']:
            raise ValueError("Passwords do not match")
        return v
```

But there’s a critical warning: **validation happens in the order fields are defined**, so you must not access fields that haven’t been validated yet. ([Pydantic][1])

### Context injection

You can pass context via `model_validate(..., context=...)` and access it as `info.context`. ([Pydantic][1])

Docs also note you **cannot** provide context when directly calling `Model(...)`; the workaround shown uses a `ContextVar` and a custom `__init__`. ([Pydantic][1])

---

# 4) Ordering: how validators compose

### Annotated ordering rules

When using `Annotated[...]`, order is:

* **before and wrap**: run **right-to-left**
* **after**: run **left-to-right** ([Pydantic][1])

### Decorator validators and ordering

Decorator validators are converted into Annotated metadata and **added last** after existing metadata, so the same ordering logic applies. ([Pydantic][1])

**Practical takeaway:** if you mix Annotated validators and decorator validators, the decorator ones behave like “extra metadata appended at the end”.

---

# 5) Raising errors: what to throw (and what not to)

Pydantic recommends three ways to raise validation errors from validator code:

1. `ValueError` (most common)
2. `AssertionError` (but `assert` is skipped with Python `-O`)
3. `PydanticCustomError` for structured error type + templated message + context ([Pydantic][1])

Also: **don’t raise `ValidationError` yourself**; raise a `ValueError` / `AssertionError` etc and Pydantic will collect them into the final `ValidationError`. ([Pydantic][4])

### `PydanticCustomError` (advanced: stable error “type” + context)

```python
from pydantic import BaseModel, field_validator
from pydantic_core import PydanticCustomError

class Model(BaseModel):
    x: int

    @field_validator('x')
    @classmethod
    def validate_x(cls, v: int) -> int:
        if v % 42 == 0:
            raise PydanticCustomError(
                'the_answer_error',
                '{number} is the answer!',
                {'number': v},
            )
        return v
```

This gives you a custom error type plus context-based formatting. ([Pydantic][1])

(There’s also `PydanticKnownError` for raising “built-in-style” errors with known types.) ([Pydantic][5])

---

# 6) Best-practice patterns for a “serious” codebase

## Pattern A: “Typed validator library” via `Annotated` aliases

Build a small `types.py` (or `validators.py`) of reusable annotated aliases:

* `NonEmptyStr`
* `EvenInt`
* `RepoPath`
* `NormalizedModuleName`
* `SHA256Hex`

Because the validators live on the type alias, they compose naturally in nested contexts like `list[RepoPath]`, without needing `each_item=True` (removed in v2). ([Pydantic][3])

## Pattern B: Use **before** validators only for normalization

Keep before validators “shape-only” (e.g., scalar→list, stripping wrappers, accepting ints and casting to str). For semantic checks, prefer after validators or model after validators. This reduces weird interactions with unions and makes type expectations clearer. ([Pydantic][1])

## Pattern C: Wrap validators for observability + controlled fallback

Wrap is the right place to:

* log failed validation with inputs
* implement “try strict, then soften” patterns
* implement bounded retries (e.g., truncate, normalize encoding) ([Pydantic][1])

## Pattern D: Make default-validation policy explicit

If invariants must hold even for defaulted fields, enable `validate_default` (field or config). ([Pydantic][2])

---


[1]: https://docs.pydantic.dev/latest/concepts/validators/ "Validators - Pydantic Validation"
[2]: https://docs.pydantic.dev/latest/concepts/fields/ "Fields - Pydantic Validation"
[3]: https://docs.pydantic.dev/latest/migration/ "Migration Guide - Pydantic Validation"
[4]: https://docs.pydantic.dev/latest/errors/errors/ "Error Handling - Pydantic Validation"
[5]: https://docs.pydantic.dev/latest/api/pydantic_core/ "pydantic_core - Pydantic Validation"

Below is a **deep dive on Pydantic v2 serialization + JSON Schema/OpenAPI**, organized as:

* **G) Serialization** (instances → dict/JSON + customization)
* **H) JSON Schema / OpenAPI** (types → schema dict + customization + FastAPI wiring)

All behavior/knobs below are from Pydantic + FastAPI docs (and a couple versioned release notes where that matters). ([Pydantic][1])

---

# G) Serialization in Pydantic v2

## G0) Mental model: “dump/serialize” + two modes

Pydantic uses “dump” and “serialize” interchangeably: you’re converting a structured object (model, dataclass, etc.) into **Python built-ins** (dict/list/str/…) and sometimes into a **JSON string/bytes**. ([Pydantic][1])

All serialization happens in **two output modes**:

* **Python mode**: may contain non-JSON-serializable Python objects (e.g., tuples).
* **JSON mode**: emits JSON-compatible types (e.g., tuples become lists). ([Pydantic][1])

This distinction is *everywhere* (model dumping, TypeAdapter dumping, schema generation).

---

## G1) Core entrypoints (models) + what they actually do

### `model_dump(...)` → dict-ish Python object

`model_dump()` is the primary “instance → python primitives” entrypoint, and it recursively converts sub-models to dictionaries. ([Pydantic][1])

Key knobs you’ll use constantly (signature shown in docs/type stubs and mirrored closely by TypeAdapter; see below):

* `mode='python'|'json'` (default `'python'`)
* `include=...`, `exclude=...` (nested selection rules)
* `by_alias=...` (use serialization aliases)
* `exclude_unset`, `exclude_defaults`, `exclude_none`
* `exclude_computed_fields`
* `round_trip`
* `warnings` (how to handle serialization errors)
* `serialize_as_any` (duck-typed/polymorphic serialization)
* `context` (passed into serializers) ([Pydantic][2])

**Python vs JSON mode example (tuple becomes list):**

```python
m.model_dump()              # python mode (tuple may stay tuple)
m.model_dump(mode="json")   # json mode (tuple becomes list)
```

([Pydantic][1])

### `model_dump_json(...)` → JSON string

`model_dump_json()` serializes directly to a JSON string (and supports e.g. `indent=...`). If Pydantic can’t serialize a value to JSON, it raises `PydanticSerializationError`. ([Pydantic][1])

---

## G2) Common footgun: `dict(model)` is **not** `model_dump()`

Iterating over a model yields `(field_name, field_value)` pairs **without converting sub-models to dicts**, so `dict(model)` will keep nested models as model instances. Use `model_dump()` when you need a recursively “jsonable-ish” structure. ([Pydantic][1])

---

## G3) Serialization for non-model types: `TypeAdapter`

When you’re not dealing with `BaseModel` instances (e.g., `list[MyModel]`, `dict[str, Foo]`, dataclasses, primitives), use `TypeAdapter`.

* `TypeAdapter.dump_python(...)`
* `TypeAdapter.dump_json(...)` ([Pydantic][3])

TypeAdapter is also where you get the richest “power knobs” surfaced cleanly, including:

* `warnings: bool | 'none' | 'warn' | 'error'`
* `fallback: Callable[[Any], Any] | None` (unknown-value fallback)
* `round_trip`, `exclude_computed_fields`, `serialize_as_any`, `context`, etc. ([Pydantic][4])

If you ever find yourself writing “manual recursive conversion” code: try a TypeAdapter first.

---

## G4) Pickling support

Pydantic models support efficient pickling/unpickling out of the box. ([Pydantic][1])

---

## G5) Aliases in serialization (and defaults that surprise people)

### `by_alias` defaults and model-level policy

`model_dump(by_alias=True)` uses serialization aliases; `by_alias` defaults to `False` unless you set a model-level config for it (e.g., `ConfigDict.serialize_by_alias`). ([Pydantic][5])

### Validation alias vs serialization alias

Pydantic lets you choose whether an alias applies to:

* validation only (`validation_alias`)
* serialization only (`serialization_alias`)
* both (`alias`)

…which matters a lot for API compatibility. ([Pydantic][5])

---

## G6) Computed fields: “property as a serialized field”

`@computed_field` lets you include `property`/`cached_property` values during serialization, and it also impacts JSON Schema **in serialization mode**. ([Pydantic][5])

Key gotchas:

* Pydantic doesn’t add validation logic or cache invalidation for computed fields—it just wraps a property and includes it. ([Pydantic][5])
* Computed fields can appear as `readOnly` in schema and are included in `model_dump()` output. ([Pydantic][5])
* You can exclude computed fields via `exclude_computed_fields` (or use `round_trip` patterns where appropriate). ([Pydantic][4])

---

## G7) Inclusion / exclusion (field-level + call-level)

### Field-level exclusion

You can exclude fields at definition time:

* `Field(exclude=True)`
* `Field(exclude_if=...)` (predicate-based) ([Pydantic][1])

Field-level exclusion takes priority over the `include=` parameter. ([Pydantic][1])

### Call-level inclusion/exclusion (nested and indexable)

On serialization methods like `model_dump()` you can:

* pass a **set** (`{'user', 'value'}`) or
* pass a **dict** (select nested fields) ([Pydantic][1])

You can also include/exclude items inside lists/dicts using indexes, and use `'__all__'` to apply a rule to every element. Also: using `False` inside include/exclude specs is not supported. ([Pydantic][1])

### Value-based exclusion

The “3 classic knobs”:

* `exclude_unset` (based on what was explicitly set)
* `exclude_defaults`
* `exclude_none` ([Pydantic][4])

(These are crucial for stable API payloads: e.g., “don’t send a bunch of default values” vs “always include defaults”.)

---

## G8) Custom serializers (field + model)

### One big rule

**Only one serializer can be defined per field/model.** You can’t stack multiple serializers (plain + wrap, etc.). If you need composition, do it in one serializer, or use a wrap serializer to call the handler and then apply additional logic. ([Pydantic][1])

---

### Field serializers

#### Two patterns

1. **Annotated pattern** (reusable)
2. **Decorator pattern** (`@field_serializer`) (easy to target multiple fields) ([Pydantic][1])

#### Plain vs wrap

* **Plain**: called unconditionally; Pydantic’s normal per-type serialization is not invoked. This can easily emit values that don’t match the annotated field type unless you enforce a `return_type`. ([Pydantic][1])
* **Wrap**: middleware; you get a handler that performs standard serialization, and you can run code before/after it (or skip it). ([Pydantic][1])

Also: `'plain'` is the default mode for the decorator and can be omitted. ([Pydantic][1])

#### `return_type` matters

If you provide `return_type` (or annotate the serializer function’s return), Pydantic can build an extra serializer layer to ensure the serialized output matches that return type. ([Pydantic][1])

`PlainSerializer` also documents a `when_used` switch: `'always'`, `'unless-none'`, `'json'`, `'json-unless-none'`. ([Pydantic][6])

---

### Model serializers (`@model_serializer`)

Model serializers can return **non-dict** outputs (e.g., a string). Plain is default; wrap is middleware around the whole-model serialization. ([Pydantic][1])

---

## G9) Serializer info + serialization context

Both field and model serializers can accept an `info` arg that includes:

* the serialization mode (`'python'` or `'json'`)
* user-defined context (`info.context`)
* parameters used for the dump (e.g. `exclude_unset`, `serialize_as_any`)
* field name (for field serializers) ([Pydantic][1])

Context is passed via `model_dump(context=...)` / `TypeAdapter.dump_python(context=...)`. ([Pydantic][1])

---

## G10) Polymorphism / subclass serialization: `SerializeAsAny` and `serialize_as_any`

By default, model-like values serialize according to their annotated schema; subclass-only fields may be dropped. If you want “duck-typed” serialization that preserves subclass fields:

* Field-level: `SerializeAsAny[BaseType]`
* Runtime: `serialize_as_any=True` on serialization methods ([Pydantic][1])

Note: the changelog indicates ongoing work/fixes around `serialize_as_any` and hints at a future polymorphic feature designed to replace it in many cases, so treat this knob as powerful-but-worth-testing in your own stack. ([Pydantic][7])

---

# H) JSON Schema + OpenAPI in Pydantic v2 (and FastAPI implications)

## H0) Two different things: schema generation vs instance serialization

* `model_dump_json()` / `TypeAdapter.dump_json()` serialize **instances** to JSON strings.
* `model_json_schema()` / `TypeAdapter.json_schema()` generate **JSON Schema documents** (dicts you can `json.dumps`). ([Pydantic][3])

Pydantic JSON Schema output is compliant with:

* JSON Schema Draft 2020-12
* OpenAPI 3.1.0 ([Pydantic][3])

---

## H1) Generating JSON Schema

You’ll use:

* `BaseModel.model_json_schema(...)`
* `TypeAdapter.json_schema(...)` ([Pydantic][3])

And Pydantic explicitly calls out these return a **jsonable dict**, not a JSON string. ([Pydantic][3])

---

## H2) JsonSchemaMode: **validation** vs **serialization**

JSON Schema generation has its own `mode` parameter:

* default: `mode='validation'` (schema representing what inputs validate)
* optional: `mode='serialization'` (schema representing serialized outputs) ([Pydantic][3])

This can change requiredness and shapes. The docs show a `Decimal` example where validation schema accepts both number and numeric-string patterns, while serialization schema is “string-like”. ([Pydantic][3])

This is *the* key to making your docs match what clients actually receive.

---

## H3) Forcing a single schema personality: `json_schema_mode_override`

If you don’t want separate “Input/Output” schema personalities (or you’re integrating with a tool that can’t handle them), you can force schema generation to always use one mode via config:

`ConfigDict(json_schema_mode_override='validation'|'serialization')` ([Pydantic][8])

This is also the documented mechanism to prevent the automatic `-Input` / `-Output` suffixing on `$ref` definition names when both modes are referenced. ([Pydantic][8])

---

## H4) Field-level schema customization

You can customize JSON Schema at the field level via `Field(...)` (and related metadata). Pydantic also supports a `field_title_generator` for programmatic titles. ([Pydantic][3])

Additionally, computed fields have their own schema metadata controls and are included in schema generation (serialization mode) with `readOnly`, etc. ([Pydantic][5])

---

## H5) Model-level schema customization

At model level, config options relevant to JSON schema include: `title`, `json_schema_extra`, `json_schema_mode_override`, `field_title_generator`, `model_title_generator`. ([Pydantic][3])

`json_schema_extra` can be a dict or a callable, and starting in v2.9 Pydantic merges `json_schema_extra` dictionaries coming from annotated types (additive behavior). ([Pydantic][3])

---

## H6) Custom types: preferred customization ladder

For custom types, Pydantic documents multiple options, and even gives preference guidance:

1. **`WithJsonSchema`** (simple override, preferred over implementing hooks) ([Pydantic][3])
2. `SkipJsonSchema`
3. Implement `__get_pydantic_core_schema__`
4. Implement `__get_pydantic_json_schema__` ([Pydantic][3])

Also: `__modify_schema__` is no longer supported in v2; use `__get_pydantic_json_schema__` instead. ([Pydantic][9])

For global/process-wide schema customization, Pydantic v2 provides `GenerateJsonSchema` with override points; the migration guide highlights this as a v2 design goal, and the docs show passing a custom subclass via `schema_generator=...`. ([Pydantic][10])

---

## H7) OpenAPI reality (FastAPI): why serialization mode suddenly matters

FastAPI generates **OpenAPI 3.1.0** (default `openapi_version='3.1.0'`). ([FastAPI][11])

FastAPI includes Pydantic model JSON Schemas in its OpenAPI, which drive `/docs` UI and client generation. ([FastAPI][12])

**Version-sensitive behavior you should bake into your docs:**
FastAPI **0.101.0** explicitly enabled **Pydantic’s serialization mode for responses**, added support for Pydantic `computed_field`, and improved OpenAPI for response models (required attributes, better clients). ([FastAPI][13])

This has a direct architectural implication for “best-in-class” API contracts:

* **Request bodies** are naturally modeled by **validation-mode** constraints (“what inputs are accepted”).
* **Responses** are naturally modeled by **serialization-mode** constraints (“what clients actually receive, including computed fields/defaulted outputs”). ([Pydantic][3])

If you generate SDKs, ensure tooling supports OpenAPI 3.1 (FastAPI calls this out). ([FastAPI][14])

---

## H8) Examples in schema and docs

FastAPI notes that adding examples in Pydantic models/fields ends up in the model’s JSON Schema, which then appears in OpenAPI/docs UI. ([FastAPI][15])

---

## H9) Extending/modifying OpenAPI (FastAPI side)

FastAPI exposes an `.openapi()` method and serves the result at `/openapi.json`; the docs show how to override/extend that output. ([FastAPI][16])

This is where you handle cross-cutting doc concerns that aren’t nicely expressible per-model (tags, externalDocs, global security schemes, etc.).

---

# Recommended “best-in-class” documentation framing (how to present this in your repo)

If you want this to be maximally useful for reviewers/agents, I’d document serialization + schema as a **two-axis contract**:

1. **Instance contract**: `model_dump` / `model_dump_json` / serializers / include-exclude policy / alias policy
2. **Schema contract**: `model_json_schema(mode=...)` / `json_schema_mode_override` / custom type strategy / FastAPI OpenAPI version + response-mode behavior

That framing makes it obvious *why* two “modes” exist and where drift can happen.

---

## Next deep dive

Next in the original order is **P + O**: **Settings (`pydantic-settings`) + dataclasses integration**.

[1]: https://docs.pydantic.dev/latest/concepts/serialization/ "Serialization - Pydantic Validation"
[2]: https://docs.pydantic.dev/latest/api/root_model/?utm_source=chatgpt.com "RootModel - Pydantic Validation"
[3]: https://docs.pydantic.dev/latest/concepts/json_schema/ "JSON Schema - Pydantic Validation"
[4]: https://docs.pydantic.dev/latest/api/type_adapter/ "TypeAdapter - Pydantic Validation"
[5]: https://docs.pydantic.dev/latest/concepts/fields/ "Fields - Pydantic Validation"
[6]: https://docs.pydantic.dev/latest/api/functional_serializers/ "Functional Serializers - Pydantic Validation"
[7]: https://docs.pydantic.dev/latest/changelog/?utm_source=chatgpt.com "Changelog - Pydantic Validation"
[8]: https://docs.pydantic.dev/latest/api/config/?utm_source=chatgpt.com "Configuration - Pydantic Validation"
[9]: https://docs.pydantic.dev/latest/errors/usage_errors/?utm_source=chatgpt.com "Usage Errors - Pydantic Validation"
[10]: https://docs.pydantic.dev/latest/migration/?utm_source=chatgpt.com "Migration Guide - Pydantic Validation"
[11]: https://fastapi.tiangolo.com/reference/fastapi/?utm_source=chatgpt.com "FastAPI class"
[12]: https://fastapi.tiangolo.com/tutorial/first-steps/?utm_source=chatgpt.com "First Steps - FastAPI - Tiangolo"
[13]: https://fastapi.tiangolo.com/release-notes/?utm_source=chatgpt.com "Release Notes - FastAPI"
[14]: https://fastapi.tiangolo.com/advanced/generate-clients/?utm_source=chatgpt.com "Generating SDKs - FastAPI"
[15]: https://fastapi.tiangolo.com/tutorial/schema-extra-example/?utm_source=chatgpt.com "Declare Request Example Data - FastAPI"
[16]: https://fastapi.tiangolo.com/how-to/extending-openapi/?utm_source=chatgpt.com "Extending OpenAPI - FastAPI - Tiangolo"

Below is a deep dive on:

* **P) Settings (`pydantic-settings`)**
* **O) Dataclasses integration (`pydantic.dataclasses`)**

I’m going to treat both as “contract surfaces” that you can design around (sources/precedence, schema/validation, and where footguns live).

---

# P) Settings with `pydantic-settings`

## P0) What it is (and the key v2 packaging change)

In Pydantic v2, **settings management lives in the separate `pydantic-settings` package**: you install it and then inherit from `pydantic_settings.BaseSettings`. ([Pydantic][1])

Migration note that matters operationally: **`parse_env_var` was removed**; if you need custom parsing logic, the intended replacement is to **customize settings sources** (see below). ([Pydantic][2])

---

## P1) The base behavior: `BaseSettings` reads from sources

If you create a model inheriting from `BaseSettings`, the initializer fills missing fields (not provided as kwargs) by reading from the environment; defaults are used if nothing provides a value. ([Pydantic][1])

So conceptually, your settings class is a normal Pydantic model **plus** a “source pipeline” that supplies inputs.

---

## P2) Default source order (priority) — memorize this

When multiple sources specify the same field, `pydantic-settings` uses this priority order (highest → lowest): ([Pydantic][1])

1. **CLI args** (if `cli_parse_args` enabled)
2. **`Settings(...)` initializer kwargs**
3. **Environment variables**
4. **dotenv (`.env`)**
5. **Secrets directory**
6. **Defaults in the class**

That ordering is the heart of “settings as policy”: it defines *who wins* when people set values in multiple places.

---

## P3) Controlling behavior via `SettingsConfigDict` + runtime overrides

Settings are configured via `model_config = SettingsConfigDict(...)` (recommended). You can also override many behaviors at instantiation-time via underscore-prefixed params on `BaseSettings.__init__` (useful for tests). ([Pydantic][3])

A few high-value knobs (from the BaseSettings API):

* `_env_prefix`: prefix for env var names ([Pydantic][3])
* `_env_file`: which dotenv file(s) to load; can be `None` to disable dotenv loading ([Pydantic][3])
* `_env_file_encoding`: dotenv encoding ([Pydantic][3])
* `_env_ignore_empty`: ignore env vars with empty string values ([Pydantic][3])
* `_env_nested_delimiter`: delimiter for nested env var mapping (e.g. `DATABASE__USER`) ([Pydantic][3])
* `_env_parse_none_str`: map a string token (e.g. `"null"`) to `None` ([Pydantic][3])
* `_env_parse_enums`: parse enum “names” to values ([Pydantic][3])
* `_case_sensitive`: env/CLI case sensitivity ([Pydantic][3])
* `_secrets_dir`: directory (or directories) for secret-file loading ([Pydantic][3])

### Nested models: env mapping

Nested config is a first-class pattern; you typically combine nested models with `env_nested_delimiter='__'`. The docs show this explicitly in secret-manager examples. ([Pydantic][1])

---

## P4) dotenv: `.env` is just another source layer

dotenv values are read *after* environment variables but *before* secrets-dir values (per the default priority list). ([Pydantic][1])
You control dotenv via `env_file` / `env_file_encoding` either in `SettingsConfigDict` or via `_env_file` / `_env_file_encoding` at init. ([Pydantic][3])

**Practical policy rule:** treat dotenv as “developer convenience,” not as a production secret mechanism.

---

## P5) Secrets directory: “one file = one secret”

A secrets directory is file-based injection of sensitive values: the file name is the key; the file content is the value. The docs compare it to dotenv, but “single value per file”. ([Pydantic][1])
You enable it by setting `secrets_dir` in `model_config` (or passing `_secrets_dir` to init). ([Pydantic][1])

And again: it’s lower priority than env/dotenv by default. ([Pydantic][1])

---

## P6) “Beyond env”: file sources and secret managers

Pydantic Settings ships additional built-in sources for common config files (JSON, TOML, YAML, `pyproject.toml`), intended to be plugged in via the same “customize sources” mechanism. ([Pydantic][1])

It also supports cloud secret managers via dedicated sources (e.g., Google Secret Manager) that you add into the source pipeline; the docs show this pattern by creating a `GoogleSecretManagerSettingsSource` and inserting it via `settings_customise_sources`. ([Pydantic][1])

---

## P7) Customizing settings sources (this is the “power user” surface)

If the default source order doesn’t match your needs, you override:

```python
@classmethod
def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings) -> tuple[...]:
    ...
```

* It receives the default sources
* You return a tuple of sources; **the order you return is the priority** (first = highest). ([Pydantic][1])
* You can **change order**, **remove sources**, and **add custom ones** (the docs provide an example custom JSON source). ([Pydantic][1])

A key example: swapping `env_settings` and `init_settings` makes env vars override init kwargs. ([Pydantic][1])

This is also the core replacement strategy for v1’s removed `parse_env_var`: build your own parsing in a custom source. ([Pydantic][2])

---

## P8) CLI support: settings can be a CLI definition language

`pydantic-settings` can parse CLI args directly into `BaseSettings` when you set `cli_parse_args=True` (or pass args explicitly). The docs show rich parsing for dictionaries, including JSON-style and `k=v` style inputs. ([Pydantic][1])

Two higher-level utilities:

* `CliApp.run(...)` to run a `BaseSettings`/`BaseModel`/Pydantic dataclass as a CLI app and call an optional `cli_cmd` method afterward ([Pydantic][1])
* `CliApp.run_subcommand(...)` for nested subcommands ([Pydantic][1])

Error/UX policy knobs:

* `cli_exit_on_error`: raise `SettingsError` instead of exiting ([Pydantic][1])
* `cli_enforce_required`: make “required” actually required from the CLI (instead of satisfied by other sources like env vars) ([Pydantic][1])

**Design implication:** Settings is no longer “just env parsing”; it can be a small CLI framework if you want it to be.

---

## P9) Best-in-class patterns for a real codebase

### Pattern 1: Treat settings as an explicit *source pipeline*

Make the precedence and source set a first-class design decision:

* “prod”: env + secret manager + secrets dir
* “dev”: env + dotenv + local config file
* “tests”: init kwargs only (or a custom source that reads test fixtures)

`settings_customise_sources` is the canonical way to encode this. ([Pydantic][1])

### Pattern 2: Use nested models + `env_nested_delimiter`

It keeps your env surface sane while still typed. ([Pydantic][1])

### Pattern 3: Don’t overuse CLI parsing unless you want Settings to be “the CLI”

If you already have Typer/Cyclopts/etc., you may still want `pydantic-settings` for env/dotenv/secrets and keep CLI parsing in your CLI framework. But if you want a “single schema for config + CLI,” Pydantic’s built-in CLI support is designed to do that. ([Pydantic][1])

---

# O) Dataclasses integration in Pydantic v2

There are two “lanes”:

1. **Using stdlib dataclasses as field types** inside `BaseModel` / Pydantic dataclasses / `TypeAdapter`
2. **Creating Pydantic-enhanced dataclasses** via `@pydantic.dataclasses.dataclass`

## O1) Pydantic dataclasses are *not* a replacement for `BaseModel`

Docs are explicit: Pydantic dataclasses provide “dataclasses + validation,” but there are cases where `BaseModel` is the better choice. ([Pydantic][4])

You typically choose dataclasses when you want:

* dataclass ergonomics (`slots`, ordering, etc.)
* a “value object” feel
* fewer model features / less framework coupling

---

## O2) Creating a Pydantic dataclass: decorator semantics

You use:

```python
from pydantic.dataclasses import dataclass

@dataclass
class User:
    id: int
```

The decorator accepts the stdlib dataclass args plus a `config` parameter. ([Pydantic][4])

Two API details that matter:

* `init` must be `False` because Pydantic inserts its own `__init__` ([Pydantic][5])
* `validate_on_init=False` is no longer supported; in v2, Pydantic dataclasses are validated on init ([Pydantic][5])

---

## O3) Dataclass config: two ways

To set Pydantic config on a dataclass, you can either:

* pass `config=ConfigDict(...)` to the decorator, or
* define `__pydantic_config__ = ConfigDict(...)` on the class ([Pydantic][4])

Example patterns are shown directly in the docs (e.g., enabling `validate_assignment=True`). ([Pydantic][4])

### Important gotcha: `extra='allow'` behaves differently than models

Dataclasses “inherit” some stdlib behavior. Even though `extra='allow'` exists, extra fields may be omitted from the dataclass string representation, and there’s no `__pydantic_extra__` mechanism for validating extras like there is with models. ([Pydantic][4])

(If you rely heavily on extras, models are usually the safer contract.)

---

## O4) Rebuilding dataclass schema

For forward refs / late binding, the docs provide `rebuild_dataclass()` to rebuild the core schema. ([Pydantic][4])

---

## O5) Validators and lifecycle hooks on dataclasses

Validators work on Pydantic dataclasses too (e.g., `@field_validator`). ([Pydantic][4])

Two lifecycle ordering facts:

* The stdlib dataclass `__post_init__` is supported
* It is called **between** “before” and “after” model validators (dataclass-specific sequencing) ([Pydantic][4])

And a subtle signature difference:

* For dataclass model validators, the `values` parameter is an `ArgsKwargs` object rather than a normal dict (the docs call this out). ([Pydantic][4])

---

## O6) Stdlib dataclasses inside models (and `revalidate_instances`)

When you use a stdlib dataclass as a field inside a Pydantic model, validation *can* apply—but the docs show a crucial requirement: if you pass an already-constructed dataclass instance with invalid internals, you may need `revalidate_instances='always'` so Pydantic revalidates its attributes. ([Pydantic][4])

This is the same “trust boundary” concept as `model_construct`: if an object instance comes in already constructed, you decide whether to revalidate it.

---

## O7) Serialization / schema for dataclasses: use `TypeAdapter`

Dataclasses don’t have the full `BaseModel` method surface. The docs recommend wrapping a dataclass in a `TypeAdapter` to validate/dump/generate JSON schema. ([Pydantic][4])

This becomes your “standard tool” if you want:

* `dump_python` / `dump_json`
* `json_schema`
* validation entrypoints

---

## O8) Introspection utilities

Pydantic dataclasses remain “real dataclasses,” so `dataclasses.is_dataclass()` returns `True`, but Pydantic provides `is_pydantic_dataclass()` to tell whether it’s Pydantic-enhanced. ([Pydantic][4])

---

# Practical synthesis for your repo docs

If you want this to be maximally useful for reviewers/agents, document these as two “contracts”:

1. **Runtime config contract (Settings)**

   * source precedence (default + customized) ([Pydantic][1])
   * env naming conventions (prefix + nested delimiter) ([Pydantic][3])
   * where secrets come from (secrets dir + secret managers) ([Pydantic][1])
   * CLI stance (use settings-as-CLI or not) ([Pydantic][1])

2. **Value object contract (Dataclasses)**

   * config location (`config=` vs `__pydantic_config__`) ([Pydantic][4])
   * differences vs models (`extra` quirks, method surface) ([Pydantic][4])
   * lifecycle ordering (`__post_init__` placement) ([Pydantic][4])
   * serialization/schema via `TypeAdapter` ([Pydantic][4])

---

If you want to continue in the original sequence, the next deep dive would be **M + N + Q + S**: **TypeAdapter**, **validate_call**, **custom types/extension hooks**, and **performance + internals**.

[1]: https://docs.pydantic.dev/latest/concepts/pydantic_settings/ "Settings Management - Pydantic Validation"
[2]: https://docs.pydantic.dev/latest/migration/ "Migration Guide - Pydantic Validation"
[3]: https://docs.pydantic.dev/latest/api/pydantic_settings/ "Pydantic Settings - Pydantic Validation"
[4]: https://docs.pydantic.dev/latest/concepts/dataclasses/ "Dataclasses - Pydantic Validation"
[5]: https://docs.pydantic.dev/latest/api/dataclasses/ "Pydantic Dataclasses - Pydantic Validation"

Below is a deep dive on:

* **M) `TypeAdapter`** (validate/serialize/schema for *any* Python type)
* **N) `validate_call`** (function argument + optional return validation)
* **Q) Custom types & extension hooks** (core schema + JSON schema customization)
* **S) Performance + internals** (what’s actually happening, and how to make it fast)

The unifying mental model is: **Pydantic compiles “a type” into a pydantic-core schema → validator + serializer**; `BaseModel`, `TypeAdapter`, and `validate_call` are just different front-ends to that same engine. ([Pydantic][1])

---

# M) `TypeAdapter`: validation/serialization/schema without `BaseModel`

## M1) What `TypeAdapter` is (and is not)

`TypeAdapter` gives you a **validator + serializer + JSON schema generator** for an arbitrary Python type — including dataclasses, primitives, containers, unions, `TypedDict`, etc. It basically exposes “BaseModel-like methods” for things that aren’t models. ([Pydantic][1])

Two important constraints:

* **A `TypeAdapter` instance is not a type** and can’t be used as a field annotation. ([Pydantic][1])
* You **can pass a `ConfigDict`** when instantiating an adapter, **unless** the adapted type already has its own config (e.g., `BaseModel`, `TypedDict`, stdlib dataclass) — in that case, Pydantic raises a `type-adapter-config-unused` user error. ([Pydantic][1])

Practical takeaway:

* Use `TypeAdapter` when you want to validate/dump **“shapes”** like `list[SomeModel]`, `dict[str, Foo]`, or `TypedDict` payloads *without* inventing a wrapper `BaseModel`.

---

## M2) The methods you actually use (and how they map to modes)

### Validation entrypoints

`TypeAdapter` has three entrypoints that match Pydantic’s validation modes:

* `validate_python(obj, *, strict, extra, from_attributes, context, experimental_allow_partial, by_alias, by_name) -> T` ([Pydantic][1])
* `validate_json(data, *, strict, extra, context, experimental_allow_partial, by_alias, by_name) -> T` ([Pydantic][1])
* `validate_strings(obj, *, strict, extra, context, experimental_allow_partial, by_alias, by_name) -> T` ([Pydantic][1])

Notes that matter in real systems:

* **Alias policy safety check:** like model validation, TypeAdapter errors if you set `by_alias=False` without also setting `by_name=True` (Pydantic must be able to populate fields somehow). ([Pydantic][1])
* **Dataclass + `from_attributes`:** TypeAdapter explicitly notes `from_attributes` is not supported when used with a Pydantic dataclass. ([Pydantic][1])
* **Streaming/partial validation:** these methods accept an **experimental** `experimental_allow_partial` knob intended for partial/stream processing use cases. ([Pydantic][1])

### Serialization entrypoints

* `dump_python(instance, *, mode, include, exclude, by_alias, exclude_unset, exclude_defaults, exclude_none, exclude_computed_fields, round_trip, warnings, fallback, serialize_as_any, context) -> Any` ([Pydantic][1])
* `dump_json(instance, *, indent, ensure_ascii, include, exclude, by_alias, exclude_unset, exclude_defaults, exclude_none, exclude_computed_fields, round_trip, warnings, fallback, serialize_as_any, context) -> bytes` ([Pydantic][1])

Key knobs (same philosophy as `model_dump*`):

* `mode='python'|'json'` (for `dump_python`)
* `warnings` controls what happens on serialization errors (ignore/warn/error)
* `fallback` lets you handle unknown values instead of raising `PydanticSerializationError`
* `serialize_as_any` enables duck-typed serialization (polymorphism) ([Pydantic][1])

### Schema entrypoints

* `json_schema(*, by_alias=True, ref_template=..., union_format=..., schema_generator=GenerateJsonSchema, mode='validation'|'serialization') -> dict` ([Pydantic][1])
* `TypeAdapter.json_schemas(inputs=[(key, mode, adapter), ...], ...) -> (schemas_map, defs_schema)` to generate multiple schemas that share one `$defs` block. ([Pydantic][1])

This is useful when you want:

* multiple related schemas (request vs response, or several endpoint payloads)
* deduped shared definitions (one `$defs`) for documentation or client generation ([Pydantic][1])

---

## M3) Forward refs + rebuilding (the subtle “namespace” gotcha)

Unlike `BaseModel`, `TypeAdapter` may not be able to infer the “defining module” of what you pass it, so its forward-ref resolution strategy can differ: it looks at the **parent stack frame globals/locals** by default, which can produce subtle differences compared with model behavior. ([Pydantic][1])

If schema construction fails due to unresolved `ForwardRef`, TypeAdapter exposes `rebuild(...)` to retry schema compilation, including the ability to pass an explicit types namespace. ([Pydantic][1])

**Practical pattern:** if you use recursive/forward-referenced types with `TypeAdapter`, instantiate adapters in a “stable” module context (or call `rebuild` explicitly after all types are defined).

---

## M4) Performance baseline rule for `TypeAdapter`

Pydantic’s performance guide explicitly calls out: **instantiate `TypeAdapter` once and reuse it**, because each instantiation builds a new validator/serializer. ([Pydantic][2])

```python
from pydantic import TypeAdapter

# module-level (or singleton)
CALL_EDGE_PAYLOAD = TypeAdapter(list[dict[str, int]])

def parse(payload):
    return CALL_EDGE_PAYLOAD.validate_python(payload)
```

---

# N) `validate_call`: “models for functions”

## N1) What it does

`@validate_call` wraps a function so that **arguments are parsed & validated using the function’s annotations before the function is called**. It’s intentionally “minimal boilerplate,” using the same underlying approach as model creation/initialization. ([Pydantic][3])

Signature-wise, it supports:

* `@validate_call`
* `@validate_call(config=..., validate_return=...)` ([Pydantic][4])

---

## N2) Type coercion + strictness

By default, `validate_call` behaves like the rest of Pydantic: it **coerces** where it can (e.g., parsing `'2000-01-01'` into a `date`). Strict mode can be enabled via custom config. ([Pydantic][3])

Also:

* Parameters **without annotations** are inferred as `Any`. ([Pydantic][3])

---

## N3) Return validation (optional)

By default, returns are not validated. If you want output enforcement:

```python
@validate_call(validate_return=True)
def f(x: int) -> int:
    ...
```

This is explicitly controlled by `validate_return=True`. ([Pydantic][3])

---

## N4) Function signature coverage (including “weird” signatures)

`validate_call` is designed to work across basically all Python signature patterns:

* positional-only (`/`)
* keyword-only (`*`)
* varargs / varkwargs (`*args`, `**kwargs`) ([Pydantic][3])

It also supports annotating `**kwargs` via `Unpack[TypedDict]` for structured kwarg validation. ([Pydantic][3])

---

## N5) Adding constraints with `Field` / `Annotated` on function params

You can use the same constraint vocabulary you use in models for function params; the docs show `Annotated[int, Field(gt=10)]` and alias usage with `Field(..., alias='number')`. ([Pydantic][3])

This is a very “agent-friendly” pattern: your function signature becomes your contract.

---

## N6) Escape hatch: `.raw_function`

If you sometimes trust your inputs and want a fast path, the decorated function exposes `.raw_function` so you can bypass validation and call the original implementation directly. ([Pydantic][3])

This is a “trust boundary” tool: validate at the boundary; use raw inside hot loops.

---

## N7) Async support

`validate_call` works on async functions too. ([Pydantic][3])

---

## N8) Limitations (important for correctness + ergonomics)

Two limitations from the docs are worth baking into your “best practices”:

1. On validation failure, **Pydantic raises `ValidationError`**, even in situations where Python would normally raise `TypeError` (e.g., missing required args). ([Pydantic][3])
2. There is **performance overhead** vs calling the raw function (inspection is once, but validation occurs per call). The docs explicitly caution this is not an equivalent to strong static typing in compiled languages. ([Pydantic][3])

---

# Q) Custom types & extension hooks: the “core schema” layer

## Q1) The safe ladder: don’t jump to core schemas first

Pydantic explicitly notes that while it uses `pydantic-core` internally, the core-schema API is newer and more likely to evolve — so **prefer built-in constructs** (`annotated-types`, `Field`, `BeforeValidator`, etc.) unless you need the escape hatch. ([Pydantic][5])

So the ladder is usually:

1. Use built-in types/constraints (`Field`, `Annotated[..., AfterValidator(...)]`, etc.)
2. Use Pydantic’s functional validators/serializers
3. Only then implement `__get_pydantic_core_schema__` / `__get_pydantic_json_schema__`

---

## Q2) The core schema hook: `__get_pydantic_core_schema__`

You can implement `__get_pydantic_core_schema__` either:

* as a method on a custom type, or
* as metadata used inside `Annotated`

In both cases, the API is “middleware-like” (similar to wrap validators): you receive a `source_type` and a `handler`. You can:

* call `handler(source_type)` to get the “next” schema
* modify the schema returned
* call handler on a different type
* or skip handler entirely ([Pydantic][5])

Pydantic’s own internals docs show this wrapper pattern explicitly, including how multiple `Annotated` metadata layers compose. ([Pydantic][6])

### Minimal custom-type example (from official docs)

The “types” concept page gives an example of a `Username(str)` type that uses `core_schema.no_info_after_validator_function` to wrap/convert a validated `str` into your subclass. ([Pydantic][5])

### What `core_schema` is

`pydantic_core.core_schema` is the module defining the schema primitives that pydantic-core can validate and serialize. ([Pydantic][7])

---

## Q3) Reducing boilerplate with `GetPydanticSchema`

If you want the power of custom hooks *without* creating marker classes, `GetPydanticSchema` is a convenience annotation that supplies the hook methods for you (core schema and/or JSON schema). ([Pydantic][8])

This is a great “third-party type adaptation” mechanism: keep type checkers happy (annotate as `int`, etc.), while telling Pydantic to treat it differently. ([Pydantic][8])

---

## Q4) Custom JSON schema: `__get_pydantic_json_schema__` + `GenerateJsonSchema`

Pydantic’s internals docs make two important points:

* The **core schema** is also used to generate JSON Schema. ([Pydantic][6])
* JSON Schema generation is handled by `GenerateJsonSchema`, and Pydantic lets you customize JSON schema generation via a wrapper pattern (`__get_pydantic_json_schema__`). ([Pydantic][6])

On the “consumer” side, TypeAdapter’s `json_schema()` explicitly lets you pass a custom `schema_generator` subclass and choose `mode='validation'|'serialization'`. ([Pydantic][1])

### Skipping schema entirely for a field/type

Pydantic also provides `SkipJsonSchema` as an annotation for excluding a field (or part of a field spec) from generated JSON schema. ([Pydantic][9])

---

## Q5) A practical “custom type” recipe (how to decide what to implement)

When you decide “this really needs to be a custom type,” pick your desired support matrix up front:

* **Python input support** (`validate_python`)
* **JSON input support** (`validate_json`)
* **String-dict input support** (`validate_strings`) (often used for env/CLI-ish sources)
* **Serialization contract** (do you want JSON mode to emit a string? a dict? etc.)
* **Schema contract** (validation vs serialization schema mode)

Then implement:

* `__get_pydantic_core_schema__` to define parse/validate + serialization behavior
* optionally `__get_pydantic_json_schema__` (or `GetPydanticSchema` / `schema_generator`) to align OpenAPI/docs with what clients should see ([Pydantic][5])

---

# S) Performance + internals: making Pydantic “best-in-class fast”

## S1) What’s inside a compiled model/adapter

At the core engine layer, pydantic-core exposes:

* `SchemaValidator` (Rust validation logic wrapper)
* `SchemaSerializer` (Rust serialization logic wrapper) ([Pydantic][10])

`TypeAdapter` exposes these directly as cached attributes:

* `.core_schema`
* `.validator`
* `.serializer` ([Pydantic][1])

And the pydantic-core docs show:

* `SchemaValidator.validate_python(..., strict, from_attributes, context, ...)`
* `SchemaSerializer.to_python(..., mode, include/exclude, by_alias, round_trip, warnings, fallback, ...)` ([Pydantic][10])

This is why `TypeAdapter` is such a strong tool: it’s basically “compiled schema artifacts you can reuse.”

---

## S2) The highest ROI performance rules (from Pydantic’s own tips)

### Prefer `model_validate_json()` over `model_validate(json.loads(...))`

Pydantic explains that `model_validate_json()` avoids a Python JSON parse + intermediate dict and performs validation internally; it also notes a few corner cases where the two-step path can be faster (notably with model `'before'` / `'wrap'` validators). ([Pydantic][2])

### Instantiate `TypeAdapter` once

Repeated adapter instantiation rebuilds validators/serializers; reuse them. ([Pydantic][2])

### Use concrete containers (`list`/`tuple`, `dict`) instead of `Sequence`/`Mapping` when you can

The performance guide explains that generic ABCs can trigger extra checks and “try multiple sequence types.” ([Pydantic][2])

### Avoid wrap validators in hot paths

Wrap validators force more Python materialization and are generally slower; use them for complex logic, not for the “million calls/sec” path. ([Pydantic][2])

### Prefer `TypedDict` for deeply nested structures when you mainly want speed

Pydantic provides a benchmark example showing `TypedDict` can be substantially faster than nested models. ([Pydantic][2])

### Fail fast for long sequences

`FailFast` is an annotation to stop validating a sequence after the first failure, trading rich error reporting for throughput. ([Pydantic][2])

---

## S3) Config-level performance knob: `cache_strings`

`ConfigDict(cache_strings=...)` can significantly improve validation speed by caching repeated strings, at a small memory cost. The docs describe modes: cache all strings, cache only keys, or none, and give guidance on when to choose `'keys'` vs `'none'`. ([Pydantic][11])

---

## S4) “Validated at the boundary” pattern (recommended architecture)

For best throughput and clean trust boundaries:

* Use `model_validate_json` / `TypeAdapter.validate_json` **at IO boundaries** (HTTP, file IO, IPC).
* Inside your system, pass around *already validated objects*, and only revalidate if you cross a trust zone.
* If you use `validate_call`, consider:

  * validate at the API edge,
  * then call `.raw_function` in performance-sensitive inner loops. ([Pydantic][3])

---

## Next deep dive

If you want to continue the original sequence after this cluster, the natural next chapter is **T) Migration & compatibility** (v1→v2 mapping, “drift-prone” areas like validators/serialization/schema, and a testing strategy with golden schemas + golden error shapes).

[1]: https://docs.pydantic.dev/latest/api/type_adapter/ "TypeAdapter - Pydantic Validation"
[2]: https://docs.pydantic.dev/latest/concepts/performance/ "Performance - Pydantic Validation"
[3]: https://docs.pydantic.dev/latest/concepts/validation_decorator/ "Validation Decorator - Pydantic Validation"
[4]: https://docs.pydantic.dev/latest/api/validate_call/ "Validate Call - Pydantic Validation"
[5]: https://docs.pydantic.dev/latest/concepts/types/ "Types - Pydantic Validation"
[6]: https://docs.pydantic.dev/latest/internals/architecture/ "Architecture - Pydantic Validation"
[7]: https://docs.pydantic.dev/latest/api/pydantic_core_schema/ "pydantic_core.core_schema - Pydantic Validation"
[8]: https://docs.pydantic.dev/latest/api/types/ "Pydantic Types - Pydantic Validation"
[9]: https://docs.pydantic.dev/latest/concepts/json_schema/?utm_source=chatgpt.com "JSON Schema - Pydantic Validation"
[10]: https://docs.pydantic.dev/2.2/api/pydantic_core/ "pydantic_core - Pydantic"
[11]: https://docs.pydantic.dev/latest/api/config/ "Configuration - Pydantic Validation"

## T) Migration & compatibility (Pydantic v1 → v2, drift-prone areas, and contract testing)

This chapter is best thought of as: **(1) upgrade strategy**, **(2) what actually changes**, and **(3) how to keep your system “contract-stable” despite allowed drift in minor releases**.

Pydantic’s own docs make two high-level constraints explicit:

* **v2 minor releases are intended to be non-breaking**, but *some kinds of change are explicitly allowed* (notably JSON Schema formatting and some `ValidationError` fields). ([Pydantic][1])
* **Pydantic v1 is not compatible with Python 3.14+**, so if you’re on 3.14 you’re effectively “v2-only.” ([Pydantic][2])

---

# 1) Migration strategy: how to upgrade without chaos

## 1.1 Pick your upgrade posture

You basically have three workable paths:

### A) “Big bang” (small codebase or few models)

* Convert models/validators/config/serialization in one push.
* Add contract tests (schemas + error shapes) **before** the conversion so you can see what changed.

### B) “Bridged” migration (recommended for large codebases)

Use the **`pydantic.v1` namespace** to keep v1 code compiling while you port incrementally.

Pydantic v2 continues to ship the v1 API under `pydantic.v1`, and even v1.10.17+ supports that namespace to ease gradual migration. ([Pydantic][3])

This lets you do:

* `from pydantic.v1 import BaseModel` for legacy models
* gradually rewrite models to v2 (`from pydantic import BaseModel`) module-by-module

**Caveat:** don’t “mix and match” v1 and v2 models in ways the docs warn against (e.g., generics where type params are v1 models). ([Pydantic][3])

### C) “Stay on v1” (only if you’re pinned below 3.14)

If you truly need v1 features, you can still install `"pydantic==1.*"`—but again, Python 3.14+ forces you off v1. ([Pydantic][3])

---

## 1.2 Use the official conversion helper (but don’t trust it blindly)

Pydantic provides a code transformation tool (`bump-pydantic`) and positions it as beta / helpful accelerator. ([Pydantic][3])

Use it to get 70–80% of the boring mechanical edits done (imports, method renames), then manually handle the semantic shifts (validators, config policy, schema diffs).

---

# 2) API mapping: the “what renamed to what” cheat sheet

Pydantic v2 regularized method names: most of the “main” APIs now start with `model_...`. The migration guide includes an explicit mapping table. ([Pydantic][3])

### BaseModel method/attribute renames you’ll touch constantly

* `construct()` → `model_construct()`
* `copy()` → `model_copy()`
* `dict()` → `model_dump()`
* `json()` → `model_dump_json()`
* `parse_obj()` → `model_validate()`
* `update_forward_refs()` → `model_rebuild()`
* `json_schema()` → `model_json_schema()`
* `__fields__` → `model_fields` ([Pydantic][3])

### Data-loading deprecations

* `parse_raw` / `parse_file` are deprecated; `model_validate_json` is the v2-era “parse raw JSON” entrypoint (otherwise, you load data yourself then call `model_validate`). ([Pydantic][3])

### ORM-style population

* `from_orm` is deprecated; use `model_validate(...)` with `from_attributes=True` set in model config. ([Pydantic][3])

### Root models

* v1’s `__root__` pattern is replaced by `RootModel`. ([Pydantic][3])

---

# 3) Semantic drift hotspots (things that break logic, not just imports)

These are the places where “it still runs” but behavior changes.

## 3.1 Required vs Optional vs nullable (biggest silent break)

In v2, `Optional[T]` **no longer implies a default of `None`**. If there’s no default, the field is required (but can be `None`). This was a breaking change made to align with dataclasses. ([Pydantic][3])

Example (v2 semantics):

* `f2: Optional[str]` → required, can be `None`
* `f3: Optional[str] = None` → not required, defaults to `None` ([Pydantic][3])

If you previously used “Optional means optional input,” you must add explicit defaults.

---

## 3.2 Union behavior (smart short-circuit vs v1 “left-to-right”)

In v2, unions can **short-circuit when the input already matches one union arm**, even if earlier arms could coerce it (classic example: `'1'` no longer becomes `1` for `int | str` when input is already a `str`). ([Pydantic][3])

If you need v1’s non-short-circuit left-to-right behavior, v2 supports a `union_mode='left_to_right'` approach via `Field(...)`. ([Pydantic][3])

---

## 3.3 Input type preservation is gone (unless you force it)

v1 tried hard to preserve input container subtypes (e.g., `Counter` stays `Counter`). v2 does not promise this; it promises the **output matches the annotation**, often producing plain `dict`/`list`. ([Pydantic][3])

If you need to preserve the input type, the migration guide demonstrates a `WrapValidator` pattern to re-wrap the validated result into the original type. ([Pydantic][3])

---

## 3.4 Dict coercion changes

Iterables of pairs no longer validate as `dict` by default (a common v1 convenience). ([Pydantic][3])

---

# 4) Config migration: “policy moved from class Config to model_config”

## 4.1 `model_config` replaces inner `Config` (v1 style deprecated)

In v2, config lives on `model_config` (a dict / `ConfigDict`), and the v1 inner `Config` class approach is deprecated. ([Pydantic][3])

## 4.2 Inheritance and merging are explicit

* `model_config` is inherited
* if you do multiple inheritance, non-default config merges and later bases override earlier ones ([Pydantic][3])

## 4.3 Removed vs renamed config keys (high ROI to scan)

The migration guide lists removals and renames; the ones that bite most:

**Removed config settings (examples):**

* `allow_mutation` → use `frozen`
* `fields` removed (use `Annotated` field metadata instead)
* `orm_mode` removed (use `from_attributes`)
* `smart_union` removed; v2 default union mode is “smart”
* `json_loads` / `json_dumps` removed ([Pydantic][3])

**Renamed config settings (examples):**

* `allow_population_by_field_name` → `populate_by_name` (or `validate_by_name` starting in v2.11)
* `orm_mode` → `from_attributes`
* `schema_extra` → `json_schema_extra`
* `validate_all` → `validate_default`
* `anystr_*` → `str_*` equivalents ([Pydantic][3])

---

# 5) Validators & serialization migration: “where most real breakage lives”

## 5.1 Validators: v1 decorators deprecated

* `@validator` → `@field_validator`
* `@root_validator` → `@model_validator` ([Pydantic][3])

Key semantic change: `each_item=True` is gone on `@field_validator`; the migration guide recommends validating container items by annotating the type parameter (e.g., `list[Annotated[int, Field(ge=0)]]`). ([Pydantic][3])

Also: `always=True` in v1 is replaced conceptually by `validate_default` (field/config), and the migration guide notes this is the preferred control mechanism. ([Pydantic][3])

## 5.2 Serialization: v2 adds first-class serializer decorators

The migration guide calls out the expanded serialization system and the addition of:

* `@field_serializer`
* `@model_serializer`
* `@computed_field` ([Pydantic][3])

If you had v1-era `json_encoders` hacks, you generally migrate them into explicit serializers (and you’ll also want to think about JSON-mode vs Python-mode output).

---

# 6) JSON Schema & OpenAPI changes (and why schema snapshots will churn)

The migration guide explicitly states v2’s JSON Schema defaults changed:

* targets **draft 2020-12** (with OpenAPI extensions)
* Optional shows `null` allowed properly
* `Decimal` is exposed (and serialized) as a **string**
* you can choose schema representing **validation inputs** vs **serialization outputs**
* schema generation is now structured around `GenerateJsonSchema` to make customization feasible ([Pydantic][3])

**Practical consequence:** if you snapshot schemas, you should snapshot both:

* `mode='validation'`
* `mode='serialization'`

…because these can legitimately differ now. ([Pydantic][3])

---

# 7) Custom types & schema hooks (v1 → v2)

If you used:

* `__get_validators__` (v1 custom types)
* `__modify_schema__` (v1 schema customization)

v2’s replacements are:

* `__get_pydantic_core_schema__`
* `__get_pydantic_json_schema__` ([Pydantic][3])

Additionally, v2 encourages using `typing.Annotated` to attach custom behavior to third-party types *without* modifying the type itself. ([Pydantic][3])

---

# 8) “Allowed drift” in v2 minor releases (what can change without being “breaking”)

Pydantic’s version policy is unusually explicit about this:

Even though they don’t *intend* breaking changes in v2 minor releases, the following are explicitly allowed:

* JSON Schema **reference format** can change
* `ValidationError` fields like `msg`, `ctx`, and `loc` may change; **`type` is the stable programmatic key**
* new keys may be added to `ValidationError`
* core schema contents (`__pydantic_core_schema__`, `TypeAdapter.core_schema`) may change between releases ([Pydantic][1])

This directly informs your testing strategy.

---

# 9) Testing strategy: “golden schema + golden error shapes” without fragile assertions

## 9.1 Golden JSON Schema snapshots (validation + serialization)

Snapshot both modes per model/type and store as canonical artifacts.

* `Model.model_json_schema(mode='validation')`
* `Model.model_json_schema(mode='serialization')`

Expect churn: the version policy explicitly allows schema reference format changes in minor releases. Treat snapshot diffs as “upgrade review work,” not as a sign your system is broken. ([Pydantic][1])

**Practical tip:** normalize before snapshotting:

* `json.dumps(schema, sort_keys=True, indent=2)`
* optionally strip volatile metadata you don’t want to gate on (titles, examples) depending on your goals

## 9.2 Golden error-shape tests (assert on `type` + `loc`, not `msg`)

Because `msg/ctx/loc` are allowed to change (and new keys may be added), base your stability checks on:

* `err["type"]` (stable)
* `err["loc"]` (usually stable enough to keep, but treat as less sacred than `type`) ([Pydantic][1])

Also: you can make errors more stable by excluding URLs/inputs/context when producing the list.

The core error API supports:

* `errors(include_url=..., include_context=..., include_input=...)` ([Pydantic][4])

So in tests, you can do something like:

* `e.errors(include_url=False, include_input=False, include_context=True)`
  and then compare only `(type, loc)` or `(type, loc, ctx)`.

## 9.3 “Contract suite” layout (recommended)

A clean setup that scales:

* `tests/contracts/schemas/<ModelName>.validation.json`
* `tests/contracts/schemas/<ModelName>.serialization.json`
* `tests/contracts/errors/<ModelName>__<case>.json` (containing the reduced error list)

Add a `--regen-contracts` test option (or a tiny script) to regenerate snapshots intentionally during upgrades.

## 9.4 What not to snapshot

Don’t snapshot:

* internal/core schemas (`__pydantic_core_schema__`), since the version policy explicitly allows them to change. ([Pydantic][1])

---

# 10) Upgrade checklist (the part that prevents “surprise breakage”)

1. **Pin versions** (pydantic + pydantic-core + pydantic-settings + fastapi if relevant).
2. Add contract tests for:

   * schema (validation + serialization)
   * error shapes (type/loc)
3. Run migration tool (`bump-pydantic`) if helpful, then manually fix:

   * `Optional` requiredness assumptions ([Pydantic][3])
   * unions relying on v1 coercion order ([Pydantic][3])
   * dict coercion patterns ([Pydantic][3])
   * validators: `@validator`/`@root_validator` → new APIs ([Pydantic][3])
   * config renames/removals ([Pydantic][3])
4. Upgrade and review schema diffs as a first-class artifact (they’re allowed to change). ([Pydantic][1])
5. If you’re on Python 3.14+, confirm you’re not accidentally depending on v1. ([Pydantic][2])

---

If you want the next increment, I can turn this into a **repo-ready “Migration Playbook” template** (a Markdown doc with: contract test scaffolding, a checklist of grep-able v1 APIs to replace, and an explicit “policy matrix” for config + validators + schema modes).

[1]: https://docs.pydantic.dev/latest/version-policy/ "Version Policy - Pydantic Validation"
[2]: https://docs.pydantic.dev/latest/changelog/ "Changelog - Pydantic Validation"
[3]: https://docs.pydantic.dev/latest/migration/ "Migration Guide - Pydantic Validation"
[4]: https://docs.pydantic.dev/latest/api/pydantic_core/?utm_source=chatgpt.com "pydantic_core - Pydantic Validation"


