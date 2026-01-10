Below is a *v2-first* field manual for **Pydantic** that’s tuned for an AI programming agent migrating off ad‑hoc validators and hand‑rolled serializers. It focuses on the v2 concepts, names, and patterns you’ll actually use, plus side‑by‑side “v1 → v2” replacements.

---

## 0) Install & scope

```bash
pip install "pydantic>=2"        # core library (validation/serialization)
pip install pydantic-settings    # settings loading (moved out of core)
```

Pydantic v2 defines models in Python and performs validation/serialization in a Rust core (**pydantic‑core**) for speed; most “work” runs in the core via a generated *core schema*. ([docs.pydantic.dev][1])

---

## 1) Mental model: models, core, and methods you’ll call

* **BaseModel** defines a schema using type hints; Pydantic builds a *core schema* and asks **pydantic‑core** to validate/serialize. ([docs.pydantic.dev][1])
* Prefer v2 method names (all start with `model_…`):

  * **validate**: `Model.model_validate(obj_or_dict)`, `Model.model_validate_json(bytes_or_str)`
  * **serialize**: `inst.model_dump()`, `inst.model_dump_json()`
  * **schema**: `Model.model_json_schema()`
  * **construct (no validation)**: `Model.model_construct(...)`
  * **forward refs**: `Model.model_rebuild()`
    The v1 equivalents (`parse_obj`, `dict`, `json`, `parse_raw`, `update_forward_refs`, etc.) are deprecated; see full mapping in §10. ([docs.pydantic.dev][2])
* **TypeAdapter[T]**: validate/serialize *any* annotated type without creating a model (e.g., `list[User]`, `dict[str, UUID]`). It mirrors model methods: `validate_python`, `validate_json`, `dump_python`, `dump_json`, `json_schema`. ([docs.pydantic.dev][3])

---

## 2) Quick start patterns

### 2.1 Validate a payload into a model

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    email: str

u = User.model_validate({"id": "123", "email": "a@b.com"})
```

`model_validate` is the v2 replacement for v1’s `parse_obj`. ([docs.pydantic.dev][2])

### 2.2 Validate arbitrary types without models

```python
from typing import Annotated
from pydantic import TypeAdapter, StringConstraints

Emails = list[Annotated[str, StringConstraints(pattern=r".+@.+")]]
emails = TypeAdapter(Emails).validate_python(["a@b.com", "c@d.com"])
```

Use `TypeAdapter` for list/dict/union trees, or to validate return values. ([docs.pydantic.dev][3])

### 2.3 Serialize (Python vs JSON mode)

```python
d = u.model_dump()              # Python mode
j = u.model_dump_json(indent=2) # JSON string
```

You can switch to JSON-compatible primitives via `mode='json'` when dumping to Python. ([docs.pydantic.dev][4])

---

## 3) Fields & constraints (v2 style)

Define fields with type hints; augment with `Field(...)` or with `typing.Annotated[...]`.

```python
from typing import Annotated
from pydantic import BaseModel, Field, StringConstraints

class Product(BaseModel):
    sku: Annotated[str, StringConstraints(min_length=3)]
    qty: int = Field(ge=1)
```

Key options you’ll reach for:

* **Defaults**: `default=` or `default_factory=`; by default, defaults are **not** validated unless you set `validate_default=True` on the field. ([docs.pydantic.dev][5])
* **Aliases**:

  * single alias for both: `Field(alias="username")`
  * separate **validation** and **serialization** aliases: `validation_alias=...`, `serialization_alias=...`
  * generator: `model_config = ConfigDict(alias_generator=..., validate_by_alias=True)`
  * populate by field name as well: `validate_by_name=True` (v2.11+ granular controls). ([docs.pydantic.dev][6])
* **Strictness**:

  * per field: `Field(strict=True)`
  * whole model: `model_config = ConfigDict(strict=True)`
  * or use strict types like `StrictInt`, `StrictStr` when needed. ([docs.pydantic.dev][7])

---

## 4) Validators you’ll actually need (and their modes)

In v2 you use **field** and **model** validators, each with explicit *modes*.

### 4.1 Field validators

```python
from typing import Any
from pydantic import BaseModel, field_validator

class Signup(BaseModel):
    age: int
    @field_validator("age", mode="after")
    @classmethod
    def check_age(cls, v: int) -> int:
        if v < 13:
            raise ValueError("too young")
        return v
```

* Modes: **before** (raw input), **after** (post‑parse), **plain** (bypass Pydantic’s own validation), **wrap** (around validation). Return the value. ([docs.pydantic.dev][8])

### 4.2 Model validators

```python
from pydantic import BaseModel, model_validator

class Passwords(BaseModel):
    a: str
    b: str
    @model_validator(mode="after")
    def passwords_match(self):
        if self.a != self.b:
            raise ValueError("mismatch")
        return self
```

Use model validators for cross‑field checks; you return the instance for `mode="after"`. ([docs.pydantic.dev][8])

> **Migration note:** v1’s `@validator`/`@root_validator` are deprecated; switch to `@field_validator`/`@model_validator`. ([docs.pydantic.dev][2])

---

## 5) Serialization customization (dumping)

* **Field‑level**: `@field_serializer("field", mode="plain" | "wrap")`
* **Model‑level**: `@model_serializer(mode="plain" | "wrap")`
* **Computed fields** (include properties in dumps): `@computed_field`

```python
from pydantic import BaseModel, field_serializer, model_serializer, computed_field

class User(BaseModel):
    first: str
    last: str

    @computed_field
    @property
    def full_name(self) -> str:
        return f"{self.first} {self.last}"

    @field_serializer("first", mode="plain")
    def title_case(self, v: str):  # returns serialized value
        return v.title()

    @model_serializer(mode="wrap")
    def as_public(self, handler):
        data = handler(self)
        data.pop("last", None)
        return data
```

Only **one** serializer per field/model can be active at a time. Computed fields serialize but can have limitations interacting with some generic `field_serializer("*")` patterns. ([docs.pydantic.dev][4])

Dumping knobs you’ll use frequently: `by_alias`, `exclude_none`, `exclude_unset`, `mode='json'`, `context={...}`, and `serialize_as_any`. ([docs.pydantic.dev][4])

---

## 6) Aliases, again (because they’re crucial when integrating)

* Use `alias` for both directions, or split with `validation_alias` / `serialization_alias`.
* For nested sources, use `validation_alias=AliasPath("outer", "inner")` or `AliasChoices(...)`.
* You can auto‑generate aliases with `alias_generator` (plus `validate_by_alias=True`/`validate_by_name=True` to control input behavior). ([docs.pydantic.dev][6])

---

## 7) Strict mode and coercion

* Default validation is *lax*: `int` will accept `"3"`.
* Turn on strictness per field or model (see §3), or use strict types.
* Strict mode affects unions and enums—be deliberate if you enable it globally. ([docs.pydantic.dev][7])

---

## 8) Advanced schema building

* **RootModel[T]** replaces v1’s `__root__` to wrap a single typed value. ([docs.pydantic.dev][9])
* **Discriminated unions**: add a discriminator (string name or a `Discriminator` callable) via `Field(discriminator="kind")`. Faster validation, clearer errors, OpenAPI‑ready schema. ([docs.pydantic.dev][10])
* **Dataclasses**: `pydantic.dataclasses.dataclass` for stdlib‑like dataclasses with validation. Great when you want dataclass ergonomics; it’s not a full model replacement. ([docs.pydantic.dev][11])
* **Generics**: supported; schema is built once you call `model_rebuild()` with all types in scope (see §10). ([docs.pydantic.dev][12])

---

## 9) JSON Schema generation

* **Models**: `Model.model_json_schema()` → JSON Schema (Draft 2020‑12 / OpenAPI 3.1).
* **TypeAdapter**: `TypeAdapter(T).json_schema()` for ad‑hoc types. ([docs.pydantic.dev][13])

---

## 10) Configuration (v2 `ConfigDict`)

Attach to the model via `model_config = ConfigDict(...)`. Common flags:

* **extra**: `'ignore' | 'forbid' | 'allow'`
* **from_attributes**: True (v1 “ORM mode”)
* **strict**: True (global strict mode)
* **frozen**: True (pseudo‑immutable)
* **validate_assignment**: True
* **revalidate_instances**: `'never'|'always'|'subclass-instances'` (controls re‑validation when passing model instances to other models)
* **alias controls**: `validate_by_alias`, `validate_by_name` (v2.11+ granularity) ([docs.pydantic.dev][14])

---

## 11) Errors & how to raise them

* Pydantic raises **ValidationError** and aggregates all issues; inside your validators, raise `ValueError`/`AssertionError` and Pydantic will translate them. Use `.errors()` for structured details. You can customize message behavior via config (e.g., `hide_input_in_errors`). ([docs.pydantic.dev][15])

---

## 12) JSON parsing helpers

* Use `model_validate_json(...)` to parse JSON bytes/str directly into models.
* For ad‑hoc or streaming cases, **pydantic‑core** exposes fast JSON helpers (including *partial* JSON parsing). ([docs.pydantic.dev][4])

---

## 13) Settings management (moved out of core)

App/config settings live in **pydantic‑settings**:

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    db_url: str
    class Config:
        env_file = ".env"
```

Install and import from `pydantic_settings`; v1’s `BaseSettings` was removed from core. ([docs.pydantic.dev][16])

---

## 14) v1 → v2 migration cheat‑sheet (use these replacements today)

| v1                                 | v2                                        |
| ---------------------------------- | ----------------------------------------- |
| `MyModel.parse_obj(data)`          | `MyModel.model_validate(data)`            |
| `MyModel.parse_raw(json)`          | `MyModel.model_validate_json(json)`       |
| `inst.dict()`                      | `inst.model_dump()`                       |
| `inst.json()`                      | `inst.model_dump_json()`                  |
| `MyModel.schema()`/`schema_json()` | `MyModel.model_json_schema()`             |
| `MyModel.construct()`              | `MyModel.model_construct()`               |
| `MyModel.update_forward_refs()`    | `MyModel.model_rebuild()`                 |
| `@validator` / `@root_validator`   | `@field_validator` / `@model_validator`   |
| `__root__` models                  | `RootModel[T]`                            |
| `from_orm=True` + `parse_obj`      | `from_attributes=True` + `model_validate` |

Full mapping and gotchas (including changed equality semantics and URL types no longer `str` subclasses) are documented in the v2 migration guide. ([docs.pydantic.dev][2])

---

## 15) Practical patterns for an AI agent

### 15.1 Schema‑first normalization (no models needed)

```python
from typing import Annotated
from pydantic import TypeAdapter, StringConstraints

Payload = dict[str, Annotated[str, StringConstraints(min_length=1)]]
normalized = TypeAdapter(Payload).validate_python(raw_payload)
```

Use `TypeAdapter` when you only need structure/cleanup and don’t want a class around it. ([docs.pydantic.dev][3])

### 15.2 Robust ingestion with aliases and strict fields

```python
from pydantic import BaseModel, Field, ConfigDict

class InboundUser(BaseModel):
    model_config = ConfigDict(strict=True, validate_by_alias=True)  # error on "3" for int
    id: int
    email: str = Field(validation_alias="emailAddress")             # accept legacy field
```

Control inputs via strictness and `validation_alias`; keep outputs stable with `serialization_alias` or `by_alias=True`. ([docs.pydantic.dev][17])

### 15.3 Cross‑field business rules

Prefer `@model_validator(mode="after")` over scattering checks across field validators; it sees the whole instance and you return `self`. ([docs.pydantic.dev][8])

### 15.4 Streaming/partial JSON

For partial or streaming data, leverage **pydantic‑core**’s JSON helpers; they can parse even when the buffer is incomplete if you enable “partial” parsing. ([docs.pydantic.dev][18])

---

## 16) Pitfalls & gotchas (v2 specifics)

* **Defaults aren’t validated** unless `validate_default=True` (per field) or you trigger validation explicitly. ([docs.pydantic.dev][5])
* **Re‑validation of instances is off by default** (`revalidate_instances='never'`), so passing a `User` instance into a `Transaction` field won’t re‑validate unless configured. ([docs.pydantic.dev][14])
* **Only one serializer** per field/model—combining multiple serializers (or mixing “plain” and “wrap”) isn’t supported. ([docs.pydantic.dev][4])
* **Computed fields** serialize but may not work with some “wildcard” field serializers (`field_serializer("*")`)—be explicit. ([GitHub][19])
* **Raising errors**: inside validators, raise `ValueError` or `AssertionError`, not `ValidationError`. Pydantic aggregates them into a single `ValidationError`. ([docs.pydantic.dev][15])
* **URL/network types** no longer inherit from `str`; use `str(url)` to convert when needed. ([docs.pydantic.dev][2])

---

## 17) Frequently used APIs (one‑glance)

* **Validation**: `model_validate`, `model_validate_json`, `TypeAdapter.validate_python`, `TypeAdapter.validate_json` ([docs.pydantic.dev][2])
* **Serialization**: `model_dump(mode='python'|'json', by_alias=..., exclude_none=...)`, `model_dump_json(...)` ([docs.pydantic.dev][4])
* **Customization**: `@field_validator`, `@model_validator`, `@field_serializer`, `@model_serializer`, `@computed_field` ([docs.pydantic.dev][8])
* **Config**: `ConfigDict(extra=..., strict=..., from_attributes=True, revalidate_instances=..., validate_by_alias=True, validate_by_name=True)` ([docs.pydantic.dev][14])
* **Aliases**: `Field(alias=..., validation_alias=..., serialization_alias=...)`, `AliasPath`, `AliasChoices` ([docs.pydantic.dev][6])
* **Schema**: `model_json_schema()`, `TypeAdapter(T).json_schema()` ([docs.pydantic.dev][13])

---

## 18) Forward references & generics

If your annotations reference classes defined later, call **`model_rebuild()`** after all types are defined (replaces v1 `update_forward_refs`). This also (re)builds the underlying core schema for nested types. ([docs.pydantic.dev][12])

---

## 19) Where settings/config went

Environment‑driven configuration (dotenv, env vars) now lives in **pydantic‑settings**. Keep your domain models lean; use `BaseSettings` classes to load configuration. ([docs.pydantic.dev][16])

---

## 20) Why v2 is faster (just enough internals)

The Python layer builds a typed *core schema*; **pydantic‑core** (Rust) executes validation/serialization against that schema. That separation is the source of most speedups and architectural clarity. ([docs.pydantic.dev][1])

---

### Minimal starter template (copy/paste)

```python
from typing import Annotated
from pydantic import BaseModel, Field, ConfigDict, field_validator, computed_field, StringConstraints

class User(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        validate_by_alias=True,
        validate_by_name=True,
        from_attributes=True,
    )

    id: int
    email: Annotated[str, StringConstraints(pattern=r".+@.+")]
    name: str = Field(serialization_alias="fullName",
                      validation_alias="full_name")

    @field_validator("name", mode="after")
    @classmethod
    def trim_name(cls, v: str) -> str:
        return v.strip()

    @computed_field
    @property
    def domain(self) -> str:
        return self.email.split("@", 1)[1]

# Validate from dict-like or attribute objects:
user = User.model_validate({"id": 1, "email": "a@b.com", "full_name": "  Ada  "})
print(user.model_dump(by_alias=True, exclude_none=True))
# {'id': 1, 'email': 'a@b.com', 'fullName': 'Ada', 'domain': 'b.com'}
```

---

## References (selected)

* **Core docs**: Models, serialization, validators, fields, unions, JSON schema, config, strict mode, TypeAdapter, dataclasses, validate_call, architecture. ([docs.pydantic.dev][20])
* **Migration guide (v1 → v2 mapping & behavior changes)**. ([docs.pydantic.dev][2])
* **Aliases & generators** (alias/validation_alias/serialization_alias; AliasPath/Choices; precedence). ([docs.pydantic.dev][6])

---

If you share a snippet of your custom validators/serializers, I can map each piece to the closest v2 pattern (field/model validator, serializer, computed field, TypeAdapter, aliasing, or config) in this style.

[1]: https://docs.pydantic.dev/latest/internals/architecture/?utm_source=chatgpt.com "Architecture - Pydantic Validation"
[2]: https://docs.pydantic.dev/latest/migration/ "Migration Guide - Pydantic Validation"
[3]: https://docs.pydantic.dev/latest/api/type_adapter/?utm_source=chatgpt.com "TypeAdapter - Pydantic Validation"
[4]: https://docs.pydantic.dev/latest/concepts/serialization/ "Serialization - Pydantic Validation"
[5]: https://docs.pydantic.dev/latest/concepts/fields/ "Fields - Pydantic Validation"
[6]: https://docs.pydantic.dev/latest/concepts/alias/?utm_source=chatgpt.com "Alias - Pydantic Validation"
[7]: https://docs.pydantic.dev/latest/concepts/strict_mode/?utm_source=chatgpt.com "Strict Mode - Pydantic Validation"
[8]: https://docs.pydantic.dev/latest/concepts/validators/ "Validators - Pydantic Validation"
[9]: https://docs.pydantic.dev/latest/api/root_model/?utm_source=chatgpt.com "RootModel - Pydantic Validation"
[10]: https://docs.pydantic.dev/latest/concepts/unions/?utm_source=chatgpt.com "Unions - Pydantic Validation"
[11]: https://docs.pydantic.dev/latest/concepts/dataclasses/?utm_source=chatgpt.com "Dataclasses - Pydantic Validation"
[12]: https://docs.pydantic.dev/latest/concepts/models/ "Models - Pydantic Validation"
[13]: https://docs.pydantic.dev/latest/concepts/json_schema/?utm_source=chatgpt.com "JSON Schema - Pydantic Validation"
[14]: https://docs.pydantic.dev/latest/api/config/ "Configuration - Pydantic Validation"
[15]: https://docs.pydantic.dev/2.5/errors/errors/?utm_source=chatgpt.com "Error Handling"
[16]: https://docs.pydantic.dev/latest/concepts/pydantic_settings/?utm_source=chatgpt.com "Settings Management - Pydantic Validation"
[17]: https://docs.pydantic.dev/latest/concepts/fields/?utm_source=chatgpt.com "Fields - Pydantic Validation"
[18]: https://docs.pydantic.dev/latest/concepts/json/?utm_source=chatgpt.com "JSON - Pydantic Validation"
[19]: https://github.com/pydantic/pydantic/issues/9683?utm_source=chatgpt.com "field_serializer(\"*\") is not compatible with @computed_field ..."
[20]: https://docs.pydantic.dev/latest/concepts/models/?utm_source=chatgpt.com "Models - Pydantic Validation"
