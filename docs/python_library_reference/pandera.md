Below is a **clustered, “surface-area-first” catalog** of **Pandera’s feature set**, styled to be a close analog of your PyArrow reference. 

---

# Pandera — comprehensive feature catalog (DataFrame-like validation + “statistical typing”)

## Mental model: Pandera = (Schema objects) + (Checks & Parsers) + (Execution controls) + (Backends) + (Integrations)

Pandera validates **schema-level** properties (column presence, dtype, index shape) and **data-level** properties (value constraints, statistical tests) across multiple dataframe ecosystems (pandas, polars, pyspark, dask, modin, ibis, etc.). ([PyPI][1])

---

# 1) Backends + import namespaces + extras

## 1.1 “Backend namespaces” (how you select semantics)

Pandera exposes backend-specific entrypoints (the ones you actually import in real code):

* **Pandas**: `import pandera.pandas as pa` (primary feature-complete backend) ([PyPI][1])
* **Polars**: `import pandera.polars as pa` (validates `polars.LazyFrame` / `polars.DataFrame`) ([Pandera][2])
* **PySpark SQL**: `import pandera.pyspark as pa` (validates `pyspark.sql.DataFrame` with spark-native semantics) ([Pandera][3])
* **Ibis**: `import pandera.ibis as pa` (validates lazy/eager ibis tables; schema-only checks can avoid `execute()`) ([Pandera][4])

…and “pandas-like distributed” variants via typing modules: Dask, Modin, pyspark.pandas, GeoPandas, Fugue. ([Pandera][5])

## 1.2 Install-time feature gating (extras)

Pandera supports multiple libraries and ships extras such as `pandera[pandas]`, `pandera[polars]`, `pandera[pyspark]`, `pandera[dask]`, etc. ([PyPI][1])

---

# 2) Object-based schema API (core primitives)

## 2.1 `DataFrameSchema` (schema for a table-like object)

The central contract object is `DataFrameSchema(columns=..., index=..., checks=..., parsers=..., …)` with major “power knobs”: ([Pandera][6])

* **Column set contract**

  * `columns: dict[name -> Column]`
  * `strict: bool | "filter"`

    * `strict=True`: require *only* schema columns exist
    * `strict="filter"`: drop extra columns, but still require schema columns to exist ([Pandera][6])
  * `ordered: bool` (validate column order) ([Pandera][6])
  * `unique_column_names: bool` (enforce unique names) ([Pandera][6])
  * `add_missing_columns: bool` (add missing columns using defaults or nulls if nullable) ([Pandera][6])

* **Type contract**

  * Per-column dtype via each `Column(dtype=...)`
  * Optional dataframe-wide `dtype=...` overriding column dtypes
  * `coerce: bool` (attempt dtype coercion during validation; can override per-column/index) ([Pandera][6])

* **Uniqueness contract**

  * `unique: str | list[str] | None` (joint uniqueness across columns)
  * `report_duplicates: "exclude_first"|"exclude_last"|"all"` ([Pandera][6])

* **DataFrame-wide checks/parsers**

  * `checks=[Check | Hypothesis | ...]` (wide constraints across columns)
  * `parsers=[Parser | ...]` (preprocessing) ([Pandera][6])

* **Metadata**

  * `name`, `title`, `description`, plus schema-level metadata accessors ([Pandera][6])

## 2.2 Column/Index building blocks

### `Column`

`Column(dtype=..., checks=..., parsers=..., nullable=..., unique=..., coerce=..., required=..., regex=..., default=..., metadata=..., drop_invalid_rows=...)` ([Pandera][7])

Key capabilities:

* **Presence**: `required=True|False`
* **Match many columns**: `regex=True` and `name=<pattern>`
* **Defaults**: `default=<value>` (works with add-missing behavior in some backends)
* **Row-dropping**: `drop_invalid_rows=True` (pandas path; requires lazy validation) ([Pandera][7])

### Index types

* `Index(dtype, checks=..., nullable=..., coerce=..., name=...)`
* `MultiIndex([...Index components...])`
* `SeriesSchema(dtype, checks=..., nullable=..., coerce=..., …)`
  (These participate in `coerce`, `nullable`, and the lazy/error-reporting model the same way as columns.) ([Pandera][8])

## 2.3 Schema composition & “schema refactor” utilities

The schema objects are designed to be **transformed/composed** (useful for pipeline staging and test harnesses): ([Pandera][9])

* `add_columns({name: Column, ...})`
* `remove_columns([name, ...])`
* `rename_columns({old: new, ...})`
* `rename_indexes({old: new, ...})`
* `reset_index(...)`
* dtype helpers like `get_dtypes(check_obj)` (expands regex-bound columns based on real data)
* persistence: `DataFrameSchema.from_yaml(...)`, `from_json(...)` (see §9) ([Pandera][9])

---

# 3) Class-based API: `DataFrameModel` (“Pydantic-style schemas”)

## 3.1 Declaring columns/indexes via type annotations

`DataFrameModel` lets you define schemas as a class with typed fields, using `pandera.typing` generics and `pa.Field(...)` constraints. ([Pandera][10])

Patterns:

* Column declarations: `col: Series[int] = pa.Field(gt=..., isin=..., coerce=...)`
* Index declaration: `index: Index[int]` (pandas typing) ([Pandera][10])
* Concise mode: annotate directly with a dtype (without `Series[...]`) and Pandera interprets it as a column under the hood. ([Pandera][10])

## 3.2 Schema-wide configuration (`class Config`)

Model config controls the same knobs as object-based schemas (and a few extra integration knobs): ([Pandera][10])

Representative config fields:

* `name`, `strict`, `coerce`, `dtype` (df-wide dtype), `add_missing_columns`
* `drop_invalid_rows=True` (model-level opt-in)
* `from_format`, `from_format_kwargs`, `to_format`, `to_format_kwargs` (data format conversion; see §4.3) ([Pandera][11])

## 3.3 Model-local custom validations & preprocessing (decorator-based)

Pandera provides **model component decorators** to attach logic to the model class itself:

* `@pa.check(...)` — field-level checks for columns/indexes (converted to classmethod) ([Pandera][12])
* `@pa.dataframe_check(...)` — dataframe-wide checks declared on the model (classmethod) ([Pandera][13])
* `@pa.parser(...)` — field-level parsers on model fields ([Pandera][14])
* `@pa.dataframe_parser(...)` — dataframe-wide parsers on the model ([Pandera][15])

(These mirror the object-based `checks=` / `parsers=` but keep the schema “in one place”.)

## 3.4 Entry points

* `Model.validate(obj, head=..., tail=..., sample=..., lazy=..., inplace=...)`
* `Model.to_schema()` (convert to `DataFrameSchema`) ([Pandera][10])

---

# 4) Typing layer + “validate-on-instantiation” + format conversion

## 4.1 Typing generics (multi-backend)

Pandera provides `pandera.typing.*` modules so you can annotate functions and objects as `DataFrame[MySchema]`, `Series[T]`, etc., across multiple dataframe ecosystems. ([Pandera][10])

Key namespaces:

* `pandera.typing.pandas` (`DataFrame`, `Series`, `Index`)
* `pandera.typing.polars` (`LazyFrame[...]`, etc.) ([Pandera][2])
* `pandera.typing.dask`, `pandera.typing.modin`, `pandera.typing.pyspark`, `pandera.typing.geopandas` ([Pandera][16])

## 4.2 `check_types()` decorator (runtime enforcement for annotated functions)

`@pa.check_types` validates **inputs & outputs** of a function based on the DataFrameModel annotations, with sampling controls and lazy/inplace toggles. ([Pandera][17])

## 4.3 Data format conversion (schema-driven I/O at function boundaries)

Pandera can auto-convert inputs/outputs to/from serialization formats **in the context of `check_types()`** using `from_format` / `to_format` model config; it’s currently documented as supported for the pandas DataFrame typing type. ([Pandera][11])

* `pandera.typing.DataFrame.from_format(...)` / `.to_format(...)` are the core conversion hooks. ([Pandera][18])

---

# 5) Validation controls & operational semantics

## 5.1 Execution modes: eager vs lazy errors

* Default: fail fast with `SchemaError`
* `lazy=True`: aggregate all failures and raise `SchemaErrors` with a structured “error report” payload ([Pandera][19])

## 5.2 Sampling + partial validation

Many validation entry points support:

* `head=n`, `tail=n`, `sample=n`, `random_state=...` to validate subsets (useful in big data contexts / smoke checks). ([Pandera][17])

## 5.3 `inplace=True` vs copy-on-validate

`inplace=True` applies coercion to the object under validation; otherwise coercion happens on a copy. ([Pandera][17])

## 5.4 Drop-invalid-rows mode

`drop_invalid_rows=True` changes validation from “raise errors” into “return only valid rows” for data-level check failures; it’s available across schema objects and `DataFrameModel`, and requires `lazy=True`. ([Pandera][20])

---

# 6) Checks: `Check` + statistical `Hypothesis` tests

## 6.1 `Check` object model

Checks can be:

* **vectorized** (operate on Series/DataFrame and return bool/boolean mask)
* **element-wise** (`element_wise=True`, callable operates on a scalar)
* **grouped** via `groupby=...` (check receives groups) ([Pandera][21])

Null handling:

* By default checks drop nulls; use `ignore_na=False` to include nulls in the check function’s input. ([Pandera][21])

Severity:

* `raise_warning=True` turns a failing check into a `SchemaWarning` rather than raising `SchemaError`. ([Pandera][21])

Built-ins:

* `Check` exposes a large set of built-in constructors (e.g., comparisons, membership, string constraints). The user guide points to the `Check` API reference for the full list. ([Pandera][21])

## 6.2 `Hypothesis` checks (statistical testing)

Pandera supports “statistical hypothesis” checks via a `Hypothesis` object (e.g., normality tests) that can warn or error. ([Pandera][21])

## 6.3 Custom check registration (extensions API)

Two ways:

* **Inline custom checks** (quick, but not serializable and not associated with a data-synthesis strategy)
* **Registered checks** via `pandera.extensions.register_check_method(...)` so they become first-class `Check.<name>(...)` methods and can be serialized / tied to strategies. ([Pandera][22])

---

# 7) Parsers: preprocessing before validation (pandas backend)

## 7.1 `Parser` pipeline

Parsers allow preprocessing of:

* dataframe-wide inputs (`DataFrameSchema(parsers=...)`)
* column/series inputs (`Column(parsers=...)`) ([Pandera][23])

Key constraint: parsers are documented as **pandas-backend-only**. ([Pandera][23])

---

# 8) Errors, failure-cases, and machine-readable reports

## 8.1 Exception hierarchy

* `SchemaError`: single failure surfaced immediately (or per-check)
* `SchemaErrors`: aggregated failures (lazy validation) with structured message payload and per-failure metadata (schema/column/check/etc.). ([Pandera][19])

## 8.2 Error Reports

Pandera provides a machine-readable “error report” summary of failures; it’s documented for pandas and pyspark.sql. ([Pandera][24])

---

# 9) Data type system & dtype coercion

## 9.1 Type specification options

Pandera supports:

* python built-ins (`int`, `str`, …)
* numpy dtypes
* pandas dtypes (including parameterized/instance dtypes)
* pandera-defined semantic dtypes (`pandera.dtypes.DataType` hierarchy) ([Pandera][8])

## 9.2 Coercion is the primary “built-in transform”

Pandera is primarily validation-only; the main transformation it performs is **dtype coercion** (`coerce=True`) at schema or component levels. ([Pandera][8])

## 9.3 Parameterized dtypes + class-based annotations

When pandas dtypes require parameters (e.g., tz-aware datetime), Pandera supports expressing them via `Annotated[...]` or by passing `dtype_kwargs` to `Field()`. ([Pandera][8])

## 9.4 Python `typing` container validation (object columns)

Pandera can validate certain `typing` containers stored in `object` dtype columns (e.g., `List[T]`, `Dict[K,V]`, `TypedDict`, `NamedTuple`) using `typeguard` under the hood. ([Pandera][8])

## 9.5 Engine abstraction

Pandera’s dtype system is implemented via per-backend engines (e.g., polars and pyspark engines expose `Engine.dtype(...)` mapping to a Pandera `DataType`). ([Pandera][25])

## 9.6 Pydantic integration as a dtype

Pandera can use a Pydantic model as a row-wise dtype (via `PydanticModel(...)` + `coerce=True`), with performance caveats (model applied per row). ([Pandera][26])

---

# 10) Schema inference & persistence (draft → serialize → load)

## 10.1 Infer a draft schema from observed data

`infer_schema(df_or_series)` can generate a draft schema from pandas data and prints a DataFrameSchema that you can then refine. ([Pandera][27])

## 10.2 Persist/round-trip schemas

Pandera supports:

* `schema.to_yaml()` / `DataFrameSchema.from_yaml(...)`
* `schema.to_json()` / `DataFrameSchema.from_json(...)`
* IO helpers: `pandera.io.to_yaml`, `pandera.io.from_yaml`, `pandera.io.to_script` ([Pandera][27])

Important limitation called out in the schema inference/persistence docs: persistence currently focuses on **built-in `Check` methods** under the serialized `checks` key. ([Pandera][27])

---

# 11) Data synthesis & property-based testing (Hypothesis-driven)

Pandera can generate:

* `schema.example(size=n)` for quick sample frames
* `schema.strategy(size=n)` for Hypothesis-based property tests (recommended over `example` for unit testing) ([Pandera][28])

Backend caveat: the Polars integration explicitly notes data synthesis strategies are not supported there (use Polars-native testing tools instead). ([Pandera][2])

---

# 12) Ecosystem integrations

## 12.1 FastAPI

* Use `DataFrameModel` types as endpoint inputs/outputs
* Use `to_format` / `from_format` schema config to produce JSON/dict outputs or read parquet uploads
* `pandera.typing.fastapi.UploadFile` provides `.data` as a validated dataframe ([Pandera][29])

## 12.2 Frictionless

* Convert a Frictionless schema into a Pandera `DataFrameSchema` (field mapping via `FrictionlessFieldParser`). ([Pandera][30])

## 12.3 Mypy plugin (static linting)

Pandera provides a mypy plugin (`plugins = pandera.mypy`), but static type-linting is documented as supported for **pandas** dataframes only and considered experimental. ([Pandera][31])

## 12.4 Pydantic

Beyond `check_types(with_pydantic=...)`, Pandera supports Pydantic models as a dtype via `PydanticModel(...)`. ([Pandera][17])

---

# 13) Backend-specific behavioral notes (important “capability boundaries”)

## 13.1 Polars backend

* Validates `LazyFrame` and `DataFrame` (DF is converted to LF internally for validation)
* Uses Polars lazy execution; checks that inspect data values require `.collect()`
* Supported versions note: `pandera>=0.21.0` supports `polars>=1.0.0`
* Data synthesis strategies explicitly not supported (as of docs) ([Pandera][2])

## 13.2 PySpark SQL backend

* `schema.validate` returns a DataFrame even when errors occur; errors are collected and exposed via a pandas accessor attribute (`dataframe.pandera.errors`)
* Default error behavior is effectively `lazy=True`
* No lambda-based vectorized checks (spark UDF cost); custom checks must return a scalar bool and are supported via `register_check_method`
* Strong recommendation to use spark SQL types (`pyspark.sql.types`) ([Pandera][3])

## 13.3 Ibis backend

* Validation aligns with ibis lazy vs eager behavior: schema-level validations can run without `.execute()`, while data-value checks require execution
* Intended to support method chaining (`.pipe(schema.validate)` in a lazy ibis expression) ([Pandera][4])

---

# 14) Extensibility map (what you customize when Pandera isn’t enough)

* **Custom checks**: register with `pandera.extensions.register_check_method(...)`, optionally provide a Hypothesis strategy so it participates in data synthesis. ([Pandera][22])
* **Custom dtypes**: Pandera’s own `DataType` abstraction is explicitly designed to be extended (semantic dtypes on top of physical dtypes) and can affect type checking and coercion behavior. ([Pandera][32])
* **Model-local check/parser decorators**: attach reusable validations and preprocessing directly on `DataFrameModel` classes (`@check`, `@dataframe_check`, `@parser`, `@dataframe_parser`). ([Pandera][12])

---

If you want the next increment (like you did for PyArrow): I can expand **each major section** above into a “deep dive page” (power knobs + failure modes + minimal snippets) and keep it **backend-differentiated** (pandas vs polars vs pyspark vs ibis), since a lot of Pandera’s real complexity is in those seams.

[1]: https://pypi.org/project/pandera/ "pandera · PyPI"
[2]: https://pandera.readthedocs.io/en/latest/polars.html "Data Validation with Polars - pandera documentation"
[3]: https://pandera.readthedocs.io/en/latest/pyspark_sql.html "Data Validation with Pyspark SQL - pandera documentation"
[4]: https://pandera.readthedocs.io/en/stable/ibis.html "Data Validation with Ibis - pandera documentation"
[5]: https://pandera.readthedocs.io/en/latest/supported_libraries.html "Supported DataFrame Libraries - pandera documentation"
[6]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.api.pandas.container.DataFrameSchema.html "pandera.api.pandas.container.DataFrameSchema - pandera documentation"
[7]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.pandas.components.Column.html "pandera.api.pandas.components.Column - pandera documentation"
[8]: https://pandera.readthedocs.io/en/stable/dtype_validation.html "Data Type Validation - pandera documentation"
[9]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.api.dataframe.container.DataFrameSchema.html "pandera.api.dataframe.container.DataFrameSchema - pandera documentation"
[10]: https://pandera.readthedocs.io/en/latest/dataframe_models.html "DataFrame Models - pandera documentation"
[11]: https://pandera.readthedocs.io/en/latest/data_format_conversion.html "Data Format Conversion - pandera documentation"
[12]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.api.dataframe.model_components.check.html?utm_source=chatgpt.com "pandera.api.dataframe.model_components.check"
[13]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.dataframe.model_components.dataframe_check.html?utm_source=chatgpt.com "pandera.api.dataframe.model_components.dataframe_check"
[14]: https://pandera.readthedocs.io/en/v0.26.0/reference/generated/pandera.api.dataframe.model_components.parser.html?utm_source=chatgpt.com "pandera.api.dataframe.model_components.parser"
[15]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.dataframe.model_components.dataframe_parser.html?utm_source=chatgpt.com "pandera.api.dataframe.model_components.dataframe_parser"
[16]: https://pandera.readthedocs.io/en/v0.24.0/reference/generated/pandera.typing.dask.DataFrame.html "pandera.typing.dask.DataFrame - pandera documentation"
[17]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.decorators.check_types.html "pandera.decorators.check_types - pandera documentation"
[18]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html "pandera.typing.DataFrame - pandera documentation"
[19]: https://pandera.readthedocs.io/en/stable/lazy_validation.html "Lazy Validation - pandera documentation"
[20]: https://pandera.readthedocs.io/en/latest/drop_invalid_rows.html "Dropping Invalid Rows - pandera documentation"
[21]: https://pandera.readthedocs.io/en/stable/checks.html "Validating with Checks - pandera documentation"
[22]: https://pandera.readthedocs.io/en/stable/extensions.html "Extensions - pandera documentation"
[23]: https://pandera.readthedocs.io/en/stable/parsers.html "Preprocessing with Parsers - pandera documentation"
[24]: https://pandera.readthedocs.io/en/latest/error_report.html "Error Reports - pandera documentation"
[25]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.engines.pyspark_engine.Engine.html?utm_source=chatgpt.com "pandera.engines.pyspark_engine."
[26]: https://pandera.readthedocs.io/en/stable/pydantic_integration.html "Pydantic - pandera documentation"
[27]: https://pandera.readthedocs.io/en/latest/schema_inference.html "Schema Inference and Persistence - pandera documentation"
[28]: https://pandera.readthedocs.io/en/stable/data_synthesis_strategies.html "Data Synthesis Strategies - pandera documentation"
[29]: https://pandera.readthedocs.io/en/latest/fastapi.html "FastAPI - pandera documentation"
[30]: https://pandera.readthedocs.io/en/latest/frictionless.html?utm_source=chatgpt.com "Reading Third-Party Schema - pandera documentation"
[31]: https://pandera.readthedocs.io/en/stable/mypy_integration.html "Mypy - pandera documentation"
[32]: https://pandera.readthedocs.io/en/stable/dtypes.html "Pandera Data Types - pandera documentation"

## A) Backends + import surface + extras — deep dive

### Mental model: **Pandera is one API, multiple validation backends**

Most “public” objects exist in multiple namespaces (`pandera.pandas`, `pandera.polars`, `pandera.ibis`, `pandera.pyspark`) with backend-specific semantics and limitations. The most future-proof habit is: **import from the backend namespace you mean** (not the top-level `pandera` module). Pandera’s repo explicitly notes that top-level access to `DataFrameSchema` and similar will be deprecated in **0.29.0**. ([GitHub][1])

### A1) Backend selection (what changes when you switch)

* **pandas**: most feature-complete; parsers, synthesis strategies, persistence, etc. are primarily documented here. ([Pandera][2])
* **polars**: built to leverage Polars’ lazy engine; `pl.LazyFrame` vs `pl.DataFrame` validation differs; synthesis strategies not yet supported. ([Pandera][3])
* **ibis** (new in 0.25.0): piggybacks on Ibis’ “one API, many engines” and laziness; synthesis strategies not yet supported. ([Pandera][4])
* **pyspark.sql** (new in 0.16.0): optimized for ETL “don’t crash the job” workflows; error reporting + execution controls via env vars; different check constraints. ([Pandera][5])

### A2) Installation knobs (extras)

Docs describe backend extras (example: `pandera[polars]`, `pandera[pyspark]`, `pandera[ibis]`). ([Pandera][3])

### A3) Operational env switches (global behavior)

Pandera exposes **process-wide** controls (esp. for non-pandas backends):

* `PANDERA_VALIDATION_DEPTH`: `SCHEMA_ONLY`, `DATA_ONLY`, `SCHEMA_AND_DATA` (default) ([Pandera][6])
* `PANDERA_VALIDATION_ENABLED=False`: disable all validations (pyspark docs, but implemented as a general switch) ([Pandera][5])

---

## B) Object-based schemas (`DataFrameSchema` / `Column` / `Index`) — deep dive

### Mental model: schema = **(structure contract)** + **(dtype contract)** + **(checks/parsers)** + **(error/report policy)**

`DataFrameSchema` is the “table contract.” Columns and indexes are “component contracts.” The same conceptual knobs show up across pandas/polars/ibis/pyspark schema objects. ([Pandera][7])

### B1) `DataFrameSchema`: power knobs (pandas API reference as canonical)

Key constructor arguments (names vary slightly across backends, but the concepts carry):

* **column set control**: `strict: bool | "filter"`; `ordered`; `unique_column_names` ([Pandera][7])
* **schema identity**: `name` (and often title/description metadata in model config; see C) ([Pandera][7])
* **df-level dtype**: `dtype`, `coerce` ([Pandera][8])
* **uniqueness**: `unique=[col,…]` with `report_duplicates={"exclude_first"|"exclude_last"|"all"}` ([Pandera][2])
* **augmentation**: `add_missing_columns=True` (+ per-column `default=` semantics) ([Pandera][3])
* **row-dropping mode**: `drop_invalid_rows=True` (requires `lazy=True`; depends on index uniqueness) ([Pandera][9])

#### Minimal snippet (pandas)

```python
import pandera.pandas as pa

schema = pa.DataFrameSchema(
    columns={
        "id": pa.Column(int, checks=[pa.Check.gt(0)], unique=True),
        "group": pa.Column(str, nullable=False),
    },
    strict="filter",          # keep only schema columns
    ordered=False,            # don’t enforce order
    coerce=True,              # attempt dtype coercion
    unique=["id", "group"],   # joint uniqueness
    report_duplicates="all",
)
out = schema.validate(df, lazy=True)
```

`strict="filter"` means “pass through only schema columns,” but missing schema columns still error. ([Pandera][7])

### B2) `Column`: power knobs + how they interact

From the pandas Column API:

* `dtype`, `coerce` (coerce has no effect if `dtype=None`) ([Pandera][10])
* `required` (may be missing vs must exist) ([Pandera][10])
* `nullable`, `unique`, `report_duplicates` ([Pandera][10])
* `checks=[Check|Hypothesis,…]` ([Pandera][10])
* `parsers=[Parser,…]` (pandas-only feature) ([Pandera][10])
* regex-based “apply this schema to many columns”: name is treated as regex when configured (`regex=True` / special name forms depending on backend) ([Pandera][11])

### B3) `Index` / `MultiIndex`: same contract pattern

Indexes can be typed, checked, nullable, and unique (and participate in `report_duplicates`). ([Pandera][12])

### B4) Schema transforms (composition / refactors)

Schema objects are “copy-on-transform”: add/remove/rename columns, etc. ([Pandera][13])

### B5) Failure modes you trip over immediately

* **`drop_invalid_rows` correctness**: requires `lazy=True`; relies on index/multi-index to identify rows; **non-unique index can drop wrong rows**. ([Pandera][9])
* **strict/filter surprises**: `strict="filter"` *filters extras*, but still errors if schema-defined columns are missing. ([Pandera][7])
* **nullable vs dtype**: `nullable` is applied after dtype check/coercion; “inherently not nullable” dtypes can fail even with `nullable=True`. ([Pandera][14])

### B6) Backend deltas (object schemas)

* **polars**: validating `pl.LazyFrame` is schema-level by default; `pl.DataFrame` does schema+data checks; Pandera may `.collect()` internally depending on depth and object type. ([Pandera][3])
* **ibis**: checks that need data values may require `.execute()`; schema.validate is designed for method chaining in lazy expressions. ([Pandera][4])
* **pyspark.sql**: schema.validate returns a DataFrame even on errors; failures accumulate in `df_out.pandera.errors`; default behavior is effectively lazy. ([Pandera][5])

---

## C) Class-based schemas (`DataFrameModel` + `Field` + model decorators) — deep dive

### Mental model: `DataFrameModel` = “schema as a class” (Pydantic-like ergonomics)

You define columns/index via annotations, attach constraints with `pa.Field(...)`, attach reusable logic with `@dataframe_check` / `@dataframe_parser`, and materialize a `DataFrameSchema` via `to_schema()` or validate directly. ([Pandera][15])

### C1) `Field(...)` dispatch model (what `Field` really does)

`Field` is a declarative adapter: keyword-only args (e.g. `gt`, `ge`, `isin`, string ops) are dispatched to built-in `Check` methods; structural knobs like `nullable`, `unique`, `coerce` live alongside those. ([Pandera][16])

#### Minimal snippet (pandas)

```python
import pandera.pandas as pa
from pandera.typing import Series, DataFrame

class Sales(pa.DataFrameModel):
    id: Series[int] = pa.Field(gt=0, unique=True)
    region: Series[str] = pa.Field(isin=["NA", "EU", "APAC"])
    amount: Series[float] = pa.Field(ge=0)

    class Config:
        strict = "filter"
        coerce = True
```

`Config` mirrors schema-wide knobs (coerce, strict, add_missing_columns, drop_invalid_rows, dtype, formats, etc.). ([Pandera][8])

### C2) Model-level checks / parsers (decorator semantics)

* `@dataframe_check`: model method becomes a classmethod; signature starts with `cls` then check args; forwarded kwargs become `Check` kwargs. ([Pandera][17])
* `@dataframe_parser`: similar, but defines preprocessing steps. ([Pandera][18])

**Polars-specific nuance**: docs show dataframe checks can return a `LazyFrame` of boolean columns for multi-check patterns, and also support element-wise checks in the decorator form. ([Pandera][3])

### C3) Backend-specific `DataFrameModel` differences

* **pyspark.sql**: type annotations are recommended to use `pyspark.sql.types.*` (limited support for plain `int/str`), and custom checks must return scalar bool (no vectorized lambdas). ([Pandera][5])
* **polars**: validating a `LazyFrame` is schema-only by default; environment depth or explicit `.collect()` boundaries determine when data checks happen. ([Pandera][3])
* **ibis**: intended for method chaining and execution-aligned validation; `.execute()` boundary matters. ([Pandera][4])

### C4) Failure modes

* **“SchemaModel vs DataFrameModel” legacy confusion**: older docs / code may use SchemaModel; DataFrameModel is the modern surface. (If you’re migrating old code, treat SchemaModel usage as legacy naming, not a different concept.) ([Pandera][19])
* **Parser ordering expectations**: parsers are *documented* as preprocessing before checks (pandas-only), but be careful with assumptions if you’re mixing per-column parsers + model parsers and expecting strict ordering across them. ([Pandera][20])

---

## D) Typing + runtime decorators (`check_types`, `check_io`, `check_input`, `check_output`) + format conversion — deep dive

### Mental model: **validate at function boundaries** (inputs/outputs) without threading `.validate()` everywhere

Pandera offers two main “boundary validation” styles:

1. **Schema arguments** (`check_input/output/io`) for existing pipelines. ([Pandera][21])
2. **Type-annotation-driven** (`check_types`) for codebases that embrace `pandera.typing.*`. ([Pandera][22])

### D1) `check_types`: power knobs

Notable decorator args:

* `with_pydantic` uses `pydantic.validate_arguments` for input validation (output still validated by Pandera). ([Pandera][22])
* `head/tail/sample/random_state` (rows overlapping are de-duplicated) ([Pandera][22])
* `lazy`, `inplace` (same semantics as schema.validate) ([Pandera][22])

#### Minimal snippet (pandas typing)

```python
import pandera.pandas as pa
from pandera.typing import DataFrame

class S(pa.DataFrameModel):
    a: int = pa.Field(gt=0)

@pa.check_types(lazy=True, head=1000)
def f(df: DataFrame[S]) -> DataFrame[S]:
    return df
```

### D2) `check_input` / `check_output` / `check_io`: when to use

* `check_input(schema, obj_getter=...)`: validate a specific argument by position/name/slice-like accessors. ([Pandera][23])
* `check_output(schema, obj_getter=...)`: validate return values. ([Pandera][24])
* `check_io(out=..., **inputs)`: multi-input + output contracts in one decorator. ([Pandera][25])

### D3) Data format conversion (`from_format` / `to_format`)

Pandera supports schema-driven conversion in the context of `check_types()` using model config: `from_format`, `to_format` (values defined by `pandera.typing.formats.Formats`, e.g. csv/json/parquet/feather/dict/pickle). ([Pandera][26])

---

## E) Validation controls (lazy, sampling, inplace, drop-invalid-rows, depth/on-off) — deep dive

### E1) Lazy validation (error aggregation)

Passing `lazy=True` to validate aggregates failures into `SchemaErrors` (vs raising the first `SchemaError`). ([Pandera][27])

### E2) Sampling controls (debug + perf knobs)

Many entry points accept `head/tail/sample/random_state` to validate subsets (especially useful on very large data). ([Pandera][22])

### E3) Inplace coercion vs copy

Decorators and schema validate APIs expose `inplace`; coercion applied to the object under validation vs returning a coerced copy. ([Pandera][25])

### E4) `drop_invalid_rows=True` (validation-as-filter)

Key semantics:

* Must call `validate(..., lazy=True)` (required). ([Pandera][9])
* Drops failing rows by index; **non-unique indexes can drop incorrect rows**. ([Pandera][9])

### E5) Global execution controls via env vars

* `PANDERA_VALIDATION_DEPTH={SCHEMA_ONLY|DATA_ONLY|SCHEMA_AND_DATA}` changes whether schema-only vs data-only vs both are executed. ([Pandera][6])
* `PANDERA_VALIDATION_ENABLED=False` disables validation (documented in pyspark). ([Pandera][5])

---

## F) Checks + Hypothesis tests — deep dive

### Mental model: checks operate at three “shapes”

1. **element-wise**: `Callable[[Any], bool]`
2. **vectorized**: `Callable[[Series|DF], bool|mask]`
3. **grouped**: via `groupby=...` + group semantics
   The `Check` API reference spells out callable signatures, especially element-wise vs vectorized. ([Pandera][28])

### F1) Built-in check constructors (the “standard library”)

You primarily use `Check.<method>(...)` and `Field(...)` sugar that dispatches to those methods. ([Pandera][29])

#### Minimal snippet (wide + column checks)

```python
import pandera.pandas as pa

schema = pa.DataFrameSchema(
    {"x": pa.Column(int, pa.Check.in_range(0, 10)),
     "y": pa.Column(str, pa.Check.isin(["a","b","c"]))},
    checks=[pa.Check(lambda df: (df["x"] % 2 == 0) | (df["y"] != "c"))],
)
```

### F2) Hypothesis tests (`Hypothesis`)

`Hypothesis` is a specialized “Check-like” type for hypothesis testing (with parameters like `alpha`, relationship, grouping, etc.). ([Pandera][30])

### F3) Custom checks: inline vs registered

* **Inline callable**: quick, but persistence/synthesis integration is limited.
* **Registered check methods** (`register_check_method`) become first-class `Check.<name>(...)` and can define `statistics` and `strategy` hooks. ([Pandera][31])

### F4) Backend deltas (checks)

* **pyspark.sql**: no lambda-based vectorized checks (would require UDFs); custom check must return **scalar bool**; use `register_check_method` and native Spark expressions. ([Pandera][5])
* **polars / ibis**: checks that require data values may force `.collect()` / `.execute()` depending on validation depth and object type. ([Pandera][3])

---

## G) Parsers (preprocessing) — deep dive

### Mental model: parsers = “schema-attached normalization steps”

Parsers run before validation checks and are intended for normalization (clip, canonicalize, fill, etc.). They are **pandas-backend only** per docs. ([Pandera][20])

### G1) Where parsers attach

* `DataFrameSchema(parsers=[...])`
* `Column(parsers=[...])`
* `DataFrameModel` via `@dataframe_parser` (and field-level parser decorators) ([Pandera][20])

#### Minimal snippet (pandas)

```python
import pandera.pandas as pa

def clip_0_1(s):
    return s.clip(0, 1)

schema = pa.DataFrameSchema(
    {"p": pa.Column(float, parsers=[clip_0_1], checks=[pa.Check.between(0, 1)])}
)
out = schema.validate(df)
```

### G2) Failure modes

* **Backend mismatch**: parsers won’t run on polars/ibis/pyspark backends (documented restriction). ([Pandera][20])
* **Order assumptions**: if you need a strict transform pipeline, be explicit (e.g., do transforms outside Pandera and reserve Pandera for contract enforcement). Parsers are best treated as “light normalization,” not a full ETL DSL. ([Pandera][20])

---

## H) Errors + error reports — deep dive

### Mental model: errors are **structured**, not just strings

Pandera has:

* **Exceptions** (`SchemaError`, `SchemaErrors`) with failure cases / message payloads. ([Pandera][32])
* **Error Report** feature (new in 0.19.0) providing a generalized machine-readable summary for pandas and pyspark.sql. ([Pandera][6])

### H1) Error Reports: knobs + payload shape

Docs describe:

* available for **pandas + pyspark.sql** ([Pandera][6])
* requires `lazy=True` on pandas to aggregate into `SchemaErrors` and print `e.message` (JSON-like dict) ([Pandera][6])
* global validation depth control via `PANDERA_VALIDATION_DEPTH` ([Pandera][6])

### H2) PySpark “errors accessor” contract

In pyspark.sql backend:

* validate returns a DataFrame even when errors occur
* errors accessible via `df_out.pandera.errors`
* default behavior collects errors (lazy-style) ([Pandera][5])

### H3) Performance controls in pyspark backend

Docs add:

* caching control via `PANDERA_CACHE_DATAFRAME` and `PANDERA_KEEP_CACHED_DATAFRAME` to reduce repeated Spark actions during validation. ([Pandera][5])

---

## I) Dtype system + coercion — deep dive

### Mental model: dtype validation is “first-class,” and coercion is the main built-in transform

Pandera’s dtype system exists to abstract over dataframe ecosystems and provide a standardized dtype API. ([Pandera][33])

### I1) Validation order matters (dtype vs nullable)

Docs emphasize:

* type validation/coercion happens before downstream checks
* `nullable` is applied as a core check after type check/coercion; some non-nullable dtypes still fail even if `nullable=True`. ([Pandera][14])

### I2) Coercion uses pydantic (where applicable)

Docs state Pandera uses **typeguard** for data type validation and **pydantic** for coercion when `coerce=True`. ([Pandera][14])

### I3) Row-wise validation with `PydanticModel`

Pandera supports a Pydantic model dtype that validates each row (and can coerce back to dict). This is powerful but inherently row-wise. ([Pandera][34])

---

## J) Schema inference + persistence — deep dive

### Mental model: infer → edit → persist → load (but only for a serializable subset)

`infer_schema()` produces a draft schema from pandas objects. ([Pandera][35])

### J1) Infer draft schema

```python
import pandera.pandas as pa
schema = pa.infer_schema(df)
```

This is explicitly documented for pandas dataframes/series. ([Pandera][35])

### J2) Persist/roundtrip

Docs show YAML/JSON persistence and script export in the schema inference/persistence guide and IO utilities. ([Pandera][36])

### J3) Hard limitation: only built-in Checks serialize cleanly

Docs explicitly note: **currently, only built-in `Check` methods are supported under the `checks` key** in schema persistence. This is the key constraint that shapes how you design “contract libraries” meant to be exported. ([Pandera][36])

---

## K) Data synthesis + property-based testing — deep dive

### Mental model: schema as a generator of valid examples (mostly pandas-first)

Pandera supports “Data Synthesis Strategies” (Hypothesis-based) in the core docs, but:

* **polars**: not yet supported ([Pandera][3])
* **ibis**: not yet supported ([Pandera][4])

For backends without native support, docs recommend using native generators (e.g., Polars parametric testing) and then validating. ([Pandera][3])

---

## L) Integrations (FastAPI, Frictionless, Mypy, etc.) — deep dive

### L1) FastAPI

Pandera supports using `DataFrameModel` types to validate dataframe inputs/outputs of endpoints; it also provides `pandera.typing.fastapi.UploadFile` which reads files using DataFrameModel config and exposes validated `.data`. ([Pandera][37])

### L2) Frictionless

Docs expose an adapter to create a `DataFrameSchema` from a Frictionless schema file/object. ([Pandera][38])

### L3) Mypy (static linting)

Pandera documents a mypy integration (noting scope/limitations); use it as lint support rather than as a full substitute for runtime validation. ([Pandera][6])

---

## M) Backend-specific “seams” that matter in real systems — deep dive

### M1) Polars: LazyFrame vs DataFrame is a semantic fork

* `pl.DataFrame` is converted to `pl.LazyFrame` under the hood to use lazy optimizations; but DataFrame validation includes schema+data checks. ([Pandera][3])
* `pl.LazyFrame` validation is schema-only by default; to enforce data-level checks you either:

  * explicitly `.collect()` → validate → `.lazy()` (clear boundary), or
  * set `PANDERA_VALIDATION_DEPTH=SCHEMA_AND_DATA` (implicit `.collect()` calls). ([Pandera][3])
* Data synthesis strategies not supported. ([Pandera][3])

### M2) Ibis: laziness + execute boundary

Ibis backend is explicitly designed around Ibis’ laziness and multi-backend execution; checks that inspect data values may require `.execute()`. Synthesis strategies not supported. ([Pandera][4])

### M3) PySpark SQL: validation is “non-throwing by default”

* validate returns a DataFrame even when errors occur; inspect `df_out.pandera.errors`. ([Pandera][5])
* default behavior collects errors (lazy-style). ([Pandera][5])
* no lambda vectorized checks; custom checks must return scalar bool; recommend Spark SQL types for annotations. ([Pandera][5])
* env-driven runtime controls: depth, enabled switch, caching, etc. ([Pandera][5])

---

## N) Extensibility — deep dive

### Mental model: “extend Pandera by making new things look like first-class things”

You mainly extend Pandera via:

1. **custom check registration** (`register_check_method`) with declared `statistics` and optional `strategy` hooks (so checks can be serialized / participate in synthesis). ([Pandera][39])
2. **dtype system** (Pandera defines a dtype interface explicitly to abstract over ecosystem dtypes). ([Pandera][33])

---

If you want the next increment after this: I can convert each page (A–N) into a **“contract harness” playbook** (how to bake Pandera into a DatasetContract system: schema registry, golden error-report snapshots, environment-driven depth policies per pipeline stage, and backend-specific test patterns for pandas vs polars vs ibis vs pyspark).

[1]: https://github.com/unionai-oss/pandera?utm_source=chatgpt.com "unionai-oss/pandera: A light-weight, flexible, and ..."
[2]: https://pandera.readthedocs.io/en/stable/dataframe_schemas.html?utm_source=chatgpt.com "DataFrame Schemas - pandera documentation"
[3]: https://pandera.readthedocs.io/en/latest/polars.html "Data Validation with Polars - pandera documentation"
[4]: https://pandera.readthedocs.io/en/stable/ibis.html "Data Validation with Ibis - pandera documentation"
[5]: https://pandera.readthedocs.io/en/latest/pyspark_sql.html "Data Validation with Pyspark SQL - pandera documentation"
[6]: https://pandera.readthedocs.io/en/latest/error_report.html "Error Reports - pandera documentation"
[7]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.api.pandas.container.DataFrameSchema.html?utm_source=chatgpt.com "pandera.api.pandas.container.DataFrameSchema"
[8]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.api.pandas.model_config.BaseConfig.html?utm_source=chatgpt.com "pandera.api.pandas.model_config.BaseConfig"
[9]: https://pandera.readthedocs.io/en/latest/drop_invalid_rows.html "Dropping Invalid Rows - pandera documentation"
[10]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.pandas.components.Column.html?utm_source=chatgpt.com "pandera.api.pandas.components.Column"
[11]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.polars.components.Column.html?utm_source=chatgpt.com "pandera.api.polars.components.Column"
[12]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.pandas.components.Index.html?utm_source=chatgpt.com "pandera.api.pandas.components.Index"
[13]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.api.dataframe.container.DataFrameSchema.html?utm_source=chatgpt.com "pandera.api.dataframe.container.DataFrameSchema"
[14]: https://pandera.readthedocs.io/en/stable/dtype_validation.html?utm_source=chatgpt.com "Data Type Validation - pandera documentation"
[15]: https://pandera.readthedocs.io/en/latest/dataframe_models.html?utm_source=chatgpt.com "DataFrame Models - pandera documentation"
[16]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.dataframe.model_components.Field.html?utm_source=chatgpt.com "pandera.api.dataframe.model_components.Field"
[17]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.dataframe.model_components.dataframe_check.html?utm_source=chatgpt.com "pandera.api.dataframe.model_components.dataframe_check"
[18]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.dataframe.model_components.dataframe_parser.html?utm_source=chatgpt.com "pandera.api.dataframe.model_components.dataframe_parser"
[19]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.api.pandas.model.DataFrameModel.html?utm_source=chatgpt.com "pandera.api.pandas.model.DataFrameModel"
[20]: https://pandera.readthedocs.io/en/stable/parsers.html?utm_source=chatgpt.com "Preprocessing with Parsers - pandera documentation"
[21]: https://pandera.readthedocs.io/en/stable/decorators.html?utm_source=chatgpt.com "Decorators for Pipeline Integration - pandera documentation"
[22]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.decorators.check_types.html?utm_source=chatgpt.com "pandera.decorators.check_types"
[23]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.decorators.check_input.html?utm_source=chatgpt.com "pandera.decorators.check_input"
[24]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.decorators.check_output.html?utm_source=chatgpt.com "pandera.decorators.check_output"
[25]: https://pandera.readthedocs.io/en/stable/reference/generated/pandera.decorators.check_io.html?utm_source=chatgpt.com "pandera.decorators.check_io"
[26]: https://pandera.readthedocs.io/en/latest/data_format_conversion.html?utm_source=chatgpt.com "Data Format Conversion - pandera documentation"
[27]: https://pandera.readthedocs.io/en/stable/lazy_validation.html?utm_source=chatgpt.com "Lazy Validation - pandera documentation - Read the Docs"
[28]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.checks.Check.html?utm_source=chatgpt.com "pandera.api.checks."
[29]: https://pandera.readthedocs.io/en/stable/checks.html?utm_source=chatgpt.com "Validating with Checks - pandera documentation"
[30]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.api.hypotheses.Hypothesis.html?utm_source=chatgpt.com "pandera.api.hypotheses.Hypothesis"
[31]: https://pandera.readthedocs.io/en/stable/extensions.html?utm_source=chatgpt.com "Extensions - pandera documentation - Read the Docs"
[32]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.errors.SchemaError.html?utm_source=chatgpt.com "pandera.errors.SchemaError"
[33]: https://pandera.readthedocs.io/en/stable/dtypes.html?utm_source=chatgpt.com "Pandera Data Types"
[34]: https://pandera.readthedocs.io/en/stable/pydantic_integration.html?utm_source=chatgpt.com "Pydantic - pandera documentation"
[35]: https://pandera.readthedocs.io/en/latest/schema_inference.html?utm_source=chatgpt.com "Schema Inference and Persistence - pandera documentation"
[36]: https://pandera.readthedocs.io/en/stable/schema_inference.html?utm_source=chatgpt.com "Schema Inference and Persistence - pandera documentation"
[37]: https://pandera.readthedocs.io/en/latest/fastapi.html?utm_source=chatgpt.com "FastAPI - pandera documentation"
[38]: https://pandera.readthedocs.io/en/latest/frictionless.html?utm_source=chatgpt.com "Reading Third-Party Schema - pandera documentation"
[39]: https://pandera.readthedocs.io/en/latest/reference/generated/pandera.extensions.html?utm_source=chatgpt.com "pandera.extensions"

Below is a **Pandera-first “contract harness” playbook** you can drop into a **DatasetContract** system: **schema registry**, **stage-specific validation policies (incl. depth controls)**, **golden error-report snapshots**, and **backend-differentiated test patterns** for **pandas / polars / ibis / pyspark**.

---

# 0) What “best-in-class” looks like (North Star)

**A DatasetContract system built on Pandera should:**

1. Treat schemas as **versioned, named artifacts** (code-first, diffable).
2. Run validation at **every boundary** (ingest → normalize → materialize → serve), but with **stage-appropriate cost controls** (schema-only vs data-only vs both).
3. Produce **machine-readable validation telemetry** (error report + failure cases) that can be:

   * snapshot-tested (“goldens”),
   * logged (structured),
   * stored (run registry),
   * charted (error categories over time).
4. Be backend-aware: Pandera’s backends differ in when they execute data checks and how they surface errors (especially PySpark). ([Pandera][1])

---

# 1) Contract primitives (what you standardize)

## 1.1 `SchemaSpec`: the “schema handle” you store in contracts

**Prefer `DataFrameModel` as the canonical authoring form**, because it:

* is type-annotation-native,
* can validate directly,
* can be converted into a schema object (for places that need `DataFrameSchema`). ([Pandera][2])

However, **don’t force one schema to serve all backends**. In practice:

* **pandas** schema/model is feature-complete (parsers, synthesis, persistence),
* **polars/ibis/pyspark** often need backend-specific constraints or limitations.

So your `SchemaSpec` should allow *either*:

* a single backend schema, or
* a dict of backend → schema.

**Recommended shape (conceptual):**

* `schemas: dict[Backend, type[pa.DataFrameModel] | pa.DataFrameSchema]`

## 1.2 `ValidationProfile`: stage-specific knobs (the “how”)

A **ValidationProfile** should define:

* `enabled: bool`
* `depth: {"SCHEMA_ONLY","DATA_ONLY","SCHEMA_AND_DATA"}`
* `lazy: bool`
* `sample: head/tail/sample/random_state` (optional)
* `inplace: bool` (if coercion is enabled)
* `drop_invalid_rows: bool` (optional salvage mode)
* backend knobs (esp. pyspark caching)

Pandera supports **global config** (and a **config context**) for things like depth/enabled, which is exactly what you want for “policy per stage.” ([Pandera][3])

## 1.3 `ValidationResult`: the single normalized output type

Standardize a backend-agnostic result envelope:

* `ok: bool`
* `dataset: str`, `stage: str`, `backend: str`
* `policy: {enabled, depth, lazy, sample...}`
* `error_report: dict | None` (machine-readable categories)
* `failure_cases: list[dict] | None` (bounded, deterministic)
* `stats: {n_rows_checked, n_failures, categories...}`
* `exception: {type, message} | None`

This prevents “each subsystem logs errors differently” and makes goldens trivial.

---

# 2) Registry: schema discovery + versioning + “compile checks”

## 2.1 Registry patterns

You want deterministic discovery for CI + tooling:

* **Explicit registration** (recommended): `ContractRegistry.register(contract)`
* Optional: **module scanning** for `DATASET_CONTRACTS = [...]`

## 2.2 Contract IDs + schema versions

Adopt a stable identity scheme:

* `dataset_name` (canonical string)
* `schema_version` (semver or monotonically increasing int)
* `contract_hash` (optional; hash the schema export or model source)

## 2.3 “Compile checks” (CI gate)

Add a “registry compile” test that:

* imports all contracts,
* for each contract, attempts:

  * backend import,
  * schema construction / `.to_schema()` if used,
  * minimal validation on an empty/minimal fixture if possible.

This catches drift (Pandera upgrades, backend upgrades) before runtime.

---

# 3) Validation engine: one entry point + backend adapters + policy context

## 3.1 Use Pandera config context for policy application

Pandera exposes a global config and a **context manager** for local overrides. ([Pandera][3])

**Policy application pattern:**

* Use a *single* `validate_contract(contract, obj, stage, overrides=...)`.
* Inside, apply policy via:

  * `pandera.config.config_context(...)` for depth/enabled/caching where supported,
  * per-call args (`lazy`, sampling, `inplace`) for schema/model validate calls.

Pandera depth semantics and the `PANDERA_VALIDATION_DEPTH` modes are explicitly documented. ([Pandera][4])
You can also disable validation globally via `PANDERA_VALIDATION_ENABLED=False` (useful for production kill-switch). ([Pandera][3])

## 3.2 Backend adapter contract (what each adapter must implement)

Define a tiny interface:

* `validate(schema_or_model, obj, policy) -> (validated_obj, ValidationResult)`
* `extract_error_report(exc_or_obj) -> dict | None`
* `extract_failure_cases(exc_or_obj) -> list[dict] | None`

### pandas adapter (exceptions)

* On `lazy=True`, Pandera raises `SchemaErrors` and exposes:

  * `e.message` as a machine-readable error report (dict) ([Pandera][4])
  * `e.failure_cases` for tabular failure case details (useful for goldens) ([Pandera][5])

### polars adapter (exceptions, but lazy boundary matters)

* Polars backend raises `SchemaError` eagerly; `SchemaErrors` when `lazy=True`. ([Pandera][6])
* Validation depth and LazyFrame behavior can force collection depending on depth/policy; treat this explicitly (see §7).

### pyspark.sql adapter (non-throwing default; accessor-based errors)

* Validation results are accessed via `df_out.pandera.errors`. ([Pandera][1])
* **Critical:** extract `df_out.pandera.errors` immediately; further Spark ops may reset the accessor state. ([Pandera][1])
* Depth/enabled and caching are controlled via config/env; pyspark docs call out caching env vars. ([Pandera][1])

### ibis adapter (execution boundary)

* Use ibis laziness intentionally:

  * schema-only checks should be allowed without `.execute()`
  * data checks may require `.execute()`
* Your adapter should accept a policy flag like `allow_execute: bool` and refuse to execute in low-cost stages.

---

# 4) Golden snapshots: deterministic error artifacts (the core harness)

## 4.1 What to snapshot

Snapshot **only stable, useful, machine-readable fields**:

* `error_report` (dict categories)
* bounded `failure_cases` (top K, deterministic ordering)
* policy + backend + dataset + stage

For pandas error report, Pandera documents printing `json.dumps(e.message, ...)` where `e` is `SchemaErrors`. ([Pandera][4])
For Spark, the error report is the dict behind `df_out.pandera.errors`. ([Pandera][1])

## 4.2 Deterministic normalization (must-have)

Implement a canonicalization pipeline:

1. **Sort keys** recursively in dicts.
2. For lists of error items, sort by a stable tuple, e.g.:

   * `(schema, column, check, error)` if present.
3. For `failure_cases`:

   * convert DataFrame → list-of-dicts,
   * sort by `(schema_context, column, check, check_number, index, failure_case)` if those exist,
   * truncate to `K` rows (e.g., 50) to keep snapshots small.
4. Redact / normalize unstable strings:

   * memory addresses,
   * repr of objects that include random ordering,
   * Spark Row reprs if they’re too verbose.

## 4.3 Test API you want

Provide two core assertions:

* `assert_contract_valid(dataset, stage, obj)`
* `assert_contract_invalid_matches_golden(dataset, stage, obj, golden_path, update=False)`

Where “invalid” uses policy `lazy=True` so you aggregate failures.

**Note:** if you rely on `drop_invalid_rows=True`, docs require `lazy=True` to drop invalid rows. ([Pandera][7])
(Your harness should test both: “error surfaced” and “salvage mode returns filtered rows.”)

## 4.4 Golden file layout

A practical layout:

* `tests/golden/contracts/<dataset>/<stage>/<backend>.json`

Keep it backend-separated so your CI matrix can run subsets.

---

# 5) Environment-driven policies per pipeline stage (the “governor”)

## 5.1 Default stage policy table (recommended)

Use a “cost gradient”:

* **Ingest / landing**: `SCHEMA_ONLY`, `lazy=False`, maybe `coerce=True`, no data checks
* **Normalize / canonicalize**: `DATA_ONLY` with sampling (head/tail/sample)
* **Pre-materialize**: `SCHEMA_AND_DATA`, `lazy=True` (full correctness gate)
* **Serve / export**: `SCHEMA_AND_DATA`, `lazy=True`, optionally `drop_invalid_rows=False` (fail hard)
* **Exploratory / dev**: sampling + warnings acceptable

Pandera explicitly defines these depth modes and how to set them (env/config). ([Pandera][4])

## 5.2 Policy override channels (priority order)

1. **Per-call overrides** (explicit in code)
2. **Stage defaults** (your config file)
3. **Env overrides** (operational kill-switch)

   * `PANDERA_VALIDATION_ENABLED=False` ([Pandera][3])
   * `PANDERA_VALIDATION_DEPTH=...` ([Pandera][4])

## 5.3 Apply policies with `config_context`

Because Pandera supports a config context manager, you can do:

* `with config_context(validation_enabled=..., validation_depth=..., cache_dataframe=..., keep_cached_dataframe=...): ...` ([Pandera][3])

This is the cleanest way to implement “stage policies” without spawning separate processes.

---

# 6) Backend-specific test patterns (what “good” looks like per backend)

## 6.1 pandas

**Testing stack**

* Positive tests: validate canonical fixtures (small, representative).
* Negative tests: intentionally broken fixtures → compare `SchemaErrors.message` and `failure_cases` to goldens. ([Pandera][4])
* Sampling tests: validate that sampling gates are applied for big fixtures (e.g., `head=...` only checks top rows).

**Golden content**

* `SchemaErrors.message` (error report dict) is compact and stable. ([Pandera][4])
* `SchemaErrors.failure_cases` gives row-level failure details for debugging. ([Pandera][5])

## 6.2 polars

**Key seam:** LazyFrame validation can be schema-only by default and may require collection for data checks; you must decide where `.collect()` is allowed (policy-driven). ([Pandera][6])

**Test matrix**

* Validate a `pl.DataFrame` fixture (small) under `SCHEMA_AND_DATA`.
* Validate a `pl.LazyFrame` fixture:

  * under `SCHEMA_ONLY` (should never collect),
  * under `SCHEMA_AND_DATA` (expect collection / data checks).

Goldens should snapshot the exception message / failure cases in `lazy=True` mode (SchemaErrors).

## 6.3 ibis

**Key seam:** execution boundary.

* For schema-only stages, validate ibis expressions without `.execute()`.
* For data-check stages, allow execution against a test backend (DuckDB is ideal) and snapshot results.

**Test pattern**

* Create ibis table expr from in-memory data or test DB.
* Validate under two policies:

  * `SCHEMA_ONLY` (no execute)
  * `SCHEMA_AND_DATA` (execute permitted)

## 6.4 pyspark.sql

**Key seam:** errors are attached to the returned dataframe; not necessarily raised. ([Pandera][1])

**Harness rules**

* Always extract `errors = dict(df_out.pandera.errors)` immediately. ([Pandera][1])
* Normalize and snapshot that dict as the golden.
* For “fail hard in CI,” your adapter can implement:

  * if `errors` non-empty → raise `ContractViolation(errors)`.

**Performance knobs**

* Expose caching policy in the ValidationProfile:

  * `PANDERA_CACHE_DATAFRAME`
  * `PANDERA_KEEP_CACHED_DATAFRAME` ([Pandera][1])

---

# 7) Integrating the harness into a DatasetContract system (practical wiring)

## 7.1 Where validation lives (enforced boundaries)

Put validation in exactly three places:

1. **Producer boundary** (after creation / extraction)
2. **Materialization boundary** (before writing to storage)
3. **Serving boundary** (before returning to external callers)

Everything else is “internal convenience validation” and should be stage-policy controlled.

## 7.2 “Contracted materializer” (single hook)

Create a single materialization helper:

* input: `(dataset_name, obj, stage="materialize")`
* does: validation → write → record ValidationResult

This ensures every dataset written is contract-checked the same way.

## 7.3 Run registry integration

Store the normalized `ValidationResult` as:

* a row in `pipeline_steps` (or similar),
* plus a JSON blob of `error_report` (and optionally bounded failure cases).

This turns “validation failures” into queryable, time-series data (especially useful for Spark’s structured error report). ([Pandera][1])

---

# 8) Drift resistance + operational playbook

## 8.1 Pin + verify the “validation ABI”

Pandera evolves; the safest pattern is:

* pin Pandera + backend libs,
* run “registry compile” in CI,
* run golden snapshot tests in CI,
* update goldens only via an explicit flag.

## 8.2 Prefer error report goldens over string messages

String messages change; structured dicts change less. Pandera’s error report format is explicitly designed to be machine-readable (pandas + pyspark). ([Pandera][4])

## 8.3 `drop_invalid_rows` is a special mode—treat as salvage, not default

Docs require `lazy=True` to use it. ([Pandera][7])
Use it only in stages where you explicitly want “best-effort salvage,” and snapshot both:

* “what was dropped” (failure cases),
* “what remains” (row counts and maybe a hash of ids).

---

# 9) Minimal implementation checklist (what to build first)

1. **Contract types**

   * `DatasetContract`, `ValidationProfile`, `ValidationResult`, `Backend` enum
2. **Registry**

   * register + list + get
3. **Validator**

   * `validate_contract(contract, obj, stage)` with:

     * config_context policy application ([Pandera][3])
     * backend adapter dispatch
4. **Normalization**

   * `normalize_error_report(...)`
   * `normalize_failure_cases(...)`
5. **Golden harness**

   * `assert_invalid_matches_golden(...)`
   * update-goldens flag
6. **Backend CI matrix**

   * pandas + polars always
   * ibis optional (if you actively use it)
   * spark optional but highly recommended if it’s in production

---

If you want the *next* increment after this playbook: I can turn it into a **repo-ready template** (module layout + concrete function signatures + a pytest golden harness skeleton + example contracts for one dataset per backend) that matches your DatasetContract + run-registry + Hamilton materialization style.

[1]: https://pandera.readthedocs.io/en/latest/pyspark_sql.html "Data Validation with Pyspark SQL - pandera documentation"
[2]: https://pandera.readthedocs.io/en/latest/dataframe_models.html?utm_source=chatgpt.com "DataFrame Models - pandera documentation"
[3]: https://pandera.readthedocs.io/en/latest/configuration.html "Configuration - pandera documentation"
[4]: https://pandera.readthedocs.io/en/latest/error_report.html "Error Reports - pandera documentation"
[5]: https://pandera.readthedocs.io/en/stable/lazy_validation.html?utm_source=chatgpt.com "Lazy Validation - pandera documentation"
[6]: https://pandera.readthedocs.io/en/latest/polars.html?utm_source=chatgpt.com "Data Validation with Polars - pandera documentation"
[7]: https://pandera.readthedocs.io/en/latest/drop_invalid_rows.html?utm_source=chatgpt.com "Dropping Invalid Rows - pandera documentation"

Below is a **repo-ready template** you can drop into `src/` that gives you:

* a **contract registry** (`DatasetContract` + backend-specific schema providers),
* a **single validation engine** with **backend adapters** (pandas / polars / ibis / pyspark),
* a **normalized `ValidationResult`** envelope (stable for logging + DB storage),
* a **pytest golden snapshot harness** (`--update-goldens`),
* and a **Hamilton-friendly contracted materializer** that records validation results into your run-registry hook.

You can copy/paste this structure as-is and then rename `codeintel` → your actual package root if needed.

---

# 1) Module layout (drop-in)

```text
src/codeintel/contracts/
  __init__.py
  base.py
  registry.py
  validator.py
  golden.py
  pytest_plugin.py
  adapters/
    __init__.py
    pandas_backend.py
    polars_backend.py
    ibis_backend.py
    pyspark_backend.py
  examples/
    __init__.py
    example_events.py

src/codeintel/hamilton/materialization/
  contracted.py

tests/contracts/
  conftest.py
  test_registry_compile.py
  test_example_events_goldens.py
  golden/
    contracts/
      example_events/
        ingest/
          pandas.json
          polars.json
          ibis.json
          pyspark.json
```

> Goldens are generated by running pytest once with `--update-goldens`. Without that, tests fail with a clear “missing golden” message.

---

# 2) Core contract types

## `src/codeintel/contracts/base.py`

```python
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Protocol


class Backend(str, Enum):
    pandas = "pandas"
    polars = "polars"
    ibis = "ibis"
    pyspark = "pyspark"


class ValidationDepth(str, Enum):
    schema_only = "SCHEMA_ONLY"
    data_only = "DATA_ONLY"
    schema_and_data = "SCHEMA_AND_DATA"


@dataclass(frozen=True, slots=True)
class ValidationProfile:
    """
    Stage-level policy knobs. Keep this stable and store it alongside results.
    """
    enabled: bool = True
    depth: ValidationDepth = ValidationDepth.schema_and_data
    lazy: bool = True

    # partial validation knobs (where supported)
    head: int | None = None
    tail: int | None = None
    sample: int | None = None
    random_state: int | None = None

    # coercion semantics
    inplace: bool = False

    # salvage mode (pandas/polars-style; pyspark behaves differently)
    drop_invalid_rows: bool = False

    # pyspark perf knobs (implemented via pandera config/env on spark backend)
    spark_cache_dataframe: bool | None = None
    spark_keep_cached_dataframe: bool | None = None

    # if True: raise on pyspark errors (instead of returning df with errors attached)
    spark_fail_hard: bool = True


@dataclass(frozen=True, slots=True)
class ValidationResult:
    ok: bool
    dataset: str
    stage: str
    backend: Backend
    profile: ValidationProfile

    # “stable artifacts” for goldens / logging / DB
    error_report: dict[str, Any] | None = None
    failure_cases: list[dict[str, Any]] | None = None

    # bounded numeric summary for dashboards
    stats: dict[str, Any] = field(default_factory=dict)

    # optional for debugging, but avoid putting huge strings in goldens
    exception_type: str | None = None
    exception_message: str | None = None


SchemaFactory = Callable[[], Any]


@dataclass(frozen=True, slots=True)
class DatasetContract:
    dataset: str
    version: str
    description: str = ""
    schemas: Mapping[Backend, SchemaFactory] = field(default_factory=dict)

    def get_schema(self, backend: Backend) -> Any:
        try:
            factory = self.schemas[backend]
        except KeyError as e:
            raise KeyError(f"No schema registered for backend={backend} dataset={self.dataset}") from e
        return factory()


class RunRecorder(Protocol):
    """
    Minimal run-registry hook. Implement this using your pipeline_steps table.
    """
    def record_validation(self, result: ValidationResult) -> None: ...
```

---

# 3) Registry

## `src/codeintel/contracts/registry.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .base import Backend, DatasetContract


@dataclass(slots=True)
class ContractRegistry:
    _by_name: dict[str, DatasetContract]

    @classmethod
    def empty(cls) -> "ContractRegistry":
        return cls(_by_name={})

    def register(self, contract: DatasetContract) -> None:
        if contract.dataset in self._by_name:
            raise ValueError(f"Duplicate contract dataset name: {contract.dataset}")
        self._by_name[contract.dataset] = contract

    def get(self, dataset: str) -> DatasetContract:
        return self._by_name[dataset]

    def all(self) -> Iterable[DatasetContract]:
        return self._by_name.values()

    def compile_check(self, backends: Iterable[Backend]) -> list[str]:
        """
        “ABI check” for CI: ensure schema factories can be imported/constructed.
        Returns warnings (strings) rather than raising; callers can decide policy.
        """
        warnings: list[str] = []
        for c in self.all():
            for b in backends:
                if b not in c.schemas:
                    continue
                try:
                    _ = c.get_schema(b)
                except Exception as e:  # noqa: BLE001
                    warnings.append(f"{c.dataset} backend={b}: schema factory failed: {type(e).__name__}: {e}")
        return warnings
```

---

# 4) Golden normalization + IO

## `src/codeintel/contracts/golden.py`

```python
from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any


def _to_jsonable(x: Any) -> Any:
    if is_dataclass(x):
        return {k: _to_jsonable(v) for k, v in asdict(x).items()}
    if isinstance(x, dict):
        return {str(k): _to_jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_to_jsonable(v) for v in x]
    return x


def normalize_for_golden(payload: Any) -> Any:
    """
    Deterministic canonicalization:
    - stable key ordering (via json dump sort_keys)
    - stable ordering for lists of dict-like items (best-effort)
    """
    x = _to_jsonable(payload)

    def norm(v: Any) -> Any:
        if isinstance(v, dict):
            return {k: norm(v[k]) for k in sorted(v.keys())}
        if isinstance(v, list):
            vv = [norm(i) for i in v]
            # best-effort stable sort for list[dict]
            if vv and all(isinstance(i, dict) for i in vv):
                def key(d: dict[str, Any]) -> tuple:
                    # common pandera fields; safe if missing
                    return (
                        str(d.get("schema_context", "")),
                        str(d.get("column", "")),
                        str(d.get("check", "")),
                        str(d.get("failure_case", "")),
                        str(d.get("index", "")),
                    )
                return sorted(vv, key=key)
            return vv
        return v

    return norm(x)


def read_golden(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_golden(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    norm = normalize_for_golden(payload)
    path.write_text(json.dumps(norm, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def assert_matches_golden(path: Path, payload: Any, *, update: bool) -> None:
    norm = normalize_for_golden(payload)
    if update:
        write_golden(path, norm)
        return
    if not path.exists():
        raise AssertionError(f"Missing golden: {path}. Re-run with --update-goldens to create it.")
    expected = read_golden(path)
    if expected != norm:
        raise AssertionError(
            f"Golden mismatch: {path}\n"
            f"Tip: re-run with --update-goldens if this change is expected."
        )
```

---

# 5) Validator + adapter dispatch + policy context

## `src/codeintel/contracts/validator.py`

```python
from __future__ import annotations

import inspect
from contextlib import contextmanager
from typing import Any, Callable

from .base import Backend, DatasetContract, ValidationProfile, ValidationResult


def _filter_kwargs(fn: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return kwargs
    return {k: v for k, v in kwargs.items() if k in sig.parameters}


@contextmanager
def pandera_policy_context(profile: ValidationProfile):
    """
    Apply stage policy using pandera's config context when available.
    We filter kwargs by signature so this remains robust across pandera versions.
    """
    try:
        import pandera
        from pandera import config as pandera_config
    except Exception:  # noqa: BLE001
        # If pandera isn't importable, caller will fail later anyway.
        yield
        return

    kwargs: dict[str, Any] = {
        "validation_enabled": profile.enabled,
        "validation_depth": profile.depth.value,
        # pyspark-specific perf knobs (only apply if supported by config_context)
        "cache_dataframe": profile.spark_cache_dataframe,
        "keep_cached_dataframe": profile.spark_keep_cached_dataframe,
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    ctx_kwargs = _filter_kwargs(pandera_config.config_context, kwargs)

    with pandera_config.config_context(**ctx_kwargs):
        yield


def validate_contract(
    contract: DatasetContract,
    obj: Any,
    *,
    backend: Backend,
    stage: str,
    profile: ValidationProfile,
) -> tuple[Any, ValidationResult]:
    schema = contract.get_schema(backend)

    with pandera_policy_context(profile):
        if backend is Backend.pandas:
            from .adapters.pandas_backend import validate as v
        elif backend is Backend.polars:
            from .adapters.polars_backend import validate as v
        elif backend is Backend.ibis:
            from .adapters.ibis_backend import validate as v
        elif backend is Backend.pyspark:
            from .adapters.pyspark_backend import validate as v
        else:
            raise ValueError(f"Unknown backend: {backend}")

        return v(
            dataset=contract.dataset,
            stage=stage,
            schema=schema,
            obj=obj,
            profile=profile,
        )
```

---

# 6) Backend adapters

## `src/codeintel/contracts/adapters/pandas_backend.py`

```python
from __future__ import annotations

from typing import Any

from ..base import Backend, ValidationProfile, ValidationResult
from ..validator import _filter_kwargs


def _failure_cases_to_records(fc: Any, *, limit: int = 50) -> list[dict[str, Any]] | None:
    # pandas SchemaErrors often provides .failure_cases as a DataFrame
    if fc is None:
        return None
    try:
        # DataFrame-like
        recs = fc.to_dict(orient="records")
        return recs[:limit]
    except Exception:  # noqa: BLE001
        return None


def validate(*, dataset: str, stage: str, schema: Any, obj: Any, profile: ValidationProfile) -> tuple[Any, ValidationResult]:
    import pandera.errors as pe

    validate_kwargs = {
        "lazy": profile.lazy,
        "inplace": profile.inplace,
        "head": profile.head,
        "tail": profile.tail,
        "sample": profile.sample,
        "random_state": profile.random_state,
        "drop_invalid_rows": profile.drop_invalid_rows,
    }
    validate_kwargs = {k: v for k, v in validate_kwargs.items() if v is not None}

    try:
        fn = schema.validate  # DataFrameSchema instance OR DataFrameModel classmethod
        call_kwargs = _filter_kwargs(fn, validate_kwargs)
        out = fn(obj, **call_kwargs)
        return out, ValidationResult(
            ok=True,
            dataset=dataset,
            stage=stage,
            backend=Backend.pandas,
            profile=profile,
            stats={"validated": True},
        )
    except pe.SchemaErrors as e:
        # e.message is a machine-readable error report (dict-like)
        error_report = getattr(e, "message", None)
        failure_cases = _failure_cases_to_records(getattr(e, "failure_cases", None))
        return obj, ValidationResult(
            ok=False,
            dataset=dataset,
            stage=stage,
            backend=Backend.pandas,
            profile=profile,
            error_report=error_report if isinstance(error_report, dict) else {"error": str(error_report)},
            failure_cases=failure_cases,
            stats={"n_failure_cases": 0 if failure_cases is None else len(failure_cases)},
            exception_type=type(e).__name__,
            exception_message=str(e),
        )
    except pe.SchemaError as e:
        return obj, ValidationResult(
            ok=False,
            dataset=dataset,
            stage=stage,
            backend=Backend.pandas,
            profile=profile,
            error_report={"error": str(e)},
            failure_cases=None,
            stats={},
            exception_type=type(e).__name__,
            exception_message=str(e),
        )
```

## `src/codeintel/contracts/adapters/polars_backend.py`

```python
from __future__ import annotations

from typing import Any

from ..base import Backend, ValidationProfile, ValidationResult
from ..validator import _filter_kwargs


def validate(*, dataset: str, stage: str, schema: Any, obj: Any, profile: ValidationProfile) -> tuple[Any, ValidationResult]:
    import pandera.errors as pe

    validate_kwargs = {
        "lazy": profile.lazy,
        "inplace": profile.inplace,
        "head": profile.head,
        "tail": profile.tail,
        "sample": profile.sample,
        "random_state": profile.random_state,
        "drop_invalid_rows": profile.drop_invalid_rows,
    }
    validate_kwargs = {k: v for k, v in validate_kwargs.items() if v is not None}

    try:
        fn = schema.validate
        call_kwargs = _filter_kwargs(fn, validate_kwargs)
        out = fn(obj, **call_kwargs)
        return out, ValidationResult(
            ok=True,
            dataset=dataset,
            stage=stage,
            backend=Backend.polars,
            profile=profile,
            stats={"validated": True},
        )
    except pe.SchemaErrors as e:
        error_report = getattr(e, "message", None)
        fc = getattr(e, "failure_cases", None)
        failure_cases = None
        try:
            failure_cases = fc.to_dicts()[:50] if fc is not None else None  # polars df
        except Exception:  # noqa: BLE001
            pass
        return obj, ValidationResult(
            ok=False,
            dataset=dataset,
            stage=stage,
            backend=Backend.polars,
            profile=profile,
            error_report=error_report if isinstance(error_report, dict) else {"error": str(error_report)},
            failure_cases=failure_cases,
            stats={"n_failure_cases": 0 if failure_cases is None else len(failure_cases)},
            exception_type=type(e).__name__,
            exception_message=str(e),
        )
    except pe.SchemaError as e:
        return obj, ValidationResult(
            ok=False,
            dataset=dataset,
            stage=stage,
            backend=Backend.polars,
            profile=profile,
            error_report={"error": str(e)},
            failure_cases=None,
            stats={},
            exception_type=type(e).__name__,
            exception_message=str(e),
        )
```

## `src/codeintel/contracts/adapters/ibis_backend.py`

```python
from __future__ import annotations

from typing import Any

from ..base import Backend, ValidationProfile, ValidationResult
from ..validator import _filter_kwargs


def validate(*, dataset: str, stage: str, schema: Any, obj: Any, profile: ValidationProfile) -> tuple[Any, ValidationResult]:
    import pandera.errors as pe

    validate_kwargs = {
        "lazy": profile.lazy,
        # ibis validation may not support sampling/inplace; we filter by signature
        "head": profile.head,
        "tail": profile.tail,
        "sample": profile.sample,
        "random_state": profile.random_state,
    }
    validate_kwargs = {k: v for k, v in validate_kwargs.items() if v is not None}

    try:
        fn = schema.validate
        call_kwargs = _filter_kwargs(fn, validate_kwargs)
        out = fn(obj, **call_kwargs)
        return out, ValidationResult(
            ok=True,
            dataset=dataset,
            stage=stage,
            backend=Backend.ibis,
            profile=profile,
            stats={"validated": True},
        )
    except pe.SchemaErrors as e:
        error_report = getattr(e, "message", None)
        return obj, ValidationResult(
            ok=False,
            dataset=dataset,
            stage=stage,
            backend=Backend.ibis,
            profile=profile,
            error_report=error_report if isinstance(error_report, dict) else {"error": str(error_report)},
            failure_cases=None,
            stats={},
            exception_type=type(e).__name__,
            exception_message=str(e),
        )
    except pe.SchemaError as e:
        return obj, ValidationResult(
            ok=False,
            dataset=dataset,
            stage=stage,
            backend=Backend.ibis,
            profile=profile,
            error_report={"error": str(e)},
            failure_cases=None,
            stats={},
            exception_type=type(e).__name__,
            exception_message=str(e),
        )
```

## `src/codeintel/contracts/adapters/pyspark_backend.py`

```python
from __future__ import annotations

from typing import Any

from ..base import Backend, ValidationProfile, ValidationResult
from ..validator import _filter_kwargs


def validate(*, dataset: str, stage: str, schema: Any, obj: Any, profile: ValidationProfile) -> tuple[Any, ValidationResult]:
    """
    Spark backend typically returns a DataFrame with errors attached via df_out.pandera.errors.
    We convert that to a normalized ValidationResult. Optionally raise (fail_hard) for CI/serve.
    """
    # NOTE: this adapter assumes obj is a pyspark.sql.DataFrame.
    fn = schema.validate
    call_kwargs = _filter_kwargs(fn, {"lazy": profile.lazy})  # spark may ignore lazy; filter anyway

    df_out = fn(obj, **call_kwargs)

    errors = None
    try:
        errors = getattr(df_out, "pandera").errors  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        errors = None

    ok = not errors
    result = ValidationResult(
        ok=ok,
        dataset=dataset,
        stage=stage,
        backend=Backend.pyspark,
        profile=profile,
        error_report=errors if isinstance(errors, dict) else (None if ok else {"errors": str(errors)}),
        failure_cases=None,
        stats={"validated": True, "has_errors": not ok},
        exception_type=None,
        exception_message=None,
    )

    if (not ok) and profile.spark_fail_hard:
        raise AssertionError(f"Spark contract failed for dataset={dataset} stage={stage}: {result.error_report}")
    return df_out, result
```

---

# 7) Example contract (one dataset, all backends)

## `src/codeintel/contracts/examples/example_events.py`

This file uses **lazy schema factories** so missing optional deps don’t break import of the whole registry.

```python
from __future__ import annotations

from typing import Any

from ..base import Backend, DatasetContract


def _pandas_schema() -> Any:
    import pandera.pandas as pa
    from pandera.typing import Series

    class ExampleEvents(pa.DataFrameModel):
        id: Series[int] = pa.Field(gt=0, unique=True)
        kind: Series[str] = pa.Field(isin=["A", "B", "C"])
        value: Series[float] = pa.Field(ge=0)

        class Config:
            strict = "filter"
            coerce = True

    return ExampleEvents


def _polars_schema() -> Any:
    import pandera.polars as pa

    return pa.DataFrameSchema(
        {
            "id": pa.Column(int, checks=[pa.Check.gt(0)], unique=True),
            "kind": pa.Column(str, checks=[pa.Check.isin(["A", "B", "C"])]),
            "value": pa.Column(float, checks=[pa.Check.ge(0)]),
        },
        strict="filter",
        coerce=True,
    )


def _ibis_schema() -> Any:
    import pandera.ibis as pa

    return pa.DataFrameSchema(
        {
            "id": pa.Column(int, checks=[pa.Check.gt(0)], unique=True),
            "kind": pa.Column(str, checks=[pa.Check.isin(["A", "B", "C"])]),
            "value": pa.Column(float, checks=[pa.Check.ge(0)]),
        },
        strict="filter",
        coerce=True,
    )


def _pyspark_schema() -> Any:
    import pandera.pyspark as pa
    from pyspark.sql import types as T

    return pa.DataFrameSchema(
        {
            "id": pa.Column(T.LongType(), checks=[pa.Check.gt(0)], unique=True),
            "kind": pa.Column(T.StringType(), checks=[pa.Check.isin(["A", "B", "C"])]),
            "value": pa.Column(T.DoubleType(), checks=[pa.Check.ge(0)]),
        },
        strict="filter",
    )


EXAMPLE_EVENTS = DatasetContract(
    dataset="example_events",
    version="1.0.0",
    description="Tiny example dataset used to prove the contract harness.",
    schemas={
        Backend.pandas: _pandas_schema,
        Backend.polars: _polars_schema,
        Backend.ibis: _ibis_schema,
        Backend.pyspark: _pyspark_schema,
    },
)
```

---

# 8) Package exports

## `src/codeintel/contracts/__init__.py`

```python
from .base import Backend, DatasetContract, ValidationDepth, ValidationProfile, ValidationResult, RunRecorder
from .registry import ContractRegistry
from .validator import validate_contract

__all__ = [
    "Backend",
    "DatasetContract",
    "ValidationDepth",
    "ValidationProfile",
    "ValidationResult",
    "RunRecorder",
    "ContractRegistry",
    "validate_contract",
]
```

---

# 9) Hamilton integration: contracted materialization (run-registry ready)

## `src/codeintel/hamilton/materialization/contracted.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from codeintel.contracts import Backend, ContractRegistry, RunRecorder, ValidationProfile, validate_contract


class StorageGateway(Protocol):
    def write_dataset(self, dataset: str, obj: Any) -> None: ...


@dataclass(frozen=True, slots=True)
class MaterializeArgs:
    dataset: str
    backend: Backend
    stage: str = "materialize"
    profile: ValidationProfile = ValidationProfile()


def contracted_materialize(
    obj: Any,
    *,
    args: MaterializeArgs,
    registry: ContractRegistry,
    storage: StorageGateway,
    run_recorder: RunRecorder | None = None,
) -> Any:
    contract = registry.get(args.dataset)
    validated, result = validate_contract(
        contract,
        obj,
        backend=args.backend,
        stage=args.stage,
        profile=args.profile,
    )

    # store validation result BEFORE write if you want "why we failed" persisted
    if run_recorder is not None:
        run_recorder.record_validation(result)

    if not result.ok:
        # fail hard by default at materialization boundary
        raise AssertionError(
            f"Contract violation at materialize: dataset={args.dataset} backend={args.backend} stage={args.stage}"
        )

    storage.write_dataset(args.dataset, validated)
    return validated
```

> This is intentionally “thin”: you plug in your real `StorageGateway` and your real run-registry recorder (pipeline_steps).

---

# 10) Pytest golden harness

## `src/codeintel/contracts/pytest_plugin.py`

```python
from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-goldens",
        action="store_true",
        default=False,
        help="Update contract golden snapshots in-place.",
    )


@pytest.fixture
def update_goldens(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.getoption("--update-goldens"))
```

## `tests/contracts/conftest.py`

```python
from __future__ import annotations

import pytest

from codeintel.contracts import ContractRegistry
from codeintel.contracts.examples.example_events import EXAMPLE_EVENTS

pytest_plugins = ["codeintel.contracts.pytest_plugin"]


@pytest.fixture(scope="session")
def contract_registry() -> ContractRegistry:
    reg = ContractRegistry.empty()
    reg.register(EXAMPLE_EVENTS)
    return reg
```

## `tests/contracts/test_registry_compile.py`

```python
from __future__ import annotations

import pytest

from codeintel.contracts import Backend


def _installed_backends() -> list[Backend]:
    backends: list[Backend] = [Backend.pandas]

    try:
        __import__("polars")
        backends.append(Backend.polars)
    except Exception:
        pass

    try:
        __import__("ibis")
        backends.append(Backend.ibis)
    except Exception:
        pass

    try:
        __import__("pyspark")
        backends.append(Backend.pyspark)
    except Exception:
        pass

    return backends


def test_registry_compile(contract_registry):
    warnings = contract_registry.compile_check(_installed_backends())
    if warnings:
        # make it a hard fail in CI; change to warn if desired
        raise AssertionError("Contract compile check failed:\n" + "\n".join(warnings))
```

## `tests/contracts/test_example_events_goldens.py`

```python
from __future__ import annotations

from pathlib import Path

import pytest

from codeintel.contracts import Backend, ValidationDepth, ValidationProfile, validate_contract
from codeintel.contracts.golden import assert_matches_golden


GOLDEN_ROOT = Path(__file__).parent / "golden" / "contracts"


@pytest.mark.parametrize("backend", [Backend.pandas, Backend.polars, Backend.ibis])
def test_example_events_invalid_matches_golden_non_spark(contract_registry, update_goldens: bool, backend: Backend):
    if backend is Backend.polars:
        pytest.importorskip("polars")
    if backend is Backend.ibis:
        pytest.importorskip("ibis")

    contract = contract_registry.get("example_events")

    # intentionally invalid fixture per backend
    if backend is Backend.pandas:
        import pandas as pd
        obj = pd.DataFrame({"id": [0, 1], "kind": ["A", "Z"], "value": [-1.0, 1.0], "extra": [1, 2]})
    elif backend is Backend.polars:
        import polars as pl
        obj = pl.DataFrame({"id": [0, 1], "kind": ["A", "Z"], "value": [-1.0, 1.0], "extra": [1, 2]})
    else:  # ibis
        import ibis
        obj = ibis.memtable({"id": [0, 1], "kind": ["A", "Z"], "value": [-1.0, 1.0], "extra": [1, 2]})

    profile = ValidationProfile(
        enabled=True,
        depth=ValidationDepth.schema_and_data,
        lazy=True,
        inplace=False,
    )

    _, result = validate_contract(contract, obj, backend=backend, stage="ingest", profile=profile)

    # We EXPECT invalid for this test
    if result.ok:
        raise AssertionError(f"Expected contract to fail for backend={backend}")

    golden_path = GOLDEN_ROOT / contract.dataset / "ingest" / f"{backend.value}.json"
    assert_matches_golden(golden_path, result, update=update_goldens)


@pytest.mark.spark
def test_example_events_invalid_matches_golden_pyspark(contract_registry, update_goldens: bool):
    pyspark = pytest.importorskip("pyspark")

    # spark session fixture is intentionally not provided here;
    # in your repo, wire your standard SparkSession fixture.
    pytest.skip("Wire SparkSession fixture (spark) and create spark df fixture here.")
```

---

# 11) “How to run” (dev ergonomics)

1. Generate goldens:

```bash
pytest -q tests/contracts --update-goldens
```

2. Normal CI run:

```bash
pytest -q tests/contracts
```

3. Run only pandas contracts:

```bash
pytest -q tests/contracts -k pandas
```

---

# 12) Where to hook into your existing DatasetContract + run-registry + Hamilton style

* If you already have a `DatasetContract` concept: keep your existing type, and **adopt only**:

  * `ValidationProfile`,
  * `ValidationResult`,
  * adapter-backed `validate_contract()`,
  * pytest golden harness.

* If you already have run tables (`pipeline_runs`, `pipeline_steps`):

  * implement `RunRecorder.record_validation(result)` to insert:

    * `dataset`, `stage`, `backend`, `ok`,
    * `profile` (as json),
    * `error_report` (as json),
    * `failure_cases` (bounded json),
    * plus your run identifiers.

* In Hamilton:

  * call `contracted_materialize(...)` inside your `t__<dataset>` nodes.
  * (Optional) tag nodes with `dataset=<name>` and `stage=materialize` to auto-derive `MaterializeArgs`.

---

If you want one more increment immediately after this: I can add a **SparkSession test fixture + a “spark contracts” marker policy** (so Spark runs only on CI jobs that opt-in), and a **DuckDB/Ibis test backend fixture** that makes ibis contract tests deterministic and fast.
