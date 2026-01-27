
# Section A — Scalar UDFs in DataFusion (Rust)

A **Scalar UDF** is a *vectorized* function: DataFusion evaluates it over **Arrow arrays (batches)** and expects an output with the **same row count**. Conceptually it’s “row-wise”, but implemented “batch-wise” for throughput. ([Apache DataFusion][1])

---

## A0) Mental model and the three API layers

### Layer 1: `create_udf` (concise / legacy-ish)

`create_udf` builds a `ScalarUDF` from a name, fixed input types, fixed return type, volatility, and an `Fn(&[ColumnarValue]) -> Result<ColumnarValue>`. ([Docs.rs][2])
It’s intentionally limited (no dynamic return types, no multiple signatures, no aliases). ([Docs.rs][2])

### Layer 2: `SimpleScalarUDF` (trait API, but still “single signature + fixed return”)

`SimpleScalarUDF` implements `ScalarUDFImpl` for you when you have one signature and one return type, but still plugs into the modern trait surface. ([Docs.rs][3])

### Layer 3: Implement `ScalarUDFImpl` (full power)

You implement `ScalarUDFImpl` directly when you need:

* **dynamic return type / metadata** via `return_field_from_args`
* **aliases**
* **custom coercion** (`TypeSignature::UserDefined` + `coerce_types`)
* **optimizer hooks** (`simplify`, `short_circuits`, ordering / bounds propagation, config-driven instances)

This is the “best-in-class” layer. ([Docs.rs][4])

---

## A1) Core “carriers”: `ColumnarValue` + `ScalarValue`

### `ColumnarValue`

`ColumnarValue` is what scalar UDFs consume/produce at runtime:

```rust
pub enum ColumnarValue {
    Array(Arc<dyn Array>),
    Scalar(ScalarValue),
}
```

`ColumnarValue::Scalar` represents a **single value repeated across rows** (a key performance optimization). ([Docs.rs][5])

Key helpers:

* `values_to_arrays(args)` → expands any `Scalar` to an array to match lengths (simple, but can be inefficient). ([Docs.rs][6])
* `into_array(num_rows)` / `to_array(num_rows)` → expand a scalar by repetition (again, easy but not always fast). ([Docs.rs][6])
* `into_array_of_size` / `to_array_of_size` → like above, but validates output length. ([Docs.rs][6])
* `cast_to(cast_type, cast_options)` → cast a value (array or scalar) to a `DataType`. ([Docs.rs][6])

### `ScalarValue`

`ScalarValue` is the dynamically typed single-value representation. It supports string variants including `Utf8`, `LargeUtf8`, and `Utf8View`, and includes helper methods like `try_as_str` for string extraction. ([Docs.rs][7])

---

## A2) Typing & resolution: `Signature`, `TypeSignature`, `Volatility`

### `Volatility`

Choose the strictest volatility you can:

* `Immutable` → same output for same input; DataFusion may inline/fold at planning time (e.g. `abs(-1)` becomes `1`). ([Docs.rs][8])
* `Stable`
* `Volatile` ([Docs.rs][8])

### `TypeSignature`

`TypeSignature` describes the argument type patterns you actually implement. Crucially: **it does not describe all types a user may call with**—DataFusion will try to **coerce** arguments to match the declared signature during planning. ([Docs.rs][9])

Important variants (non-exhaustive):

* `Exact(Vec<DataType>)`, `Uniform(n, valid_types)`, `Numeric(n)`, `String(n)`, `Any(n)`
* `OneOf(Vec<TypeSignature>)`
* `Coercible(Vec<Coercion>)`
* `Nullary`, `Variadic`, `VariadicAny`
* `UserDefined` → triggers your `coerce_types` hook ([Docs.rs][9])

### `Signature`

`Signature` = `{ type_signature, volatility, parameter_names }`. It also supports **named arguments** via `parameter_names` / `with_parameter_names`, with constraints (e.g., cannot name params for variadic signatures). ([Docs.rs][10])

---

## A3) The planning/execution interface: `ScalarUDF`, `ScalarUDFImpl`, `ScalarFunctionArgs`

### `ScalarUDF`

`ScalarUDF` is the *logical* UDF object DataFusion registers and plans with. It wraps a `ScalarUDFImpl`. It includes:

* `new_from_impl` / `new_from_shared_impl`
* `inner()` to access the trait object
* `with_aliases(...)`
* `call(args: Vec<Expr>) -> Expr` (build an expression invoking the UDF) ([Docs.rs][11])

### `ScalarUDFImpl` (full API)

Trait bounds include `Send + Sync` (instances may be shared across threads). ([Docs.rs][4])

**Required methods**:

* `as_any(&self) -> &dyn Any`
* `name(&self) -> &str`
* `signature(&self) -> &Signature`
* `return_type(&self, arg_types: &[DataType]) -> Result<DataType>`
* `invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue>` ([Docs.rs][4])

**Performance note (official)**: handle `ColumnarValue::Scalar` fast paths where possible; `values_to_arrays` is simpler but slower. ([Docs.rs][4])

### `ScalarFunctionArgs`

Runtime arguments passed to `invoke_with_args`:

* `args: Vec<ColumnarValue>` (evaluated inputs)
* `arg_fields: Vec<Arc<Field>>` (per-arg fields/metadata)
* `number_rows: usize` (batch size)
* `return_field: Arc<Field>` (planner-computed output field)
* `config_options: Arc<ConfigOptions>` ([Docs.rs][12])

This is the “modern” interface that makes metadata/extension types workable. ([Apache DataFusion][13])

---

## A4) Return typing & metadata: `return_field_from_args` + `ReturnFieldArgs`

Historically, UDFs often only used `return_type`. Newer DataFusion versions encourage `return_field_from_args` so you can set **type + nullability + metadata** together; older `return_type_from_args` patterns are replaced by this approach. ([Apache DataFusion][13])

### `ReturnFieldArgs`

Includes:

* `arg_fields: &[Arc<Field>]`
* `scalar_arguments: &[Option<&ScalarValue>]` (lets you detect literal arguments at planning time) ([Docs.rs][14])

Key rule (important for agents implementing “value-dependent output type”):

> If output type depends on argument *values*, you must return the **same type for the same logical input even after simplification** (e.g., constant folding / expression rewrites). ([Docs.rs][4])

Also: the **output `Field` name is ignored** *except for structured types like `DataType::Struct`* where field names matter. ([Docs.rs][4])

---

## A5) Optimizer/semantic hooks you can override (high leverage)

These are “advanced knobs” on `ScalarUDFImpl` that materially affect correctness/performance:

### `simplify(args, info) -> ExprSimplifyResult`

* Lets you rewrite the call (e.g., `arrow_cast(x, 'Int32') -> Expr::Cast`)
* **Must** return args unmodified if no simplification is applied
* Returned expression must preserve **both type and nullability**, or planning can fail ([Docs.rs][4])

DataFusion already does argument simplification and constant folding; don’t re-implement constant folding unless you’re doing something function-specific. ([Docs.rs][4])

### `short_circuits() -> bool` and `conditional_arguments(args)`

If your function may not evaluate all args (think `if_then_else`, `coalesce`, etc.), mark `short_circuits = true` to prevent optimizations that assume eager evaluation; optionally describe eager vs lazy args. ([Docs.rs][4])

### `coerce_types(arg_types) -> Vec<DataType>` (only with `TypeSignature::UserDefined`)

If you set `TypeSignature::UserDefined`, DataFusion will call `coerce_types` so you can pick explicit cast targets. ([Docs.rs][9])

### Ordering / bounds / constraints

* `evaluate_bounds`, `propagate_constraints` (interval arithmetic)
* `output_ordering`, `preserves_lex_ordering` (ordering propagation) ([Docs.rs][4])

### `with_updated_config(config) -> Option<ScalarUDF>`

If behavior depends on runtime `ConfigOptions`, you can create a config-specialized instance. ([Docs.rs][4])

---

## A6) Invariants & footguns (what an LLM agent should “auto-check”)

### Row-count invariant

If you return `ColumnarValue::Array`, the array length must match `args.number_rows` (or you’ll trip runtime/schema validation). Use `into_array_of_size` / `to_array_of_size` to validate when assembling output. ([Docs.rs][6])

### Scalar fast paths

Blindly calling `values_to_arrays` expands scalars to full-length arrays and can be expensive; DataFusion explicitly recommends specialized scalar handling when performance matters. ([Docs.rs][4])

### `Send + Sync` / shared instances

Your `ScalarUDFImpl` can be shared across threads. Avoid non-thread-safe interior mutability. If you cache (e.g., compiled regex), use thread-safe structures or immutable caches. ([Docs.rs][4])

### Value-dependent return types

If return type depends on a literal argument, implement `return_field_from_args` and enforce “literal-only” at planning time using `scalar_arguments`. Respect the “same logical input ⇒ same type” requirement. ([Docs.rs][14])

### `simplify` correctness trap

If you rewrite expressions in `simplify`, the rewritten expression must preserve schema (type + nullability). If you can’t simplify, return args unmodified. ([Docs.rs][4])

### Named arguments

If you add `parameter_names`, ensure name count matches arity; variadic signatures cannot specify parameter names. ([Docs.rs][10])

---

## A7) Performance patterns (battle-tested)

### Pattern 1: Handle `ColumnarValue::Scalar` without full expansion

* Scalar–scalar: compute once, return `ColumnarValue::Scalar`
* Array–scalar: prefer kernels that accept scalar, or implement a special path
* If you *must* “arrayify” a scalar, consider O(1)-sized expansion strategies (e.g., some internals use `make_scalar_function` with an “accepts singular” hint to avoid per-batch O(n) expansions, depending on kernel support). ([GitHub][15])

### Pattern 2: Use Arrow kernels when possible

Prefer `arrow::compute` kernels (or DataFusion helpers) over manual row loops for numeric/boolean transforms.

### Pattern 3: Don’t throw away `arg_fields` / `return_field`

If you’re dealing with extension types / metadata-aware behavior, you want `arg_fields` and `return_field`. DataFusion explicitly migrated UDF interfaces toward `FieldRef`-style metadata handling. ([Apache DataFusion][13])

### Pattern 4: Cache compile-heavy constants safely

If an argument is a literal (e.g., regex pattern), detect it via `ColumnarValue::Scalar` and cache compiled forms in the UDF instance (thread-safe). This is a common tactic discussed by DataFusion contributors. ([GitHub][15])

---

# A8) End-to-end examples

## Example 1 (Simple): `add_one(i64) -> i64` via `create_udf`

### Implementation

```rust
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{cast::as_int64_array, Result};
use datafusion::logical_expr::ColumnarValue;

pub fn add_one(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Simple but may expand scalar args to arrays (see perf notes).
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let input = as_int64_array(&arrays[0])?;

    let out = input
        .iter()
        .map(|v| v.map(|x| x + 1))
        .collect::<Int64Array>();

    Ok(ColumnarValue::from(Arc::new(out) as ArrayRef))
}
```

### Register + call

```rust
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{create_udf, Volatility};
use datafusion::arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let udf = create_udf(
        "add_one",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(add_one),
    );

    let mut ctx = SessionContext::new();
    ctx.register_udf(udf);

    let df = ctx.sql("SELECT add_one(1) AS x").await?;
    df.show().await?;
    Ok(())
}
```

**Notes**

* `create_udf`’s signature and limitations are explicit in the docs. ([Docs.rs][2])
* `values_to_arrays` is easy but may be slower due to scalar expansion. ([Docs.rs][6])

---

## Example 2 (Advanced): `cast_as(value, type_name_literal) -> <dynamic type>`

This demonstrates **value-dependent output type** using `return_field_from_args`, and then uses the runtime `return_field` (via `ScalarFunctionArgs`) to drive execution.

### Behavior

* `cast_as(col, 'Int16')` returns `Int16`
* `cast_as(col, 'Float64')` returns `Float64`
* If `type_name` is not a literal scalar, planning errors (because output type can’t be determined reliably).

### Implementation

```rust
use std::{any::Any, sync::Arc};

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result, DataFusionError};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CastAs {
    signature: Signature,
}

impl CastAs {
    pub fn new() -> Self {
        // Accept any two args; we will enforce "2nd is string literal" ourselves.
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    fn parse_type_name(s: &str) -> Result<DataType> {
        let t = s.trim().to_ascii_lowercase();
        let dt = match t.as_str() {
            "int8" => DataType::Int8,
            "int16" => DataType::Int16,
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "uint8" => DataType::UInt8,
            "uint16" => DataType::UInt16,
            "uint32" => DataType::UInt32,
            "uint64" => DataType::UInt64,
            "float32" => DataType::Float32,
            "float64" => DataType::Float64,
            "utf8" => DataType::Utf8,
            "largeutf8" => DataType::LargeUtf8,
            "utf8view" => DataType::Utf8View,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "cast_as: unsupported type name '{other}'"
                )))
            }
        };
        Ok(dt)
    }
}

impl ScalarUDFImpl for CastAs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cast_as"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // If return_field_from_args is implemented, DataFusion won't call return_type.
        // Return an internal error rather than panicking.
        Err(DataFusionError::Internal(
            "cast_as uses return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<Arc<Field>> {
        // Require type_name to be a scalar string literal:
        let type_sv = args
            .scalar_arguments
            .get(1)
            .and_then(|x| *x)
            .ok_or_else(|| DataFusionError::Plan("cast_as: 2nd arg must be a string literal".into()))?;

        // ScalarValue provides try_as_str for string-like variants.
        let type_name = type_sv
            .try_as_str()
            .map_err(|e| DataFusionError::Plan(format!("cast_as: invalid type literal: {e}")))?;

        let out_dt = Self::parse_type_name(type_name)?;
        let nullable = args.arg_fields[0].is_nullable();

        // Field name is ignored for non-struct outputs.
        Ok(Arc::new(Field::new("ignored_name", out_dt, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Use the planner-computed return type (from return_field_from_args).
        let target_type = args.return_type().clone();
        let value = &args.args[0];

        value.cast_to(&target_type, None)
    }
}

pub fn cast_as_udf() -> ScalarUDF {
    ScalarUDF::from(CastAs::new())
}
```

### Register + call

```rust
use datafusion::execution::context::SessionContext;

#[tokio::main]
async fn main() -> datafusion::common::Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_udf(cast_as_udf());

    let df = ctx.sql("SELECT cast_as('123', 'Int64') AS x").await?;
    df.show().await?;

    Ok(())
}
```

**Why this example is “advanced”**

* Uses `ReturnFieldArgs.scalar_arguments` to make output type depend on a literal argument. ([Docs.rs][14])
* Uses `ScalarFunctionArgs.return_field/return_type()` (planner-provided) at execution time. ([Docs.rs][12])
* Mirrors the UDF interface migration toward `FieldRef`/metadata-aware planning. ([Apache DataFusion][13])
* Shows the “don’t panic in `return_type` if you use `return_field_from_args`” guidance. ([Docs.rs][4])
* Uses `ScalarValue::try_as_str` (available on string variants like `Utf8`, `Utf8View`, `LargeUtf8`). ([Docs.rs][7])

---

## Example 3 (Constant-fast-path): `regex_is_match(text, pattern_literal_or_column) -> bool`

Goal: avoid repeated work when `pattern` is a **literal** by detecting `ColumnarValue::Scalar` and caching compiled regex safely.

### Key idea

DataFusion contributors explicitly call out:

* Recognize `ColumnarValue::Scalar`
* Optionally store compiled regex on a `ScalarUDFImpl` instance (thread-safe / shared) ([GitHub][15])

### Implementation sketch (thread-safe cache)

```rust
use std::{any::Any, sync::Arc};

use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{cast::as_string_array, Result, DataFusionError};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;

// Thread-safe map is recommended if you cache multiple patterns.
// (DashMap / parking_lot / std locks are all viable; pick one policy-wise.)
use dashmap::DashMap;

#[derive(Debug)]
pub struct RegexIsMatch {
    signature: Signature,
    cache: DashMap<String, Arc<Regex>>,
}

impl RegexIsMatch {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
            cache: DashMap::new(),
        }
    }

    fn compile_cached(&self, pat: &str) -> Result<Arc<Regex>> {
        if let Some(r) = self.cache.get(pat) {
            return Ok(r.clone());
        }
        let re = Regex::new(pat)
            .map_err(|e| DataFusionError::Execution(format!("invalid regex '{pat}': {e}")))?;
        let re = Arc::new(re);
        self.cache.insert(pat.to_string(), re.clone());
        Ok(re)
    }
}

impl ScalarUDFImpl for RegexIsMatch {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "regex_is_match" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;

        // Cast both args to Utf8 for simplicity (trade-off: may lose Utf8View perf).
        let text_cv = args.args[0].cast_to(&DataType::Utf8, None)?;
        let pat_cv  = args.args[1].cast_to(&DataType::Utf8, None)?;

        // Fast path: both scalars → single scalar bool.
        if let (ColumnarValue::Scalar(text_sv), ColumnarValue::Scalar(pat_sv)) = (&text_cv, &pat_cv) {
            let text = text_sv.try_as_str().map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let pat  = pat_sv.try_as_str().map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let re = self.compile_cached(pat)?;
            return Ok(ColumnarValue::Scalar((re.is_match(text)).into()));
        }

        // Pattern scalar fast path (common in SQL: WHERE regex_is_match(col, 'literal'))
        let re_opt: Option<Arc<Regex>> = match &pat_cv {
            ColumnarValue::Scalar(pat_sv) => {
                let pat = pat_sv.try_as_str().map_err(|e| DataFusionError::Execution(e.to_string()))?;
                Some(self.compile_cached(pat)?)
            }
            _ => None,
        };

        // Convert text to an array of length n
        let text_arr: ArrayRef = text_cv.to_array(n)?;
        let text_arr = as_string_array(&text_arr)?;

        // Convert pattern to array only if needed
        let out: BooleanArray = match re_opt {
            Some(re) => {
                let vals = text_arr.iter().map(|opt_s| opt_s.map(|s| re.is_match(s)));
                vals.collect()
            }
            None => {
                // Fallback: pattern varies per-row (less common, but correct)
                let pat_arr: ArrayRef = pat_cv.to_array(n)?;
                let pat_arr = as_string_array(&pat_arr)?;

                let vals = text_arr
                    .iter()
                    .zip(pat_arr.iter())
                    .map(|(t, p)| match (t, p) {
                        (Some(t), Some(p)) => Regex::new(p).ok().map(|re| re.is_match(t)),
                        _ => None,
                    });
                vals.collect()
            }
        };

        Ok(ColumnarValue::from(Arc::new(out) as ArrayRef))
    }
}

pub fn regex_is_match_udf() -> ScalarUDF {
    ScalarUDF::from(RegexIsMatch::new())
}
```

**Notes**

* This illustrates DataFusion’s recommended pattern: special-case `ColumnarValue::Scalar` to avoid costly scalar expansion. ([Docs.rs][4])
* `ScalarValue::try_as_str` makes scalar string extraction straightforward (covers `Utf8`, `Utf8View`, `LargeUtf8`). ([Docs.rs][7])
* If you want *maximum* performance with modern SQL schemas, add a dedicated `Utf8View` array path rather than casting to `Utf8` (casting may undo the `Utf8View` performance advantage). ([Apache DataFusion][13])

---

## A9) Documentation metadata (optional but “professional-grade”)

You can attach documentation through `ScalarUDFImpl::documentation()`; DataFusion’s SQL function documentation is generated from these `Documentation` structs. ([Docs.rs][16])

---

## A10) Minimal “LLM agent” implementation checklist

When generating or reviewing a scalar UDF patch, enforce:

1. **Signature**: correct `Signature` / `TypeSignature` / `Volatility` chosen (strictest volatility). ([Docs.rs][8])
2. **Return typing**:

   * fixed type → implement `return_type`
   * metadata/value-dependent type → implement `return_field_from_args` (+ consistent-type rule) ([Apache DataFusion][13])
3. **Row counts**: any returned arrays match `number_rows`. ([Docs.rs][12])
4. **Scalar fast paths**: avoid `values_to_arrays` in hot paths; special-case `ColumnarValue::Scalar`. ([Docs.rs][4])
5. **Thread safety**: `ScalarUDFImpl` is `Send + Sync`; caches must be safe. ([Docs.rs][4])
6. **Optimizer hooks** (only if needed): `simplify` schema-preserving; `short_circuits` if lazy eval possible. ([Docs.rs][4])

---

If you want, next we can turn this into a **drop-in “pyarrow-advanced.md style” file section** (with a TOC block, “pitfall callouts”, and a “golden test harness template” for RecordBatch-based unit tests) and then proceed to **Section B (Async Scalar UDFs)**.

[1]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html?utm_source=chatgpt.com "Adding User Defined Functions: Scalar/Window/Aggregate ..."
[2]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.create_udf.html "create_udf in datafusion::logical_expr - Rust"
[3]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr_fn/struct.SimpleScalarUDF.html "SimpleScalarUDF in datafusion_expr::expr_fn - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html "ScalarUDFImpl in datafusion::logical_expr - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.ColumnarValue.html?utm_source=chatgpt.com "ColumnarValue in datafusion::logical_expr - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.ColumnarValue.html "ColumnarValue in datafusion::logical_expr - Rust"
[7]: https://docs.rs/deltalake/latest/deltalake/datafusion/common/scalar/enum.ScalarValue.html "ScalarValue in deltalake::datafusion::common::scalar - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Volatility.html "Volatility in datafusion::logical_expr - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.TypeSignature.html "TypeSignature in datafusion::logical_expr - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Signature.html "Signature in datafusion::logical_expr - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.ScalarUDF.html "ScalarUDF in datafusion::logical_expr - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.ScalarFunctionArgs.html "ScalarFunctionArgs in datafusion::logical_expr - Rust"
[13]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[14]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.ReturnFieldArgs.html "ReturnFieldArgs in datafusion::logical_expr - Rust"
[15]: https://github.com/apache/datafusion/discussions/13852 "Optimising regex functions (and literal function arguments in general) · apache datafusion · Discussion #13852 · GitHub"
[16]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.Documentation.html?utm_source=chatgpt.com "Documentation in datafusion_expr - Rust"

# Section B — Async Scalar UDFs in DataFusion (Rust)

Async Scalar UDFs exist for one reason: **you need `await` inside a scalar function** (HTTP calls, RPCs, DB lookups, LLM completions, etc.) without blocking the query engine’s execution threads. DataFusion added first-class support so these calls can be integrated into SQL / DataFrame execution. ([Apache DataFusion][1])

---

## B0) What’s different vs normal Scalar UDFs

A “normal” scalar UDF (`ScalarUDFImpl`) is expected to be **fast and fully synchronous**, often backed by Arrow kernels. Async UDFs add overhead and are explicitly described as **less efficient** than sync scalar UDFs, but enable remote/async integrations. ([Docs.rs][2])

---

# B1) Async trait layer

## B1.1 API inventory (types, traits, functions)

### `AsyncScalarUDFImpl` (trait)

Location: `datafusion::logical_expr::async_udf::AsyncScalarUDFImpl`

Key properties:

* **Extends `ScalarUDFImpl`** → you *must* also implement the standard scalar UDF interface (name/signature/return typing). ([Docs.rs][2])
* **Required method**: an async invocation entry point.
* **Provided method**: `ideal_batch_size()` to control chunking.

Current signature (DataFusion 52.x docs.rs):

* `invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Future<Output = Result<ColumnarValue, DataFusionError>>`
* `ideal_batch_size(&self) -> Option<usize>` (“if None, evaluate whole batch at once”). ([Docs.rs][2])

> ⚠️ Version footgun: early public examples/blogs showed an async signature that returned `ArrayRef` and accepted `&ConfigOptions`. Newer APIs return `ColumnarValue` and `ConfigOptions` is available via `ScalarFunctionArgs.config_options`. Don’t cargo-cult older snippets—match your DataFusion version’s trait signature. ([Apache DataFusion][1])

---

### `AsyncScalarUDF` (struct wrapper)

Location: `datafusion::logical_expr::async_udf::AsyncScalarUDF`

Methods that matter:

* `AsyncScalarUDF::new(inner: Arc<dyn AsyncScalarUDFImpl>)`
* `ideal_batch_size()`
* `invoke_async_with_args(...)` (async)
* `into_scalar_udf(self) -> ScalarUDF` (**conversion step used for registration**) ([Docs.rs][3])

Important note from docs.rs: `AsyncScalarUDF` is an implementation detail wrapper around the async trait, but it’s the standard way to get something registerable. ([Docs.rs][3])

---

### `ScalarFunctionArgs` (runtime argument carrier)

Async UDFs receive the **same** `ScalarFunctionArgs` payload as sync UDFs, including:

* `args: Vec<ColumnarValue>`
* `number_rows`
* `arg_fields`, `return_field`
* `config_options: Arc<ConfigOptions>` ([Docs.rs][4])

This is how you access runtime config (credentials, endpoints, timeouts, knobs) without needing an extra `&ConfigOptions` parameter. ([Docs.rs][4])

---

## B1.2 Contract & lifecycle (what DataFusion calls, when)

### Planning / type resolution stage

Because `AsyncScalarUDFImpl: ScalarUDFImpl`, DataFusion uses your **ScalarUDFImpl methods** during planning:

* `name`, `signature`, `return_type` / `return_field_from_args`, volatility, etc.
* Named args and coercions work the same way (because those are part of scalar UDF planning). ([Docs.rs][2])

### Execution stage

At execution time, DataFusion evaluates async scalar UDFs via a **special physical plan node**:

* The official example shows `AsyncFuncExec` appears in `EXPLAIN` output and produces an internal column (e.g., `__async_fn_0`) that downstream nodes (filters/projections) consume. ([Apache Git Repositories][5])

Crucial behavioral point:

* Your **sync** `invoke_with_args` is **not called**; example implementations deliberately return `not_impl_err!(...)` there. ([Apache DataFusion][6])

---

## B1.3 Conversion / registration path (async UDF → registered ScalarUDF)

This is the canonical “wiring” sequence:

1. Implement `ScalarUDFImpl` (for name/signature/return typing) and make `invoke_with_args` unreachable.
2. Implement `AsyncScalarUDFImpl` with `async fn invoke_async_with_args(...)`.
3. Wrap it: `AsyncScalarUDF::new(Arc::new(my_impl))`
4. Convert: `.into_scalar_udf()`
5. Register: `ctx.register_udf(...)`

This exact flow is shown in the DataFusion user guide and the official example. ([Apache DataFusion][6])

### Minimal skeleton (agent-ready)

```rust
use std::{any::Any, sync::Arc};
use async_trait::async_trait;

use datafusion::common::{Result, not_impl_err};
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::{SessionContext};

#[derive(Debug, PartialEq, Eq, Hash)]
struct MyAsyncUdf {
    signature: Signature,
}

impl MyAsyncUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for MyAsyncUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "my_async" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _arg_types: &[arrow_schema::DataType]) -> Result<arrow_schema::DataType> {
        Ok(arrow_schema::DataType::Utf8)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("my_async can only be called from async contexts")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for MyAsyncUdf {
    fn ideal_batch_size(&self) -> Option<usize> { Some(64) } // optional
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // args.config_options is available here
        // do async I/O, return ColumnarValue with args.number_rows rows
        todo!()
    }
}

// registration
fn register(ctx: &SessionContext) {
    let udf = AsyncScalarUDF::new(Arc::new(MyAsyncUdf::new())).into_scalar_udf();
    ctx.register_udf(udf);
}
```

---

## B1.4 `ideal_batch_size`: why it exists and how it’s used

`ideal_batch_size()` lets the engine decide **how many rows to evaluate per async call**:

* `None` → evaluate the whole input batch at once
* `Some(n)` → chunk evaluation into pieces of ~`n` rows ([Docs.rs][2])

Why you care (practically):

* Remote services often have **rate limits** and **payload size limits**.
* Chunking gives you backpressure knobs: smaller chunks reduce “blast radius” for a slow endpoint, but increase overhead.

Real-world gotcha:

* Chunking means the final chunk may be smaller than `ideal_batch_size` (remainder). There was a bug around this in `AsyncFuncExpr` when `ideal_batch_size` didn’t divide the row count evenly, highlighting that **variable chunk sizes are expected** and must be tolerated. ([GitHub][7])

---

## B1.5 Current scope constraints (where async UDFs can appear)

The official example states (at least in that snapshot) async UDFs can be used in:

* **SELECT list**
* **filter conditions** ([Apache Git Repositories][5])

(Engine support is evolving; check current DataFusion issues/notes if you need async UDFs inside aggregates, joins, etc.)

---

# B2) Runtime + correctness considerations

## B2.1 Where async awaits “live” (execution scheduling)

### DataFusion runs query execution on a Tokio runtime

DataFusion uses Tokio as its execution thread pool and relies on **cooperative scheduling** (tasks must yield). ([Docs.rs][8])

### Async UDFs run on the *same* runtime as the query

The official async UDF example is explicit:

* `invoke_async_with_args` runs on the **same Tokio Runtime processing the query**
* You may want to run actual network I/O on a **different runtime**, and the docs point to the `thread_pools` example as the pattern. ([Apache Git Repositories][5])

**Implication for agents implementing async UDFs:**

* **Never** do blocking network I/O (or long CPU loops) inside `invoke_async_with_args` on the query runtime threads.
* Prefer async clients (`reqwest`, tonic gRPC, etc.).
* If you must call a blocking library, isolate it (e.g., `spawn_blocking`) and be deliberate about cancellation / resource cleanup.

---

## B2.2 Cooperative scheduling: how you accidentally make queries “feel hung”

Tokio cannot preempt a task mid-poll; it can only stop/abort it when it yields. The DataFusion cancellation deep dive explains:

* In cooperative scheduling, tasks must voluntarily yield control.
* `JoinHandle::abort()` only takes effect once the task yields.
* If a task never yields, it can’t be aborted promptly. ([Apache DataFusion][9])

**For async UDFs this means:**

* If your async UDF implementation performs heavy CPU work without `.await` points (or uses blocking calls), it can prevent timely cancellation and degrade latency for other work.

---

## B2.3 Failure semantics (what errors surface where)

### What you return

`invoke_async_with_args` returns `Result<ColumnarValue, DataFusionError>` (as a Future output). ([Docs.rs][2])

### How failures propagate

At runtime, an error returned by your async UDF propagates as a query execution failure (i.e., it bubbles through the stream/pipeline). This matches the general DataFusion execution model: query execution is a polled async stream where errors are returned from `poll_next` / `next()`. ([Apache DataFusion][9])

**Agent implementation guidelines (error hygiene):**

* Use `DataFusionError::Execution(...)` (or helpers) for runtime failures (HTTP 500, timeout, parse errors).
* Preserve context: include function name, endpoint, HTTP status, retry attempts (but do not log secrets).
* Decide on a null/error policy explicitly:

  * “Hard fail query on any row error” (simple, default)
  * “Return NULL for failed rows” (requires per-row error handling and careful semantics)

---

## B2.4 Cancellation semantics (the part that bites production systems)

DataFusion cancellation model (conceptually):

* The query is an **async stream**; cancellation often happens by **dropping** the stream / stopping polling it. ([Apache DataFusion][9])
* Tokio abort semantics are cooperative: “abort” means “don’t poll again after the task yields.” ([Apache DataFusion][9])

**Practical consequences for async UDF authors:**

1. **If the query is cancelled, your UDF future may be dropped**

   * If you spawned detached tasks (fire-and-forget), those may continue running after the query is gone → resource leaks / unintended side effects.

2. **Network requests need explicit timeouts**

   * Even “async” I/O can hang indefinitely without timeouts.
   * Use timeouts at the client level and/or wrap futures with timeouts.

3. **Don’t starve the runtime**

   * DataFusion warns that mixing CPU-intensive work and latency-sensitive network I/O on the same runtime can inflate tail latency; it recommends separate runtimes for plans under load (see `thread_pools`). ([Docs.rs][8])

---

## B2.5 Concurrency and backpressure patterns for remote async UDFs (LLM/RPC)

Async UDFs + columnar execution introduces a specific set of “agent gotchas”:

### Pattern A: “Batch RPC” first, per-row RPC second

* If your remote supports batch endpoints, map an input chunk of `n` rows → one request → `n` outputs.
* Set `ideal_batch_size` to match remote constraints (payload size, max batch). ([Docs.rs][2])

### Pattern B: bounded fanout inside a chunk

If you must do per-row requests, limit in-flight concurrency (semaphore) to avoid hammering the remote and to preserve query fairness.

### Pattern C: scalar fast paths still matter

Even async UDFs get `ColumnarValue` inputs, so you can still:

* special-case constant scalar arguments (e.g., constant prompt prefix, constant model name) for less parsing/alloc per row.
  The official example even calls this out as a performance improvement idea. ([Apache Git Repositories][5])

---

## B2.6 “Explain plan literacy” for async UDF debugging

When debugging “why is my async UDF slow / where is it executed?” you want to inspect `EXPLAIN`:

* Look for `AsyncFuncExec`
* Notice it materializes an internal column (e.g. `__async_fn_0`) and downstream nodes reference that column in filters/projections. ([Apache Git Repositories][5])

This tells you:

* whether the planner actually recognized it as async
* whether the async computation is happening before/after repartition
* how many partitions are executing concurrently (impacts concurrency vs remote limits)

---

## B2.7 Agent checklist (what to validate in PR review)

1. **Implements both traits correctly** (`ScalarUDFImpl` + `AsyncScalarUDFImpl`) and sync `invoke_with_args` is unreachable. ([Apache DataFusion][6])
2. **Registration uses** `AsyncScalarUDF::new(...).into_scalar_udf()` and `ctx.register_udf(...)`. ([Apache DataFusion][6])
3. **Respects row-count invariants**: output `ColumnarValue` matches `args.number_rows` for each invocation (including remainder chunks under `ideal_batch_size`). ([Docs.rs][2])
4. **No blocking calls** on the query runtime; consider separate runtimes / thread pools for real network I/O under load. ([Apache Git Repositories][5])
5. **Cancellation-safe design**: no detached work; timeouts and bounded concurrency. ([Apache DataFusion][9])
6. **Config-driven** via `args.config_options` (don’t bake secrets into code). ([Docs.rs][4])

---

If you want the next “pyarrow-advanced.md style” increment for this section, I can generate:

* a **full “template module”** (one file) containing an async UDF + unit tests + an EXPLAIN snapshot assertion (showing `AsyncFuncExec`),
* and a **production-grade remote-call pattern** (timeouts, retry policy hooks, concurrency limits, structured cancellation) that uses `args.config_options` as the configuration plumbing.

[1]: https://datafusion.apache.org/blog/2025/07/28/datafusion-49.0.0/ "Apache DataFusion 49.0.0 Released - Apache DataFusion Blog"
[2]: https://docs.rs/datafusion/latest/datafusion/logical_expr/async_udf/trait.AsyncScalarUDFImpl.html "AsyncScalarUDFImpl in datafusion::logical_expr::async_udf - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/logical_expr/async_udf/struct.AsyncScalarUDF.html "AsyncScalarUDF in datafusion::logical_expr::async_udf - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.ScalarFunctionArgs.html?utm_source=chatgpt.com "ScalarFunctionArgs in datafusion::logical_expr - Rust"
[5]: https://apache.googlesource.com/datafusion-sandbox/%2B/refs/heads/main/datafusion-examples/examples/udf/async_udf.rs "datafusion-examples/examples/udf/async_udf.rs - datafusion-sandbox - Git at Google"
[6]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html "Adding User Defined Functions: Scalar/Window/Aggregate/Table Functions — Apache DataFusion  documentation"
[7]: https://github.com/apache/datafusion/issues/18822 "AsyncScalarUDFs break when batch_size doesn't divide number of rows · Issue #18822 · apache/datafusion · GitHub"
[8]: https://docs.rs/datafusion/latest/datafusion/ "datafusion - Rust"
[9]: https://datafusion.apache.org/blog/2025/06/30/cancellation/ "Using Rust async for Query Execution and Cancelling Long-Running Queries - Apache DataFusion Blog"

# Section C — Named arguments in DataFusion (cross-cutting)

Named arguments in DataFusion are a **planner-time feature**: the SQL parser captures `name => expr`, and the planner **validates + reorders** arguments into canonical positional order *before* invoking your Scalar/Window/Aggregate function implementation. ([Apache DataFusion][1])

---

## C1) Call-site semantics

### C1.1 Where named args are supported (what the user can write)

DataFusion explicitly supports named arguments for:

* **Scalar functions** (e.g., `substr(str => 'hello', start_pos => 2, length => 3)`)
* **Window functions** (e.g., `lead(expr => value, offset => 1) OVER (...)`)
* **Aggregate functions** (e.g., `corr(y => col1, x => col2)`) ([Apache DataFusion][1])

> Practical implication: this is not “only for UDFs”; it applies to any function whose signature exposes parameter names (including many built-ins). ([Docs.rs][2])

---

### C1.2 Mixing positional + named args: ordering rules

**Rule:** positional arguments must come first; named arguments may follow in any order. ([Apache DataFusion][1])

Valid example:

```sql
SELECT substr('hello', start_pos => 2, length => 3);
```

(Shown as valid in the official docs.) ([Apache DataFusion][1])

Invalid pattern (enforced by the planner): **positional after named**. The underlying validator produces a planning error along the lines of “positional argument … follows named argument; all positional arguments must come before named arguments.” ([Docs.rs][3])

---

### C1.3 The actual binding algorithm (what DataFusion does internally)

At planning time DataFusion calls:

```rust
pub fn resolve_function_arguments(
    param_names: &[String],
    args: Vec<Expr>,
    arg_names: Vec<Option<ArgumentName>>,
) -> Result<Vec<Expr>, DataFusionError>
```

Its job is to **validate and reorder** arguments so they align with the function’s declared parameter order. ([Docs.rs][4])

**Core rules (canonical, not folklore):**

* All positional args come before named args
* Named args can be in any order after positionals
* Parameter-name matching follows SQL identifier case rules (unquoted case-insensitive; quoted case-sensitive)
* No duplicate parameter names ([Docs.rs][4])

**Concrete example from the docs.rs contract:**

Given parameters `["a","b","c"]` and a call `func(10, c => 30, b => 20)`, DataFusion returns the reordered list `[10, 20, 30]` (positional `a=10`, named `b=20`, named `c=30`). ([Docs.rs][4])

---

### C1.4 Case-sensitivity and quoting (SQL identifier rules applied to argument names)

DataFusion preserves whether the caller **quoted** the argument name via:

```rust
pub struct ArgumentName {
  pub value: String,
  pub is_quoted: bool,
}
```

* `is_quoted = false` → match **case-insensitively**
* `is_quoted = true` → match **case-sensitively** ([Docs.rs][5])

The resolver implements this explicitly using exact match for quoted, and ASCII case-insensitive match for unquoted names. ([Docs.rs][3])

**Implication for API authors:** if you publish parameter names, treat them like SQL identifiers. Lowercase is the safest default, and quoting is a tool for callers who need exact case semantics.

---

### C1.5 Error modes you should expect (and what the user sees)

From `resolve_function_arguments` (planner-time), the user can hit:

1. **Positional after named**

* Trigger: `func(a => 1, 2)`
* Error: planning error indicating positional appears after named. ([Docs.rs][3])

2. **Unknown parameter name**

* Trigger: `func(nope => 1)` when `nope` isn’t in `param_names`
* Error includes the valid parameter list: “Unknown parameter name … Valid parameters are: […]” ([Docs.rs][3])

3. **Duplicate parameter**

* Trigger: `func(a => 1, a => 2)` (or mixing that assigns the same parameter twice)
* Error: “Parameter … specified multiple times” ([Docs.rs][3])

4. **Missing required parameter (when caller provides `N` args but leaves a hole in the first `N`)**
   The resolver’s logic *requires* parameters up to the number of arguments supplied (it explicitly checks the first `required_count = args_len`). If you skip an earlier parameter while specifying a later one, you’ll get “Missing required parameter …”. ([Docs.rs][3])

5. **Too many positional arguments**
   If the caller supplies more positional args than there are parameters, you get “Too many positional arguments …”. ([Docs.rs][3])

> Why this matters: as a UDF author, you generally **won’t see these** at runtime—your function simply receives a positional argument vector that already passed these checks.

---

### C1.6 “Optional trailing parameters” behavior (important nuance)

The resolver contains explicit logic to **only require** parameters up to the number of arguments provided, and to return only the assigned prefix, “handling optional trailing parameters.” ([Docs.rs][3])

Interpretation for implementers:

* Named arguments make it ergonomic to call functions with trailing optional params **without** forcing callers to remember the positional order.
* However, DataFusion does **not** magically supply defaults; if you want defaults, your function must (a) accept the shorter arity in its signature(s) and (b) fill defaults internally.

---

## C2) Authoring support

### C2.1 The API surface you use

#### `Signature.parameter_names`

`Signature` has an optional `parameter_names: Option<Vec<String>>`. If it’s set, DataFusion enables named argument notation `func(a => 1, b => 2)`. ([Docs.rs][2])

#### `Signature::with_parameter_names(...) -> Result<Signature>`

This is the canonical way to attach names:

```rust
let sig =
  Signature::exact(vec![DataType::Int32, DataType::Utf8], Volatility::Immutable)
    .with_parameter_names(vec!["count", "name"])?;
```

The method:

* validates arity compatibility
* stores the names on the signature ([Docs.rs][2])

---

### C2.2 Validation rules for `.with_parameter_names(...)` (what fails fast)

From the actual implementation:

1. **Arity must match (fixed arity signatures)**
   If your signature has fixed arity `k`, the names vector must have length `k`. ([Docs.rs][6])

2. **Variable arity signatures generally cannot specify names**
   For variable arity (`Variadic`, `VariadicAny`, etc.), it errors — **except** for `TypeSignature::UserDefined`, where names are allowed but the implementer is responsible for ensuring they match usage. ([Docs.rs][6])

3. **No duplicate names**
   Duplicate parameter names are rejected at signature construction time. ([Docs.rs][6])

This “fail fast” validation is a big deal for UDF quality: many named-argument bugs can be eliminated just by constructing the signature.

---

### C2.3 What your UDF actually receives at runtime

Once you’ve attached parameter names to the signature:

* callers can use named args in any order
* DataFusion resolves them to positional order **before invoking your UDF**
* your implementation reads arguments exactly as if they were positional-only ([Apache DataFusion][1])

This is why `with_parameter_names` is “cross-cutting”: it upgrades call-site ergonomics without changing your runtime execution contract.

---

## C3) How parameter names appear in error messages and “function resolution output”

### C3.1 Candidate function signatures include parameter names

When a call fails due to mismatch (arity/types), DataFusion displays candidate functions including parameter names. Example from docs:

* It shows `substr(str: Any, start_pos: Any)` and `substr(str: Any, start_pos: Any, length: Any)` rather than anonymous positional slots. ([Apache DataFusion][1])

This is the main “UX payoff” for naming parameters: it makes planner errors actionable, especially when you support multiple arities.

### C3.2 Argument-binding errors name the relevant parameters

When the failure is about named arguments specifically, the resolver emits errors that mention:

* the **unknown name** and the list of valid names
* the **duplicated name**
* the **missing required parameter name** ([Docs.rs][3])

As an API author, this is how you “document by enforcement”: your parameter names become the error contract.

---

## LLM-agent implementation checklist (what to enforce in PRs)

1. **If you want named args, set `Signature.parameter_names` via `.with_parameter_names`** (don’t hand-roll). ([Docs.rs][2])
2. **Names must match fixed arity** (and be unique). ([Docs.rs][6])
3. **Avoid variable-arity signatures with names** unless you intentionally use `TypeSignature::UserDefined` and add your own validation. ([Docs.rs][6])
4. **Assume callers may mix positional+named**; positional must precede named. ([Docs.rs][4])
5. **Assume case-insensitive matching for unquoted argument names; quoted names are case-sensitive.** ([Docs.rs][4])
6. **Do not implement runtime “name handling” inside the UDF**—DataFusion already reorders and validates at planning time. ([Apache DataFusion][1])

---

If you want to continue in the same style, the next strong “agent-facing” section is **D) Aggregate UDFs (UDAFs)**—especially the state/merge contracts (`Accumulator`, `GroupsAccumulator`) and window-aggregate `retract_batch` semantics.

[1]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html "Adding User Defined Functions: Scalar/Window/Aggregate/Table Functions — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Signature.html "Signature in datafusion::logical_expr - Rust"
[3]: https://docs.rs/datafusion-expr/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_expr/arguments.rs.html "arguments.rs - source"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/arguments/fn.resolve_function_arguments.html "resolve_function_arguments in datafusion::logical_expr::arguments - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/arguments/struct.ArgumentName.html "ArgumentName in datafusion::logical_expr::arguments - Rust"
[6]: https://docs.rs/datafusion-expr-common/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_expr_common/signature.rs.html "signature.rs - source"

# Section D — Aggregate UDFs (UDAFs) in DataFusion (Rust)

## D0) Object model (what exists, what you implement, what DataFusion calls)

**Planning / registration layer**

* `AggregateUDF` (logical handle): name + signature/typing + factories/hints; can be invoked in **GROUP BY** or **OVER** (window aggregate). ([Docs.rs][1])
* Register into engine scope:

  * `SessionContext::register_udaf(f: AggregateUDF)` (SQL lookup is **lowercased unless quoted**). ([Docs.rs][2])
  * Underlying registry symmetry via `FunctionRegistry::register_udaf(...)` exists at the engine layer. ([Docs.rs][3])

**Implementation layer (you write)**

* Minimal: `Accumulator` (single-group state machine). ([Docs.rs][4])
* High-perf optional: `GroupsAccumulator` (multi-group state machine). ([Docs.rs][5])
* Full UDAF API: `AggregateUDFImpl` (ties signatures + state schemas + accumulator factories + optimizer hooks). ([Docs.rs][6])

---

## D1) Registration + invocation “commands” (agent-facing)

### D1.1 `create_udaf` (simple helper)

Signature (DataFusion 52.x):

```rust
pub fn create_udaf(
  name: &str,
  input_type: Vec<DataType>,
  return_type: Arc<DataType>,
  volatility: Volatility,
  accumulator: Arc<dyn Fn(AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>, DataFusionError> + Send + Sync>,
  state_type: Arc<Vec<DataType>>,
) -> AggregateUDF
```

Constraints: declared **signature + `state_type` must match your `Accumulator`’s `state()/merge_batch()` contract**. ([Docs.rs][7])

### D1.2 Full-power: implement `AggregateUDFImpl` and wrap into `AggregateUDF`

* Construct: `let udaf = AggregateUDF::from(MyUdaf::new())` (or `new_from_impl`). ([Docs.rs][1])
* Register: `ctx.register_udaf(udaf);` ([Docs.rs][2])

### D1.3 Call from DataFrame API without registry: `AggregateUDF::call`

* `AggregateUDF::call(args: Vec<Expr>) -> Expr` yields an aggregate expression node usable in `DataFrame::aggregate` pipelines. ([Docs.rs][1])

---

## D2) `AggregateUDFImpl` — full API inventory (what you can override, why it matters)

### D2.1 Required methods (minimum viable “advanced” UDAF)

* `fn as_any(&self) -> &dyn Any`
* `fn name(&self) -> &str`
* `fn signature(&self) -> &Signature`
* `fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError>`
* `fn accumulator(&self, acc_args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>, DataFusionError>` ([Docs.rs][6])

### D2.2 Provided methods (the “real” feature surface)

Key ones for **state correctness**, **window behavior**, and **throughput**:

**Typing + schema**

* `return_field(arg_fields) -> Arc<Field>`: advanced output typing (nullability, value-dependent type, metadata-dependent type). ([Docs.rs][6])
* `is_nullable() -> bool`: declare whether result can be null; if `false`, ensure `default_value` is non-null. ([Docs.rs][6])
* `state_fields(args: StateFieldsArgs) -> Vec<FieldRef>`: schema of intermediate state; default = 1 field named `name` with “value_type” (good for SUM/MIN), override for multi-component state (AVG-like). ([Docs.rs][6])

**Multi-group acceleration**

* `groups_accumulator_supported(args) -> bool`
* `create_groups_accumulator(args) -> Box<dyn GroupsAccumulator>` ([Docs.rs][6])
  Note: even if supported, DataFusion may still use row-oriented `accumulator()` for **window aggregates** or **no GROUP BY columns**. ([Docs.rs][6])

**Window sliding semantics**

* `create_sliding_accumulator(args) -> Box<dyn Accumulator>`: alternate accumulator for window usage with retraction; “has retract method to revert the previous update” and ties to `Accumulator::retract_batch`. ([Docs.rs][6])

**SQL surface enablement**

* `supports_null_handling_clause() -> bool`: enable `[IGNORE NULLS | RESPECT NULLS]`; if `true`, you must honor `AccumulatorArgs.ignore_nulls`. ([Docs.rs][6])
* `supports_within_group_clause() -> bool`: enable ordered-set `WITHIN GROUP (ORDER BY ...)`; **DataFusion does not insert a sort**—ordered-set aggregates must buffer/sort internally. ([Docs.rs][6])

**Optimization hooks**

* `simplify() -> Option<Fn(AggregateFunction, &SimplifyInfo) -> Expr>`: per-UDAF rewrite; must preserve **type + nullability**; constant folding handled by DataFusion. ([Docs.rs][6])
* `coerce_types(arg_types) -> Vec<DataType>`: only called when `signature().type_signature == UserDefined`; planner inserts CASTs. ([Docs.rs][6])
* `value_from_stats(stats_args) -> Option<ScalarValue>`: statistics-derived aggregate shortcut (e.g., MIN from column stats). ([Docs.rs][6])
* `default_value(data_type) -> ScalarValue`: result when all input rows are null (COUNT→0; many others → NULL). ([Docs.rs][6])

**Naming / UX (optional but production-grade)**

* `schema_name`, `human_display`, `display_name`, and window variants: control output naming / explain formatting. ([Docs.rs][6])
* `documentation() -> Option<&Documentation>`: programmatic + generated docs. ([Docs.rs][6])

### D2.3 Trait-identity requirement change (version footgun)

Modern DataFusion requires types implementing `AggregateUDFImpl` to implement `PartialEq`, `Eq`, `Hash` (replacing older `equals/hash_value` methods). ([Apache DataFusion][8])
**Agent rule:** always `#[derive(Debug, Clone, PartialEq, Eq, Hash)]` on your UDF descriptor unless you intentionally embed non-hashable state.

---

## D3) `Accumulator` — single-group state/merge contract (the core correctness surface)

### D3.1 Required methods

````rust
trait Accumulator: Send + Sync + Debug {
  fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError>;
  fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError>;
  fn size(&self) -> usize;
  fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError>;
  fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError>;

  // provided:
  fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError>;
  fn supports_retract_batch(&self) -> bool;
}
``` :contentReference[oaicite:26]{index=26}

### D3.2 Semantics (what DataFusion assumes; violate = wrong answers / plan failures)
- `update_batch(values)` updates *this group’s* state using argument arrays. :contentReference[oaicite:27]{index=27}
- `evaluate()` returns final value **consuming internal state**; **must not be called twice** (nondeterminism risk). :contentReference[oaicite:28]{index=28}
- `state()` serializes intermediate state for **multi-phase aggregation**; DataFusion serializes state into arrays and later calls `merge_batch` on the final phase. :contentReference[oaicite:29]{index=29}
- `merge_batch(states)` merges arrays formed by concatenating `state()` outputs from other accumulators; for many aggregates merge≠update (COUNT partials must be summed, etc.). :contentReference[oaicite:30]{index=30}
- `size()` reports state memory; used for budgeting; should track internal allocations, not input batch sizes. :contentReference[oaicite:31]{index=31}

### D3.3 `AccumulatorArgs` (what you learn when building an accumulator)
Factory signature receives `AccumulatorArgs<'_>` with:
- `return_field: Arc<Field>` (output field)
- `schema: &Schema` (input schema; prefer `expr_fields` for per-arg metadata)
- `ignore_nulls: bool` (from SQL `IGNORE NULLS`)
- `order_bys: &[PhysicalSortExpr]` (aggregate-level `ORDER BY` / within-group info)
- `is_reversed: bool` (execution in reverse order)
- `name: &str` (aggregate expression name; useful for unique state field names)
- `is_distinct: bool`
- `exprs: &[Arc<dyn PhysicalExpr>]` and `expr_fields: &[Arc<Field>]` (arg exprs + corresponding fields). :contentReference[oaicite:32]{index=32}

---

## D4) Window aggregates + `retract_batch` (sliding window semantics)

### D4.1 The contract
- `retract_batch(values)` is the **inverse** of `update_batch(values)`; used to incrementally compute bounded window aggregates. :contentReference[oaicite:33]{index=33}
- If `supports_retract_batch() == true`, DataFusion will call `retract_batch` for sliding windows (example given: `OVER (ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)`). :contentReference[oaicite:34]{index=34}
- DataFusion’s documented call order when sliding the window:
  1) `evaluate()` for current window  
  2) `retract_batch(leaving_values)`  
  3) `update_batch(entering_values)` :contentReference[oaicite:35]{index=35}

**Agent rule:** if your UDAF is plausibly used in window contexts and you do **not** implement retraction, explicitly make that visible (default `retract_batch` errors; default `supports_retract_batch` false), otherwise you risk runtime failures when users apply bounded frames.

### D4.2 `AggregateUDFImpl::create_sliding_accumulator`
- Use when you want different state/algorithm for:
  - GROUP BY (multi-phase merge-friendly)
  - Window sliding (retract-friendly)  
  DataFusion exposes a dedicated factory for this: `create_sliding_accumulator(args)` and documents it as “alternative accumulator … used for window functions … has retract method”. :contentReference[oaicite:36]{index=36}

**Pattern taxonomy**
- **Invertible aggregations**: SUM/COUNT/AVG-like (track sum+count; retract subtract + decrement).
- **Non-invertible**: MEDIAN/PERCENTILE-like (need order-statistics structure; retract requires multiset deletion or full recomputation).

---

## D5) `GroupsAccumulator` — multi-group state machine (high-cardinality throughput)

### D5.1 Why it exists (value case)
DataFusion’s high-cardinality performance work moved from “one accumulator per group” to “one accumulator managing all group states” to reduce allocations + improve vectorization. :contentReference[oaicite:37]{index=37}  
Docs describe `GroupsAccumulator` as storing state mapping `group_index -> state`, typically in contiguous `Vec` storage. :contentReference[oaicite:38]{index=38}

### D5.2 Required methods + signatures
```rust
trait GroupsAccumulator: Send {
  fn update_batch(
    &mut self,
    values: &[ArrayRef],
    group_indices: &[usize],
    opt_filter: Option<&BooleanArray>,
    total_num_groups: usize
  ) -> Result<(), DataFusionError>;

  fn merge_batch(
    &mut self,
    values: &[ArrayRef],          // prior state() output arrays
    group_indices: &[usize],
    opt_filter: Option<&BooleanArray>,
    total_num_groups: usize
  ) -> Result<(), DataFusionError>;

  fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef, DataFusionError>;
  fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>, DataFusionError>;
  fn size(&self) -> usize;

  // provided:
  fn convert_to_state(&self, values: &[ArrayRef], opt_filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>, DataFusionError>;
  fn supports_convert_to_state(&self) -> bool;
}
``` :contentReference[oaicite:39]{index=39}

### D5.3 Group index invariants
- `group_index` values are **contiguous** (0..total_num_groups-1, no gaps); implementation is expected to use `Vec<...>` for per-group state. :contentReference[oaicite:40]{index=40}
- `total_num_groups` is the authoritative “current group cardinality”; you must grow internal vectors to at least this length. :contentReference[oaicite:41]{index=41}

### D5.4 Filtering semantics (`opt_filter`)
- `opt_filter[i] == true` ⇒ apply row `i` to its group; otherwise ignore row `i` (both for `update_batch` and `merge_batch`). :contentReference[oaicite:42]{index=42}  
This is the hook used by per-aggregate SQL `FILTER (WHERE ...)` pipelines to avoid pre-materializing filtered arrays at call-sites.

### D5.5 Output ordering + emission semantics (`EmitTo`)
- `evaluate(emit_to)` returns one array whose rows are **in `group_index` order**; groups with no values must output null. :contentReference[oaicite:43]{index=43}
- `EmitTo` variants (DataFusion 52.x):
  - `EmitTo::All` — emit all groups.
  - `EmitTo::First(n)` — emit groups `[0..n)` and **shift remaining group indexes down by n**. :contentReference[oaicite:44]{index=44}
- Practical implication: if you support incremental emission, your internal per-group `Vec` state must implement “drop prefix n, shift indices”. `EmitTo` also provides `take_needed(&mut Vec<T>) -> Vec<T>` to help remove emitted prefix efficiently. :contentReference[oaicite:45]{index=45}

### D5.6 `size()` complexity constraint
`GroupsAccumulator::size()` is called once per batch; docs require it be `O(n)` to compute, **not `O(num_groups)`**. :contentReference[oaicite:46]{index=46}  
(Interpretation: track memory usage incrementally rather than summing whole vectors each time.)

### D5.7 `convert_to_state`: multi-phase shortcut
- `convert_to_state(values, opt_filter)` converts *raw inputs* directly into *intermediate state arrays* “as if each input row were its own group”. :contentReference[oaicite:47]{index=47}
- Invoked when DataFusion decides partial pre-aggregation isn’t reducing cardinality enough; it bypasses expensive per-group accumulation and forwards state to later phases. :contentReference[oaicite:48]{index=48}
- Examples given in docs:
  - COUNT ⇒ array of 1s
  - SUM/MIN/MAX ⇒ values themselves (subject to filter). :contentReference[oaicite:49]{index=49}

### D5.8 Null tracking helpers
Docs explicitly call out `NullState` as a helper for groups that have not seen any values to produce correct output nulls. :contentReference[oaicite:50]{index=50}

---

## D6) Engine selection logic (when each path is used)

### D6.1 Row accumulator (`Accumulator`) is always required
Even with `GroupsAccumulator`, DataFusion still needs a single-group accumulator for:
- window aggregate usage
- aggregations with no GROUP BY columns (global aggregates). :contentReference[oaicite:51]{index=51}

### D6.2 `GroupsAccumulator` is optional and conditional
- Advertise support: `groups_accumulator_supported(args) -> true`
- Then DataFusion may call `create_groups_accumulator(args)` for group-by execution. :contentReference[oaicite:52]{index=52}

### D6.3 Sliding window selection
- For bounded windows where retraction is beneficial/required, DataFusion can request a “sliding” accumulator via `create_sliding_accumulator(args)`. :contentReference[oaicite:53]{index=53}

---

## D7) End-to-end implementations (dense, agent-ready)

### Example 1 — Minimal UDAF via `create_udaf`: `sum_sq(x) -> f64` (supports sliding via `retract_batch`)
```rust
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{cast::as_float64_array, Result, DataFusionError};
use datafusion::logical_expr::{create_udaf, Accumulator, AccumulatorArgs, AggregateUDF, Volatility};
use datafusion::scalar::ScalarValue;

#[derive(Debug)]
struct SumSq {
    sum: f64,
}
impl SumSq {
    fn new() -> Self { Self { sum: 0.0 } }
}

impl Accumulator for SumSq {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() { return Ok(()); }
        let a = as_float64_array(&values[0])?;
        for v in a.iter().flatten() {
            self.sum += v * v;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(Some(self.sum)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64(Some(self.sum))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() { return Ok(()); }
        let s = as_float64_array(&states[0])?;
        for v in s.iter().flatten() {
            self.sum += v;
        }
        Ok(())
    }

    // sliding-window support
    fn supports_retract_batch(&self) -> bool { true }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() { return Ok(()); }
        let a = as_float64_array(&values[0])?;
        for v in a.iter().flatten() {
            self.sum -= v * v;
        }
        Ok(())
    }
}

pub fn sum_sq_udaf() -> AggregateUDF {
    create_udaf(
        "sum_sq",
        vec![DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_args: AccumulatorArgs<'_>| Ok(Box::new(SumSq::new()))),
        Arc::new(vec![DataType::Float64]), // state_type must match state()/merge_batch()
    )
}
````

Mechanics this example exercises:

* `create_udaf` contract + state schema alignment. ([Docs.rs][7])
* Sliding window retraction is implemented via `retract_batch` + `supports_retract_batch`. ([Docs.rs][4])

Register + SQL lookup rules:

```rust
use datafusion::prelude::SessionContext;

let ctx = SessionContext::new();
ctx.register_udaf(sum_sq_udaf()); // lookup lowercased unless quoted
```

([Docs.rs][2])

---

### Example 2 — Full `AggregateUDFImpl`: adds GroupsAccumulator + sliding accumulator factory

Skeleton illustrating where each feature is wired (omit boilerplate; focus on surfaces):

```rust
use std::{any::Any, sync::Arc};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, DataFusionError};
use datafusion::logical_expr::{
    Accumulator, AccumulatorArgs, AggregateUDF, AggregateUDFImpl,
    GroupsAccumulator, Signature, Volatility,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SumSqUdaf {
    sig: Signature,
}
impl SumSqUdaf {
    fn new() -> Self {
        Self { sig: Signature::exact(vec![DataType::Float64], Volatility::Immutable) }
    }
}

impl AggregateUDFImpl for SumSqUdaf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "sum_sq" }
    fn signature(&self) -> &Signature { &self.sig }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>, DataFusionError> {
        Ok(Box::new(super::SumSq::new()))
    }

    // enable groups path
    fn groups_accumulator_supported(&self, _args: AccumulatorArgs<'_>) -> bool { true }
    fn create_groups_accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>, DataFusionError> {
        Ok(Box::new(super::SumSqGroups::new()))
    }

    // enable window sliding path (can return a different accumulator than group-by)
    fn create_sliding_accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>, DataFusionError> {
        Ok(Box::new(super::SumSq::new())) // must support retract_batch
    }
}

pub fn sum_sq_udaf_full() -> AggregateUDF {
    AggregateUDF::from(SumSqUdaf::new())
}
```

Feature wiring demonstrated:

* `AggregateUDFImpl` required methods. ([Docs.rs][6])
* `create_groups_accumulator` / `groups_accumulator_supported`. ([Docs.rs][6])
* `create_sliding_accumulator` ties to `retract_batch`. ([Docs.rs][6])
* Trait identity requirements (`PartialEq/Eq/Hash`). ([Apache DataFusion][8])

---

### Example 3 — Minimal `GroupsAccumulator` for `sum_sq` (handles opt_filter + EmitTo shifting)

```rust
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array};
use datafusion::common::{cast::as_float64_array, Result, DataFusionError};
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};

#[derive(Debug)]
pub struct SumSqGroups {
    sums: Vec<f64>,
    seen: Vec<bool>, // null-state tracking: groups with no values => null output
}
impl SumSqGroups {
    pub fn new() -> Self { Self { sums: vec![], seen: vec![] } }

    fn ensure_groups(&mut self, total: usize) {
        if self.sums.len() < total {
            self.sums.resize(total, 0.0);
            self.seen.resize(total, false);
        }
    }

    fn apply_rows(&mut self, a: &Float64Array, group_indices: &[usize], opt_filter: Option<&BooleanArray>, sign: f64) {
        for i in 0..a.len() {
            if let Some(f) = opt_filter {
                if !f.value(i) { continue; }
            }
            let g = group_indices[i];
            if let Some(v) = a.value_opt(i) {
                self.sums[g] += sign * (v * v);
                self.seen[g] = true;
            }
        }
    }

    fn emit_prefix(&mut self, emit_to: EmitTo) -> (Vec<f64>, Vec<bool>) {
        match emit_to {
            EmitTo::All => (std::mem::take(&mut self.sums), std::mem::take(&mut self.seen)),
            EmitTo::First(n) => {
                let out_sums = self.sums.drain(0..n.min(self.sums.len())).collect();
                let out_seen = self.seen.drain(0..n.min(self.seen.len())).collect();
                (out_sums, out_seen)
            }
        }
    }
}

impl GroupsAccumulator for SumSqGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<(), DataFusionError> {
        self.ensure_groups(total_num_groups);
        let a = as_float64_array(&values[0])?;
        self.apply_rows(a, group_indices, opt_filter, 1.0);
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<(), DataFusionError> {
        // values are prior state() outputs; for this accumulator state is just sums
        self.ensure_groups(total_num_groups);
        let s = as_float64_array(&values[0])?;
        for i in 0..s.len() {
            if let Some(f) = opt_filter { if !f.value(i) { continue; } }
            let g = group_indices[i];
            if let Some(v) = s.value_opt(i) {
                self.sums[g] += v;
                self.seen[g] = true;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef, DataFusionError> {
        let (sums, seen) = self.emit_prefix(emit_to);
        let out = sums.into_iter()
            .zip(seen.into_iter())
            .map(|(v, ok)| if ok { Some(v) } else { None })
            .collect::<Float64Array>();
        Ok(Arc::new(out) as ArrayRef)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>, DataFusionError> {
        let (sums, seen) = self.emit_prefix(emit_to);
        let out = sums.into_iter()
            .zip(seen.into_iter())
            .map(|(v, ok)| if ok { Some(v) } else { None })
            .collect::<Float64Array>();
        Ok(vec![Arc::new(out) as ArrayRef])
    }

    fn size(&self) -> usize {
        // must be cheap per-batch; approximate ok
        (self.sums.capacity() * std::mem::size_of::<f64>())
        + (self.seen.capacity() * std::mem::size_of::<bool>())
    }
}
```

Semantics this example hits:

* `GroupsAccumulator` methods + `group_indices/opt_filter/total_num_groups`. ([Docs.rs][5])
* Output must be in `group_index` order; missing groups => null. ([Docs.rs][5])
* `EmitTo::First(n)` requires shifting indices by draining prefix. ([Docs.rs][9])

---

## D8) High-density agent checklist (what to verify before merging a UDAF PR)

**Surface + wiring**

* [ ] Registration: `SessionContext::register_udaf(AggregateUDF)`; confirm naming/quoting semantics if mixed-case SQL is required. ([Docs.rs][2])
* [ ] Descriptor type derives `PartialEq, Eq, Hash` (required by modern DataFusion). ([Apache DataFusion][8])
* [ ] If using `create_udaf`, ensure `state_type` matches `Accumulator::state()` schema and `merge_batch` expectations. ([Docs.rs][7])

**Accumulator correctness**

* [ ] `evaluate()` not called twice / state consumed semantics respected. ([Docs.rs][4])
* [ ] `merge_batch(states)` interprets arrays as concatenations of `state()` outputs. ([Docs.rs][4])
* [ ] `size()` tracks state memory (budgeting) and is cheap. ([Docs.rs][4])

**Window / sliding**

* [ ] If bounded window usage is plausible, implement `supports_retract_batch=true` + correct inverse `retract_batch`; validate call order (evaluate → retract leaving → update entering). ([Docs.rs][4])
* [ ] If window strategy differs, implement `AggregateUDFImpl::create_sliding_accumulator`. ([Docs.rs][6])

**GroupsAccumulator performance (optional)**

* [ ] `groups_accumulator_supported=true` and `create_groups_accumulator` implemented; confirm DataFusion still uses row accumulator for window/no-group-by cases (so keep `Accumulator` correct). ([Docs.rs][6])
* [ ] Enforce invariants: contiguous group indices; honor `opt_filter`; correct `EmitTo` shifting; `size()` not O(num_groups). ([Docs.rs][5])
* [ ] Consider `convert_to_state` if state is “identity-ish” (COUNT/SUM/MIN/MAX) to speed multi-phase paths. ([Docs.rs][5])

If you want to keep going at this density, the next high-leverage section is **E) Window UDFs (UDWFs)**, but we can also do a narrower follow-on here: **“UDAF testing harness patterns”** (sqllogictest + plan snapshot + window bounded-frame correctness matrices) tailored to your codegen agents.

[1]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.AggregateUDF.html "AggregateUDF in datafusion_expr - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html?utm_source=chatgpt.com "SessionContext in datafusion::execution::context - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/execution/trait.FunctionRegistry.html?utm_source=chatgpt.com "FunctionRegistry in datafusion::execution - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.Accumulator.html "Accumulator in datafusion::logical_expr - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.GroupsAccumulator.html "GroupsAccumulator in datafusion::logical_expr - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.AggregateUDFImpl.html "AggregateUDFImpl in datafusion::logical_expr - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.create_udaf.html "create_udaf in datafusion::logical_expr - Rust"
[8]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[9]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.EmitTo.html "EmitTo in datafusion::logical_expr - Rust"

# UDAF testing harness patterns (sqllogictest + plan snapshots + bounded-window matrices)

This is a **testing toolkit** for Aggregate UDFs (UDAFs) that covers (1) correctness at the SQL surface, (2) correctness of **multi-phase state / merge**, and (3) correctness of **sliding window / retraction** behavior.

---

## T0) Test stack map (what to use, what it proves)

### T0.1 sqllogictest (SQL surface, lowest friction)

DataFusion’s SQL implementation is heavily tested with **sqllogictest**; it’s explicitly recommended for new function testing because it avoids recompile cycles and supports auto-updating expected output. ([datafusion.apache.org][1])

Run modes / commands:

* Run all: `cargo test --profile=ci --test sqllogictests` ([datafusion.apache.org][1])
* Run a file: `cargo test --profile=ci --test sqllogictests -- aggregate.slt` ([datafusion.apache.org][1])
* Auto-fill/update expected outputs: `cargo test --profile=ci --test sqllogictests -- --complete` ([datafusion.apache.org][1])

DataFusion’s sqllogictest runner also documents the “TLDR” variants without `--profile=ci` and the `--complete` usage in its own README. ([apache.googlesource.com][2])

### T0.2 Rust unit / integration tests (internal invariants)

Use when you need to validate **Accumulator / GroupsAccumulator state math** directly (merge associativity, retract inverse, memory sizing) or test behaviors hard to express in SQL (e.g. optimizer `simplify()` interactions). ([datafusion.apache.org][3])

Useful built-ins for result checking:

* `assert_batches_eq!` / `assert_batches_sorted_eq!` for RecordBatch output equality. ([Docs.rs][4])

### T0.3 Plan snapshots (regression guardrails)

DataFusion uses **Insta** for snapshot testing; it’s the canonical mechanism for “expected output changed” review flows (`cargo insta review`). ([datafusion.apache.org][1])
Plan snapshots are most useful for ensuring:

* Your UDAF appears as expected in **logical/physical plans**
* Window queries compile into expected plan shapes after refactors (even if results remain correct)

### T0.4 Sliding-window correctness matrix (retract path proof)

For UDAFs that implement `retract_batch`, you need tests that specifically validate the **evaluate → retract(leaving) → update(entering)** lifecycle required by bounded window frames. ([Docs.rs][5])

---

## T1) sqllogictest harness (DataFusion-specific mechanics you actually need)

### T1.1 File execution model / isolation

Each `.slt` file runs in its own isolated `SessionContext` and tests are designed to run in parallel; avoid external side effects (e.g. writing to `/tmp`). ([apache.googlesource.com][2])

### T1.2 Minimal authoring workflow (“command surface”)

**Create/edit** `datafusion/sqllogictest/test_files/<topic>.slt`, then:

* Generate expected output: `cargo test --test sqllogictests -- <topic> --complete` ([apache.googlesource.com][2])
* Validate: `cargo test --test sqllogictests -- <topic>` ([apache.googlesource.com][2])

The runner explicitly supports `cargo test` substring filtering over filenames (e.g. `-- information`). ([apache.googlesource.com][2])

### T1.3 Record syntax (what an agent should generate)

DataFusion documents the per-test record format as:

```sql
# <test_name>
query <type_string> <sort_mode>
<sql_query>
----
<expected_result>
```

and notes `<test_name>` is an “arrow-datafusion only” unique identifier. ([apache.googlesource.com][2])

**Type string codes** (1 char per result column):

* `B` boolean, `D` datetime, `I` integer, `P` timestamp, `R` float, `T` text, `?` any other. ([apache.googlesource.com][2])

**Expected result normalization** (important when authoring by hand):

* floats rounded to scale 12, NULL→`NULL`, empty string→`(empty)`, boolean→`true/false` (plus other conversions in runner code). ([apache.googlesource.com][2])

### T1.4 Sorting controls (nondeterminism management)

`slt` supports `sort_mode`:

* `nosort` (default) — only safe with `ORDER BY` or a single row (otherwise row order undefined)
* `rowsort` — client-side row sort on rendered text (note lexical comparisons; `"9"` sorts after `"10"`)
* `valuesort` — like rowsort but doesn’t preserve row groupings (sort individual values). ([apache.googlesource.com][2])

DataFusion explicitly encourages `ORDER BY` or `rowsort` for queries without explicit ordering. ([apache.googlesource.com][2])

### T1.5 “Completion mode” semantics

`--complete` reads a prototype script and writes a full script with results inserted (the preferred way to author expected output). ([apache.googlesource.com][2])

### T1.6 Temporary files: scratchdir contract

Runner creates and clears `test_files/scratch/<filename>` for each test file; tests that write files must write **only** there to avoid interference. ([apache.googlesource.com][2])

### T1.7 Where to put UDAF tests (best practice)

DataFusion contributor HOWTOs: prefer sqllogictest integration tests for new functions; use Rust unit tests only when sqllogictest can’t express the behavior (e.g. isolated optimizer tests / simplify). ([datafusion.apache.org][3])

---

## T2) Plan snapshot testing (EXPLAIN + insta)

### T2.1 EXPLAIN command surface (SQL)

SQL syntax:

* `EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] statement` ([datafusion.apache.org][6])

Plan acquisition:

* Use `EXPLAIN ...` in SQL, or call `DataFrame::explain` to see plans without running queries. ([datafusion.apache.org][7])

### T2.2 Explain formatting controls (stability knobs)

Config options include:

* `datafusion.explain.format` (`indent` default; `tree` for tree rendering)
* `datafusion.explain.show_schema`
* `datafusion.explain.tree_maximum_render_width`
* `datafusion.explain.analyze_level` ([datafusion.apache.org][8])

**Agent rule for stable snapshots:** lock explain format + schema printing so you don’t churn snapshots when defaults change.

### T2.3 Snapshot tool surface (insta)

DataFusion uses **Insta** snapshot testing; review updates via:

* `cargo install cargo-insta`
* `cargo insta review` ([datafusion.apache.org][1])

### T2.4 “Plan snapshot” patterns (choose one)

**Pattern A — sqllogictest plan snapshot (zero Rust)**
Add tests like:

```sql
# udaf_plan_shape_basic
query T rowsort
EXPLAIN FORMAT tree SELECT my_udaf(x) FROM t GROUP BY k
----
<expected plan lines...>
```

Then generate/update with `--complete`. ([apache.googlesource.com][2])

**Pattern B — Rust + insta snapshot (for tight refactor workflows)**

1. Execute EXPLAIN into RecordBatches and stringify.
2. Snapshot string with `insta::assert_snapshot!`.

(Use `assert_batches_eq!` when you want line-by-line stable matching; it’s designed so failures are copy/pasteable. ([Docs.rs][4]))

---

## T3) Window bounded-frame correctness matrices (retract-path proof)

### T3.1 Why you need a dedicated matrix

`retract_batch` is the inverse of `update_batch` and is used to incrementally compute window aggregates with bounded `OVER` frames; DataFusion’s docs specify the operational sequence: evaluate current window, then retract leaving values, then update entering values. ([Docs.rs][5])
If your UDAF advertises/supports retraction and it’s wrong, you can get “mostly correct” group-by results but **incorrect window results**.

### T3.2 Minimal dimension set (high coverage per test)

For a new UDAF, cover these axes (small data; explicit expected outputs):

**A) Frame shapes (ROWS)**

* `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (prefix windows)
* `ROWS BETWEEN N PRECEDING AND CURRENT ROW`
* `ROWS BETWEEN CURRENT ROW AND N FOLLOWING`
* `ROWS BETWEEN N PRECEDING AND N FOLLOWING` (true sliding) — matches DataFusion’s retract example semantics. ([Docs.rs][5])

**B) Partitioning**

* no partition
* `PARTITION BY k` (multiple independent windows)

**C) Ordering**

* `ORDER BY t ASC`
* `ORDER BY t DESC` (forces leaving/entering behavior to invert)

**D) Null behavior**

* Inputs with NULLs (and, if your UDAF supports it, test `[IGNORE NULLS | RESPECT NULLS]`; the switch is wired through `AccumulatorArgs.ignore_nulls` when enabled by the UDAF). ([datafusion.apache.org][3])

**E) Determinism guard**

* Ensure deterministic output ordering: either use `ORDER BY` on final query or use `rowsort` in sqllogictest for output stability. ([apache.googlesource.com][2])

### T3.3 Two oracle strategies

**Oracle 1 — sqllogictest explicit expected outputs**
Best when:

* small data
* deterministic results
* you want pure SQL regression scripts

Use the `.slt` record format with `type_string` and `rowsort/nosort`. ([apache.googlesource.com][2])

**Oracle 2 — Rust “naive recompute” vs “sliding retract” unit test**
Best when:

* you need to *prove* `retract_batch` is inverse-correct
* you want high coverage over many frame widths quickly

Core loop matches DataFusion’s documented sliding algorithm:

1. build “current window” with `update_batch`, then `evaluate`
2. slide: `retract_batch(leaving)` then `update_batch(entering)`, then `evaluate` ([Docs.rs][5])

---

## T4) Agent-ready templates (what to generate)

### T4.1 sqllogictest template: GROUP BY + merge correctness smoke

* create table with values
* run:

  * `SELECT my_udaf(x) FROM t` (global)
  * `SELECT k, my_udaf(x) FROM t GROUP BY k ORDER BY k` (group)
  * `SELECT my_udaf(x) FILTER (WHERE ...) FROM t GROUP BY ...` (if supported)

Remember:

* Provide `type_string` for output columns
* Use `ORDER BY` or `rowsort` for nondeterministic results. ([apache.googlesource.com][2])

### T4.2 sqllogictest template: bounded window frames

One file, one dataset, multiple queries:

* `... OVER (ORDER BY t ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)`
* same with `PARTITION BY k`
* same with descending order
  Use `rowsort` only if your outer query does not enforce a stable order. ([apache.googlesource.com][2])

### T4.3 Rust template: retract correctness matrix (unit level)

* Implement helper: `eval_naive(frame_i)` constructs window slice and runs fresh accumulator
* Implement helper: `eval_sliding()` uses a single accumulator and applies retract/update increments in the documented order ([Docs.rs][5])
* Compare per-row `ScalarValue` outputs
* Add merge associativity tests by splitting input into partitions and verifying `merge_batch(state())` equals single-pass `update_batch`. ([Docs.rs][5])

---

## T5) Practical “definition of done” checklist (UDAF PR gate)

1. **sqllogictest coverage exists** for:

   * global aggregate, group-by aggregate, window bounded frames ([datafusion.apache.org][3])
2. **Expected outputs auto-generated** via `--complete` and committed. ([datafusion.apache.org][1])
3. **Sorting correctness**: outer query `ORDER BY` or `rowsort` set (no `nosort` on multi-row unordered results). ([apache.googlesource.com][2])
4. **Retract correctness** validated either by window SQL outputs or by a unit-level sliding oracle matching the documented retract lifecycle. ([Docs.rs][5])
5. **Plan snapshots (optional but recommended)** exist for at least one canonical query shape using `EXPLAIN` + insta (review via `cargo insta review`). ([datafusion.apache.org][1])

---

If you want, I can now do **Section E (Window UDFs / UDWFs)** with the same density as Section D (full API inventory: `WindowUDF`, `WindowUDFImpl`, `PartitionEvaluator`, ordering/frame semantics, and end-to-end examples) and cross-link it to these test patterns.

[1]: https://datafusion.apache.org/contributor-guide/testing.html "Testing — Apache DataFusion  documentation"
[2]: https://apache.googlesource.com/datafusion/%2Bshow/refs/heads/branch-35/datafusion/sqllogictest/README.md "datafusion/sqllogictest/README.md - datafusion - Git at Google"
[3]: https://datafusion.apache.org/contributor-guide/howtos.html "HOWTOs — Apache DataFusion  documentation"
[4]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html?utm_source=chatgpt.com "assert_batches_eq in datafusion - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.Accumulator.html "Accumulator in datafusion::physical_plan - Rust"
[6]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[7]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
[8]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"

# Section E — User-Defined Window Functions (UDWFs) in DataFusion (Rust)

## E0) Execution model (where your code runs)

DataFusion evaluates window functions via a **sort-based** pipeline: input is sorted by `PARTITION BY` + `ORDER BY`, then `WindowAggExec` finds partition boundaries and instantiates per-partition evaluators, invoking different `PartitionEvaluator` methods depending on the query’s `OVER` clause and the evaluator’s declared capabilities. ([datafusion.apache.org][1])
A **UDWF** is explicitly **stateful across batches** (unlike scalar UDFs): DataFusion may feed multiple `RecordBatch`es for the same partition to the same evaluator instance. ([Docs.rs][2])

---

## E1) Primary API surface inventory (logical wrapper + trait implementation)

### E1.1 `WindowUDF` (logical wrapper / registry handle)

`WindowUDF` is the logical representation used for SQL/plan integration; it is a distinct struct from `WindowUDFImpl` for backwards compatibility. ([Docs.rs][2])

Key constructors / accessors

* `WindowUDF::new_from_impl(F: WindowUDFImpl + 'static)` / `From` impl. ([Docs.rs][2])
* `WindowUDF::new_from_shared_impl(Arc<dyn WindowUDFImpl>)`. ([Docs.rs][2])
* `inner(&self) -> &Arc<dyn WindowUDFImpl>`. ([Docs.rs][2])

Key planner/expression utilities

* `call(args: Vec<Expr>) -> Expr`: builds an `Expr` invoking the UDWF with **default** `order_by/partition_by/window_frame`; then refine via `ExprFunctionExt` (builder). ([Docs.rs][2])

Key “delegated” methods (thin wrappers around `WindowUDFImpl`)

* `name()`, `aliases()`, `signature()`. ([Docs.rs][2])
* `simplify()`, `expressions(...)`, `field(...)`, `sort_options()`, `coerce_types(...)`, `reverse_expr()`, `documentation()`. ([Docs.rs][2])

### E1.2 `create_udwf` (simple helper)

`create_udwf(name, input_type: DataType, return_type: Arc<DataType>, volatility, partition_evaluator_factory: Arc<dyn Fn() -> Result<Box<dyn PartitionEvaluator>>> ) -> WindowUDF`. This helper is **unary** (single argument type) and does **not** expose the full trait surface. ([Docs.rs][3])

### E1.3 `SimpleWindowUDF` (single signature + fixed return type)

`SimpleWindowUDF::new(name, input_type: DataType, return_type: DataType, volatility, partition_evaluator_factory)` implements `WindowUDFImpl` for the “one signature, one return type” case. ([Docs.rs][4])

### E1.4 `WindowUDFImpl` (full power UDWF authoring)

`WindowUDFImpl` is the “UDAF-style” trait interface: implement this when you need multi-arg signatures, dynamic field selection/metadata, coercions, rewrites, reversal, ordering equivalences, limit pushdown behavior, etc. Required/provided method lists are explicit. ([Docs.rs][5])

**Required methods** (must implement)

* `as_any(&self) -> &dyn Any`
* `name(&self) -> &str`
* `signature(&self) -> &Signature`
* `partition_evaluator(&self, PartitionEvaluatorArgs<'_>) -> Result<Box<dyn PartitionEvaluator>>`
* `field(&self, WindowUDFFieldArgs<'_>) -> Result<Arc<Field>>` ([Docs.rs][5])

**Provided methods** (override for advanced behavior)

* `aliases()`
* `expressions(ExpressionArgs<'_>) -> Vec<Arc<dyn PhysicalExpr>>`
* `simplify() -> Option<Box<dyn Fn(WindowFunction, &dyn SimplifyInfo) -> Result<Expr>>>`
* `sort_options() -> Option<SortOptions>`
* `coerce_types(arg_types) -> Result<Vec<DataType>>` (only if `TypeSignature::UserDefined`)
* `reverse_expr() -> ReversedUDWF`
* `documentation()`
* `limit_effect(args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect` ([Docs.rs][5])

Trait identity / hashing: the trait depends on DynEq/DynHash; implement `Eq` + `Hash` and use blanket impls (same pattern as other UDF impl traits). ([Docs.rs][5])

---

## E2) Metadata/argument carriers (what you receive while building evaluators / fields)

### E2.1 `PartitionEvaluatorArgs<'a>` (passed to `partition_evaluator`)

Purpose: describe the physical call site (input exprs/fields, reversal, null treatment) at **physical execution** time. ([Docs.rs][6])

Methods / contents

* `new(input_exprs, input_fields, is_reversed, ignore_nulls)`
* `input_exprs() -> &[Arc<dyn PhysicalExpr>]`
* `input_fields() -> &[Arc<Field>]`
* `is_reversed() -> bool` (true iff UDWF is reversible and evaluation is reversed)
* `ignore_nulls() -> bool` (true when `IGNORE NULLS` is specified) ([Docs.rs][6])

### E2.2 `WindowUDFFieldArgs<'a>` (passed to `field`)

Purpose: compute the output `Field` (type + nullability + name) from input argument fields and the fully qualified display name. ([Docs.rs][7])

Methods

* `new(input_fields, display_name)`
* `input_fields() -> &[Arc<Field>]`
* `name() -> &str` (name for output field)
* `get_input_field(i) -> Option<Arc<Field>>` ([Docs.rs][7])

### E2.3 `ExpressionArgs<'a>` (passed to `expressions`)

Purpose: allow a UDWF to customize which physical exprs are evaluated as “function arguments” (e.g., refine NULL literal behavior / type shaping like built-in `lead/lag`). ([Docs.rs][8])

Methods

* `ExpressionArgs::new(input_exprs, input_fields)`
* `input_exprs()`, `input_fields()` ([Docs.rs][8])

---

## E3) `PartitionEvaluator` — the core execution contract

### E3.1 Partition instantiation & lifecycle

A `PartitionEvaluator` is instantiated **per partition** defined by `OVER (PARTITION BY ...)` and is driven by the runtime. ([Docs.rs][9])
Because data arrives batch-wise, evaluators may be asked to operate incrementally; DataFusion chooses which method(s) to call based on three capability flags. ([Docs.rs][9])

### E3.2 Capability flags → required implementation (the dispatch table)

DataFusion documents a concrete implementation table driven by:

* `uses_window_frame()`
* `supports_bounded_execution()`
* `include_rank()` ([Docs.rs][9])

Mapping (verbatim from docs’ table, paraphrased):

* default (`uses_window_frame=false`, `supports_bounded_execution=false`, `include_rank=false`) ⇒ implement **`evaluate_all`**
* (`uses_window_frame=false`, `supports_bounded_execution=true`, `include_rank=false`) ⇒ implement **`evaluate`**
* (`include_rank=true`) ⇒ implement **`evaluate_all_with_rank`** (independent of boundedness; window frame not required)
* (`uses_window_frame=true`) ⇒ implement **`evaluate`** ([Docs.rs][9])

### E3.3 Methods you can/should implement (and what DataFusion passes)

#### `evaluate_all(values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef>`

* Called **once per partition** for functions that do **not** use window frame values (e.g., `ROW_NUMBER`, `RANK`, `LEAD/LAG` class). ([Docs.rs][9])
* Must return an output array with **exactly one output row per input row**; `num_rows` exists to handle edge cases like empty `values`. ([Docs.rs][9])

#### `evaluate(values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue>`

* General fallback; called **per output row** (slower, but universal). ([Docs.rs][9])
* `range` tells you which row indices are in the row’s logical window. ([datafusion.apache.org][1])
* **Critical: `values` contains (a) function argument arrays and (b) ORDER BY expression result arrays.** If the UDWF has one argument, `values[1..]` contains ORDER BY results. ([Docs.rs][9])

#### `evaluate_all_with_rank(num_rows, ranks_in_partition: &[Range<usize>]) -> Result<ArrayRef>`

* Used for functions evaluable from **rank information only**; DataFusion supplies `ranks_in_partition` segments. ([Docs.rs][9])

#### `supports_bounded_execution() -> bool`

* Declares whether the function can be computed incrementally with **bounded memory**. ([Docs.rs][9])

#### `uses_window_frame() -> bool`

* Declares whether the function uses window frame values when a frame is specified. ([Docs.rs][9])

#### `include_rank() -> bool`

* Declares whether the function can be evaluated with only rank information (enables `evaluate_all_with_rank`). ([Docs.rs][9])

#### `get_range(idx, n_rows) -> Result<Range<usize>>`

* Used when `uses_window_frame == false` to compute the minimal required range for stateful/bounded execution; default is “current row only”. ([Docs.rs][9])

#### `is_causal() -> bool`

* True if the evaluator does **not** require future data; false if it depends on future rows (non-causal). ([Docs.rs][9])

#### `memoize(state: &mut WindowAggState) -> Result<()>`

* Called after each input batch; for fixed-beginning frames (e.g., `UNBOUNDED PRECEDING`), functions like `FIRST_VALUE/LAST_VALUE/NTH_VALUE` can store what they need and modify `WindowAggState` to allow pruning. ([Docs.rs][9])

---

## E4) Ordering + frame semantics (what SQL means; what your evaluator sees)

### E4.1 SQL syntax surface (engine contract)

DataFusion’s SQL reference defines `OVER` syntax with `PARTITION BY`, `ORDER BY`, and optional `frame_clause` with units `{RANGE|ROWS|GROUPS}` and bounds (`UNBOUNDED PRECEDING`, `offset PRECEDING`, `CURRENT ROW`, `offset FOLLOWING`, `UNBOUNDED FOLLOWING`). ([GitHub][10])
Constraints: `RANGE` and `GROUPS` require `ORDER BY`, and `RANGE` requires exactly one `ORDER BY` term. ([GitHub][10])

### E4.2 Default frame behavior (important for “frame-sensitive” evaluators)

If a frame isn’t specified:

* with `ORDER BY`: default is `UNBOUNDED PRECEDING` to `CURRENT ROW`
* without `ORDER BY`: default is entire partition (`UNBOUNDED PRECEDING` to `UNBOUNDED FOLLOWING`) ([datafusion.apache.org][11])

### E4.3 What order-by values look like inside `PartitionEvaluator`

Even if your UDWF has only one argument, DataFusion provides ORDER BY expression results appended in `values[1..]` when calling `evaluate`. That’s the primary bridge from SQL window ordering to your code (e.g., dt for derivatives). ([Docs.rs][9])

---

## E5) Advanced planner hooks on `WindowUDFImpl` (where “engine-native” performance comes from)

### E5.1 `expressions(expr_args) -> Vec<PhysicalExpr>`

Override to customize the physical expressions that are evaluated and passed as “UDWF argument arrays” to the evaluator. This is how built-ins handle tricky typing cases (e.g., `lead/lag` when a NULL literal is provided). ([Docs.rs][5])

### E5.2 `simplify()`

Optional rewrite hook; DataFusion already simplifies args + constant folds; your rewrite must preserve **type + nullability** or planning can fail later. ([Docs.rs][5])

### E5.3 `coerce_types(arg_types)`

Only invoked when the signature is `TypeSignature::UserDefined`; DataFusion inserts CASTs to the returned types. ([Docs.rs][5])

### E5.4 `sort_options()`

Allows a UDWF to declare it introduces a custom output ordering; used to update ordering equivalences (default: no new ordering). ([Docs.rs][5])

### E5.5 Reversal: `reverse_expr()` + `PartitionEvaluatorArgs.is_reversed`

* `reverse_expr()` lets you customize behavior when evaluation order is reversed. ([Docs.rs][5])
* `ReversedUDWF` variants:

  * `Identical` (reversal doesn’t change results)
  * `NotSupported`
  * `Reversed(Arc<WindowUDF>)` (supply an alternate UDWF for reverse evaluation) ([Docs.rs][12])
* At execution time you also get `PartitionEvaluatorArgs.is_reversed()` to know whether you’re running reversed. ([Docs.rs][6])

### E5.6 LIMIT pushdown interaction: `is_causal` + `limit_effect` + `LimitEffect`

For non-causal window functions, DataFusion allows communicating the LIMIT interaction via `limit_effect(...) -> LimitEffect`. ([Docs.rs][5])
`LimitEffect` encodes how a window function affects limit pushdown:

* `None` (causal; does not affect limit)
* `Unknown` (undeclared/dynamic)
* `Relative(n)` (grow limit by `n`)
* `Absolute(n)` (limit must be at least `n`) ([Docs.rs][13])

---

## E6) End-to-end examples (agent-oriented, minimal narrative)

### Example 1 — Unary moving average UDWF (frame-sensitive; simplest `evaluate`)

This matches DataFusion’s own UDWF tutorial pattern: `PartitionEvaluator::uses_window_frame = true` and implement row-wise `evaluate(values, range)` averaging `values[0][range]`. ([datafusion.apache.org][1])

```rust
use std::sync::Arc;
use datafusion::arrow::array::{ArrayRef, Float64Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{PartitionEvaluator, Volatility, create_udwf};

#[derive(Debug)]
struct SmoothEval;
impl PartitionEvaluator for SmoothEval {
    fn uses_window_frame(&self) -> bool { true }

    fn evaluate(&mut self, values: &[ArrayRef], range: &std::ops::Range<usize>) -> Result<ScalarValue> {
        let arr: &Float64Array = values[0].as_ref().as_primitive::<Float64Type>();
        let n = range.end - range.start;
        if n == 0 { return Ok(ScalarValue::Float64(None)); }
        let sum: f64 = arr.values().iter().skip(range.start).take(n).sum();
        Ok(ScalarValue::Float64(Some(sum / n as f64)))
    }
}

fn make_eval() -> Result<Box<dyn PartitionEvaluator>> { Ok(Box::new(SmoothEval)) }

async fn register_and_use(ctx: &SessionContext) -> Result<()> {
    let smooth_it = create_udwf(
        "smooth_it",
        DataType::Float64,
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(make_eval),
    );
    ctx.register_udwf(smooth_it);

    let df = ctx.sql(r#"
      SELECT car, speed,
             smooth_it(speed) OVER (PARTITION BY car ORDER BY time) AS smooth_speed
      FROM cars
    "#).await?;
    df.show().await?;
    Ok(())
}
```

Registration + SQL usage aligns with the official blog example (helper + `register_udwf` + `OVER (PARTITION BY ... ORDER BY ...)`). ([datafusion.apache.org][1])

---

### Example 2 — “Derivative” UDWF (bounded execution + `get_range` + ORDER BY arrays)

Goal: `derivative(y) OVER (PARTITION BY k ORDER BY t)` computes `(y[i]-y[i-1])/(t[i]-t[i-1])`.
Key mechanics:

* declare `uses_window_frame = false` (frame clause irrelevant; we only need adjacent rows)
* declare `supports_bounded_execution = true`
* override `get_range(i, ..)` to request `{i-1, i}` (stateful range selection)
* implement `evaluate(values, range)`; use `values[0]` for `y` and `values[1]` for ORDER BY `t` (ORDER BY arrays are appended). ([Docs.rs][9])

```rust
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, AsArray};
use datafusion::arrow::datatypes::{Float64Type, Int64Type};
use datafusion::common::ScalarValue;
use datafusion::error::{Result, DataFusionError};
use datafusion::logical_expr::PartitionEvaluator;

#[derive(Debug)]
struct DerivativeEval;

impl PartitionEvaluator for DerivativeEval {
    fn uses_window_frame(&self) -> bool { false }
    fn supports_bounded_execution(&self) -> bool { true }

    // Request minimal needed range when frame is irrelevant:
    fn get_range(&self, idx: usize, _n_rows: usize) -> Result<std::ops::Range<usize>> {
        if idx == 0 { Ok(idx..idx+1) } else { Ok((idx-1)..(idx+1)) }
    }

    fn evaluate(&mut self, values: &[ArrayRef], range: &std::ops::Range<usize>) -> Result<ScalarValue> {
        // values[0] = function arg y
        let y: &Float64Array = values[0].as_ref().as_primitive::<Float64Type>();
        // values[1] = first ORDER BY expression t (because single function arg)
        let t: &Int64Array = values[1].as_ref().as_primitive::<Int64Type>();

        // first row -> NULL (no previous)
        if range.end - range.start < 2 { return Ok(ScalarValue::Float64(None)); }

        let i0 = range.start;
        let i1 = range.end - 1;

        let (Some(y0), Some(y1)) = (y.value_opt(i0), y.value_opt(i1)) else {
            return Ok(ScalarValue::Float64(None));
        };
        let (Some(t0), Some(t1)) = (t.value_opt(i0), t.value_opt(i1)) else {
            return Ok(ScalarValue::Float64(None));
        };

        let dt = (t1 - t0) as f64;
        if dt == 0.0 {
            return Err(DataFusionError::Execution("derivative: dt==0".into()));
        }
        Ok(ScalarValue::Float64(Some((y1 - y0) / dt)))
    }
}
```

This example is basically a “how to use ORDER BY values + bounded execution” template: the contracts for `get_range` (used when `uses_window_frame=false`) and ORDER BY arrays in `values[1..]` are documented explicitly. ([Docs.rs][9])

---

### Example 3 — Partition length UDWF (fast `evaluate_all`)

Goal: `part_len(x) OVER (PARTITION BY k ORDER BY t)` returns the partition size repeated for each row.
Mechanics:

* frame-independent, rank not required ⇒ implement `evaluate_all(values, num_rows)` (called once per partition) and return an output array of length `num_rows`. ([Docs.rs][9])

```rust
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::common::Result;
use datafusion::logical_expr::PartitionEvaluator;
use std::sync::Arc;

#[derive(Debug)]
struct PartLenEval;

impl PartitionEvaluator for PartLenEval {
    fn uses_window_frame(&self) -> bool { false }
    fn supports_bounded_execution(&self) -> bool { false } // default path => evaluate_all

    fn evaluate_all(&mut self, _values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let n = num_rows as i64;
        let out = (0..num_rows).map(|_| Some(n)).collect::<Int64Array>();
        Ok(Arc::new(out) as ArrayRef)
    }
}
```

`evaluate_all` is explicitly described as a single-pass per partition path that bypasses per-row frame computations. ([Docs.rs][9])

---

## E7) Testing cross-links (reuse the UDAF harness patterns for UDWFs)

### E7.1 sqllogictest as primary surface test

DataFusion recommends SQL-level `sqllogictest` integration tests for functions called through SQL and returning expected output. ([datafusion.apache.org][14])
Author UDWF tests exactly like UDAF tests, but with `OVER(...)` coverage; update expected results via `--complete`. ([Apache Git Repositories][15])

### E7.2 Plan snapshots for regression (EXPLAIN + insta)

DataFusion uses `cargo insta` snapshot review workflows (`cargo insta review`) for snapshot changes. ([datafusion.apache.org][16])
For UDWFs: snapshot at least one canonical plan shape (`EXPLAIN FORMAT tree ...`) to catch accidental rewrites/changes in window execution behavior (especially if you override `expressions`, `reverse_expr`, or `limit_effect`). ([Docs.rs][5])

### E7.3 Bounded-window correctness matrix (UDWF-specific)

Adopt the prior “bounded-frame matrix” idea, but tuned to `PartitionEvaluator` capability flags:

* Frame shapes: `ROWS BETWEEN N PRECEDING AND M FOLLOWING`, unbounded→current, entire partition, etc. ([GitHub][10])
* Units: `ROWS` vs `RANGE`/`GROUPS` (and enforce ORDER BY constraints). ([GitHub][10])
* Ordering directions: `ORDER BY ... ASC/DESC` + verify `reverse_expr` or `NotSupported` handling where applicable. ([Docs.rs][12])
* Non-causal functions: validate LIMIT behavior if you declare a non-`None` `LimitEffect` (Relative/Absolute) by comparing result correctness under early LIMIT vs full scan. ([Docs.rs][13])

---

## E8) Agent “definition of done” checklist (UDWF PR gate)

* [ ] Correct trait choice: `create_udwf`/`SimpleWindowUDF` for unary fixed return; `WindowUDFImpl` for everything else. ([Docs.rs][3])
* [ ] Output field computed via `field(WindowUDFFieldArgs)` using `field_args.name()` (qualified expression name) and input field typing checks. ([Docs.rs][5])
* [ ] Evaluator implements the right method per capability table (`evaluate_all` vs `evaluate` vs `evaluate_all_with_rank`). ([Docs.rs][9])
* [ ] If you rely on ORDER BY values, document/assume `values[1..]` ordering and enforce expected ORDER BY arity/types. ([Docs.rs][9])
* [ ] If non-causal, set `is_causal=false` and provide a justified `limit_effect`. ([Docs.rs][9])
* [ ] sqllogictest coverage + `--complete` regenerated outputs committed. ([Apache Git Repositories][15])
* [ ] Optional but recommended: EXPLAIN plan snapshot with insta review workflow. ([datafusion.apache.org][16])

If you want the next section after UDWFs, the natural continuation is **F) Table UDFs / UDTFs**, but we can also do a narrower follow-on here: **“UDWF performance patterns”** (bounded execution, memoize/pruning, reversal, and ordering equivalence exploitation) using the same high-density style.

[1]: https://datafusion.apache.org/blog/2025/04/19/user-defined-window-functions/ "https://datafusion.apache.org/blog/2025/04/19/user-defined-window-functions/"
[2]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.WindowUDF.html "WindowUDF in datafusion::logical_expr - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.create_udwf.html "https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.create_udwf.html"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/expr_fn/struct.SimpleWindowUDF.html "https://docs.rs/datafusion/latest/datafusion/logical_expr/expr_fn/struct.SimpleWindowUDF.html"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.WindowUDFImpl.html "WindowUDFImpl in datafusion::logical_expr - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/function/struct.PartitionEvaluatorArgs.html "https://docs.rs/datafusion/latest/datafusion/logical_expr/function/struct.PartitionEvaluatorArgs.html"
[7]: https://docs.rs/datafusion/latest/datafusion/logical_expr/function/struct.WindowUDFFieldArgs.html "https://docs.rs/datafusion/latest/datafusion/logical_expr/function/struct.WindowUDFFieldArgs.html"
[8]: https://docs.rs/datafusion-expr/latest/datafusion_expr/function/struct.ExpressionArgs.html?utm_source=chatgpt.com "ExpressionArgs in datafusion_expr::function - Rust"
[9]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.PartitionEvaluator.html "PartitionEvaluator in datafusion_expr - Rust"
[10]: https://raw.githubusercontent.com/apache/datafusion/main/docs/source/user-guide/sql/window_functions.md "https://raw.githubusercontent.com/apache/datafusion/main/docs/source/user-guide/sql/window_functions.md"
[11]: https://datafusion.apache.org/python/user-guide/common-operations/windows.html "https://datafusion.apache.org/python/user-guide/common-operations/windows.html"
[12]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.ReversedUDWF.html "https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.ReversedUDWF.html"
[13]: https://docs.rs/datafusion-expr/latest/datafusion_expr/enum.LimitEffect.html "https://docs.rs/datafusion-expr/latest/datafusion_expr/enum.LimitEffect.html"
[14]: https://datafusion.apache.org/contributor-guide/howtos.html "https://datafusion.apache.org/contributor-guide/howtos.html"
[15]: https://apache.googlesource.com/datafusion/%2Bshow/refs/heads/branch-35/datafusion/sqllogictest/README.md "https://apache.googlesource.com/datafusion/%2Bshow/refs/heads/branch-35/datafusion/sqllogictest/README.md"
[16]: https://datafusion.apache.org/contributor-guide/testing.html "https://datafusion.apache.org/contributor-guide/testing.html"

# UDWF performance patterns (DataFusion / Rust) — bounded execution, memoize+prune, reversal, ordering equivalence

This section is “what you can actually *do*” as a UDWF author to materially change **latency**, **memory**, and **plan shape**, using the hooks exposed by `WindowUDFImpl` + `PartitionEvaluator`.

---

## P0) Control surface map (what levers exist)

### Trait hooks that directly affect runtime costs

**`PartitionEvaluator` (hot path)**

* `evaluate_all(...) -> ArrayRef` (single-pass, whole-partition)
* `evaluate_all_with_rank(...) -> ArrayRef` (rank-only specialization)
* `supports_bounded_execution() -> bool` + `evaluate(values, range) -> ScalarValue` (incremental / bounded-memory path)
* `get_range(idx, n_rows) -> Range<usize>` (only used when `uses_window_frame=false`; bounds the *required* history)
* `memoize(&mut WindowAggState)` (batch-end callback; enables pruning for fixed-begin frames)
* `is_causal() -> bool` (future-dependence signal) ([Docs.rs][1])

**`WindowUDFImpl` (planner/optimizer interface)**

* `sort_options() -> Option<SortOptions>` (declare output ordering; updates ordering equivalences) ([Docs.rs][2])
* `reverse_expr() -> ReversedUDWF` (reversal support / alternate function when evaluated in reverse) ([Docs.rs][3])
* `limit_effect(args) -> LimitEffect` (limit pushdown interaction for non-causal functions) ([Docs.rs][4])
* `expressions(ExpressionArgs)` (avoid planner-induced casts / refine NULL literal typing; prevents slow paths) ([Docs.rs][2])

---

## P1) Choose the correct evaluation mode (this dominates throughput)

DataFusion dispatches to different `PartitionEvaluator` methods based on three capability flags (`uses_window_frame`, `supports_bounded_execution`, `include_rank`) and the documented implementation table. ([Docs.rs][1])

### Pattern P1-A: Prefer `evaluate_all` whenever frame is irrelevant

`evaluate_all` is explicitly an optimization: it *skips costly window-frame boundary calculation* and avoids per-row `evaluate(...)` overhead. ([Docs.rs][1])
Use for functions whose output is independent of the frame even if the query specifies one (classic examples: `ROW_NUMBER`, `RANK`, `LEAD/LAG`). ([Docs.rs][1])

**Template (vectorized whole-partition)**

```rust
impl PartitionEvaluator for MyEval {
  fn uses_window_frame(&self) -> bool { false }
  fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
    // compute output for all rows in one pass
  }
}
```

**Value case:** maximal throughput, minimal frame bookkeeping. ([Docs.rs][1])

### Pattern P1-B: Use `evaluate_all_with_rank` if you only need peer-group ranks

If your function depends only on peer group boundaries (rank segments), implement `include_rank=true` + `evaluate_all_with_rank`. DataFusion will supply `ranks_in_partition: &[Range<usize>]`. ([Docs.rs][1])
**Value case:** avoid scanning / comparing ORDER BY columns yourself when rank metadata suffices.

### Pattern P1-C: `supports_bounded_execution=true` when you can stream outputs with bounded memory

`supported_bounded_execution` means the function “can be incrementally computed using bounded memory.” ([Docs.rs][1])
This is the lever that turns some window functions from “partition-materializing” into “streaming-ish” (lower peak memory / earlier output).

Concrete built-in example: `row_number` supports bounded execution (`supports_bounded_execution -> true`) and provides both `evaluate_all` and per-row `evaluate` (the runtime can choose based on plan context). ([Docs.rs][5])

---

## P2) Bounded execution recipe (stateful incremental, minimal history)

### P2-A: Implement bounded execution + per-row `evaluate` + (optional) `get_range`

* If `uses_window_frame=true`, DataFusion passes `range` derived from the frame; your cost is dominated by how expensive you make `evaluate(values, range)`. ([Docs.rs][1])
* If `uses_window_frame=false`, `get_range` becomes the key knob: it tells the engine what history you truly need (default is “current row only”). ([Docs.rs][1])

**Micro-pattern: small fixed history (lag/derivative)**

```rust
fn uses_window_frame(&self) -> bool { false }
fn supports_bounded_execution(&self) -> bool { true }
fn get_range(&self, idx: usize, _n: usize) -> Result<Range<usize>> {
  Ok(idx.saturating_sub(1)..(idx+1)) // need [i-1, i]
}
fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> { ... }
```

**Value case:** DataFusion does not need to retain the full partition buffer—only the required suffix.

### P2-B: Make causality explicit (`is_causal`) and align `WindowUDFImpl::limit_effect`

`PartitionEvaluator::is_causal()` answers “does this evaluator need future data?” ([Docs.rs][1])
`WindowUDFImpl::limit_effect` returns `LimitEffect::{None,Unknown,Relative(n),Absolute(n)}` describing how LIMIT pushdown must be adjusted for this function. ([Docs.rs][4])

Built-in reference points:

* `row_number`: causal (`is_causal=true`) and `limit_effect=LimitEffect::None`. ([Docs.rs][5])
* `rank/dense_rank`: causal and `limit_effect=None`; `percent_rank` uses `LimitEffect::Unknown` in the built-in implementation. ([Docs.rs][6])

**Why you care:** DataFusion has an optimizer setting to push LIMIT past window functions when possible (`datafusion.optimizer.enable_window_limits`). Your `is_causal` + `limit_effect` are the semantic inputs needed for safe pushdown. ([Apache DataFusion][7])

---

## P3) Memoize + pruning (fixed-begin frames, amortize memory)

### P3-A: Understand the engine’s window state objects

DataFusion’s window execution keeps per-partition state in `WindowAggState` and uses `WindowFrameContext` to compute frame ranges (ROWS/RANGE/GROUPS). ([Docs.rs][8])

Key details you exploit:

* `WindowAggState` stores `window_frame_range`, `out_col` (already computed results), `last_calculated_index`, `offset_pruned_rows`, and an `is_end` flag. ([Docs.rs][8])
* `WindowAggState::prune_state(n_prune)` shifts indices, increments `offset_pruned_rows`, adjusts `last_calculated_index`, and also updates GROUPS context bookkeeping. ([Docs.rs][9])
* `WindowFrameContext` tells you:

  * `ROWS` frames are “inherently stateless”
  * `RANGE` and `GROUPS` frames are stateful and store search progress to amortize cost to `O(n)`; RANGE uses `SortOptions` because null ordering affects comparisons. ([Docs.rs][9])

### P3-B: `PartitionEvaluator::memoize(&mut WindowAggState)` is the prune hook

Docs: when the frame has a fixed beginning (e.g., `UNBOUNDED PRECEDING`), functions like `FIRST_VALUE/LAST_VALUE/NTH_VALUE` can save what they need and “modify `WindowAggState` appropriately to allow rows to be pruned”; it is called after each input batch. ([Docs.rs][1])

**Concrete built-in pattern: `NthValueEvaluator::memoize`**

* Maintains an internal `finalized_result: Option<ScalarValue>` to short-circuit future evaluations.
* Uses `WindowAggState` (notably `out_col` and `window_frame_range`) to detect when the result is stable.
* When prunable and `ignore_nulls` is false, it stores the finalized result and then advances `state.window_frame_range.start` to keep only a small tail buffer. ([Docs.rs][10])

This is the “prune without explicit prune_state” variant: instead of calling `prune_state`, it constrains the live frame start (`window_frame_range.start = end - buffer_size`) so upstream buffer management can drop older rows. ([Docs.rs][10])

**Implications / reusable recipe**

1. Define an “early-finalize condition” keyed on the frame and required N (e.g., FIRST_VALUE finalizes after you’ve seen the first non-null if `RESPECT NULLS` semantics; for NTH_VALUE, after you’ve seen enough rows). ([Docs.rs][10])
2. Store finalized scalar result in evaluator state (so `evaluate` becomes `O(1)` returning a clone). ([Docs.rs][10])
3. Shrink retained buffer by advancing `WindowAggState.window_frame_range.start` (or, if you are coordinating with GROUPS internals, call `prune_state(n_prune)` when safe). ([Docs.rs][10])
4. Be explicit about `IGNORE NULLS`: built-in NTH_VALUE avoids memoization when nulls are ignored (because the “stable point” depends on future non-nulls). ([Docs.rs][10])

---

## P4) Reversal (compute in reverse order to unlock plan options)

### P4-A: `WindowUDFImpl::reverse_expr() -> ReversedUDWF`

`ReversedUDWF` is:

* `Identical` (same results when reversed)
* `NotSupported`
* `Reversed(Arc<WindowUDF>)` (supply an alternate UDWF for reverse evaluation) ([Docs.rs][3])

This hook exists so the engine can evaluate a window function in reverse order when that is beneficial/required, without violating semantics. ([Docs.rs][2])

### P4-B: Execution-time reversal signal

When constructing evaluators, DataFusion passes `PartitionEvaluatorArgs.is_reversed()` so your evaluator can branch on direction if you keep a single implementation. ([Docs.rs][2])

### P4-C: Practical reversal patterns

* **Symmetric / order-invariant outputs**: return `Identical` (cheapest; no alternate impl).
* **Direction-dependent but invertible**: return `Reversed(other_udwf)` where `other_udwf` implements the mirrored semantics; avoid “if reversed then …” branches in hot loops. ([Docs.rs][3])
* **Non-reversible**: `NotSupported` and force the planner to avoid reverse execution for this function.

---

## P5) Ordering equivalence exploitation (avoid sorts, enable streaming-friendly plans)

Window execution has hard ordering requirements (`PARTITION BY` + `ORDER BY`), and sorts are pipeline-breaking. DataFusion’s ordering analysis exists explicitly to decide when a sort requirement is already satisfied, enabling removal of unnecessary sorts and more streaming-friendly execution. ([Apache DataFusion][11])

### P5-A: What the planner can already do (your UDWF should not block it)

Ordering analysis reasons about:

* constant expressions (can be removed from ordering requirements),
* equivalence groups (e.g., cloned columns / monotonic transforms),
* succinct encodings of valid orderings,
  to decide if an ordering requirement is satisfied (and thus a `SortExec` can be skipped). ([Apache DataFusion][11])

**UDWF author action:** keep your function’s physical argument expressions clean and predictable so ordering inference can match them (e.g., don’t embed opaque rewrites that destroy equivalence detection; if you must, do it in `simplify` with schema-preserving rewrites). ([Docs.rs][2])

### P5-B: Declare output ordering when it is semantically correct (`sort_options`)

`WindowUDFImpl::sort_options()` exists so a UDWF can declare that its **result column is ordered** (and with what `SortOptions`), which is then used to update ordering equivalences. ([Docs.rs][2])

Built-in evidence (production patterns):

* `row_number` declares ascending, nulls_last sort options. ([Docs.rs][5])
* `rank` declares ascending, nulls_last sort options. ([Docs.rs][6])

**Interpretation:** these ranking outputs are monotone in the evaluation order, so downstream operators can potentially exploit that ordering.

### P5-C: RANGE/GROUPS cost model awareness (you can influence via documentation + defaults)

Internals:

* ROWS frames are stateless.
* RANGE/GROUPS frames are stateful; they maintain search state to amortize cost to `O(n)`, and RANGE computations depend on `SortOptions` because null ordering affects comparisons. ([Docs.rs][9])

**UDWF author action:**

* If your function does not truly need frame semantics, set `uses_window_frame=false` and implement `evaluate_all` / bounded execution paths so the engine can avoid RANGE/GROUPS boundary machinery entirely. ([Docs.rs][1])
* If you *do* need frame semantics, push users toward ROWS frames (or narrow RANGE usage) in your documentation; ROWS avoids the stateful RANGE comparator path. ([Docs.rs][9])

---

## P6) “Don’t regress into slow paths” (expression shaping)

`WindowUDFImpl::expressions(ExpressionArgs)` lets you control the exact physical expressions passed into `PartitionEvaluator` (including typing refinements). This is not cosmetic: it prevents pathological casting / NULL-literal typing issues that can force slower evaluation or runtime errors. ([Docs.rs][2])

Built-in example: `WindowShift` (lead/lag machinery) overrides `expressions` specifically to handle `NULL` default arguments by refining types (documented in its Rustdoc and tied to a real issue). ([Docs.rs][12])

---

## P7) Performance-oriented test hooks (tie back to your harness patterns)

### Plan-shape regression: “prove sorts/limits moved”

* Add `EXPLAIN FORMAT tree ...` sqllogictest cases and snapshot the plan; ordering analysis exists to remove sorts, so the plan should show fewer `SortExec`/enforcers when inputs are already ordered. ([Apache DataFusion][11])
* Toggle `datafusion.optimizer.enable_window_limits` and verify LIMIT pushdown behavior matches your `is_causal + limit_effect` declarations (causal → `LimitEffect::None`). ([Apache DataFusion][7])

### Correctness matrix (bounded + prune + reversal)

* Bounded execution: validate `supports_bounded_execution` path yields identical outputs to full-partition evaluation across varying batch boundaries. ([Docs.rs][1])
* Memoize/pruning: craft UNBOUNDED PRECEDING frames and assert identical outputs while forcing multi-batch partitions; ensure `memoize` doesn’t fire under `IGNORE NULLS` unless correct. ([Docs.rs][1])
* Reversal: if you return `Reversed(...)` or `Identical`, explicitly test both directions (or at minimum `ORDER BY ASC` vs `DESC` plans) for equivalence. ([Docs.rs][3])

---

## P8) Reality check / roadmap signal

DataFusion has active work items discussing further vectorization of window functions (i.e., doing for windows what `GroupsAccumulator` did for aggregates). Until that lands, your biggest wins as a UDWF author are still: **use `evaluate_all`/rank-specializations when possible**, and **bounded/memoize/prune** for long partitions. ([GitHub][13])

[1]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.PartitionEvaluator.html "PartitionEvaluator in datafusion_expr - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.WindowUDFImpl.html "WindowUDFImpl in datafusion::logical_expr - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.ReversedUDWF.html?utm_source=chatgpt.com "ReversedUDWF in datafusion::logical_expr - Rust"
[4]: https://docs.rs/datafusion-expr/latest/datafusion_expr/enum.LimitEffect.html?utm_source=chatgpt.com "LimitEffect in datafusion_expr - Rust"
[5]: https://docs.rs/datafusion-functions-window/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_functions_window/row_number.rs.html "row_number.rs - source"
[6]: https://docs.rs/datafusion-functions-window/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_functions_window/rank.rs.html "rank.rs - source"
[7]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[8]: https://docs.rs/datafusion-expr/latest/datafusion_expr/window_state/struct.WindowAggState.html "WindowAggState in datafusion_expr::window_state - Rust"
[9]: https://docs.rs/datafusion-expr/latest/src/datafusion_expr/window_state.rs.html "window_state.rs - source"
[10]: https://docs.rs/datafusion-functions-window/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_functions_window/nth_value.rs.html "nth_value.rs - source"
[11]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/ "Using Ordering for Better Plans in Apache DataFusion - Apache DataFusion Blog"
[12]: https://docs.rs/datafusion/latest/datafusion/functions_window/lead_lag/struct.WindowShift.html?utm_source=chatgpt.com "WindowShift in datafusion::functions_window::lead_lag - Rust"
[13]: https://github.com/apache/datafusion/issues/15607?utm_source=chatgpt.com "Vectorize window functions #15607 - apache/datafusion"

# Section F — Table UDFs / UDTFs (table-valued functions) in DataFusion (Rust)

A **UDTF** is a **compile/planning-time** function that returns a **`TableProvider`**; DataFusion then treats the returned provider as the `FROM` relation for the rest of the query plan. DataFusion’s own docs frame it exactly this way: “UDTF … takes parameters and returns a `TableProvider`,” often used for “reading data from external sources and interactive analysis.” ([Apache DataFusion][1])

---

## F1) Core contract

### F1.1 `TableFunctionImpl`: the authoring contract

The entire UDTF surface reduces to a single trait method:

```rust
pub trait TableFunctionImpl: Debug + Send + Sync {
  fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>, DataFusionError>;
}
```

This is the hard contract: **inputs are logical `Expr`s**, output is an **owned `Arc<dyn TableProvider>`**. ([Docs.rs][2])

DataFusion ships built-in `TableFunctionImpl` implementors for at least **`RangeFunc`** and **`GenerateSeriesFunc`** (these are the built-in “range/generate_series” style table functions). ([Docs.rs][2])

### F1.2 `TableFunction`: the registry wrapper

DataFusion wraps a `TableFunctionImpl` with a lightweight handle:

* `TableFunction::new(name: String, fun: Arc<dyn TableFunctionImpl>)`
* `TableFunction::create_table_provider(args: &[Expr]) -> Result<Arc<dyn TableProvider>, DataFusionError>`

Think of `TableFunction` as *“named callable that materializes a provider.”* ([Docs.rs][3])

### F1.3 Session commands: register / deregister / fetch UDTFs

`SessionContext` exposes explicit entry points:

* `register_udtf(&self, name: &str, fun: Arc<dyn TableFunctionImpl>)`
* `deregister_udtf(&self, name: &str)`
* `table_function(&self, name: &str) -> Result<Arc<TableFunction>>` ([Docs.rs][4])

These are the primitives your agents use to wire UDTFs into a runtime.

### F1.4 SQL call-site semantics (table factor)

UDTFs are invoked where a table reference would be used, e.g.:

```sql
SELECT * FROM echo(1);
SELECT * FROM read_csv('/path/file.csv', 1 + 1);
```

This pattern is shown in DataFusion’s docs (“use it in your query: `SELECT * FROM echo(1)`”) and the official `simple_udtf.rs` example (`SELECT * FROM read_csv(...);`). ([Apache DataFusion][1])

---

## F2) Implementation surface (what you actually implement)

### F2.1 `TableFunctionImpl::call`: argument parsing + validation + constant-folding strategy

`call` receives `&[Expr]`. In practice, UDTFs are **parameterized relation constructors**, so *the only sane semantics* are: **accept only literals / constant-foldable expressions** (anything row-dependent is meaningless at planning time).

DataFusion’s UDTF tutorial demonstrates the canonical parsing pattern:

```rust
let Some(Expr::Literal(ScalarValue::Int64(Some(value)), _)) = exprs.get(0) else {
  return plan_err!("First argument must be an integer");
};
```

…and then builds a provider (often `MemTable`) and returns it. ([Apache DataFusion][1])

**Constant folding in `call` is on you (today).** The official `simple_udtf.rs` example explicitly simplifies an expression argument (e.g. `1 + 1`) into a literal using `ExprSimplifier`, then pattern-matches the simplified `Expr::Literal`. ([Apache Git Repositories][5])
There is an open enhancement request to improve this: “optimize the args of table functions … transform `2+3` into a literal.” ([GitHub][6])

**Type coercion watchout:** a recent bug report notes “Expressions to udtf are not coerced” and shows a panic scenario for `SELECT * FROM f(ARRAY[0.1, 1, 2])`. Until this is resolved, treat UDTF argument typing as “raw Expr” and do your own coercion/validation. ([GitHub][7])

#### Recommended `call` pipeline (agent template)

1. **Arity check** (fast fail)
2. **Optional simplify** constant expressions you’re willing to accept (numeric ops, casts, etc.)
3. **Literal extraction** (pattern match on `Expr::Literal(ScalarValue::...)`)
4. **Semantic validation** (ranges, paths, option enums)
5. **Provider construction** (schema fixed *per provider instance*, can depend on args)
6. Return `Arc<dyn TableProvider>`

### F2.2 `TableProvider`: execution-time contract returned by the UDTF

UDTF output must be a `TableProvider`. The required methods are minimal:

* `schema() -> Arc<Schema>`
* `table_type() -> TableType`
* `scan(state, projection, filters, limit) -> Future<Result<Arc<dyn ExecutionPlan>>>` ([Docs.rs][8])

`TableProvider` is explicitly the bridge: it provides **planning info** (schema, pushdown capabilities) and returns an **`ExecutionPlan`** to actually read data at runtime. ([Docs.rs][8])

#### Pushdowns you should surface (the value case for UDTFs that read “external sources”)

* `scan` receives:

  * `projection: Option<&Vec<usize>>` (column pruning)
  * `filters: &[Expr]` (predicate pushdown candidate)
  * `limit: Option<usize>` (limit pushdown candidate) ([Docs.rs][8])

* `supports_filters_pushdown(filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>` lets you declare pushdown level per filter. ([Docs.rs][8])
  DataFusion documents the semantics:

  * `Exact`: provider guarantees the filter is fully applied
  * `Inexact`: provider applies it partially; DataFusion will re-apply after scan for correctness ([Apache DataFusion][9])

If your UDTF is meant to behave like DuckDB’s `read_parquet/read_csv`, *pushdowns are the core differentiator*: the function returns a provider that can implement filter/projection/limit pushdown *as a function of the parameters* (path, schema hints, partitions, credentials).

#### `scan_with_args` (newer ergonomic surface)

Instead of separate `(projection, filters, limit)` parameters, DataFusion also exposes `scan_with_args(state, args: ScanArgs)` where `ScanArgs` is a builder-style carrier:

* `with_projection(Option<&[usize]>)`
* `with_filters(Option<&[Expr]>)` (AND-combined; pushdown depends on `supports_filters_pushdown`)
* `with_limit(Option<usize>)` ([Docs.rs][10])
  Its result type is `ScanResult(plan: Arc<dyn ExecutionPlan>)`. ([Docs.rs][11])

### F2.3 TableProvider vs TableSource boundary (planning-time vs execution-time)

DataFusion intentionally splits *logical planning* from *physical execution*:

* `TableSource` = **planning-time** subset: schema + constraints + pushdown capabilities; used by logical planner/optimizer. ([Docs.rs][12])
* `TableProvider` = **execution-time** superset: includes `scan` / `insert_into` / etc. ([Docs.rs][12])

Rationale: keep logical plan code independent of the execution engine; some consumers use DataFusion logical plans but a different execution backend. ([Docs.rs][12])

Conversion utilities:

* `provider_as_source(table_provider)` or `DefaultTableSource` convert a provider into a `TableSource` for logical planning. ([Apache DataFusion][13])

**Implication for UDTFs:** your `TableProvider` returned from `call` will be “seen” by the planner through the `TableSource` lens during optimization, then used as a `TableProvider` again during physical planning/execution.

---

## F3) Built-in & ecosystem patterns (what to copy)

### F3.1 `MemTable` as the canonical toy provider (and still useful)

`MemTable` is the simplest `TableProvider`: in-memory `RecordBatch` partitions. ([Docs.rs][14])
Constructor constraints:

* `MemTable::try_new(schema, partitions: Vec<Vec<RecordBatch>>)` requires **at least one partition**; empty table uses `vec![vec![]]`. ([Docs.rs][14])

UDTF tutorial’s `EchoFunction` uses `MemTable` as the returned provider (literal arg → 1-row table). ([Apache DataFusion][1])

Advanced `MemTable` features (often missed):

* `with_sort_order(...)`: if you declare sort order and it’s wrong, results may be incorrect; if correct, DataFusion can omit sorts / pick better algorithms. ([Docs.rs][14])
* `load(table_provider, output_partitions, state)` to materialize another provider into memory. ([Docs.rs][14])

**When `MemTable` is still the right production choice:** metadata readers, small dimension tables, “describe/inspect” UDTFs, caches, and single-file introspection.

### F3.2 Built-in table functions: `range` / `generate_series`

DataFusion’s table function crate exposes:

* `range() -> Arc<TableFunction>`
* `all_default_table_functions() -> Vec<Arc<TableFunction>>`
* a helper macro `create_udtf_function!` to ensure singleton instances when registering function sets ([Docs.rs][15])

The `generate_series` module defines the underlying structs (`RangeFunc`, `GenerateSeriesFunc`, `GenerateSeriesTable`, etc.), i.e. real reference implementations of “parameterized provider factories.” ([Docs.rs][16])

### F3.3 “Read external source” pattern (DuckDB-like `read_csv/read_parquet`)

**Goal:** make `FROM read_csv('path', …options…)` return a provider that scans files lazily, supports pushdowns, and config is driven by the UDTF args.

DataFusion ships a working example UDTF that mimics DuckDB’s `read_csv(filename, [limit])`:

* Registers via `ctx.register_udtf("read_csv", Arc::new(LocalCsvTableFunc {}));`
* Accepts `read_csv('{csv_file}', 1 + 1)` and uses an `ExprSimplifier` to fold `1+1` into `2`
* Returns a custom `TableProvider` whose `scan` returns a `MemorySourceConfig` exec over in-memory `RecordBatch`es (demo approach). ([Apache Git Repositories][5])

**Production-grade variant of the same pattern (what agents should implement):**

* In `call`: parse `(url/path, format options, schema hints, partition columns, credentials key, …)`
* Return a provider that uses:

  * `ListingTable` (for file sets / object stores) or a custom file-format provider
  * implements `supports_filters_pushdown` and applies filters in `scan`
  * uses `projection` and `limit` to reduce IO ([Docs.rs][17])

DataFusion’s crate docs explicitly call out `ListingTable` as a built-in provider that reads Parquet/JSON/CSV/AVRO from local or remote directories, supporting partitioning, compression, object stores, metadata caching, etc. ([Docs.rs][17])

### F3.4 “Metadata readers” pattern (introspection UDTFs)

The DataFusion CLI includes table functions not present by default in the core engine:

* `parquet_metadata('file.parquet')` returns a table describing column-chunk metadata/stats and is shown with real SQL examples. ([Apache DataFusion][18])
* `metadata_cache()` and `statistics_cache()` expose ListingTable’s internal caches as queryable tables, including fields and example queries (e.g., `select sum(metadata_size_bytes) from metadata_cache();`). ([Apache DataFusion][18])

**Generalizable recipe for your own “inspect_*” UDTFs:**

* `call(args)` parses (path / table name / object store key / flags)
* provider is often a `MemTable` (small outputs) or a lightweight custom `TableProvider` that streams computed batches
* output schema is stable and **designed for downstream querying** (filterable columns, typed metrics)

---

## Minimal agent-ready implementation templates

### Template A — UDTF registration + retrieval + execution without SQL

Use `table_function` + `create_table_provider` + `read_table` to test a UDTF directly:

```rust
let tf = ctx.table_function("echo")?;                    // fetch TableFunction
let provider = tf.create_table_provider(&[lit(1i64)])?;  // Expr args
let df = ctx.read_table(provider)?;                      // DataFrame over provider
let batches = df.collect().await?;
```

APIs involved are directly documented on `SessionContext` and `TableFunction`. ([Docs.rs][4])

### Template B — external read UDTF (shape)

* `call`: fold constants, extract literals, build provider config
* provider: implement `scan` and pushdowns (`supports_filters_pushdown`) ([Docs.rs][2])

---

## Practical “gotchas” checklist (what to enforce in PR review)

* **Args are `Expr`**; if you accept non-literal constants, you must simplify them yourself (see `simple_udtf.rs`), and be aware this behavior is under active improvement discussion. ([Apache Git Repositories][5])
* **No guaranteed type coercion** for UDTF args (track/guard; avoid panics). ([GitHub][7])
* **Provider must surface pushdown capabilities** or you’ll pay full scan cost; use `supports_filters_pushdown` + honor `projection`/`limit`. ([Docs.rs][8])
* **Understand TableSource vs TableProvider**: planner sees `TableSource` subset; execution needs `scan` returning an `ExecutionPlan`. ([Docs.rs][12])
* **Use `MemTable` deliberately**: great for metadata/small outputs; for large external reads, return a lazy scanning provider (often `ListingTable` or a custom provider). ([Docs.rs][14])

If you want the next increment at the same density, the natural continuation is **G) Function registration + discovery + registry surfaces** (how UDTFs/UDFs/UDAFs/UDWFs live in `SessionState`, how to enumerate them, interaction with `FunctionRegistry`, and how SQL planning resolves table factors into providers).

[1]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html "Adding User Defined Functions: Scalar/Window/Aggregate/Table Functions — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableFunctionImpl.html "TableFunctionImpl in datafusion::catalog - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/catalog/struct.TableFunction.html "TableFunction in datafusion::catalog - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[5]: https://apache.googlesource.com/datafusion/%2B/refs/tags/48.0.0/datafusion-examples/examples/simple_udtf.rs "datafusion-examples/examples/simple_udtf.rs - datafusion - Git at Google"
[6]: https://github.com/apache/datafusion/issues/14958?utm_source=chatgpt.com "Table function supports non-literal args · Issue #14958"
[7]: https://github.com/apache/datafusion/issues/19914?utm_source=chatgpt.com "Expressions to udtf are not coerced · Issue #19914"
[8]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html "TableProvider in datafusion::datasource - Rust"
[9]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html "Custom Table Provider — Apache DataFusion  documentation"
[10]: https://docs.rs/datafusion/latest/datafusion/catalog/struct.ScanArgs.html "ScanArgs in datafusion::catalog - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/catalog/struct.ScanResult.html "ScanResult in datafusion::catalog - Rust"
[12]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.TableSource.html "TableSource in datafusion_expr - Rust"
[13]: https://datafusion.apache.org/library-user-guide/building-logical-plans.html "Building Logical Plans — Apache DataFusion  documentation"
[14]: https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html "MemTable in datafusion::datasource - Rust"
[15]: https://docs.rs/datafusion-functions-table/latest/datafusion_functions_table/fn.range.html "range in datafusion_functions_table - Rust"
[16]: https://docs.rs/datafusion-functions-table/latest/datafusion_functions_table/generate_series/index.html "datafusion_functions_table::generate_series - Rust"
[17]: https://docs.rs/datafusion/latest/datafusion/ "datafusion - Rust"
[18]: https://datafusion.apache.org/user-guide/cli/functions.html "CLI Specific Functions — Apache DataFusion  documentation"

# Section G — Function registration, discovery, and registry surfaces (DataFusion / Rust)

This section is the “wiring diagram”: **where** each function class is stored, **how** it’s registered/discovered, and **how** SQL planning resolves *function calls* (expressions) vs *table factors* (relations) into `LogicalPlan` / `TableProvider` / `ExecutionPlan`.

---

## G0) Layering: `SessionContext` vs `SessionState` vs `TaskContext`

### `SessionState` is the authoritative “session kernel”

`SessionState` is explicitly documented as containing the state required to **plan and execute** queries, including **configuration, functions, and runtime environment**. ([Docs.rs][1])

Key consequence: if you need to reason about “where a thing lives”, assume it lives in `SessionState` (or a registry owned by it), and `SessionContext` is the ergonomic façade.

### `SessionState` is *not* `Default`

There is no `Default`/`new()` for `SessionState` to avoid accidentally running planning/execution without explicit config/runtime; the canonical construction path is `SessionStateBuilder`. ([Docs.rs][1])

---

## G1) Where functions live (the actual storage maps)

`SessionState` exposes direct references to its internal registries (all keyed by `String` name):

### Scalar / Aggregate / Window functions (expression functions)

* `scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>>` ([Docs.rs][1])
* `aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>>` ([Docs.rs][1])
* `window_functions(&self) -> &HashMap<String, Arc<WindowUDF>>` ([Docs.rs][1])

### Table functions (UDTFs / relation-producing functions)

* `table_functions(&self) -> &HashMap<String, Arc<TableFunction>>` ([Docs.rs][1])
* `register_udtf(&mut self, name: &str, fun: Arc<dyn TableFunctionImpl>)` ([Docs.rs][1])
* `deregister_udtf(&mut self, name: &str) -> Result<Option<Arc<dyn TableFunctionImpl>>>` ([Docs.rs][1])

> Architectural note: **UDTFs are stored separately** from the `FunctionRegistry` trait (below). That separation is explicit in API surface: `FunctionRegistry` covers scalar/aggregate/window only, while `SessionState` has separate `register_udtf/table_functions`. ([Docs.rs][2])

---

## G2) The `FunctionRegistry` trait (core registry interface for expression functions)

### Purpose statement (what the trait is for)

DataFusion documents `FunctionRegistry` as: “A registry [that] knows how to build logical expressions out of user-defined function’ names.” ([Docs.rs][2])

### Required discovery + lookup methods (scalar/aggregate/window only)

* `udfs() -> HashSet<String>`
* `udafs() -> HashSet<String>`
* `udwfs() -> HashSet<String>`
* `udf(name) -> Result<Arc<ScalarUDF>>`
* `udaf(name) -> Result<Arc<AggregateUDF>>`
* `udwf(name) -> Result<Arc<WindowUDF>>`
* `expr_planners() -> Vec<Arc<dyn ExprPlanner>>` ([Docs.rs][2])

### Provided mutation hooks (default = error for read-only registries)

* `register_udf / register_udaf / register_udwf` (returns prior impl if overwritten)
* `deregister_udf / deregister_udaf / deregister_udwf`
* `register_function_rewrite(rewrite: Arc<dyn FunctionRewrite + Send + Sync>)`
* `register_expr_planner(expr_planner: Arc<dyn ExprPlanner>)` ([Docs.rs][2])

`register_function_rewrite` is the “operator-to-function” bridge: rewrite rules can map logical operators into function calls (e.g. `a || b` → `array_concat(a,b)`), explicitly called out in the trait docs. ([Docs.rs][2])

### Implementors (critical for “where do I call this?”)

`FunctionRegistry` is implemented for:

* `SessionContext`
* `SessionState`
* `TaskContext`
* `MemoryFunctionRegistry` ([Docs.rs][2])

This means you can use a single “registry protocol” across:

* **planning time** (`SessionState`)
* **query runtime** (`TaskContext`)
* **tests / standalone expression building** (`MemoryFunctionRegistry`) ([Docs.rs][2])

---

## G3) Programmatic registration + enumeration: “commands” you actually use

### G3.1 Registering functions (Rust)

There are two “layers” you can wire against:

#### A) High-level: register on `SessionContext` (common case)

`SessionContext` is the “main interface for executing queries” and is an implementor of `FunctionRegistry`, so you can:

* call its explicit registration helpers (e.g., `register_udaf/register_udwf/register_udtf`)
* or call `FunctionRegistry::register_udf` methods against it (trait method) ([Docs.rs][2])

#### B) Low-level: mutate a `SessionState` directly (embedding / advanced use)

Direct mutation surfaces:

* scalar/aggregate/window maps accessible via getters (read) ([Docs.rs][1])
* UDTF register/deregister functions (write) ([Docs.rs][1])

### G3.2 Enumerating what’s registered (Rust)

You have three practical enumeration “planes”:

#### Plane 1 — Registry protocol (names only; scalar/agg/window)

Use `FunctionRegistry::{udfs, udafs, udwfs}` for name sets. ([Docs.rs][2])

#### Plane 2 — `SessionState` maps (name → implementation)

Use `SessionState::{scalar_functions, aggregate_functions, window_functions, table_functions}` to inspect the actual objects and metadata wrappers. ([Docs.rs][1])

#### Plane 3 — SQL / information_schema (user-facing discovery)

DataFusion’s SQL docs explicitly support:

* `SHOW FUNCTIONS [LIKE <pattern>]`
* `information_schema.information_schema.routines` (functions + descriptions)
* `information_schema.information_schema.parameters` (parameters + descriptions) ([Apache DataFusion][3])

**Enablement footgun:** access to `information_schema` is gated by config; the config docs list `datafusion.catalog.information_schema` (default `false`) controlling whether `information_schema` virtual tables exist. ([Apache DataFusion][4])

---

## G4) Builtins: default registration, composition, and “don’t accidentally overwrite”

### G4.1 `SessionStateBuilder::with_default_features()` (the canonical default install)

`SessionStateBuilder::with_default_features()` adds defaults for:

* `table_factories`
* file formats
* expr planners
* builtin scalar + aggregate + window functions
  …and explicitly notes it **overwrites** previously registered items with the same name. ([Docs.rs][5])

### G4.2 `SessionStateDefaults` (the “default sets” and the “register_*” entrypoints)

`SessionStateDefaults` provides:

* `default_scalar_functions() -> Vec<Arc<ScalarUDF>>`
* `default_aggregate_functions() -> Vec<Arc<AggregateUDF>>`
* `default_window_functions() -> Vec<Arc<WindowUDF>>`
* `default_table_functions() -> Vec<Arc<TableFunction>>`
* `default_expr_planners() -> Vec<Arc<dyn ExprPlanner>>`
* `register_builtin_functions(state: &mut SessionState)` (scalar + array + aggregate)
* `register_scalar_functions / register_aggregate_functions / register_array_functions`
* plus defaults for catalogs, table factories, file formats, schema registration, etc. ([Docs.rs][6])

**Deployment implication:** if you want a **minimal surface area** (binary size / attack surface / deterministic behavior), you can bypass `with_default_features()` and explicitly choose which defaults to register via `SessionStateDefaults::*` or via function packages (next). ([Docs.rs][6])

### G4.3 Function packages (`datafusion-functions`): curated registration at module granularity

The `datafusion-functions` crate is explicitly designed to let you control which function packages are available (binary size, dialect-specific behavior). It supports:

* registering all functions via `register_all`
* or registering per-package via `module::functions()` and calling `registry.register_udf(udf)` ([Docs.rs][7])

This is the “best practice” way to keep a tight function set while still using DataFusion’s extension API.

---

## G5) SQL planning resolution pipeline (how names become plans)

### G5.1 Parsing and statement planning (`SessionState` SQL surfaces)

When the `sql` feature is enabled, `SessionState` exposes the full parse→plan pipeline:

* `sql_to_statement(sql, dialect) -> Result<Statement>`: parse SQL string into DataFusion’s SQL AST. ([Docs.rs][1])
* `resolve_table_references(statement: &Statement) -> Result<Vec<TableReference>>`: finds table refs (excluding CTE refs). ([Docs.rs][1])
* `statement_to_plan(statement: Statement) -> Result<LogicalPlan>`: convert AST statement into a logical plan. ([Docs.rs][1])
* `create_logical_plan(sql: &str) -> Result<LogicalPlan>`: end-to-end SQL→LogicalPlan; docs note it plans **any SQL DataFusion supports** (including DML like `CREATE TABLE` / `COPY`). ([Docs.rs][1])

### G5.2 Expression compilation includes type coercion + function rewrites

`SessionState::create_physical_expr(expr, df_schema) -> Result<Arc<dyn PhysicalExpr>>` explicitly states it builds a physical expression “after applying type coercion, and function rewrites.” ([Docs.rs][1])

Those “function rewrites” are precisely what `FunctionRegistry::register_function_rewrite` installs (operator→function call transforms). ([Docs.rs][2])

### G5.3 Relation planning and table-factor resolution

Table factors in SQL (`FROM ...`) resolve via two distinct mechanisms:

#### A) Table references (`schema.table`) resolve through the catalog layer

`SessionState::schema_for_ref(table_ref: TableReference) -> Result<Arc<dyn SchemaProvider>>` is the explicit API for resolving a `TableReference` to a schema provider (catalog/schema/table namespace resolution). ([Docs.rs][1])

#### B) Table functions (`FROM my_udtf(...)`) resolve through `table_functions`

UDTFs are registered via `register_udtf(name, Arc<dyn TableFunctionImpl>)` and show up in `table_functions() -> HashMap<String, Arc<TableFunction>>`. ([Docs.rs][1])
The DataFusion UDTF docs define the semantic contract: a UDTF takes parameters and returns a `TableProvider`. ([Apache DataFusion][8])

**Operationally:** SQL planning sees a “table function” table factor, looks up `name` in the session’s table-function map, executes the function’s planning-time `call(...)` to create a `TableProvider`, and then plans the rest of the query against that provider’s schema and scan plan. (The APIs above are the stable seams; this is the only coherent interpretation of “UDTF returns a TableProvider used in the query plan.”) ([Docs.rs][1])

### G5.4 Extensibility hook for relation resolution: `RelationPlanner`

If you’re building a system that needs to intercept/override how SQL relations are planned (custom FROM factors, federated sources, virtual schemas), `SessionState` exposes:

* `relation_planners() -> &[Arc<dyn RelationPlanner>]` (priority order)
* `register_relation_planner(planner)` (new planners get **higher priority**) ([Docs.rs][1])

This is the “escape hatch” for custom SQL relation planning that sits adjacent to table functions and catalog resolution.

---

## G6) Dynamic registration via `CREATE FUNCTION` (FunctionFactory)

### G6.1 `FunctionFactory` (async, pluggable, no default)

`FunctionFactory` is the engine interface for handling `CREATE FUNCTION` statements. It is:

* `Debug + Send + Sync`
* async `create(&self, state: &SessionState, statement: CreateFunction) -> Future<Output = Result<RegisterFunction>>`
* **no default implementation provided** (by design; “requirements vary widely”) ([Docs.rs][9])

### G6.2 `RegisterFunction` result enum (what you return)

`RegisterFunction` variants:

* `Scalar(Arc<ScalarUDF>)`
* `Aggregate(Arc<AggregateUDF>)`
* `Window(Arc<WindowUDF>)`
* `Table(String, Arc<dyn TableFunctionImpl>)` ([Docs.rs][10])

This is the precise bridging mechanism for “SQL-defined function creation” → “register into the appropriate registry”.

### G6.3 Wiring into a session

`SessionState` exposes:

* `set_function_factory(function_factory: Arc<dyn FunctionFactory>)` (register handler for `CREATE FUNCTION`)
* `function_factory() -> Option<&Arc<dyn FunctionFactory>>` ([Docs.rs][1])

**Implication:** “CREATE FUNCTION” is not a magical core feature; it is a hook-point. You decide how functions are created (load from shared libs, JIT, remote UDFs, interpret DSL, etc.), then return `RegisterFunction` to integrate them. ([Docs.rs][9])

---

## G7) SQL-level discovery surfaces (what users/agents query)

### `SHOW FUNCTIONS` + LIKE filtering

DataFusion documents:

```sql
SHOW FUNCTIONS;
SHOW FUNCTIONS LIKE 'regexp%';
```

and ties it to `information_schema` views for richer metadata. ([Apache DataFusion][3])

### `information_schema` views

* `information_schema.information_schema.routines`: functions + descriptions
* `information_schema.information_schema.parameters`: parameters + descriptions ([Apache DataFusion][3])

Enablement: requires `datafusion.catalog.information_schema=true` (config). ([Apache DataFusion][4])

---

## G8) Current seam: UDTFs are *not* part of `FunctionRegistry`

Today:

* `FunctionRegistry` names/lookup cover scalar/aggregate/window + expr planners + function rewrites. ([Docs.rs][2])
* UDTFs live in `SessionState::table_functions` with separate `register_udtf/deregister_udtf` methods. ([Docs.rs][1])

There is explicit historical discussion about adding table functions into `FunctionRegistry` for discovery/unification, which is useful context if you’re designing an abstraction layer and wondering “why isn’t UDTF in the same registry?” ([GitHub][11])

---

## G9) Agent-oriented deployment checklist (high signal)

1. **Decide your registry boundary**

   * “Use `SessionContext` only” (most apps) vs “own `SessionState` and use `SessionStateBuilder`” (embedded engines, multi-tenant kernels). ([Docs.rs][1])

2. **Control builtin registration explicitly**

   * If you need a minimal function surface: skip `with_default_features()` and selectively apply `SessionStateDefaults` and/or function packages. ([Docs.rs][5])

3. **Make overwrites a conscious policy**

   * `with_default_features()` overwrites name collisions; treat this as either a feature (override builtins) or a footgun (accidental clobber). ([Docs.rs][5])

4. **Use the right discovery plane**

   * Programmatic: `FunctionRegistry::{udfs,udafs,udwfs}`; `SessionState::{scalar_functions,aggregate_functions,window_functions,table_functions}`. ([Docs.rs][2])
   * SQL: `SHOW FUNCTIONS` / `information_schema` (ensure enabled). ([Apache DataFusion][3])

5. **If you want SQL-defined functions, implement `FunctionFactory`**

   * Return `RegisterFunction::{Scalar,Aggregate,Window,Table}` and install into state via `set_function_factory`. ([Docs.rs][9])

If you want the next section after G, the highest-leverage continuation is **H) Custom expression planning + operator rewrites** (deep dive on `ExprPlanner`, `FunctionRewrite`, and how they interact with the coercion / rewrite phase in `create_physical_expr`).

[1]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html "SessionState in datafusion::execution::session_state - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/execution/trait.FunctionRegistry.html "FunctionRegistry in datafusion::execution - Rust"
[3]: https://datafusion.apache.org/user-guide/sql/information_schema.html?utm_source=chatgpt.com "Information Schema — Apache DataFusion documentation"
[4]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[5]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionStateBuilder.html "SessionStateBuilder in datafusion::execution::session_state - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/execution/struct.SessionStateDefaults.html "SessionStateDefaults in datafusion::execution - Rust"
[7]: https://docs.rs/datafusion-functions?utm_source=chatgpt.com "datafusion_functions - Rust"
[8]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html?utm_source=chatgpt.com "Adding User Defined Functions: Scalar/Window/Aggregate ..."
[9]: https://docs.rs/datafusion/latest/datafusion/execution/context/trait.FunctionFactory.html "FunctionFactory in datafusion::execution::context - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/execution/context/enum.RegisterFunction.html "RegisterFunction in datafusion::execution::context - Rust"
[11]: https://github.com/apache/arrow-datafusion/issues/7926?utm_source=chatgpt.com "Support User Defined Table Functions / Table Value ..."

# Section H — Custom expression planning + operator rewrites (ExprPlanner + FunctionRewrite) and their interaction with `create_physical_expr`

DataFusion exposes **two orthogonal extension planes** for “non-standard SQL semantics”:

1. **SQL AST → logical `Expr`**: implement + register an **`ExprPlanner`** (runs during `SqlToRel`, before type coercion/optimization). ([Apache DataFusion][1])
2. **logical `Expr` → canonical function calls**: implement + register **`FunctionRewrite`** rules (runs during physical expression construction, after type coercion, via bottom-up `transform_up`). ([Docs.rs][2])

These meet at `SessionState::create_physical_expr` / `SessionContext::create_physical_expr`, which **(a) coerces types** and **(b) applies function rewrites** before building a `PhysicalExpr`. ([Docs.rs][3])

---

## H0) Phase map (where each hook runs)

### H0.1 SQL planning phase (`SqlToRel`)

* DataFusion’s SQL planning extension system explicitly intercepts the SQL AST during **`SqlToRel`** and lets you customize AST→LogicalPlan translation using extension planners (ExprPlanner / TypePlanner / RelationPlanner). ([Apache DataFusion][1])
* Multiple `ExprPlanner`s can be registered; precedence is **reverse registration order** (“last registered wins”). Return `Original(...)` to delegate to next planner. ([Apache DataFusion][1])

### H0.2 Physical expression construction (Expr → PhysicalExpr)

`SessionState::create_physical_expr(expr, df_schema)` does, in-order:

1. `ExprSimplifier::coerce(expr, df_schema)`  (**type coercion**)
2. apply each `FunctionRewrite` from `self.analyzer.function_rewrites()` using **bottom-up** `expr.transform_up(...)`
3. call lower-level `datafusion_physical_expr::create_physical_expr(...)` ([Docs.rs][3])

**Implication:** function rewrites see **already-coerced** types (via `DFSchema`) and have access to `ConfigOptions`. ([Docs.rs][3])

---

## H1) `ExprPlanner` deep dive (custom SQL expressions/operators)

### H1.1 Core contract

`ExprPlanner` is a **planner-time hook**: “Customize planning of SQL AST expressions to `Expr`s.” It is `Debug + Send + Sync` and consists entirely of **provided methods** you can override. ([Docs.rs][4])

### H1.2 Return type: `PlannerResult<T>`

Every planner method returns `PlannerResult<T>`:

```rust
pub enum PlannerResult<T> { Planned(Expr), Original(T) }
```

* `Planned(Expr)`: you successfully replaced the raw construct with a new logical `Expr`.
* `Original(T)`: you did not handle it; DataFusion continues with the next planner / default planning. ([Docs.rs][5])

### H1.3 The “raw” inputs you plan (what’s actually passed to you)

#### `RawBinaryExpr` (operators)

```rust
pub struct RawBinaryExpr { pub op: BinaryOperator, pub left: Expr, pub right: Expr }
```

* `left/right` are already logical `Expr`s
* `op` is the **SQL AST** operator (from `sqlparser`) ([Docs.rs][6])

Other raw structures you may handle (selected):

* `RawFieldAccessExpr { field_access: GetFieldAccess, expr: Expr }` for `foo.bar` / `foo['k']`-style access. ([Docs.rs][7])
* `RawAggregateExpr`, `RawWindowExpr` for custom aggregate/window handling entrypoints. ([Docs.rs][4])
* `RawDictionaryExpr` for `{k:v,...}` literals; plus array/struct literal hooks. ([Docs.rs][4])

### H1.4 Full method inventory (what you can intercept)

`ExprPlanner` exposes 14 intercept points, including:

* Operators: `plan_binary_op`, `plan_any`
* Literals: `plan_array_literal`, `plan_dictionary_literal`, `plan_struct_literal`
* Functions: `plan_extract`, `plan_substring`, `plan_overlay`, `plan_position`, `plan_make_map`
* Identifiers: `plan_field_access`, `plan_compound_identifier`
* Aggregates/Windows: `plan_aggregate`, `plan_window` ([Apache DataFusion][1])

### H1.5 Registration surfaces + precedence

#### Register via SessionContext

Docs explicitly list `ctx.register_expr_planner(...)` as the registration method. ([Apache DataFusion][1])

#### Register via FunctionRegistry (common in “engine embedding”)

`FunctionRegistry` has `register_expr_planner(Arc<dyn ExprPlanner>)`. ([Docs.rs][8])

#### How it’s stored

`SessionState` stores planners in `expr_planners: Vec<Arc<dyn ExprPlanner>>` and `register_expr_planner` appends to that vec. ([Docs.rs][3])

**Precedence rule:** official docs: planners are invoked in **reverse registration order** (“last registered wins”). ([Apache DataFusion][1])

### H1.6 Canonical usage patterns (what to emit from ExprPlanner)

**Pattern A: emit a canonical function call immediately**

* If your custom syntax is fundamentally “call some function”, emit `ScalarUDF::call(args)` (or DataFusion’s `expr_fn` helpers) directly from the planner.
* Benefit: avoids downstream dependence on rewrite passes.

**Pattern B: emit a canonical DataFusion operator**

* If DataFusion already has a logical operator you want (e.g., `Operator::StringConcat`), emit `Expr::BinaryExpr` and rely on later rewrite/physical planner support.

**Pattern C: emit a placeholder/alias purely for display stability**

* If you care about preserving “original syntax” in explain/display, wrap in an alias (common in optimizer contexts; same mechanism used widely in DataFusion expression rewriting workflows). ([Apache DataFusion][9])

---

## H2) `FunctionRewrite` deep dive (operator→function translation)

### H2.1 Contract

`FunctionRewrite` lives in `datafusion_expr::expr_rewriter` and is **explicitly for rewriting `Expr`s into function calls**. ([Docs.rs][2])

```rust
pub trait FunctionRewrite: Debug {
  fn name(&self) -> &str;
  fn rewrite(&self, expr: Expr, schema: &DFSchema, config: &ConfigOptions)
    -> Result<Transformed<Expr>>;
}
```

Critical semantics:

* `rewrite` is **non-recursive**: “recursion is handled by the caller – this method should only handle `expr`, not recurse to its children.” ([Docs.rs][2])
* Returns `Transformed<Expr>`; use `Transformed::yes(new_expr)` / `Transformed::no(expr)` (documented usage pattern). ([Apache DataFusion][9])

### H2.2 Registration + storage

* Register via `FunctionRegistry::register_function_rewrite(Arc<dyn FunctionRewrite + Send + Sync>)`. ([Docs.rs][8])
* In `SessionState`’s `FunctionRegistry` impl, `register_function_rewrite` forwards to `self.analyzer.add_function_rewrite(rewrite)` (i.e., rewrites are tracked under the Analyzer and later consumed by `create_physical_expr`). ([Docs.rs][3])

### H2.3 When rewrites run (exact placement)

`SessionState::create_physical_expr`:

1. coerces types
2. iterates `self.analyzer.function_rewrites()`
3. for each rewrite, runs:
   `expr = expr.transform_up(|expr| rewrite.rewrite(expr, df_schema, config_options))?.data;`
4. then builds physical expr. ([Docs.rs][3])

Thus:

* traversal is **bottom-up (post-order)** (`transform_up`), so your rewrite sees children already rewritten. ([Docs.rs][10])
* rewrites see **coerced** expression types (because coercion runs first). ([Docs.rs][3])

### H2.4 What rewrites are for (explicitly)

Docs give the canonical example: rewrite operators into function calls, e.g. `a || b` → `array_concat(a,b)`, enabling operator behavior to be customized / implemented via extension function packages. ([Docs.rs][8])

---

## H3) `create_physical_expr` integration (coercion + rewrites are not optional in custom pipelines)

### H3.1 Two APIs, different guarantees

#### “Safe” API: `SessionContext::create_physical_expr` / `SessionState::create_physical_expr`

* Guaranteed to apply **type coercion** and **function rewrites**; docs explicitly call this out. ([Docs.rs][11])

#### “Low-level” API: `datafusion_physical_expr::create_physical_expr`

* Creates a physical expr from an `Expr`, but does **not** itself promise coercion/rewrites; downstream users have explicitly flagged that it assumes coercion already happened. ([Docs.rs][12])

### H3.2 Why this matters (real failure mode)

DataFusion has had regressions where physical planning hits an internal unreachable: “NamedStructField should be rewritten in OperatorToFunction” when rewrites weren’t applied in a specific subquery shape. This is a concrete signal that **some logical expression forms require rewrite passes before physical planning**. ([GitHub][13])

### H3.3 Minimal “custom physical planning” recipe (do not skip)

If you’re constructing `PhysicalExpr` outside of the normal planner (custom engines, custom physical nodes), do either:

* call `ctx.create_physical_expr(expr, df_schema)` (preferred), or
* replicate SessionState’s pipeline: `simplifier.coerce` → apply all `FunctionRewrite`s via `transform_up` → call low-level `create_physical_expr`. ([Docs.rs][3])

---

## H4) End-to-end examples (ExprPlanner + FunctionRewrite)

### Example 1 — Custom SQL operator (`->`) via ExprPlanner (planner-time)

DataFusion’s docs show mapping the Postgres `->` operator by implementing `plan_binary_op` and returning `PlannerResult::Planned(...)`, with dialect set to `postgres` to enable parsing. ([Apache DataFusion][1])

**Template (emit canonical function call):**

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion_common::Result;
use sqlparser::ast::BinaryOperator;

// suppose you already registered a ScalarUDF "json_get" (or similar)
#[derive(Debug)]
struct JsonOpPlanner {
    json_get: Arc<datafusion::logical_expr::ScalarUDF>,
}

impl ExprPlanner for JsonOpPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &datafusion_common::DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        match &expr.op {
            BinaryOperator::Arrow => {
                // Convert `a -> b` into `json_get(a, b)`
                let planned = self.json_get.call(vec![expr.left.clone(), expr.right.clone()]);
                Ok(PlannerResult::Planned(planned))
            }
            _ => Ok(PlannerResult::Original(expr)),
        }
    }
}

// ctx.register_expr_planner(Arc::new(JsonOpPlanner{...}))  // planner registration
```

**Notes (mechanics to respect):**

* `RawBinaryExpr.op` is the SQL AST operator, while `left/right` are DataFusion `Expr`s. ([Docs.rs][6])
* Delegate by returning `PlannerResult::Original(expr)`. ([Docs.rs][5])
* Precedence is last-registered-wins (reverse registration). ([Apache DataFusion][1])

### Example 2 — Operator→function mapping via FunctionRewrite (rewrite-time)

Use this when DataFusion already produces a logical operator but you want to “rebind” it to a function implementation (e.g., extension function packages), or when certain logical forms must be rewritten before physical planning. ([Docs.rs][2])

```rust
use std::sync::Arc;
use datafusion_common::{Result, DFSchema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{Expr, BinaryExpr, Operator};
use datafusion::logical_expr::ScalarUDF;

#[derive(Debug)]
struct ArrayConcatRewrite {
    array_concat: Arc<ScalarUDF>,
}

impl FunctionRewrite for ArrayConcatRewrite {
    fn name(&self) -> &str { "array_concat_operator_rewrite" }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        Ok(match expr {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::ArrowAt, right }) => {
                // a || b  (array concat operator form)  ->  array_concat(a, b)
                let planned = self.array_concat.call(vec![*left, *right]);
                Transformed::yes(planned)
            }
            other => Transformed::no(other),
        })
    }
}

// registration: registry.register_function_rewrite(Arc::new(ArrayConcatRewrite{...}))?
```

**Rules to follow:**

* `rewrite` must **not recurse**; engine applies `transform_up` externally. ([Docs.rs][2])
* Use `Transformed::yes/no` patterns (documented). ([Apache DataFusion][9])

### Example 3 — Show exact coercion→rewrite→physical pipeline (what you inherit for free)

SessionState implementation is explicit:

* `simplifier.coerce(expr, df_schema)?;`
* apply `function_rewrites()` via `expr.transform_up(...)`
* then `datafusion_physical_expr::create_physical_expr(...)` ([Docs.rs][3])

This is why **registering function rewrites** is the supported way to ensure operator→function translation participates in physical expr creation. ([Docs.rs][3])

---

## H5) Decision matrix (choose the right extension hook)

| Requirement                                                                                                               | Use                                                     | Rationale                                                                                                                             |
| ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| SQL syntax not supported / dialect operator (`->`, `?`, custom field access)                                              | `ExprPlanner`                                           | Runs in `SqlToRel`, can reinterpret SQL AST nodes into any logical `Expr`. ([Apache DataFusion][1])                                   |
| Operator already parsed into a logical `Expr` but you want different semantics or an implementation via function packages | `FunctionRewrite`                                       | Explicitly designed for rewriting operators into function calls; applied after coercion during physical expr creation. ([Docs.rs][2]) |
| You build physical exprs manually                                                                                         | Use `SessionContext/SessionState::create_physical_expr` | Ensures coercion + rewrites; low-level API may miss coercion and can hit rewrite-dependent panics. ([Docs.rs][11])                    |

---

## H6) Testing hooks (tie into your earlier harness patterns)

**sqllogictest**: best surface-area regression coverage for planner extensions (ExprPlanner) and rewrite behavior (FunctionRewrite) because it exercises SQL parsing + planning end-to-end. (You already have the harness patterns.)
**Plan snapshots**: add `EXPLAIN FORMAT tree` tests to ensure your custom operator resolves into the expected canonical expression/function form and doesn’t regress when planner internals change.

(If you want, next I can produce a “drop-in test pack” for this section: `expr_planner.rs` unit tests + a `sqllogictest` file that validates both planner precedence and rewrite application via `EXPLAIN`.)

[1]: https://datafusion.apache.org/library-user-guide/extending-sql.html "Extending SQL Syntax — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr_rewriter/trait.FunctionRewrite.html "FunctionRewrite in datafusion_expr::expr_rewriter - Rust"
[3]: https://docs.rs/datafusion/latest/src/datafusion/execution/session_state.rs.html "session_state.rs - source"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.ExprPlanner.html "ExprPlanner in datafusion::logical_expr::planner - Rust"
[5]: https://docs.rs/datafusion-expr/latest/datafusion_expr/planner/enum.PlannerResult.html?utm_source=chatgpt.com "PlannerResult in datafusion_expr::planner - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/struct.RawBinaryExpr.html "RawBinaryExpr in datafusion::logical_expr::planner - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/struct.RawFieldAccessExpr.html?utm_source=chatgpt.com "RawFieldAccessExpr in datafusion::logical_expr::planner"
[8]: https://docs.rs/datafusion/latest/datafusion/execution/trait.FunctionRegistry.html "FunctionRegistry in datafusion::execution - Rust"
[9]: https://datafusion.apache.org/library-user-guide/working-with-exprs.html?utm_source=chatgpt.com "Working with Expr s - Apache DataFusion"
[10]: https://docs.rs/datafusion/latest/datafusion/common/tree_node/trait.TreeNode.html?utm_source=chatgpt.com "TreeNode in datafusion::common::tree_node - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html?utm_source=chatgpt.com "SessionContext in datafusion::execution::context - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/physical_expr/planner/fn.create_physical_expr.html?utm_source=chatgpt.com "create_physical_expr in datafusion::physical_expr::planner"
[13]: https://github.com/apache/datafusion/issues/10029?utm_source=chatgpt.com "Regression: Error in `NamedStructField should be rewritten ..."

# Section I — `CREATE FUNCTION` and dynamic function factories (SQL-defined wiring)

## I0) Mental model: `CREATE FUNCTION` is a parser feature + a pluggable *compiler*

DataFusion parses `CREATE FUNCTION ...` into a **DataFusion-owned AST** (`CreateFunction`) and then delegates the “turn this into something executable and register it” step to a user-supplied **`FunctionFactory`**. The factory is the *only* component that can make `CREATE FUNCTION` do anything meaningful; **DataFusion ships no default implementation** because requirements vary widely. ([Docs.rs][1])

---

## I1) FunctionFactory surface (what exists; what you implement)

### I1.1 `FunctionFactory` trait (async “compiler” hook)

**Module:** `datafusion::execution::context::FunctionFactory`
**Trait bounds:** `Debug + Send + Sync`
**Core method:** async-ish `create(&self, state: &SessionState, statement: CreateFunction) -> Future<Output=Result<RegisterFunction>>` ([Docs.rs][1])

Key properties:

* Called with an **immutable** `&SessionState` (access session config/catalogs/runtime env) and an owned `CreateFunction` statement payload. ([Docs.rs][1])
* Returns `RegisterFunction` which *declares the kind* of function to register (Scalar/Aggregate/Window/Table). ([Docs.rs][2])
* No builtin factory: if you don’t configure one, `CREATE FUNCTION` cannot be executed. ([Docs.rs][1])

**Documented “syntaxes a factory can support”** (i.e., SQL forms the parser can represent; semantics are yours):

```sql
CREATE FUNCTION f1(BIGINT)
  RETURNS BIGINT
  RETURN $1 + 1;

CREATE FUNCTION to_miles(DOUBLE)
RETURNS DOUBLE
LANGUAGE PYTHON
AS '... python ...';
```

([Docs.rs][1])

---

### I1.2 `RegisterFunction` enum (factory output = registry intent)

**Module:** `datafusion::execution::context::RegisterFunction`

Variants:

* `Scalar(Arc<ScalarUDF>)`
* `Aggregate(Arc<AggregateUDF>)`
* `Window(Arc<WindowUDF>)`
* `Table(String, Arc<dyn TableFunctionImpl>)` ([Docs.rs][2])

This is how your factory tells DataFusion “install this into the scalar registry” vs “this is a UDTF” (table-valued function). ([Docs.rs][2])

---

### I1.3 `CreateFunction` payload (DataFusion-owned AST)

**Module:** `datafusion::logical_expr::CreateFunction`

Fields (the exact “command surface” you can interpret):

* `or_replace: bool` — `OR REPLACE`
* `temporary: bool` — `TEMPORARY`
* `name: String` — function name
* `args: Option<Vec<OperateFunctionArg>>` — typed args + defaults
* `return_type: Option<DataType>` — `RETURNS ...` (optional)
* `params: CreateFunctionBody` — language/volatility/body
* `schema: Arc<DFSchema>` — “dummy schema” carried with the statement ([Docs.rs][3])

Notes:

* DataFusion intentionally mirrors sqlparser’s `Statement::CreateFunction` shape but uses its own struct to avoid tying core to sqlparser. ([Docs.rs][3])

---

### I1.4 `OperateFunctionArg` (arg typing + defaults; what you can support)

**Module:** `datafusion_expr::logical_plan::OperateFunctionArg` (used inside `CreateFunction.args`)

Fields:

* `name: Option<Ident>` — optional arg name (SQL identifier)
* `data_type: DataType` — arg type
* `default_expr: Option<Expr>` — default value expression (if present) ([Docs.rs][4])

Practical implication: if you want SQL macros / scripting-like definitions with optional args, the AST can carry both **arg names** and **default expressions**, but you must implement the binding rules in your factory or in the emitted UDF implementation. ([Docs.rs][4])

---

### I1.5 `CreateFunctionBody` (language + volatility + body)

**Module:** `datafusion::logical_expr::CreateFunctionBody`

Fields:

* `language: Option<Ident>` — `LANGUAGE lang_name`
* `behavior: Option<Volatility>` — `IMMUTABLE | STABLE | VOLATILE`
* `function_body: Option<Expr>` — `RETURN ...` or `AS ...` body (represented as a logical `Expr`) ([Docs.rs][5])

This is the **main branch point** in a multi-language factory:

* `language=None` + `function_body=Some(expr)` → treat as “SQL expression macro”
* `language=Some("python")` + `function_body` likely contains a literal payload expression you interpret (e.g., string literal → code blob), etc. ([Docs.rs][5])

---

### I1.6 Enablement / wiring: configure the factory on the session

Two equivalent registration surfaces:

**(A) `SessionState` (low-level)**

* `set_function_factory(Arc<dyn FunctionFactory>)`
* `function_factory() -> Option<&Arc<dyn FunctionFactory>>` ([Docs.rs][6])

**(B) `SessionContext` (high-level)**

* `with_function_factory(self, Arc<dyn FunctionFactory>) -> Self` (“Registers a FunctionFactory to handle `CREATE FUNCTION` statements”) ([Docs.rs][7])

---

## I2) Current reality / “why it errors in practice”

If no factory is configured, `CREATE FUNCTION` will parse but fail at execution/planning time with:

> `Invalid or Unsupported Configuration: Function factory has not been configured` ([GitHub][8])

This shows up in `datafusion-cli` because the CLI does not ship with a configured factory by default; the DataFusion maintainers confirm the *framework exists* but is otherwise not implemented in core/CLI “to the best of my knowledge,” and point to the **function_factory example** and tests as reference implementations. ([GitHub][8])

---

## I3) Reference pattern: SQL macros via `ScalarUDFImpl::simplify` (canonical “factory” use-case)

The DataFusion example `datafusion-examples/examples/function_factory.rs` is the clearest “how to actually make CREATE FUNCTION useful” pattern:

### I3.1 Configure context with factory

```rust
let ctx = SessionContext::new()
  .with_function_factory(Arc::new(CustomFunctionFactory::default()));
```

Then execute:

```sql
CREATE FUNCTION f1(BIGINT) RETURNS BIGINT RETURN $1 + 1
```

and later:

```sql
SELECT f2(1, 2)
DROP FUNCTION f1
```

([Apache Git Repositories][9])

### I3.2 Factory returns a `ScalarUDF` whose *runtime invoke is unreachable*

The example’s wrapper `ScalarUDFImpl`:

* stores `name`, an expression template (`Expr`) containing placeholders like `$1`, a `Signature`, and an explicit return type
* implements `invoke` as “should never run”
* implements `simplify(args)` to **rewrite `f(x,y,...)` into the stored expression with placeholders substituted** ([Apache Git Repositories][9])

This is the *SQL macro* architecture:

* **`CREATE FUNCTION` stores an expression template**
* **calls** are replaced by the expanded expression during simplification/optimization
* execution runs the expanded expression, not a custom kernel ([Apache Git Repositories][9])

### I3.3 Placeholder binding via `Expr` tree transform

The example replaces `Expr::Placeholder("$k")` nodes with the corresponding call arg:

* parse placeholder id → index (`$1`→0)
* substitute or raise an execution error if missing ([Apache Git Repositories][9])

---

## I4) Production-grade FunctionFactory design checklist (what to implement beyond the demo)

### I4.1 Parsing + validation pipeline (factory front-end)

Given `CreateFunction`:

1. **Name policy**: normalize vs preserve quoting (you decide; DataFusion registries typically lookup unquoted names lowercased).
2. **Arity/type validation**:

   * validate `args` presence, each `OperateFunctionArg.data_type`, and whether `default_expr` is allowed for your language/semantics. ([Docs.rs][4])
3. **Return typing policy**:

   * require `return_type` for scalar/window/aggregate definitions (or infer if your language allows inference)
4. **Volatility policy**:

   * use `CreateFunctionBody.behavior` if provided; otherwise default conservatively (example defaults to `Volatile` when absent). ([Docs.rs][5])
5. **Language dispatch**:

   * `CreateFunctionBody.language` selects the compiler/interpreter (SQL macro, python, wasm, dylib, etc.). ([Docs.rs][5])
6. **Body decoding**:

   * interpret `function_body: Expr` according to your language: expression AST (SQL macro) vs literal payload (e.g., `AS '...code...'`). ([Docs.rs][5])

### I4.2 Emitting the correct function kind (`RegisterFunction`)

Your factory must decide which variant to return:

* `Scalar` / `Aggregate` / `Window`: return the respective UDF wrapper. ([Docs.rs][2])
* `Table(name, TableFunctionImpl)`: for UDTFs; note the variant carries an explicit name string + the impl object (you can choose to register under `statement.name` or a derived name). ([Docs.rs][2])

### I4.3 State + persistence (non-core)

DataFusion core does **not** prescribe persistence for created functions; `FunctionFactory` is the extension point. If you want persistence across sessions (or per-catalog schemas), implement it:

* store definitions in your metastore
* on startup, replay into the session registry (or lazy-load in `create`). (Design inference; not a DataFusion guarantee.)

### I4.4 Security/tenancy (the real reason there is no default factory)

Because the docs explicitly show `LANGUAGE PYTHON AS '...'`, a factory is inherently a code-loading boundary (could be untrusted). DataFusion intentionally avoids shipping a default implementation. ([Docs.rs][1])

---

## I5) Minimal “modern API” skeleton (DataFusion 52.x shape)

Below is a *shape-correct* skeleton for a **SQL-macro factory** aligned to the current `CreateFunctionBody.function_body` field names (note: older examples used different field names; adjust accordingly per your DataFusion version).

```rust
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::{FunctionFactory, RegisterFunction};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{CreateFunction, Volatility};
use datafusion::common::{Result, DataFusionError};
use datafusion_expr::{Expr, ScalarUDF, ScalarUDFImpl, Signature, ExprSimplifyResult};
use datafusion_common::tree_node::{TreeNode, Transformed};

#[derive(Debug, Default)]
struct MacroFactory;

#[async_trait]
impl FunctionFactory for MacroFactory {
    async fn create(&self, _state: &SessionState, stmt: CreateFunction) -> Result<RegisterFunction> {
        let body_expr = stmt
            .params
            .function_body
            .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION missing body".into()))?;

        let ret = stmt
            .return_type
            .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION missing return type".into()))?;

        let arg_types = stmt
            .args
            .unwrap_or_default()
            .into_iter()
            .map(|a| a.data_type)
            .collect::<Vec<_>>();

        let volatility = stmt.params.behavior.unwrap_or(Volatility::Volatile);

        let udf_impl = SqlMacroUdf {
            name: stmt.name,
            template: body_expr,
            signature: Signature::exact(arg_types, volatility),
            return_type: ret,
        };

        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(udf_impl))))
    }
}

#[derive(Debug)]
struct SqlMacroUdf {
    name: String,
    template: Expr,
    signature: Signature,
    return_type: arrow_schema::DataType,
}

impl ScalarUDFImpl for SqlMacroUdf {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _arg_types: &[arrow_schema::DataType]) -> Result<arrow_schema::DataType> {
        Ok(self.return_type.clone())
    }

    // Macro pattern: never invoked; always simplified
    fn invoke(&self, _args: &[datafusion_expr::ColumnarValue]) -> Result<datafusion_expr::ColumnarValue> {
        Err(DataFusionError::Internal("macro udf should not invoke".into()))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn datafusion_expr::simplify::SimplifyInfo)
        -> Result<ExprSimplifyResult>
    {
        // Replace $k placeholders with args[k-1]
        let out = self.template.clone().transform(&|e| {
            Ok(match e {
                Expr::Placeholder(p) => {
                    let idx = p.id.strip_prefix('$')
                        .and_then(|s| s.parse::<usize>().ok())
                        .ok_or_else(|| DataFusionError::Plan(format!("bad placeholder {}", p.id)))?
                        .saturating_sub(1);

                    if idx < args.len() { Transformed::yes(args[idx].clone()) }
                    else { return Err(DataFusionError::Plan(format!("missing arg {}", p.id))); }
                }
                _ => Transformed::no(e),
            })
        })?.data;

        Ok(ExprSimplifyResult::Simplified(out))
    }
}
```

The binding + `simplify` approach is exactly the strategy used in DataFusion’s published `function_factory.rs` example (macro-like expansion + placeholder replacement), but updated to match the current `CreateFunctionBody.function_body` field naming. ([Apache Git Repositories][9])

---

If you want the next section after I, the natural continuation is **J) Documentation + UX surfaces** for user-defined/dynamic functions (how to surface your dynamically created functions in `SHOW FUNCTIONS` / `information_schema`, and how to attach `Documentation` metadata so SQL help tooling remains usable).

[1]: https://docs.rs/datafusion/latest/datafusion/execution/context/trait.FunctionFactory.html "FunctionFactory in datafusion::execution::context - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/execution/context/enum.RegisterFunction.html "RegisterFunction in datafusion::execution::context - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.CreateFunction.html "CreateFunction in datafusion::logical_expr - Rust"
[4]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.OperateFunctionArg.html "OperateFunctionArg in datafusion_expr::logical_plan - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.CreateFunctionBody.html "CreateFunctionBody in datafusion::logical_expr - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html "SessionState in datafusion::execution::session_state - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[8]: https://github.com/apache/datafusion/discussions/12848 "What is the status of \"CREATE FUNCTION\"? · apache datafusion · Discussion #12848 · GitHub"
[9]: https://apache.googlesource.com/datafusion/%2B/refs/tags/37.1.0-rc2/datafusion-examples/examples/function_factory.rs "datafusion-examples/examples/function_factory.rs - datafusion - Git at Google"

# Section J — Documentation + UX surfaces for user-defined / dynamic functions (DataFusion)

This section is the *plumbing + contracts* you need so that **compiled** and **SQL-defined** functions are discoverable via `SHOW FUNCTIONS` / `information_schema` and expose machine-readable docs for CLIs, agents, and UIs.

---

## J0) Enable the discovery surfaces (`information_schema` gate)

DataFusion’s SQL metadata surfaces are gated by **`datafusion.catalog.information_schema`** (default `false`). When enabled, DataFusion “provide[s] access to `information_schema` virtual tables for displaying schema information.” ([Apache DataFusion][1])

### Enable via SQL

```sql
SET datafusion.catalog.information_schema = true;
```

This is also the switch that makes `SHOW TABLES` usable; otherwise DataFusion can error that SHOW commands are not supported unless `information_schema` is enabled (shown in a DataFusion issue thread). ([GitHub][2])

### Enable via Rust

```rust
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;

let cfg = SessionConfig::new()
    .set_bool("datafusion.catalog.information_schema", true);

let ctx = SessionContext::new_with_config(cfg);
```

`SessionConfig` is the configuration object passed to `SessionContext::new_with_config`. ([Docs.rs][3])

---

## J1) SQL UX surfaces: `SHOW FUNCTIONS` + `information_schema` views

### J1.1 `SHOW FUNCTIONS` (primary “human + agent” UX)

Syntax:

```sql
SHOW FUNCTIONS [ LIKE <pattern> ];
```

and it is explicitly documented as backed by:

* `information_schema.information_schema.routines` (functions + descriptions)
* `information_schema.information_schema.parameters` (parameters + descriptions) ([Apache DataFusion][4])

The documented example output includes (at least) these columns:

* `function_name`
* `return_type`
* `parameters` (names)
* `parameter_types`
* `function_type`
* `description`
* `syntax_example` ([Apache DataFusion][4])

Operationally, this means **overloads** show up as multiple rows (e.g., `datetrunc` appears many times for different parameter_types/return_type). ([Apache DataFusion][4])

### J1.2 Query the underlying views directly (more stable than parsing SHOW output)

For programmatic consumption, prefer direct SQL:

```sql
SELECT * FROM information_schema.information_schema.routines;
SELECT * FROM information_schema.information_schema.parameters;
```

These are the explicitly documented backing views for `SHOW FUNCTIONS`. ([Apache DataFusion][4])

---

## J2) Documentation object model: `Documentation`, `DocumentationBuilder`, `DocSection`

### J2.1 `Documentation` (what populates function docs)

`Documentation` is the canonical metadata payload “for use by `ScalarUDFImpl`, `AggregateUDFImpl` and `WindowUDFImpl` functions.” It is used to generate DataFusion’s SQL function documentation; the function’s name is pulled from the corresponding `*UDFImpl::name`. ([Docs.rs][5])

**Field contract (what you can supply):**

* `doc_section: DocSection` (where it appears)
* `description: String` (**required**)
* `syntax_example: String` (**required**, e.g. `ascii(str)`)
* `sql_example: Option<String>` (recommended prompt-style example)
* `arguments: Option<Vec<(String, String)>>` (ordered; name + description)
* `alternative_syntax: Option<Vec<String>>`
* `related_udfs: Option<Vec<String>>` (must match names; must be same UDF type for correct linking) ([Docs.rs][5])

**Formatting constraints:**

* All strings are required to be **markdown**.
* Documentation is currently **single-language**, so text should be English. ([Docs.rs][5])

### J2.2 `DocumentationBuilder` (the authoring API)

You typically build docs via:

```rust
Documentation::builder(doc_section, description, syntax_example)
```

and then attach options:

* `with_sql_example(...)`
* `with_argument(name, description)` (ordered)
* `with_standard_argument(name, expression_type)` (canonical “expression argument” boilerplate)
* `with_alternative_syntax(...)`
* `with_related_udf(...)`
* `build()` (**panics** if `doc_section`, `description`, or `syntax_example` missing). ([Docs.rs][6])

### J2.3 `DocSection` (routing + public/private doc inclusion)

`DocSection` is a small struct:

* `include: bool` (include in public docs)
* `label: &'static str` (display label)
* `description: Option<&'static str>` ([Docs.rs][7])

`DocSection::default()` is explicitly “suitable for user defined functions that do not appear in the DataFusion documentation.” ([Docs.rs][7])

---

## J3) Attaching docs to UDFs (compiled Rust functions)

### J3.1 Trait hook: `documentation() -> Option<&Documentation>`

All three expression-function traits expose a `documentation()` hook:

* `ScalarUDFImpl::documentation()` ([Docs.rs][8])
* `AggregateUDFImpl::documentation()` ([Docs.rs][9])
* `WindowUDFImpl::documentation()` ([Docs.rs][10])

And the logical wrappers expose pass-through accessors:

* `ScalarUDF::documentation()` ([Docs.rs][11])
* `AggregateUDF::documentation()` ([Docs.rs][12])
* `WindowUDF::documentation()` ([Docs.rs][13])

**Canonical implementation pattern (static docs stored on the impl):**

````rust
use std::any::Any;
use datafusion_expr::{Documentation, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr::DocSection; // re-exported (depends on datafusion-doc)

#[derive(Debug)]
struct MyUdf {
    sig: Signature,
    doc: Documentation,
}

impl MyUdf {
    fn new() -> Self {
        let doc = Documentation::builder(
                DocSection::default(),
                "Computes foo(x) with bar semantics.",
                "foo(x)"
            )
            .with_standard_argument("x", Some("Any"))
            .with_sql_example(r#"```sql
SELECT foo(1);
----
2
```"#)
            .build();

        Self {
            sig: Signature::any(1, Volatility::Immutable),
            doc,
        }
    }
}

impl ScalarUDFImpl for MyUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "foo" }
    fn signature(&self) -> &Signature { &self.sig }
    fn return_type(&self, _arg_types: &[arrow_schema::DataType]) -> datafusion_common::Result<arrow_schema::DataType> {
        Ok(arrow_schema::DataType::Int64)
    }
    fn invoke(&self, _args: &[datafusion_expr::ColumnarValue]) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        todo!()
    }
    fn documentation(&self) -> Option<&Documentation> {
        Some(&self.doc)
    }
}
````

(Builder API and required fields per docs above.) ([Docs.rs][6])

### J3.2 Attribute macro: `#[user_doc(...)]` (fastest way to keep docs consistent)

`datafusion_macros::user_doc` parses a custom attribute and constructs a `DocumentationBuilder` automatically; the resulting docs are returned from the `documentation()` method on the UDF traits. ([Docs.rs][14])

Concrete in-tree pattern (from a built-in function):

* annotate the UDF impl struct with `#[user_doc(...)]`
* implement `documentation()` as `self.doc()` (macro-generated) ([Apache Git Repositories][15])

This is the most “agent-proof” pattern because it keeps documentation colocated with the UDF definition and avoids hand-written builder glue.

---

## J4) Dynamic functions (`CREATE FUNCTION` / `FunctionFactory`) that remain discoverable

If you’re using `FunctionFactory` (SQL-defined wiring), you typically construct `ScalarUDF/AggregateUDF/WindowUDF` objects dynamically. For those to show up cleanly in `SHOW FUNCTIONS` / `information_schema.*`, you need to do **two** things:

### J4.1 Provide docs via `documentation()` on the generated `*UDFImpl`

Even for macro-style functions (where `invoke()` is unreachable and `simplify()` inlines an expression), you can still attach `Documentation` exactly the same way: store `Documentation` on the impl returned by your factory and implement `documentation()` to return it. ([Docs.rs][8])

**Factory-side doc synthesis recipe (high signal):**

* `doc_section`: `DocSection::default()` (or a stable label you control)
* `description`: if `CREATE FUNCTION` has a comment/metadata field in your system, use that; otherwise auto-generate “SQL macro: `<body>`”
* `syntax_example`: `${name}(${arg_names...})`
* `arguments`: from `OperateFunctionArg` (names + type strings) or the factory’s own arg model
* `sql_example`: auto-generate a 1-line example for deterministic literal invocations

All of those fields are first-class in `Documentation`. ([Docs.rs][5])

### J4.2 Set parameter names on `Signature` for named-arg UX + display

If you want:

* named argument calls (`f(a => 1, b => 2)`)
* human-friendly parameter lists in `SHOW FUNCTIONS`
  …then set `Signature::with_parameter_names(...)`. It errors if the name list doesn’t match arity and cannot be used for variable-arity signatures. ([Docs.rs][16])

This is especially important for dynamically created functions where argument names are present in the SQL DDL and you want them reflected consistently.

---

## J5) Known limitations / footguns (important for “dynamic + metadata”)

### J5.1 `information_schema` is off by default → `SHOW FUNCTIONS` is effectively “feature gated”

You must enable `datafusion.catalog.information_schema` or SHOW/info_schema may not be available. ([Apache DataFusion][1])

### J5.2 Return type display may lag newer UDF typing APIs

There is an active issue noting that DataFusion’s `information_schema` implementation “only handle[s] the function which implemented [`ScalarUDFImpl::return_type`]” and should prefer `return_field_from_args` (newer, metadata-aware typing). ([GitHub][17])

**Practical mitigation (until resolved):**

* implement `return_type` even if you primarily use `return_field_from_args` (mirror the same type logic), so metadata surfaces can compute return_type reliably. (The issue indicates current behavior; this mitigation is the obvious workaround.)

### J5.3 `DocumentationBuilder::build()` panics if required fields missing

If you construct docs dynamically, fail closed (validate) before calling `build()`, or use `#[user_doc]` for compile-time safety. ([Docs.rs][6])

### J5.4 Markdown + English-only constraints

Your doc payload must be markdown and is currently single-language. ([Docs.rs][5])

---

## J6) Validation & regression tests (keep docs usable for agents)

DataFusion explicitly recommends `sqllogictest` because it is easy to maintain and supports auto-updating expected outputs. ([Apache DataFusion][18])

**Minimal doc-surface test pack (what to assert):**

1. Enable info_schema: `SET datafusion.catalog.information_schema = true;` ([Apache DataFusion][1])
2. Register your UDFs dynamically or compiled.
3. Assert `SHOW FUNCTIONS LIKE '%foo%'` contains:

   * `description` and `syntax_example`
   * correct `parameter_types` and `return_type` (and overload row counts) ([Apache DataFusion][4])
4. Query the underlying views directly:

   * `SELECT * FROM information_schema.information_schema.routines WHERE function_name='foo'`
   * `SELECT * FROM information_schema.information_schema.parameters WHERE function_name='foo'` ([Apache DataFusion][4])

That gives you a stable guardrail for both **human UX** (CLI) and **agent UX** (structured metadata ingestion).

---

If you want to continue the sequence, the next “agent-facing” section after J is typically **K) Performance + determinism patterns for UDFs**, but we can also do a narrower follow-on specifically on **“doc automation for FunctionFactory-created functions”** (generating `Documentation` + parameter names from `CreateFunction` AST, plus an sqllogictest harness template).

[1]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[2]: https://github.com/apache/arrow-datafusion/issues/4850?utm_source=chatgpt.com "Support `select .. from 'data.parquet'` files in SQL from any ` ..."
[3]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionConfig.html?utm_source=chatgpt.com "SessionConfig in datafusion::execution::context - Rust"
[4]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[5]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.Documentation.html "Documentation in datafusion_expr - Rust"
[6]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.DocumentationBuilder.html?utm_source=chatgpt.com "DocumentationBuilder in datafusion_expr - Rust"
[7]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.DocSection.html "DocSection in datafusion_expr - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html?utm_source=chatgpt.com "ScalarUDFImpl in datafusion::logical_expr - Rust"
[9]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.AggregateUDFImpl.html?utm_source=chatgpt.com "AggregateUDFImpl in datafusion_expr - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.WindowUDFImpl.html?utm_source=chatgpt.com "WindowUDFImpl in datafusion::logical_expr - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.ScalarUDF.html?utm_source=chatgpt.com "ScalarUDF in datafusion::logical_expr - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.AggregateUDF.html?utm_source=chatgpt.com "AggregateUDF in datafusion::logical_expr - Rust"
[13]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.WindowUDF.html?utm_source=chatgpt.com "WindowUDF in datafusion::logical_expr - Rust"
[14]: https://docs.rs/datafusion-macros?utm_source=chatgpt.com "datafusion_macros - Rust"
[15]: https://apache.googlesource.com/datafusion/%2B/refs/tags/46.0.0/datafusion/functions-nested/src/map_values.rs "datafusion/functions-nested/src/map_values.rs - datafusion - Git at Google"
[16]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Signature.html?utm_source=chatgpt.com "Signature in datafusion::logical_expr - Rust"
[17]: https://github.com/apache/datafusion/issues/19870?utm_source=chatgpt.com "Information schema should use `return_field_from_args`"
[18]: https://datafusion.apache.org/contributor-guide/testing.html?utm_source=chatgpt.com "Testing — Apache DataFusion documentation"

# Doc automation for FunctionFactory-created functions

Goal: when your `FunctionFactory` turns a `CREATE FUNCTION` statement into a `ScalarUDF / AggregateUDF / WindowUDF`, you want **zero-manual** population of:

* `Signature` **parameter names** (so named-arg calls work + show up in `SHOW FUNCTIONS`)
* `Documentation` (so `SHOW FUNCTIONS` / `information_schema.*` expose descriptions, syntax examples, argument docs)

…and you want regression tests that verify those surfaces.

---

## J-F0) Inputs: what you get from the `CREATE FUNCTION` AST

`CreateFunction` carries everything you need to synthesize docs and signature metadata:

* `or_replace`, `temporary`, `name`
* `args: Option<Vec<OperateFunctionArg>>`
* `return_type: Option<DataType>`
* `params: CreateFunctionBody { language, behavior, function_body }` ([Docs.rs][1])

`OperateFunctionArg` exposes:

* `name: Option<Ident>`
* `data_type: DataType`
* `default_expr: Option<Expr>` ([Docs.rs][2])

`CreateFunctionBody` exposes:

* `language: Option<Ident>` (`LANGUAGE ...`)
* `behavior: Option<Volatility>` (`IMMUTABLE|STABLE|VOLATILE`)
* `function_body: Option<Expr>` (`RETURN ...` or `AS ...`) ([Docs.rs][3])

---

## J-F1) Parameter names: synthesize once, wire into `Signature`

### J-F1.1 Why this matters

* Named-arg calls only work if the function signature has parameter names.
* DataFusion’s binder resolves named args by **validating + reordering** call args to match the function’s parameter names (`resolve_function_arguments`). ([Docs.rs][4])

### J-F1.2 Constructing a named-arg `Signature`

Use `Signature::with_parameter_names(...)`:

* errors if name count ≠ arity
* **cannot** name params for variadic signatures (e.g., `Variadic`, `VariadicAny`) ([Docs.rs][5])

**Factory rule** (recommended): for SQL-defined functions, default to **fixed arity** + named params; only use variadic if you’re intentionally dropping named args.

### J-F1.3 Canonical synthesis algorithm

1. Take `stmt.args.unwrap_or_default()`.
2. Derive `param_names[i]`:

   * if `args[i].name.is_some()`: use that identifier (normalize case policy consistently with your engine)
   * else: generate stable synthetic names (`arg1`, `arg2`, …)
3. Derive `arg_types[i] = args[i].data_type.clone()`.
4. Create signature and attach names.

```rust
use datafusion_expr::{Signature, Volatility};
use datafusion_expr::logical_plan::OperateFunctionArg;

fn build_signature(
    args: &[OperateFunctionArg],
    volatility: Volatility,
) -> datafusion_common::Result<Signature> {
    let arg_types = args.iter().map(|a| a.data_type.clone()).collect::<Vec<_>>();
    let mut param_names = Vec::with_capacity(args.len());
    for (i, a) in args.iter().enumerate() {
        let n = a.name.as_ref()
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| format!("arg{}", i + 1));
        param_names.push(n);
    }

    Signature::exact(arg_types, volatility).with_parameter_names(param_names)
}
```

---

## J-F2) Documentation: synthesize `Documentation` from `CreateFunction` deterministically

### J-F2.1 What `SHOW FUNCTIONS` actually surfaces

`SHOW FUNCTIONS` is backed by:

* `information_schema.information_schema.routines` (functions + descriptions)
* `information_schema.information_schema.parameters` (parameters + descriptions)

and the output includes `description` and `syntax_example` columns (plus return/parameter types, etc.). ([Apache DataFusion][6])

### J-F2.2 Enable `information_schema` (otherwise you’re testing nothing)

`datafusion.catalog.information_schema` defaults to `false`; enable it to make the views (and thus `SHOW FUNCTIONS`) available. ([Apache DataFusion][7])

### J-F2.3 `Documentation` payload constraints

`Documentation` is:

* intended for `ScalarUDFImpl / AggregateUDFImpl / WindowUDFImpl`
* strings must be **Markdown**, currently English-only
* includes `description`, `syntax_example`, optional `sql_example`, `arguments`, etc. ([Docs.rs][8])

Build it using `Documentation::builder(...)` / `DocumentationBuilder` (builder fields and example are documented). ([Docs.rs][9])

Choose `DocSection::default()` for user-defined/dynamic functions that should not land in DataFusion’s public docs set (but still can power internal UX). ([Docs.rs][10])

### J-F2.4 Synthesis recipe (high-signal, low-churn)

Given `CreateFunction stmt`:

**DocSection**

* `DocSection::default()` unless you have your own curated sections. ([Docs.rs][10])

**description** (single paragraph; include machine-friendly tags)

* Include: origin (`CREATE FUNCTION`), volatility, language, and a short body synopsis (avoid dumping huge code blobs).
* Example:
  `SQL macro created via CREATE FUNCTION. volatility=IMMUTABLE. body=$1 + 1.`

**syntax_example** (what appears in SHOW FUNCTIONS)

* Use parameter names: `foo(a, b)` rather than types. This is exactly the UX shown in DataFusion’s `SHOW FUNCTIONS` output (`syntax_example` column). ([Apache DataFusion][6])

**arguments**

* For each arg: create a stable description string:

  * `Type: <DataType>`
  * if `default_expr`: append `Default: <expr-string>` (your renderer)
* Put these into `.with_argument(name, description)` in positional order. ([Docs.rs][8])

**sql_example** (optional but recommended)

* Provide a minimal snippet that’s always valid:

  * `SELECT foo(<literal1>, <literal2>);`
* If you can’t guarantee output deterministically, omit the output section and just show the query (still useful for agents). (Docs strongly recommend examples except for trivial UDFs.) ([Docs.rs][8])

````rust
use datafusion_doc::{DocSection, Documentation};
use datafusion_expr::logical_plan::OperateFunctionArg;
use datafusion::logical_expr::{CreateFunction, CreateFunctionBody};

fn build_docs(stmt: &CreateFunction, args: &[OperateFunctionArg]) -> Documentation {
    let param_names = args.iter().enumerate().map(|(i, a)| {
        a.name.as_ref().map(|id| id.value.clone()).unwrap_or_else(|| format!("arg{}", i+1))
    }).collect::<Vec<_>>();

    let syntax = format!("{}({})", stmt.name, param_names.join(", "));

    let language = stmt.params.language.as_ref().map(|i| i.value.as_str()).unwrap_or("SQL");
    let behavior = stmt.params.behavior.map(|v| format!("{v:?}")).unwrap_or_else(|| "VOLATILE".into());

    let body = stmt.params.function_body.as_ref().map(|e| e.to_string()).unwrap_or_else(|| "<none>".into());
    let desc = format!(
        "Created via `CREATE FUNCTION` (language={language}, volatility={behavior}).\n\nBody: `{}`",
        body.replace('`', "\\`")
    );

    let mut b = Documentation::builder(DocSection::default(), desc, syntax);

    for (i, a) in args.iter().enumerate() {
        let name = param_names[i].clone();
        let mut ad = format!("Type: `{}`.", a.data_type);
        if let Some(def) = &a.default_expr {
            ad.push_str(&format!(" Default: `{}`.", def.to_string().replace('`', "\\`")));
        }
        b = b.with_argument(name, ad);
    }

    // Optional, deterministic “how to call”
    let example = format!("```sql\nSELECT {};\n```", syntax.replace(", ", ", ").replace("arg", "1"));
    b.with_sql_example(example).build()
}
````

> Compile-time alternative for static functions: `#[user_doc(...)]` generates `DocumentationBuilder` code for you, but you can’t use it for runtime-defined functions. ([Docs.rs][11])
> Migration / codegen trick: `Documentation::to_doc_attribute()` can emit a Rust attribute form for semi-automated conversion to `#[user_doc]` later. ([Docs.rs][8])

---

## J-F3) Wiring docs + param names into the generated UDF impl

### J-F3.1 Store docs on the impl and return it from `documentation()`

Dynamic functions are just normal UDF impls at runtime: you attach docs by storing a `Documentation` and returning `Some(&self.doc)` from the UDF trait’s `documentation()` hook (Scalar/Aggregate/Window). The `Documentation` struct exists specifically for these traits. ([Docs.rs][8])

### J-F3.2 Ensure return types show in `information_schema` today

There is an open issue noting `information_schema` currently “only handle[s]” UDFs implementing `ScalarUDFImpl::return_type` (vs newer field-based typing). If you rely solely on `return_field_from_args`, your `SHOW FUNCTIONS`/routines view may be incomplete. Mitigation: **also implement `return_type`** consistently for dynamic functions. ([GitHub][12])

---

## J-F4) Named-arg calls “just work” if you set signature names

Once the signature has param names, named calls are resolved by the binder that validates/reorders arguments to match those names. ([Docs.rs][4])
So your FunctionFactory does **not** implement named-arg handling; it only emits a named-arg-capable signature.

---

## J-F5) sqllogictest harness template (prototype + complete mode) for doc surfaces

### J-F5.1 Why sqllogictest is the right tool here

DataFusion’s sqllogictest runner:

* runs `.slt` files
* supports `--complete` to auto-fill expected output
* runs each `.slt` file in an isolated `SessionContext` (so your test setup must register your FunctionFactory per context). ([Apache Git Repositories][13])

`--complete` workflow and record format (`statement ok`, `query ...`, `----`) are documented explicitly. ([Apache Git Repositories][13])

### J-F5.2 Prototype `.slt` script (commit this; let `--complete` fill outputs)

Create `test_files/create_function_docs.slt` as a **prototype**:

```sql
# enable information_schema surfaces
query
SET datafusion.catalog.information_schema = true;

# create a SQL macro (requires your SessionContext to have a FunctionFactory configured)
query
CREATE FUNCTION add_xy(a INT, b INT) RETURNS INT RETURN $1 + $2;

# SHOW FUNCTIONS row should include params + types + description + syntax_example
query
SHOW FUNCTIONS LIKE '%add_xy%';

# routines view: structured metadata backing SHOW FUNCTIONS
query
SELECT * FROM information_schema.information_schema.routines
WHERE function_name = 'add_xy';

# parameters view: per-argument metadata
query
SELECT * FROM information_schema.information_schema.parameters
WHERE specific_name = 'add_xy' OR function_name = 'add_xy';

# named-arg call (tests Signature::with_parameter_names + resolver)
query
SELECT add_xy(b => 2, a => 1);
```

Then generate expected output:

```sh
cargo test --test sqllogictests -- create_function_docs --complete
```

That exact `--complete` mode is the documented workflow; it rewrites your prototype into `statement ok`/`query <type_string>` records with expected results. ([Apache Git Repositories][13])

### J-F5.3 Runner requirement: make sure the SessionContext has your factory

If you run this inside DataFusion’s own sqllogictest harness, `CREATE FUNCTION` will error unless a FunctionFactory is configured (this is a known practical reality in CLI contexts). ([GitHub][14])
So your sqllogictest harness (or wrapper test binary) must instantiate `SessionContext` with your `with_function_factory(...)` and register it before executing the `.slt` file.

---

## J-F6) Agent checklist (what to enforce for “doc automation” PRs)

* [ ] FunctionFactory extracts `CreateFunction.{name,args,return_type,params}` and uses `OperateFunctionArg.{name,data_type,default_expr}` in synthesis. ([Docs.rs][1])
* [ ] Signature created with `.with_parameter_names(...)` (arity matches; fixed arity). ([Docs.rs][5])
* [ ] UDF impl stores `Documentation` and returns it via `documentation()`. Docs are markdown; include `description` + `syntax_example`; `DocSection::default()` for dynamic functions. ([Docs.rs][8])
* [ ] `return_type` implemented (even if you later add `return_field_from_args`) to keep `information_schema` / `SHOW FUNCTIONS` accurate given current limitations. ([GitHub][12])
* [ ] sqllogictest prototype + `--complete` output committed; includes `SET datafusion.catalog.information_schema=true`, `SHOW FUNCTIONS`, and routines/parameters queries. ([Apache Git Repositories][13])

If you want, next we can do the same “automation” pass for **UDTF docs** (TableFunctionImpl). There is no `Documentation` carrier for UDTFs today, so the pattern becomes “generate an `inspect_*` scalar function” or “add a virtual information_schema extension” rather than reusing `Documentation`.

[1]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.CreateFunction.html "CreateFunction in datafusion::logical_expr - Rust"
[2]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.OperateFunctionArg.html "OperateFunctionArg in datafusion_expr::logical_plan - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.CreateFunctionBody.html "CreateFunctionBody in datafusion::logical_expr - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/arguments/fn.resolve_function_arguments.html?utm_source=chatgpt.com "resolve_function_arguments in datafusion - arguments"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Signature.html "Signature in datafusion::logical_expr - Rust"
[6]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[8]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.Documentation.html "Documentation in datafusion_expr - Rust"
[9]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.DocumentationBuilder.html "DocumentationBuilder in datafusion_expr - Rust"
[10]: https://docs.rs/datafusion-doc/latest/datafusion_doc/struct.DocSection.html?utm_source=chatgpt.com "DocSection in datafusion_doc - Rust"
[11]: https://docs.rs/datafusion-macros "datafusion_macros - Rust"
[12]: https://github.com/apache/datafusion/issues/19870?utm_source=chatgpt.com "Information schema should use `return_field_from_args`"
[13]: https://apache.googlesource.com/datafusion/%2Bshow/refs/heads/branch-35/datafusion/sqllogictest/README.md "datafusion/sqllogictest/README.md - datafusion - Git at Google"
[14]: https://github.com/apache/datafusion/discussions/12848?utm_source=chatgpt.com "What is the status of \"CREATE FUNCTION\"? #12848"

# K) Performance, correctness, and “engine-native” best practices (DataFusion UDFs)

## K1) Scalar UDF performance patterns

### K1.1 Treat `ColumnarValue::Scalar` as a first-class fast path

DataFusion’s scalar UDF runtime uses `ColumnarValue` (Array or Scalar). **Best-practice per core docs:** handle the common case where one or more arguments are `ColumnarValue::Scalar`; blindly calling `ColumnarValue::values_to_arrays` is simpler but slower. ([Docs.rs][1])

**Why:** `values_to_arrays` expands scalars into full arrays by repetition; `to_array/into_array` does the same and is explicitly “not as efficient as handling the scalar directly.” ([Docs.rs][2])

**Canonical dispatch skeleton**

```rust
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let n = args.number_rows;

    match (&args.args[0], &args.args[1]) {
        (ColumnarValue::Scalar(a), ColumnarValue::Scalar(b)) => {
            // compute once
            Ok(ColumnarValue::Scalar(compute_scalar(a, b)?))
        }
        (ColumnarValue::Array(a), ColumnarValue::Scalar(b)) => {
            // vectorized array ⨉ scalar path (avoid scalar expansion)
            Ok(ColumnarValue::Array(Arc::new(kernel_array_scalar(a.as_ref(), b, n)?) as _))
        }
        (ColumnarValue::Scalar(a), ColumnarValue::Array(b)) => {
            Ok(ColumnarValue::Array(Arc::new(kernel_scalar_array(a, b.as_ref(), n)?) as _))
        }
        (ColumnarValue::Array(a), ColumnarValue::Array(b)) => {
            // array ⨉ array path (validate equal lengths if needed)
            Ok(ColumnarValue::Array(Arc::new(kernel_array_array(a.as_ref(), b.as_ref())?) as _))
        }
    }
}
```

**Guardrail:** if you must “arrayify” to reuse one implementation, prefer `into_array_of_size / to_array_of_size` to validate output length. ([Docs.rs][2])

---

### K1.2 Allocation discipline (avoid per-row churn)

Scalar UDFs are expected to be **vectorized**: input Arrow arrays → output Arrow array of the same row count. ([Apache DataFusion][3])

Practical allocation rules (Arrow-native, engine-friendly):

* **One output allocation per batch**: pre-size builders to `n` rows (e.g., `PrimitiveBuilder::with_capacity(n)` / `BooleanBuilder::with_capacity(n)`), append in tight loops, finish once.
* **Avoid per-element heap allocations**: don’t build `String` per row if you can slice/borrow (`Utf8View`/views) or use kernel APIs.
* **Prefer Arrow compute kernels** where they exist (casts, arithmetic, comparisons) to leverage SIMD/vectorization and avoid per-row dynamic dispatch (you’ll typically downcast once, then operate on raw buffers).
* **Exploit constant scalar args** not only for compute but also to avoid allocating intermediate arrays at all (e.g., early-return constant output, or reuse a pre-built output buffer if semantics allow).

Engine contract reminders:

* `ColumnarValue::values_to_arrays` exists, but its docs explicitly warn it can be inefficient; “specialized implementations for scalar values” are recommended when performance matters. ([Docs.rs][2])
* Scalar UDF trait docs repeat this guidance as the primary performance recommendation for `invoke_with_args`. ([Docs.rs][1])

---

### K1.3 “Engine-native” correctness invariants (that prevent perf cliffs)

* **Row-count invariant**: returning an Array must match the batch’s row count (use `_of_size` helpers when converting). ([Docs.rs][2])
* **Avoid unnecessary casts**: if you cast inside the UDF, do it once per argument (not per row), ideally using `ColumnarValue::cast_to` on the whole value. ([Docs.rs][2])

---

## K2) Aggregate UDF performance patterns

### K2.1 State representation: “native first”, `ScalarValue` only at boundaries

For `Accumulator` (single group) and `GroupsAccumulator` (many groups), the performance bottleneck is almost always:

* state size and cache locality
* how many allocations occur during `update_batch/merge_batch`
* the cost of converting state ↔ Arrow representations

**Accumulator contract points to optimize**

* `evaluate()` and `state()` **consume internal state** and must not be called twice (nondeterminism risk); use this to build Arrow-compatible outputs without copying when possible. ([Docs.rs][4])
* `merge_batch(states)` is fed by **concatenating outputs of `state()`**, and it may differ from `update_batch` (e.g., COUNT merges partial counts vs counting raw values). This is the correctness/perf seam for multi-phase aggregation. ([Docs.rs][4])
* `size()` must report allocated bytes, using container **capacity** (not len), because DataFusion uses it for memory budgeting. ([Docs.rs][4])
* If used as a **window aggregate** with bounded frames, implement `retract_batch` as the inverse of `update_batch`; docs specify the sliding lifecycle (evaluate → retract leaving → update entering). ([Docs.rs][4])

**Recommended internal layout**

* Keep state in native fields (`i128`, `f64`, `u64`, `Vec<T>`, `SmallVec`, `HashSet` only when required).
* Convert to `ScalarValue` only in `evaluate/state`.
* For multi-component state (AVG, variance, t-digest), emit a single `StructArray` for state rather than many arrays when it improves cache locality / reduces plumbing (explicitly supported by `GroupsAccumulator::state` docs). ([Docs.rs][5])

---

### K2.2 When to “graduate” to `GroupsAccumulator`

DataFusion calls out `GroupsAccumulator` as **optional, harder, but much faster** for many groups. ([Docs.rs][5])

**High-cardinality value case (why it’s faster)**
DataFusion’s “fast grouping” redesign attributes large wins to:

* eliminating per-group allocations
* storing accumulator state for *all groups* in contiguous `Vec<T>` allocations
* vectorized, type-specialized update loops optimized by LLVM ([Apache DataFusion][6])

**Decision trigger (practical)**

* If GROUP BY cardinality is high enough that “one accumulator instance per group” overhead dominates, implement `GroupsAccumulator`.
* If your aggregate is “heavy state” (array_agg-like), start with a simple primitive type first; groups-accumulator correctness is hard, and performance wins come primarily from contiguous storage patterns (this matches how DataFusion itself frames the design). ([Docs.rs][5])

---

### K2.3 `GroupsAccumulator` implementation rules that directly impact perf/correctness

From the trait docs (these are not optional conventions):

* **Contiguous group indices**: `group_index` values are contiguous; implement state as `Vec<...>` indexed by group_index. ([Docs.rs][5])
* **Respect `opt_filter`**: only apply row `i` if `opt_filter[i]` is true. ([Docs.rs][5])
* **Output ordering**: `evaluate/state` must return rows in group_index order, with null for groups that saw no values. ([Docs.rs][5])
* **`EmitTo` semantics**: support emitting prefixes (`EmitTo::First(n)`) by removing emitted groups and shifting subsequent group indices down by `n`. ([Docs.rs][5])
* **`size()` must be cheap**: called once per batch; docs require it be `O(n)` to compute, not `O(num_groups)`. Track memory incrementally. ([Docs.rs][5])
* Use `NullState` helper to track “group has seen any values” correctly. ([Docs.rs][5])

---

### K2.4 Implement `convert_to_state` if you want “skip partial aggregation mode” wins

DataFusion has a “skip partial aggregation mode” for high-cardinality cases (do minimal work in the partial phase), and it requires `GroupsAccumulator::convert_to_state` to be implemented to participate. ([GitHub][7])

**Design intent:** `convert_to_state` turns raw inputs into intermediate state as if each row were its own group—ideal for SUM/MIN/MAX/COUNT-like aggregates where state is “identity-ish”. ([GitHub][7])

---

## K3) Window UDF performance + ordering pitfalls

### K3.1 Identify your dominant cost center

DataFusion window execution is **sort-based**: input is sorted by `PARTITION BY` + `ORDER BY`, then `WindowAggExec` detects partitions and invokes `PartitionEvaluator` APIs. ([Apache DataFusion][8])

Dominant costs usually fall into one of:

1. **Sorting / enforcing ordering** (pipeline breaker; memory heavy)
2. **Frame boundary computation** (especially RANGE/GROUPS)
3. **Evaluator method choice** (`evaluate_all` vs per-row `evaluate`)
4. **State retention per partition** (large partitions + unbounded frames)

---

### K3.2 Pick the right `PartitionEvaluator` mode (this is the biggest single lever)

DataFusion dispatches based on evaluator capability flags and documents an explicit implementation table. ([Docs.rs][9])

**Key rule:** `evaluate(values, range)` is the “most general but least performant” method because it produces output **one row at a time**; implementing specialized methods is typically much faster. ([Docs.rs][9])

**Mode selection**

* Use `evaluate_all` when the function can compute all rows in a partition in one pass (frame-independent).
* Use `supports_bounded_execution=true` + `get_range` when you only need limited history (bounded memory).
* Use `memoize` for fixed-beginning frames (e.g., `UNBOUNDED PRECEDING`) to enable pruning.

---

### K3.3 Pruning for unbounded frames: implement `memoize` correctly

`PartitionEvaluator::memoize` exists specifically so functions like FIRST_VALUE/LAST_VALUE/NTH_VALUE can stop needing the full unbounded input after seeing enough rows; it is called after each input batch, and you can modify `WindowAggState` to allow pruning. ([Docs.rs][9])

**Performance pattern**

* Detect “result is finalized” (e.g., FIRST_VALUE once first needed value is observed).
* Store finalized result in evaluator state to make subsequent evaluation O(1).
* Advance frame start / prune state so older rows can be dropped.

---

### K3.4 Ordering pitfalls: avoid unnecessary sorts and don’t assume output order

**Fact:** Window functions require the correct `PARTITION BY`/`ORDER BY` ordering; if not satisfied, DataFusion sorts. ([Apache DataFusion][8])

**Engine-native optimization:** DataFusion minimizes resorting by tracking ordering and applying ordering analysis; the UDWF blog explicitly links window performance to the ordering-analysis machinery. ([Apache DataFusion][8])

From the ordering analysis blog: determining when input satisfies an operator’s ordering requirement is crucial for:

* removing unnecessary sorts
* generating streaming-friendly plans
* choosing more efficient operator variants ([Apache DataFusion][10])

**UDWF author implications**

* Keep your UDWF’s physical argument expressions “transparent” (avoid unnecessary expression wrapping/casting) so ordering equivalences remain recognizable.
* If your UDWF output is monotone in input order, consider declaring output ordering via `sort_options` (when semantically correct) so downstream operators can exploit it (this is how built-in rank/row_number style functions behave).

---

### K3.5 Frame sensitivity: ROWS vs RANGE/GROUPS

Even if not authoring the SQL layer, your performance characteristics differ dramatically:

* Frame-dependent evaluators (`uses_window_frame=true`) force frame range computation and generally push you toward per-row `evaluate`. ([Docs.rs][9])
* Unnecessary RANGE/GROUPS usage increases complexity; prefer ROWS when semantics permit (ROWS frames are conceptually simpler; RANGE/GROUPS require ordering constraints and more stateful boundary logic in many engines).

---

## K4) Determinism and volatility (planning/optimization effects)

### K4.1 Use the strictest `Volatility` that is correct

DataFusion’s own volatility docs: volatility “determines eligibility for certain optimizations” and you should pick the strictest possible to maximize performance and avoid unexpected results. ([Docs.rs][11])

**DataFusion-specific semantics (not generic folklore):**

* **Immutable**: same output for same input; DataFusion will inline immutable functions during planning (e.g., `abs(-1)` → `1`). ([Docs.rs][11])
* **Stable**: same output for same input *within a query* but may vary across queries; DataFusion can inline stable functions when planning a query for execution, but not in view definitions or prepared statements. ([Docs.rs][11])
* **Volatile**: may change per evaluation; DataFusion cannot evaluate during planning and cannot push volatile predicates into scans; volatile functions may be evaluated per output row (e.g., `random()`). ([Docs.rs][11])

### K4.2 Practical volatility guidance (correctness + perf)

* If you mistakenly label a time/random/config-dependent function as `Immutable`, you risk **constant folding** and “frozen” results (semantic break). ([Docs.rs][11])
* If you mistakenly label a deterministic function `Volatile`, you block planning-time inlining and certain pushdowns, and you may force per-row evaluation and prevent predicate pushdown into scans. ([Docs.rs][11])
* For “constant per query” values (timestamps captured at query start, session parameters), `Stable` is the intended choice; DataFusion can inline it for query execution planning. ([Docs.rs][11])

---

## Practical “merge-ready” checklists (agent-oriented)

### Scalar UDF

* Handle `ColumnarValue::Scalar` explicitly; avoid `values_to_arrays` on hot paths. ([Docs.rs][1])
* Validate output length if converting scalars (`*_of_size`). ([Docs.rs][2])
* Allocate output once per batch; avoid per-row heap churn.

### Aggregate UDF

* Keep state native; use `ScalarValue` only at boundaries (`evaluate/state`). ([Docs.rs][4])
* Implement correct `merge_batch` (often differs from `update_batch`); states come from concatenated `state()` outputs. ([Docs.rs][4])
* Implement `size()` using **capacity** for memory accounting. ([Docs.rs][4])
* Add `retract_batch` iff you want sliding-window performance; follow the documented sliding lifecycle. ([Docs.rs][4])
* For high cardinality, implement `GroupsAccumulator` (contiguous `Vec` state) and consider `convert_to_state` for skip-partial mode. ([Apache DataFusion][6])

### Window UDF

* Avoid per-row `evaluate` unless unavoidable; prefer `evaluate_all`/bounded execution. ([Docs.rs][9])
* Implement `memoize` for fixed-beginning frames to enable pruning. ([Docs.rs][9])
* Treat ordering as a first-order performance constraint; design to let DataFusion’s ordering analysis remove sorts. ([Apache DataFusion][8])

[1]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.ScalarUDFImpl.html "ScalarUDFImpl in datafusion_expr - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.ColumnarValue.html "ColumnarValue in datafusion::logical_expr - Rust"
[3]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html "Adding User Defined Functions: Scalar/Window/Aggregate/Table Functions — Apache DataFusion  documentation"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.Accumulator.html "Accumulator in datafusion::logical_expr - Rust"
[5]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.GroupsAccumulator.html "GroupsAccumulator in datafusion_expr - Rust"
[6]: https://datafusion.apache.org/blog/2023/08/05/datafusion_fast_grouping/ "Aggregating Millions of Groups Fast in Apache Arrow DataFusion 28.0.0 - Apache DataFusion Blog"
[7]: https://github.com/apache/datafusion/issues/11819 "Improve performance of  other non GroupsAdapter aggregates: implement `convert_to_state` · Issue #11819 · apache/datafusion · GitHub"
[8]: https://datafusion.apache.org/blog/2025/04/19/user-defined-window-functions/ "User defined Window Functions in DataFusion - Apache DataFusion Blog"
[9]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.PartitionEvaluator.html "PartitionEvaluator in datafusion_expr - Rust"
[10]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/ "Using Ordering for Better Plans in Apache DataFusion - Apache DataFusion Blog"
[11]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Volatility.html "Volatility in datafusion::logical_expr - Rust"

# L) Testing, debugging, and reproducibility (DataFusion / Rust UDFs)

This section is a **testing + debugging toolchain spec** for DataFusion extensions (Scalar UDFs / UDAFs / UDWFs / UDTFs), optimized for **engine-native correctness** and **low-churn regressions**.

---

## L0) Test stack layering (what each layer proves)

### L0.1 Unit-level (pure Rust, no SQL)

**Proves:** UDF kernel semantics, null propagation, scalar fast paths, state/merge invariants, retract inverses.
**Mechanism:** directly call `invoke_with_args` / `Accumulator` / `GroupsAccumulator` / `PartitionEvaluator`.

### L0.2 Engine-level integration (SessionContext + RecordBatches)

**Proves:** planner bindings, type coercion, function resolution, pushdowns, stability under different partitioning/batch sizes.
**Mechanism:** create a `SessionContext`, register function/provider, execute query, assert `Vec<RecordBatch>`.

### L0.3 Golden output (RecordBatch pretty tables)

**Proves:** end-to-end semantics and display stability for small test fixtures.
**Mechanism:** `assert_batches_eq!` / `assert_batches_sorted_eq!`. These are designed so failures are copy/pasteable. ([Docs.rs][1])

### L0.4 Planner snapshots (logical/physical plan text)

**Proves:** plan shape stability, rewrite/coercion placement, “operator selection” regressions, accidental reintroductions of sorts/repartitions.
**Mechanism:** `EXPLAIN` + snapshot tool (`insta`), with config pins. DataFusion’s own testing guide uses `insta` + `cargo insta review`. ([DataFusion][2])

### L0.5 Property tests (null semantics, coercion, boundary cases)

**Proves:** invariant correctness across wide input space, catches null/edge bugs early (DataFusion has had real bugs around NULL handling in functions). ([GitHub][3])

---

## L1) Golden tests using `RecordBatch` outputs

### L1.1 Primary assertions (DataFusion-native)

* `datafusion::assert_batches_eq!(expected_lines, &batches)`
  Compares pretty-formatted output; failure output is designed for copy/paste into tests. ([Docs.rs][1])
* `datafusion::assert_batches_sorted_eq!(expected_lines, &batches)`
  Same, but row order doesn’t matter (use when results are unordered unless explicitly `ORDER BY`). ([Docs.rs][4])

> DataFusion’s contributor guide explicitly calls out these macros (plus `assert_contains` / `assert_not_contains`) as the standard unit-test utilities. ([DataFusion][2])

### L1.2 Canonical integration harness (register → query → collect → assert)

```rust
use std::sync::Arc;
use arrow::array::{Int64Array, ArrayRef};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;

use datafusion::prelude::*;
use datafusion::datasource::MemTable;

#[tokio::test]
async fn golden_udf_result() -> datafusion::error::Result<()> {
    // 1) Input batches
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef],
    )?;

    // 2) Table provider (toy but perfect for tests)
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;

    // 3) Context + registration
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(table))?;

    // TODO: ctx.register_udf(my_udf());

    // 4) Execute + collect (buffers into Vec<RecordBatch>)
    let df = ctx.sql("SELECT x FROM t").await?;
    let batches = df.collect().await?; // DataFrame::collect buffers all output
    // collect/streaming APIs are explicitly documented
    // - collect: Vec<RecordBatch>
    // - execute_stream: streaming record batches
    // - cache: materialized DataFrame
    // :contentReference[oaicite:6]{index=6}

    // 5) Golden assertion
    datafusion::assert_batches_eq!(
        vec![
            "+---+",
            "| x |",
            "+---+",
            "| 1 |",
            "|   |",
            "| 3 |",
            "+---+",
        ],
        &batches
    );

    Ok(())
}
```

DataFrame execution modes (`collect`, `execute_stream`, `cache`) are documented and should be treated as distinct testing surfaces (buffered vs streaming). ([DataFusion][5])

### L1.3 Output-order determinism rule

* Prefer **explicit `ORDER BY`** in golden tests.
* Otherwise use `assert_batches_sorted_eq!` for unordered relations. ([Docs.rs][4])

---

## L2) Planner snapshots (logical + physical) for regression detection

### L2.1 Why snapshot plans

DataFusion explicitly distinguishes:

* **Logical plan**: query structure independent of physical layout.
* **Physical plan**: can vary with hardware, configuration (CPU count/partitions), and data organization (files). ([DataFusion][6])

**Snapshot guidance:**

* Snapshot **logical plans** for cross-environment stability.
* Snapshot **physical plans** only when you also pin config + inputs tightly (or accept churn).

### L2.2 Plan acquisition APIs

**SQL**:

* `EXPLAIN ...` prints both logical and physical plans (default `indent` format). ([DataFusion][7])
* `EXPLAIN FORMAT tree ...` gives a tree-rendered plan (more readable; can change test baselines). ([DataFusion][7])

**Config knobs for stable plan text**

* `datafusion.explain.format` (`indent` default, `tree` optional)
* `datafusion.explain.show_schema`
* `datafusion.explain.tree_maximum_render_width`
* `datafusion.explain.analyze_level` ([DataFusion][8])

### L2.3 Snapshot mechanism (DataFusion-native practice: `insta`)

DataFusion’s testing guide uses snapshot testing with **Insta** and `cargo insta review` for updates. ([DataFusion][2])

**Minimal pattern (Rust + insta)**

```rust
#[tokio::test]
async fn snapshot_explain_tree() -> datafusion::error::Result<()> {
    use datafusion::prelude::*;
    let ctx = SessionContext::new();

    // ... register tables/functions ...

    let df = ctx.sql("EXPLAIN FORMAT tree SELECT 1").await?;
    let batches = df.collect().await?;
    // Convert to pretty lines (same formatter used by assert_batches_eq)
    let s = datafusion::arrow::util::pretty::pretty_format_batches(&batches)?.to_string();

    insta::assert_snapshot!(s);
    Ok(())
}
```

(Use `cargo insta review` to accept changes.) ([DataFusion][9])

### L2.4 “Plan-shape only” assertions (low churn)

For small invariants (“a CAST got inserted”, “FilterExec exists”), use string containment macros:

* `assert_contains!(actual, expected_substring)` / `assert_not_contains!` are in DataFusion common test utilities. ([Docs.rs][10])

---

## L3) Type coercion and rewrite debugging (engine-correct way)

### L3.1 Use `SessionContext/SessionState::create_physical_expr` when testing coercion

`create_physical_expr` is explicitly documented to:

* apply **type coercion** and **function rewrites**
* but **not** do full simplification/optimization (e.g., `1+2` won’t become `3`) ([Docs.rs][11])

This makes it the correct harness for:

* “Does calling my UDF with `(Int32, Int64)` insert the right casts?”
* “Did my `FunctionRewrite` actually run before physical planning?”

### L3.2 Coercion rules: lossless casts inserted by DataFusion

DataFusion’s type coercion module states coercion inserts **lossless CASTs** to satisfy operator/function requirements. ([Docs.rs][12])

If your function uses `TypeSignature::Coercible` / `UserDefined` coercions, the `Coercion` model is first-class and testable via plan inspection. ([Docs.rs][13])

---

## L4) Property tests: null semantics, coercion, boundary cases

### L4.1 Why property testing is mandatory for UDFs

DataFusion has had real-world function bugs around NULL handling (errors/panics on NULL inputs). ([GitHub][3])
Property tests are the cheapest way to guarantee: “no panics, stable null propagation, correct row counts, correct coercion behavior.”

### L4.2 Scalar UDF invariants (high ROI)

**Invariant S1: scalar vs array equivalence**
For any scalar input `s` and any `n`, evaluating with `ColumnarValue::Scalar(s)` must equal evaluating with an array that repeats `s` `n` times (modulo null semantics). (Enforces correct scalar fast paths; avoids logic drift.)

**Invariant S2: output length**
If result is `ColumnarValue::Array`, its length must equal `number_rows` (use `_of_size` conversions in code; assert in tests). ([Docs.rs][14])

**Invariant S3: null propagation policy is total**
Define & test: if any arg is NULL, does output become NULL, error, or some value? Don’t leave “implicit” behavior.

### L4.3 UDAF invariants (merge + retract correctness)

**Invariant A1: merge associativity**

* Split input into random partitions.
* For each partition: `acc.update_batch`, then `state()`.
* Merge states into a fresh accumulator using `merge_batch`.
* Compare to single-pass update on full input.
  This matches DataFusion’s documented semantics: `merge_batch` consumes concatenated `state()` outputs. ([Docs.rs][11])

**Invariant A2: sliding window inverse (`retract_batch`)**
If your accumulator supports retract, enforce the documented lifecycle: evaluate → retract leaving → update entering. ([Docs.rs][11])
Property test: random window movement over a small array; sliding accumulator equals naive recompute for each window.

### L4.4 UDWF invariants (batching + frame sensitivity)

* **Batch boundary invariance**: same partition, different RecordBatch chunking must yield identical outputs (UDWF evaluators are stateful across batches). ([DataFusion][6])
* **Frame semantics invariance**: `ROWS` frames vs default frame rules; assert correct behavior under multiple frame definitions (especially for `uses_window_frame=true`). (Use SQL-driven property tests when possible.)

### L4.5 Type coercion property tests

Use random pairs of “compatible types” and check:

* implicit casts are inserted (plan contains `Cast` / or evaluation equals explicit cast)
* incompatible types fail planning (returns `DataFusionError::Plan`)

Ground truth:

* coercion is automatic and lossless per DataFusion type coercion docs. ([Docs.rs][12])

---

## L5) Debugging toolkit (minimal set that actually moves the needle)

### L5.1 Explain-first (structure before data)

* `EXPLAIN` for plan shape
* `EXPLAIN ANALYZE` only when you want runtime metrics (expect churn; don’t snapshot timings) ([DataFusion][7])

### L5.2 Streaming execution for “where did the batch boundary break me”

`execute_stream` yields record batches incrementally; use it to isolate stateful-evaluator bugs and to confirm batch-size sensitivity. ([DataFusion][5])

### L5.3 CPU profiling for UDF hot paths

DataFusion ships a profiling cookbook (e.g., `cargo-flamegraph` workflows) for flamegraphs and CPU profiling; use this to verify scalar fast paths and aggregator state layouts are actually being hit. ([DataFusion][15])

---

## L6) Reproducibility knobs (pin the “plan + output” degrees of freedom)

### L6.1 Pin execution parallelism for stable physical plans

* `datafusion.execution.target_partitions` controls parallelism/streams; default influences how many streams each plan produces. ([Docs.rs][16])
  For deterministic tests, consider pinning `target_partitions=1` (accept lower concurrency) and snapshot **logical** plans when possible (physical plans can legitimately differ by environment). ([DataFusion][6])

### L6.2 Pin batch boundaries when testing stateful operators

* `datafusion.execution.batch_size` influences batch formation. SessionConfig exposes `with_batch_size` and key-value configuration. ([Docs.rs][17])
  For UDWF/UDAF state boundary tests, force small batch sizes to stress incremental paths, then re-run with large batches to prove invariance.

### L6.3 Pin explain format for stable snapshots

Set:

* `datafusion.explain.format = indent|tree`
* optionally `datafusion.explain.show_schema` and `tree_maximum_render_width` ([DataFusion][8])

---

## L7) Minimal “Definition of Done” for a new UDF/UDxF PR

1. **Golden test** via `assert_batches_eq` or `assert_batches_sorted_eq`. ([Docs.rs][1])
2. **Plan snapshot** (logical preferred) via `EXPLAIN FORMAT tree|indent` + `insta`. ([DataFusion][7])
3. **Property tests** for:

   * null semantics (no panics; consistent policy) ([GitHub][3])
   * scalar-vs-array invariants (scalar UDF) ([Docs.rs][14])
   * merge associativity (UDAF) ([Docs.rs][11])
   * retract correctness if supported (window aggregates) ([Docs.rs][11])
   * coercion correctness using `create_physical_expr` (casts inserted; lossless coercion). ([Docs.rs][11])

If you want the next section after L, the usual continuation is **M) Packaging + versioning + distribution** for Rust UDF crates (feature flags, MSRV, semver pinning against DataFusion/Arrow, and test matrix design).

[1]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_eq.html?utm_source=chatgpt.com "assert_batches_eq in datafusion - Rust"
[2]: https://datafusion.apache.org/contributor-guide/testing.html?utm_source=chatgpt.com "Testing — Apache DataFusion documentation"
[3]: https://github.com/apache/datafusion/issues/14451?utm_source=chatgpt.com "Proper NULL handling in array functions · Issue #14451"
[4]: https://docs.rs/datafusion/latest/datafusion/macro.assert_batches_sorted_eq.html?utm_source=chatgpt.com "assert_batches_sorted_eq in datafusion - Rust"
[5]: https://datafusion.apache.org/library-user-guide/using-the-dataframe-api.html?utm_source=chatgpt.com "Using the DataFrame API — Apache DataFusion documentation"
[6]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
[7]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[8]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[9]: https://datafusion.apache.org/_sources/contributor-guide/testing.md.txt?utm_source=chatgpt.com "testing.md.txt - Apache DataFusion"
[10]: https://docs.rs/datafusion-common?utm_source=chatgpt.com "datafusion_common - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html?utm_source=chatgpt.com "SessionState in datafusion::execution::session_state - Rust"
[12]: https://docs.rs/datafusion-expr/latest/datafusion_expr/type_coercion/index.html?utm_source=chatgpt.com "datafusion_expr::type_coercion - Rust"
[13]: https://docs.rs/datafusion-expr/latest/datafusion_expr/enum.Coercion.html?utm_source=chatgpt.com "Coercion in datafusion_expr - Rust"
[14]: https://docs.rs/deltalake/latest/deltalake/datafusion/common/macro.assert_batches_eq.html?utm_source=chatgpt.com "assert_batches_eq in deltalake::datafusion::common - Rust"
[15]: https://datafusion.apache.org/library-user-guide/profiling.html?utm_source=chatgpt.com "Profiling Cookbook — Apache DataFusion documentation"
[16]: https://docs.rs/datafusion/latest/datafusion/?utm_source=chatgpt.com "datafusion - Rust"
[17]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionConfig.html?utm_source=chatgpt.com "SessionConfig in datafusion::execution::context - Rust"

# M) Packaging, versioning, and distribution of Rust UDF libraries (DataFusion)

## M0) The core constraint: **dependency unification**

A “UDF library” is only usable if the host app and the library compile against the **same** (unified) versions of:

* DataFusion public crates (`datafusion`, `datafusion-expr`, `datafusion-common`, …)
* Arrow Rust crates (`arrow-*`, `parquet`, …)

DataFusion major releases routinely upgrade Arrow/Parquet (e.g., DataFusion 51 upgraded Arrow/Parquet to 57.x per the upgrade guide / release notes), so **your crate’s compatibility boundary is effectively “DataFusion major + Arrow major.”** ([DataFusion][1])

---

## M1) “UDFs as a crate”: recommended crate topologies

### Topology A — **single crate** (fastest; OK for internal use)

Use when you control the app + UDF crate in the same workspace.

* Pros: simplest.
* Cons: you will almost certainly leak DataFusion/Arrow types into your public API and make upgrades noisier.

### Topology B — **two-tier** (best default for reusable UDF libraries)

**`my_udfs_core`**: implements UDF logic against `datafusion-expr` (traits + logical wrappers), keeps runtime dependencies minimal.
**`my_udfs_register`**: provides “wiring helpers” that register UDFs/UDWFs/UDAFs/UDTFs into a `FunctionRegistry` / `SessionContext`.

Why this works:

* `datafusion-expr` is explicitly the submodule crate providing `LogicalPlan`, `Expr`, and expression utilities; it’s the natural dependency boundary for most UDF authoring. ([Docs.rs][2])
* The host app can depend on `datafusion` for execution, while your core crate stays small and avoids pulling SQL/parquet by default.

### Topology C — **package-per-feature** (best for distributing many function “packages”)

Mirror `datafusion-functions`: separate packages activated by feature flags to control binary size and dialiect-specific variants; offer `register_all()` and per-module `functions()` entrypoints. ([Docs.rs][3])

---

## M2) Dependency boundaries (what you depend on, and why)

### M2.1 Prefer `datafusion-expr` for *expression functions* (scalar/aggregate/window)

If your library only defines **Scalar UDF / UDAF / UDWF**, make `datafusion-expr` your primary dependency:

* It is the logical expression/plan crate (traits + `Expr` building), which is exactly where the UDF implementation traits live. ([Docs.rs][2])
* `datafusion-expr` has a small feature surface (notably `sql` is enabled by default, which pulls `sqlparser`). If you do not need SQL parsing inside your UDF crate, disable defaults. ([Docs.rs][4])

**Cargo pattern**

```toml
[dependencies]
datafusion-expr = { version = "52", default-features = false }  # avoid sqlparser unless you need it
datafusion-common = { version = "52" }
# (optional) arrow-array / arrow-schema only if you truly need them explicitly
```

### M2.2 Use `datafusion` only when you need **execution surfaces**

You need `datafusion` (or the datasource/catalog subcrates) when:

* you provide **UDTFs** (`TableFunctionImpl` returning `TableProvider`)
* you provide **custom TableProvider/TableSource** backends
* you ship an end-to-end “register + run” harness or CLI.

**Why avoid `datafusion` in the core crate:** the `datafusion` crate’s default features are extensive (SQL planning, Parquet, many function packages, compression), which increases compile time and pulls in many transitive dependencies. ([Docs.rs][5])

---

## M3) Feature flags (yours + DataFusion’s) and build-size control

### M3.1 DataFusion feature reality (what you inherit)

`datafusion` ships many feature flags; a large set is enabled by default (e.g., `sql`, `parquet`, multiple expression packages, compression). ([Docs.rs][5])
It also has optional features like `avro`, `backtrace`, `parquet_encryption`, `serde`. ([Docs.rs][6])

### M3.2 Best practice: **turn off defaults, opt in explicitly**

DataFusion’s own crate config docs show this exact pattern (including for git deps), e.g.:

```toml
datafusion = { git = "...", branch = "main",
               default-features = false, features = ["unicode_expressions"] }
```

([DataFusion][7])

For a UDF library, common minimal sets:

* **UDF-only tests**: `datafusion` as a **dev-dependency** with `default-features = false` and maybe `sql` to run SQL-based assertions.
* **UDTF/Provider libs**: enable only the datasource features you need (e.g., Parquet reading only if your UDTF wraps Parquet).

### M3.3 Model your own crate after `datafusion-functions`

`datafusion-functions` is the canonical “packages behind features” reference:

* packages are separate modules
* activated by feature flags
* provides `register_all()` and per-module `functions()` for selective registration. ([Docs.rs][3])

**Your crate surface should look like:**

```rust
pub fn register_all(reg: &mut impl FunctionRegistry) -> Result<()> { ... }
pub mod string_pkg { pub fn functions() -> Vec<Arc<ScalarUDF>> { ... } }
pub mod geo_pkg    { pub fn functions() -> Vec<Arc<ScalarUDF>> { ... } }
```

### M3.4 Cargo workspace footgun (feature unification)

Features are additive across the dependency graph. If you use `[workspace.dependencies]`, `default-features = false` can be accidentally defeated (member crates can’t “turn off” defaults if the workspace dep enables them). This cargo behavior is tracked as a real issue. ([GitHub][8])

**Policy:** if you want “minimal-defaults” guarantees, enforce them at the workspace dependency declaration, not only in leaf crates.

---

## M4) MSRV (Minimum Supported Rust Version) strategy

### M4.1 DataFusion MSRV policy (what you’re signing up for)

DataFusion’s documented Rust compatibility policy:

* supports the **last 4** stable Rust minor versions and any released within the last **4 months**
* enforced via an MSRV CI check. ([DataFusion][9])

DataFusion also bumps MSRV frequently (explicitly called out in upgrade guides), so your UDF crate should expect MSRV drift when you upgrade DataFusion. ([DataFusion][1])

### M4.2 What to do in your crate

1. Set `rust-version` in `Cargo.toml` aligned to your supported DataFusion major.
2. CI matrix:

   * test the DataFusion-supported MSRV window (at least the minimum and latest stable)
   * test both “minimal features” and “full features” builds.

(If you want Cargo to enforce MSRV, the Rust ecosystem’s MSRV mechanism is standardized via the Cargo MSRV field RFC.) ([Rust Language][10])

---

## M5) Versioning + upgrade strategy (avoid brittle pinning)

### M5.1 Don’t pin patches; pin **DataFusion major**

Avoid:

```toml
datafusion = "=52.1.0"
```

Prefer semver-compatible:

```toml
datafusion = "52"          # ^52.0.0, allows 52.x bugfix/minor
datafusion-expr = "52"
```

Rationale:

* DataFusion patch releases are frequent; patch pinning turns security/bugfix updates into “dependency surgery” for downstreams.
* Major upgrades are where real API + Arrow upgrades happen; handle those intentionally using the upgrade guide. ([DataFusion][1])

### M5.2 Treat “DataFusion major” as a compatibility axis

You have three viable compatibility policies:

**Policy 1 — one crate version line per DataFusion major (recommended for libraries)**

* `my-udfs` v0.52.x supports DataFusion 52.x
* `my-udfs` v0.53.x supports DataFusion 53.x
  Pros: simple; mirrors DataFusion’s release cadence.
  Cons: you publish more often.

**Policy 2 — feature-selected DataFusion major (single crate, multiple optional deps)**
Cargo supports dependency renaming + features so you compile against exactly one DF major at a time:

```toml
[features]
df52 = ["dep:datafusion52", "dep:datafusion_expr52"]
df51 = ["dep:datafusion51", "dep:datafusion_expr51"]
default = ["df52"]

[dependencies]
datafusion52 = { package="datafusion", version="52", optional=true, default-features=false }
datafusion_expr52 = { package="datafusion-expr", version="52", optional=true, default-features=false }
datafusion51 = { package="datafusion", version="51", optional=true, default-features=false }
datafusion_expr51 = { package="datafusion-expr", version="51", optional=true, default-features=false }
```

Pros: one crate name.
Cons: you must maintain `cfg(feature="df51")` shims and your users must pick the right feature.

**Policy 3 — internal workspace only**
If not publishing, lock DF/Arrow in the workspace and treat upgrades as monorepo refactors.

### M5.3 Isolate DataFusion API churn behind a “compat” module

DataFusion has frequent API evolution (with deprecations, removals, signature shifts). Keep all DF imports + glue in one module so upgrades are localized.

### M5.4 FFI / dynamic linking: assume **lockstep versioning**

If you distribute UDFs as a dynamic library (cdylib/FFI), treat ABI as **not stable** across DataFusion majors; the upgrade guide documents FFI signature changes for user-defined aggregates and recommends keeping all interacting libraries on the same underlying Rust version. ([DataFusion][1])

---

## M6) Distribution modes (and what they imply)

### M6.1 crates.io (normal library)

* publish `my_udfs_core` and optionally `my_udfs_register`
* keep defaults minimal; expose opt-in features

### M6.2 git dependency for “nightly DataFusion”

DataFusion explicitly documents using Cargo git deps for merged-but-not-released DF, including feature selection and package-level deps. ([DataFusion][7])
Use this in CI to catch breakage early *before* DF releases.

### M6.3 “Function pack” crate (DataFusion-style)

Ship “packages” behind features and a `register_all()` entry point exactly like `datafusion-functions`. ([Docs.rs][3])
This is the best form factor for large organizations: app teams enable only the packages they want.

---

## M7) Concrete template: a reusable UDF crate skeleton

### `my_udfs_core/Cargo.toml`

```toml
[package]
name = "my_udfs_core"
version = "0.52.0"
rust-version = "1.88"   # align to DF major’s MSRV window
edition = "2021"

[features]
default = []
sql_macros = ["datafusion-expr/sql"]   # only if you need sqlparser inside the crate

[dependencies]
datafusion-expr = { version = "52", default-features = false }
datafusion-common = { version = "52" }
```

### `my_udfs_register/Cargo.toml`

```toml
[dependencies]
my_udfs_core = { path = "../my_udfs_core" }
datafusion = { version = "52", default-features = false, features = ["sql"] } # dev / registration surfaces
```

### Entry points

* `pub fn all_scalar_udfs() -> Vec<Arc<ScalarUDF>>`
* `pub fn register_all(reg: &mut impl FunctionRegistry) -> Result<()>` (DataFusion-style) ([Docs.rs][3])

---

If you want the next section, the most practical follow-on is **N) Cross-language + plugin distribution** (FFI boundaries, cdylib packaging, and “why ABI stability is hard” with DataFusion/Arrow), building directly on the FFI breakage notes in the upgrade guide.

[1]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[2]: https://docs.rs/datafusion-expr?utm_source=chatgpt.com "datafusion_expr - Rust"
[3]: https://docs.rs/datafusion-functions "datafusion_functions - Rust"
[4]: https://docs.rs/crate/datafusion-expr/latest/features?utm_source=chatgpt.com "datafusion-expr 51.0.0"
[5]: https://docs.rs/crate/datafusion/latest/features "datafusion 52.1.0 - Docs.rs"
[6]: https://docs.rs/crate/datafusion/latest "datafusion 52.1.0 - Docs.rs"
[7]: https://datafusion.apache.org/user-guide/crate-configuration.html "Crate Configuration — Apache DataFusion  documentation"
[8]: https://github.com/rust-lang/cargo/issues/11329?utm_source=chatgpt.com "workspaces.dependencies causes ignore of default ..."
[9]: https://datafusion.apache.org/user-guide/introduction.html "Introduction — Apache DataFusion  documentation"
[10]: https://rust-lang.github.io/rfcs/2495-min-rust-version.html?utm_source=chatgpt.com "2495-min-rust-version - The Rust RFC Book"

# N) Cross-language + plugin distribution for DataFusion (FFI boundaries, `cdylib`, ABI stability)

## N0) First principles: what you can make stable (and what you can’t)

### Rust ABI is *not stable* → “just depend on the same Rust crate” is not a plugin story

DataFusion Python’s extension guide is explicit: Rust does **not** have a stable ABI, so separately compiled Rust libraries (even if they “use the same crate versions”) are not guaranteed to interoperate; it can appear to work locally and then fail once you change compiler/optimizations/distribution conditions. ([DataFusion][1])

### Separate *data* interchange from *engine object* interchange

In practice you’ll use **two** boundary technologies:

1. **Arrow C Data / C Stream interfaces** for zero-copy **data** interchange (arrays / record batches / streams). ([Apache Arrow][2])
2. **`datafusion-ffi`** for stable-ish interchange of **DataFusion trait objects** (TableProvider, UDFs, catalogs) between separately compiled Rust libraries. ([Docs.rs][3])

---

## N1) Data boundary: Arrow C Data Interface + C Stream Interface (your cross-language “wire format”)

### N1.1 Arrow C Data Interface (arrays + schema)

Arrow’s spec defines a “very small, stable set of C definitions” (`ArrowSchema`, `ArrowArray`) intended to be copied into any project to support **ABI-stable**, **zero-copy** Arrow memory interchange in-process, without depending on Arrow’s language libraries. ([Apache Arrow][2])

**ABI stability promise:** once supported in an official Arrow release, the C ABI is *frozen* (struct definitions shouldn’t change; incompatible changes require a new v2 spec). ([Apache Arrow][2])

### N1.2 Arrow C Stream Interface (streams of batches)

For streaming results, Arrow defines `ArrowArrayStream` (callbacks `get_schema`, `get_next`, `get_last_error`, `release`, plus `private_data`), designed for “communication of streaming data within a single process.” ([Apache Arrow][4])

**Operational constraints you must model in plugins:**

* pull-based iteration; `get_next` returns released `ArrowArray` to signal EOS ([Apache Arrow][4])
* lifetime management via `release` callbacks; results’ lifetimes are independent of the stream ([Apache Arrow][4])
* not assumed thread-safe; serialize `get_next` calls ([Apache Arrow][4])

### N1.3 When to use Arrow C interfaces vs IPC / Flight

Arrow C Data/Stream are explicitly about **in-process** interchange; not cross-process persistence/transport. ([Apache Arrow][2])
If your plugin boundary is cross-process, you generally move to Arrow IPC/Flight/ADBC (not covered here).

---

## N2) Engine boundary: `datafusion-ffi` (stable-ish trait-object interchange between Rust libraries)

### N2.1 What `datafusion-ffi` is trying to be

`datafusion-ffi` positions itself as the set of interfaces that “will remain stable across different versions of DataFusion,” so libraries can be loaded at runtime rather than compiled into one executable; it exists because Rust lacks a stable ABI. ([Docs.rs][3])

It’s explicitly optimized for **Rust ↔ Rust** dynamic linking. If you need to integrate a non-Rust library, the crate’s guidance is: write a small Rust wrapper crate and then connect via `datafusion-ffi`; `bindgen`-style direct consumption is noted as “currently not supported.” ([Docs.rs][3])

### N2.2 The two-sided type model: `FFI_*` (producer) vs `Foreign*` (consumer)

DataFusion Python docs describe the pattern: each shared trait has:

* an `FFI_` struct used by the **producer** (the library that owns the real trait object)
* a `Foreign` wrapper used by the **consumer** (implements the DataFusion trait by calling back through the stable FFI functions). ([DataFusion][1])

Example for TableProvider:

* `FFI_TableProvider`: “stable struct for sharing TableProvider across FFI boundaries” ([Docs.rs][5])
* `ForeignTableProvider`: receiver-side wrapper; must not access producer’s `private_data` directly—only via stable FFI methods ([Docs.rs][5])

### N2.3 Memory management is explicit at the boundary (release discipline)

`datafusion-ffi` docs call out why: an FFI wrapper may be dropped on the consumer side where producer-private state is inaccessible, so many FFI structs carry explicit **release** logic and/or reference counting. ([Docs.rs][3])
If you’re designing “plugin modules,” treat “who frees what” as a first-class API contract, not an implementation detail.

### N2.4 Library identity + “boomerang” objects: `library_marker_id`

The crate documents a real pitfall: a producer may receive back an object it originally created but now wrapped through an FFI path; naive “foreign vs local” handling can segfault. `library_marker_id` exists to detect whether an FFI struct is effectively local to the current library, allowing safe access to private data when appropriate. ([Docs.rs][3])

### N2.5 Why TaskContext appears in FFI constructors (serialization + registry dependence)

The crate explains that some FFI operations serialize/deserialize via `datafusion-proto`, and that de/serializing functions requires a `TaskContext` (which implements `FunctionRegistry`). Therefore, many FFI structs hold an `FFI_TaskContextProvider` as a weak reference, to avoid circular dependencies while enabling late-binding to “the current TaskContext.” ([Docs.rs][3])

**Upgrade impact:** DataFusion’s upgrade guide notes that constructing `FFI_*` providers now requires supplying a `TaskContextProvider` (commonly `SessionContext`) and optionally a `LogicalExtensionCodec`; `new()` / `new_with_ffi_codec()` exist for convenience. ([DataFusion][6])

---

## N3) `cdylib` plugin packaging patterns (in-process)

You can think of “plugin distribution” as choosing *how much* you freeze at the boundary.

### Pattern A — lockstep Rust linking (simplest, most brittle)

**Mechanics:** both host and plugin are Rust crates built against the same DataFusion/Arrow versions; you pass native `Arc<dyn TableProvider>` / `ScalarUDF` etc directly (no FFI wrappers).

**When acceptable:** internal monorepos/workspaces where you control the entire build and can enforce a single toolchain + dependency graph.

**Failure mode:** exactly what DataFusion Python warns about—separate compilation and different toolchains break ABI assumptions. ([DataFusion][1])

### Pattern B — Rust↔Rust FFI plugin via `datafusion-ffi` (intended direction)

**Mechanics:** plugin compiles as a dynamic library and exposes **FFI-wrapped** objects; host loads them and converts to `Foreign*` wrappers (which implement DataFusion traits).

High-level producer/consumer lifecycle (conceptual, but aligned with documented boundaries):

**Producer (plugin)**

1. Implement DataFusion trait normally (e.g., `TableProvider`, `ScalarUDFImpl`, `AggregateUDFImpl`, `WindowUDFImpl`).
2. Wrap with `FFI_*` equivalents (`FFI_TableProvider`, etc.). ([Docs.rs][5])
3. Provide required `TaskContextProvider` / codec plumbing when instantiating FFI wrappers (per upgrade guide). ([DataFusion][6])
4. Export a stable entrypoint that returns the FFI object(s). `datafusion-ffi` docs mention a complete end-to-end example in the crate’s examples directory. ([Docs.rs][3])

**Consumer (host)**

1. Load the dynamic library.
2. Receive `FFI_*` structs.
3. Convert to `Foreign*` wrappers (e.g., `ForeignTableProvider::from(ffi_provider)`), then register with `SessionContext`/catalog as usual. ([Docs.rs][3])

**Operational considerations**

* async FFI calls are implemented via `async-ffi` (nonblocking semantics intended to mirror Rust async). ([Docs.rs][3])
* you must treat `library_marker_id` as part of correctness if objects can “boomerang” through multiple plugin layers. ([Docs.rs][3])

### Pattern C — cross-language plugin via Arrow C Stream (data-only boundary)

If your “plugin” is primarily a **data source**, you can side-step most engine ABI issues by:

* exposing an Arrow C Stream (`ArrowArrayStream`) from the plugin
* having DataFusion ingest it (or a wrapper provider ingest it)

This gives you a stable in-process data channel via a C ABI. ([Apache Arrow][4])
It does *not* transmit DataFusion trait objects; it transmits Arrow data.

---

## N4) Python: PyCapsule conventions + DataFusion-FFI interop

DataFusion Python’s extension guide is explicit about the packaging goal: allow users to ship Python packages with Rust-backed extensions that can interact with the published `datafusion-python` wheels without requiring a shared Rust toolchain build. ([DataFusion][1])

### N4.1 The approach

* Use Arrow’s FFI protocols (C Stream / PyCapsule style) for Arrow data interchange. ([DataFusion][1])
* Use `datafusion-ffi` for DataFusion engine objects (starting with `TableProvider` as the most mature). ([DataFusion][1])

### N4.2 TableProvider via PyCapsule

Python guide describes:

* producer side: create a `PyCapsule` that contains the FFI-stable struct (e.g., `FFI_TableProvider`) ([DataFusion][1])
* receiver side: `datafusion-python` expects a method named `__datafusion_table_provider__` to return the capsule; it downcasts, extracts `FFI_TableProvider`, then converts to a `ForeignTableProvider` implementing `TableProvider`. ([DataFusion][1])

DataFusion Python’s IO docs also state this requires DataFusion ≥ 43 and that you expose `FFI_TableProvider` via PyCapsule. ([DataFusion][7])

### N4.3 UDFs across FFI boundaries

DataFusion 48 release notes call out explicit FFI support for `AggregateUDF` and `WindowUDF`, enabling shared libraries to pass these functions back and forth and reuse them in projects like `datafusion-python`. ([DataFusion][8])

---

## N5) ABI stability is hard: what’s stable today vs what’s still moving

### N5.1 `datafusion-ffi` *aims* for stability, but the boundary has had breaking changes

The project is actively working toward stability, but real breakage has occurred. The “Stabilize FFI Boundary” issue explicitly notes:

* a breaking change between DataFusion 48 and 49 for record batch stream FFI
* another planned breaking change from 49 to 50 in user-defined functions
* this forces `datafusion-python` and extension libraries to match exact versions, undermining the plugin goal. ([GitHub][9])

### N5.2 Upgrade guide: concrete breaking changes you must design around

The DataFusion upgrade guide documents FFI-level API shifts, including:

* new conversion paths (e.g., converting FFI UDFs into `Arc<dyn ScalarUDFImpl>` and then `ScalarUDF::new_from_shared_impl`) ([DataFusion][6])
* requirement to supply `TaskContextProvider` (typically `SessionContext`) and optional codecs when constructing `FFI_*` providers/functions ([DataFusion][6])
* FFI scalar UDF structure no longer includes a `return_type` call, since `return_field_from_args` is used instead ([DataFusion][6])
* **FFI UDAF signature change** to call `return_field` instead of `return_type` (metadata handling), flagged as a breaking FFI API change; the guide states a current best practice is keeping all interacting libraries on the same Rust version and references ongoing stabilization work. ([DataFusion][6])

### N5.3 Practical takeaway

Treat “FFI boundary stability” as:

* **not fully frozen yet**, and
* sensitive to both **DataFusion version** and **Rust toolchain version**
  even though the crate’s intent is eventual cross-version stability. ([Docs.rs][3])

---

## N6) “Engine-native” plugin API checklist (what to standardize in your own system)

### N6.1 Decide your exported surface

* **Data-only plugin:** Arrow C Stream (`ArrowArrayStream`) + schema via Arrow C Data Interface. ([Apache Arrow][4])
* **Engine-object plugin:** `datafusion-ffi` `FFI_*` wrappers for TableProvider/UDFs/etc. ([Docs.rs][3])

### N6.2 Version negotiation + compatibility policy

Given current churn, add explicit version gates:

* plugin exports: `(datafusion_major, datafusion_ffi_major, arrow_major, rustc_semver-ish)` metadata
* host rejects incompatible plugins early (fail-fast > segfault)

### N6.3 Memory + ownership

* ensure every exported object has a clear release path; don’t assume “Drop runs where private data exists” ([Docs.rs][3])
* if you wrap Arrow C interfaces, follow the Arrow spec’s release callback semantics strictly ([Apache Arrow][2])

### N6.4 TaskContext plumbing (avoid dead registries)

If your plugin uses `datafusion-proto` serialization or returns complex objects that may need later resolution, pass a `TaskContextProvider` (or a codec that holds it) exactly as the upgrade guide and `datafusion-ffi` docs describe. ([DataFusion][6])

---

If you want to go one level deeper, the next most actionable artifact is a **“plugin ABI spec”** you can hand to codegen agents: a small set of exported symbols (or an `abi_stable` module root), version handshake schema, and producer/consumer skeletons for (1) a `TableProvider` plugin and (2) a bundle of UDFs (Scalar + Aggregate + Window) using the post-48 FFI capabilities.

[1]: https://datafusion.apache.org/python/contributor-guide/ffi.html "Python Extensions — Apache Arrow DataFusion  documentation"
[2]: https://arrow.apache.org/docs/format/CDataInterface.html "The Arrow C data interface — Apache Arrow v23.0.0"
[3]: https://docs.rs/crate/datafusion-ffi/latest "datafusion-ffi 52.1.0 - Docs.rs"
[4]: https://arrow.apache.org/docs/format/CStreamInterface.html "The Arrow C stream interface — Apache Arrow v23.0.0"
[5]: https://docs.rs/datafusion-ffi/latest/datafusion_ffi/table_provider/index.html "datafusion_ffi::table_provider - Rust"
[6]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/python/user-guide/io/table_provider.html?utm_source=chatgpt.com "Custom Table Provider - Apache DataFusion"
[8]: https://datafusion.apache.org/blog/2025/07/16/datafusion-48.0.0/ "Apache DataFusion 48.0.0 Released - Apache DataFusion Blog"
[9]: https://github.com/apache/datafusion/issues/17374 "Stabilize FFI Boundary · Issue #17374 · apache/datafusion · GitHub"

## DataFusion plugin ABI spec (Rust↔Rust, in-process) — **v1**

This is a **concrete, codegen-ready ABI** for runtime-loaded DataFusion extensions that export:

1. **`TableProvider` plugins** (data sources) via `FFI_TableProvider`
2. **UDF bundles** (Scalar + Aggregate + Window) via `FFI_ScalarUDF / FFI_AggregateUDF / FFI_WindowUDF`

It is built on **`datafusion-ffi`** (stable FFI structs + Foreign wrappers) and optionally **`abi_stable`** (root module + version/layout checks). `datafusion-ffi` explicitly targets runtime-loaded modular interfaces as a primary use case (beyond datafusion-python). ([Docs.rs][1])

> **Reality check / motivation:** DataFusion’s FFI boundary is still stabilizing; there were breaking changes between DF 48→49 and planned between 49→50. Your plugin needs a hard **handshake gate** and must fail fast on incompatibility. ([GitHub][2])

---

# 0) Ground rules (must-haves for correctness)

### 0.1 “FFI major” compatibility

`datafusion_ffi::version()` returns the **major version of the FFI implementation** and is explicitly intended for compatibility checks across the unsafe boundary. ([Docs.rs][3])

### 0.2 Foreign wrappers never touch `private_data`

Every `FFI_*` struct is paired with a `Foreign*` consumer-side wrapper; consumer code must not access `private_data`, even if it “works locally”. `FFI_TableProvider` docs spell this out explicitly. ([Docs.rs][4])

### 0.3 Lifetime: host must keep the dynamic library loaded while any exported objects exist

`datafusion-ffi` objects often call back across the boundary during `Drop`/`release` to free producer-private state. The crate docs explain the release mechanism and why it’s necessary. ([Docs.rs][1])

### 0.4 TaskContext/codec plumbing is part of the ABI

Recent `datafusion-ffi` changes require a **`TaskContextProvider`** (commonly `SessionContext`) and optionally a `LogicalExtensionCodec` when constructing key FFI structs (including `FFI_TableProvider`). The upgrade guide provides the canonical construction patterns. ([DataFusion][5])

---

# 1) ABI surface: **exported root module** (recommended) or **raw symbols**

You asked for “exported symbols (or an abi_stable module root)”. The most robust pattern is **`abi_stable` root module**, because it does:

* load-time ABI/header checks + version checks + layout checks
* a structured export table (“module of function pointers”) you can evolve

`abi_stable` root modules are exported via `#[export_root_module]`. ([Docs.rs][6])
And loaded via `RootModule::load_from*`, which documents compatibility errors like `InvalidAbiHeader`, `IncompatibleVersionNumber`, and `AbiInstability`. ([Docs.rs][7])

Below I specify the ABI in **module form** (best), then give a minimal “raw symbol” equivalent.

---

# 2) Handshake schema (version gate)

### 2.1 Manifest struct (ABI v1)

Use a fixed, append-only manifest with a `struct_size` field (classic C ABI evolution pattern). Keep it **pure data** and use `abi_stable` types for safe cross-dylib ownership.

```rust
// df_plugin_api/src/manifest.rs
use abi_stable::StableAbi;
use abi_stable::std_types::{RString, RVec};

pub const DF_PLUGIN_ABI_MAJOR: u16 = 1;
pub const DF_PLUGIN_ABI_MINOR: u16 = 0;

// Bitflags: advertise what the plugin can export
pub mod caps {
    pub const TABLE_PROVIDER: u64 = 1 << 0;
    pub const SCALAR_UDF:     u64 = 1 << 1;
    pub const AGG_UDF:        u64 = 1 << 2;
    pub const WINDOW_UDF:     u64 = 1 << 3;
}

#[repr(C)]
#[derive(Debug, StableAbi, Clone)]
pub struct DfPluginManifestV1 {
    pub struct_size: u32,

    // This spec’s ABI version
    pub plugin_abi_major: u16,
    pub plugin_abi_minor: u16,

    // datafusion_ffi::version() (FFI ABI major)
    pub df_ffi_major: u64,

    // Producer’s DataFusion/Arrow majors (policy gate; keep small ints)
    pub datafusion_major: u16,
    pub arrow_major: u16,

    // Human identity
    pub plugin_name: RString,
    pub plugin_version: RString,   // semver string
    pub build_id: RString,         // git SHA / build hash / reproducibility token

    // Capabilities + feature strings
    pub capabilities: u64,
    pub features: RVec<RString>,   // e.g., ["udfs:regex", "provider:s3"]
}
```

### 2.2 Host-side validation algorithm

**Hard gate (must pass):**

* `manifest.plugin_abi_major == DF_PLUGIN_ABI_MAJOR`
* `manifest.df_ffi_major == datafusion_ffi::version()` (FFI ABI major) ([Docs.rs][3])
* `manifest.struct_size >= size_of::<DfPluginManifestV1>()` (or at least the fields you read)

**Policy gate (recommended):**

* `manifest.datafusion_major == host_datafusion_major`
* `manifest.arrow_major == host_arrow_major`

Even though `datafusion-ffi` aims to provide stability across DataFusion versions, it still recommends using the same DataFusion version on both sides. ([Docs.rs][1])
And again: FFI boundary churn is real; strict gating is safer. ([GitHub][2])

---

# 3) Export schema (what the plugin provides)

You want (1) a **TableProvider** plugin and (2) a **UDF bundle**. Define a single export surface that can provide both:

```rust
// df_plugin_api/src/lib.rs
use abi_stable::StableAbi;
use abi_stable::std_types::{RResult, RString, RVec, RStr, ROption};

use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_ffi::udwf::FFI_WindowUDF;
use datafusion_ffi::proto::FFI_LogicalExtensionCodec;

use crate::manifest::DfPluginManifestV1;

pub type DfResult<T> = RResult<T, RString>;

#[repr(C)]
#[derive(StableAbi)]
pub struct DfUdfBundleV1 {
    pub scalar: RVec<FFI_ScalarUDF>,
    pub aggregate: RVec<FFI_AggregateUDF>,
    pub window: RVec<FFI_WindowUDF>,
}

#[repr(C)]
#[derive(StableAbi)]
pub struct DfPluginExportsV1 {
    pub table_provider_names: RVec<RString>,   // enumerate
    pub udf_bundle: DfUdfBundleV1,             // bulk-export UDFs
}

#[repr(C)]
#[derive(StableAbi)]
pub struct DfPluginMod {
    pub manifest: extern "C" fn() -> DfPluginManifestV1,

    // List everything (no host context needed)
    pub exports: extern "C" fn() -> DfPluginExportsV1,

    // Instantiate a table provider by name, embedding host codec/task ctx provider
    pub create_table_provider: extern "C" fn(
        name: RStr<'_>,
        options_json: ROption<RString>,            // stable, extensible args channel
        ffi_codec: FFI_LogicalExtensionCodec,      // host-provided (holds TaskContextProvider)
    ) -> DfResult<FFI_TableProvider>,
}

// abi_stable will generate DfPluginMod_Ref (prefix ref) automatically
```

**Why `FFI_LogicalExtensionCodec` in `create_table_provider`:**
Newer `FFI_TableProvider::new` requires a `TaskContextProvider` and optionally a `LogicalExtensionCodec`, and `new_with_ffi_codec` embeds the codec. ([Docs.rs][4])
The FFI crate explains that many FFI structs need a TaskContextProvider to (de)serialize via `datafusion-proto`, and it holds a **Weak** ref; the provider must stay valid for the lifetime of calls. ([Docs.rs][1])

---

# 4) Producer skeletons (plugin side)

## 4.1 `cdylib` crate layout

* `df_plugin_api` (interface crate): types + module definition above
* `df_plugin_impl` (implementation crate): compiled as `cdylib`, implements the module and exports it

## 4.2 Export the root module (abi_stable)

```rust
// df_plugin_impl/src/lib.rs
use abi_stable::prefix_type::PrefixTypeTrait;
use abi_stable::export_root_module;

use df_plugin_api::{DfPluginMod, DfPluginMod_Ref};

#[export_root_module]
pub fn df_plugin_root() -> DfPluginMod_Ref {
    DfPluginMod {
        manifest: my_manifest,
        exports: my_exports,
        create_table_provider: my_create_table_provider,
    }
    .leak_into_prefix()
}
```

This is the canonical `#[export_root_module]` pattern. ([Docs.rs][6])

---

## 4.3 TableProvider plugin producer: build `FFI_TableProvider`

### 4.3.1 Build the underlying provider

Use any `TableProvider` implementation you want; the ABI only cares about the returned `FFI_TableProvider`.

### 4.3.2 Wrap into `FFI_TableProvider`

Use `FFI_TableProvider::new_with_ffi_codec(...)` if you want the host to control codecs/context.

`FFI_TableProvider` provides:

* a stable struct layout + foreign wrapper contract ([Docs.rs][4])
* `new(...)` signature: `(provider, can_support_pushdown_filters, runtime, task_ctx_provider, logical_codec)` ([Docs.rs][4])
* `new_with_ffi_codec(...)` signature: `(provider, can_support_pushdown_filters, runtime, logical_codec: FFI_LogicalExtensionCodec)` ([Docs.rs][4])
* and `version: fn() -> u64` field returning the **major DataFusion version number of the provider** ([Docs.rs][4])

```rust
use abi_stable::std_types::{RResult, RString, RStr, ROption};
use df_plugin_api::DfResult;

use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_ffi::proto::FFI_LogicalExtensionCodec;

use datafusion::datasource::TableProvider;
use std::sync::Arc;

extern "C" fn my_create_table_provider(
    name: RStr<'_>,
    options_json: ROption<RString>,
    ffi_codec: FFI_LogicalExtensionCodec,
) -> DfResult<FFI_TableProvider> {
    // 1) parse options_json (plugin-defined contract)
    // 2) build Arc<dyn TableProvider + Send>
    let provider: Arc<dyn TableProvider + Send> = build_provider(name, options_json)
        .map_err(|e| RString::from(e))?;

    // 3) wrap (host codec already includes task context provider)
    let runtime = None; // or Some(tokio::runtime::Handle::current())
    let ffi_tp = FFI_TableProvider::new_with_ffi_codec(
        provider,
        /*can_support_pushdown_filters=*/ true,
        runtime,
        ffi_codec,
    );

    RResult::ROk(ffi_tp)
}

// your internal builder
fn build_provider(
    _name: RStr<'_>,
    _options_json: ROption<RString>,
) -> Result<Arc<dyn TableProvider + Send>, String> {
    // TODO: return a real provider; use MemTable for toy, ListingTable for files, etc.
    Err("not implemented".into())
}
```

---

## 4.4 UDF bundle producer: export `FFI_ScalarUDF / FFI_AggregateUDF / FFI_WindowUDF`

### 4.4.1 Post-48 FFI support for UDAF + UDWF

DataFusion 48 explicitly calls out FFI support for `AggregateUDF` and `WindowUDF` to enable shared libraries to pass functions back and forth. ([DataFusion][8])

### 4.4.2 Create native UDF objects → convert to FFI structs

All of these FFI structs are `#[repr(C)]` stable ABI structs with explicit `release` callbacks and private data pointers (e.g., `FFI_ScalarUDF` defines `return_field_from_args`, `invoke_with_args`, `coerce_types`, `release`, `private_data`). ([Docs.rs][9])

Producer conversion is trivial because each struct implements `From<Arc<…UDF…>>`:

* `FFI_ScalarUDF: From<Arc<ScalarUDF>>` ([Docs.rs][9])
* `FFI_AggregateUDF: From<Arc<AggregateUDF>>` ([Docs.rs][10])
* `FFI_WindowUDF: From<Arc<WindowUDF>>` ([Docs.rs][11])

```rust
use abi_stable::std_types::RVec;
use df_plugin_api::DfUdfBundleV1;

use datafusion::logical_expr::ScalarUDF;
use datafusion_expr::AggregateUDF;
use datafusion::logical_expr::WindowUDF;

use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_ffi::udwf::FFI_WindowUDF;

use std::sync::Arc;

extern "C" fn my_exports() -> df_plugin_api::DfPluginExportsV1 {
    // build real UDFs here
    let scalar_udfs: Vec<Arc<ScalarUDF>> = vec![];
    let agg_udfs: Vec<Arc<AggregateUDF>> = vec![];
    let win_udfs: Vec<Arc<WindowUDF>> = vec![];

    df_plugin_api::DfPluginExportsV1 {
        table_provider_names: RVec::from(vec![]),

        udf_bundle: DfUdfBundleV1 {
            scalar: RVec::from(scalar_udfs.into_iter().map(Into::into).collect::<Vec<FFI_ScalarUDF>>()),
            aggregate: RVec::from(agg_udfs.into_iter().map(Into::into).collect::<Vec<FFI_AggregateUDF>>()),
            window: RVec::from(win_udfs.into_iter().map(Into::into).collect::<Vec<FFI_WindowUDF>>()),
        },
    }
}
```

**Scalar UDF FFI note:** the upgrade guide states the FFI scalar UDF structure no longer has a `return_type` call because `ForeignScalarUDF` uses `return_field_from_args`. Don’t design ABI expecting `return_type` here. ([DataFusion][5])

---

# 5) Consumer skeletons (host side)

## 5.1 Load plugin module (abi_stable) + validate handshake

```rust
use abi_stable::library::RootModule;
use std::path::Path;

use df_plugin_api::{DfPluginMod_Ref, manifest::DF_PLUGIN_ABI_MAJOR};
use datafusion_ffi;

fn load_plugin(path: &Path) -> anyhow::Result<DfPluginMod_Ref> {
    // RootModule::load_from* does header + layout + version checks; see docs for failure modes.
    // :contentReference[oaicite:24]{index=24}
    let plugin = DfPluginMod_Ref::load_from_file(path).map_err(anyhow::Error::msg)?;

    let m = (plugin.manifest)();
    if m.plugin_abi_major != DF_PLUGIN_ABI_MAJOR {
        anyhow::bail!("plugin ABI major mismatch");
    }
    if m.df_ffi_major != datafusion_ffi::version() {
        anyhow::bail!("datafusion-ffi ABI major mismatch");
    }

    Ok(plugin)
}
```

`RootModule::load_from*` warning: don’t call it from static initializers inside a dynamic library; call it from normal runtime code (docs explicitly warn about this). ([Docs.rs][7])

## 5.2 Build host codec (TaskContextProvider) to pass into provider creation

Upgrade guide canonical pattern (codec + ctx + `FFI_LogicalExtensionCodec`). ([DataFusion][5])

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_ffi::proto::FFI_LogicalExtensionCodec;

fn host_ffi_codec(ctx: Arc<SessionContext>) -> FFI_LogicalExtensionCodec {
    let codec = Arc::new(DefaultLogicalExtensionCodec {});
    FFI_LogicalExtensionCodec::new(codec, None, ctx)
}
```

## 5.3 Import a `TableProvider` and register it

`FFI_TableProvider` is a stable wrapper; consumer must not touch `private_data` (use Foreign wrapper / conversions). ([Docs.rs][4])
There is `From<&FFI_TableProvider> for Arc<dyn TableProvider>` on the type. ([Docs.rs][4])

```rust
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

fn register_provider(
    ctx: &SessionContext,
    plugin: &df_plugin_api::DfPluginMod_Ref,
) -> datafusion::error::Result<()> {
    let ffi_codec = host_ffi_codec(Arc::new(ctx.clone()));

    // Instantiate provider (plugin embeds codec inside returned FFI_TableProvider)
    let ffi_tp = (plugin.create_table_provider)(
        "my_table".into(),
        abi_stable::std_types::ROption::RNone,
        ffi_codec,
    )
    .into_result()
    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))))?;

    // Convert to Arc<dyn TableProvider> and register
    let tp: Arc<dyn TableProvider> = (&ffi_tp).into();
    ctx.register_table("my_table", tp)?;

    Ok(())
}
```

**Lifetime rule:** the plugin dylib must remain loaded while `ffi_tp` (and the `tp` derived from it) exist, because drops/releases may call back across the boundary. ([Docs.rs][1])

## 5.4 Import UDF bundle and register (Scalar + Aggregate + Window)

### Scalar UDF (FFI → `Arc<dyn ScalarUDFImpl>` → `ScalarUDF`)

* `FFI_ScalarUDF` provides `From<&FFI_ScalarUDF> for Arc<dyn ScalarUDFImpl>`. ([Docs.rs][9])
* Upgrade guide canonical wrapper: `ScalarUDF::new_from_shared_impl(foreign_impl)`. ([DataFusion][5])

```rust
use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl};
use std::sync::Arc;

fn import_scalar(ffi: &datafusion_ffi::udf::FFI_ScalarUDF) -> ScalarUDF {
    let impl_: Arc<dyn ScalarUDFImpl> = ffi.into();
    ScalarUDF::new_from_shared_impl(impl_)
}
```

### Aggregate UDF (FFI → `Arc<dyn AggregateUDFImpl>` → `AggregateUDF`)

* `FFI_AggregateUDF` provides `From<&FFI_AggregateUDF> for Arc<dyn AggregateUDFImpl>`. ([Docs.rs][10])
* `AggregateUDF::new_from_shared_impl` exists explicitly. ([Docs.rs][12])

```rust
use datafusion_expr::{AggregateUDF, AggregateUDFImpl};
use std::sync::Arc;

fn import_aggregate(ffi: &datafusion_ffi::udaf::FFI_AggregateUDF) -> AggregateUDF {
    let impl_: Arc<dyn AggregateUDFImpl> = ffi.into();
    AggregateUDF::new_from_shared_impl(impl_)
}
```

### Window UDF (FFI → `Arc<dyn WindowUDFImpl>` → `WindowUDF`)

* `FFI_WindowUDF` provides `From<&FFI_WindowUDF> for Arc<dyn WindowUDFImpl>`. ([Docs.rs][11])
* `WindowUDF::new_from_shared_impl` exists explicitly. ([Docs.rs][13])

```rust
use datafusion::logical_expr::{WindowUDF, WindowUDFImpl};
use std::sync::Arc;

fn import_window(ffi: &datafusion_ffi::udwf::FFI_WindowUDF) -> WindowUDF {
    let impl_: Arc<dyn WindowUDFImpl> = ffi.into();
    WindowUDF::new_from_shared_impl(impl_)
}
```

### Register all

```rust
fn register_udfs(ctx: &datafusion::prelude::SessionContext, plugin: &df_plugin_api::DfPluginMod_Ref) {
    let exports = (plugin.exports)();

    for ffi in exports.udf_bundle.scalar.iter() {
        ctx.register_udf(import_scalar(ffi));
    }
    for ffi in exports.udf_bundle.aggregate.iter() {
        ctx.register_udaf(import_aggregate(ffi));
    }
    for ffi in exports.udf_bundle.window.iter() {
        ctx.register_udwf(import_window(ffi));
    }
}
```

---

# 6) Minimal “raw symbol” ABI (if you don’t want abi_stable RootModule)

If you want the smallest possible exported surface (e.g., use `libloading`), you can export these symbols:

* `df_plugin_manifest_v1(out: *mut DfPluginManifestV1)`
* `df_plugin_exports_v1(out: *mut DfPluginExportsV1)`
* `df_plugin_create_table_provider_v1(name_ptr, opts_ptr, ffi_codec, out_provider) -> status`

But if you do this, **you must implement your own layout/version checks** that `abi_stable` otherwise gives you for free (including ABI header compatibility and root module layout checks). ([Docs.rs][7])
Given FFI churn risk, I strongly recommend the RootModule pattern.

---

# 7) Operational constraints (the things that break production)

1. **FFI ABI drift exists** — you must gate on `datafusion_ffi::version()` and your own plugin ABI major. ([Docs.rs][3])
2. **TaskContextProvider must remain valid** — `FFI_TaskContextProvider` holds a `Weak` ref; if the provider dies, later (de)serialization may fail or worse. ([Docs.rs][1])
3. **Never unload the dylib early** — drops/releases may execute foreign calls. ([Docs.rs][1])
4. **Don’t touch `private_data` on the consumer side** — use Foreign wrappers / stable functions only. ([Docs.rs][4])

---

If you want, the next “artifact” that makes this immediately actionable is a **repo skeleton** (`df_plugin_api`, `df_plugin_impl`, `df_plugin_host`) with:

* a toy `MemTable` provider plugin,
* one scalar UDF + one aggregate UDF + one window UDF,
* an `EXPLAIN FORMAT tree` snapshot test verifying the functions/providers are actually used in plans (and that the plugin stays loaded for the duration).

[1]: https://docs.rs/crate/datafusion-ffi/latest "datafusion-ffi 52.1.0 - Docs.rs"
[2]: https://github.com/apache/datafusion/issues/17374?utm_source=chatgpt.com "Stabilize FFI Boundary · Issue #17374 · apache/datafusion"
[3]: https://docs.rs/datafusion-ffi/latest/datafusion_ffi/ "datafusion_ffi - Rust"
[4]: https://docs.rs/datafusion-ffi/latest/datafusion_ffi/table_provider/struct.FFI_TableProvider.html "FFI_TableProvider in datafusion_ffi::table_provider - Rust"
[5]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[6]: https://docs.rs/abi_stable/latest/abi_stable/attr.export_root_module.html?utm_source=chatgpt.com "export_root_module in abi_stable - Rust"
[7]: https://docs.rs/abi_stable/latest/abi_stable/library/trait.RootModule.html?utm_source=chatgpt.com "RootModule in abi_stable::library - Rust"
[8]: https://datafusion.apache.org/blog/2025/07/16/datafusion-48.0.0/?utm_source=chatgpt.com "Apache DataFusion 48.0.0 Released"
[9]: https://docs.rs/datafusion-ffi/latest/datafusion_ffi/udf/struct.FFI_ScalarUDF.html "FFI_ScalarUDF in datafusion_ffi::udf - Rust"
[10]: https://docs.rs/datafusion-ffi/latest/datafusion_ffi/udaf/struct.FFI_AggregateUDF.html "FFI_AggregateUDF in datafusion_ffi::udaf - Rust"
[11]: https://docs.rs/datafusion-ffi/latest/datafusion_ffi/udwf/struct.FFI_WindowUDF.html "FFI_WindowUDF in datafusion_ffi::udwf - Rust"
[12]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.AggregateUDF.html "AggregateUDF in datafusion_expr - Rust"
[13]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.WindowUDF.html "WindowUDF in datafusion::logical_expr - Rust"
