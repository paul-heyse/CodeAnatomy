# Polars querying deep dive: Expressions + contexts

This section is the “authoring IR” layer of Polars: how you *construct* computation (expressions) and how you *execute* it (contexts). Everything else (lazy optimization, streaming, IO, Delta, etc.) is downstream of this.

---

## 1) Expression IR fundamentals

### 1.1 `Expr` is a lazy transformation node

In Polars, an **expression** is a *lazy representation* of a transformation—an IR fragment you compose into larger IR, and later execute in a context. ([Polars User Guide][1])
The public Python type for this is `polars.Expr`, “expressions that can be used in various contexts.” ([Polars User Guide][2])

Key implication: you should think of expressions as **pure, composable building blocks**. They don’t “run” until you plug them into a context (select/with_columns/filter/group_by/over/list.eval/etc.). ([Polars User Guide][1])

---

### 1.2 `DataFrame` vs `LazyFrame`: where expressions live

Both `DataFrame` (eager) and `LazyFrame` (lazy) accept expressions, but the “first-class” execution model for performance is `LazyFrame`: it is a **lazy computation graph/query** that enables **whole-query optimisation + parallelism** and is described as the *preferred / highest-performance* mode. ([Polars User Guide][3])

Minimal mental model:

* `Expr`: node(s) in a query plan / compute graph
* `DataFrame`: eager execution, still expression-based
* `LazyFrame`: builds a plan; executes later (`collect()`), enabling optimization passes and execution strategies

(You’ll usually author the same expressions either way.)

---

### 1.3 `IntoExpr` coercions: the most important “footgun” in Polars authoring

Most Polars APIs accept `IntoExpr` (roughly: `Expr` | column name | literal-ish input). The coercion rules are consistent across core contexts:

* **Strings are parsed as column names**
* **Non-expression inputs are parsed as literals**

You can see this explicitly in `DataFrame.select` / `LazyFrame.select` and `DataFrame.with_columns` / `LazyFrame.with_columns` docs. ([Polars User Guide][4])

**Concrete consequence:** if you write `"A"` where Polars expects `IntoExpr`, it will try to resolve a column named `"A"` (not the string literal `"A"`). This matters *everywhere*, but it matters **most** in conditional expressions (`when/then/otherwise`). ([Polars User Guide][5])

---

### 1.4 Expression constructors you use constantly

#### `pl.col(...)`: column reference expressions

`pl.col` creates column expressions; it also supports an attribute shorthand `pl.col.foo` but the docs call the function-call syntax the idiomatic form and note readability/expressiveness tradeoffs with attribute lookup. ([Polars User Guide][6])

```python
import polars as pl

e1 = pl.col("foo")
e2 = pl.col("foo") + pl.col("bar")
e3 = pl.col.foo  # shorthand; handy, but less explicit
```

#### `pl.lit(value, dtype=None, allow_object=False)`: literal expressions

`pl.lit` returns an expression representing a literal value. ([Polars User Guide][7])

This is the “escape hatch” to prevent the *string-as-column-name* parsing rule from biting you, especially inside `when/then/otherwise`. ([Polars User Guide][5])

```python
pl.lit("A")          # literal string
pl.lit(1)            # literal int
pl.lit(None)         # literal null
pl.lit("1", dtype=pl.Int64)  # force dtype
```

---

### 1.5 Naming: `alias` + name transforms

Polars names outputs based on expression roots unless you override. The core primitive is:

* `Expr.alias(name)` renames an expression’s output; docs explicitly call out using it to avoid overwriting and to avoid duplicate names from literals. ([Polars User Guide][8])

```python
df = df.with_columns(
    (pl.col("a") + 10).alias("a_plus_10"),
    pl.lit(True).alias("flag"),
)
```

You can also systematically rename by transforming expression root names via `expr.name.*` (prefix/suffix/map/etc.). ([Polars User Guide][9])

---

### 1.6 Type discipline: `cast(strict=...)` is not “just a cast”

`Expr.cast(dtype, strict=True, ...)` casts types. Crucial nuance: `strict=True` can raise **after predicate pushdown**, and `strict=False` converts invalid casts to `null`. ([Polars User Guide][10])

This means `cast(strict=True)` is a correctness tool, not just formatting—useful when you want the pipeline to *fail fast* on bad input, and `strict=False` when you want “null on invalid.” ([Polars User Guide][11])

---

## 2) The four core contexts (the “Polars basics” you build everything on)

Polars explicitly frames “contexts” as the mechanism that executes expressions; the same expression can produce different results depending on context. ([Polars User Guide][1])

### 2.1 `select` context: projection + derived columns

**API surface**

* `DataFrame.select(*exprs, **named_exprs)` ([Polars User Guide][4])
* `LazyFrame.select(*exprs, **named_exprs)` ([Polars User Guide][12])
* `pl.select(*exprs, eager=True/False, **named_exprs)` (context-free convenience wrapper; returns `DataFrame` or `LazyFrame`) ([Polars User Guide][13])

**Semantics**

* Builds a new frame containing only selected/derived columns.
* Coercions: strings→columns, non-exprs→literals. ([Polars User Guide][4])

**Minimal patterns**

```python
import polars as pl

df = pl.DataFrame({"a": [1,2,3], "b": [10,20,30]})

out = df.select(
    "a",
    (pl.col("b") * 2).alias("b2"),
    const=pl.lit("x"),
)
```

**Parallel vs sequential evaluation**
Polars exposes `LazyFrame.select_seq(...)` that runs expressions sequentially “instead of in parallel” for cases where work per expression is cheap. ([Polars User Guide][14])
(That implies the default `select` path is parallel evaluation at the expression level.)

---

### 2.2 `with_columns` context: add/replace columns (treat expressions as independent)

**API surface**

* `DataFrame.with_columns(*exprs, **named_exprs)` ([Polars User Guide][15])
* `LazyFrame.with_columns(*exprs, **named_exprs)` ([Polars User Guide][16])
* Sequential variants: `with_columns_seq` on both `DataFrame` and `LazyFrame` ([Polars User Guide][17])

**Semantics**

* Adds new columns; if names collide, replaces existing columns. ([Polars User Guide][15])
* Does **not** copy existing data when producing the new DataFrame (important for performance intuition). ([Polars User Guide][15])
* Coercions: strings→columns; non-exprs→literals. ([Polars User Guide][15])

**“Independence rule” (authoring discipline)**
By default Polars evaluates expressions “in parallel” (the existence of `with_columns_seq` is the documented switch to sequential execution). ([Polars User Guide][18])
As a result, **treat expressions passed in the same `with_columns` call as order-independent**. If you need a dependency, either:

* compose into a *single* expression, or
* split into multiple `with_columns` calls.

**Minimal pattern**

```python
df = df.with_columns(
    (pl.col("a") ** 2).alias("a2"),
    (pl.col("b") / pl.col("a")).alias("b_over_a"),
)
```

**Sequential variants**
`with_columns_seq` is explicitly documented as “run all expression sequentially instead of in parallel.” ([Polars User Guide][18])
This is a performance knob (not a semantic guarantee for dependency ordering—author as if expressions are independent unless you explicitly stage calls).

---

### 2.3 `filter` context: row filtering (null is not True)

**API surface**

* `DataFrame.filter(*predicates, **constraints)` ([Polars User Guide][19])
* `LazyFrame.filter(*predicates, **constraints)` ([Polars User Guide][20])

**Semantics**

* Keeps rows where predicate evaluates to **True**; rows where predicate is **False or null are discarded**. ([Polars User Guide][19])
* `constraints` provides a “column = value” shorthand, combined with `&` (AND). ([Polars User Guide][19])

**Minimal pattern**

```python
out = df.filter(
    (pl.col("a") > 0) & (pl.col("b").is_not_null()),
    category="foo",  # constraint shorthand => pl.col("category") == "foo"
)
```

**Aggregation-level filtering vs DataFrame-level filtering**
There is also `Expr.filter(...)` which filters *within an expression*, documented as “mostly useful in an aggregation context,” and it also discards null predicate results. ([Polars User Guide][21])
This is the correct tool for “filter-then-aggregate” per group:

```python
out = df.group_by("k").agg(
    pl.col("v").filter(pl.col("is_valid")).sum().alias("v_sum_valid")
)
```

---

### 2.4 `group_by` context: aggregation over groups

**API surface**

* `LazyFrame.group_by(*by, maintain_order=False, **named_by) -> LazyGroupBy` ([Polars User Guide][22])
* Then `GroupBy.agg(...)` / `LazyGroupBy.agg(...)` (the core “aggregation context”) ([Polars User Guide][23])

**Semantics**

* `group_by` partitions rows into groups; `agg` evaluates expressions per group.
* `maintain_order=True` makes group order consistent with input but is slower; docs explicitly state it blocks the streaming engine. ([Polars User Guide][22])
* Aggregations can produce scalars or lists; the `group_by(..., maintain_order=True).agg(pl.col("c"))` example shows list results per group. ([Polars User Guide][22])

**Minimal patterns**

```python
out = df.group_by("k").agg(
    pl.col("v").sum().alias("v_sum"),
    pl.col("v").mean().alias("v_mean"),
)
```

---

## 3) “Non-core but essential” expression contexts and bindings

These are still “expressions + contexts”—they’re just not part of the canonical four.

### 3.1 Window context: `Expr.over(...)` (“group_by then join back”)

`Expr.over` computes an expression “over the given groups,” documented as similar to doing a group-by aggregation and joining back; outcome is similar to SQL window functions. ([Polars User Guide][24])
The user guide explicitly contrasts shapes: `group_by` usually yields rows per group, while `over` usually preserves original row count. ([Polars User Guide][25])

```python
out = df.with_columns(
    pl.col("v").sum().over("k").alias("k_sum_v"),     # per-group scalar mapped to each row
    pl.col("v").rank().over("k").alias("k_rank_v"),   # per-group window op
)
```

The mapping behavior is controlled by `mapping_strategy` (see window functions guide). ([Polars User Guide][25])

---

### 3.2 List element context: `list.eval(...)` + `pl.element()`

`Expr.list.eval(expr, parallel=False)` runs a Polars expression against list elements; `pl.element()` binds “the element being evaluated in an eval or filter expression.” ([Polars User Guide][26])

```python
df = pl.DataFrame({"a":[1,8,3], "b":[4,5,2]})

out = df.with_columns(
    pl.concat_list(["a","b"])
      .list.eval(pl.element().rank())
      .alias("rank")
)
```

`list.eval(..., parallel=True)` exists, but docs warn not to enable blindly (worth it only if there’s enough per-thread work). ([Polars User Guide][26])

---

### 3.3 Struct context: `pl.struct(...)` + `.struct.field(...)` and unnesting

Polars expressions normally operate on a single series; the user guide highlights `Struct` as the type that enables multiple columns as input/output to an expression. ([Polars User Guide][27])
`pl.struct(...)` collects columns into a struct; `.struct.field(...)` retrieves fields. ([Polars User Guide][28])

```python
df2 = df.select(pl.struct(pl.all()).alias("s"))  # collect all columns into one struct column
out = df2.select(
    pl.col("s").struct.field("a").alias("a_from_struct"),
)
```

---

### 3.4 Horizontal (row-wise) contexts: `all_horizontal`, `any`, `fold`

These are still expressions, but the “context” is “across columns horizontally” rather than down a single column:

* `pl.all_horizontal(...)` computes logical AND across columns, and notes **Kleene logic** for null handling (if any null and no False ⇒ null). ([Polars User Guide][29])
* `pl.any(..., ignore_nulls=True/False)` documents both “ignore nulls” and Kleene logic mode. ([Polars User Guide][30])
* `pl.fold(acc, function, exprs, ...)` is the general row-wise reducer across multiple expressions/columns. ([Polars User Guide][31])

```python
out = df.with_columns(
    pl.all_horizontal(pl.col("x") > 0, pl.col("y") > 0).alias("both_pos"),
    pl.any("flag_a", "flag_b", ignore_nulls=False).alias("any_flag_kleene"),
)
```

---

### 3.5 Selectors: late-bound column sets that broadcast expressions

Selectors are an expression-adjacent construct that represent *sets of columns* by name/dtype/properties, support set algebra (`|`, `&`, `-`, `~`), and can broadcast expressions over selected columns. ([Polars User Guide][32])

```python
import polars as pl
import polars.selectors as cs

df = pl.DataFrame({"w":["x","y"], "x":[1,2], "y":[3.0,4.0]})

out = df.group_by(by=cs.string()).agg(cs.numeric().sum())
```

That exact pattern is shown in the selector docs (`cs.string()` for group keys, `cs.numeric().sum()` for aggregations). ([Polars User Guide][32])

---

## 4) High-value failure modes and authoring rules (for programming agents)

### 4.1 “Strings are columns” everywhere (fix with `pl.lit`)

* Core contexts parse strings as column names; non-exprs become literals. ([Polars User Guide][4])
* `when/then/otherwise` explicitly repeats this rule: `then("string")` is a column lookup; use `lit()` for string values. ([Polars User Guide][5])

### 4.2 `when-then-otherwise` evaluates branches in parallel

Polars warns that **all expressions in a `when-then-otherwise` chain are computed in parallel and filtered afterwards**, so each branch must be valid regardless of the condition. ([Polars User Guide][5])
This affects patterns like “divide only when denominator != 0” — you must construct an expression that is safe in both branches (or pre-mask/cast) rather than relying on short-circuit semantics.

### 4.3 `filter` treats null as “not True”

Both `DataFrame.filter` and `LazyFrame.filter` state: rows where predicate is not True are discarded, including nulls. ([Polars User Guide][19])
Same for `Expr.filter` used inside aggregation contexts. ([Polars User Guide][21])

### 4.4 `cast(strict=True)` interacts with pushdown

`Expr.cast` documents that strict mode can raise after predicate pushdown; strict=False yields nulls on invalid casts. ([Polars User Guide][10])

### 4.5 `group_by(maintain_order=True)` blocks streaming

Polars explicitly notes this; it’s a correctness-vs-throughput knob you should choose intentionally. ([Polars User Guide][22])

---

If you want the next increment immediately, the natural follow-on within “Expressions + contexts” is a dense catalog of the **expression namespaces** (string/datetime/list/struct/window/horizontal) and a minimal “shape + dtype + null semantics” rubric per namespace—using the public `Expr` surface as the index. ([Polars User Guide][2])

[1]: https://docs.pola.rs/user-guide/concepts/expressions-and-contexts/ "Expressions and contexts - Polars user guide"
[2]: https://docs.pola.rs/py-polars/html/reference/expressions/index.html "Expressions — Polars  documentation"
[3]: https://docs.pola.rs/py-polars/html/reference/lazyframe/index.html "LazyFrame — Polars  documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.select.html "polars.DataFrame.select — Polars  documentation"
[5]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.when.html "polars.when — Polars  documentation"
[6]: https://docs.pola.rs/py-polars/html/reference/expressions/col.html "polars.col — Polars  documentation"
[7]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.lit.html "polars.lit — Polars  documentation"
[8]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.alias.html "polars.Expr.alias — Polars  documentation"
[9]: https://docs.pola.rs/py-polars/html/reference/expressions/name.html "Name — Polars  documentation"
[10]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.cast.html "polars.Expr.cast — Polars  documentation"
[11]: https://docs.pola.rs/user-guide/expressions/casting/ "Casting - Polars user guide"
[12]: https://docs.pola.rs/api/python/version/0.18/reference/lazyframe/api/polars.LazyFrame.select.html "polars.LazyFrame.select — Polars  documentation"
[13]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.select.html "polars.select — Polars  documentation"
[14]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.select_seq.html "polars.LazyFrame.select_seq — Polars  documentation"
[15]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.with_columns.html "polars.DataFrame.with_columns — Polars  documentation"
[16]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.with_columns.html?utm_source=chatgpt.com "polars.LazyFrame.with_columns — Polars documentation"
[17]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.with_columns_seq.html?utm_source=chatgpt.com "polars.DataFrame.with_columns_seq"
[18]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.with_columns_seq.html "polars.LazyFrame.with_columns_seq — Polars  documentation"
[19]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.filter.html "polars.DataFrame.filter — Polars  documentation"
[20]: https://docs.pola.rs/docs/python/dev/reference/lazyframe/api/polars.LazyFrame.filter.html "polars.LazyFrame.filter — Polars  documentation"
[21]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.filter.html "polars.Expr.filter — Polars  documentation"
[22]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.group_by.html "polars.LazyFrame.group_by — Polars  documentation"
[23]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.dataframe.group_by.GroupBy.agg.html?utm_source=chatgpt.com "polars.dataframe.group_by.GroupBy.agg"
[24]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.over.html "polars.Expr.over — Polars  documentation"
[25]: https://docs.pola.rs/user-guide/expressions/window-functions/ "Window functions - Polars user guide"
[26]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.list.eval.html "polars.Expr.list.eval — Polars  documentation"
[27]: https://docs.pola.rs/user-guide/expressions/structs/ "Structs - Polars user guide"
[28]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.struct.html "polars.struct — Polars  documentation"
[29]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.all_horizontal.html "polars.all_horizontal — Polars  documentation"
[30]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.any.html "polars.any — Polars  documentation"
[31]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.fold.html "polars.fold — Polars  documentation"
[32]: https://docs.pola.rs/py-polars/html/reference/selectors.html "Selectors — Polars  documentation"

# Expressions + contexts: namespace catalog (shape · dtype · null semantics)

This is an **index-by-namespace** view of Polars’ public `Expr` surface, with a **minimal execution rubric** per namespace:

* **Shape**: does the expression preserve row count? can it expand/contract rows?
* **Dtype**: what input dtype(s) does it require? what output dtype(s) does it produce?
* **Null semantics**: how are nulls propagated/introduced? are there knobs (`strict`, `ignore_nulls`, etc.)?

The authoritative “everything list” for `Expr` lives in the expressions reference; this section narrows to the requested namespaces and the key shape/dtype rules. ([Polars User Guide][1])

---

## 0) Namespace access patterns (how you “index” the API)

* **Column expression root**: `pl.col("x") -> Expr`
* **Namespace dispatch**:

  * `pl.col("s").str.<method>(...)` (string)
  * `pl.col("ts").dt.<method>(...)` (temporal)
  * `pl.col("xs").list.<method>(...)` (list)
  * `pl.col("st").struct.<method>(...)` (struct)
* **Window context is not a namespace attribute**: `expr.over(...)` (window mapping + partitioning). ([Polars User Guide][2])
* **Horizontal context is mostly module-level expression functions**: `pl.all_horizontal(...)`, `pl.any_horizontal(...)`, `pl.fold(...)`, etc. ([Polars User Guide][3])

---

## 1) `expr.str.*` — String namespace

Polars groups string processing under `Expr.str` (40+ functions in the docs). ([Polars User Guide][4])
Polars’ string performance story is explicitly tied to following the Arrow columnar format specification for string storage. ([Polars User Guide][4])

### 1.1 Shape rubric

* **Row-preserving** (most methods): output has the same number of rows as input, per-row transformation.
* **Row-expanding**:

  * `str.explode()` expands each string into one row per character. ([Polars User Guide][5])
* **Row-reducing / aggregation-context**:

  * `str.concat(...)` is a vertical concatenation of all values to a single string (typically used in aggregation contexts). ([Polars User Guide][5])

### 1.2 Dtype rubric

* Input: requires **String** (Utf8) dtype.
* Output dtype patterns:

  * **Boolean** predicates: `contains`, `starts_with`, `ends_with`, etc. ([Polars User Guide][6])
  * **Numeric**: `len_bytes()` / `len_chars()` return integer lengths (docs show UInt32). ([Polars User Guide][7])
  * **String**: most transforms return String (slice/replace/case/trim).
  * **Temporal / numeric parses**: `str.strptime`, `to_date`, `to_datetime`, `to_time`, `to_integer`, `to_decimal`. ([Polars User Guide][5])

### 1.3 Null semantics rubric (and “strictness” knobs)

* Most string transforms **propagate nulls** (null in → null out).
* Regex predicates have an explicit “invalid pattern handling” control:

  * `str.contains(..., strict=True)` **raises** on invalid regex; `strict=False` masks invalid-regex cases to **null**. ([Polars User Guide][6])
* Regex replace has **capture-group semantics** that can accidentally reinterpret `$`:

  * In `str.replace_all`, `$` is treated as capture-group syntax unless you escape it as `$$` or set `literal=True`. ([Polars User Guide][8])

### 1.4 Method index (public `Expr.str` surface)

From the expression reference index (`expr.str`): ([Polars User Guide][5])

* concat: `concat([delimiter, ignore_nulls])`
* predicates/search: `contains(pattern, *, literal, strict)`, `contains_any(patterns, ...)`, `starts_with(prefix)`, `ends_with(suffix)`, `find(pattern, *, literal, strict)`
* counting: `count_match(pattern)`, `count_matches(pattern, *, literal)`
* encoding: `encode(encoding)`, `decode(encoding, *, strict)`
* explode: `explode()`
* extraction: `extract(pattern, group_index)`, `extract_all(pattern)`, `extract_groups(pattern)`
* json: `json_decode(...)`, `json_extract(...)`, `json_path_match(json_path)`
* lengths: `len_bytes()`, `len_chars()`, `lengths()`, `n_chars()`
* padding/justify: `ljust(length, fill_char)`, `rjust(length, fill_char)`, `pad_start(length, fill_char)`, `pad_end(length, fill_char)`, `zfill(length)`
* strip/trim: `strip(...)`, `strip_chars(...)`, `strip_chars_start(...)`, `strip_chars_end(...)`, `lstrip(...)`, `rstrip(...)`, `strip_prefix(prefix)`, `strip_suffix(suffix)`
* replace: `replace(pattern, value, *, literal, n)`, `replace_all(pattern, value, *, literal)`, `replace_many(patterns, replace_with, *)`
* slice/head/tail: `slice(offset, length)`, `head(n)`, `tail(n)`
* split: `split(by, *, inclusive)`, `split_exact(by, n, *, inclusive)`, `splitn(by, n)`
* parse/convert: `parse_int(base, strict)`, `strptime(dtype, format, strict, ...)`, `to_date(...)`, `to_datetime(...)`, `to_time(...)`, `to_integer(..., base, strict)`, `to_decimal(...)`
* transforms: `reverse()`, `to_lowercase()`, `to_uppercase()`, `to_titlecase()`

### 1.5 Two high-impact semantics callouts

* **Length is bytes vs characters**: `len_bytes()` is O(1) and `len_chars()` is O(n) (docs explicitly recommend `len_bytes` for ASCII). ([Polars User Guide][7])
* **Regex default**: methods like `contains` are regex-based by default and require `literal=True` if you mean “substring” rather than “regex pattern.” ([Polars User Guide][6])

---

## 2) `expr.dt.*` — Temporal namespace

The temporal namespace is the `Expr.dt` surface. ([Polars User Guide][9])
Temporal ops apply to **Date / Datetime / Time / Duration** depending on the method. ([Polars User Guide][9])

### 2.1 Shape rubric

* Nearly all temporal transforms are **row-preserving** (extract/round/offset etc.), returning one output value per input row.

### 2.2 Dtype rubric

* **Extractors**: return small ints / ints (e.g., `day`, `month`, `weekday`, etc.) depending on the extractor. ([Polars User Guide][9])
* **Timezone ops**: output remains Datetime but timezone metadata and/or underlying timestamp can change (see convert vs replace). ([Polars User Guide][10])
* **Bucketing**:

  * `round(every, ...)` and `truncate(every, ...)` are “bucketize” primitives for Date/Datetime. ([Polars User Guide][9])
* **Duration decomposition**: `total_*` family emits numeric totals. ([Polars User Guide][9])

### 2.3 Null semantics rubric

* Most temporal ops propagate nulls.
* Timezone localization has explicit null/error controls for edge cases:

  * `dt.replace_time_zone(..., ambiguous=..., non_existent=...)` supports “raise/earliest/latest/null” for ambiguous, and “raise/null” for non-existent datetimes. ([Polars User Guide][11])

### 2.4 Time zones: “convert” vs “replace” (semantics-critical)

* `dt.convert_time_zone(tz)` **converts** a timezone-aware Datetime; if the input is timezone-naive, conversion behaves **as if from UTC**, regardless of system timezone. ([Polars User Guide][10])
* `dt.replace_time_zone(tz)` **sets/unsets/changes** the timezone and explicitly differs from convert: it “will also modify the underlying timestamp and will ignore the original time zone.” ([Polars User Guide][11])

### 2.5 Method index (`Expr.dt`)

From the temporal reference index: ([Polars User Guide][9])

* offsets/bizdays: `add_business_days(n, ...)`, `offset_by(by)`
* timezone: `base_utc_offset()`, `dst_offset()`, `convert_time_zone(tz)`, `replace_time_zone(tz, *, ambiguous, non_existent)`
* time unit / casting: `cast_time_unit(time_unit)`, `with_time_unit(time_unit)`
* combining: `combine(time, time_unit)`
* extractors (date/datetime): `century()`, `millennium()`, `year()`, `iso_year()`, `quarter()`, `month()`, `month_start()`, `month_end()`, `week()`, `weekday()`, `ordinal_day()`, `day()`, `hour()`, `minute()`, `second(fractional)`, `millisecond()`, `microsecond()`, `nanosecond()`
* duration totals: `days()`, `hours()`, `minutes()`, `seconds()`, `milliseconds()`, `microseconds()`, `nanoseconds()`, plus `total_days()`, `total_hours()`, `total_minutes()`, `total_seconds()`, `total_milliseconds()`, `total_microseconds()`, `total_nanoseconds()`
* epoch/timestamps: `epoch(time_unit)`, `timestamp(time_unit)`
* formatting: `strftime(format)`, `to_string(format)`
* bucketing: `round(every, offset, ambiguous)`, `truncate(every, offset, ...)`
* predicates: `is_leap_year()`
* extraction helpers: `date()`, `time()`, `datetime()`

---

## 3) `expr.list.*` — List namespace

The list namespace is `Expr.list` for `List<T>` columns. The reference index enumerates a broad surface (membership, slicing, set ops, statistics, explode, eval, etc.). ([Polars User Guide][12])

### 3.1 Shape rubric

There are *two different* “shapes” to track with list expressions:

1. **Outer shape (rows)**: most list methods are row-preserving (one output per input row).
2. **Inner shape (list length)**: list methods may change the length/content of each list cell.

Row-expanding:

* `list.explode()` **changes row count**: one output row per element; it has explicit controls for exploding empty lists and null lists. ([Polars User Guide][13])

### 3.2 Dtype rubric

* Input: requires `List<inner_dtype>` (and sometimes inner dtype constraints, e.g. numeric stats).
* Output patterns:

  * scalar per-row: `list.len`, `list.sum`, `list.mean`, `list.min/max`, `list.n_unique`, etc. ([Polars User Guide][12])
  * list per-row: `list.unique`, `list.sort`, `list.reverse`, `list.slice`, set ops, `list.eval` (often yields `List<...>`). ([Polars User Guide][12])
  * dtype-changing conversions: `list.to_array(width)`, `list.to_struct(...)`. ([Polars User Guide][12])
  * row-expanding: `list.explode()` outputs **inner element dtype** (docs: “Expression with the data type of the list elements”). ([Polars User Guide][13])

### 3.3 Null semantics rubric

* `list.explode(empty_as_null=True, keep_nulls=True)` explicitly controls how empty lists and null lists explode:

  * empty list → null (if `empty_as_null=True`)
  * null list → null (if `keep_nulls=True`) ([Polars User Guide][13])
* `list.eval(expr, parallel=...)` warns about parallelism (don’t enable blindly; group_by already parallelizes per group). ([Polars User Guide][14])

### 3.4 Method index (`Expr.list`)

From the list reference index: ([Polars User Guide][12])

* boolean reductions: `all()`, `any()`
* null handling: `drop_nulls()`
* arg ops: `arg_max()`, `arg_min()`
* concat: `concat(other)`
* membership/count: `contains(item)`, `count_match(element)`, `count_matches(element)`
* diffs/shifts: `diff(n, null_behavior)`, `shift(n)`
* eval/explode: `eval(expr, parallel)`, `explode(empty_as_null, keep_nulls)`
* access/slicing: `first()`, `last()`, `get(index, null_on_oob)`, `gather(indices, null_on_oob)`, `take(indices, null_on_oob)`, `head(n)`, `tail(n)`, `slice(offset, length)`, `gather_every(n, offset)`
* size: `len()`, `lengths()`
* ordering: `sort(descending, nulls_last)`, `reverse()`
* stats: `sum()`, `mean()`, `median()`, `min()`, `max()`, `std(ddof)`, `var(ddof)`
* sampling: `sample(n, fraction, ...)`
* set ops: `set_difference(other)`, `set_intersection(other)`, `set_symmetric_difference(other)`, `set_union(other)`
* uniqueness: `unique(maintain_order)`, `n_unique()`
* conversions: `to_array(width)`, `to_struct(...)`
* join (string lists): `join(separator, ignore_nulls)`

---

## 4) `expr.struct.*` — Struct namespace

Struct is the “multi-field” type that lets you treat multiple fields as one series value. The user guide states why it exists: Polars expressions operate on a single series and return another series; `Struct` is the dtype that allows multiple columns as input or output. ([Polars User Guide][15])

### 4.1 Shape rubric

* Row-preserving: struct field selection and struct field updates preserve row count.
* “Explode fields” is typically done by `unnest` (struct → multiple columns). ([Polars User Guide][15])

### 4.2 Dtype rubric

* Input: `Struct{field1: T1, field2: T2, ...}`
* Output patterns:

  * `struct.field(...)` returns a Series/Expr of the field’s dtype.
  * `struct.json_encode()` returns String.
  * `struct.with_fields(...)` returns Struct.
  * `struct.rename_fields(...)` returns Struct. ([Polars User Guide][16])

### 4.3 Null semantics rubric

* Struct-level nulls propagate (null struct → null field extract).
* Field selection may raise if the field doesn’t exist; the API notes `struct` supports `__getitem__` as an alternate field access mechanism. ([Polars User Guide][17])

### 4.4 Method index (`Expr.struct`)

From the struct reference index: ([Polars User Guide][16])

* `field(name, *more_names)`
* `json_encode()`
* `rename_fields(names)`
* `with_fields(*exprs, **named_exprs)`

---

## 5) Window context: `expr.over(...)`

`Expr.over(...)` is the core window context operator: it computes expressions “over the given groups,” similar to group-by aggregation + join-back, analogous to PostgreSQL window functions. ([Polars User Guide][2])

### 5.1 Shape rubric (mapping strategy is the controlling knob)

`over(..., mapping_strategy=...)` controls how per-group results map back to rows. The API lists: `{group_to_rows, join, explode}`. ([Polars User Guide][2])

* **`group_to_rows`** (default):

  * If the expression yields **one scalar per group**, it is broadcast to every row in the group.
  * If it yields **multiple values**, it maps back to original row positions (requires “same elements before vs after”). ([Polars User Guide][2])

* **`join`**:

  * If multiple values per group, they are joined to each row as a `List<group_dtype>`. Warning: can be memory intensive. ([Polars User Guide][2])

* **`explode`**:

  * If multiple values per group, map each value to a **new row** (like `group_by + agg + explode`).
  * This **changes row count** and implies reordering; docs explicitly state sorting/group considerations. ([Polars User Guide][2])

### 5.2 Dtype rubric

* Window is *dtype-polymorphic*: any expression that makes sense per group can be used (rank/sort_by/aggregates/list aggregation, etc.). The critical dtype impacts are from `mapping_strategy` (scalar dtype vs `List<dtype>` join). ([Polars User Guide][2])

### 5.3 Null semantics rubric

* Window ordering controls include `nulls_last` when `order_by` is set. ([Polars User Guide][2])
* Otherwise, null handling is expression-dependent (e.g., aggregations with ignore_nulls knobs).

### 5.4 Parameter index (the “API envelope”)

From `Expr.over`:

* partitioning: `partition_by`, `*more_exprs`
* ordering: `order_by`, `descending`, `nulls_last`
* mapping: `mapping_strategy={'group_to_rows','join','explode'}` ([Polars User Guide][2])

---

## 6) Horizontal contexts (row-wise across columns)

These are expression functions that compute **per-row** results across multiple columns.

### 6.1 `pl.all_horizontal(*exprs)` / `pl.any_horizontal(*exprs)`

* Row-wise AND/OR across columns; accepts `IntoExpr` inputs (strings as column names). ([Polars User Guide][3])
* Null semantics are explicitly **Kleene logic**:

  * `all_horizontal`: if any null and no False → result null. ([Polars User Guide][3])
  * `any_horizontal`: if any null and no True → result null. ([Polars User Guide][18])

### 6.2 Vertical boolean reductions (`pl.all` / `pl.any`, and `Expr.all/any`)

For boolean columns, `ignore_nulls` toggles “ignore nulls” vs Kleene logic. ([Polars User Guide][19])

* `ignore_nulls=True` default:

  * `all`: nulls ignored; if no non-null values → True. ([Polars User Guide][19])
  * `any`: nulls ignored; if no non-null values → False. ([Polars User Guide][20])
* `ignore_nulls=False`:

  * Kleene logic, producing null in the “unknown” case. ([Polars User Guide][19])

### 6.3 `pl.fold(acc, function, exprs, *, returns_scalar, return_dtype)`

`fold` is the general horizontal reducer: it accumulates left-to-right over expressions/columns. ([Polars User Guide][21])
The key correctness knobs are:

* `returns_scalar`: must be set correctly by the user
* `return_dtype`: optional; otherwise inferred from accumulator dtype ([Polars User Guide][21])

### 6.4 `cumfold` / `cum_fold` (cumulative horizontal fold)

The (older) `cumfold` is documented as renamed to `cum_fold()`; it emits cumulative states as fields in a **Struct** column. ([Polars User Guide][22])

---

## 7) (Optional but important) Extending namespaces: custom `Expr.<your_ns>`

If you want to make your own “house style” expression namespace (for project-wide transforms without UDFs sprinkled everywhere), Polars supports registering a custom `Expr` namespace via `polars.api.register_expr_namespace`. ([Polars User Guide][23])

---

If you want the next increment after this catalog, the natural “Expressions + contexts” continuation is a **pattern library**: canonical recipes that combine these namespaces without UDFs (e.g., regex parse → temporal localization; list.eval + window rank; struct pack/unpack around group_by; fold-based row scoring), each with “shape + dtype + null” assertions and a minimal test harness.

[1]: https://docs.pola.rs/py-polars/html/reference/expressions/index.html?utm_source=chatgpt.com "Expressions — Polars documentation"
[2]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.over.html "polars.Expr.over — Polars  documentation"
[3]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.all_horizontal.html "polars.all_horizontal — Polars  documentation"
[4]: https://docs.pola.rs/user-guide/expressions/strings/ "Strings - Polars user guide"
[5]: https://docs.pola.rs/py-polars/html/reference/expressions/string.html "String — Polars  documentation"
[6]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.str.contains.html "polars.Expr.str.contains — Polars  documentation"
[7]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.str.len_bytes.html?utm_source=chatgpt.com "polars.Expr.str.len_bytes — Polars documentation"
[8]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.str.replace_all.html "polars.Expr.str.replace_all — Polars  documentation"
[9]: https://docs.pola.rs/py-polars/html/reference/expressions/temporal.html "Temporal — Polars  documentation"
[10]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.dt.convert_time_zone.html "polars.Expr.dt.convert_time_zone — Polars  documentation"
[11]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.dt.replace_time_zone.html "polars.Expr.dt.replace_time_zone — Polars  documentation"
[12]: https://docs.pola.rs/py-polars/html/reference/expressions/list.html "List — Polars  documentation"
[13]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.list.explode.html?utm_source=chatgpt.com "polars.Expr.list.explode — Polars documentation"
[14]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.list.eval.html "polars.Expr.list.eval — Polars  documentation"
[15]: https://docs.pola.rs/user-guide/expressions/structs/ "Structs - Polars user guide"
[16]: https://docs.pola.rs/py-polars/html/reference/expressions/struct.html "Struct — Polars  documentation"
[17]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.struct.field.html?utm_source=chatgpt.com "polars.Expr.struct.field — Polars documentation"
[18]: https://docs.pola.rs/py-polars/html/reference/expressions/api/polars.any_horizontal.html "polars.any_horizontal — Polars  documentation"
[19]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.all.html "polars.all — Polars  documentation"
[20]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.any.html?utm_source=chatgpt.com "polars.any — Polars documentation"
[21]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.fold.html "polars.fold — Polars  documentation"
[22]: https://docs.pola.rs/api/python/version/0.20/reference/expressions/api/polars.cumfold.html "polars.cumfold — Polars  documentation"
[23]: https://docs.pola.rs/api/python/dev/reference/api/polars.api.register_expr_namespace.html?utm_source=chatgpt.com "polars.api.register_expr_namespace"

# Pattern library: composing Polars namespaces without UDFs

*(each recipe includes: IR pattern → runnable snippet → shape/dtype/null rubric → minimal pytest harness)*

A few **global authoring constraints** that show up repeatedly in these recipes:

* **Separate expressions inside a context are “embarrassingly parallel”**; don’t rely on implicit evaluation order inside a single `with_columns/select/agg`. Stage dependent steps in separate calls if you need ordering. ([Polars User Guide][1])
* **`when-then-otherwise` is not short-circuiting**: Polars computes *all* branch expressions in parallel and filters afterward, so every branch expression must be valid on its own. Also, string inputs in `when/then/otherwise` are parsed as column names unless you use `lit()`. ([Polars User Guide][2])

Below, I’ll write transforms as `LazyFrame -> LazyFrame` so they’re “plan-native” and drop cleanly into later optimizer/streaming deep dives.

---

## 1) Regex-ish precheck → `str.strptime` → DST-safe timezone localization → timezone conversion

### Why this pattern exists

* `Expr.str.strptime` gives you **typed** Date/Datetime/Time parsing; `strict=True` raises if any conversion fails. ([Polars User Guide][3])
* You **cannot** safely “guard” a strict parse behind `when(...)` because `when-then-otherwise` evaluates all branches anyway. ([Polars User Guide][2])
* `dt.replace_time_zone` is the correct “localize / reinterpret naive wall-clock as timezone” operator; it has explicit handling for DST **ambiguous** and **non-existent** local times, and it differs from `convert_time_zone` by modifying the underlying timestamp / ignoring the original timezone. ([Polars User Guide][4])
* `dt.convert_time_zone` converts between timezones; if you call it on a timezone-naive datetime it behaves “as if converting from UTC.” ([Polars User Guide][5])

### IR skeleton

1. Parse string → `Datetime` (avoid raising by using `strict=False` if input can be dirty)
2. Localize naive datetime → timezone with `replace_time_zone(…, ambiguous=…, non_existent=…)`
3. Convert to storage timezone (often UTC) via `convert_time_zone("UTC")`

### Implementation

```python
import polars as pl

def parse_localize_ts(
    lf: pl.LazyFrame,
    *,
    src: str = "ts_str",
    fmt: str = "%Y-%m-%d %H:%M:%S",
    local_tz: str = "America/New_York",
    out_tz: str = "UTC",
) -> pl.LazyFrame:
    # Note: strict=True will raise on any bad row; you cannot guard it with when().
    parsed = pl.col(src).str.strptime(pl.Datetime, fmt, strict=False)

    localized = parsed.dt.replace_time_zone(
        local_tz,
        ambiguous="earliest",     # or: "latest" | "null" | "raise"
        non_existent="null",      # or: "raise"
    )

    return lf.with_columns(
        pl.col(src),
        localized.dt.convert_time_zone(out_tz).alias("ts"),
    )
```

### Shape · dtype · null rubric

* **Shape**: row-preserving (adds one column).
* **Dtype**: output is `Datetime[..., <out_tz>]` after `convert_time_zone`. ([Polars User Guide][5])
* **Null semantics**:

  * non-existent local times become null when `non_existent="null"`. ([Polars User Guide][4])
  * ambiguous local times are resolved per `ambiguous=...` (earliest/latest/null/raise). ([Polars User Guide][4])
  * dirty strings won’t raise with `strict=False` (since `strict=True` is the “raise” mode). ([Polars User Guide][3])

### Minimal pytest harness

```python
import polars as pl

def test_parse_localize_ts__dst_and_bad_rows():
    df = pl.DataFrame(
        {
            "ts_str": [
                "2024-01-01 12:00:00",  # normal
                "2024-11-03 01:30:00",  # ambiguous in NY (fall back)
                "2024-03-10 02:30:00",  # non-existent in NY (spring forward)
                "not-a-timestamp",      # invalid
            ]
        }
    )

    out = parse_localize_ts(df.lazy()).collect()

    # shape
    assert out.height == df.height

    # dtype: ensure tz-aware and matches output tz
    dtype = out.schema["ts"]  # ordered mapping of name -> dtype :contentReference[oaicite:10]{index=10}
    assert getattr(dtype, "time_zone", None) == "UTC"

    # nulls: non-existent + invalid expected to produce nulls
    assert out["ts"].null_count() >= 2
```

#### Variant: strings include offset → parse directly to tz-aware Datetime

If your source strings include an offset (e.g. `Z` or `+01:00`), parse with an offset-aware format. The Polars docs show an example using `"%Y-%m-%d %H:%M%#z"` producing `datetime[..., UTC]`. ([Polars User Guide][3])

---

## 2) “Many columns → per-row list → list.eval(element…) → scalar summary → window rank”

This is the “horizontal vectorization” pattern: you turn a set of columns into a single list column so you can apply **list-native** transforms and get back either a list or a scalar.

### Primitives you’re composing

* `Expr.list.eval(expr, parallel=...)` runs any Polars expression against list elements; element binding is `pl.element()`. ([Polars User Guide][6])
* The list user guide shows “element-wise computation within lists” (e.g., cast each element, detect nulls, sum). ([Polars User Guide][7])
* `Expr.over(..., mapping_strategy=...)` is “group_by + join-back” semantics for window-style transforms; mapping strategy controls whether you broadcast/join-list/explode. ([Polars User Guide][8])

### 2A) Error counting inside list cells (string list → numeric parse → null count)

Directly adapted from the Polars list user guide: cast each element with `strict=False`, mark nulls, then sum them. ([Polars User Guide][7])

```python
import polars as pl

def list_error_count(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        pl.col("measurements")
          .list.eval(pl.element().cast(pl.Int64, strict=False).is_null())
          .list.sum()
          .alias("errors")
    )
```

**Rubric**

* Shape: row-preserving.
* Dtype: `errors` is integer.
* Null semantics: invalid casts become null when `strict=False` (so `.is_null()` flags errors). ([Polars User Guide][7])

**Test**

```python
import polars as pl

def test_list_error_count():
    df = pl.DataFrame(
        {"station": ["S1", "S2"], "measurements": [["1", "x", "3"], ["4", "5"]]}
    )
    out = list_error_count(df.lazy()).collect()

    assert out.height == 2
    assert out.schema["errors"].is_integer()
    assert out["errors"].to_list() == [1, 0]
```

### 2B) Per-row percent-rank across a list + per-group window rank by a derived scalar

The list user guide demonstrates a percent-rank expression built from `rank/round/count` inside `list.eval`. ([Polars User Guide][7])

```python
import polars as pl

def row_rank_then_group_rank(lf: pl.LazyFrame) -> pl.LazyFrame:
    rank_pct = (pl.element().rank(descending=True) / pl.element().count()).round(2)

    return (
        lf.with_columns(
            # 1) collapse day_1..day_3 into a list
            pl.concat_list(["day_1", "day_2", "day_3"]).alias("temps"),
        )
        .with_columns(
            # 2) per-row list rank
            pl.col("temps").list.eval(rank_pct).alias("temps_rank"),
            # 3) derive a scalar metric from the list
            pl.col("temps").list.max().alias("max_temp"),
        )
        .with_columns(
            # 4) window-rank stations within each region by derived scalar
            pl.col("max_temp").rank("dense", descending=True).over("region").alias("region_rank")
        )
    )
```

**Rubric**

* Shape: row-preserving (unless you choose an `over(..., mapping_strategy="explode")`, which *can change row count*). ([Polars User Guide][8])
* Dtype:

  * `temps`: `list[int/float]`
  * `temps_rank`: `list[f64]` style output is typical for rank. ([Polars User Guide][6])
* Null semantics:

  * `list.eval` does not short-circuit; define element expressions that are valid for all elements (same rule-of-thumb as `when`). ([Polars User Guide][6])
* Performance knob: `list.eval(..., parallel=True)` exists but is explicitly “don’t activate blindly” and generally not needed inside `group_by` contexts. ([Polars User Guide][6])

**Test**

```python
import polars as pl

def test_row_rank_then_group_rank__shape_and_types():
    df = pl.DataFrame(
        {
            "region": ["E", "E", "W"],
            "station": ["S1", "S2", "S3"],
            "day_1": [10, 12, 9],
            "day_2": [11, 8, 10],
            "day_3": [7, 13, 6],
        }
    )

    out = row_rank_then_group_rank(df.lazy()).collect()

    assert out.height == df.height
    assert str(out.schema["temps"]).startswith("List")
    assert str(out.schema["temps_rank"]).startswith("List")
    assert out.schema["region_rank"].is_integer()

    # per-region ranking exists for every row (no nulls here)
    assert out["region_rank"].null_count() == 0
```

---

## 3) Struct pack/unpack around group_by + multi-column ranking (struct + window)

This is Polars’ “multi-column as a single value” trick.

### Why struct exists (and why unnest matters)

Polars expressions operate on a single series and return a series; `Struct` is the dtype that lets you treat multiple fields as a single value for input/output. The user guide also states `unnest` turns each struct field into its own column. ([Polars User Guide][9])

Also: `Expr.struct.unnest()` exists as an expression-level expansion (alias of `Expr.struct.field("*")`). ([Polars User Guide][10])

### 3A) Composite key group_by without repeating key columns everywhere

```python
import polars as pl

def group_by_struct_key(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.group_by(pl.struct(["movie", "theatre"]).alias("k"))
          .agg(
              pl.col("avg_rating").mean().alias("avg_rating_mean"),
              pl.col("count").sum().alias("count_sum"),
          )
          .select(
              pl.col("k").struct.unnest(),  # expand struct key back into columns :contentReference[oaicite:24]{index=24}
              pl.col("avg_rating_mean"),
              pl.col("count_sum"),
          )
    )
```

**Rubric**

* Shape: row count becomes `#unique(movie,theatre)` (group-by contraction).
* Dtype: `k` is struct; after `.struct.unnest()` you’re back to `movie: str`, `theatre: str`, etc. ([Polars User Guide][10])
* Null semantics: if key fields contain nulls, they participate in grouping under struct null semantics (treat as values); be explicit if you need null-dropping pre-group.

**Test**

```python
import polars as pl

def test_group_by_struct_key__contracts_shape():
    df = pl.DataFrame(
        {
            "movie": ["Cars", "Cars", "ET"],
            "theatre": ["NE", "NE", "IL"],
            "avg_rating": [4.5, 4.6, 4.9],
            "count": [30, 28, 26],
        }
    )
    out = group_by_struct_key(df.lazy()).collect()

    # 2 unique pairs: (Cars,NE), (ET,IL)
    assert out.height == 2
    assert set(out.columns) == {"movie", "theatre", "avg_rating_mean", "count_sum"}
```

### 3B) Multi-column ranking: struct fields as the ranking key + window partitioning

The struct user guide includes a canonical pattern: build a struct from multiple columns (priority order), rank it, and window it by identifiers. ([Polars User Guide][9])

```python
import polars as pl

def multi_col_rank(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        pl.struct(["count", "avg_rating"])
          .rank("dense", descending=True)
          .over("movie", "theatre")
          .alias("rank")
    )
```

**Rubric**

* Shape: row-preserving (window join-back). ([Polars User Guide][8])
* Dtype: `rank` integer-ish (u32 in the user guide example). ([Polars User Guide][9])
* Null semantics: nulls in struct fields affect ordering; if nulls must be last/ignored, normalize before structing (fill/coalesce).

---

## 4) Fold-based row scoring (many columns → one scalar) without `when` chains

### Why fold is the canonical tool here

`polars.fold(acc, function, exprs, ...)` is explicitly “accumulate over multiple columns horizontally/row-wise with a left fold”. ([Polars User Guide][11])
This tends to be the most maintainable way to do “score = Σ wᵢ fᵢ(… )” style transforms without huge `when-then-otherwise` ladders (and without accidentally relying on branch short-circuiting that doesn’t exist). ([Polars User Guide][2])

### Implementation: weighted sum with nulls treated as 0

```python
import polars as pl

def weighted_score(lf: pl.LazyFrame) -> pl.LazyFrame:
    weighted_terms = [
        (pl.col("f1").fill_null(0).cast(pl.Float64) * 0.2),
        (pl.col("f2").fill_null(0).cast(pl.Float64) * 0.3),
        (pl.col("f3").fill_null(0).cast(pl.Float64) * 0.5),
    ]

    score = pl.fold(
        acc=pl.lit(0.0),
        function=lambda acc, x: acc + x,
        exprs=weighted_terms,
        return_dtype=pl.Float64,  # optional; fold can infer from accumulator :contentReference[oaicite:31]{index=31}
    ).alias("score")

    return lf.with_columns(score)
```

**Rubric**

* Shape: row-preserving.
* Dtype: Float64 (explicit or inferred from `acc`). ([Polars User Guide][11])
* Null semantics: null inputs become 0 by construction; score never null unless you choose “propagate null” semantics instead.

### Minimal pytest harness (using Polars’ testing helpers)

Polars provides `polars.testing` asserts and notes it is not imported by default; you import from `polars.testing`. ([Polars User Guide][12])

```python
import polars as pl
from polars.testing import assert_series_equal

def test_weighted_score():
    df = pl.DataFrame({"f1": [10, None], "f2": [0, 10], "f3": [2, None]})
    out = weighted_score(df.lazy()).collect()

    expected = pl.Series("score", [10*0.2 + 0*0.3 + 2*0.5, 0*0.2 + 10*0.3 + 0*0.5])
    assert_series_equal(out["score"], expected, check_dtypes=True)
```

---

## 5) Bonus (high leverage in real pipelines): conditional aggregations via `Expr.filter` (aggregation context)

This is a recurring “no-UDF” pattern for producing multiple conditional metrics in one group-by pass.

`Expr.filter` is “mostly useful in an aggregation context”; it discards elements where predicate isn’t True (including nulls), and the docs show a direct group-by example. ([Polars User Guide][13])

```python
import polars as pl

def conditional_sums(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.group_by("group")
          .agg(
              lt_sum=pl.col("x").filter(pl.col("x") < 2).sum(),
              gte_sum=pl.col("x").filter(pl.col("x") >= 2).sum(),
          )
    )
```

**Rubric**

* Shape: `#rows == #groups`
* Null semantics: filtered-out and null predicate rows are discarded before aggregation. ([Polars User Guide][13])

**Test**

```python
import polars as pl

def test_conditional_sums():
    df = pl.DataFrame({"group": ["g1","g1","g2"], "x": [1,2,3]})
    out = conditional_sums(df.lazy()).collect().sort("group")

    assert out.height == 2
    assert out.select("lt_sum").to_series().to_list() == [1, 0]
    assert out.select("gte_sum").to_series().to_list() == [2, 3]
```

---

## Suggested “drop-in” layout for your repo docs/tests

* `docs/polars_advanced_querying_expressions_and_contexts.md`

  * this pattern library as the end of the section (after namespace catalog)
* `tests/test_polars_patterns_expressions_contexts.py`

  * one `test_*` per recipe (as above)
* Optional: add a “plan snapshot” assert later when we deep dive `LazyFrame.explain/profile`

If you want the next deep dive, the natural continuation is: **“Expression normalization + determinism contracts”** (naming stability, null semantics invariants, and how you encode these as schema/plan golden tests using Polars’ testing helpers).

[1]: https://docs.pola.rs/user-guide/concepts/expressions-and-contexts/ "Expressions and contexts - Polars user guide"
[2]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.when.html "polars.when — Polars  documentation"
[3]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.str.strptime.html "polars.Expr.str.strptime — Polars  documentation"
[4]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.dt.replace_time_zone.html "polars.Expr.dt.replace_time_zone — Polars  documentation"
[5]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.dt.convert_time_zone.html "polars.Expr.dt.convert_time_zone — Polars  documentation"
[6]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.list.eval.html "polars.Expr.list.eval — Polars  documentation"
[7]: https://docs.pola.rs/user-guide/expressions/lists-and-arrays/ "Lists and arrays - Polars user guide"
[8]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.over.html "polars.Expr.over — Polars  documentation"
[9]: https://docs.pola.rs/user-guide/expressions/structs/ "Structs - Polars user guide"
[10]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.struct.unnest.html "polars.Expr.struct.unnest — Polars  documentation"
[11]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.fold.html "polars.fold — Polars  documentation"
[12]: https://docs.pola.rs/py-polars/html/reference/testing.html "Testing — Polars  documentation"
[13]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.filter.html "polars.Expr.filter — Polars  documentation"

# Expression normalization + determinism contracts (Polars)

This section is about turning “a correct Polars query” into a **stable, reproducible transformation artifact** that an agent can reason about, refactor, and regression-test: **stable names, stable dtypes, stable row/column order, stable null semantics, and stable plan shape**.

---

## 0) Contract surfaces you can actually lock down

| Contract dimension    | What can drift without explicit control                                                                                               | Primary control knobs                                                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Column names**      | default output names; literal default names; name changes through chaining                                                            | `Expr.alias`, `Expr.name.*`, `Expr.meta.output_name`, `Expr.meta.undo_aliases` ([Polars User Guide][1])                                                |
| **Dtypes / schema**   | implicit inference; casts/parse behavior; lazy type checks at `collect`                                                               | `Expr.cast(strict=...)`, `LazyFrame.collect_schema()`, `DataFrame.schema`, `pl.Schema(...)` ([Polars User Guide][2])                                   |
| **Row / group order** | group-by output group order; join output order; unique/dedup row selection                                                            | `group_by(maintain_order=...)`, `join(maintain_order=...)`, `unique(keep=..., maintain_order=...)`, explicit `sort` ([Polars User Guide][3])           |
| **Null semantics**    | filters discard nulls; boolean reducers ignore/null via Kleene logic; casts/parses produce null vs raise; `when` is non-short-circuit | `filter` semantics, `any/all(ignore_nulls=...)`, `*_horizontal` Kleene logic, `cast(strict=...)`, `when` warning, `fill_null` ([Polars User Guide][4]) |
| **Plan shape**        | optimizer may rewrite; streaming/cluster passes change node structure                                                                 | `LazyFrame.explain(... optimization flags ...)` and snapshot tests ([Polars User Guide][5])                                                            |

---

## 1) Naming normalization (output-name stability)

### 1.1 Rule: **never rely on default output names for derived columns**

Polars will derive an output name from the expression chain (and for `when-then-otherwise`, it’s explicitly taken from the first `then`). You want deterministic naming, so you should **always alias** final outputs. ([Polars User Guide][6])

**Primitive**

* `Expr.alias(name)` renames the expression output. It is also the canonical fix for “literal columns produce duplicate default names”. ([Polars User Guide][1])

```python
import polars as pl

def normalize_with_aliases(lf: pl.LazyFrame) -> pl.LazyFrame:
    return lf.with_columns(
        (pl.col("a") + pl.col("b")).alias("a_plus_b"),
        pl.lit(True).alias("is_active"),      # prevent duplicate literal names
    )
```

### 1.2 Use the `.name` namespace for systematic renaming

Polars exposes a first-class naming namespace (root-name transforms). Key pieces:

* `Expr.name.keep()` **keeps the original root name**, undoing prior renaming; constraints: must be the last call in a chain; only one name op per expression; `.name.map` recommended for advanced renames. ([Polars User Guide][7])
* `Expr.name.map(fn)` maps root name → new name (rename policy function). ([Polars User Guide][8])
* `Expr.name.prefix/suffix` apply mechanical prefixes/suffixes (useful for “namespace” outputs). ([Polars User Guide][9])
* Struct-specific: `Expr.name.map_fields/prefix_fields/suffix_fields` for stable nested-field naming. ([Polars User Guide][10])

**Example: standardize “metric_” prefix for all numeric derived columns**

```python
import polars as pl

def add_metrics(lf: pl.LazyFrame) -> pl.LazyFrame:
    metrics = [
        (pl.col("x").mean().over("k")).alias("mean_x"),
        (pl.col("x").std().over("k")).alias("std_x"),
    ]
    # enforce naming policy
    metrics = [m.name.prefix("metric_") for m in metrics]
    return lf.with_columns(metrics)
```

### 1.3 Expression introspection for name policies + lineage validation (`Expr.meta`)

`Expr.meta` is the “introspect the IR” namespace. The relevant pieces for normalization:

* `Expr.meta.output_name(raise_if_undetermined=...)`: compute the output column name the expression would produce. ([Polars User Guide][11])
* `Expr.meta.root_names()`: compute root column dependencies (useful for validating “this derived field only depends on …”). ([Polars User Guide][12])
* `Expr.meta.undo_aliases()`: strip name operations (`alias`, `name.keep`, etc.) so you can canonicalize/have stable equivalence checks across renames. ([Polars User Guide][13])
* `Expr.meta.serialize()`: JSON serialization of the expression IR (useful for hashing/plan caches). ([Polars User Guide][11])

**Practical normalization pattern**

* Define transforms in code with aliases for user-facing stability.
* For internal equivalence/hashing/dedup, compare `expr.meta.undo_aliases().meta.serialize()` (or `.meta.eq()`), so renames don’t break identity. ([Polars User Guide][13])

---

## 2) Ordering determinism (row / group ordering is *not* implicit)

Polars is multithreaded and optimizes aggressively; order frequently isn’t guaranteed unless you ask for it.

### 2.1 Group-by output order

`group_by(..., maintain_order=True)` ensures group order is consistent with input order, but is slower and blocks streaming; within each group, row order is preserved regardless. ([Polars User Guide][3])

**Contract options**

* For deterministic group output ordering:

  1. `maintain_order=True` (input-order determinism), or
  2. `group_by(...).agg(...).sort(keys...)` (explicit total ordering).

### 2.2 Join output order

`DataFrame.join(..., maintain_order=...)` explicitly warns: **don’t rely on observed ordering** unless you set the parameter; ordering may differ across versions or runs. Options include `'none'|'left'|'right'|'left_right'|'right_left'`. ([Polars User Guide][14])

**Contract recommendation**

* If order matters, set `maintain_order="left"` (or whichever contract you need), then (optionally) apply `sort` for a total order.

### 2.3 Dedup/unique determinism

`DataFrame.unique` / `LazyFrame.unique`:

* `keep='any'` gives **no guarantee** which duplicate row is kept (enables more optimizations).
* `maintain_order=True` preserves original order but is more expensive and blocks streaming. ([Polars User Guide][15])

**Contract recommendation**

* If your output depends on which representative row is chosen, use `keep='first'|'last'|'none'` plus (often) an explicit pre-sort to define “first/last” deterministically.

### 2.4 Explicit sorting (global and per-group)

* `DataFrame.sort(..., descending=..., nulls_last=..., multithreaded=...)` gives total ordering; `nulls_last` can be specified per-column. ([Polars User Guide][16])
* `Expr.sort_by(by=..., descending=..., nulls_last=...)` sorts values; in a group-by context, sorts **within groups** (critical when an aggregation is order-sensitive). ([Polars User Guide][17])
* List ordering: `Expr.list.sort(descending=..., nulls_last=...)` for deterministic list-cell contents. ([Polars User Guide][18])

---

## 3) Null semantics invariants (make “null policy” explicit)

### 3.1 Filters discard nulls (rows/elements where predicate is not True are dropped)

* `DataFrame.filter` / `LazyFrame.filter`: rows where predicate is not True (False or null) are discarded. ([Polars User Guide][4])
* `Expr.filter`: element-wise filter also discards null predicate results (commonly used inside aggregations). ([Polars User Guide][19])

**Contract implications**

* If you want “null treated as False” in filters, you already get that (null rows drop).
* If you want “null treated as True” (rare), you must explicitly coerce predicate, e.g. `pred.fill_null(True)` (and make it obvious in code).

### 3.2 Boolean reducers: ignore_nulls vs Kleene logic

* `pl.any(..., ignore_nulls=True)` ignores nulls; if no non-null values → False.
* `ignore_nulls=False` uses Kleene logic: if any null and no True → null. ([Polars User Guide][20])
  Similarly for `pl.all` (True default / Kleene optional). ([Polars User Guide][21])

**Contract recommendation**

* Create project-wide wrappers like `any_kleene` / `any_ignore_nulls` and mandate their usage at call sites.

### 3.3 Horizontal boolean (`all_horizontal` / `any_horizontal`) is Kleene by design

Both document Kleene logic null behavior. ([Polars User Guide][22])

**Contract workaround (if you want ignore-null behavior horizontally)**

* Pre-fill nulls before applying horizontal logic (choose fill value consistent with your semantics):

  * AND-case: fill nulls with `True` (neutral element)
  * OR-case: fill nulls with `False` (neutral element)

### 3.4 Cast strictness is part of your contract

`Expr.cast(strict=True)` can raise if the cast is invalid on rows after predicate pushdown; `strict=False` turns invalid casts into nulls. ([Polars User Guide][2])

**Contract pattern**

* Use `strict=True` when “bad data must fail the pipeline”.
* Use `strict=False` when “bad data becomes null”, and then assert/null-count expectations downstream.

### 3.5 `when-then-otherwise` is non-short-circuiting

Polars computes all branch expressions in parallel and filters afterward; each expression must be valid regardless of condition. It also notes string inputs are parsed as column names unless you use `lit()`. ([Polars User Guide][6])

**Contract implication**

* You cannot safely “guard” strict parsing or operations that would raise behind `when`.
* Instead: use tolerant operators (`strict=False` parse/cast) + explicit null handling + downstream assertions.

### 3.6 Explicit null filling

`Expr.fill_null(value|strategy, ...)` is the canonical “choose a null policy” operator; treat it as part of your public transform contract (not a tweak). ([Polars User Guide][23])

---

## 4) Schema/dtype contracts (compile-time checks in lazy mode)

### 4.1 Schema is first-class and ordered

* `DataFrame.schema` returns an **ordered mapping** of column name → dtype. ([Polars User Guide][24])
* `LazyFrame.collect_schema()` resolves schema; it can be expensive (metadata reads / network). ([Polars User Guide][25])
* The user guide emphasizes schema’s role in the lazy API: Polars checks schema before processing data and can raise `InvalidOperationError` at `collect` time if expressions don’t match types; it also notes some operations (e.g., pivot) aren’t available in lazy because schema can’t be known in advance. ([Polars User Guide][26])

### 4.2 `pl.Schema(...)` as a contract artifact

`pl.Schema` is an ordered mapping class; it accepts mappings/tuples and Arrow C Schema exporters (e.g., PyArrow schema), supports `.names()`, `.dtypes()`, and conversions like `.to_arrow()` and `.to_frame()`. ([Polars User Guide][27])

**Contract use**

* Define expected schema as `pl.Schema({...})` and assert equality against `collect_schema()` in tests.

---

## 5) Plan determinism (golden plan snapshots)

### 5.1 `LazyFrame.explain` is the plan snapshot surface

`LazyFrame.explain(format='plain'|'tree', optimized=True, ...)` produces a query plan string and exposes **explicit optimization toggles** (type coercion, predicate/projection pushdown, simplify expressions, slice pushdown, common subplan/expr elimination, `cluster_with_columns`, and `streaming`). ([Polars User Guide][5])

**Why this matters for determinism**

* Optimizer changes can legitimately rewrite plans (even when results are identical).
* If you want “plan as contract”, freeze flags in tests to make diffs attributable.

### 5.2 Practical plan snapshot policy

* Snapshot **both**:

  * naive plan (`optimized=False`) and
  * optimized plan (`optimized=True` with explicit flags).
* Fix `format="tree"` for readability; fix `streaming=False` unless you’re explicitly testing streaming. ([Polars User Guide][5])

---

## 6) Encoding contracts as golden tests (Polars testing helpers)

### 6.1 `assert_frame_equal` / `assert_series_equal`

Polars provides unit-test asserts:

* `polars.testing.assert_frame_equal(left, right, check_row_order, check_column_order, check_dtypes, check_exact, rel_tol, abs_tol, categorical_as_str)` ([Polars User Guide][28])
* `polars.testing.assert_series_equal(left, right, check_dtypes, check_names, check_order, check_exact, rel_tol, abs_tol, categorical_as_str)` ([Polars User Guide][29])

Key contract controls:

* **Row/column order** can be required (`check_row_order=True`, `check_column_order=True`). ([Polars User Guide][28])
* Float tolerances can be exact or tolerant (`check_exact`, `rel_tol`, `abs_tol`). ([Polars User Guide][28])
* Categoricals can be compared as strings when string cache differs (`categorical_as_str=True`). ([Polars User Guide][28])

### 6.2 Minimal “contract harness” example (schema + plan + value)

```python
from __future__ import annotations
from pathlib import Path
import polars as pl
from polars.testing import assert_frame_equal

def assert_schema(lf: pl.LazyFrame, expected: pl.Schema) -> None:
    got = lf.collect_schema()
    assert got == expected, f"Schema mismatch:\n got={got}\n exp={expected}"

def assert_plan(lf: pl.LazyFrame, golden_path: Path) -> None:
    plan = lf.explain(
        format="tree",
        optimized=True,
        type_coercion=True,
        predicate_pushdown=True,
        projection_pushdown=True,
        simplify_expression=True,
        slice_pushdown=True,
        comm_subplan_elim=True,
        comm_subexpr_elim=True,
        cluster_with_columns=True,
        streaming=False,
    )
    if not golden_path.exists():
        golden_path.write_text(plan)
        raise AssertionError(f"Wrote new golden plan: {golden_path}")
    assert plan == golden_path.read_text()

def test_transform_contract(tmp_path: Path) -> None:
    df = pl.DataFrame({"k": ["a","a","b"], "x": [1, None, 3]})
    lf = (
        df.lazy()
          .with_columns(pl.col("x").fill_null(0).cast(pl.Int64).alias("x_norm"))
          .group_by("k", maintain_order=True)
          .agg(pl.col("x_norm").sum().alias("sum_x"))
          .sort("k")
    )

    assert_schema(lf, pl.Schema({"k": pl.String(), "sum_x": pl.Int64()}))
    assert_plan(lf, tmp_path / "transform.plan.txt")

    out = lf.collect()
    expected = pl.DataFrame({"k": ["a","b"], "sum_x": [1, 3]})
    assert_frame_equal(out, expected, check_row_order=True, check_column_order=True, check_dtypes=True)
```

Notes on the example:

* `group_by(..., maintain_order=True)` makes group ordering consistent with input but blocks streaming; we then `sort("k")` to impose a total order anyway. ([Polars User Guide][3])
* `fill_null` + `cast(strict=True)` makes null policy and dtype policy explicit. ([Polars User Guide][23])
* `collect_schema()` is your pre-execution contract check; remember it can be expensive on remote scans. ([Polars User Guide][25])

---

## 7) “Normalization checklist” to apply to every new transform node

1. **Names**: every derived column uses `.alias(...)` or a `.name.*` policy; no implicit default names. ([Polars User Guide][1])
2. **Nulls**: every boolean reduction declares its null policy (`ignore_nulls` vs Kleene or explicit fill); filters understand null-drop; `when` does not short-circuit. ([Polars User Guide][20])
3. **Dtypes**: all “boundary” columns (IDs/timestamps/measurements) are explicitly cast/parsed, with `strict` chosen intentionally. ([Polars User Guide][2])
4. **Ordering**: any transform where order matters sets `maintain_order` (group_by/join/unique) or sorts explicitly; never rely on observed ordering defaults. ([Polars User Guide][14])
5. **Plan**: snapshot `explain(format="tree", optimized=True, flags...)` as a golden if you care about plan regressions. ([Polars User Guide][5])

If you want the next deep dive, the natural follow-on is: **“Lazy optimizer interactions with determinism”** — specifically, how toggles like `cluster_with_columns`, predicate/projection pushdown, and common subexpression elimination can change *plan shape* (and sometimes ordering requirements), and how you structure pipelines so “determinism contracts” remain valid under optimization.

[1]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.alias.html "polars.Expr.alias — Polars  documentation"
[2]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.cast.html "polars.Expr.cast — Polars  documentation"
[3]: https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.group_by.html?utm_source=chatgpt.com "polars.DataFrame.group_by — Polars documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.filter.html "polars.DataFrame.filter — Polars  documentation"
[5]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.explain.html "polars.LazyFrame.explain — Polars  documentation"
[6]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.when.html "polars.when — Polars  documentation"
[7]: https://docs.pola.rs/py-polars/html/reference/expressions/api/polars.Expr.name.keep.html "polars.Expr.name.keep — Polars  documentation"
[8]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.name.map.html?utm_source=chatgpt.com "polars.Expr.name.map — Polars documentation"
[9]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.name.prefix.html?utm_source=chatgpt.com "polars.Expr.name.prefix — Polars documentation"
[10]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.name.map_fields.html?utm_source=chatgpt.com "polars.Expr.name.map_fields — Polars documentation"
[11]: https://docs.pola.rs/py-polars/html/reference/expressions/meta.html "Meta — Polars  documentation"
[12]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.meta.root_names.html "polars.Expr.meta.root_names — Polars  documentation"
[13]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.meta.undo_aliases.html "polars.Expr.meta.undo_aliases — Polars  documentation"
[14]: https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.join.html "polars.DataFrame.join — Polars  documentation"
[15]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.unique.html?utm_source=chatgpt.com "polars.DataFrame.unique — Polars documentation"
[16]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.sort.html?utm_source=chatgpt.com "polars.DataFrame.sort — Polars documentation"
[17]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.sort_by.html?utm_source=chatgpt.com "polars.Expr.sort_by — Polars documentation"
[18]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.list.sort.html?utm_source=chatgpt.com "polars.Expr.list.sort — Polars documentation"
[19]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.filter.html "polars.Expr.filter — Polars  documentation"
[20]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.any.html "polars.any — Polars  documentation"
[21]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.all.html?utm_source=chatgpt.com "polars.all — Polars documentation"
[22]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.all_horizontal.html "polars.all_horizontal — Polars  documentation"
[23]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.fill_null.html?utm_source=chatgpt.com "polars.Expr.fill_null — Polars documentation"
[24]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.schema.html?utm_source=chatgpt.com "polars.DataFrame.schema — Polars documentation"
[25]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.collect_schema.html "polars.LazyFrame.collect_schema — Polars  documentation"
[26]: https://docs.pola.rs/user-guide/lazy/schemas/ "Schema - Polars user guide"
[27]: https://docs.pola.rs/api/python/dev/reference/schema/index.html "Schema — Polars  documentation"
[28]: https://docs.pola.rs/api/python/stable/reference/api/polars.testing.assert_frame_equal.html "polars.testing.assert_frame_equal — Polars  documentation"
[29]: https://docs.pola.rs/api/python/stable/reference/api/polars.testing.assert_series_equal.html "polars.testing.assert_series_equal — Polars  documentation"

# Lazy optimizer interactions with determinism (Polars)

This deep dive is about the **gap between “what you wrote” and “what actually runs”** in Polars Lazy: the optimizer rewrites your logical plan to reduce IO / CPU / memory, and those rewrites can change **plan shape** and (in a few cases) **observable behavior** unless you encode explicit determinism contracts (names, schema, ordering, null policy, error policy).

Polars exposes the relevant knobs *directly* on `LazyFrame.explain/collect/profile/show_graph`: you can turn major optimization passes on/off, including `predicate_pushdown`, `projection_pushdown`, `comm_subexpr_elim`, and `cluster_with_columns`. ([Polars User Guide][1])

---

## 0) The core mental model: “pure IR + optimizer” (and where determinism leaks)

A Polars `LazyFrame` is an abstraction over a **logical plan**, incrementally modified by API calls until you `collect()`. ([Polars User Guide][2])
Because expressions are lazy and composable, Polars can **simplify** and **reorder** work; separate expressions within a context are “embarrassingly parallel,” which is a major reason many rewrites are semantics-preserving. ([Polars User Guide][3])

Determinism “leaks” when your program accidentally depends on:

* **implicit ordering** (group/join/unique outputs, tie-breaking, “head without sort”)
* **implicit naming** (default output names, temporary columns from optimization)
* **implicit schema/dtype inference** (casts/parses/overflow behavior)
* **error timing** (strict casts/parses that may or may not see filtered-out bad rows)
* **debug-only intermediate columns** (optimizer drops unused work)

Your job is to make these contracts explicit so rewrites can’t invalidate them.

---

## 1) Your instrumentation surface: pin the optimizer knobs and compare plans

### 1.1 Plan inspection: “non-optimized vs optimized”

Polars explicitly distinguishes:

* a **non-optimized plan** (what you wrote)
* an **optimized plan** (rewritten by the optimizer) ([Polars User Guide][4])

You can snapshot both via `explain(optimized=False)` and `explain(optimized=True, …flags…)`. ([Polars User Guide][1])

### 1.2 The optimization toggles you care about (the ones that affect determinism contracts)

From `LazyFrame.explain` (same flags exist on `collect`/`profile`): ([Polars User Guide][1])

* **pushdowns**: `predicate_pushdown`, `projection_pushdown`, `slice_pushdown`
* **expression rewrites**: `simplify_expression`, `type_coercion`
* **caching**: `comm_subplan_elim` (common subplan), `comm_subexpr_elim` (common subexpression)
* **statement clustering**: `cluster_with_columns` (merge sequential independent `with_columns`)
* **execution mode**: `streaming` (changes physical plan, and can affect which optimizations run)

### 1.3 Canonical “plan matrix” helper (debugging determinism regressions)

```python
import polars as pl

OPT_FLAGS = dict(
    type_coercion=True,
    predicate_pushdown=True,
    projection_pushdown=True,
    simplify_expression=True,
    slice_pushdown=True,
    comm_subplan_elim=True,
    comm_subexpr_elim=True,
    cluster_with_columns=True,
    streaming=False,
)

def explain_matrix(lf: pl.LazyFrame) -> dict[str, str]:
    plans = {}
    plans["optimized_all"] = lf.explain(format="tree", optimized=True, **OPT_FLAGS)

    for k in ["predicate_pushdown", "projection_pushdown", "comm_subexpr_elim", "cluster_with_columns"]:
        flags = dict(OPT_FLAGS)
        flags[k] = False
        plans[f"optimized_no_{k}"] = lf.explain(format="tree", optimized=True, **flags)

    plans["unoptimized"] = lf.explain(format="tree", optimized=False)
    return plans
```

This is the fastest way to answer: *“Did the optimizer rewrite the plan in a way that invalidates an ordering / null / error policy?”* ([Polars User Guide][1])

---

## 2) Pass-by-pass: what can change, and what determinism contract you must enforce

### 2.1 Projection pushdown: unused columns never get read (and unused compute can disappear)

**What it does:** selects only needed columns at scan level. ([Polars User Guide][5])

**How it affects plan shape:** scan nodes show fewer columns; downstream operators may simplify; in combination with dead expression elimination/with_columns clustering, whole branches become removable.

**Determinism hazard:** “debug/validation-only” columns that aren’t referenced later may be dropped (so you think you’re computing a check, but the optimizer deletes it).

**Contract pattern: “validation anchors”**

* If a derived column is *meant to assert correctness*, it must be **used**:

  * keep it in output, or
  * reduce it into a scalar metric that remains in output (e.g., `bad_count`, `all_valid`).

Example anchor:

```python
def add_parse_and_anchor(lf: pl.LazyFrame) -> pl.LazyFrame:
    parsed = pl.col("s").cast(pl.Int64, strict=False)
    return (
        lf.with_columns(parsed.alias("_parsed"))
          .with_columns(pl.col("_parsed").is_null().sum().alias("bad_count"))
          .drop("_parsed")  # safe: bad_count now anchors the work
    )
```

---

### 2.2 Predicate pushdown: filters move earlier — **and strict-cast error behavior can change**

**What it does:** applies filters as early as possible, ideally at scan level. ([Polars User Guide][5])

**Determinism hazard:** **strict cast behavior is explicitly defined in terms of pushdown**:

> `strict`: “Raise if cast is invalid on rows **after predicates are pushed down**.” ([Polars User Guide][6])

That means the same *written* query can:

* **raise** when pushdown is disabled (cast sees bad rows),
* **not raise** when pushdown is enabled (bad rows get filtered out before cast is evaluated).

#### Demonstration: pushdown toggles change whether strict cast errors

```python
import polars as pl

df = pl.DataFrame({"s": ["1", "x", "2"]})

lf = (
    df.lazy()
      .filter(pl.col("s") != "x")               # can be pushed down
      .with_columns(pl.col("s").cast(pl.Int64, strict=True).alias("i"))
)

# Default: pushdown enabled => filter can remove "x" before strict cast sees it
ok = lf.collect()

# If we disable predicate pushdown, the strict cast may evaluate on "x" and raise
boom = lf.collect(predicate_pushdown=False)
```

**Contract options**

* If “bad rows should be dropped, not error”: stage filter *before* cast in code and use strict=True (and accept that pushdown is now part of semantics).
* If “bad rows should always be detected regardless of filtering”: cast with `strict=False` and then anchor an explicit assertion (`bad_count == 0`) on the *post-filter* dataset.

---

### 2.3 Slice pushdown: `head/limit` can move to scan-level (beware “top-N without sort”)

**What it does:** only loads the required slice from scan level and avoids materializing sliced outputs. ([Polars User Guide][5])

**Determinism hazard:** if you do `head(n)` without an explicit sort, you’re depending on whatever row order the scan emits. Slice pushdown can make that dependence even more “physical” (scan chunk boundaries, file order, partition order).

**Contract pattern**

* If output must be stable: **`sort` then `head`** (or `top_k` with a deterministic sort key), never `head` on an unordered frame.

---

### 2.4 `cluster_with_columns`: merges sequential independent `with_columns` (and removes dead assignments)

**What it does:** the pass “combine[s] sequential independent calls to with_columns.” ([Polars User Guide][1])
Polars’ own release notes describe it as removing redundancies and dropping “column assignments that are never used.” ([Polars][7])

**Determinism hazard:** any intermediate computed column that is not ultimately used can disappear, taking with it:

* strict cast/parsing errors you expected to surface,
* expensive computations you intended to benchmark,
* debug columns you assumed existed.

**Contract pattern**

* Treat `with_columns` as a declarative staging area; if a column matters, ensure it participates in:

  * final output, or
  * a downstream derivation that survives to output.

---

### 2.5 Common subexpression elimination (`comm_subexpr_elim`): caches repeated expressions (adds temp columns)

**What it does:** “Common subexpressions will be cached and reused.” ([Polars User Guide][1])

**Plan-shape effect:** Polars introduces temporary cached columns; you’ll often see internal names like `__POLARS_CSER_<hash>`. ([Stack Overflow][8])

**Determinism hazards**

1. **Plan snapshot churn**: internal temp names may change with expression structure or version (even when semantics don’t).
2. **Streaming interaction**: enabling streaming can implicitly disable this pass in some cases.

On the streaming interaction: Polars’ Rust implementation notes:

> the new streaming engine “can’t deal with the way the common subexpression elimination adds length-incorrect with_columns,” so it disables `COMM_SUBEXPR_ELIM` under that condition. ([Polars User Guide][9])

**Contract patterns**

* If you treat **plan text as a golden**:

  * either set `comm_subexpr_elim=False` for plan snapshot tests, or
  * canonicalize the plan text by regex-replacing `__POLARS_CSER_[0-9a-fx]+` with a placeholder.
* If you treat **performance** as critical:

  * keep `comm_subexpr_elim=True`, but remember that `streaming=True` may alter it; treat streaming/non-streaming as separate plan contracts.

---

### 2.6 Common subplan elimination (`comm_subplan_elim`): caches shared subtrees (self-joins/unions)

**What it does:** caches branching subplans occurring on self-joins or unions; the optimizer guide summarizes it as caching subtrees/file scans used by multiple subtrees. ([Polars User Guide][1])

**Determinism hazard:** usually none for results, but it can:

* change plan structure (cache nodes appear),
* change concurrency and memory pressure (timing changes),
* make plan snapshots differ.

**Contract pattern:** snapshot plans with explicit flags (on/off) depending on whether you want to lock in caching behavior.

---

### 2.7 Streaming mode (`streaming=True`) changes both execution and what optimizations are viable

`LazyFrame.explain` labels streaming “alpha” and exposes it as a plan mode. ([Polars User Guide][1])

**Determinism interactions**

* Several “stability knobs” explicitly **block streaming**, so you must choose:

  * stable ordering **or** streaming execution
* Examples:

  * `group_by(maintain_order=True)` blocks streaming; within each group row order is always preserved regardless. ([Polars User Guide][10])
  * `unique(maintain_order=True)` blocks streaming; and `keep='any'` gives no guarantee which duplicate row is kept. ([Polars User Guide][11])

**Contract pattern**

* Treat streaming as a separate execution contract:

  * avoid `maintain_order=True` in streaming pipelines,
  * impose ordering only at the very end via `sort` if needed (accepting memory cost), or
  * materialize intermediate results (breaking streaming) where determinism matters.

---

## 3) The ordering contract layer (optimizer-safe “fences”)

### 3.1 Joins: never rely on observed order without `maintain_order` or explicit `sort`

Polars explicitly warns:

> “Do not rely on any observed ordering without explicitly setting this parameter.” (re: `maintain_order`) ([Polars User Guide][12])

Even though the docs note “A left join preserves the row order of the left DataFrame,” the contract surface is still `maintain_order` (because algorithms/engines/streaming can change). ([Polars User Guide][12])

**Fence pattern**

* Immediately after join: either set `maintain_order="left"` (or appropriate) *and/or* `sort` by stable keys.

### 3.2 Group-by: pick either `maintain_order=True` (non-streaming) or `sort` (portable)

`maintain_order=True` gives group order consistent with input; but it blocks streaming. ([Polars User Guide][10])
If you need determinism *and* streaming, you generally sort the grouped result explicitly.

### 3.3 Unique/dedup: never use `keep="any"` in a deterministic pipeline

Docs: `keep='any'` “does not give any guarantee of which row is kept,” and enables more optimizations. ([Polars User Guide][11])
If you need deterministic dedup:

* define a tie-breaker sort order,
* then use `keep='first'|'last'` (or `none`) with `maintain_order=True` if you can afford non-streaming, or sort afterward.

---

## 4) How to structure pipelines so determinism contracts survive optimization

### 4.1 “Fence + normalize + assert” pipeline template

1. **Normalize schema early**: explicit casts/parses with chosen strictness.
2. **Fence ordering after nondeterministic operators**: join/group_by/unique/head.
3. **Anchor validation**: reduce checks into surviving columns (`bad_count`, `all_valid`).
4. **Assert with golden tests**:

   * schema (via `collect_schema`)
   * values (via `polars.testing.assert_frame_equal`)
   * plan (via `explain(format="tree")` with pinned flags)

### 4.2 Golden tests that are robust to optimizer churn

```python
from pathlib import Path
import re
import polars as pl
from polars.testing import assert_frame_equal

CSER_RE = re.compile(r"__POLARS_CSER_[0-9a-fx]+")  # temp columns from CSE

def explain_stable(lf: pl.LazyFrame, *, flags: dict) -> str:
    plan = lf.explain(format="tree", optimized=True, **flags)
    # optional: scrub CSE temp names if you snapshot plans with comm_subexpr_elim=True
    return CSER_RE.sub("__POLARS_CSER_<id>", plan)

def test_contract(tmp_path: Path):
    df = pl.DataFrame({"k": ["a","a","b"], "s": ["1","x","2"]})

    lf = (
        df.lazy()
          .filter(pl.col("s") != "x")
          .with_columns(pl.col("s").cast(pl.Int64, strict=False).alias("i"))
          .with_columns(pl.col("i").is_null().sum().alias("bad_count"))
          .group_by("k", maintain_order=True)     # determinism fence (non-streaming)
          .agg(pl.col("i").sum().alias("sum_i"), pl.col("bad_count").sum().alias("bad"))
          .sort("k")                              # total order fence
    )

    # schema contract
    assert lf.collect_schema() == pl.Schema({"k": pl.String(), "sum_i": pl.Int64(), "bad": pl.UInt32()})

    # plan contract (pin flags)
    flags = dict(
        type_coercion=True,
        predicate_pushdown=True,
        projection_pushdown=True,
        simplify_expression=True,
        slice_pushdown=True,
        comm_subplan_elim=True,
        comm_subexpr_elim=True,
        cluster_with_columns=True,
        streaming=False,
    )
    plan_path = tmp_path / "plan.txt"
    plan = explain_stable(lf, flags=flags)
    if not plan_path.exists():
        plan_path.write_text(plan)
        raise AssertionError("wrote golden plan; re-run")

    assert plan == plan_path.read_text()

    # value contract
    out = lf.collect()
    expected = pl.DataFrame({"k": ["a","b"], "sum_i": [1, 2], "bad": [0, 0]})
    assert_frame_equal(out, expected, check_row_order=True, check_column_order=True, check_dtypes=True)
```

Key ideas the optimizer can’t break:

* validation is anchored (`bad_count` survives)
* ordering is fenced (`maintain_order` + `sort`)
* strictness is explicit (`strict=False` + downstream “bad” assertions)
* plan snapshot is pinned to a specific set of flags (and scrubbed for temp names)

---

## 5) Quick “determinism under optimization” checklist (agent-friendly)

* **If a computed column matters, keep it alive** (or reduce it into a metric). Cluster/dead-expression elimination will remove unused work. ([Polars][7])
* **Assume pushdown can change error behavior**: strict casts are defined relative to predicates pushed down. ([Polars User Guide][6])
* **Never depend on output order without an order fence**:

  * join: set `maintain_order` or sort; docs explicitly warn. ([Polars User Guide][12])
  * group_by: `maintain_order=True` or sort; streaming tradeoff. ([Polars User Guide][10])
  * unique: avoid `keep="any"`, use stable tie-breakers; maintain_order blocks streaming. ([Polars User Guide][11])
* **If you snapshot plans, treat `comm_subexpr_elim` carefully** (temp names) and remember streaming can disable it. ([Polars User Guide][9])
* **Use `explain` as your “diff tool”**: compare optimized vs unoptimized and toggle passes to localize changes. ([Polars User Guide][1])

---

If you want the next deep dive, the natural continuation is: **“Streaming engine vs in-memory engine: determinism/performance trade space”**—mapping exactly which operators block streaming (`maintain_order`, certain joins, sorts, unique variants), and how to design a pipeline that is *streamable by default* but can selectively materialize deterministic “fence points” when needed.

[1]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.explain.html?utm_source=chatgpt.com "polars.LazyFrame.explain — Polars documentation"
[2]: https://docs.pola.rs/api/rust/dev/polars_lazy/frame/struct.LazyFrame.html?utm_source=chatgpt.com "LazyFrame in polars_lazy::frame - Rust"
[3]: https://docs.pola.rs/user-guide/concepts/expressions-and-contexts/?utm_source=chatgpt.com "Expressions and contexts"
[4]: https://docs.pola.rs/user-guide/lazy/query-plan/?utm_source=chatgpt.com "Query plan - Polars user guide"
[5]: https://docs.pola.rs/user-guide/lazy/optimizations/?utm_source=chatgpt.com "Optimizations - Polars user guide"
[6]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.cast.html?utm_source=chatgpt.com "polars.Expr.cast — Polars documentation"
[7]: https://pola.rs/posts/polars-in-aggregate-jun24/?utm_source=chatgpt.com "Faster CSV writer, dead expression elimination, and more"
[8]: https://stackoverflow.com/questions/78191944/what-is-the-difference-between-read-scan-and-sink-in-polars?utm_source=chatgpt.com "What is the difference between read, scan, and sink in ..."
[9]: https://docs.pola.rs/api/rust/dev/src/polars_lazy/frame/mod.rs.html?utm_source=chatgpt.com "polars_lazy/frame/ mod.rs"
[10]: https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.group_by.html?utm_source=chatgpt.com "polars.DataFrame.group_by — Polars documentation"
[11]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.unique.html?utm_source=chatgpt.com "polars.LazyFrame.unique — Polars documentation"
[12]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.join.html?utm_source=chatgpt.com "polars.DataFrame.join — Polars documentation"

# Streaming engine vs in-memory engine: determinism/performance trade space (Polars)

Polars’ **lazy** API can execute your logical plan in two broad modes:

* **In-memory (“single batch”) execution**: `collect()` processes the query as one batch; peak memory must fit in RAM. ([Polars User Guide][1])
* **Streaming execution**: the engine executes the query **in batches**, enabling “larger-than-memory” workflows and often improving performance. ([Polars User Guide][2])

Polars’ own docs emphasize that the streaming engine is still considered **unstable/subject to change**, and that you should use `explain()` to see whether streaming applies. ([Polars User Guide][3])

A key architectural point: the “new streaming engine” (tracked publicly) aims for *all* queries that run on the in-memory engine to run on streaming, but **some operations may not yet have native streaming implementations**, in which case Polars **falls back transparently**. ([GitHub][4])

---

## 1) How you *select* streaming, and how you *verify* what happened

### 1.1 Selecting streaming

Current docs/use-guides show two main entrypoints:

* **Preferred** (user guide): `lf.collect(engine="streaming")` ([Polars User Guide][2])
* **Legacy / still present in API**: `lf.collect(streaming=True)` exists as a boolean flag on `collect`, and is described as “process the query in batches.” ([Polars User Guide][3])

### 1.2 Verifying streaming applicability (the “truth surface”)

* `LazyFrame.explain(streaming=True, format="tree")`: the `streaming` flag is explicitly documented as “run parts of the query in a streaming fashion (alpha state)”. ([Polars User Guide][5])
* `LazyFrame.show_graph(... streaming=True ...)`: visual plan inspection; Graphviz required. ([Polars User Guide][6])
* For the *physical* streaming plan, Polars’ user guide and streaming-engine tracking issue explicitly call out `.show_graph(engine="streaming", plan_stage="physical")` as the right inspection tool. ([Polars User Guide][2])

---

## 2) The determinism knobs that *explicitly* block streaming

The highest-signal rule is: **“order-preservation features tend to block streaming”**, because they require global coordination across batches.

### 2.1 Operator/knob matrix (documented blockers)

| Operation  | Determinism knob                         | Streaming impact (documented)         | Determinism alternative that keeps streaming viable                                                       |
| ---------- | ---------------------------------------- | ------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `group_by` | `maintain_order=True`                    | **Blocks streaming**                  | `group_by(...).agg(...).sort(keys...)` (total order after aggregation)                                    |
| `unique`   | `maintain_order=True`                    | **Blocks streaming**                  | pre-sort with deterministic tie-break → `unique(keep="first"/"last", maintain_order=False)` → sort output |
| `sort`     | `maintain_order=True` (stable ties)      | **Blocks streaming**                  | add a tie-breaker column so “stable ties” don’t matter; keep `maintain_order=False`                       |
| `join`     | `validate=...` (join cardinality checks) | **Not supported by streaming engine** | compute uniqueness checks separately (group_by counts) or run validation in a materialized fence step     |

**Proof points in docs**

* `group_by(..., maintain_order=True)` explicitly: “blocks the possibility to run on the streaming engine.” ([Polars User Guide][7])
* `unique(..., maintain_order=True)` explicitly blocks streaming; and `keep="any"` is explicitly non-guaranteed. ([Polars User Guide][8])
* `LazyFrame.sort(maintain_order=True)` explicitly: “if true streaming is not possible … requires a stable search.” ([Polars User Guide][9])
* `join(validate=...)` explicitly: “not supported by the streaming engine.” ([Polars User Guide][10])

---

## 3) “Certain joins / sorts / uniques” in streaming: what’s actually stream-native vs what falls back

### 3.1 Streaming engine capability is *node-dependent*, not “method-dependent”

Even if an API method exists, the question is: **does your plan lower to native streaming nodes**?

Polars’ streaming-engine tracking issue is the most concrete “operator inventory” available: it lists native streaming nodes such as **projection, filter, group-by, equi-join, cross-join, asof/inequality join, anti/semi join, slices/head/tail, concat, gather**, plus many plan-lowering patterns like `top_k/bottom_k`, `unique`, and `.over()` lowering to group-by + join. ([GitHub][4])

Two important implications:

* **Joins and sorts can be streaming/out-of-core**, but *specific options* (like join `validate` checks, or stable sort ties) can disable native streaming or force fallback. ([GitHub][4])
* “Works” doesn’t mean “native streaming”: unsupported pieces may silently run via fallback to in-memory; the tracking issue explicitly describes this behavior. ([GitHub][4])

### 3.2 Sorting: streamable vs stable

* Polars is actively treating **sort** as an “out-of-core” target for the streaming engine. ([GitHub][4])
* But **stable tie ordering** (`maintain_order=True`) disables streaming for `sort`. ([Polars User Guide][9])

### 3.3 Dedup (`unique`) variants: determinism vs optimization

* `unique(keep="any")` explicitly provides **no guarantee which row is kept**, and that non-guarantee enables more optimizations. ([Polars User Guide][8])
* `unique(maintain_order=True)` explicitly blocks streaming. ([Polars User Guide][8])
* The streaming-engine tracker lists `unique` as a plan translation / native capability target. ([GitHub][4])

---

## 4) Designing a pipeline that is “streamable by default” but can enforce determinism at fence points

### 4.1 Foundational rule: stay lazy and favor `scan_*`

Polars’ user guide explicitly recommends using `scan_*` over `read_*` for lazy pipelines: it enables **pushdown** (skip unneeded columns/rows) and allows streaming to process batches before the file is fully read. ([Polars User Guide][11])

**Template**

```python
import polars as pl

lf = (
    pl.scan_parquet("s3://bucket/path/*.parquet")  # or scan_csv/scan_delta/etc.
      .select([...])   # projection pushdown
      .filter(...)     # predicate pushdown
      .with_columns(...)  # expression transforms
      # keep this “orderless” as long as possible
)
```

### 4.2 Replace “order-preserving knobs” with “explicit total ordering”

If you need deterministic output order **but also want streaming**, don’t use `maintain_order=True` (which blocks streaming on key operators). Instead:

* **After `group_by`**: sort by the group keys (or by a deterministic derived key) rather than `maintain_order=True`. (`maintain_order=True` blocks streaming.) ([Polars User Guide][7])
* **After `join`**: impose deterministic ordering with an explicit sort key (don’t rely on incidental join order; Polars documents a specific order guarantee only for left join row order in the standard join docs, and streaming/native lowering may differ). ([Polars User Guide][10])
* **For stable sort ties without `maintain_order=True`**: make ties impossible by adding a deterministic tiebreaker column to the sort key:

  * content-hash tiebreaker (`hash(keys...)`)
  * stable row id from source metadata if available (file + row-group + row-offset, etc.)
  * “semantic rank” tiebreaker (timestamp, version, ingestion id)

This lets you keep `sort(maintain_order=False)` (streamable/out-of-core) while still producing deterministic results.

### 4.3 Deterministic dedup without `unique(maintain_order=True)`

Goal: pick a representative row deterministically, without requiring “preserve input order”.

Pattern:

1. Pre-sort by deterministic priority (e.g., newest timestamp first) and a deterministic tie-breaker.
2. Dedup with `keep="first"` (or `"last"`) and `maintain_order=False`.
3. Sort output by your desired presentation keys.

Avoid `keep="any"` if the chosen representative matters (it’s explicitly nondeterministic). ([Polars User Guide][8])

### 4.4 Fence points: materialize determinism *without* forcing RAM materialization

If you “must” do something that blocks streaming (e.g., stable sort ties, order-preserving group order, heavy validations), use a **fence point**:

* **Fence-to-disk (preferred for big data)**: use native sinks that execute in streaming mode and write results without collecting all data into RAM.

  * `LazyFrame.sink_parquet(...)` is explicitly “evaluate the query in streaming mode and write to a Parquet file,” enabling larger-than-RAM results to disk. ([Polars User Guide][12])
  * The user guide shows sinks as a way to stream results to storage and avoid holding all data in RAM, including partitioning strategies (e.g., max rows per file). ([Polars User Guide][11])

* **Fence-to-memory (only if small enough)**: `df = lf.collect(engine="streaming")` then `df.lazy()`; but you’ve now accepted that the fence requires RAM.

Concrete “streamable by default” architecture:

1. `scan_*` → transformations → **sink_parquet** (streaming)
2. `scan_parquet(fenced_output)` → deterministic operations that require global state → final sink

### 4.5 Streaming sinks and “processed order”

Some sink APIs expose `maintain_order` (defaulting to True in `sink_parquet`) controlling processing order; turning it off can be faster. ([Polars User Guide][12])
Treat this as a *separate* knob from “logical ordering correctness”: if you require deterministic ordering of rows in the written file, enforce it with explicit sort keys (and avoid stable ties as described above).

### 4.6 “Python sink” escape hatch (only when you must)

`LazyFrame.sink_batches` exists for “call a user-defined function for every ready batch,” enabling streaming results larger than RAM “in certain cases,” but it’s explicitly marked unstable and “much slower than native sinks.” ([Polars User Guide][13])
Use it only when a native sink (parquet/ipc/csv/ndjson) can’t express your output shape.

---

## 5) Practical operator mapping for your “blockers list”

### 5.1 `maintain_order` (the canonical streaming killer)

* `group_by(..., maintain_order=True)` blocks streaming. ([Polars User Guide][7])
* `unique(..., maintain_order=True)` blocks streaming. ([Polars User Guide][8])
* `sort(..., maintain_order=True)` blocks streaming. ([Polars User Guide][9])

### 5.2 Sorts

* Sort is explicitly on the streaming engine roadmap as out-of-core and as a streaming node class, but stable ties block streaming. ([GitHub][4])
* Prefer `top_k/bottom_k` over full sorts when you only need “N best”; these are explicitly called out as being lowered to the streaming engine. ([GitHub][4])

### 5.3 Joins

* Join *cardinality validation* (`validate=...`) is explicitly not supported by streaming. ([Polars User Guide][10])
* Streaming engine supports multiple join node types (equi-join, cross, anti/semi, asof/inequality) per the streaming engine tracker. ([GitHub][4])
* If you need deterministic order after join, add an explicit sort fence (don’t rely on incidental ordering across engines). ([GitHub][4])

---

## 6) Minimal “streaming-first pipeline skeleton” with deterministic fences

```python
import polars as pl

def stage1_streamable(src_glob: str) -> pl.LazyFrame:
    return (
        pl.scan_parquet(src_glob)
          .select(["k", "ts", "v"])              # pushdown-friendly
          .filter(pl.col("v").is_not_null())     # pushdown-friendly
          .with_columns(
              pl.col("ts").cast(pl.Datetime),    # explicit types
              pl.col("v").cast(pl.Float64),
          )
          # avoid maintain_order knobs here
    )

def stage2_aggregate_streamable(lf: pl.LazyFrame) -> pl.LazyFrame:
    return (
        lf.group_by("k")                          # maintain_order=False default
          .agg(
              v_sum=pl.col("v").sum(),
              ts_max=pl.col("ts").max(),
          )
          .sort("k")                               # deterministic output order (avoid group_by maintain_order)
    )

def fence_to_parquet(lf: pl.LazyFrame, out_path: str) -> None:
    lf.sink_parquet(out_path)                      # streaming sink to disk

# usage:
lf1 = stage1_streamable("s3://bucket/in/*.parquet")
lf2 = stage2_aggregate_streamable(lf1)
fence_to_parquet(lf2, "s3://bucket/stage2/out.parquet")
```

This pattern:

* uses `scan_*` so pushdowns and streaming batch processing can kick in ([Polars User Guide][11])
* enforces determinism by explicit `sort` (rather than `maintain_order=True` which would block streaming on group_by) ([Polars User Guide][7])
* materializes at a fence via `sink_parquet` to avoid RAM materialization for large outputs ([Polars User Guide][12])

---

If you want the next deep dive after this, the natural continuation is **“Streaming physical plan inspection + memory-intensity reasoning”**: how to interpret the streaming physical graph output, how to identify fallback-to-in-memory segments, and how to restructure joins/aggregations to reduce “memory-heavy” nodes (including “sorted join/group_by” patterns that the streaming engine explicitly optimizes). ([Polars User Guide][2])

[1]: https://docs.pola.rs/user-guide/lazy/execution/ "Query execution - Polars user guide"
[2]: https://docs.pola.rs/user-guide/concepts/streaming/ "Streaming - Polars user guide"
[3]: https://docs.pola.rs/docs/python/dev/reference/lazyframe/api/polars.LazyFrame.collect.html "polars.LazyFrame.collect — Polars  documentation"
[4]: https://github.com/pola-rs/polars/issues/20947 "Tracking issue for the new streaming engine · Issue #20947 · pola-rs/polars · GitHub"
[5]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.explain.html "polars.LazyFrame.explain — Polars  documentation"
[6]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.show_graph.html "polars.LazyFrame.show_graph — Polars  documentation"
[7]: https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.group_by.html?utm_source=chatgpt.com "polars.DataFrame.group_by — Polars documentation"
[8]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.unique.html "polars.DataFrame.unique — Polars  documentation"
[9]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.sort.html "polars.LazyFrame.sort — Polars  documentation"
[10]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.join.html "polars.DataFrame.join — Polars  documentation"
[11]: https://docs.pola.rs/user-guide/lazy/sources_sinks/ "Sources and sinks - Polars user guide"
[12]: https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html "polars.LazyFrame.sink_parquet — Polars  documentation"
[13]: https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.sink_batches.html "polars.LazyFrame.sink_batches — Polars  documentation"

# Streaming physical plan inspection + memory-intensity reasoning (Polars)

This section is about turning “streaming=True didn’t fix my OOM” into a **repeatable diagnosis loop**:

1. **Inspect** the *streaming physical plan* (not just logical IR),
2. **Identify** where the plan is **falling back** to in-memory execution,
3. **Restructure** joins/aggregations so the physical plan uses **streaming-native** (and ideally **sorted**) operators.

---

## 1) The two plan layers you must differentiate

### 1.1 Logical IR vs streaming physical plan

Polars lazy has a logical plan (“IR”) that the optimizer rewrites, and a physical plan that gets executed. The streaming engine is special in that it exposes a *separate physical stage* (and in general may rewrite for batched execution). The Polars user guide explicitly recommends inspecting the **physical plan of a streaming query** via the physical graph. ([Polars User Guide][1])

### 1.2 Streaming is batch execution + selective fallback

Polars streaming executes in **batches** (to reduce memory pressure and enable larger-than-memory execution) and can be **more performant** than the in-memory engine. ([Polars User Guide][1])
If a part of your query is not supported (or not implemented natively yet), Polars will **fall back** to the in-memory engine for those parts—often transparently, i.e. without you changing code. ([Polars User Guide][1])

---

## 2) Inspection toolkit (what to run every time)

### 2.1 Produce the streaming physical graph (+ memory legend)

The streaming guide’s canonical debug call is:

```python
q.show_graph(plan_stage="physical", engine="streaming")
```

…and it explicitly notes: **the legend shows how memory intensive the operation can be**, and that fallback happens for non-streaming ops. ([Polars User Guide][1])

> Practical interpretation: this graph is your “OOM heatmap.” The nodes with the “most memory-intensive” legend category are the ones to attack first.

### 2.2 Generate DOT output for programmatic analysis (no screenshots needed)

`LazyFrame.show_graph` can return **Graphviz DOT** (so you can diff/parse it) and requires Graphviz installed to render. It also documents that `raw_output=True` returns DOT and can’t be combined with `show` or `output_path`. ([Polars User Guide][2])

```python
from pathlib import Path
import polars as pl

def dump_plan_dot(lf: pl.LazyFrame, dot_path: Path) -> None:
    dot = lf.show_graph(raw_output=True)  # DOT text :contentReference[oaicite:5]{index=5}
    dot_path.write_text(dot)
```

If you’re using the streaming physical stage API (`plan_stage="physical", engine="streaming"`), prefer dumping that DOT as your “ground truth” artifact (it’s the only view that will tell you where streaming is actually applied). ([Polars User Guide][1])

### 2.3 How to read plan graphs (direction + operator glyphs)

Polars’ query plan guide states the visualization should be read **bottom-to-top**; each box is a stage; `sigma` is selection (filter) and `pi` is projection (column selection). ([Polars User Guide][3])
That applies conceptually to the physical graph too: start from sinks/outputs and walk “down” to scans.

---

## 3) Identifying fallback-to-in-memory segments

### 3.1 The documented behavior

Streaming docs: “Some operations are inherently non-streaming, or are not implemented in a streaming manner (yet)… Polars will fall back to the in-memory engine for those operations.” ([Polars User Guide][1])
Polars (Dec 2025 blog): fallback can be **silent** and localized to the unsupported parts. ([Polars][4])
Streaming tracking issue: same message, and it tells you to visualize with `.show_graph(engine='streaming', plan_stage='physical')`. ([GitHub][5])

### 3.2 A practical detection loop (works even when fallback is silent)

1. **Render the streaming physical graph** (the memory legend is your guide). ([Polars User Guide][1])
2. **Locate the first memory-heavy node upstream of your scans** (joins/group-bys/sorts are typical).
3. **If that node is not a streaming-native form**, restructure around it (next sections).
4. **Re-render** and confirm the physical plan now contains streaming-native nodes (and ideally sorted variants).

> You don’t need to guess. You iterate until the physical graph is “all streaming-native” or you’ve isolated the remaining non-streaming operator.

---

## 4) Memory-intensity reasoning: which operators usually dominate

### 4.1 “Out-of-core” doesn’t mean “free”

Polars explicitly tracks **out-of-core** implementations for the big three: **group-by, equi-join, sort**. ([GitHub][5])
These are still the nodes that tend to dominate memory footprint; out-of-core just means the engine *can* stream / spill / batch them rather than requiring full materialization.

### 4.2 Streaming-native operator inventory (what you *want* to see)

The streaming tracking issue lists streaming nodes such as Projection, Filter, Group-by, Equi-join, Cross-join, ASOF/Inequality join, Anti/Semi join, slices/head/tail, gather, and explicitly calls out **Merge-Sorted**, **Sorted Group By**, and **Merge / Sorted Equality Join** as streaming capabilities. ([GitHub][5])

That’s your “target state”: when your workload is memory-bound, aim to transform the plan so it uses these streaming nodes instead of a fallback in-memory subplan.

---

## 5) Restructuring playbook: making joins and group-bys less memory-heavy

### 5.1 First principles: shrink width, then shrink rows, then do the heavy op

**Width shrink:** projection pushdown / early `select`
**Row shrink:** predicate pushdown / early `filter`
Polars’ query plan guide shows that the optimizer can push filters down into scans (“predicate pushdown”) so fewer rows are loaded/processed. ([Polars User Guide][3])

Pattern:

```python
lf = (
    pl.scan_parquet("...")     # prefer scan_* for pushdowns
      .select(["k", "ts", "v"])  # shrink width before the expensive nodes
      .filter(pl.col("v").is_not_null())
)
```

Then join/aggregate.

### 5.2 Avoid determinism knobs that force global coordination (they often block streaming)

* `group_by(..., maintain_order=True)` blocks streaming. ([Polars User Guide][6])
* `sort(..., maintain_order=True)` makes streaming impossible for that sort (stable tie order). ([Polars User Guide][7])

If you need deterministic output order **and** streaming, prefer:

* streaming-friendly execution → then apply an explicit sort key (with deterministic tie-breakers) *after* the contraction step, and avoid stable sorting requirements.

### 5.3 Don’t use join validation in streaming paths

Join `validate=...` checks are **not supported by the streaming engine** (per Polars docs). ([Polars User Guide][8])
If you need correctness checks, do them as a separate (possibly materialized) fence step (e.g., group_by key counts) rather than `validate=` inside the join.

---

## 6) “Sorted” fast paths: the biggest single lever for memory + throughput

Polars has explicit sorted fast paths, and the streaming engine tracks **Merge-Sorted**, **Sorted Group By**, and **Merge / Sorted Equality Join**. ([GitHub][5])
Your job is to *make sortedness true* (or make it *known* to the engine) so these nodes can be used.

### 6.1 Mark sorted columns when they really are sorted (`set_sorted`)

`Expr.set_sorted` explicitly exists to “enable downstream code to use fast paths for sorted arrays,” and warns it can lead to incorrect results if the data is not actually sorted. ([Polars User Guide][9])

Example (lazy):

```python
import polars as pl

lf = (
    pl.scan_parquet("...")
      .with_columns(pl.col("k").set_sorted())  # only if k is truly sorted :contentReference[oaicite:20]{index=20}
)
```

### 6.2 Sorted merge operations: `merge_sorted`

`LazyFrame.merge_sorted(other, key)` is explicitly “take two sorted DataFrames and merge them by the sorted key,” output remains sorted, and it is **the caller’s responsibility** that frames are actually sorted (otherwise output “will not make sense”). ([Polars User Guide][10])

This is the most “explicit” way to force a merge-sorted pattern when schemas match and your semantics are a merge rather than a join.

### 6.3 Sorted join fast path (single-key sorted merge join)

There is an explicit Polars issue documenting that **joining on a single sorted column** uses a fast **sorted merge join** path. ([GitHub][11])
In the streaming engine inventory, this corresponds to the “Merge / Sorted Equality Join” class of nodes. ([GitHub][5])

**Practical recipe: turn a hash-join into a merge-join**

1. Ensure both sides are sorted by the join key (ideally already sorted at source; otherwise sort once).
2. Mark the key as sorted (or rely on Polars tracking sortedness from your sort).
3. Re-run `.show_graph(plan_stage="physical", engine="streaming")` and verify you’re seeing the sorted join node. ([Polars User Guide][1])

> Caveat: sorting can be expensive; this is best when data is *already naturally sorted* (time series, ingestion order, partitioned layouts) or when you can amortize sort cost across multiple downstream joins.

### 6.4 Sorted group-by: when it helps most

The streaming engine tracks a dedicated **Sorted Group By** optimization. ([GitHub][5])
Sortedness is especially common in time-series pipelines; for example, Polars time-window grouping APIs require the index column be sorted ascending (globally or within group). ([Polars User Guide][12])

If your data is already sorted by the group key, mark it sorted and let the engine pick the sorted-group-by path. ([Polars User Guide][9])

---

## 7) Join/aggregation rewrites that reduce memory footprint

### 7.1 Pre-aggregate before join (reduce build-side cardinality)

Instead of joining raw fact tables:

* aggregate the “dimension” side to one row per key (or per key window)
* then join.

This shrinks the join’s hash table / state size and is often the simplest memory win.

```python
dim = (
    pl.scan_parquet("dim.parquet")
      .select(["k", "attr"])
      .group_by("k")
      .agg(pl.col("attr").last().alias("attr"))   # 1 row per key
)

fact = pl.scan_parquet("fact.parquet").select(["k", "v", "ts"])

q = fact.join(dim, on="k", how="left")
```

Then inspect streaming physical plan again. ([Polars User Guide][1])

### 7.2 Use semi/anti joins when you only need filtering

Streaming node inventory explicitly includes **anti/semi join**. ([GitHub][5])
If your goal is “keep rows whose keys exist in the other table,” a semi join is typically cheaper than a full join + projection.

### 7.3 Prefer `top_k/bottom_k` over `sort().head(n)` when you need top-N

Streaming plan translation includes lowering `top_k/bottom_k` to streaming nodes. ([GitHub][5])
This often reduces memory vs a full global sort.

---

## 8) “Fence points” to isolate heavy nodes without collecting to RAM

If you need deterministic ordering, validations, or a non-streaming operation, insert a **materialization fence** to disk using streaming sinks, rather than calling `collect()`.

* `sink_*` is recommended as the memory-efficient alternative to writing after `collect`, and it runs on the streaming engine by default. ([Polars][4])
* `LazyFrame.sink_parquet` explicitly allows “results larger than RAM to be written to disk.” ([Polars User Guide][13])

This lets you:

* Stage 1: all streaming-native ops → `sink_parquet`
* Stage 2: reload (`scan_parquet`) → do determinism/validation ops → sink again

---

## 9) Minimal “inspection harness” you can reuse in your repo

```python
from __future__ import annotations
from pathlib import Path
import polars as pl

def inspect_streaming(lf: pl.LazyFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Logical plan (optimized): useful but not sufficient
    (out_dir / "logical_tree.txt").write_text(lf.explain(format="tree"))  # :contentReference[oaicite:33]{index=33}

    # Streaming physical graph: truth surface for memory + fallback
    # (requires Graphviz to render; otherwise use raw DOT)
    try:
        lf.show_graph(plan_stage="physical", engine="streaming", output_path=out_dir / "physical.svg")
    except TypeError:
        # fallback for older Polars signatures: at least dump DOT of default graph
        dot = lf.show_graph(raw_output=True)  # DOT text :contentReference[oaicite:34]{index=34}
        (out_dir / "graph.dot").write_text(dot)
```

Use this as a baseline artifact generator; then your “rewrite loop” is: change query → regenerate → diff `physical.svg`/`graph.dot`.

---

### Where to go next

The natural follow-on after this is **a catalog of “streaming blockers and rewrites” by operator family**, driven by the streaming engine’s tracked capabilities (e.g., which window functions lower to streaming; how `.over()` lowers; which join patterns lower to sorted equality join; which aggregates remain memory-heavy). The tracking issue’s “Streaming Nodes / Plan translation” section is the best seed list for that inventory. ([GitHub][5])

[1]: https://docs.pola.rs/user-guide/concepts/streaming/ "Streaming - Polars user guide"
[2]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.show_graph.html "polars.LazyFrame.show_graph — Polars  documentation"
[3]: https://docs.pola.rs/user-guide/lazy/query-plan/?utm_source=chatgpt.com "Query plan - Polars user guide"
[4]: https://pola.rs/posts/polars-in-aggregate-dec25/ "Polars — Polars in Aggregate: Polars Cloud, Streaming engine, and New Data Types"
[5]: https://github.com/pola-rs/polars/issues/20947 "Tracking issue for the new streaming engine · Issue #20947 · pola-rs/polars · GitHub"
[6]: https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.group_by.html?utm_source=chatgpt.com "polars.DataFrame.group_by — Polars documentation"
[7]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.sort.html?utm_source=chatgpt.com "polars.LazyFrame.sort — Polars documentation"
[8]: https://docs.pola.rs/docs/python/version/0.19/reference/lazyframe/index.html?utm_source=chatgpt.com "LazyFrame — Polars documentation"
[9]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.set_sorted.html?utm_source=chatgpt.com "polars.Expr.set_sorted — Polars documentation"
[10]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.merge_sorted.html?utm_source=chatgpt.com "polars.LazyFrame.merge_sorted"
[11]: https://github.com/pola-rs/polars/issues/10961?utm_source=chatgpt.com "Faster join on multiple columns where one is sorted ..."
[12]: https://docs.pola.rs/py-polars/html/reference/dataframe/index.html?utm_source=chatgpt.com "DataFrame — Polars documentation"
[13]: https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_parquet.html?utm_source=chatgpt.com "polars.LazyFrame.sink_parquet — Polars documentation"

# Streaming blockers + rewrites catalog (by operator family)

This is a **streaming-centric operator inventory** for Polars Lazy, keyed off the streaming engine tracking issue’s **“Streaming Nodes”** and **“Plan translation to streaming”** lists, plus the documented API “gotchas” that explicitly disable streaming. ([GitHub][1])

A crucial frame: since Polars’ streaming engine runs queries **in batches**, some operators run natively streaming, some are translated into streaming nodes, and anything else may **fall back** to the in-memory engine (often silently). Use the **streaming physical plan** (`show_graph(engine="streaming", plan_stage="physical")`) to confirm where streaming is actually happening and see the memory-intensity legend. ([Polars User Guide][2])

---

## 0) Inspection loop (your invariant workflow)

**Author → visualize → rewrite → re-visualize**:

```python
q = (
    pl.scan_parquet("...")  # or scan_csv/scan_delta/etc.
      .filter(...)
      .group_by("k")
      .agg(...)
)

# truth surface for streaming: physical plan + memory legend
q.show_graph(engine="streaming", plan_stage="physical")
# then run:
df = q.collect(engine="streaming")
```

Polars documents that streaming runs in batches, can reduce memory pressure, and that unsupported ops can fall back to in-memory; the physical graph is the inspection tool for this. ([Polars User Guide][2])
The streaming tracking issue reiterates the same entrypoints (`collect(engine="streaming")` / `sink_*`) and that the *physical* plan can be visualized with `.show_graph(engine="streaming", plan_stage="physical")`. ([GitHub][1])

---

## 1) Sources & sinks (IO family)

### 1.1 Streaming sources (what can stay out-of-core from the start)

Tracked sources include: Parquet, CSV, IPC, NDJson, multifile/Hive, IO plugins, PyArrow dataset, AnonymousScan, and explicitly `scan_delta` / `scan_iceberg`. ([GitHub][1])

**Rewrite rule:** if you’re starting from `read_*` and hitting memory walls, move to `scan_*` so predicate/projection pushdown + streaming batching can apply earlier (this is the backbone of “streamable by default”). ([Polars User Guide][2])

### 1.2 Streaming sinks (the “fence without collect()”)

Tracked sinks include: Parquet/CSV/IPC/NDJson, partitioned writers (ByKey/Hive), anonymous sink, plus Python sinks (`sink_batches` / `collect_batches`). ([GitHub][1])

**Rewrite rule:** when you need a **determinism fence** or you hit a non-streaming operator, prefer `sink_*` to disk/object-store over `collect()` to RAM; it keeps the upstream part streaming-native.

---

## 2) “Core pipeline” operators (projection/filter/concat/slice/gather/unpivot)

### 2.1 Streaming-native nodes in this family

The tracking issue lists streaming nodes for: **Projection**, **Filter**, **Positive slice / head**, **Negative slice / tail**, **Gather**, **Horizontal concat**, **Vertical concat**, and **Unpivot**. ([GitHub][1])

### 2.2 Plan translations that matter here

Polars also tracks plan-lowering of **filters/sorts/aggregates/literal series in selections**, which means you should prefer expressing work inside `select/with_columns` (not post-collect) to let the planner lower it into streaming nodes. ([GitHub][1])

### 2.3 Rewrite rules

* **Push down width early**: `select([...])` before joins/groupbys/sorts.
* **Push down rows early**: `filter(...)` before joins/groupbys/sorts.
* Prefer `pl.concat` / `LazyFrame.concat`-style composition over Python-side concatenation; concatenation is explicitly tracked for streaming translation and has native concat nodes. ([GitHub][1])
* Prefer `unpivot/melt`-style reshape over “manual pivot-ish Python loops”; unpivot is explicitly a streaming node. ([GitHub][1])

---

## 3) Group-by + aggregates (aggregation family)

### 3.1 What the streaming engine explicitly targets

The tracking issue calls out:

* **Out-of-core**: Group-by (meaning: it’s one of the “big three” the streaming engine is designed to handle) ([GitHub][1])
* **Streaming nodes**: Group-by (with a single-key optimization) and **Sorted Group By**. ([GitHub][1])
* **`LazyFrame.group_by_dynamic`** has streaming-native support in specific cases (notably `group_by=None` and when the `group_by` column is sorted). ([GitHub][1])

### 3.2 Aggregate inventory (what’s tracked)

The tracking issue enumerates aggregates that are being supported natively/streaming: sum, mean, min/max, last/first, var/std, count, nunique (normal & grouped), implode, median/quantile, mode, approx nunique, arg_min/arg_max/arg_unique, and any/all. ([GitHub][1])

### 3.3 Hard streaming blocker knob: `maintain_order=True`

`group_by(..., maintain_order=True)` is explicitly documented as blocking streaming. ([Polars User Guide][3])

**Rewrite:** replace `maintain_order=True` with an explicit `sort(keys...)` after aggregation if you need deterministic group output ordering.

### 3.4 Memory-intensity reasoning (how to triage)

Even when an aggregate is “supported,” state size differs:

* **Typically O(1) state per group**: `count/sum/min/max/first/last/mean/var/std/any/all` (streaming-friendly).
* **Typically “heavier” state per group**: `n_unique`, `mode`, `median/quantile`, `implode` (these generally require keeping more information per group: sets, frequency maps, order statistics, or lists). The tracking issue explicitly lists these aggregates, so you should expect these nodes to show up as more memory-intensive in the streaming physical graph and tune accordingly. ([GitHub][1])
* Prefer **ApproxNUnique** when exact cardinality is not required (it is explicitly tracked as an aggregate). ([GitHub][1])

### 3.5 Streaming-friendly rewrites

* **Pre-aggregate before join** (reduce cardinality on the build side).
* Prefer **sorted group-by** when the grouping key is naturally sorted (time-series, partitioned scans): mark sortedness (see §6) to unlock Sorted Group By. ([GitHub][1])
* For heavy aggregates (mode/quantile/nunique), consider:

  * two-phase aggregation (local partials → merge),
  * bucketing/compression before group-by (e.g., dictionary encode categoricals, downcast numeric),
  * or moving them to a “fence stage” where you accept more memory.

---

## 4) Join family (equi/cross/asof/inequality/semi/anti + sorted equality join)

### 4.1 Streaming engine targets

The tracking issue lists streaming nodes for:

* **Equi-join** (with single-key optimization)
* Cross-join
* ASOF / Inequality join
* Anti/semi join
* **Merge / Sorted Equality Join**
* Merge-Sorted node ([GitHub][1])

It also calls out **out-of-core Equi-join** as a primary target. ([GitHub][1])

### 4.2 Known “streaming blocker” option: join cardinality validation

The `validate=` join checks (“1:1”, “1:m”, “m:1”, etc.) are explicitly documented as **not supported by the streaming engine**. ([Polars User Guide][4])

**Rewrite:** if you need validation, compute it as a separate step (e.g., group-by key counts) and keep the actual join streaming-native.

### 4.3 Rewrite patterns (memory + streaming)

**(A) Replace “join + project” with semi/anti joins when you only need filtering**
Semi/anti joins are explicit streaming nodes. ([GitHub][1])

**(B) Pre-aggregate or pre-dedup the build side**
If you can reduce build-side rows to 1 row per key (or a small number), you reduce join state dramatically.

**(C) Prefer sorted equality join when feasible**
If both sides are sorted by the join key, Polars can use sorted/merge-style join paths (tracked as “Merge / Sorted Equality Join” + “Merge-Sorted” nodes). ([GitHub][1])

Practical knobs:

* **Mark sortedness** only if true:

  * `Expr.set_sorted()` flags an expression as sorted and “enables downstream code to use fast paths,” with an explicit warning that incorrect flags can yield incorrect results. ([Polars User Guide][5])
* Or use `merge_sorted` when schemas match and you’re merging two sorted frames by key:

  * `DataFrame.merge_sorted(other, key)` requires both sides sorted and yields sorted output. ([Polars User Guide][6])

---

## 5) Sort / top-k / slices (ordering family)

### 5.1 Streaming engine targets

* **Out-of-core Sort** is a first-class target. ([GitHub][1])
* Plan translation includes `top_k/bottom_k` and “sorts in selections”. ([GitHub][1])
* Streaming nodes include slices (`head`/`tail`). ([GitHub][1])

### 5.2 Hard streaming blocker knob: stable-tie ordering

`LazyFrame.sort(..., maintain_order=True)` explicitly states: if true, **streaming is not possible** (stable tie ordering requires a stable sort). ([Polars User Guide][7])

**Rewrite:** avoid stable ties by making ties impossible:

* add a deterministic tie-breaker to your sort key (e.g., `(sort_key, stable_id)`),
* keep `maintain_order=False`,
* then apply `top_k/bottom_k` when you only need the top-N. ([GitHub][1])

### 5.3 Memory rewrite: prefer `top_k/bottom_k` over full sort + head

Since `top_k/bottom_k` is explicitly tracked for streaming plan translation, using it encourages a plan that avoids a full global sort. ([GitHub][1])

---

## 6) Window family (`.over()`, window-like translations)

### 6.1 Core lowering rule: `.over()` → group-by + join

The tracking issue explicitly lists: “`.over()` to group-by + join” as a plan translation. ([GitHub][1])
That matches the `Expr.over` doc: it’s similar to performing a group-by aggregation and joining the result back. ([Polars User Guide][8])

**Rewrite lever:** if a window is expensive or falls back, rewrite it explicitly as:

1. `lf.group_by(keys).agg(...)` producing one row per group, then
2. join back to the original (often a left join on keys).

This gives you control over:

* which aggregates are used (prefer streaming-friendly ones),
* whether you can enable sorted group-by paths,
* whether join validation is avoided.

### 6.2 Sorted `.over(keys)` special-case

The tracking issue also lists: “`.over(keys)` where keys are sorted → `sorted-group-by + explode`.” ([GitHub][1])

**Interpretation for rewrites:** if you need a window pattern that naturally “expands” group results, keeping partition keys sorted can unlock a streaming-native sorted-group-by path (often cheaper than a generic hash grouping). Your concrete control is to ensure sortedness (and mark it) where correct. ([GitHub][1])

---

## 7) Time-series / rolling / dynamic windows (temporal family)

### 7.1 Streaming engine targets

The tracking issue lists streaming support for:

* `{Expr, LazyFrame}.rolling`
* rolling functions like `rolling_sum/std/var/...`
* `LazyFrame.group_by_dynamic` (notably `group_by=None` and sorted group-by column cases)
* `ewm_{mean,std,var}` and `ewm_*_by` via plan translation ([GitHub][1])

### 7.2 Rewrite rules

* Treat time column sortedness as a first-class contract (many temporal groupings benefit from sortedness; the engine tracks sorted-group-by and group_by_dynamic sorted variants). ([GitHub][1])
* Prefer rolling/dynamic APIs rather than manual “self-join on time range” patterns; they have explicit streaming nodes/translations. ([GitHub][1])

---

## 8) Distinctness / dedup / counts (set-like family)

### 8.1 Streaming translations in this family

Plan translation list includes:

* `unique`
* `is_unique`, `is_duplicated`
* `is_first_distinct` / `is_last_distinct`
* `unique_counts` / `value_counts`
* `arg_unique` (also appears in aggregates list) ([GitHub][1])

### 8.2 Hard streaming blocker knobs: order-preserving dedup

* `unique(..., maintain_order=True)` blocks streaming. ([Polars User Guide][9])
* `keep="any"` explicitly provides **no guarantee** which row is kept (more optimizable, but not deterministic). ([Polars User Guide][10])

**Rewrite for deterministic dedup without blocking streaming:**

1. Pre-sort by deterministic priority + tie-breakers,
2. `unique(keep="first"|"last", maintain_order=False)`,
3. re-sort output if needed.

---

## 9) “Elementwise + cumulative + misc” plan translations (the “express it this way” list)

The tracking issue’s plan translation section is effectively a cheat sheet of **operations you should prefer** (instead of custom Python transforms) if you want them to lower into streaming nodes:

* elementwise/selection: literal series, aggregates/sorts/filters in selections, `drop_nans/drop_nulls`, `reshape` ([GitHub][1])
* row shifts/diffs: `shift`, `diff`, `pct_change` ([GitHub][1])
* cumulative: `cum_count/sum/prod/min/max` ([GitHub][1])
* indexing: `arg_where`, `arg_unique`, `index_of`, `gather_every` ([GitHub][1])
* binning/segmentation: `cut/qcut`, `rle/rle_id`, `peak_min/peak_max` ([GitHub][1])
* sampling/repetition: `sample`, `repeat`, `extend_constant` ([GitHub][1])
* replacement: `.replace()` lowered to **map** for small dicts or **join** for large dicts ([GitHub][1])
* filling/interp: `fill_null(strategy="min"/"max"/"mean")`, `forward_fill`, `backward_fill`, `interpolate/interpolate_by` ([GitHub][1])
* ewm: `ewm_{mean,std,var}` and `_by` variants ([GitHub][1])

**Rewrite rule:** if you find yourself writing a Python UDF for one of these categories, first try to express it with the built-in operation—the streaming planner explicitly tracks them for native lowering. ([GitHub][1])

---

## 10) The “explicit blockers” list (things that predictably disable streaming)

These are *documented* hard blockers (not “maybe” blockers):

* `group_by(..., maintain_order=True)` blocks streaming. ([Polars User Guide][3])
* `unique(..., maintain_order=True)` blocks streaming. ([Polars User Guide][9])
* `sort(..., maintain_order=True)` makes streaming impossible. ([Polars User Guide][7])
* `join(validate=...)` cardinality checks are not supported by streaming. ([Polars User Guide][4])

Everything else is “inspect the physical plan”: unsupported operations may simply fall back. ([Polars User Guide][2])

---

## 11) Rewrite playbook (streamable by default, deterministic by fences)

**Default posture:** design the bulk of the pipeline as *orderless streaming*, then add deterministic constraints only at explicit fence points.

1. **Scan** via streaming-supported sources (`scan_*`) and immediately apply **projection + filter**. ([GitHub][1])
2. **Favor streaming-native/translated operators** (lists above) over Python UDFs. ([GitHub][1])
3. **Avoid maintain-order knobs** in the core path; instead, **sort explicitly** at the end (with tie-breakers) if you need stable output. ([Polars User Guide][7])
4. **Prefer sorted fast paths** when your data is naturally sorted:

   * mark sortedness with `set_sorted` (carefully) to unlock fast paths / sorted nodes. ([Polars User Guide][5])
5. When you hit a true blocker or need determinism/validation, insert a **sink fence** (`sink_parquet/csv/ipc/...`) rather than collecting to RAM, then resume with another scan from the staged output. ([GitHub][1])
6. Always validate with the **streaming physical graph** and its memory legend. ([Polars User Guide][2])

---

If you want the next increment: I can turn this catalog into a **one-page “rewrite decision table”** (symptom → likely non-streaming node → rewrite → “what to look for in the physical graph”), plus a small helper utility that snapshots the streaming physical DOT and diffs it across PRs.

[1]: https://github.com/pola-rs/polars/issues/20947 "Tracking issue for the new streaming engine · Issue #20947 · pola-rs/polars · GitHub"
[2]: https://docs.pola.rs/user-guide/concepts/streaming/ "Streaming - Polars user guide"
[3]: https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.group_by.html?utm_source=chatgpt.com "polars.DataFrame.group_by — Polars documentation"
[4]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.join.html?utm_source=chatgpt.com "polars.DataFrame.join — Polars documentation"
[5]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.set_sorted.html?utm_source=chatgpt.com "polars.Expr.set_sorted — Polars documentation"
[6]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.merge_sorted.html?utm_source=chatgpt.com "polars.DataFrame.merge_sorted — Polars documentation"
[7]: https://docs.pola.rs/py-polars/html/reference/lazyframe/api/polars.LazyFrame.sort.html "polars.LazyFrame.sort — Polars  documentation"
[8]: https://docs.pola.rs/api/python/dev/reference/expressions/api/polars.Expr.over.html?utm_source=chatgpt.com "polars.Expr.over — Polars documentation"
[9]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.unique.html?utm_source=chatgpt.com "polars.DataFrame.unique — Polars documentation"
[10]: https://docs.pola.rs/py-polars/html/reference/dataframe/api/polars.DataFrame.unique.html "polars.DataFrame.unique — Polars  documentation"

