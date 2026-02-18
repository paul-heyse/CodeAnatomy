**datafusion_functions > regex**

# Module: regex

## Contents

**Modules**

- [`expr_fn`](#expr_fn)
- [`regexpcount`](#regexpcount)
- [`regexpinstr`](#regexpinstr)
- [`regexplike`](#regexplike) - Regex expressions
- [`regexpmatch`](#regexpmatch) - Regex expressions
- [`regexpreplace`](#regexpreplace) - Regex expressions

**Functions**

- [`compile_and_cache_regex`](#compile_and_cache_regex)
- [`compile_regex`](#compile_regex)
- [`functions`](#functions) - Returns all DataFusion functions defined in this package
- [`regexp_count`](#regexp_count) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_count
- [`regexp_instr`](#regexp_instr) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_instr
- [`regexp_like`](#regexp_like) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_like
- [`regexp_match`](#regexp_match) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_match
- [`regexp_replace`](#regexp_replace) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_replace

---

## datafusion_functions::regex::compile_and_cache_regex

*Function*

```rust
fn compile_and_cache_regex<'strings, 'cache>(regex: &'strings str, flags: Option<&'strings str>, regex_cache: &'cache  mut std::collections::HashMap<(&'strings str, Option<&'strings str>), regex::Regex>) -> Result<&'cache regex::Regex, arrow::error::ArrowError>
```



## datafusion_functions::regex::compile_regex

*Function*

```rust
fn compile_regex(regex: &str, flags: Option<&str>) -> Result<regex::Regex, arrow::error::ArrowError>
```



## Module: expr_fn



## datafusion_functions::regex::functions

*Function*

Returns all DataFusion functions defined in this package

```rust
fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## datafusion_functions::regex::regexp_count

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_count

```rust
fn regexp_count() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::regex::regexp_instr

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_instr

```rust
fn regexp_instr() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::regex::regexp_like

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_like

```rust
fn regexp_like() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::regex::regexp_match

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_match

```rust
fn regexp_match() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::regex::regexp_replace

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of regexp_replace

```rust
fn regexp_replace() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: regexpcount



## Module: regexpinstr



## Module: regexplike

Regex expressions



## Module: regexpmatch

Regex expressions



## Module: regexpreplace

Regex expressions



