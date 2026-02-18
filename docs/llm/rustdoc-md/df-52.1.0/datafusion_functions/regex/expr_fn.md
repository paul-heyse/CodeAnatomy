**datafusion_functions > regex > expr_fn**

# Module: regex::expr_fn

## Contents

**Functions**

- [`regexp_count`](#regexp_count) - Returns the number of consecutive occurrences of a regular expression in a string.
- [`regexp_instr`](#regexp_instr) - Returns index of regular expression matches in a string.
- [`regexp_like`](#regexp_like) - Returns true if a regex has at least one match in a string, false otherwise.
- [`regexp_match`](#regexp_match) - Returns a list of regular expression matches in a string.
- [`regexp_replace`](#regexp_replace) - Replaces substrings in a string that match.

---

## datafusion_functions::regex::expr_fn::regexp_count

*Function*

Returns the number of consecutive occurrences of a regular expression in a string.

```rust
fn regexp_count(values: datafusion_expr::Expr, regex: datafusion_expr::Expr, start: Option<datafusion_expr::Expr>, flags: Option<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::regex::expr_fn::regexp_instr

*Function*

Returns index of regular expression matches in a string.

```rust
fn regexp_instr(values: datafusion_expr::Expr, regex: datafusion_expr::Expr, start: Option<datafusion_expr::Expr>, n: Option<datafusion_expr::Expr>, endoption: Option<datafusion_expr::Expr>, flags: Option<datafusion_expr::Expr>, subexpr: Option<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::regex::expr_fn::regexp_like

*Function*

Returns true if a regex has at least one match in a string, false otherwise.

```rust
fn regexp_like(values: datafusion_expr::Expr, regex: datafusion_expr::Expr, flags: Option<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::regex::expr_fn::regexp_match

*Function*

Returns a list of regular expression matches in a string.

```rust
fn regexp_match(values: datafusion_expr::Expr, regex: datafusion_expr::Expr, flags: Option<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::regex::expr_fn::regexp_replace

*Function*

Replaces substrings in a string that match.

```rust
fn regexp_replace(string: datafusion_expr::Expr, pattern: datafusion_expr::Expr, replacement: datafusion_expr::Expr, flags: Option<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



