**datafusion_functions > unicode**

# Module: unicode

## Contents

**Modules**

- [`character_length`](#character_length)
- [`expr_fn`](#expr_fn)
- [`find_in_set`](#find_in_set)
- [`initcap`](#initcap)
- [`left`](#left)
- [`lpad`](#lpad)
- [`planner`](#planner) - SQL planning extensions like [`UnicodeFunctionPlanner`]
- [`reverse`](#reverse)
- [`right`](#right)
- [`rpad`](#rpad)
- [`strpos`](#strpos)
- [`substr`](#substr)
- [`substrindex`](#substrindex)
- [`translate`](#translate)

**Functions**

- [`character_length`](#character_length) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of character_length
- [`find_in_set`](#find_in_set) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of find_in_set
- [`functions`](#functions) - Returns all DataFusion functions defined in this package
- [`initcap`](#initcap) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of initcap
- [`left`](#left) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of left
- [`lpad`](#lpad) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of lpad
- [`reverse`](#reverse) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of reverse
- [`right`](#right) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of right
- [`rpad`](#rpad) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of rpad
- [`strpos`](#strpos) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of strpos
- [`substr`](#substr) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of substr
- [`substr_index`](#substr_index) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of substr_index
- [`substring`](#substring) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of substring
- [`translate`](#translate) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of translate

---

## datafusion_functions::unicode::character_length

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of character_length

```rust
fn character_length() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: character_length



## Module: expr_fn



## Module: find_in_set



## datafusion_functions::unicode::find_in_set

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of find_in_set

```rust
fn find_in_set() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::unicode::functions

*Function*

Returns all DataFusion functions defined in this package

```rust
fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## Module: initcap



## datafusion_functions::unicode::initcap

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of initcap

```rust
fn initcap() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::unicode::left

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of left

```rust
fn left() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: left



## Module: lpad



## datafusion_functions::unicode::lpad

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of lpad

```rust
fn lpad() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: planner

SQL planning extensions like [`UnicodeFunctionPlanner`]



## datafusion_functions::unicode::reverse

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of reverse

```rust
fn reverse() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: reverse



## datafusion_functions::unicode::right

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of right

```rust
fn right() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: right



## datafusion_functions::unicode::rpad

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of rpad

```rust
fn rpad() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: rpad



## datafusion_functions::unicode::strpos

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of strpos

```rust
fn strpos() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: strpos



## datafusion_functions::unicode::substr

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of substr

```rust
fn substr() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: substr



## datafusion_functions::unicode::substr_index

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of substr_index

```rust
fn substr_index() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: substrindex



## datafusion_functions::unicode::substring

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of substring

```rust
fn substring() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: translate



## datafusion_functions::unicode::translate

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of translate

```rust
fn translate() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



