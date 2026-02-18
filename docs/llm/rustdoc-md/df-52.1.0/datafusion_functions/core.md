**datafusion_functions > core**

# Module: core

## Contents

**Modules**

- [`arrow_cast`](#arrow_cast) - [`ArrowCastFunc`]: Implementation of the `arrow_cast`
- [`arrow_metadata`](#arrow_metadata)
- [`arrowtypeof`](#arrowtypeof)
- [`coalesce`](#coalesce)
- [`expr_ext`](#expr_ext) - Extension methods for Expr.
- [`expr_fn`](#expr_fn)
- [`getfield`](#getfield)
- [`greatest`](#greatest)
- [`least`](#least)
- [`named_struct`](#named_struct)
- [`nullif`](#nullif)
- [`nvl`](#nvl)
- [`nvl2`](#nvl2)
- [`overlay`](#overlay)
- [`planner`](#planner)
- [`struct`](#struct)
- [`union_extract`](#union_extract)
- [`union_tag`](#union_tag)
- [`version`](#version) - [`VersionFunc`]: Implementation of the `version` function.

**Functions**

- [`arrow_cast`](#arrow_cast) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of arrow_cast
- [`arrow_metadata`](#arrow_metadata) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of arrow_metadata
- [`arrow_typeof`](#arrow_typeof) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of arrow_typeof
- [`coalesce`](#coalesce) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of coalesce
- [`functions`](#functions) - Returns all DataFusion functions defined in this package
- [`get_field`](#get_field) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of get_field
- [`greatest`](#greatest) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of greatest
- [`least`](#least) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of least
- [`named_struct`](#named_struct) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of named_struct
- [`nullif`](#nullif) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of nullif
- [`nvl`](#nvl) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of nvl
- [`nvl2`](#nvl2) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of nvl2
- [`overlay`](#overlay) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of overlay
- [`struct`](#struct) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of r#struct
- [`union_extract`](#union_extract) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of union_extract
- [`union_tag`](#union_tag) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of union_tag
- [`version`](#version) - Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of version

---

## Module: arrow_cast

[`ArrowCastFunc`]: Implementation of the `arrow_cast`



## datafusion_functions::core::arrow_cast

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of arrow_cast

```rust
fn arrow_cast() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: arrow_metadata



## datafusion_functions::core::arrow_metadata

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of arrow_metadata

```rust
fn arrow_metadata() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::core::arrow_typeof

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of arrow_typeof

```rust
fn arrow_typeof() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: arrowtypeof



## Module: coalesce



## datafusion_functions::core::coalesce

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of coalesce

```rust
fn coalesce() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: expr_ext

Extension methods for Expr.



## Module: expr_fn



## datafusion_functions::core::functions

*Function*

Returns all DataFusion functions defined in this package

```rust
fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>>
```



## datafusion_functions::core::get_field

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of get_field

```rust
fn get_field() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: getfield



## Module: greatest



## datafusion_functions::core::greatest

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of greatest

```rust
fn greatest() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: least



## datafusion_functions::core::least

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of least

```rust
fn least() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: named_struct



## datafusion_functions::core::named_struct

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of named_struct

```rust
fn named_struct() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::core::nullif

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of nullif

```rust
fn nullif() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: nullif



## datafusion_functions::core::nvl

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of nvl

```rust
fn nvl() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: nvl



## Module: nvl2



## datafusion_functions::core::nvl2

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of nvl2

```rust
fn nvl2() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: overlay



## datafusion_functions::core::overlay

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of overlay

```rust
fn overlay() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: planner



## Module: struct



## datafusion_functions::core::struct

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of r#struct

```rust
fn struct() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: union_extract



## datafusion_functions::core::union_extract

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of union_extract

```rust
fn union_extract() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## datafusion_functions::core::union_tag

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of union_tag

```rust
fn union_tag() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: union_tag



## datafusion_functions::core::version

*Function*

Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of version

```rust
fn version() -> std::sync::Arc<datafusion_expr::ScalarUDF>
```



## Module: version

[`VersionFunc`]: Implementation of the `version` function.



