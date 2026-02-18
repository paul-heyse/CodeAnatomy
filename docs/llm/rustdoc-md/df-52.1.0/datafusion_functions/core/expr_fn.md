**datafusion_functions > core > expr_fn**

# Module: core::expr_fn

## Contents

**Functions**

- [`arrow_cast`](#arrow_cast) - Returns value2 if value1 is NULL; otherwise it returns value1
- [`arrow_metadata`](#arrow_metadata) - Returns the metadata of the input expression
- [`arrow_typeof`](#arrow_typeof) - Returns the Arrow type of the input expression.
- [`coalesce`](#coalesce) - Returns `coalesce(args...)`, which evaluates to the value of the first expr which is not NULL
- [`get_field`](#get_field) - Returns the value of the field with the given name from the struct
- [`get_field_path`](#get_field_path) - Returns the value of nested fields by traversing multiple field names
- [`greatest`](#greatest) - Returns `greatest(args...)`, which evaluates to the greatest value in the list of expressions or NULL if all the expressions are NULL
- [`least`](#least) - Returns `least(args...)`, which evaluates to the smallest value in the list of expressions or NULL if all the expressions are NULL
- [`named_struct`](#named_struct) - Returns a struct with the given names and arguments pairs
- [`nullif`](#nullif) - Returns NULL if value1 equals value2; otherwise it returns value1. This can be used to perform the inverse operation of the COALESCE expression
- [`nvl`](#nvl) - Returns value2 if value1 is NULL; otherwise it returns value1
- [`nvl2`](#nvl2) - Returns value2 if value1 is not NULL; otherwise, it returns value3.
- [`overlay`](#overlay) - replace the substring of string that starts at the start'th character and extends for count characters with new substring
- [`struct`](#struct) - Returns a struct with the given arguments
- [`union_extract`](#union_extract) - Returns the value of the field with the given name from the union when it's selected, or NULL otherwise
- [`union_tag`](#union_tag) - Returns the name of the currently selected field in the union

---

## datafusion_functions::core::expr_fn::arrow_cast

*Function*

Returns value2 if value1 is NULL; otherwise it returns value1

```rust
fn arrow_cast(arg1: datafusion_expr::Expr, arg2: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::arrow_metadata

*Function*

Returns the metadata of the input expression

```rust
fn arrow_metadata(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::arrow_typeof

*Function*

Returns the Arrow type of the input expression.

```rust
fn arrow_typeof(arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::coalesce

*Function*

Returns `coalesce(args...)`, which evaluates to the value of the first expr which is not NULL

```rust
fn coalesce(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::get_field

*Function*

Returns the value of the field with the given name from the struct

```rust
fn get_field<impl Literal>(arg1: datafusion_expr::Expr, arg2: impl Trait) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::get_field_path

*Function*

Returns the value of nested fields by traversing multiple field names

```rust
fn get_field_path(base: datafusion_expr::Expr, field_names: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::greatest

*Function*

Returns `greatest(args...)`, which evaluates to the greatest value in the list of expressions or NULL if all the expressions are NULL

```rust
fn greatest(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::least

*Function*

Returns `least(args...)`, which evaluates to the smallest value in the list of expressions or NULL if all the expressions are NULL

```rust
fn least(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::named_struct

*Function*

Returns a struct with the given names and arguments pairs

```rust
fn named_struct(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::nullif

*Function*

Returns NULL if value1 equals value2; otherwise it returns value1. This can be used to perform the inverse operation of the COALESCE expression

```rust
fn nullif(arg1: datafusion_expr::Expr, arg2: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::nvl

*Function*

Returns value2 if value1 is NULL; otherwise it returns value1

```rust
fn nvl(arg1: datafusion_expr::Expr, arg2: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::nvl2

*Function*

Returns value2 if value1 is not NULL; otherwise, it returns value3.

```rust
fn nvl2(arg1: datafusion_expr::Expr, arg2: datafusion_expr::Expr, arg3: datafusion_expr::Expr) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::overlay

*Function*

replace the substring of string that starts at the start'th character and extends for count characters with new substring

```rust
fn overlay(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::struct

*Function*

Returns a struct with the given arguments

```rust
fn struct(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::union_extract

*Function*

Returns the value of the field with the given name from the union when it's selected, or NULL otherwise

```rust
fn union_extract<impl Literal>(arg1: datafusion_expr::Expr, arg2: impl Trait) -> datafusion_expr::Expr
```



## datafusion_functions::core::expr_fn::union_tag

*Function*

Returns the name of the currently selected field in the union

```rust
fn union_tag(arg1: datafusion_expr::Expr) -> datafusion_expr::Expr
```



