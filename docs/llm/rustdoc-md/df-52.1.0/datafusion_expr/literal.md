**datafusion_expr > literal**

# Module: literal

## Contents

**Functions**

- [`lit`](#lit) - Create a literal expression
- [`lit_timestamp_nano`](#lit_timestamp_nano) - Create a literal timestamp expression
- [`lit_with_metadata`](#lit_with_metadata)

**Traits**

- [`Literal`](#literal) - Trait for converting a type to a [`Literal`] literal expression.
- [`TimestampLiteral`](#timestampliteral) - Trait for converting a type to a literal timestamp

---

## datafusion_expr::literal::Literal

*Trait*

Trait for converting a type to a [`Literal`] literal expression.

**Methods:**

- `lit`: convert the value to a Literal expression



## datafusion_expr::literal::TimestampLiteral

*Trait*

Trait for converting a type to a literal timestamp

**Methods:**

- `lit_timestamp_nano`



## datafusion_expr::literal::lit

*Function*

Create a literal expression

```rust
fn lit<T>(n: T) -> crate::Expr
```



## datafusion_expr::literal::lit_timestamp_nano

*Function*

Create a literal timestamp expression

```rust
fn lit_timestamp_nano<T>(n: T) -> crate::Expr
```



## datafusion_expr::literal::lit_with_metadata

*Function*

```rust
fn lit_with_metadata<T>(n: T, metadata: Option<datafusion_common::metadata::FieldMetadata>) -> crate::Expr
```



