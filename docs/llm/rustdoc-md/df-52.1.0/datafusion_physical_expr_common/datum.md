**datafusion_physical_expr_common > datum**

# Module: datum

## Contents

**Functions**

- [`apply`](#apply) - Applies a binary [`Datum`] kernel `f` to `lhs` and `rhs`
- [`apply_cmp`](#apply_cmp) - Applies a binary [`Datum`] comparison operator `op` to `lhs` and `rhs`
- [`apply_cmp_for_nested`](#apply_cmp_for_nested) - Applies a binary [`Datum`] comparison operator `op` to `lhs` and `rhs` for nested type like
- [`compare_op_for_nested`](#compare_op_for_nested) - Compare on nested type List, Struct, and so on
- [`compare_with_eq`](#compare_with_eq) - Compare with eq with either nested or non-nested

---

## datafusion_physical_expr_common::datum::apply

*Function*

Applies a binary [`Datum`] kernel `f` to `lhs` and `rhs`

This maps arrow-rs' [`Datum`] kernels to DataFusion's [`ColumnarValue`] abstraction

```rust
fn apply<impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>>(lhs: &datafusion_expr_common::columnar_value::ColumnarValue, rhs: &datafusion_expr_common::columnar_value::ColumnarValue, f: impl Trait) -> datafusion_common::Result<datafusion_expr_common::columnar_value::ColumnarValue>
```



## datafusion_physical_expr_common::datum::apply_cmp

*Function*

Applies a binary [`Datum`] comparison operator `op` to `lhs` and `rhs`

```rust
fn apply_cmp(op: datafusion_expr_common::operator::Operator, lhs: &datafusion_expr_common::columnar_value::ColumnarValue, rhs: &datafusion_expr_common::columnar_value::ColumnarValue) -> datafusion_common::Result<datafusion_expr_common::columnar_value::ColumnarValue>
```



## datafusion_physical_expr_common::datum::apply_cmp_for_nested

*Function*

Applies a binary [`Datum`] comparison operator `op` to `lhs` and `rhs` for nested type like
List, FixedSizeList, LargeList, Struct, Union, Map, or a dictionary of a nested type

```rust
fn apply_cmp_for_nested(op: datafusion_expr_common::operator::Operator, lhs: &datafusion_expr_common::columnar_value::ColumnarValue, rhs: &datafusion_expr_common::columnar_value::ColumnarValue) -> datafusion_common::Result<datafusion_expr_common::columnar_value::ColumnarValue>
```



## datafusion_physical_expr_common::datum::compare_op_for_nested

*Function*

Compare on nested type List, Struct, and so on

```rust
fn compare_op_for_nested(op: datafusion_expr_common::operator::Operator, lhs: &dyn Datum, rhs: &dyn Datum) -> datafusion_common::Result<arrow::array::BooleanArray>
```



## datafusion_physical_expr_common::datum::compare_with_eq

*Function*

Compare with eq with either nested or non-nested

```rust
fn compare_with_eq(lhs: &dyn Datum, rhs: &dyn Datum, is_nested: bool) -> datafusion_common::Result<arrow::array::BooleanArray>
```



