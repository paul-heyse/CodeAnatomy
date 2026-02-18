**datafusion_expr > expr_rewriter > guarantees**

# Module: expr_rewriter::guarantees

## Contents

**Structs**

- [`GuaranteeRewriter`](#guaranteerewriter) - Rewrite expressions to incorporate guarantees.

**Functions**

- [`rewrite_with_guarantees`](#rewrite_with_guarantees) - Rewrite expressions to incorporate guarantees.
- [`rewrite_with_guarantees_map`](#rewrite_with_guarantees_map) - Rewrite expressions to incorporate guarantees.

---

## datafusion_expr::expr_rewriter::guarantees::GuaranteeRewriter

*Struct*

Rewrite expressions to incorporate guarantees.

See [`rewrite_with_guarantees`] for more information

**Generic Parameters:**
- 'a

**Methods:**

- `fn new<impl IntoIterator<Item = &'a (Expr, NullableInterval)>>(guarantees: impl Trait) -> Self`

**Trait Implementations:**

- **TreeNodeRewriter**
  - `fn f_up(self: & mut Self, expr: Expr) -> Result<Transformed<Expr>>`



## datafusion_expr::expr_rewriter::guarantees::rewrite_with_guarantees

*Function*

Rewrite expressions to incorporate guarantees.

Guarantees are a mapping from an expression (which currently is always a
column reference) to a [NullableInterval] that represents the known possible
values of the expression.

Rewriting expressions using this type of guarantee can make the work of other expression
simplifications, like const evaluation, easier.

For example, if we know that a column is not null and has values in the
range [1, 10), we can rewrite `x IS NULL` to `false` or `x < 10` to `true`.

If the set of guarantees will be used to rewrite more than one expression, consider using
[rewrite_with_guarantees_map] instead.

A full example of using this rewrite rule can be found in
[`ExprSimplifier::with_guarantees()`](https://docs.rs/datafusion/latest/datafusion/optimizer/simplify_expressions/struct.ExprSimplifier.html#method.with_guarantees).

```rust
fn rewrite_with_guarantees<'a, impl IntoIterator<Item = &'a (Expr, NullableInterval)>>(expr: crate::Expr, guarantees: impl Trait) -> datafusion_common::Result<datafusion_common::tree_node::Transformed<crate::Expr>>
```



## datafusion_expr::expr_rewriter::guarantees::rewrite_with_guarantees_map

*Function*

Rewrite expressions to incorporate guarantees.

Guarantees are a mapping from an expression (which currently is always a
column reference) to a [NullableInterval]. The interval represents the known
possible values of the column.

For example, if we know that a column is not null and has values in the
range [1, 10), we can rewrite `x IS NULL` to `false` or `x < 10` to `true`.

```rust
fn rewrite_with_guarantees_map<'a>(expr: crate::Expr, guarantees: &'a datafusion_common::HashMap<&'a crate::Expr, &'a datafusion_expr_common::interval_arithmetic::NullableInterval>) -> datafusion_common::Result<datafusion_common::tree_node::Transformed<crate::Expr>>
```



