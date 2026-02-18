**datafusion_expr > expr_rewriter**

# Module: expr_rewriter

## Contents

**Structs**

- [`NamePreserver`](#namepreserver) - Handles ensuring the name of rewritten expressions is not changed.

**Enums**

- [`SavedName`](#savedname) - If the qualified name of an expression is remembered, it will be preserved

**Functions**

- [`coerce_plan_expr_for_schema`](#coerce_plan_expr_for_schema) - Returns plan with expressions coerced to types compatible with
- [`create_col_from_scalar_expr`](#create_col_from_scalar_expr) - Create a Column from the Scalar Expr
- [`normalize_col`](#normalize_col) - Recursively call `LogicalPlanBuilder::normalize` on all [`Column`] expressions
- [`normalize_col_with_schemas_and_ambiguity_check`](#normalize_col_with_schemas_and_ambiguity_check) - See [`Column::normalize_with_schemas_and_ambiguity_check`] for usage
- [`normalize_cols`](#normalize_cols) - Recursively normalize all [`Column`] expressions in a list of expression trees
- [`normalize_sorts`](#normalize_sorts)
- [`replace_col`](#replace_col) - Recursively replace all [`Column`] expressions in a given expression tree with
- [`strip_outer_reference`](#strip_outer_reference) - Recursively remove all the ['OuterReferenceColumn'] and return the inside Column
- [`unalias`](#unalias) - Recursively un-alias an expressions
- [`unnormalize_col`](#unnormalize_col) - Recursively 'unnormalize' (remove all qualifiers) from an
- [`unnormalize_cols`](#unnormalize_cols) - Recursively un-normalize all [`Column`] expressions in a list of expression trees

**Traits**

- [`FunctionRewrite`](#functionrewrite) - Trait for rewriting [`Expr`]s into function calls.

---

## datafusion_expr::expr_rewriter::FunctionRewrite

*Trait*

Trait for rewriting [`Expr`]s into function calls.

This trait is used with `FunctionRegistry::register_function_rewrite` to
to evaluating `Expr`s using functions that may not be built in to DataFusion

For example, concatenating arrays `a || b` is represented as
`Operator::ArrowAt`, but can be implemented by calling a function
`array_concat` from the `functions-nested` crate.

**Methods:**

- `name`: Return a human readable name for this rewrite
- `rewrite`: Potentially rewrite `expr` to some other expression



## datafusion_expr::expr_rewriter::NamePreserver

*Struct*

Handles ensuring the name of rewritten expressions is not changed.

This is important when optimizing plans to ensure the output
schema of plan nodes don't change after optimization.
For example, if an expression `1 + 2` is rewritten to `3`, the name of the
expression should be preserved: `3 as "1 + 2"`

See <https://github.com/apache/datafusion/issues/3555> for details

**Methods:**

- `fn new(plan: &LogicalPlan) -> Self` - Create a new NamePreserver for rewriting the `expr` that is part of the specified plan
- `fn new_for_projection() -> Self` - Create a new NamePreserver for rewriting the `expr`s in `Projection`
- `fn save(self: &Self, expr: &Expr) -> SavedName`



## datafusion_expr::expr_rewriter::SavedName

*Enum*

If the qualified name of an expression is remembered, it will be preserved
when rewriting the expression

**Variants:**
- `Saved{ relation: Option<datafusion_common::TableReference>, name: String }` - Saved qualified name to be preserved
- `None` - Name is not preserved

**Methods:**

- `fn restore(self: Self, expr: Expr) -> Expr` - Ensures the qualified name of the rewritten expression is preserved

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::expr_rewriter::coerce_plan_expr_for_schema

*Function*

Returns plan with expressions coerced to types compatible with
schema types

```rust
fn coerce_plan_expr_for_schema(plan: crate::LogicalPlan, schema: &datafusion_common::DFSchema) -> datafusion_common::Result<crate::LogicalPlan>
```



## datafusion_expr::expr_rewriter::create_col_from_scalar_expr

*Function*

Create a Column from the Scalar Expr

```rust
fn create_col_from_scalar_expr(scalar_expr: &crate::Expr, subqry_alias: String) -> datafusion_common::Result<datafusion_common::Column>
```



## datafusion_expr::expr_rewriter::normalize_col

*Function*

Recursively call `LogicalPlanBuilder::normalize` on all [`Column`] expressions
in the `expr` expression tree.

```rust
fn normalize_col(expr: crate::Expr, plan: &crate::LogicalPlan) -> datafusion_common::Result<crate::Expr>
```



## datafusion_expr::expr_rewriter::normalize_col_with_schemas_and_ambiguity_check

*Function*

See [`Column::normalize_with_schemas_and_ambiguity_check`] for usage

```rust
fn normalize_col_with_schemas_and_ambiguity_check(expr: crate::Expr, schemas: &[&[&datafusion_common::DFSchema]], using_columns: &[std::collections::HashSet<datafusion_common::Column>]) -> datafusion_common::Result<crate::Expr>
```



## datafusion_expr::expr_rewriter::normalize_cols

*Function*

Recursively normalize all [`Column`] expressions in a list of expression trees

```rust
fn normalize_cols<impl Into<Expr>, impl IntoIterator<Item = impl Into<Expr>>>(exprs: impl Trait, plan: &crate::LogicalPlan) -> datafusion_common::Result<Vec<crate::Expr>>
```



## datafusion_expr::expr_rewriter::normalize_sorts

*Function*

```rust
fn normalize_sorts<impl Into<Sort>, impl IntoIterator<Item = impl Into<Sort>>>(sorts: impl Trait, plan: &crate::LogicalPlan) -> datafusion_common::Result<Vec<crate::expr::Sort>>
```



## datafusion_expr::expr_rewriter::replace_col

*Function*

Recursively replace all [`Column`] expressions in a given expression tree with
`Column` expressions provided by the hash map argument.

```rust
fn replace_col(expr: crate::Expr, replace_map: &std::collections::HashMap<&datafusion_common::Column, &datafusion_common::Column>) -> datafusion_common::Result<crate::Expr>
```



## datafusion_expr::expr_rewriter::strip_outer_reference

*Function*

Recursively remove all the ['OuterReferenceColumn'] and return the inside Column
in the expression tree.

```rust
fn strip_outer_reference(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_rewriter::unalias

*Function*

Recursively un-alias an expressions

```rust
fn unalias(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_rewriter::unnormalize_col

*Function*

Recursively 'unnormalize' (remove all qualifiers) from an
expression tree.

For example, if there were expressions like `foo.bar` this would
rewrite it to just `bar`.

```rust
fn unnormalize_col(expr: crate::Expr) -> crate::Expr
```



## datafusion_expr::expr_rewriter::unnormalize_cols

*Function*

Recursively un-normalize all [`Column`] expressions in a list of expression trees

```rust
fn unnormalize_cols<impl IntoIterator<Item = Expr>>(exprs: impl Trait) -> Vec<crate::Expr>
```



