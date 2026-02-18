**datafusion_expr > logical_plan > invariants**

# Module: logical_plan::invariants

## Contents

**Enums**

- [`InvariantLevel`](#invariantlevel)

**Functions**

- [`assert_expected_schema`](#assert_expected_schema) - Returns an error if the plan does not have the expected schema.
- [`check_subquery_expr`](#check_subquery_expr) - Do necessary check on subquery expressions and fail the invalid plan

---

## datafusion_expr::logical_plan::invariants::InvariantLevel

*Enum*

**Variants:**
- `Always` - Invariants that are always true in DataFusion `LogicalPlan`s
- `Executable` - Invariants that must hold true for the plan to be "executable"

**Traits:** Copy, Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &InvariantLevel) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> InvariantLevel`
- **PartialEq**
  - `fn eq(self: &Self, other: &InvariantLevel) -> bool`



## datafusion_expr::logical_plan::invariants::assert_expected_schema

*Function*

Returns an error if the plan does not have the expected schema.
Ignores metadata and nullability.

```rust
fn assert_expected_schema(schema: &datafusion_common::DFSchemaRef, plan: &crate::LogicalPlan) -> datafusion_common::Result<()>
```



## datafusion_expr::logical_plan::invariants::check_subquery_expr

*Function*

Do necessary check on subquery expressions and fail the invalid plan
1) Check whether the outer plan is in the allowed outer plans list to use subquery expressions,
   the allowed while list: [Projection, Filter, Window, Aggregate, Join].
2) Check whether the inner plan is in the allowed inner plans list to use correlated(outer) expressions.
3) Check and validate unsupported cases to use the correlated(outer) expressions inside the subquery(inner) plans/inner expressions.
   For example, we do not want to support to use correlated expressions as the Join conditions in the subquery plan when the Join
   is a Full Out Join

```rust
fn check_subquery_expr(outer_plan: &crate::LogicalPlan, inner_plan: &crate::LogicalPlan, expr: &crate::Expr) -> datafusion_common::Result<()>
```



