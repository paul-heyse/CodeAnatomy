**datafusion_expr > simplify**

# Module: simplify

## Contents

**Structs**

- [`SimplifyContext`](#simplifycontext) - Provides simplification information based on DFSchema and

**Enums**

- [`ExprSimplifyResult`](#exprsimplifyresult) - Was the expression simplified?

**Traits**

- [`SimplifyInfo`](#simplifyinfo) - Provides the information necessary to apply algebraic simplification to an

---

## datafusion_expr::simplify::ExprSimplifyResult

*Enum*

Was the expression simplified?

**Variants:**
- `Simplified(crate::Expr)` - The function call was simplified to an entirely new Expr
- `Original(Vec<crate::Expr>)` - The function call could not be simplified, and the arguments

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::simplify::SimplifyContext

*Struct*

Provides simplification information based on DFSchema and
[`ExecutionProps`]. This is the default implementation used by DataFusion

# Example
See the `simplify_demo` in the [`expr_api` example]

[`expr_api` example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/query_planning/expr_api.rs

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(props: &'a ExecutionProps) -> Self` - Create a new SimplifyContext
- `fn with_schema(self: Self, schema: DFSchemaRef) -> Self` - Register a [`DFSchemaRef`] with this context

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **SimplifyInfo**
  - `fn is_boolean_type(self: &Self, expr: &Expr) -> Result<bool>` - Returns true if this Expr has boolean type
  - `fn nullable(self: &Self, expr: &Expr) -> Result<bool>` - Returns true if expr is nullable
  - `fn get_data_type(self: &Self, expr: &Expr) -> Result<DataType>` - Returns data type of this expr needed for determining optimized int type of a value
  - `fn execution_props(self: &Self) -> &ExecutionProps`
- **Clone**
  - `fn clone(self: &Self) -> SimplifyContext<'a>`



## datafusion_expr::simplify::SimplifyInfo

*Trait*

Provides the information necessary to apply algebraic simplification to an
[Expr]. See [SimplifyContext] for one concrete implementation.

This trait exists so that other systems can plug schema
information in without having to create `DFSchema` objects. If you
have a [`DFSchemaRef`] you can use [`SimplifyContext`]

**Methods:**

- `is_boolean_type`: Returns true if this Expr has boolean type
- `nullable`: Returns true of this expr is nullable (could possibly be NULL)
- `execution_props`: Returns details needed for partial expression evaluation
- `get_data_type`: Returns data type of this expr needed for determining optimized int type of a value



