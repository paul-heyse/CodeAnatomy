**datafusion_functions_nested > planner**

# Module: planner

## Contents

**Structs**

- [`FieldAccessPlanner`](#fieldaccessplanner)
- [`NestedFunctionPlanner`](#nestedfunctionplanner)

---

## datafusion_functions_nested::planner::FieldAccessPlanner

*Struct*

**Unit Struct**

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ExprPlanner**
  - `fn plan_field_access(self: &Self, expr: RawFieldAccessExpr, schema: &DFSchema) -> Result<PlannerResult<RawFieldAccessExpr>>`



## datafusion_functions_nested::planner::NestedFunctionPlanner

*Struct*

**Unit Struct**

**Trait Implementations:**

- **ExprPlanner**
  - `fn plan_binary_op(self: &Self, expr: RawBinaryExpr, schema: &DFSchema) -> Result<PlannerResult<RawBinaryExpr>>`
  - `fn plan_array_literal(self: &Self, exprs: Vec<Expr>, _schema: &DFSchema) -> Result<PlannerResult<Vec<Expr>>>`
  - `fn plan_make_map(self: &Self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>>`
  - `fn plan_any(self: &Self, expr: RawBinaryExpr) -> Result<PlannerResult<RawBinaryExpr>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



