**datafusion_functions > planner**

# Module: planner

## Contents

**Structs**

- [`UserDefinedFunctionPlanner`](#userdefinedfunctionplanner)

---

## datafusion_functions::planner::UserDefinedFunctionPlanner

*Struct*

**Unit Struct**

**Trait Implementations:**

- **Default**
  - `fn default() -> UserDefinedFunctionPlanner`
- **ExprPlanner**
  - `fn plan_extract(self: &Self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>>`
  - `fn plan_position(self: &Self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>>`
  - `fn plan_substring(self: &Self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



