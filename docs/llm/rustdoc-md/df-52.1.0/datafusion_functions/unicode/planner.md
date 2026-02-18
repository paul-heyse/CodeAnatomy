**datafusion_functions > unicode > planner**

# Module: unicode::planner

## Contents

**Structs**

- [`UnicodeFunctionPlanner`](#unicodefunctionplanner)

---

## datafusion_functions::unicode::planner::UnicodeFunctionPlanner

*Struct*

**Unit Struct**

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ExprPlanner**
  - `fn plan_position(self: &Self, args: Vec<Expr>) -> datafusion_common::Result<PlannerResult<Vec<Expr>>>`
  - `fn plan_substring(self: &Self, args: Vec<Expr>) -> datafusion_common::Result<PlannerResult<Vec<Expr>>>`
- **Default**
  - `fn default() -> UnicodeFunctionPlanner`



