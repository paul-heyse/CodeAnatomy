**datafusion_functions > core > planner**

# Module: core::planner

## Contents

**Structs**

- [`CoreFunctionPlanner`](#corefunctionplanner)

---

## datafusion_functions::core::planner::CoreFunctionPlanner

*Struct*

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> CoreFunctionPlanner`
- **ExprPlanner**
  - `fn plan_dictionary_literal(self: &Self, expr: RawDictionaryExpr, _schema: &DFSchema) -> Result<PlannerResult<RawDictionaryExpr>>`
  - `fn plan_struct_literal(self: &Self, args: Vec<Expr>, is_named_struct: bool) -> Result<PlannerResult<Vec<Expr>>>`
  - `fn plan_overlay(self: &Self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>>`
  - `fn plan_compound_identifier(self: &Self, field: &Field, qualifier: Option<&TableReference>, nested_names: &[String]) -> Result<PlannerResult<Vec<Expr>>>`



