**deltalake_core > delta_datafusion > planner**

# Module: delta_datafusion::planner

## Contents

**Structs**

- [`DeltaExtensionPlanner`](#deltaextensionplanner)
- [`DeltaPlanner`](#deltaplanner) - Deltaplanner

---

## deltalake_core::delta_datafusion::planner::DeltaExtensionPlanner

*Struct*

**Unit Struct**

**Methods:**

- `fn new() -> Arc<Self>`

**Trait Implementations:**

- **ExtensionPlanner**
  - `fn plan_extension(self: &'life0 Self, planner: &'life1 dyn PhysicalPlanner, node: &'life2 dyn UserDefinedLogicalNode, logical_inputs: &'life3 [&'life4 LogicalPlan], physical_inputs: &'life5 [Arc<dyn ExecutionPlan>], session_state: &'life6 SessionState) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`



## deltalake_core::delta_datafusion::planner::DeltaPlanner

*Struct*

Deltaplanner

**Unit Struct**

**Methods:**

- `fn new() -> Arc<Self>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **QueryPlanner**
  - `fn create_physical_plan(self: &'life0 Self, logical_plan: &'life1 LogicalPlan, session_state: &'life2 SessionState) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`



