**datafusion_common > display**

# Module: display

## Contents

**Modules**

- [`human_readable`](#human_readable) - Helpers for rendering sizes, counts, and durations in human readable form.

**Structs**

- [`StringifiedPlan`](#stringifiedplan) - Represents some sort of execution plan, in String form

**Enums**

- [`PlanType`](#plantype) - Represents which type of plan, when storing multiple

**Traits**

- [`ToStringifiedPlan`](#tostringifiedplan) - Trait for something that can be formatted as a stringified plan

---

## datafusion_common::display::PlanType

*Enum*

Represents which type of plan, when storing multiple
for use in EXPLAIN plans

**Variants:**
- `InitialLogicalPlan` - The initial LogicalPlan provided to DataFusion
- `AnalyzedLogicalPlan{ analyzer_name: String }` - The LogicalPlan which results from applying an analyzer pass
- `FinalAnalyzedLogicalPlan` - The LogicalPlan after all analyzer passes have been applied
- `OptimizedLogicalPlan{ optimizer_name: String }` - The LogicalPlan which results from applying an optimizer pass
- `FinalLogicalPlan` - The final, fully optimized LogicalPlan that was converted to a physical plan
- `InitialPhysicalPlan` - The initial physical plan, prepared for execution
- `InitialPhysicalPlanWithStats` - The initial physical plan with stats, prepared for execution
- `InitialPhysicalPlanWithSchema` - The initial physical plan with schema, prepared for execution
- `OptimizedPhysicalPlan{ optimizer_name: String }` - The ExecutionPlan which results from applying an optimizer pass
- `FinalPhysicalPlan` - The final, fully optimized physical plan which would be executed
- `FinalPhysicalPlanWithStats` - The final with stats, fully optimized physical plan which would be executed
- `FinalPhysicalPlanWithSchema` - The final with schema, fully optimized physical plan which would be executed
- `PhysicalPlanError` - An error creating the physical plan

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &PlanType) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> PlanType`
- **PartialEq**
  - `fn eq(self: &Self, other: &PlanType) -> bool`



## datafusion_common::display::StringifiedPlan

*Struct*

Represents some sort of execution plan, in String form

**Fields:**
- `plan_type: PlanType` - An identifier of what type of plan this string represents
- `plan: std::sync::Arc<String>` - The string representation of the plan

**Methods:**

- `fn new<impl Into<String>>(plan_type: PlanType, plan: impl Trait) -> Self` - Create a new Stringified plan of `plan_type` with string
- `fn should_display(self: &Self, verbose_mode: bool) -> bool` - Returns true if this plan should be displayed. Generally

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> StringifiedPlan`
- **PartialEq**
  - `fn eq(self: &Self, other: &StringifiedPlan) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &StringifiedPlan) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::display::ToStringifiedPlan

*Trait*

Trait for something that can be formatted as a stringified plan

**Methods:**

- `to_stringified`: Create a stringified plan with the specified type



## Module: human_readable

Helpers for rendering sizes, counts, and durations in human readable form.



