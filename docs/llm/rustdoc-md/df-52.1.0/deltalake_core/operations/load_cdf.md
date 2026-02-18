**deltalake_core > operations > load_cdf**

# Module: operations::load_cdf

## Contents

**Structs**

- [`CdfLoadBuilder`](#cdfloadbuilder) - Builder for create a read of change data feeds for delta tables

---

## deltalake_core::operations::load_cdf::CdfLoadBuilder

*Struct*

Builder for create a read of change data feeds for delta tables

**Methods:**

- `fn with_starting_version(self: Self, starting_version: i64) -> Self` - Version to start at (version 0 if not provided)
- `fn with_ending_version(self: Self, ending_version: i64) -> Self` - Version (inclusive) to end at
- `fn with_ending_timestamp(self: Self, timestamp: DateTime<Utc>) -> Self` - Timestamp (inclusive) to end at
- `fn with_starting_timestamp(self: Self, timestamp: DateTime<Utc>) -> Self` - Timestamp to start from
- `fn with_allow_out_of_range(self: Self) -> Self` - Enable ending version or timestamp exceeding the last commit
- `fn with_session_state(self: Self, session: Arc<dyn Session>) -> Self` - The Datafusion session state to use
- `fn build(self: &Self, session: &dyn Session, filters: Option<&Arc<dyn PhysicalExpr>>) -> DeltaResult<Arc<dyn ExecutionPlan>>` - Executes the scan

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> CdfLoadBuilder`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



