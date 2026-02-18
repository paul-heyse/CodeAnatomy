**datafusion_tracing > rule_options**

# Module: rule_options

## Contents

**Structs**

- [`RuleInstrumentationOptions`](#ruleinstrumentationoptions) - Configuration options for instrumented DataFusion rules (Analyzer, Optimizer, Physical Optimizer).
- [`RuleInstrumentationOptionsBuilder`](#ruleinstrumentationoptionsbuilder) - The builder for `RuleInstrumentationOptions`.

---

## datafusion_tracing::rule_options::RuleInstrumentationOptions

*Struct*

Configuration options for instrumented DataFusion rules (Analyzer, Optimizer, Physical Optimizer).

**Methods:**

- `fn builder() -> RuleInstrumentationOptionsBuilder` - Creates a new builder for `RuleInstrumentationOptions`.
- `fn full() -> Self` - Creates options with all phases enabled at `Full` level.
- `fn phase_only() -> Self` - Creates options with all phases enabled at `PhaseOnly` level.
- `fn with_plan_diff(self: Self) -> Self` - Returns a new options with plan diff enabled.

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> RuleInstrumentationOptions`



## datafusion_tracing::rule_options::RuleInstrumentationOptionsBuilder

*Struct*

The builder for `RuleInstrumentationOptions`.



