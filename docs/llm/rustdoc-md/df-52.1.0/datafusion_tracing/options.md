**datafusion_tracing > options**

# Module: options

## Contents

**Structs**

- [`InstrumentationOptions`](#instrumentationoptions) - Configuration options for instrumented execution plans.
- [`InstrumentationOptionsBuilder`](#instrumentationoptionsbuilder) - The builder for `InstrumentationOptions`.

---

## datafusion_tracing::options::InstrumentationOptions

*Struct*

Configuration options for instrumented execution plans.

**Fields:**
- `record_metrics: bool` - Whether to record metrics during execution.
- `preview_limit: usize` - Maximum number of rows to preview per span.
- `preview_fn: Option<std::sync::Arc<dyn Fn>>` - Optional callback function for formatting previewed record batches.
- `custom_fields: std::collections::HashMap<String, String>` - User-defined custom fields for extensible configuration.

**Methods:**

- `fn builder() -> InstrumentationOptionsBuilder` - Creates a new builder for `InstrumentationOptions`.

**Trait Implementations:**

- **Default**
  - `fn default() -> InstrumentationOptions`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> InstrumentationOptions`



## datafusion_tracing::options::InstrumentationOptionsBuilder

*Struct*

The builder for `InstrumentationOptions`.



