**deltalake_core > delta_datafusion > session**

# Module: delta_datafusion::session

## Contents

**Structs**

- [`DeltaParserOptions`](#deltaparseroptions) - A wrapper for sql_parser's ParserOptions to capture sane default table defaults
- [`DeltaRuntimeEnvBuilder`](#deltaruntimeenvbuilder) - A builder for configuring DataFusion RuntimeEnv with Delta-specific defaults
- [`DeltaSessionConfig`](#deltasessionconfig) - A wrapper for Deltafusion's SessionConfig to capture sane default table defaults
- [`DeltaSessionContext`](#deltasessioncontext) - A wrapper for DataFusion's SessionContext with Delta-specific defaults

**Enums**

- [`SessionFallbackPolicy`](#sessionfallbackpolicy) - Controls how delta-rs resolves a caller-provided DataFusion `Session` into a `SessionState`.

**Functions**

- [`create_session`](#create_session)

---

## deltalake_core::delta_datafusion::session::DeltaParserOptions

*Struct*

A wrapper for sql_parser's ParserOptions to capture sane default table defaults

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## deltalake_core::delta_datafusion::session::DeltaRuntimeEnvBuilder

*Struct*

A builder for configuring DataFusion RuntimeEnv with Delta-specific defaults

**Methods:**

- `fn new() -> Self`
- `fn with_max_spill_size(self: Self, size: usize) -> Self`
- `fn with_max_temp_directory_size(self: Self, size: u64) -> Self`
- `fn build(self: Self) -> Arc<RuntimeEnv>`

**Trait Implementations:**

- **Default**
  - `fn default() -> DeltaRuntimeEnvBuilder`



## deltalake_core::delta_datafusion::session::DeltaSessionConfig

*Struct*

A wrapper for Deltafusion's SessionConfig to capture sane default table defaults

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## deltalake_core::delta_datafusion::session::DeltaSessionContext

*Struct*

A wrapper for DataFusion's SessionContext with Delta-specific defaults

This provides a way of creating DataFusion sessions with consistent
Delta Lake configuration (case-sensitive identifiers, Delta planner, etc.)

**Methods:**

- `fn new() -> Self` - Create a new DeltaSessionContext with default configuration
- `fn with_runtime_env(runtime_env: Arc<RuntimeEnv>) -> Self` - Create a DeltaSessionContext with a custom RuntimeEnv
- `fn into_inner(self: Self) -> SessionContext`
- `fn state(self: &Self) -> SessionState`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## deltalake_core::delta_datafusion::session::SessionFallbackPolicy

*Enum*

Controls how delta-rs resolves a caller-provided DataFusion `Session` into a `SessionState`.

This is an opt-in knob on operations that accept `with_session_state(...)`. Defaults to
`InternalDefaults` to preserve existing behavior.

**Variants:**
- `InternalDefaults` - If the provided session is not a `SessionState`, log a warning and use internal defaults.
- `DeriveFromTrait` - Derive a `SessionState` from the `Session` trait (runtime/config/UDF registries).
- `RequireSessionState` - Return an error if the provided session is not a `SessionState`.

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &SessionFallbackPolicy) -> bool`
- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> SessionFallbackPolicy`



## deltalake_core::delta_datafusion::session::create_session

*Function*

```rust
fn create_session() -> DeltaSessionContext
```



