**datafusion_expr > var_provider**

# Module: var_provider

## Contents

**Enums**

- [`VarType`](#vartype) - Variable type, system/user defined

**Functions**

- [`is_system_variables`](#is_system_variables) - Returns true if the specified string is a "system" variable such as

**Traits**

- [`VarProvider`](#varprovider) - A var provider for `@variable` and `@@variable` runtime values.

---

## datafusion_expr::var_provider::VarProvider

*Trait*

A var provider for `@variable` and `@@variable` runtime values.

**Methods:**

- `get_value`: Get variable value
- `get_type`: Return the type of the given variable



## datafusion_expr::var_provider::VarType

*Enum*

Variable type, system/user defined

**Variants:**
- `System` - System variable, like @@version
- `UserDefined` - User defined variable, like @name

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> VarType`
- **PartialEq**
  - `fn eq(self: &Self, other: &VarType) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::var_provider::is_system_variables

*Function*

Returns true if the specified string is a "system" variable such as
`@@version`

See [`SessionContext::register_variable`] for more details

[`SessionContext::register_variable`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.register_variable

```rust
fn is_system_variables(variable_names: &[String]) -> bool
```



