**datafusion > test > variable**

# Module: test::variable

## Contents

**Structs**

- [`SystemVar`](#systemvar) - System variable
- [`UserDefinedVar`](#userdefinedvar) - user defined variable

---

## datafusion::test::variable::SystemVar

*Struct*

System variable

**Methods:**

- `fn new() -> Self` - new system variable

**Trait Implementations:**

- **Default**
  - `fn default() -> SystemVar`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **VarProvider**
  - `fn get_value(self: &Self, var_names: Vec<String>) -> Result<ScalarValue>` - get system variable value
  - `fn get_type(self: &Self, _: &[String]) -> Option<DataType>`



## datafusion::test::variable::UserDefinedVar

*Struct*

user defined variable

**Methods:**

- `fn new() -> Self` - new user defined variable

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **VarProvider**
  - `fn get_value(self: &Self, var_names: Vec<String>) -> Result<ScalarValue>` - Get user defined variable value
  - `fn get_type(self: &Self, var_names: &[String]) -> Option<DataType>`
- **Default**
  - `fn default() -> UserDefinedVar`



