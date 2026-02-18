**datafusion_functions > datetime > to_timestamp**

# Module: datetime::to_timestamp

## Contents

**Structs**

- [`ToTimestampFunc`](#totimestampfunc)
- [`ToTimestampMicrosFunc`](#totimestampmicrosfunc)
- [`ToTimestampMillisFunc`](#totimestampmillisfunc)
- [`ToTimestampNanosFunc`](#totimestampnanosfunc)
- [`ToTimestampSecondsFunc`](#totimestampsecondsfunc)

---

## datafusion_functions::datetime::to_timestamp::ToTimestampFunc

*Struct*

**Methods:**

- `fn new() -> Self` - Deprecated constructor retained for backwards compatibility.
- `fn new_with_config(config: &ConfigOptions) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn with_updated_config(self: &Self, config: &ConfigOptions) -> Option<ScalarUDF>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ToTimestampFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



## datafusion_functions::datetime::to_timestamp::ToTimestampMicrosFunc

*Struct*

**Methods:**

- `fn new() -> Self` - Deprecated constructor retained for backwards compatibility.
- `fn new_with_config(config: &ConfigOptions) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn with_updated_config(self: &Self, config: &ConfigOptions) -> Option<ScalarUDF>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ToTimestampMicrosFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions::datetime::to_timestamp::ToTimestampMillisFunc

*Struct*

**Methods:**

- `fn new() -> Self` - Deprecated constructor retained for backwards compatibility.
- `fn new_with_config(config: &ConfigOptions) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &ToTimestampMillisFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn with_updated_config(self: &Self, config: &ConfigOptions) -> Option<ScalarUDF>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions::datetime::to_timestamp::ToTimestampNanosFunc

*Struct*

**Methods:**

- `fn new() -> Self` - Deprecated constructor retained for backwards compatibility.
- `fn new_with_config(config: &ConfigOptions) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn with_updated_config(self: &Self, config: &ConfigOptions) -> Option<ScalarUDF>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ToTimestampNanosFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_functions::datetime::to_timestamp::ToTimestampSecondsFunc

*Struct*

**Methods:**

- `fn new() -> Self` - Deprecated constructor retained for backwards compatibility.
- `fn new_with_config(config: &ConfigOptions) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **ScalarUDFImpl**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn name(self: &Self) -> &str`
  - `fn signature(self: &Self) -> &Signature`
  - `fn return_type(self: &Self, _arg_types: &[DataType]) -> Result<DataType>`
  - `fn with_updated_config(self: &Self, config: &ConfigOptions) -> Option<ScalarUDF>`
  - `fn invoke_with_args(self: &Self, args: datafusion_expr::ScalarFunctionArgs) -> Result<ColumnarValue>`
  - `fn documentation(self: &Self) -> Option<&Documentation>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ToTimestampSecondsFunc) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Default**
  - `fn default() -> Self`



