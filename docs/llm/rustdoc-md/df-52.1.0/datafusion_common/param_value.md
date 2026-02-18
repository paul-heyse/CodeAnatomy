**datafusion_common > param_value**

# Module: param_value

## Contents

**Enums**

- [`ParamValues`](#paramvalues) - The parameter value corresponding to the placeholder

---

## datafusion_common::param_value::ParamValues

*Enum*

The parameter value corresponding to the placeholder

**Variants:**
- `List(Vec<crate::metadata::ScalarAndMetadata>)` - For positional query parameters, like `SELECT * FROM test WHERE a > $1 AND b = $2`
- `Map(std::collections::HashMap<String, crate::metadata::ScalarAndMetadata>)` - For named query parameters, like `SELECT * FROM test WHERE a > $foo AND b = $goo`

**Methods:**

- `fn verify(self: &Self, expect: &[DataType]) -> Result<()>` - Verify parameter list length and DataType
- `fn verify_fields(self: &Self, expect: &[FieldRef]) -> Result<()>` - Verify parameter list length and type
- `fn get_placeholders_with_values(self: &Self, id: &str) -> Result<ScalarAndMetadata>`

**Trait Implementations:**

- **From**
  - `fn from(value: HashMap<K, ScalarValue>) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> ParamValues`
- **From**
  - `fn from(value: Vec<ScalarValue>) -> Self`
- **From**
  - `fn from(value: Vec<(K, ScalarValue)>) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



