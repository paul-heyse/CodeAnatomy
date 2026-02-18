**datafusion_functions > core > expr_ext**

# Module: core::expr_ext

## Contents

**Traits**

- [`FieldAccessor`](#fieldaccessor) - Return access to the named field. Example `expr["name"]`

---

## datafusion_functions::core::expr_ext::FieldAccessor

*Trait*

Return access to the named field. Example `expr["name"]`

## Access field "my_field" from column "c1"

For example if column "c1" holds documents like this

```json
{
  "my_field": 123.34,
  "other_field": "Boston",
}
```

You can access column "my_field" with

```
# use datafusion_expr::{col};
# use datafusion_functions::core::expr_ext::FieldAccessor;
let expr = col("c1").field("my_field");
assert_eq!(expr.schema_name().to_string(), "c1[my_field]");
```

**Methods:**

- `field`



