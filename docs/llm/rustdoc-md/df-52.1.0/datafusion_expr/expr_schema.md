**datafusion_expr > expr_schema**

# Module: expr_schema

## Contents

**Functions**

- [`cast_subquery`](#cast_subquery) - Cast subquery in InSubquery/ScalarSubquery to a given type.

**Traits**

- [`ExprSchemable`](#exprschemable) - Trait to allow expr to typable with respect to a schema

---

## datafusion_expr::expr_schema::ExprSchemable

*Trait*

Trait to allow expr to typable with respect to a schema

**Methods:**

- `get_type`: Given a schema, return the type of the expr
- `nullable`: Given a schema, return the nullability of the expr
- `metadata`: Given a schema, return the expr's optional metadata
- `to_field`: Convert to a field with respect to a schema
- `cast_to`: Cast to a type with respect to a schema
- `data_type_and_nullable`: Given a schema, return the type and nullability of the expr



## datafusion_expr::expr_schema::cast_subquery

*Function*

Cast subquery in InSubquery/ScalarSubquery to a given type.

1. **Projection plan**: If the subquery is a projection (i.e. a SELECT statement with specific
   columns), it casts the first expression in the projection to the target type and creates a
   new projection with the casted expression.
2. **Non-projection plan**: If the subquery isn't a projection, it adds a projection to the plan
   with the casted first column.

```rust
fn cast_subquery(subquery: crate::Subquery, cast_to_type: &arrow::datatypes::DataType) -> datafusion_common::Result<crate::Subquery>
```



