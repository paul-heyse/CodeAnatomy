**deltalake_core > operations > set_tbl_properties**

# Module: operations::set_tbl_properties

## Contents

**Structs**

- [`SetTablePropertiesBuilder`](#settablepropertiesbuilder) - Remove constraints from the table

---

## deltalake_core::operations::set_tbl_properties::SetTablePropertiesBuilder

*Struct*

Remove constraints from the table

**Methods:**

- `fn with_properties(self: Self, table_properties: HashMap<String, String>) -> Self` - Specify the properties to be removed
- `fn with_raise_if_not_exists(self: Self, raise: bool) -> Self` - Specify if you want to raise if the property does not exist
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



