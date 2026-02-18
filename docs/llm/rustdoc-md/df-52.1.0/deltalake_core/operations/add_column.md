**deltalake_core > operations > add_column**

# Module: operations::add_column

## Contents

**Structs**

- [`AddColumnBuilder`](#addcolumnbuilder) - Add new columns and/or nested fields to a table

---

## deltalake_core::operations::add_column::AddColumnBuilder

*Struct*

Add new columns and/or nested fields to a table

**Methods:**

- `fn with_fields<impl IntoIterator<Item = StructField> + Clone>(self: Self, fields: impl Trait) -> Self` - Specify the fields to be added
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



