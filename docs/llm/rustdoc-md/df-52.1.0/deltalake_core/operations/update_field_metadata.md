**deltalake_core > operations > update_field_metadata**

# Module: operations::update_field_metadata

## Contents

**Structs**

- [`UpdateFieldMetadataBuilder`](#updatefieldmetadatabuilder) - Update a field's metadata in a schema. If the key does not exists, the entry is inserted.

---

## deltalake_core::operations::update_field_metadata::UpdateFieldMetadataBuilder

*Struct*

Update a field's metadata in a schema. If the key does not exists, the entry is inserted.

**Methods:**

- `fn with_field_name(self: Self, field_name: &str) -> Self` - Specify the field you want to update the metadata for
- `fn with_metadata(self: Self, metadata: HashMap<String, MetadataValue>) -> Self` - Specify the metadata to be added or modified on a field
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



