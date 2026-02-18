**deltalake_core > operations > update_table_metadata**

# Module: operations::update_table_metadata

## Contents

**Structs**

- [`TableMetadataUpdate`](#tablemetadataupdate)
- [`UpdateTableMetadataBuilder`](#updatetablemetadatabuilder) - Update table metadata operation

---

## deltalake_core::operations::update_table_metadata::TableMetadataUpdate

*Struct*

**Fields:**
- `name: Option<String>`
- `description: Option<String>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Validate**
  - `fn validate(self: &Self) -> ::std::result::Result<(), ::validator::ValidationErrors>`
- **Clone**
  - `fn clone(self: &Self) -> TableMetadataUpdate`
- **ValidateArgs**
  - `fn validate_with_args(self: &Self, args: <Self as >::Args) -> ::std::result::Result<(), ::validator::ValidationErrors>`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## deltalake_core::operations::update_table_metadata::UpdateTableMetadataBuilder

*Struct*

Update table metadata operation

**Methods:**

- `fn with_update(self: Self, update: TableMetadataUpdate) -> Self` - Specify the complete metadata update
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



