**deltalake_core > operations > add_feature**

# Module: operations::add_feature

## Contents

**Structs**

- [`AddTableFeatureBuilder`](#addtablefeaturebuilder) - Enable table features for a table

---

## deltalake_core::operations::add_feature::AddTableFeatureBuilder

*Struct*

Enable table features for a table

**Methods:**

- `fn with_feature<S>(self: Self, name: S) -> Self` - Specify the features to be added
- `fn with_features<S>(self: Self, name: Vec<S>) -> Self` - Specify the features to be added
- `fn with_allow_protocol_versions_increase(self: Self, allow: bool) -> Self` - Specify if you want to allow protocol version to be increased
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



