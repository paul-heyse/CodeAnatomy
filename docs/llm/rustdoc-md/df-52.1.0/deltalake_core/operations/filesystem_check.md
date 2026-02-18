**deltalake_core > operations > filesystem_check**

# Module: operations::filesystem_check

## Contents

**Structs**

- [`FileSystemCheckBuilder`](#filesystemcheckbuilder) - Audit the Delta Table's active files with the underlying file system.
- [`FileSystemCheckMetrics`](#filesystemcheckmetrics) - Details of the FSCK operation including which files were removed from the log

---

## deltalake_core::operations::filesystem_check::FileSystemCheckBuilder

*Struct*

Audit the Delta Table's active files with the underlying file system.
See this module's documentation for more information

**Methods:**

- `fn with_dry_run(self: Self, dry_run: bool) -> Self` - Only determine which add actions should be removed. A dry run will not commit actions to the Delta log
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional information to write to the commit
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::operations::filesystem_check::FileSystemCheckMetrics

*Struct*

Details of the FSCK operation including which files were removed from the log

**Fields:**
- `dry_run: bool` - Was this a dry run
- `files_removed: Vec<String>` - Files that were removed successfully

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



