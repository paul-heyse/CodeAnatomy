**deltalake_core > delta_datafusion > table_provider > next**

# Module: delta_datafusion::table_provider::next

## Contents

**Structs**

- [`DeletionVectorSelection`](#deletionvectorselection) - Deletion vector selection for one data file.
- [`DeltaScan`](#deltascan)

**Enums**

- [`SnapshotWrapper`](#snapshotwrapper)

---

## deltalake_core::delta_datafusion::table_provider::next::DeletionVectorSelection

*Struct*

Deletion vector selection for one data file.

**Fields:**
- `filepath: String` - Fully-qualified file URI.
- `keep_mask: Vec<bool>` - Row-level keep mask where `true` means keep and `false` means deleted.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DeletionVectorSelection`
- **PartialEq**
  - `fn eq(self: &Self, other: &DeletionVectorSelection) -> bool`



## deltalake_core::delta_datafusion::table_provider::next::DeltaScan

*Struct*

**Methods:**

- `fn new<impl Into<SnapshotWrapper>>(snapshot: impl Trait, config: DeltaScanConfig) -> Result<Self>`
- `fn deletion_vectors(self: &Self, session: &dyn Session) -> Result<Vec<DeletionVectorSelection>>` - Materialize deletion vector keep masks for files in this scan.
- `fn builder() -> TableProviderBuilder`

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **TableProvider**
  - `fn as_any(self: &Self) -> &dyn Any`
  - `fn schema(self: &Self) -> SchemaRef`
  - `fn table_type(self: &Self) -> TableType`
  - `fn get_table_definition(self: &Self) -> Option<&str>`
  - `fn get_logical_plan(self: &Self) -> Option<Cow<LogicalPlan>>`
  - `fn scan(self: &'life0 Self, session: &'life1 dyn Session, projection: Option<&'life2 Vec<usize>>, filters: &'life3 [Expr], limit: Option<usize>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn insert_into(self: &'life0 Self, state: &'life1 dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>`
  - `fn supports_filters_pushdown(self: &Self, filter: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>`
- **Clone**
  - `fn clone(self: &Self) -> DeltaScan`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::delta_datafusion::table_provider::next::SnapshotWrapper

*Enum*

**Variants:**
- `Snapshot(std::sync::Arc<crate::kernel::Snapshot>)`
- `EagerSnapshot(std::sync::Arc<crate::kernel::EagerSnapshot>)`



