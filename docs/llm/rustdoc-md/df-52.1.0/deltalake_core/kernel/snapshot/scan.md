**deltalake_core > kernel > snapshot > scan**

# Module: kernel::snapshot::scan

## Contents

**Structs**

- [`Scan`](#scan)
- [`ScanBuilder`](#scanbuilder) - Builder to scan a snapshot of a table.

**Type Aliases**

- [`SendableScanMetadataStream`](#sendablescanmetadatastream)

---

## deltalake_core::kernel::snapshot::scan::Scan

*Struct*

**Methods:**

- `fn table_root(self: &Self) -> &Url` - The table's root URL. Any relative paths returned from `scan_data` (or in a callback from
- `fn snapshot(self: &Self) -> &SnapshotRef` - Get a shared reference to the [`Snapshot`] of this scan.
- `fn logical_schema(self: &Self) -> &SchemaRef` - Get a shared reference to the logical [`Schema`] of the scan (i.e. the output schema of the
- `fn physical_schema(self: &Self) -> &SchemaRef` - Get a shared reference to the physical [`Schema`] of the scan. This represents the schema
- `fn physical_predicate(self: &Self) -> Option<PredicateRef>` - Get the predicate [`PredicateRef`] of the scan.
- `fn scan_metadata(self: &Self, engine: Arc<dyn Engine>) -> SendableScanMetadataStream`
- `fn scan_metadata_from<T>(self: &Self, engine: Arc<dyn Engine>, existing_version: Version, existing_data: Box<T>, existing_predicate: Option<PredicateRef>) -> SendableScanMetadataStream`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(inner: KernelScan) -> Self`
- **From**
  - `fn from(inner: Arc<KernelScan>) -> Self`



## deltalake_core::kernel::snapshot::scan::ScanBuilder

*Struct*

Builder to scan a snapshot of a table.

**Methods:**

- `fn new<impl Into<Arc<KernelSnapshot>>>(snapshot: impl Trait) -> Self` - Create a new [`ScanBuilder`] instance.
- `fn with_schema(self: Self, schema: SchemaRef) -> Self` - Provide [`Schema`] for columns to select from the [`Snapshot`].
- `fn with_schema_opt(self: Self, schema_opt: Option<SchemaRef>) -> Self` - Optionally provide a [`SchemaRef`] for columns to select from the [`Snapshot`]. See
- `fn with_predicate<impl Into<Option<PredicateRef>>>(self: Self, predicate: impl Trait) -> Self` - Optionally provide an expression to filter rows. For example, using the predicate `x <
- `fn build(self: Self) -> DeltaResult<Scan>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::kernel::snapshot::scan::SendableScanMetadataStream

*Type Alias*: `std::pin::Pin<Box<dyn Stream>>`



