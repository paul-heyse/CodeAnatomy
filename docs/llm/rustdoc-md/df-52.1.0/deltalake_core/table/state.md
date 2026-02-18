**deltalake_core > table > state**

# Module: table::state

## Contents

**Structs**

- [`DeltaTableState`](#deltatablestate) - State snapshot currently held by the Delta Table instance.

---

## deltalake_core::table::state::DeltaTableState

*Struct*

State snapshot currently held by the Delta Table instance.

**Methods:**

- `fn new(snapshot: EagerSnapshot) -> Self`
- `fn try_new(log_store: &dyn LogStore, config: DeltaTableConfig, version: Option<i64>) -> DeltaResult<Self>` - Create a new DeltaTableState
- `fn version(self: &Self) -> i64` - Return table version
- `fn protocol(self: &Self) -> &Protocol` - The most recent protocol of the table.
- `fn metadata(self: &Self) -> &Metadata` - The most recent metadata of the table.
- `fn schema(self: &Self) -> KernelSchemaRef` - The table schema
- `fn load_config(self: &Self) -> &DeltaTableConfig` - Get the table config which is loaded with of the snapshot
- `fn table_config(self: &Self) -> &TableProperties` - Well known table configuration
- `fn version_timestamp(self: &Self, version: i64) -> Option<i64>` - Get the timestamp when a version commit was created.
- `fn log_data(self: &Self) -> LogDataHandler` - Returns a semantic accessor to the currently loaded log data.
- `fn all_tombstones(self: &Self, log_store: &dyn LogStore) -> BoxStream<DeltaResult<TombstoneView>>` - Full list of tombstones (remove actions) representing files removed from table state).
- `fn transaction_version<impl ToString>(self: &Self, log_store: &dyn LogStore, app_id: impl Trait) -> DeltaResult<Option<i64>>` - Get the transaction version for the given application ID.
- `fn snapshot(self: &Self) -> &EagerSnapshot` - Obtain the Eager snapshot of the state
- `fn update(self: & mut Self, log_store: &dyn LogStore, version: Option<i64>) -> Result<(), DeltaTableError>` - Update the state of the table to the given version.
- `fn add_actions_table(self: &Self, flatten: bool) -> Result<arrow::record_batch::RecordBatch, DeltaTableError>` - Get an [arrow::record_batch::RecordBatch] containing add action data.
- `fn add_actions_batches(self: &Self, flatten: bool) -> Result<Vec<arrow::record_batch::RecordBatch>, DeltaTableError>` - Get add action data as a list of [arrow::record_batch::RecordBatch]

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> DeltaTableState`
- **TableReference**
  - `fn config(self: &Self) -> &TableProperties`
  - `fn protocol(self: &Self) -> &Protocol`
  - `fn metadata(self: &Self) -> &Metadata`
  - `fn eager_snapshot(self: &Self) -> &EagerSnapshot`



