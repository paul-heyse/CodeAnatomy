**deltalake_core > writer > json**

# Module: writer::json

## Contents

**Structs**

- [`JsonWriter`](#jsonwriter) - Writes messages to a delta lake table.

---

## deltalake_core::writer::json::JsonWriter

*Struct*

Writes messages to a delta lake table.

**Methods:**

- `fn try_new(table_url: Url, schema_ref: ArrowSchemaRef, partition_columns: Option<Vec<String>>, storage_options: Option<HashMap<String, String>>) -> Result<Self, DeltaTableError>` - Create a new JsonWriter instance
- `fn for_table(table: &DeltaTable) -> Result<JsonWriter, DeltaTableError>` - Creates a JsonWriter to write to the given table
- `fn buffer_len(self: &Self) -> usize` - Returns the current byte length of the in memory buffer.
- `fn buffered_record_batch_count(self: &Self) -> usize` - Returns the number of records held in the current buffer.
- `fn reset(self: & mut Self)` - Resets internal state.
- `fn arrow_schema(self: &Self) -> Arc<arrow::datatypes::Schema>` - Returns the user-defined arrow schema representation or the schema defined for the wrapped

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **DeltaWriter**
  - `fn write(self: &'life0  mut Self, values: Vec<Value>) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Write a chunk of values into the internal write buffers with the default write mode
  - `fn write_with_mode(self: &'life0  mut Self, values: Vec<Value>, mode: WriteMode) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Writes the given values to internal parquet buffers for each represented partition.
  - `fn flush(self: &'life0  mut Self) -> ::core::pin::Pin<Box<dyn ::core::future::Future>>` - Writes the existing parquet bytes to storage and resets internal state to handle another



