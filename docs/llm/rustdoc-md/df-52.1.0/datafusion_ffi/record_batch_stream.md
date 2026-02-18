**datafusion_ffi > record_batch_stream**

# Module: record_batch_stream

## Contents

**Structs**

- [`FFI_RecordBatchStream`](#ffi_recordbatchstream) - A stable struct for sharing [`RecordBatchStream`] across FFI boundaries.
- [`RecordBatchStreamPrivateData`](#recordbatchstreamprivatedata)

---

## datafusion_ffi::record_batch_stream::FFI_RecordBatchStream

*Struct*

A stable struct for sharing [`RecordBatchStream`] across FFI boundaries.
We use the async-ffi crate for handling async calls across libraries.

**Fields:**
- `poll_next: fn(...)` - This mirrors the `poll_next` of [`RecordBatchStream`] but does so
- `schema: fn(...)` - Return the schema of the record batch
- `release: fn(...)` - Release the memory of the private data when it is no longer being used.
- `private_data: *mut std::ffi::c_void` - Internal data. This is only to be accessed by the provider of the plan.

**Methods:**

- `fn new(stream: SendableRecordBatchStream, runtime: Option<Handle>) -> Self`

**Traits:** GetStaticEquivalent_, StableAbi, Send

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **RecordBatchStream**
  - `fn schema(self: &Self) -> arrow::datatypes::SchemaRef`
- **Stream**
  - `fn poll_next(self: std::pin::Pin<& mut Self>, cx: & mut std::task::Context) -> Poll<Option<<Self as >::Item>>`
- **From**
  - `fn from(stream: SendableRecordBatchStream) -> Self`
- **Drop**
  - `fn drop(self: & mut Self)`



## datafusion_ffi::record_batch_stream::RecordBatchStreamPrivateData

*Struct*

**Fields:**
- `rbs: datafusion_execution::SendableRecordBatchStream`
- `runtime: Option<tokio::runtime::Handle>`



