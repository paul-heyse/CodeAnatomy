**datafusion_datasource > file_stream**

# Module: file_stream

## Contents

**Structs**

- [`FileStream`](#filestream) - A stream that iterates record batch by record batch, file over file.
- [`FileStreamMetrics`](#filestreammetrics) - Metrics for [`FileStream`]
- [`StartableTime`](#startabletime) - A timer that can be started and stopped.

**Enums**

- [`FileStreamState`](#filestreamstate)
- [`NextOpen`](#nextopen) - Represents the state of the next `FileOpenFuture`. Since we need to poll
- [`OnError`](#onerror) - Describes the behavior of the `FileStream` if file opening or scanning fails

**Traits**

- [`FileOpener`](#fileopener) - Generic API for opening a file using an [`ObjectStore`] and resolving to a

**Type Aliases**

- [`FileOpenFuture`](#fileopenfuture) - A fallible future that resolves to a stream of [`RecordBatch`]

---

## datafusion_datasource::file_stream::FileOpenFuture

*Type Alias*: `futures::future::BoxFuture<'static, datafusion_common::error::Result<futures::stream::BoxStream<'static, datafusion_common::error::Result<arrow::record_batch::RecordBatch>>>>`

A fallible future that resolves to a stream of [`RecordBatch`]



## datafusion_datasource::file_stream::FileOpener

*Trait*

Generic API for opening a file using an [`ObjectStore`] and resolving to a
stream of [`RecordBatch`]

[`ObjectStore`]: object_store::ObjectStore

**Methods:**

- `open`: Asynchronously open the specified file and return a stream



## datafusion_datasource::file_stream::FileStream

*Struct*

A stream that iterates record batch by record batch, file over file.

**Methods:**

- `fn new(config: &FileScanConfig, partition: usize, file_opener: Arc<dyn FileOpener>, metrics: &ExecutionPlanMetricsSet) -> Result<Self>` - Create a new `FileStream` using the give `FileOpener` to scan underlying files
- `fn with_on_error(self: Self, on_error: OnError) -> Self` - Specify the behavior when an error occurs opening or scanning a file

**Trait Implementations:**

- **Stream**
  - `fn poll_next(self: Pin<& mut Self>, cx: & mut Context) -> Poll<Option<<Self as >::Item>>`
- **RecordBatchStream**
  - `fn schema(self: &Self) -> SchemaRef`



## datafusion_datasource::file_stream::FileStreamMetrics

*Struct*

Metrics for [`FileStream`]

Note that all of these metrics are in terms of wall clock time
(not cpu time) so they include time spent waiting on I/O as well
as other operators.

[`FileStream`]: <https://github.com/apache/datafusion/blob/main/datafusion/datasource/src/file_stream.rs>

**Fields:**
- `time_opening: StartableTime` - Wall clock time elapsed for file opening.
- `time_scanning_until_data: StartableTime` - Wall clock time elapsed for file scanning + first record batch of decompression + decoding
- `time_scanning_total: StartableTime` - Total elapsed wall clock time for scanning + record batch decompression / decoding
- `time_processing: StartableTime` - Wall clock time elapsed for data decompression + decoding
- `file_open_errors: datafusion_physical_plan::metrics::Count` - Count of errors opening file.
- `file_scan_errors: datafusion_physical_plan::metrics::Count` - Count of errors scanning file

**Methods:**

- `fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self`



## datafusion_datasource::file_stream::FileStreamState

*Enum*

**Variants:**
- `Idle` - The idle state, no file is currently being read
- `Open{ future: FileOpenFuture }` - Currently performing asynchronous IO to obtain a stream of RecordBatch
- `Scan{ reader: futures::stream::BoxStream<'static, datafusion_common::error::Result<arrow::record_batch::RecordBatch>>, next: Option<NextOpen> }` - Scanning the [`BoxStream`] returned by the completion of a [`FileOpenFuture`]
- `Error` - Encountered an error
- `Limit` - Reached the row limit



## datafusion_datasource::file_stream::NextOpen

*Enum*

Represents the state of the next `FileOpenFuture`. Since we need to poll
this future while scanning the current file, we need to store the result if it
is ready

**Variants:**
- `Pending(FileOpenFuture)`
- `Ready(datafusion_common::error::Result<futures::stream::BoxStream<'static, datafusion_common::error::Result<arrow::record_batch::RecordBatch>>>)`



## datafusion_datasource::file_stream::OnError

*Enum*

Describes the behavior of the `FileStream` if file opening or scanning fails

**Variants:**
- `Fail` - Fail the entire stream and return the underlying error
- `Skip` - Continue scanning, ignoring the failed file

**Trait Implementations:**

- **Default**
  - `fn default() -> OnError`



## datafusion_datasource::file_stream::StartableTime

*Struct*

A timer that can be started and stopped.

**Fields:**
- `metrics: datafusion_physical_plan::metrics::Time`
- `start: Option<datafusion_common::instant::Instant>`

**Methods:**

- `fn start(self: & mut Self)`
- `fn stop(self: & mut Self)`



