**datafusion_datasource > write > orchestration**

# Module: write::orchestration

## Contents

**Functions**

- [`spawn_writer_tasks_and_join`](#spawn_writer_tasks_and_join) - Orchestrates multipart put of a dynamic number of output files from a single input stream

---

## datafusion_datasource::write::orchestration::spawn_writer_tasks_and_join

*Function*

Orchestrates multipart put of a dynamic number of output files from a single input stream
for any statelessly serialized file type. That is, any file type for which each [RecordBatch]
can be serialized independently of all other [RecordBatch]s.

```rust
fn spawn_writer_tasks_and_join(context: &std::sync::Arc<datafusion_execution::TaskContext>, serializer: std::sync::Arc<dyn BatchSerializer>, compression: crate::file_compression_type::FileCompressionType, compression_level: Option<u32>, object_store: std::sync::Arc<dyn ObjectStore>, demux_task: datafusion_common_runtime::SpawnedTask<datafusion_common::error::Result<()>>, file_stream_rx: super::demux::DemuxedStreamReceiver) -> datafusion_common::error::Result<u64>
```



