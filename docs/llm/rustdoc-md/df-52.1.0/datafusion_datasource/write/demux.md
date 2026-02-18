**datafusion_datasource > write > demux**

# Module: write::demux

## Contents

**Type Aliases**

- [`DemuxedStreamReceiver`](#demuxedstreamreceiver)

---

## datafusion_datasource::write::demux::DemuxedStreamReceiver

*Type Alias*: `tokio::sync::mpsc::UnboundedReceiver<(object_store::path::Path, tokio::sync::mpsc::Receiver<arrow::array::RecordBatch>)>`



