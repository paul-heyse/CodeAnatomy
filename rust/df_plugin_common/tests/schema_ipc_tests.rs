use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use df_plugin_common::schema_from_ipc;

#[test]
fn schema_from_ipc_round_trips_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
    )
    .expect("record batch");
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish");
    }

    let decoded = schema_from_ipc(&bytes).expect("decode schema");
    assert_eq!(decoded.as_ref(), schema.as_ref());
}
