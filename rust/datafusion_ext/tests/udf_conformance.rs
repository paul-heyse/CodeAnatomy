use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use tokio::runtime::Runtime;

use datafusion_ext::udf_registry;

fn hash64_value(value: &str) -> i64 {
    let mut hasher = Blake2bVar::new(8).expect("blake2b supports 8-byte output");
    hasher.update(value.as_bytes());
    let mut out = [0u8; 8];
    hasher.finalize_variable(&mut out).expect("hash output");
    let unsigned = u64::from_be_bytes(out);
    let masked = unsigned & ((1_u64 << 63) - 1);
    masked as i64
}

fn hash128_value(value: &str) -> String {
    let mut hasher = Blake2bVar::new(16).expect("blake2b supports 16-byte output");
    hasher.update(value.as_bytes());
    let mut out = [0u8; 16];
    hasher.finalize_variable(&mut out).expect("hash output");
    hex::encode(out)
}

fn run_query(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let runtime = Runtime::new().expect("tokio runtime");
    let df = runtime.block_on(ctx.sql(sql))?;
    runtime.block_on(df.collect())
}

#[test]
fn stable_hash64_matches_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx);
    let batches = run_query(&ctx, "SELECT stable_hash64('alpha') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), hash64_value("alpha"));
    Ok(())
}

#[test]
fn stable_hash128_matches_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx);
    let batches = run_query(&ctx, "SELECT stable_hash128('beta') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    assert_eq!(array.value(0), hash128_value("beta"));
    Ok(())
}

#[test]
fn prefixed_hash64_matches_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx);
    let batches = run_query(&ctx, "SELECT prefixed_hash64('ns', 'gamma') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    let expected = format!("ns:{}", hash64_value("gamma"));
    assert_eq!(array.value(0), expected);
    Ok(())
}

#[test]
fn stable_id_matches_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx);
    let batches = run_query(&ctx, "SELECT stable_id('ns', 'delta') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    let expected = format!("ns:{}", hash128_value("delta"));
    assert_eq!(array.value(0), expected);
    Ok(())
}

#[test]
fn col_to_byte_returns_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx);
    let batches = run_query(
        &ctx,
        "SELECT col_to_byte('abcdef', 3, 'BYTE') AS value",
    )?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), 3);
    Ok(())
}

#[test]
fn arrow_metadata_extracts_key() -> Result<()> {
    let mut metadata = HashMap::new();
    metadata.insert("line_base".to_string(), "10".to_string());
    let field = Field::new("code", DataType::Utf8, true).with_metadata(metadata);
    let schema = Arc::new(Schema::new(vec![field]));
    let array = Arc::new(StringArray::from(vec!["ok"])) as Arc<dyn arrow::array::Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx);
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(&ctx, "SELECT arrow_metadata(code, 'line_base') AS value FROM t")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    assert_eq!(array.value(0), "10");
    Ok(())
}

#[test]
fn range_table_generates_series() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx);
    let batches = run_query(&ctx, "SELECT value FROM range_table(1, 4) ORDER BY value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    let values: Vec<i64> = array.iter().flatten().collect();
    assert_eq!(values, vec![1, 2, 3]);
    Ok(())
}
