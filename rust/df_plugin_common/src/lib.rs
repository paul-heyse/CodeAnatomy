use std::io::Cursor;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;

pub fn parse_major(version: &str) -> Result<u16, String> {
    let Some((major, _)) = version.split_once('.') else {
        return Err(format!("Invalid version string {version:?}"));
    };
    major
        .parse::<u16>()
        .map_err(|err| format!("Invalid version string {version:?}: {err}"))
}

pub fn schema_from_ipc(payload: &[u8]) -> Result<SchemaRef, ArrowError> {
    let reader = StreamReader::try_new(Cursor::new(payload), None)?;
    Ok(Arc::clone(&reader.schema()))
}

pub const DELTA_SCAN_CONFIG_VERSION: u32 = 1;

