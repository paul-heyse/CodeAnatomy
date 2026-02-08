//! Delta output table creation and schema enforcement.

use std::sync::Arc;

use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, Result};
use datafusion_ext::delta_control_plane::{delta_provider_from_session, DeltaScanOverrides};
use datafusion_ext::DeltaFeatureGate;
use deltalake::delta_datafusion::DeltaScanConfig;
use deltalake::kernel::{
    ArrayType as DeltaArrayType, DataType as DeltaDataType, MapType as DeltaMapType,
    PrimitiveType, StructField as DeltaStructField, StructType as DeltaStructType,
};
use deltalake::{ensure_table_uri, DeltaTable};

fn map_arrow_type(data_type: &ArrowDataType) -> Result<DeltaDataType> {
    match data_type {
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            Ok(DeltaDataType::Primitive(PrimitiveType::String))
        }
        ArrowDataType::Int64 | ArrowDataType::UInt64 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Long))
        }
        ArrowDataType::Int32 | ArrowDataType::UInt32 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Integer))
        }
        ArrowDataType::Int16 | ArrowDataType::UInt16 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Short))
        }
        ArrowDataType::Int8 | ArrowDataType::UInt8 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Byte))
        }
        ArrowDataType::Float32 => Ok(DeltaDataType::Primitive(PrimitiveType::Float)),
        ArrowDataType::Float64 => Ok(DeltaDataType::Primitive(PrimitiveType::Double)),
        ArrowDataType::Boolean => Ok(DeltaDataType::Primitive(PrimitiveType::Boolean)),
        ArrowDataType::Binary
        | ArrowDataType::LargeBinary
        | ArrowDataType::FixedSizeBinary(_)
        | ArrowDataType::BinaryView => Ok(DeltaDataType::Primitive(PrimitiveType::Binary)),
        ArrowDataType::Date32 | ArrowDataType::Date64 => {
            Ok(DeltaDataType::Primitive(PrimitiveType::Date))
        }
        ArrowDataType::Timestamp(_, tz) => {
            if tz.is_some() {
                Ok(DeltaDataType::Primitive(PrimitiveType::Timestamp))
            } else {
                Ok(DeltaDataType::Primitive(PrimitiveType::TimestampNtz))
            }
        }
        ArrowDataType::Decimal128(precision, scale)
        | ArrowDataType::Decimal256(precision, scale) => {
            if *scale < 0 {
                return Err(DataFusionError::Plan(format!(
                    "Negative decimal scale is not supported for Delta output schema: {scale}"
                )));
            }
            let decimal = PrimitiveType::decimal(*precision, *scale as u8).map_err(|err| {
                DataFusionError::Plan(format!(
                    "Invalid decimal precision/scale for Delta output schema: {err}"
                ))
            })?;
            Ok(DeltaDataType::Primitive(decimal))
        }
        ArrowDataType::Struct(fields) => {
            let converted = fields
                .iter()
                .map(|field| map_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>>>()?;
            let struct_type = DeltaStructType::try_new(converted).map_err(|err| {
                DataFusionError::Plan(format!(
                    "Failed to build Delta struct type from Arrow schema: {err}"
                ))
            })?;
            Ok(DeltaDataType::Struct(Box::new(struct_type)))
        }
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::ListView(field)
        | ArrowDataType::LargeListView(field)
        | ArrowDataType::FixedSizeList(field, _) => {
            let element_type = map_arrow_type(field.data_type())?;
            Ok(DeltaDataType::Array(Box::new(DeltaArrayType::new(
                element_type,
                field.is_nullable(),
            ))))
        }
        ArrowDataType::Map(entries, _) => {
            let ArrowDataType::Struct(fields) = entries.data_type() else {
                return Err(DataFusionError::Plan(
                    "Arrow map entries must be represented as struct(key, value)".to_string(),
                ));
            };
            if fields.len() != 2 {
                return Err(DataFusionError::Plan(format!(
                    "Arrow map entries struct must contain exactly 2 fields, found {}",
                    fields.len()
                )));
            }
            let key_type = map_arrow_type(fields[0].data_type())?;
            let value_type = map_arrow_type(fields[1].data_type())?;
            Ok(DeltaDataType::Map(Box::new(DeltaMapType::new(
                key_type,
                value_type,
                fields[1].is_nullable(),
            ))))
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported Arrow type for Delta output schema: {data_type:?}"
        ))),
    }
}

fn map_arrow_field(field: &ArrowField) -> Result<DeltaStructField> {
    Ok(DeltaStructField::new(
        field.name(),
        map_arrow_type(field.data_type())?,
        field.is_nullable(),
    ))
}

fn map_arrow_schema(schema: &ArrowSchema) -> Result<Vec<DeltaStructField>> {
    schema
        .fields()
        .iter()
        .map(|field| map_arrow_field(field.as_ref()))
        .collect()
}

/// Ensure an output Delta table exists with the expected schema.
/// Creates the table if it doesn't exist; validates schema if it does.
pub async fn ensure_output_table(
    ctx: &SessionContext,
    table_name: &str,
    delta_location: &str,
    expected_schema: &SchemaRef,
) -> Result<()> {
    let table_uri = ensure_table_uri(delta_location)
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    let mut table = DeltaTable::try_from_url(table_uri)
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

    if table.version().is_none() {
        let columns = map_arrow_schema(expected_schema.as_ref())?;
        table = table
            .create()
            .with_table_name(table_name)
            .with_columns(columns)
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
    }

    let scan_config = DeltaScanConfig::default();
    let scan_overrides = DeltaScanOverrides {
        file_column_name: scan_config.file_column_name,
        enable_parquet_pushdown: Some(scan_config.enable_parquet_pushdown),
        schema_force_view_types: Some(scan_config.schema_force_view_types),
        wrap_partition_values: Some(scan_config.wrap_partition_values),
        schema: scan_config.schema,
    };
    let (provider, _snapshot, _scan_config, _pruned_files, predicate_error) =
        delta_provider_from_session(
            ctx,
            delta_location,
            None,
            table.version(),
            None,
            None,
            scan_overrides,
            Some(DeltaFeatureGate::default()),
        )
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    if let Some(error) = predicate_error {
        return Err(DataFusionError::Plan(format!(
            "Delta predicate parsing failed while registering output '{table_name}': {error}"
        )));
    }
    validate_output_schema(provider.schema().as_ref(), expected_schema.as_ref())?;
    ctx.register_table(table_name, Arc::new(provider))?;
    Ok(())
}

/// Validate that a DataFrame's output schema matches the expected target schema.
pub fn validate_output_schema(
    actual: &arrow::datatypes::Schema,
    expected: &arrow::datatypes::Schema,
) -> Result<()> {
    // Check that all expected columns exist in actual with compatible types
    for expected_field in expected.fields() {
        match actual.field_with_name(expected_field.name()) {
            Ok(actual_field) => {
                if actual_field.data_type() != expected_field.data_type() {
                    return Err(DataFusionError::Plan(format!(
                        "Schema mismatch for column '{}': expected {:?}, got {:?}",
                        expected_field.name(),
                        expected_field.data_type(),
                        actual_field.data_type(),
                    )));
                }
            }
            Err(_) => {
                return Err(DataFusionError::Plan(format!(
                    "Missing required column '{}' in output",
                    expected_field.name(),
                )));
            }
        }
    }
    Ok(())
}

/// Extract total row count from write result batches.
///
/// DataFusion's write_table() returns Vec<RecordBatch> containing the write statistics.
/// This helper sums the row counts across all batches.
pub fn extract_row_count(batches: &[RecordBatch]) -> u64 {
    batches.iter().map(|b| b.num_rows() as u64).sum()
}
