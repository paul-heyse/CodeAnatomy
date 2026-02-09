//! Preview formatting helpers for `datafusion-tracing`.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{new_null_array, ArrayRef, LargeStringArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion_tracing::pretty_format_compact_batch;

use crate::spec::runtime::{PreviewRedactionMode, TracingConfig};

pub type PreviewFormatter = dyn Fn(&RecordBatch) -> Result<String, ArrowError> + Send + Sync;

/// Build compact preview formatter from runtime config.
pub fn build_preview_formatter(
    config: &TracingConfig,
) -> Arc<PreviewFormatter> {
    let max_width = config.preview_max_width;
    let max_row_height = config.preview_max_row_height;
    let min_compacted_col_width = config.preview_min_compacted_col_width;
    let redaction_mode = config.preview_redaction_mode;
    let redaction_token = config.preview_redaction_token.clone();
    let redacted_columns: BTreeSet<String> = config
        .preview_redacted_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect();
    Arc::new(move |batch: &RecordBatch| {
        let safe_batch = redact_preview_batch(
            batch,
            redaction_mode,
            &redacted_columns,
            redaction_token.as_str(),
        )?;
        pretty_format_compact_batch(
            &safe_batch,
            max_width,
            max_row_height,
            min_compacted_col_width,
        )
        .map(|fmt| fmt.to_string())
    })
}

fn redact_preview_batch(
    batch: &RecordBatch,
    mode: PreviewRedactionMode,
    redacted_columns: &BTreeSet<String>,
    token: &str,
) -> Result<RecordBatch, ArrowError> {
    if mode == PreviewRedactionMode::None {
        return Ok(batch.clone());
    }

    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (idx, field) in schema.fields().iter().enumerate() {
        let source = batch.column(idx);
        if should_redact_column(field.name(), mode, redacted_columns) {
            columns.push(redact_array(source.clone(), token)?);
        } else {
            columns.push(source.clone());
        }
    }

    RecordBatch::try_new(schema, columns)
}

fn should_redact_column(
    column_name: &str,
    mode: PreviewRedactionMode,
    redacted_columns: &BTreeSet<String>,
) -> bool {
    let key = column_name.to_ascii_lowercase();
    match mode {
        PreviewRedactionMode::None => false,
        PreviewRedactionMode::DenyList => redacted_columns.contains(&key),
        PreviewRedactionMode::AllowList => !redacted_columns.contains(&key),
    }
}

fn redact_array(array: ArrayRef, token: &str) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Utf8 => {
            let Some(values) = array.as_any().downcast_ref::<StringArray>() else {
                return Err(ArrowError::CastError(
                    "Failed to cast Utf8 array for preview redaction".to_string(),
                ));
            };
            let masked = values
                .iter()
                .map(|value| value.map(|_| token.to_string()))
                .collect::<StringArray>();
            Ok(Arc::new(masked))
        }
        DataType::LargeUtf8 => {
            let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() else {
                return Err(ArrowError::CastError(
                    "Failed to cast LargeUtf8 array for preview redaction".to_string(),
                ));
            };
            let masked = values
                .iter()
                .map(|value| value.map(|_| token.to_string()))
                .collect::<LargeStringArray>();
            Ok(Arc::new(masked))
        }
        _ => Ok(new_null_array(array.data_type(), array.len())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};

    #[test]
    fn test_redaction_deny_list_masks_selected_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("token", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("abc"), Some("def")])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            ],
        )
        .expect("batch");

        let mut deny = BTreeSet::new();
        deny.insert("token".to_string());
        let redacted = redact_preview_batch(
            &batch,
            PreviewRedactionMode::DenyList,
            &deny,
            "[REDACTED]",
        )
        .expect("redacted");

        let rendered = pretty_format_compact_batch(&redacted, 120, 10, 10)
            .expect("format")
            .to_string();
        assert!(rendered.contains("[REDACTED]"));
    }

    #[test]
    fn test_redaction_allow_list_masks_unlisted_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("safe", DataType::Utf8, true),
            Field::new("secret", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("ok")])),
                Arc::new(StringArray::from(vec![Some("hidden")])),
            ],
        )
        .expect("batch");

        let mut allow = BTreeSet::new();
        allow.insert("safe".to_string());
        let redacted = redact_preview_batch(
            &batch,
            PreviewRedactionMode::AllowList,
            &allow,
            "[REDACTED]",
        )
        .expect("redacted");
        let rendered = pretty_format_compact_batch(&redacted, 120, 10, 10)
            .expect("format")
            .to_string();
        assert!(rendered.contains("ok"));
        assert!(rendered.contains("[REDACTED]"));
    }
}
