//! Schema runtime utilities for drift detection and evolution.
//!
//! Provides helpers for:
//! - Schema hashing (BLAKE3) for drift detection
//! - Schema diffing (added/removed/changed fields)
//! - Additive evolution detection (safe for Delta append)
//! - Cast alignment expression generation

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{DataFusionError, Result};
use std::collections::{BTreeMap, BTreeSet};

pub type SchemaDiff = (Vec<String>, Vec<String>, Vec<(String, DataType, DataType)>);

/// Compute a BLAKE3 hash of an Arrow schema for drift detection.
///
/// The hash includes field names, data types, and nullability flags.
/// Field order is significant - reordering fields produces a different hash.
///
/// # Example
///
/// ```
/// use arrow::datatypes::{Schema, Field, DataType};
/// use codeanatomy_engine::schema::introspection::hash_arrow_schema;
///
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
/// ]);
///
/// let hash = hash_arrow_schema(&schema);
/// assert_eq!(hash.len(), 32);
/// ```
pub fn hash_arrow_schema(schema: &Schema) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();

    // Hash field count for quick mismatch detection
    hasher.update(&(schema.fields().len() as u32).to_le_bytes());

    for field in schema.fields() {
        // Hash field name
        hasher.update(field.name().as_bytes());

        // Hash data type (using Debug representation for consistency)
        let type_repr = format!("{:?}", field.data_type());
        hasher.update(type_repr.as_bytes());

        // Hash nullability
        hasher.update(&[field.is_nullable() as u8]);

        // Hash metadata if present
        let metadata = field.metadata();
        if !metadata.is_empty() {
            let mut sorted_keys: Vec<_> = metadata.keys().collect();
            sorted_keys.sort();
            for key in sorted_keys {
                hasher.update(key.as_bytes());
                if let Some(value) = metadata.get(key) {
                    hasher.update(value.as_bytes());
                }
            }
        }
    }

    // Hash schema-level metadata if present
    let schema_metadata = schema.metadata();
    if !schema_metadata.is_empty() {
        let mut sorted_keys: Vec<_> = schema_metadata.keys().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            hasher.update(key.as_bytes());
            if let Some(value) = schema_metadata.get(key) {
                hasher.update(value.as_bytes());
            }
        }
    }

    *hasher.finalize().as_bytes()
}

/// Detect schema differences between two Arrow schemas.
///
/// Returns a tuple of:
/// - Added fields (field names in `new` but not in `old`)
/// - Removed fields (field names in `old` but not in `new`)
/// - Type-changed fields (field name, old type, new type)
///
/// # Example
///
/// ```
/// use arrow::datatypes::{Schema, Field, DataType};
/// use codeanatomy_engine::schema::introspection::schema_diff;
///
/// let old_schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
/// ]);
///
/// let new_schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
///     Field::new("email", DataType::Utf8, true),
/// ]);
///
/// let (added, removed, changed) = schema_diff(&old_schema, &new_schema);
/// assert_eq!(added, vec!["email".to_string()]);
/// assert!(removed.is_empty());
/// assert!(changed.is_empty());
/// ```
pub fn schema_diff(old: &Schema, new: &Schema) -> SchemaDiff {
    let old_fields: BTreeMap<String, &Field> = old
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.as_ref()))
        .collect();

    let new_fields: BTreeMap<String, &Field> = new
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.as_ref()))
        .collect();

    let old_names: BTreeSet<String> = old_fields.keys().cloned().collect();
    let new_names: BTreeSet<String> = new_fields.keys().cloned().collect();

    // Added fields (in new but not in old)
    let added: Vec<String> = new_names.difference(&old_names).cloned().collect();

    // Removed fields (in old but not in new)
    let removed: Vec<String> = old_names.difference(&new_names).cloned().collect();

    // Type-changed fields (in both, but different types)
    let mut changed = Vec::new();
    for name in old_names.intersection(&new_names) {
        let old_field = old_fields[name];
        let new_field = new_fields[name];

        if old_field.data_type() != new_field.data_type()
            || old_field.is_nullable() != new_field.is_nullable()
        {
            changed.push((
                name.clone(),
                old_field.data_type().clone(),
                new_field.data_type().clone(),
            ));
        }
    }

    (added, removed, changed)
}

/// Check if schema evolution is additive-only (safe for Delta append).
///
/// Additive evolution means:
/// - No fields were removed
/// - No field types were changed (including nullability)
/// - Only new fields were added
///
/// # Example
///
/// ```
/// use arrow::datatypes::{Schema, Field, DataType};
/// use codeanatomy_engine::schema::introspection::is_additive_evolution;
///
/// let old_schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
/// ]);
///
/// let new_schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
/// ]);
///
/// assert!(is_additive_evolution(&old_schema, &new_schema));
/// ```
pub fn is_additive_evolution(old: &Schema, new: &Schema) -> bool {
    let (_added, removed, changed) = schema_diff(old, new);

    // Evolution is additive if:
    // - No fields were removed
    // - No field types were changed
    // - New fields may have been added (allowed)
    removed.is_empty() && changed.is_empty()
}

/// Generate cast expressions for type-compatible schema alignment.
///
/// Returns a list of (field_name, target_type) tuples representing casts needed
/// to align `source_schema` with `target_schema`.
///
/// Only supports safe, widening casts:
/// - Int32 -> Int64
/// - Float32 -> Float64
/// - Date32 -> Date64
/// - Null -> any nullable type
///
/// Returns an error if:
/// - Fields are missing in source schema
/// - Incompatible type pairs are detected
///
/// # Example
///
/// ```
/// use arrow::datatypes::{Schema, Field, DataType};
/// use codeanatomy_engine::schema::introspection::cast_alignment_exprs;
///
/// let source = Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
/// ]);
///
/// let target = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
/// ]);
///
/// let casts = cast_alignment_exprs(&source, &target).unwrap();
/// assert_eq!(casts.len(), 1);
/// assert_eq!(casts[0].0, "id");
/// assert_eq!(casts[0].1, DataType::Int64);
/// ```
pub fn cast_alignment_exprs(
    source_schema: &Schema,
    target_schema: &Schema,
) -> Result<Vec<(String, DataType)>> {
    let source_fields: BTreeMap<String, &Field> = source_schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.as_ref()))
        .collect();

    let target_fields: BTreeMap<String, &Field> = target_schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.as_ref()))
        .collect();

    let mut cast_exprs = Vec::new();

    for (name, target_field) in &target_fields {
        let source_field = source_fields.get(name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Field '{}' missing in source schema for alignment",
                name
            ))
        })?;

        let source_type = source_field.data_type();
        let target_type = target_field.data_type();

        // Skip if types already match
        if source_type == target_type {
            continue;
        }

        // Check if cast is safe and supported
        if is_safe_cast(source_type, target_type) {
            cast_exprs.push((name.clone(), target_type.clone()));
        } else {
            return Err(DataFusionError::Plan(format!(
                "Incompatible type alignment for field '{}': {:?} -> {:?}",
                name, source_type, target_type
            )));
        }
    }

    Ok(cast_exprs)
}

/// Check if a cast from source type to target type is safe (widening).
fn is_safe_cast(source: &DataType, target: &DataType) -> bool {
    use DataType::*;

    match (source, target) {
        // Exact match
        (s, t) if s == t => true,

        // Null can be cast to any nullable type
        (Null, _) => true,

        // Integer widening
        (Int8, Int16 | Int32 | Int64) => true,
        (Int16, Int32 | Int64) => true,
        (Int32, Int64) => true,
        (UInt8, UInt16 | UInt32 | UInt64) => true,
        (UInt16, UInt32 | UInt64) => true,
        (UInt32, UInt64) => true,

        // Float widening
        (Float16, Float32 | Float64) => true,
        (Float32, Float64) => true,

        // Integer to float (may lose precision, but common in practice)
        (Int8 | Int16 | Int32, Float32 | Float64) => true,
        (Int64, Float64) => true,

        // Date/time widening
        (Date32, Date64) => true,
        (Time32(_), Time64(_)) => true,

        // String types
        (Utf8, LargeUtf8) => true,
        (Binary, LargeBinary) => true,

        // List types (recursive check)
        (List(source_field), List(target_field)) => {
            is_safe_cast(source_field.data_type(), target_field.data_type())
        }
        (List(source_field), LargeList(target_field)) => {
            is_safe_cast(source_field.data_type(), target_field.data_type())
        }

        // All other combinations are considered unsafe
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn test_hash_arrow_schema_deterministic() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let hash1 = hash_arrow_schema(&schema);
        let hash2 = hash_arrow_schema(&schema);

        assert_eq!(hash1, hash2, "Schema hash should be deterministic");
    }

    #[test]
    fn test_hash_arrow_schema_different_for_different_schemas() {
        let schema1 = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let schema2 = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let hash1 = hash_arrow_schema(&schema1);
        let hash2 = hash_arrow_schema(&schema2);

        assert_ne!(
            hash1, hash2,
            "Different schemas should have different hashes"
        );
    }

    #[test]
    fn test_hash_arrow_schema_field_order_matters() {
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let schema2 = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]);

        let hash1 = hash_arrow_schema(&schema1);
        let hash2 = hash_arrow_schema(&schema2);

        assert_ne!(hash1, hash2, "Field order should affect schema hash");
    }

    #[test]
    fn test_schema_diff_added_fields() {
        let old_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let new_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let (added, removed, changed) = schema_diff(&old_schema, &new_schema);

        assert_eq!(added, vec!["name".to_string()]);
        assert!(removed.is_empty());
        assert!(changed.is_empty());
    }

    #[test]
    fn test_schema_diff_removed_fields() {
        let old_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let new_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let (added, removed, changed) = schema_diff(&old_schema, &new_schema);

        assert!(added.is_empty());
        assert_eq!(removed, vec!["name".to_string()]);
        assert!(changed.is_empty());
    }

    #[test]
    fn test_schema_diff_changed_types() {
        let old_schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let new_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let (added, removed, changed) = schema_diff(&old_schema, &new_schema);

        assert!(added.is_empty());
        assert!(removed.is_empty());
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].0, "id");
        assert_eq!(changed[0].1, DataType::Int32);
        assert_eq!(changed[0].2, DataType::Int64);
    }

    #[test]
    fn test_schema_diff_changed_nullability() {
        let old_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let new_schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);

        let (added, removed, changed) = schema_diff(&old_schema, &new_schema);

        assert!(added.is_empty());
        assert!(removed.is_empty());
        assert_eq!(changed.len(), 1, "Nullability change should be detected");
    }

    #[test]
    fn test_is_additive_evolution_true() {
        let old_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let new_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        assert!(is_additive_evolution(&old_schema, &new_schema));
    }

    #[test]
    fn test_is_additive_evolution_false_removed_field() {
        let old_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let new_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        assert!(!is_additive_evolution(&old_schema, &new_schema));
    }

    #[test]
    fn test_is_additive_evolution_false_changed_type() {
        let old_schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let new_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        assert!(!is_additive_evolution(&old_schema, &new_schema));
    }

    #[test]
    fn test_cast_alignment_exprs_int32_to_int64() {
        let source = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let target = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let casts = cast_alignment_exprs(&source, &target).unwrap();

        assert_eq!(casts.len(), 1);
        assert_eq!(casts[0].0, "id");
        assert_eq!(casts[0].1, DataType::Int64);
    }

    #[test]
    fn test_cast_alignment_exprs_float32_to_float64() {
        let source = Schema::new(vec![Field::new("value", DataType::Float32, false)]);

        let target = Schema::new(vec![Field::new("value", DataType::Float64, false)]);

        let casts = cast_alignment_exprs(&source, &target).unwrap();

        assert_eq!(casts.len(), 1);
        assert_eq!(casts[0].0, "value");
        assert_eq!(casts[0].1, DataType::Float64);
    }

    #[test]
    fn test_cast_alignment_exprs_no_casts_needed() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let casts = cast_alignment_exprs(&schema, &schema).unwrap();

        assert!(
            casts.is_empty(),
            "No casts should be needed for identical schemas"
        );
    }

    #[test]
    fn test_cast_alignment_exprs_missing_field() {
        let source = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let target = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let result = cast_alignment_exprs(&source, &target);

        assert!(result.is_err(), "Should error on missing source field");
    }

    #[test]
    fn test_cast_alignment_exprs_incompatible_types() {
        let source = Schema::new(vec![Field::new("id", DataType::Utf8, false)]);

        let target = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let result = cast_alignment_exprs(&source, &target);

        assert!(result.is_err(), "Should error on incompatible types");
    }

    #[test]
    fn test_is_safe_cast_integer_widening() {
        assert!(is_safe_cast(&DataType::Int8, &DataType::Int16));
        assert!(is_safe_cast(&DataType::Int16, &DataType::Int32));
        assert!(is_safe_cast(&DataType::Int32, &DataType::Int64));
        assert!(!is_safe_cast(&DataType::Int64, &DataType::Int32));
    }

    #[test]
    fn test_is_safe_cast_float_widening() {
        assert!(is_safe_cast(&DataType::Float16, &DataType::Float32));
        assert!(is_safe_cast(&DataType::Float32, &DataType::Float64));
        assert!(!is_safe_cast(&DataType::Float64, &DataType::Float32));
    }

    #[test]
    fn test_is_safe_cast_int_to_float() {
        assert!(is_safe_cast(&DataType::Int32, &DataType::Float64));
        assert!(is_safe_cast(&DataType::Int64, &DataType::Float64));
    }

    #[test]
    fn test_is_safe_cast_date_widening() {
        assert!(is_safe_cast(&DataType::Date32, &DataType::Date64));
        assert!(!is_safe_cast(&DataType::Date64, &DataType::Date32));
    }

    #[test]
    fn test_is_safe_cast_time_widening() {
        assert!(is_safe_cast(
            &DataType::Time32(TimeUnit::Second),
            &DataType::Time64(TimeUnit::Microsecond)
        ));
    }

    #[test]
    fn test_is_safe_cast_string_widening() {
        assert!(is_safe_cast(&DataType::Utf8, &DataType::LargeUtf8));
        assert!(is_safe_cast(&DataType::Binary, &DataType::LargeBinary));
    }

    #[test]
    fn test_is_safe_cast_null_to_any() {
        assert!(is_safe_cast(&DataType::Null, &DataType::Int64));
        assert!(is_safe_cast(&DataType::Null, &DataType::Utf8));
    }
}
