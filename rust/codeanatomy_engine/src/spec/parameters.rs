//! Typed parameter bindings for parametric planning (WS-P3).
//!
//! Replaces env-var string templating with typed bindings that compile
//! directly to `Expr` nodes. Placeholder bindings are positional-only
//! (`$1`, `$2`, ...) and compile to `ParamValues::List`.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion_common::{plan_err, Result};
use serde::{Deserialize, Serialize};

/// A typed parameter binding for parametric plan compilation.
///
/// Each parameter specifies a target (positional placeholder or direct
/// filter binding) and a typed value. The optional label is diagnostic
/// only and does not affect binding identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedParameter {
    /// Optional diagnostic label (not used for binding identity).
    pub label: Option<String>,
    /// Parameter target: positional placeholder or direct filter binding.
    pub target: ParameterTarget,
    /// Typed value.
    pub value: ParameterValue,
}

/// Target for a typed parameter binding.
///
/// Two strategies:
/// - `PlaceholderPos`: positional placeholder binding (`$1`, `$2`, ...)
/// - `FilterEq`: direct typed filter expression (no SQL literal interpolation)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ParameterTarget {
    /// Positional placeholder binding: `$1`, `$2`, ...
    PlaceholderPos { position: u32 },
    /// Direct typed filter expression (no SQL literal interpolation).
    FilterEq {
        base_table: Option<String>,
        filter_column: String,
    },
}

/// Typed parameter value with variant-specific serialization.
///
/// Each variant maps to a DataFusion `ScalarValue` via `to_scalar_value()`.
/// Date and Timestamp variants carry ISO 8601 string representations that
/// are parsed at compilation time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum ParameterValue {
    Utf8(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    /// ISO 8601 date string ("YYYY-MM-DD").
    Date(String),
    /// ISO 8601 timestamp string.
    Timestamp(String),
    /// Typed null with explicit data type name.
    Null { data_type: String },
}

impl ParameterValue {
    /// Convert to DataFusion `ScalarValue`.
    ///
    /// Each variant maps directly:
    /// - Utf8 -> ScalarValue::Utf8
    /// - Int64 -> ScalarValue::Int64
    /// - Float64 -> ScalarValue::Float64
    /// - Boolean -> ScalarValue::Boolean
    /// - Date -> ScalarValue::Date32 (days since epoch)
    /// - Timestamp -> ScalarValue::TimestampMicrosecond (microseconds since epoch)
    /// - Null -> ScalarValue::try_from(&DataType)
    pub fn to_scalar_value(&self) -> Result<ScalarValue> {
        match self {
            Self::Utf8(v) => Ok(ScalarValue::Utf8(Some(v.clone()))),
            Self::Int64(v) => Ok(ScalarValue::Int64(Some(*v))),
            Self::Float64(v) => Ok(ScalarValue::Float64(Some(*v))),
            Self::Boolean(v) => Ok(ScalarValue::Boolean(Some(*v))),
            Self::Date(v) => Ok(ScalarValue::Date32(Some(parse_date32(v)?))),
            Self::Timestamp(v) => Ok(ScalarValue::TimestampMicrosecond(
                Some(parse_timestamp_micros(v)?),
                None,
            )),
            Self::Null { data_type } => {
                let dt = parse_data_type(data_type)?;
                Ok(ScalarValue::try_from(&dt)?)
            }
        }
    }
}

/// Parse an ISO 8601 date string ("YYYY-MM-DD") to days since Unix epoch.
///
/// Uses chrono for reliable date parsing.
fn parse_date32(s: &str) -> Result<i32> {
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(|e| {
        datafusion_common::DataFusionError::Plan(format!(
            "Invalid Date parameter '{}': expected YYYY-MM-DD format. Parse error: {}",
            s, e
        ))
    })?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = date.signed_duration_since(epoch).num_days();
    i32::try_from(days).map_err(|_| {
        datafusion_common::DataFusionError::Plan(format!(
            "Date '{}' is out of range for Date32 (days since epoch: {})",
            s, days
        ))
    })
}

/// Parse an ISO 8601 timestamp string to microseconds since Unix epoch.
///
/// Supports formats:
/// - "YYYY-MM-DDThh:mm:ss" (no fractional seconds)
/// - "YYYY-MM-DDThh:mm:ss.f" (fractional seconds)
/// - "YYYY-MM-DD hh:mm:ss" (space separator)
fn parse_timestamp_micros(s: &str) -> Result<i64> {
    // Try parsing with chrono NaiveDateTime (no timezone)
    let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| {
            datafusion_common::DataFusionError::Plan(format!(
                "Invalid Timestamp parameter '{}': expected ISO 8601 format. Parse error: {}",
                s, e
            ))
        })?;

    Ok(dt.and_utc().timestamp_micros())
}

/// Map a type name string to a DataFusion `DataType`.
///
/// Supports common type names used in the CodeAnatomy spec contract:
/// - "utf8" / "string" -> DataType::Utf8
/// - "int64" / "long" -> DataType::Int64
/// - "int32" / "int" -> DataType::Int32
/// - "float64" / "double" -> DataType::Float64
/// - "float32" / "float" -> DataType::Float32
/// - "boolean" / "bool" -> DataType::Boolean
/// - "date32" / "date" -> DataType::Date32
/// - "timestamp" -> DataType::Timestamp(Microsecond, None)
fn parse_data_type(s: &str) -> Result<DataType> {
    match s.trim().to_ascii_lowercase().as_str() {
        "utf8" | "string" => Ok(DataType::Utf8),
        "int64" | "long" => Ok(DataType::Int64),
        "int32" | "int" => Ok(DataType::Int32),
        "float64" | "double" => Ok(DataType::Float64),
        "float32" | "float" => Ok(DataType::Float32),
        "boolean" | "bool" => Ok(DataType::Boolean),
        "date32" | "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        other => {
            plan_err!("Unsupported Null data_type '{}' in TypedParameter", other)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_to_scalar() {
        let pv = ParameterValue::Utf8("hello".to_string());
        let sv = pv.to_scalar_value().unwrap();
        assert_eq!(sv, ScalarValue::Utf8(Some("hello".to_string())));
    }

    #[test]
    fn test_int64_to_scalar() {
        let pv = ParameterValue::Int64(42);
        let sv = pv.to_scalar_value().unwrap();
        assert_eq!(sv, ScalarValue::Int64(Some(42)));
    }

    #[test]
    fn test_float64_to_scalar() {
        let pv = ParameterValue::Float64(3.14);
        let sv = pv.to_scalar_value().unwrap();
        assert_eq!(sv, ScalarValue::Float64(Some(3.14)));
    }

    #[test]
    fn test_boolean_to_scalar() {
        let pv = ParameterValue::Boolean(true);
        let sv = pv.to_scalar_value().unwrap();
        assert_eq!(sv, ScalarValue::Boolean(Some(true)));
    }

    #[test]
    fn test_date_to_scalar() {
        // 2024-01-15 is 19737 days since epoch (1970-01-01)
        let pv = ParameterValue::Date("2024-01-15".to_string());
        let sv = pv.to_scalar_value().unwrap();
        if let ScalarValue::Date32(Some(days)) = sv {
            assert_eq!(days, 19737);
        } else {
            panic!("Expected Date32 scalar, got {:?}", sv);
        }
    }

    #[test]
    fn test_date_epoch_to_scalar() {
        let pv = ParameterValue::Date("1970-01-01".to_string());
        let sv = pv.to_scalar_value().unwrap();
        assert_eq!(sv, ScalarValue::Date32(Some(0)));
    }

    #[test]
    fn test_timestamp_to_scalar() {
        let pv = ParameterValue::Timestamp("2024-01-15T10:30:00".to_string());
        let sv = pv.to_scalar_value().unwrap();
        if let ScalarValue::TimestampMicrosecond(Some(micros), None) = sv {
            // 2024-01-15T10:30:00 UTC in micros since epoch
            assert!(micros > 0);
        } else {
            panic!("Expected TimestampMicrosecond scalar, got {:?}", sv);
        }
    }

    #[test]
    fn test_timestamp_with_fractional_to_scalar() {
        let pv = ParameterValue::Timestamp("2024-01-15T10:30:00.123456".to_string());
        let sv = pv.to_scalar_value().unwrap();
        if let ScalarValue::TimestampMicrosecond(Some(_), None) = sv {
            // Valid parse
        } else {
            panic!("Expected TimestampMicrosecond scalar, got {:?}", sv);
        }
    }

    #[test]
    fn test_null_utf8_to_scalar() {
        let pv = ParameterValue::Null {
            data_type: "utf8".to_string(),
        };
        let sv = pv.to_scalar_value().unwrap();
        assert_eq!(sv, ScalarValue::Utf8(None));
    }

    #[test]
    fn test_null_int64_to_scalar() {
        let pv = ParameterValue::Null {
            data_type: "int64".to_string(),
        };
        let sv = pv.to_scalar_value().unwrap();
        assert_eq!(sv, ScalarValue::Int64(None));
    }

    #[test]
    fn test_null_boolean_to_scalar() {
        let pv = ParameterValue::Null {
            data_type: "boolean".to_string(),
        };
        let sv = pv.to_scalar_value().unwrap();
        assert_eq!(sv, ScalarValue::Boolean(None));
    }

    #[test]
    fn test_invalid_date_format() {
        let pv = ParameterValue::Date("not-a-date".to_string());
        assert!(pv.to_scalar_value().is_err());
    }

    #[test]
    fn test_invalid_timestamp_format() {
        let pv = ParameterValue::Timestamp("not-a-timestamp".to_string());
        assert!(pv.to_scalar_value().is_err());
    }

    #[test]
    fn test_unsupported_null_data_type() {
        let pv = ParameterValue::Null {
            data_type: "unsupported_type".to_string(),
        };
        assert!(pv.to_scalar_value().is_err());
    }

    #[test]
    fn test_parse_date32_epoch() {
        assert_eq!(parse_date32("1970-01-01").unwrap(), 0);
    }

    #[test]
    fn test_parse_date32_future() {
        let days = parse_date32("2024-01-15").unwrap();
        assert!(days > 0);
    }

    #[test]
    fn test_parse_timestamp_micros_basic() {
        let micros = parse_timestamp_micros("2024-01-15T10:30:00").unwrap();
        assert!(micros > 0);
    }

    #[test]
    fn test_parse_data_type_coverage() {
        assert_eq!(parse_data_type("utf8").unwrap(), DataType::Utf8);
        assert_eq!(parse_data_type("string").unwrap(), DataType::Utf8);
        assert_eq!(parse_data_type("int64").unwrap(), DataType::Int64);
        assert_eq!(parse_data_type("long").unwrap(), DataType::Int64);
        assert_eq!(parse_data_type("int32").unwrap(), DataType::Int32);
        assert_eq!(parse_data_type("int").unwrap(), DataType::Int32);
        assert_eq!(parse_data_type("float64").unwrap(), DataType::Float64);
        assert_eq!(parse_data_type("double").unwrap(), DataType::Float64);
        assert_eq!(parse_data_type("float32").unwrap(), DataType::Float32);
        assert_eq!(parse_data_type("float").unwrap(), DataType::Float32);
        assert_eq!(parse_data_type("boolean").unwrap(), DataType::Boolean);
        assert_eq!(parse_data_type("bool").unwrap(), DataType::Boolean);
        assert_eq!(parse_data_type("date32").unwrap(), DataType::Date32);
        assert_eq!(parse_data_type("date").unwrap(), DataType::Date32);
        assert!(parse_data_type("unknown").is_err());
    }

    #[test]
    fn test_typed_parameter_serialization_roundtrip() {
        let param = TypedParameter {
            label: Some("test_param".to_string()),
            target: ParameterTarget::PlaceholderPos { position: 1 },
            value: ParameterValue::Int64(42),
        };

        let json = serde_json::to_string(&param).unwrap();
        let deserialized: TypedParameter = serde_json::from_str(&json).unwrap();

        if let ParameterTarget::PlaceholderPos { position } = deserialized.target {
            assert_eq!(position, 1);
        } else {
            panic!("Expected PlaceholderPos target");
        }
    }

    #[test]
    fn test_filter_eq_serialization_roundtrip() {
        let param = TypedParameter {
            label: None,
            target: ParameterTarget::FilterEq {
                base_table: Some("input_table".to_string()),
                filter_column: "repo_root".to_string(),
            },
            value: ParameterValue::Utf8("/path/to/repo".to_string()),
        };

        let json = serde_json::to_string(&param).unwrap();
        let deserialized: TypedParameter = serde_json::from_str(&json).unwrap();

        if let ParameterTarget::FilterEq {
            base_table,
            filter_column,
        } = &deserialized.target
        {
            assert_eq!(base_table.as_deref(), Some("input_table"));
            assert_eq!(filter_column, "repo_root");
        } else {
            panic!("Expected FilterEq target");
        }
    }

    #[test]
    fn test_null_variant_serialization() {
        let param = TypedParameter {
            label: None,
            target: ParameterTarget::PlaceholderPos { position: 1 },
            value: ParameterValue::Null {
                data_type: "utf8".to_string(),
            },
        };

        let json = serde_json::to_string(&param).unwrap();
        assert!(json.contains("\"type\":\"Null\""));

        let deserialized: TypedParameter = serde_json::from_str(&json).unwrap();
        if let ParameterValue::Null { data_type } = &deserialized.value {
            assert_eq!(data_type, "utf8");
        } else {
            panic!("Expected Null value variant");
        }
    }
}
