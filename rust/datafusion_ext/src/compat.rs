//! DataFusion API surface re-exports for CodeAnatomy UDFs.
//!
//! Targeted at DataFusion 52.1. Update these re-exports when
//! upstream API surfaces change to reduce churn across UDF modules.

pub use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
