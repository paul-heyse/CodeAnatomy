//! DataFusion API compatibility shims for CodeAnatomy UDFs.
//!
//! Targeted at DataFusion 51.x. Update these re-exports and helpers when
//! upgrading DataFusion to reduce churn across UDF modules.

pub use datafusion_expr::{
    ColumnarValue,
    Documentation,
    Expr,
    ReturnFieldArgs,
    ScalarFunctionArgs,
    ScalarUDF,
    ScalarUDFImpl,
    Signature,
    TypeSignature,
    Volatility,
};
