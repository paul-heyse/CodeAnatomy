use arrow::datatypes::DataType;
use datafusion_common::{DFSchema, Result};
use datafusion_expr::sqlparser::ast::BinaryOperator;
use datafusion_expr::{Expr, ExprSchemable, Operator};

pub fn is_arrow_binary_operator(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Arrow
            | BinaryOperator::LongArrow
            | BinaryOperator::HashArrow
            | BinaryOperator::HashLongArrow
    )
}

pub fn is_arrow_operator(op: Operator) -> bool {
    matches!(
        op,
        Operator::Arrow | Operator::LongArrow | Operator::HashArrow | Operator::HashLongArrow
    )
}

pub fn can_use_get_field(expr: &Expr, schema: &DFSchema) -> Result<bool> {
    match expr.get_type(schema)? {
        DataType::Struct(_) | DataType::Map(_, _) | DataType::Null => Ok(true),
        _ => Ok(false),
    }
}
