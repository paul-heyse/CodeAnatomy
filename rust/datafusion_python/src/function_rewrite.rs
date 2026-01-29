use arrow::datatypes::DataType;
use datafusion_common::{
    config::ConfigOptions, tree_node::Transformed, utils::list_ndims, DFSchema, Result,
};
use datafusion_expr::expr::{BinaryExpr, ScalarFunction};
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{Expr, ExprSchemable, Operator};
use datafusion_functions::core::get_field as get_field_udf;
use datafusion_functions_nested::expr_fn::array_has_all;

#[derive(Debug, Default)]
pub struct CodeAnatomyOperatorRewrite;

impl FunctionRewrite for CodeAnatomyOperatorRewrite {
    fn name(&self) -> &str {
        "codeanatomy_operator_rewrite"
    }

    fn rewrite(
        &self,
        expr: Expr,
        schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let Expr::BinaryExpr(binary_expr) = expr else {
            return Ok(Transformed::no(expr));
        };
        let BinaryExpr { left, op, right } = binary_expr;
        let left_expr = *left;
        let right_expr = *right;

        if is_arrow_operator(op) && can_use_get_field(&left_expr, schema)? {
            let planned = Expr::ScalarFunction(ScalarFunction::new_udf(
                get_field_udf(),
                vec![left_expr, right_expr],
            ));
            return Ok(Transformed::yes(planned));
        }

        if matches!(op, Operator::AtArrow | Operator::ArrowAt) {
            let left_ndims = list_ndims(&left_expr.get_type(schema)?);
            let right_ndims = list_ndims(&right_expr.get_type(schema)?);
            if left_ndims > 0 && right_ndims > 0 {
                let planned = if op == Operator::AtArrow {
                    array_has_all(left_expr, right_expr)
                } else {
                    array_has_all(right_expr, left_expr)
                };
                return Ok(Transformed::yes(planned));
            }
        }

        Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left_expr),
            op,
            Box::new(right_expr),
        ))))
    }
}

fn is_arrow_operator(op: Operator) -> bool {
    matches!(
        op,
        Operator::Arrow | Operator::LongArrow | Operator::HashArrow | Operator::HashLongArrow
    )
}

fn can_use_get_field(expr: &Expr, schema: &DFSchema) -> Result<bool> {
    match expr.get_type(schema)? {
        DataType::Struct(_) | DataType::Map(_, _) | DataType::Null => Ok(true),
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::expr::BinaryExpr;
    use datafusion_expr::expr_fn::col;

    #[test]
    fn rewrite_arrow_to_get_field() -> Result<()> {
        let schema = Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into()),
            true,
        )]);
        let df_schema = DFSchema::try_from(schema)?;
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("payload")),
            Operator::Arrow,
            Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("name".to_string())),
                None,
            )),
        ));
        let rewrite = CodeAnatomyOperatorRewrite::default();
        let transformed = rewrite.rewrite(expr, &df_schema, &ConfigOptions::new())?;
        assert!(transformed.transformed);
        let Expr::ScalarFunction(scalar) = transformed.data else {
            panic!("expected scalar function rewrite");
        };
        assert_eq!(scalar.func.name(), "get_field");
        Ok(())
    }
}
