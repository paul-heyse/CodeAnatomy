use datafusion_common::{utils::list_ndims, DFSchema, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion_expr::sqlparser::ast::BinaryOperator;
use datafusion_expr::{Expr, ExprSchemable};
use datafusion_functions::core::get_field as get_field_udf;
use datafusion_functions_nested::expr_fn::array_has_all;

use crate::operator_utils::{can_use_get_field, is_arrow_binary_operator};

#[derive(Debug, Default)]
pub struct CodeAnatomyDomainPlanner;

impl ExprPlanner for CodeAnatomyDomainPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let RawBinaryExpr { op, left, right } = expr;

        if is_arrow_binary_operator(&op) && can_use_get_field(&left, schema)? {
            let planned =
                Expr::ScalarFunction(ScalarFunction::new_udf(get_field_udf(), vec![left, right]));
            return Ok(PlannerResult::Planned(planned));
        }

        if matches!(op, BinaryOperator::AtArrow | BinaryOperator::ArrowAt) {
            let left_ndims = list_ndims(&left.get_type(schema)?);
            let right_ndims = list_ndims(&right.get_type(schema)?);
            if left_ndims > 0 && right_ndims > 0 {
                let planned = if op == BinaryOperator::AtArrow {
                    array_has_all(left, right)
                } else {
                    array_has_all(right, left)
                };
                return Ok(PlannerResult::Planned(planned));
            }
        }

        Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }))
    }
}
