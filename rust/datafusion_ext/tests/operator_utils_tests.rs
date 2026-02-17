use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::DFSchema;
use datafusion_expr::expr_fn::col;
use datafusion_expr::sqlparser::ast::BinaryOperator;
use datafusion_expr::Operator;
use datafusion_ext::operator_utils::{
    can_use_get_field, is_arrow_binary_operator, is_arrow_operator,
};

#[test]
fn arrow_operator_classification_matches_known_operators() {
    assert!(is_arrow_binary_operator(&BinaryOperator::Arrow));
    assert!(is_arrow_operator(Operator::LongArrow));
    assert!(!is_arrow_operator(Operator::Plus));
}

#[test]
fn can_use_get_field_for_struct_inputs() {
    let schema = Schema::new(vec![Field::new(
        "payload",
        DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into()),
        true,
    )]);
    let df_schema = DFSchema::try_from(schema).expect("df schema");
    let expr = col("payload");
    assert!(can_use_get_field(&expr, &df_schema).expect("type check"));
}

