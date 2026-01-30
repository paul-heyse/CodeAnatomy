use datafusion_expr::{lit, Expr};
use datafusion_functions::core::expr_fn as core_expr_fn;
use datafusion_functions_aggregate::expr_fn as agg_expr_fn;
use datafusion_functions_aggregate::string_agg::string_agg as string_agg_fn;
use datafusion_functions_nested::expr_fn as nested_expr_fn;
use datafusion_functions_window::expr_fn as window_expr_fn;

pub fn map_entries(expr: Expr) -> Expr {
    nested_expr_fn::map_entries(expr)
}

pub fn map_keys(expr: Expr) -> Expr {
    nested_expr_fn::map_keys(expr)
}

pub fn map_values(expr: Expr) -> Expr {
    nested_expr_fn::map_values(expr)
}

pub fn map_extract(expr: Expr, key: &str) -> Expr {
    nested_expr_fn::map_extract(expr, lit(key))
}

pub fn list_extract(expr: Expr, index: i64) -> Expr {
    nested_expr_fn::array_element(expr, lit(index))
}

pub fn list_unique(expr: Expr) -> Expr {
    let agg = agg_expr_fn::array_agg(expr);
    nested_expr_fn::array_distinct(agg)
}

pub fn first_value_agg(expr: Expr) -> Expr {
    agg_expr_fn::first_value(expr, Vec::new())
}

pub fn last_value_agg(expr: Expr) -> Expr {
    agg_expr_fn::last_value(expr, Vec::new())
}

pub fn count_distinct_agg(expr: Expr) -> Expr {
    agg_expr_fn::count_distinct(expr)
}

pub fn string_agg(value: Expr, delimiter: Expr) -> Expr {
    string_agg_fn(value, delimiter)
}

pub fn row_number_window(_value: Expr) -> Expr {
    window_expr_fn::row_number()
}

pub fn lag_window(expr: Expr) -> Expr {
    window_expr_fn::lag(expr, None, None)
}

pub fn lead_window(expr: Expr) -> Expr {
    window_expr_fn::lead(expr, None, None)
}

pub fn union_tag(expr: Expr) -> Expr {
    core_expr_fn::union_tag(expr)
}

pub fn union_extract(expr: Expr, tag: &str) -> Expr {
    core_expr_fn::union_extract(expr, tag)
}
