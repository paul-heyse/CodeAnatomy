use datafusion_expr::WindowUDF;
use datafusion_functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion_functions_window::row_number::row_number_udwf;

pub fn builtin_udwfs() -> Vec<WindowUDF> {
    vec![
        row_number_udwf()
            .as_ref()
            .clone()
            .with_aliases(["row_number_window"]),
        lag_udwf().as_ref().clone().with_aliases(["lag_window"]),
        lead_udwf().as_ref().clone().with_aliases(["lead_window"]),
    ]
}
