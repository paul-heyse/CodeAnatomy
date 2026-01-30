use datafusion_expr::WindowUDF;
use datafusion_functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion_functions_window::row_number::row_number_udwf;

use crate::{macros::WindowUdfSpec, window_udfs};

pub fn builtin_udwfs() -> Vec<WindowUDF> {
    builtin_udwf_specs()
        .into_iter()
        .map(|spec| {
            let mut udwf = (spec.builder)();
            if !spec.aliases.is_empty() {
                udwf = udwf.with_aliases(spec.aliases.iter().copied());
            }
            udwf
        })
        .collect()
}

fn builtin_udwf_specs() -> Vec<WindowUdfSpec> {
    window_udfs![
        "row_number" => row_number_base_udwf, aliases: ["row_number_window", "dedupe_best_by_score"];
        "lag" => lag_base_udwf, aliases: ["lag_window"];
        "lead" => lead_base_udwf, aliases: ["lead_window"];
    ]
}

fn row_number_base_udwf() -> WindowUDF {
    row_number_udwf().as_ref().clone()
}

fn lag_base_udwf() -> WindowUDF {
    lag_udwf().as_ref().clone()
}

fn lead_base_udwf() -> WindowUDF {
    lead_udwf().as_ref().clone()
}
