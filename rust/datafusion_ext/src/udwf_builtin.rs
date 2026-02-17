//! Built-in window UDFs that delegate to DataFusion's native implementations.
//!
//! ## Optimizer Hook Coverage (sort_options / reverse_expr)
//!
//! All UDWFs in this module delegate to DataFusion's built-in `WindowUDFImpl`
//! implementations, which handle optimizer hooks natively:
//!
//! | UDWF         | sort_options | reverse_expr      | Notes                              |
//! |--------------|-------------|-------------------|------------------------------------|
//! | `lag`        | Delegated   | Delegated (lead)  | DataFusion lag <-> lead reversal   |
//! | `lead`       | Delegated   | Delegated (lag)   | DataFusion lead <-> lag reversal   |
//! | `row_number` | N/A         | N/A               | Sequential numbering, no reversal  |
//!
//! Because we clone the built-in UDFs via `*_udwf().as_ref().clone()`, all
//! trait method dispatches (including `sort_options()` and `reverse_expr()`)
//! route through the original DataFusion implementations. No additional
//! wrapping or override is needed.

use datafusion_expr::WindowUDF;
use datafusion_functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion_functions_window::row_number::row_number_udwf;

use crate::function_types::FunctionKind;
use crate::registry::metadata::FunctionMetadata;
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

/// Lag window function, delegated to DataFusion's built-in implementation.
///
/// Optimizer hooks delegated to DataFusion:
/// - `sort_options()`: Returns the ordering requirement for the lag expression.
/// - `reverse_expr()`: Returns the equivalent lead expression for reverse execution.
fn lag_base_udwf() -> WindowUDF {
    lag_udwf().as_ref().clone()
}

/// Lead window function, delegated to DataFusion's built-in implementation.
///
/// Optimizer hooks delegated to DataFusion:
/// - `sort_options()`: Returns the ordering requirement for the lead expression.
/// - `reverse_expr()`: Returns the equivalent lag expression for reverse execution.
fn lead_base_udwf() -> WindowUDF {
    lead_udwf().as_ref().clone()
}

pub(crate) fn function_metadata(name: &str) -> Option<FunctionMetadata> {
    let (rewrite_tags, has_reverse_expr, has_sort_options): (&'static [&'static str], bool, bool) =
        match name {
            "lag_window" => (&["window"], true, true),
            "lead_window" => (&["window"], true, true),
            "row_number_window" => (&["window"], false, false),
            _ => return None,
        };

    Some(FunctionMetadata {
        name: None,
        kind: FunctionKind::Window,
        rewrite_tags,
        has_simplify: false,
        has_coerce_types: false,
        has_short_circuits: false,
        has_groups_accumulator: false,
        has_retract_batch: false,
        has_reverse_expr,
        has_sort_options,
    })
}
