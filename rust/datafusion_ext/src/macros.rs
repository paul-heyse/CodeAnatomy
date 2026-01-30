use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::execution::context::SessionContext;
use datafusion_common::Result;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};

#[derive(Clone, Copy)]
pub struct ScalarUdfSpec {
    pub name: &'static str,
    pub builder: fn() -> ScalarUDF,
    pub aliases: &'static [&'static str],
}

#[derive(Clone, Copy)]
pub struct AggregateUdfSpec {
    pub name: &'static str,
    pub builder: fn() -> AggregateUDF,
    pub aliases: &'static [&'static str],
}

#[derive(Clone, Copy)]
pub struct WindowUdfSpec {
    pub name: &'static str,
    pub builder: fn() -> WindowUDF,
    pub aliases: &'static [&'static str],
}

#[derive(Clone, Copy)]
pub struct TableUdfSpec {
    pub name: &'static str,
    pub builder: fn(&SessionContext) -> Result<Arc<dyn TableFunctionImpl>>,
    pub aliases: &'static [&'static str],
}

#[macro_export]
macro_rules! scalar_udfs {
    (
        $( $name:expr => $builder:path $(, aliases: [$($alias:expr),* $(,)?])? );* $(;)?
    ) => {
        vec![
            $(
                $crate::macros::ScalarUdfSpec {
                    name: $name,
                    builder: $builder,
                    aliases: &[$($($alias),*)?],
                }
            ),*
        ]
    };
}

#[macro_export]
macro_rules! aggregate_udfs {
    (
        $( $name:expr => $builder:path $(, aliases: [$($alias:expr),* $(,)?])? );* $(;)?
    ) => {
        vec![
            $(
                $crate::macros::AggregateUdfSpec {
                    name: $name,
                    builder: $builder,
                    aliases: &[$($($alias),*)?],
                }
            ),*
        ]
    };
}

#[macro_export]
macro_rules! window_udfs {
    (
        $( $name:expr => $builder:path $(, aliases: [$($alias:expr),* $(,)?])? );* $(;)?
    ) => {
        vec![
            $(
                $crate::macros::WindowUdfSpec {
                    name: $name,
                    builder: $builder,
                    aliases: &[$($($alias),*)?],
                }
            ),*
        ]
    };
}

#[macro_export]
macro_rules! table_udfs {
    (
        $( $name:expr => $builder:path $(, aliases: [$($alias:expr),* $(,)?])? );* $(;)?
    ) => {
        vec![
            $(
                $crate::macros::TableUdfSpec {
                    name: $name,
                    builder: $builder,
                    aliases: &[$($($alias),*)?],
                }
            ),*
        ]
    };
}
