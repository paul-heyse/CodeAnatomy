#[allow(unused_macros)]
macro_rules! unary_bool_expr_wrapper {
    ($ty:ident, $name:literal) => {
        #[pyclass(frozen, name = $name, module = "datafusion.expr", subclass)]
        #[derive(Clone, Debug)]
        pub struct $ty {
            expr: datafusion::logical_expr::Expr,
        }

        impl $ty {
            pub fn new(expr: datafusion::logical_expr::Expr) -> Self {
                Self { expr }
            }
        }

        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}\n            Expr: {}", $name, &self.expr)
            }
        }

        #[pymethods]
        impl $ty {
            fn expr(&self) -> pyo3::prelude::PyResult<super::PyExpr> {
                Ok(self.expr.clone().into())
            }
        }
    };
}

#[allow(unused_macros)]
macro_rules! logical_plan_wrapper {
    (
        wrapper = $wrapper:ident,
        inner = $inner:ty,
        field = $field:ident,
        py_name = $py_name:literal,
        display = |$display_self:ident, $display_f:ident| $display:block,
        inputs = |$inputs_self:ident| $inputs:block
    ) => {
        #[pyclass(frozen, name = $py_name, module = "datafusion.expr", subclass)]
        #[derive(Clone, Debug)]
        pub struct $wrapper {
            $field: $inner,
        }

        impl From<$inner> for $wrapper {
            fn from(inner: $inner) -> Self {
                Self { $field: inner }
            }
        }

        impl From<$wrapper> for $inner {
            fn from(wrapper: $wrapper) -> Self {
                wrapper.$field
            }
        }

        impl std::fmt::Display for $wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                let $display_self = self;
                let $display_f = f;
                $display
            }
        }

        impl crate::expr::logical_node::LogicalNode for $wrapper {
            fn inputs(&self) -> Vec<crate::sql::logical::PyLogicalPlan> {
                let $inputs_self = self;
                $inputs
            }

            fn to_variant<'py>(
                &self,
                py: pyo3::Python<'py>,
            ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::PyAny>> {
                use pyo3::IntoPyObjectExt;
                self.clone().into_bound_py_any(py)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn logical_wrapper_macro_is_used_in_target_modules() {
        for source in [
            include_str!("filter.rs"),
            include_str!("projection.rs"),
            include_str!("limit.rs"),
            include_str!("create_view.rs"),
            include_str!("drop_view.rs"),
            include_str!("join.rs"),
            include_str!("union.rs"),
            include_str!("sort.rs"),
        ] {
            assert!(
                source.contains("logical_plan_wrapper!("),
                "expected logical_plan_wrapper! macro usage"
            );
        }
    }
}
