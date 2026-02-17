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
                write!(
                    f,
                    "{}\n            Expr: {}",
                    $name,
                    &self.expr
                )
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
    ($ty:ident, $plan_ty:ty, $variant:pat => $bind:ident) => {
        #[pyclass(frozen, name = stringify!($ty), module = "datafusion.expr", subclass)]
        #[derive(Clone, Debug)]
        pub struct $ty {
            plan: $plan_ty,
        }

        impl From<$plan_ty> for $ty {
            fn from(plan: $plan_ty) -> Self {
                Self { plan }
            }
        }
    };
}
