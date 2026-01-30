#[macro_export]
macro_rules! impl_error_from {
    ($target:ty, $source:ty, $variant:ident) => {
        impl From<$source> for $target {
            fn from(err: $source) -> Self {
                Self::$variant(err)
            }
        }
    };
    ($target:ty, $source:ty, $variant:ident, |$err:ident| $transform:expr) => {
        impl From<$source> for $target {
            fn from($err: $source) -> Self {
                Self::$variant($transform)
            }
        }
    };
}
