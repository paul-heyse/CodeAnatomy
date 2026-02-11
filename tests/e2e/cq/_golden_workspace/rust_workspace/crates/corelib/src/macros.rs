#[macro_export]
macro_rules! make_metric {
    ($name:ident) => {
        pub fn $name() -> usize {
            1
        }
    };
}
