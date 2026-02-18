# datafusion_functions_aggregate

 Aggregate Function packages for [DataFusion].

 This crate contains a collection of various aggregate function packages for DataFusion,
 implemented using the extension API. Users may wish to control which functions
 are available to control the binary size of their application as well as
 use dialect specific implementations of functions (e.g. Spark vs Postgres)

 Each package is implemented as a separate
 module, activated by a feature flag.

 [DataFusion]: https://crates.io/crates/datafusion

 # Available Packages
 See the list of [modules](#modules) in this crate for available packages.

 # Using A Package
 You can register all functions in all packages using the [`register_all`] function.

 Each package also exports an `expr_fn` submodule to help create [`Expr`]s that invoke
 functions using a fluent style. For example:

[`Expr`]: datafusion_expr::Expr

 # Implementing A New Package

 To add a new package to this crate, you should follow the model of existing
 packages. The high level steps are:

 1. Create a new module with the appropriate [AggregateUDF] implementations.

 2. Use the macros in [`macros`] to create standard entry points.

 3. Add a new feature to `Cargo.toml`, with any optional dependencies

 4. Use the `make_package!` macro to expose the module when the
    feature is enabled.

## Modules

### [`datafusion_functions_aggregate`](datafusion_functions_aggregate.md)

*2 functions, 26 modules, 3 macros*

### [`approx_distinct`](approx_distinct.md)

*1 struct, 2 functions*

### [`approx_median`](approx_median.md)

*1 struct, 2 functions*

### [`approx_percentile_cont`](approx_percentile_cont.md)

*2 functions, 2 structs*

### [`approx_percentile_cont_with_weight`](approx_percentile_cont_with_weight.md)

*2 functions, 2 structs*

### [`array_agg`](array_agg.md)

*2 functions, 2 structs*

### [`average`](average.md)

*2 structs, 3 functions*

### [`bit_and_or_xor`](bit_and_or_xor.md)

*6 functions*

### [`bool_and_or`](bool_and_or.md)

*2 structs, 4 functions*

### [`correlation`](correlation.md)

*2 functions, 3 structs*

### [`count`](count.md)

*2 structs, 5 functions*

### [`covariance`](covariance.md)

*3 structs, 4 functions*

### [`first_last`](first_last.md)

*4 functions, 5 structs*

### [`grouping`](grouping.md)

*1 struct, 2 functions*

### [`median`](median.md)

*1 struct, 2 functions*

### [`min_max`](min_max.md)

*4 functions, 6 structs*

### [`nth_value`](nth_value.md)

*2 functions, 3 structs*

### [`percentile_cont`](percentile_cont.md)

*1 struct, 2 functions*

### [`planner`](planner.md)

*1 struct*

### [`regr`](regr.md)

*1 enum, 18 functions, 2 structs*

### [`stddev`](stddev.md)

*4 functions, 4 structs*

### [`string_agg`](string_agg.md)

*1 struct, 2 functions*

### [`sum`](sum.md)

*2 structs, 3 functions*

### [`variance`](variance.md)

*4 functions, 4 structs*

