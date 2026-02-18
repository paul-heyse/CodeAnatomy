# datafusion_functions

 Function packages for [DataFusion].

 This crate contains a collection of various function packages for DataFusion,
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

 To access and use only the functions in a certain package, use the
 `functions()` method in each module.

 ```
 # fn main() -> datafusion_common::Result<()> {
 # let mut registry = datafusion_execution::registry::MemoryFunctionRegistry::new();
 # use datafusion_execution::FunctionRegistry;
 // get the encoding functions
 use datafusion_functions::encoding;
 for udf in encoding::functions() {
   registry.register_udf(udf)?;
 }
 # Ok(())
 # }
 ```

 Each package also exports an `expr_fn` submodule to help create [`Expr`]s that invoke
 functions using a fluent style. For example:

 ```
 // create an Expr that will invoke the encode function
 use datafusion_expr::{col, lit};
 use datafusion_functions::expr_fn;
 // Equivalent to "encode(my_data, 'hex')" in SQL:
 let expr = expr_fn::encode(col("my_data"), lit("hex"));
 ```

[`Expr`]: datafusion_expr::Expr

 # Implementing A New Package

 To add a new package to this crate, you should follow the model of existing
 packages. The high level steps are:

 1. Create a new module with the appropriate [`ScalarUDF`] implementations.

 2. Use the macros in [`macros`] to create standard entry points.

 3. Add a new feature to `Cargo.toml`, with any optional dependencies

 4. Use the `make_package!` macro to expose the module when the
    feature is enabled.

 [`ScalarUDF`]: datafusion_expr::ScalarUDF

## Modules

### [`datafusion_functions`](datafusion_functions.md)

*13 modules, 2 functions, 7 macros*

### [`core`](core.md)

*17 functions, 19 modules*

### [`core::arrow_cast`](core/arrow_cast.md)

*1 struct*

### [`core::arrow_metadata`](core/arrow_metadata.md)

*1 struct*

### [`core::arrowtypeof`](core/arrowtypeof.md)

*1 struct*

### [`core::coalesce`](core/coalesce.md)

*1 struct*

### [`core::expr_ext`](core/expr_ext.md)

*1 trait*

### [`core::expr_fn`](core/expr_fn.md)

*16 functions*

### [`core::getfield`](core/getfield.md)

*1 struct*

### [`core::greatest`](core/greatest.md)

*1 struct*

### [`core::least`](core/least.md)

*1 struct*

### [`core::named_struct`](core/named_struct.md)

*1 struct*

### [`core::nullif`](core/nullif.md)

*1 struct*

### [`core::nvl`](core/nvl.md)

*1 struct*

### [`core::nvl2`](core/nvl2.md)

*1 struct*

### [`core::overlay`](core/overlay.md)

*1 struct*

### [`core::planner`](core/planner.md)

*1 struct*

### [`core::struct`](core/struct.md)

*1 struct*

### [`core::union_extract`](core/union_extract.md)

*1 struct*

### [`core::union_tag`](core/union_tag.md)

*1 struct*

### [`core::version`](core/version.md)

*1 struct*

### [`crypto`](crypto.md)

*5 modules, 7 functions*

### [`crypto::basic`](crypto/basic.md)

*1 enum, 9 functions*

### [`crypto::digest`](crypto/digest.md)

*1 struct*

### [`crypto::expr_fn`](crypto/expr_fn.md)

*6 functions*

### [`crypto::md5`](crypto/md5.md)

*1 struct*

### [`crypto::sha`](crypto/sha.md)

*1 struct*

### [`datetime`](datetime.md)

*18 modules, 20 functions*

### [`datetime::current_date`](datetime/current_date.md)

*1 struct*

### [`datetime::current_time`](datetime/current_time.md)

*1 struct*

### [`datetime::date_bin`](datetime/date_bin.md)

*1 struct*

### [`datetime::date_part`](datetime/date_part.md)

*1 struct*

### [`datetime::date_trunc`](datetime/date_trunc.md)

*1 struct*

### [`datetime::expr_fn`](datetime/expr_fn.md)

*19 functions*

### [`datetime::from_unixtime`](datetime/from_unixtime.md)

*1 struct*

### [`datetime::make_date`](datetime/make_date.md)

*1 struct*

### [`datetime::make_time`](datetime/make_time.md)

*1 struct*

### [`datetime::now`](datetime/now.md)

*1 struct*

### [`datetime::planner`](datetime/planner.md)

*1 struct*

### [`datetime::to_char`](datetime/to_char.md)

*1 struct*

### [`datetime::to_date`](datetime/to_date.md)

*1 struct*

### [`datetime::to_local_time`](datetime/to_local_time.md)

*1 struct*

### [`datetime::to_time`](datetime/to_time.md)

*1 struct*

### [`datetime::to_timestamp`](datetime/to_timestamp.md)

*5 structs*

### [`datetime::to_unixtime`](datetime/to_unixtime.md)

*1 struct*

### [`encoding`](encoding.md)

*2 modules, 3 functions*

### [`encoding::expr_fn`](encoding/expr_fn.md)

*2 functions*

### [`encoding::inner`](encoding/inner.md)

*2 structs*

### [`math`](math.md)

*20 modules, 39 functions*

### [`math::abs`](math/abs.md)

*1 struct*

### [`math::ceil`](math/ceil.md)

*1 struct*

### [`math::cot`](math/cot.md)

*1 struct*

### [`math::expr_fn`](math/expr_fn.md)

*38 functions*

### [`math::factorial`](math/factorial.md)

*1 struct*

### [`math::floor`](math/floor.md)

*1 struct*

### [`math::gcd`](math/gcd.md)

*1 function, 1 struct*

### [`math::iszero`](math/iszero.md)

*1 struct*

### [`math::lcm`](math/lcm.md)

*1 struct*

### [`math::log`](math/log.md)

*1 struct*

### [`math::monotonicity`](math/monotonicity.md)

*44 functions*

### [`math::nans`](math/nans.md)

*1 struct*

### [`math::nanvl`](math/nanvl.md)

*1 struct*

### [`math::pi`](math/pi.md)

*1 struct*

### [`math::power`](math/power.md)

*1 struct*

### [`math::random`](math/random.md)

*1 struct*

### [`math::round`](math/round.md)

*1 struct*

### [`math::signum`](math/signum.md)

*1 struct*

### [`math::trunc`](math/trunc.md)

*1 struct*

### [`planner`](planner.md)

*1 struct*

### [`regex`](regex.md)

*6 modules, 8 functions*

### [`regex::expr_fn`](regex/expr_fn.md)

*5 functions*

### [`regex::regexpcount`](regex/regexpcount.md)

*1 function, 1 struct*

### [`regex::regexpinstr`](regex/regexpinstr.md)

*1 function, 1 struct*

### [`regex::regexplike`](regex/regexplike.md)

*1 function, 1 struct*

### [`regex::regexpmatch`](regex/regexpmatch.md)

*1 function, 1 struct*

### [`regex::regexpreplace`](regex/regexpreplace.md)

*1 function, 1 struct*

### [`string`](string.md)

*21 functions, 23 modules*

### [`string::ascii`](string/ascii.md)

*1 function, 1 struct*

### [`string::bit_length`](string/bit_length.md)

*1 struct*

### [`string::btrim`](string/btrim.md)

*1 struct*

### [`string::chr`](string/chr.md)

*1 struct*

### [`string::concat`](string/concat.md)

*1 struct*

### [`string::concat_ws`](string/concat_ws.md)

*1 struct*

### [`string::contains`](string/contains.md)

*1 struct*

### [`string::ends_with`](string/ends_with.md)

*1 struct*

### [`string::expr_fn`](string/expr_fn.md)

*21 functions*

### [`string::levenshtein`](string/levenshtein.md)

*1 struct*

### [`string::lower`](string/lower.md)

*1 struct*

### [`string::ltrim`](string/ltrim.md)

*1 struct*

### [`string::octet_length`](string/octet_length.md)

*1 struct*

### [`string::repeat`](string/repeat.md)

*1 struct*

### [`string::replace`](string/replace.md)

*1 struct*

### [`string::rtrim`](string/rtrim.md)

*1 struct*

### [`string::split_part`](string/split_part.md)

*1 struct*

### [`string::starts_with`](string/starts_with.md)

*1 struct*

### [`string::to_hex`](string/to_hex.md)

*1 struct*

### [`string::upper`](string/upper.md)

*1 struct*

### [`string::uuid`](string/uuid.md)

*1 struct*

### [`strings`](strings.md)

*1 enum, 1 function, 3 structs*

### [`unicode`](unicode.md)

*14 functions, 14 modules*

### [`unicode::character_length`](unicode/character_length.md)

*1 struct*

### [`unicode::expr_fn`](unicode/expr_fn.md)

*17 functions*

### [`unicode::find_in_set`](unicode/find_in_set.md)

*1 struct*

### [`unicode::initcap`](unicode/initcap.md)

*1 struct*

### [`unicode::left`](unicode/left.md)

*1 struct*

### [`unicode::lpad`](unicode/lpad.md)

*1 struct*

### [`unicode::planner`](unicode/planner.md)

*1 struct*

### [`unicode::reverse`](unicode/reverse.md)

*1 struct*

### [`unicode::right`](unicode/right.md)

*1 struct*

### [`unicode::rpad`](unicode/rpad.md)

*1 struct*

### [`unicode::strpos`](unicode/strpos.md)

*1 struct*

### [`unicode::substr`](unicode/substr.md)

*1 struct*

### [`unicode::substrindex`](unicode/substrindex.md)

*1 struct*

### [`unicode::translate`](unicode/translate.md)

*1 struct*

### [`utils`](utils.md)

*6 functions*

