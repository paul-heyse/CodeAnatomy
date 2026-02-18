**datafusion_sql > unparser > dialect**

# Module: unparser::dialect

## Contents

**Structs**

- [`BigQueryDialect`](#bigquerydialect)
- [`CustomDialect`](#customdialect)
- [`CustomDialectBuilder`](#customdialectbuilder) - `CustomDialectBuilder` to build `CustomDialect` using builder pattern
- [`DefaultDialect`](#defaultdialect)
- [`DuckDBDialect`](#duckdbdialect)
- [`MySqlDialect`](#mysqldialect)
- [`PostgreSqlDialect`](#postgresqldialect)
- [`SqliteDialect`](#sqlitedialect)

**Enums**

- [`CharacterLengthStyle`](#characterlengthstyle) - `CharacterLengthStyle` to use for unparsing
- [`DateFieldExtractStyle`](#datefieldextractstyle) - Datetime subfield extraction style for unparsing
- [`IntervalStyle`](#intervalstyle) - `IntervalStyle` to use for unparsing

**Traits**

- [`Dialect`](#dialect) - `Dialect` to use for Unparsing

**Type Aliases**

- [`ScalarFnToSqlHandler`](#scalarfntosqlhandler)

---

## datafusion_sql::unparser::dialect::BigQueryDialect

*Struct*

**Methods:**

- `fn new() -> Self`

**Trait Implementations:**

- **Dialect**
  - `fn identifier_quote_style(self: &Self, _: &str) -> Option<char>`
  - `fn col_alias_overrides(self: &Self, alias: &str) -> Result<Option<String>>`
  - `fn unnest_as_table_factor(self: &Self) -> bool`
- **Default**
  - `fn default() -> BigQueryDialect`



## datafusion_sql::unparser::dialect::CharacterLengthStyle

*Enum*

`CharacterLengthStyle` to use for unparsing

Different DBMSs uses different names for function calculating the number of characters in the string
`Length` style uses length(x)
`SQLStandard` style uses character_length(x)

**Variants:**
- `Length`
- `CharacterLength`

**Traits:** Copy

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &CharacterLengthStyle) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> CharacterLengthStyle`



## datafusion_sql::unparser::dialect::CustomDialect

*Struct*

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Dialect**
  - `fn identifier_quote_style(self: &Self, _: &str) -> Option<char>`
  - `fn supports_nulls_first_in_sort(self: &Self) -> bool`
  - `fn use_timestamp_for_date64(self: &Self) -> bool`
  - `fn interval_style(self: &Self) -> IntervalStyle`
  - `fn float64_ast_dtype(self: &Self) -> ast::DataType`
  - `fn utf8_cast_dtype(self: &Self) -> ast::DataType`
  - `fn large_utf8_cast_dtype(self: &Self) -> ast::DataType`
  - `fn date_field_extract_style(self: &Self) -> DateFieldExtractStyle`
  - `fn character_length_style(self: &Self) -> CharacterLengthStyle`
  - `fn int64_cast_dtype(self: &Self) -> ast::DataType`
  - `fn int32_cast_dtype(self: &Self) -> ast::DataType`
  - `fn timestamp_cast_dtype(self: &Self, _time_unit: &TimeUnit, tz: &Option<Arc<str>>) -> ast::DataType`
  - `fn date32_cast_dtype(self: &Self) -> ast::DataType`
  - `fn supports_column_alias_in_table_alias(self: &Self) -> bool`
  - `fn scalar_function_to_sql_overrides(self: &Self, unparser: &Unparser, func_name: &str, args: &[Expr]) -> Result<Option<ast::Expr>>`
  - `fn requires_derived_table_alias(self: &Self) -> bool`
  - `fn division_operator(self: &Self) -> BinaryOperator`
  - `fn window_func_support_window_frame(self: &Self, _func_name: &str, _start_bound: &WindowFrameBound, _end_bound: &WindowFrameBound) -> bool`
  - `fn full_qualified_col(self: &Self) -> bool`
  - `fn unnest_as_table_factor(self: &Self) -> bool`



## datafusion_sql::unparser::dialect::CustomDialectBuilder

*Struct*

`CustomDialectBuilder` to build `CustomDialect` using builder pattern


# Examples

Building a custom dialect with all default options set in CustomDialectBuilder::new()
but with `use_timestamp_for_date64` overridden to `true`

```
use datafusion_sql::unparser::dialect::CustomDialectBuilder;
let dialect = CustomDialectBuilder::new()
    .with_use_timestamp_for_date64(true)
    .build();
```

**Methods:**

- `fn new() -> Self`
- `fn build(self: Self) -> CustomDialect`
- `fn with_identifier_quote_style(self: Self, identifier_quote_style: char) -> Self` - Customize the dialect with a specific identifier quote style, e.g. '`', '"'
- `fn with_supports_nulls_first_in_sort(self: Self, supports_nulls_first_in_sort: bool) -> Self` - Customize the dialect to support `NULLS FIRST` in `ORDER BY` clauses
- `fn with_use_timestamp_for_date64(self: Self, use_timestamp_for_date64: bool) -> Self` - Customize the dialect to uses TIMESTAMP when casting Date64 rather than DATETIME
- `fn with_interval_style(self: Self, interval_style: IntervalStyle) -> Self` - Customize the dialect with a specific interval style listed in `IntervalStyle`
- `fn with_character_length_style(self: Self, character_length_style: CharacterLengthStyle) -> Self` - Customize the dialect with a specific character_length_style listed in `CharacterLengthStyle`
- `fn with_float64_ast_dtype(self: Self, float64_ast_dtype: ast::DataType) -> Self` - Customize the dialect with a specific SQL type for Float64 casting: DOUBLE, DOUBLE PRECISION, etc.
- `fn with_utf8_cast_dtype(self: Self, utf8_cast_dtype: ast::DataType) -> Self` - Customize the dialect with a specific SQL type for Utf8 casting: VARCHAR, CHAR, etc.
- `fn with_large_utf8_cast_dtype(self: Self, large_utf8_cast_dtype: ast::DataType) -> Self` - Customize the dialect with a specific SQL type for LargeUtf8 casting: TEXT, CHAR, etc.
- `fn with_date_field_extract_style(self: Self, date_field_extract_style: DateFieldExtractStyle) -> Self` - Customize the dialect with a specific date field extract style listed in `DateFieldExtractStyle`
- `fn with_int64_cast_dtype(self: Self, int64_cast_dtype: ast::DataType) -> Self` - Customize the dialect with a specific SQL type for Int64 casting: BigInt, SIGNED, etc.
- `fn with_int32_cast_dtype(self: Self, int32_cast_dtype: ast::DataType) -> Self` - Customize the dialect with a specific SQL type for Int32 casting: Integer, SIGNED, etc.
- `fn with_timestamp_cast_dtype(self: Self, timestamp_cast_dtype: ast::DataType, timestamp_tz_cast_dtype: ast::DataType) -> Self` - Customize the dialect with a specific SQL type for Timestamp casting: Timestamp, Datetime, etc.
- `fn with_date32_cast_dtype(self: Self, date32_cast_dtype: ast::DataType) -> Self`
- `fn with_supports_column_alias_in_table_alias(self: Self, supports_column_alias_in_table_alias: bool) -> Self` - Customize the dialect to support column aliases as part of alias table definition
- `fn with_requires_derived_table_alias(self: Self, requires_derived_table_alias: bool) -> Self`
- `fn with_division_operator(self: Self, division_operator: BinaryOperator) -> Self`
- `fn with_window_func_support_window_frame(self: Self, window_func_support_window_frame: bool) -> Self`
- `fn with_full_qualified_col(self: Self, full_qualified_col: bool) -> Self` - Customize the dialect to allow full qualified column names
- `fn with_unnest_as_table_factor(self: Self, unnest_as_table_factor: bool) -> Self`

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`



## datafusion_sql::unparser::dialect::DateFieldExtractStyle

*Enum*

Datetime subfield extraction style for unparsing

`<https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT>`
Different DBMSs follow different standards; popular ones are:
date_part('YEAR', date '2001-02-16')
EXTRACT(YEAR from date '2001-02-16')
Some DBMSs, like Postgres, support both, whereas others like MySQL require EXTRACT.

**Variants:**
- `DatePart`
- `Extract`
- `Strftime`

**Traits:** Copy

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &DateFieldExtractStyle) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> DateFieldExtractStyle`



## datafusion_sql::unparser::dialect::DefaultDialect

*Struct*

**Trait Implementations:**

- **Dialect**
  - `fn identifier_quote_style(self: &Self, identifier: &str) -> Option<char>`



## datafusion_sql::unparser::dialect::Dialect

*Trait*

`Dialect` to use for Unparsing

The default dialect tries to avoid quoting identifiers unless necessary (e.g. `a` instead of `"a"`)
but this behavior can be overridden as needed

**Note**: This trait will eventually be replaced by the Dialect in the SQLparser package

See <https://github.com/sqlparser-rs/sqlparser-rs/pull/1170>
See also the discussion in <https://github.com/apache/datafusion/pull/10625>

**Methods:**

- `identifier_quote_style`: Return the character used to quote identifiers.
- `supports_nulls_first_in_sort`: Does the dialect support specifying `NULLS FIRST/LAST` in `ORDER BY` clauses?
- `use_timestamp_for_date64`: Does the dialect use TIMESTAMP to represent Date64 rather than DATETIME?
- `interval_style`
- `float64_ast_dtype`: Does the dialect use DOUBLE PRECISION to represent Float64 rather than DOUBLE?
- `utf8_cast_dtype`: The SQL type to use for Arrow Utf8 unparsing
- `large_utf8_cast_dtype`: The SQL type to use for Arrow LargeUtf8 unparsing
- `date_field_extract_style`: The date field extract style to use: `DateFieldExtractStyle`
- `character_length_style`: The character length extraction style to use: `CharacterLengthStyle`
- `int64_cast_dtype`: The SQL type to use for Arrow Int64 unparsing
- `int32_cast_dtype`: The SQL type to use for Arrow Int32 unparsing
- `timestamp_cast_dtype`: The SQL type to use for Timestamp unparsing
- `date32_cast_dtype`: The SQL type to use for Arrow Date32 unparsing
- `supports_column_alias_in_table_alias`: Does the dialect support specifying column aliases as part of alias table definition?
- `requires_derived_table_alias`: Whether the dialect requires a table alias for any subquery in the FROM clause
- `division_operator`: The division operator for the dialect
- `scalar_function_to_sql_overrides`: Allows the dialect to override scalar function unparsing if the dialect has specific rules.
- `window_func_support_window_frame`: Allows the dialect to choose to omit window frame in unparsing
- `with_custom_scalar_overrides`: Extends the dialect's default rules for unparsing scalar functions.
- `full_qualified_col`: Allow to unparse a qualified column with a full qualified name
- `unnest_as_table_factor`: Allow to unparse the unnest plan as [ast::TableFactor::UNNEST].
- `col_alias_overrides`: Allows the dialect to override column alias unparsing if the dialect has specific rules.
- `supports_qualify`: Allows the dialect to support the QUALIFY clause
- `timestamp_with_tz_to_string`: Allows the dialect to override logic of formatting datetime with tz into string.
- `supports_empty_select_list`: Whether the dialect supports an empty select list such as `SELECT FROM table`.



## datafusion_sql::unparser::dialect::DuckDBDialect

*Struct*

**Methods:**

- `fn new() -> Self`

**Trait Implementations:**

- **Dialect**
  - `fn identifier_quote_style(self: &Self, _: &str) -> Option<char>`
  - `fn character_length_style(self: &Self) -> CharacterLengthStyle`
  - `fn division_operator(self: &Self) -> BinaryOperator`
  - `fn with_custom_scalar_overrides(self: Self, handlers: Vec<(&str, ScalarFnToSqlHandler)>) -> Self`
  - `fn scalar_function_to_sql_overrides(self: &Self, unparser: &Unparser, func_name: &str, args: &[Expr]) -> Result<Option<ast::Expr>>`
  - `fn timestamp_with_tz_to_string(self: &Self, dt: DateTime<Tz>, unit: TimeUnit) -> String`
- **Default**
  - `fn default() -> DuckDBDialect`



## datafusion_sql::unparser::dialect::IntervalStyle

*Enum*

`IntervalStyle` to use for unparsing

<https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT>
different DBMS follows different standards, popular ones are:
postgres_verbose: '2 years 15 months 100 weeks 99 hours 123456789 milliseconds' which is
compatible with arrow display format, as well as duckdb
sql standard format is '1-2' for year-month, or '1 10:10:10.123456' for day-time
<https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt>

**Variants:**
- `PostgresVerbose`
- `SQLStandard`
- `MySQL`

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> IntervalStyle`



## datafusion_sql::unparser::dialect::MySqlDialect

*Struct*

**Trait Implementations:**

- **Dialect**
  - `fn supports_qualify(self: &Self) -> bool`
  - `fn identifier_quote_style(self: &Self, _: &str) -> Option<char>`
  - `fn supports_nulls_first_in_sort(self: &Self) -> bool`
  - `fn interval_style(self: &Self) -> IntervalStyle`
  - `fn utf8_cast_dtype(self: &Self) -> ast::DataType`
  - `fn large_utf8_cast_dtype(self: &Self) -> ast::DataType`
  - `fn date_field_extract_style(self: &Self) -> DateFieldExtractStyle`
  - `fn int64_cast_dtype(self: &Self) -> ast::DataType`
  - `fn int32_cast_dtype(self: &Self) -> ast::DataType`
  - `fn timestamp_cast_dtype(self: &Self, _time_unit: &TimeUnit, _tz: &Option<Arc<str>>) -> ast::DataType`
  - `fn requires_derived_table_alias(self: &Self) -> bool`
  - `fn scalar_function_to_sql_overrides(self: &Self, unparser: &Unparser, func_name: &str, args: &[Expr]) -> Result<Option<ast::Expr>>`



## datafusion_sql::unparser::dialect::PostgreSqlDialect

*Struct*

**Trait Implementations:**

- **Dialect**
  - `fn supports_qualify(self: &Self) -> bool`
  - `fn requires_derived_table_alias(self: &Self) -> bool`
  - `fn supports_empty_select_list(self: &Self) -> bool`
  - `fn identifier_quote_style(self: &Self, _: &str) -> Option<char>`
  - `fn interval_style(self: &Self) -> IntervalStyle`
  - `fn float64_ast_dtype(self: &Self) -> ast::DataType`
  - `fn scalar_function_to_sql_overrides(self: &Self, unparser: &Unparser, func_name: &str, args: &[Expr]) -> Result<Option<ast::Expr>>`



## datafusion_sql::unparser::dialect::ScalarFnToSqlHandler

*Type Alias*: `Box<dyn Fn>`



## datafusion_sql::unparser::dialect::SqliteDialect

*Struct*

**Trait Implementations:**

- **Dialect**
  - `fn supports_qualify(self: &Self) -> bool`
  - `fn identifier_quote_style(self: &Self, _: &str) -> Option<char>`
  - `fn date_field_extract_style(self: &Self) -> DateFieldExtractStyle`
  - `fn date32_cast_dtype(self: &Self) -> ast::DataType`
  - `fn character_length_style(self: &Self) -> CharacterLengthStyle`
  - `fn supports_column_alias_in_table_alias(self: &Self) -> bool`
  - `fn timestamp_cast_dtype(self: &Self, _time_unit: &TimeUnit, _tz: &Option<Arc<str>>) -> ast::DataType`
  - `fn scalar_function_to_sql_overrides(self: &Self, unparser: &Unparser, func_name: &str, args: &[Expr]) -> Result<Option<ast::Expr>>`



