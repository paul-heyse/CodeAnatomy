**datafusion_macros**

# Module: datafusion_macros

## Contents

**Proc Macros**

- [`user_doc`](#user_doc) - This procedural macro is intended to parse a rust custom attribute and create user documentation

---

## datafusion_macros::user_doc

*Attribute Macro*

This procedural macro is intended to parse a rust custom attribute and create user documentation
from it by constructing a `DocumentBuilder()` automatically. The `Documentation` can be
retrieved from the `documentation()` method
declared on `AggregateUDF`, `WindowUDFImpl`, `ScalarUDFImpl` traits.
For `doc_section`, this macro will try to find corresponding predefined `DocSection` by label field
Predefined `DocSection` can be found in datafusion/expr/src/udf.rs
Example:
```ignore
#[user_doc(
    doc_section(label = "Time and Date Functions"),
    description = r"Converts a value to a date (`YYYY-MM-DD`).",
    syntax_example = "to_date('2017-05-31', '%Y-%m-%d')",
    sql_example = r#"```sql
> select to_date('2023-01-31');
+-----------------------------+
| to_date(Utf8(\"2023-01-31\")) |
+-----------------------------+
| 2023-01-31                  |
+-----------------------------+
```"#,
    standard_argument(name = "expression", prefix = "String"),
    argument(
        name = "format_n",
        description = r"Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned."
   )
)]
#[derive(Debug)]
pub struct ToDateFunc {
    signature: Signature,
}
```
will generate the following code
```ignore
pub struct ToDateFunc {
    signature: Signature,
}
impl ToDateFunc {
    fn doc(&self) -> Option<&datafusion_doc::Documentation> {
        static DOCUMENTATION: std::sync::LazyLock<
            datafusion_doc::Documentation,
        > = std::sync::LazyLock::new(|| {
            datafusion_doc::Documentation::builder(
                    datafusion_doc::DocSection {
                        include: true,
                        label: "Time and Date Functions",
                        description: None,
                    },
                    r"Converts a value to a date (`YYYY-MM-DD`).".to_string(),
                    "to_date('2017-05-31', '%Y-%m-%d')".to_string(),
                )
                .with_sql_example(
                    r#"```sql
> select to_date('2023-01-31');
+-----------------------------+
| to_date(Utf8(\"2023-01-31\")) |
+-----------------------------+
| 2023-01-31                  |
+-----------------------------+
```"#,
                )
                .with_standard_argument("expression", "String".into())
                .with_argument(
                    "format_n",
                    r"Optional [Chrono format](https://docs.rs/chrono/latest/chrono/format/strftime/index.html) strings to use to parse the expression. Formats will be tried in the order
they appear with the first successful one being returned. If none of the formats successfully parse the expression
an error will be returned.",
                )
                .build()
        });
        Some(&DOCUMENTATION)
    }
}
```

```rust
#[user_doc]
```



