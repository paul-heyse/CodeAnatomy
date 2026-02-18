**datafusion_expr > logical_plan > display**

# Module: logical_plan::display

## Contents

**Structs**

- [`GraphvizVisitor`](#graphvizvisitor) - Formats plans for graphical display using the `DOT` language. This
- [`IndentVisitor`](#indentvisitor) - Formats plans with a single line per node. For example:
- [`PgJsonVisitor`](#pgjsonvisitor) - Formats plans to display as postgresql plan json format.

**Functions**

- [`display_schema`](#display_schema) - Print the schema in a compact representation to `buf`

---

## datafusion_expr::logical_plan::display::GraphvizVisitor

*Struct*

Formats plans for graphical display using the `DOT` language. This
format can be visualized using software from
[`graphviz`](https://graphviz.org/)

**Generic Parameters:**
- 'a
- 'b

**Methods:**

- `fn new(f: &'a  mut fmt::Formatter<'b>) -> Self`
- `fn set_with_schema(self: & mut Self, with_schema: bool)` - Sets a flag which controls if the output schema is displayed
- `fn pre_visit_plan(self: & mut Self, label: &str) -> fmt::Result`
- `fn post_visit_plan(self: & mut Self) -> fmt::Result`
- `fn start_graph(self: & mut Self) -> fmt::Result`
- `fn end_graph(self: & mut Self) -> fmt::Result`

**Trait Implementations:**

- **TreeNodeVisitor**
  - `fn f_down(self: & mut Self, plan: &'n LogicalPlan) -> datafusion_common::Result<TreeNodeRecursion>`
  - `fn f_up(self: & mut Self, _plan: &LogicalPlan) -> datafusion_common::Result<TreeNodeRecursion>`



## datafusion_expr::logical_plan::display::IndentVisitor

*Struct*

Formats plans with a single line per node. For example:

Projection: id
   Filter: state Eq Utf8(\"CO\")\
      CsvScan: employee.csv projection=Some([0, 3])";

**Generic Parameters:**
- 'a
- 'b

**Methods:**

- `fn new(f: &'a  mut fmt::Formatter<'b>, with_schema: bool) -> Self` - Create a visitor that will write a formatted LogicalPlan to f. If `with_schema` is

**Trait Implementations:**

- **TreeNodeVisitor**
  - `fn f_down(self: & mut Self, plan: &'n LogicalPlan) -> datafusion_common::Result<TreeNodeRecursion>`
  - `fn f_up(self: & mut Self, _plan: &'n LogicalPlan) -> datafusion_common::Result<TreeNodeRecursion>`



## datafusion_expr::logical_plan::display::PgJsonVisitor

*Struct*

Formats plans to display as postgresql plan json format.

There are already many existing visualizer for this format, for example [dalibo](https://explain.dalibo.com/).
Unfortunately, there is no formal spec for this format, but it is widely used in the PostgreSQL community.

Here is an example of the format:

```json
[
    {
        "Plan": {
            "Node Type": "Sort",
            "Output": [
                "question_1.id",
                "question_1.title",
                "question_1.text",
                "question_1.file",
                "question_1.type",
                "question_1.source",
                "question_1.exam_id"
            ],
            "Sort Key": [
                "question_1.id"
            ],
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Parent Relationship": "Left",
                    "Relation Name": "question",
                    "Schema": "public",
                    "Alias": "question_1",
                    "Output": [
                       "question_1.id",
                        "question_1.title",
                       "question_1.text",
                        "question_1.file",
                        "question_1.type",
                        "question_1.source",
                        "question_1.exam_id"
                    ],
                    "Filter": "(question_1.exam_id = 1)"
                }
            ]
        }
    }
]
```

**Generic Parameters:**
- 'a
- 'b

**Methods:**

- `fn new(f: &'a  mut fmt::Formatter<'b>) -> Self`
- `fn with_schema(self: & mut Self, with_schema: bool)` - Sets a flag which controls if the output schema is displayed

**Trait Implementations:**

- **TreeNodeVisitor**
  - `fn f_down(self: & mut Self, node: &'n LogicalPlan) -> datafusion_common::Result<TreeNodeRecursion>`
  - `fn f_up(self: & mut Self, _node: &<Self as >::Node) -> datafusion_common::Result<TreeNodeRecursion>`



## datafusion_expr::logical_plan::display::display_schema

*Function*

Print the schema in a compact representation to `buf`

For example: `foo:Utf8` if `foo` can not be null, and
`foo:Utf8;N` if `foo` is nullable.

```
use arrow::datatypes::{DataType, Field, Schema};
# use datafusion_expr::logical_plan::display_schema;
let schema = Schema::new(vec![
    Field::new("id", DataType::Int32, false),
    Field::new("first_name", DataType::Utf8, true),
]);

assert_eq!(
    "[id:Int32, first_name:Utf8;N]",
    format!("{}", display_schema(&schema))
);
```

```rust
fn display_schema(schema: &arrow::datatypes::Schema) -> impl Trait
```



