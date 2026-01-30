use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Expr;
use datafusion_functions_table::all_default_table_functions;

use crate::{table_udfs, macros::TableUdfSpec};
use crate::registry_snapshot;

const REGISTRY_TABLE_FUNCTION: &str = "udf_registry";
const DOCS_TABLE_FUNCTION: &str = "udf_docs";

pub fn register_builtin_udtfs(ctx: &SessionContext) -> Result<()> {
    for func in all_default_table_functions() {
        ctx.register_udtf(func.name(), Arc::clone(func.function()));
    }
    for spec in builtin_udtf_specs() {
        let table_fn = (spec.builder)(ctx)?;
        ctx.register_udtf(spec.name, Arc::clone(&table_fn));
        for alias in spec.aliases {
            ctx.register_udtf(alias, Arc::clone(&table_fn));
        }
    }
    Ok(())
}

fn builtin_udtf_specs() -> Vec<TableUdfSpec> {
    table_udfs![
        REGISTRY_TABLE_FUNCTION => registry_table_function;
        DOCS_TABLE_FUNCTION => docs_table_function;
    ]
}

fn registry_table_function(ctx: &SessionContext) -> Result<Arc<dyn TableFunctionImpl>> {
    let table = registry_table_provider(ctx)?;
    Ok(Arc::new(RegistryTableFunction { table }))
}

fn docs_table_function(ctx: &SessionContext) -> Result<Arc<dyn TableFunctionImpl>> {
    let table = docs_table_provider(ctx)?;
    Ok(Arc::new(DocsTableFunction { table }))
}

fn registry_table_provider(ctx: &SessionContext) -> Result<Arc<dyn TableProvider>> {
    let snapshot = registry_snapshot::registry_snapshot(&ctx.state());
    let mut name_builder = StringBuilder::new();
    let mut kind_builder = StringBuilder::new();
    let mut volatility_builder = StringBuilder::new();
    let mut alias_builder = ListBuilder::new(StringBuilder::new());
    let mut params_builder = ListBuilder::new(StringBuilder::new());
    let mut simplify_builder = BooleanBuilder::new();
    let mut coerce_builder = BooleanBuilder::new();
    let mut short_builder = BooleanBuilder::new();

    let mut append_row = |name: &str, kind: &str| {
        name_builder.append_value(name);
        kind_builder.append_value(kind);
        let volatility = snapshot
            .volatility
            .get(name)
            .map(String::as_str)
            .unwrap_or("unknown");
        volatility_builder.append_value(volatility);
        append_list(&mut alias_builder, snapshot.aliases.get(name));
        append_list(&mut params_builder, snapshot.parameter_names.get(name));
        simplify_builder.append_value(*snapshot.simplify.get(name).unwrap_or(&false));
        coerce_builder.append_value(*snapshot.coerce_types.get(name).unwrap_or(&false));
        short_builder.append_value(*snapshot.short_circuits.get(name).unwrap_or(&false));
    };

    for name in &snapshot.scalar {
        append_row(name, "scalar");
    }
    for name in &snapshot.aggregate {
        append_row(name, "aggregate");
    }
    for name in &snapshot.window {
        append_row(name, "window");
    }
    for name in &snapshot.table {
        append_row(name, "table");
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("volatility", DataType::Utf8, false),
        Field::new(
            "aliases",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "parameter_names",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new("simplify", DataType::Boolean, false),
        Field::new("coerce_types", DataType::Boolean, false),
        Field::new("short_circuits", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(name_builder.finish()) as ArrayRef,
            Arc::new(kind_builder.finish()) as ArrayRef,
            Arc::new(volatility_builder.finish()) as ArrayRef,
            Arc::new(alias_builder.finish()) as ArrayRef,
            Arc::new(params_builder.finish()) as ArrayRef,
            Arc::new(simplify_builder.finish()) as ArrayRef,
            Arc::new(coerce_builder.finish()) as ArrayRef,
            Arc::new(short_builder.finish()) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(Arc::new(table))
}

fn docs_table_provider(ctx: &SessionContext) -> Result<Arc<dyn TableProvider>> {
    let state = ctx.state();
    let snapshot = registry_snapshot::registry_snapshot(&state);
    let kind_map = registry_kind_map(&snapshot);
    let docs = crate::udf_docs::registry_docs(&state);

    let mut name_builder = StringBuilder::new();
    let mut kind_builder = StringBuilder::new();
    let mut section_builder = StringBuilder::new();
    let mut description_builder = StringBuilder::new();
    let mut syntax_builder = StringBuilder::new();
    let mut sql_example_builder = StringBuilder::new();
    let mut arg_name_builder = ListBuilder::new(StringBuilder::new());
    let mut arg_desc_builder = ListBuilder::new(StringBuilder::new());
    let mut alternative_builder = ListBuilder::new(StringBuilder::new());
    let mut related_builder = ListBuilder::new(StringBuilder::new());

    for (name, doc) in docs {
        name_builder.append_value(name.as_str());
        if let Some(kind) = kind_map.get(&name) {
            kind_builder.append_value(*kind);
        } else {
            kind_builder.append_null();
        }
        section_builder.append_value(doc.doc_section.label);
        description_builder.append_value(doc.description.as_str());
        syntax_builder.append_value(doc.syntax_example.as_str());
        if let Some(example) = &doc.sql_example {
            sql_example_builder.append_value(example.as_str());
        } else {
            sql_example_builder.append_null();
        }
        append_argument_pairs(
            &mut arg_name_builder,
            &mut arg_desc_builder,
            doc.arguments.as_ref(),
        );
        append_list(&mut alternative_builder, doc.alternative_syntax.as_ref());
        append_list(&mut related_builder, doc.related_udfs.as_ref());
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, true),
        Field::new("section", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("syntax", DataType::Utf8, false),
        Field::new("sql_example", DataType::Utf8, true),
        Field::new(
            "argument_names",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "argument_descriptions",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "alternative_syntax",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "related_udfs",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        ),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(name_builder.finish()) as ArrayRef,
            Arc::new(kind_builder.finish()) as ArrayRef,
            Arc::new(section_builder.finish()) as ArrayRef,
            Arc::new(description_builder.finish()) as ArrayRef,
            Arc::new(syntax_builder.finish()) as ArrayRef,
            Arc::new(sql_example_builder.finish()) as ArrayRef,
            Arc::new(arg_name_builder.finish()) as ArrayRef,
            Arc::new(arg_desc_builder.finish()) as ArrayRef,
            Arc::new(alternative_builder.finish()) as ArrayRef,
            Arc::new(related_builder.finish()) as ArrayRef,
        ],
    )?;

    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(Arc::new(table))
}

fn registry_kind_map(snapshot: &registry_snapshot::RegistrySnapshot) -> BTreeMap<String, &str> {
    let mut kinds: BTreeMap<String, &str> = BTreeMap::new();
    for name in &snapshot.scalar {
        kinds.insert(name.clone(), "scalar");
    }
    for name in &snapshot.aggregate {
        kinds.insert(name.clone(), "aggregate");
    }
    for name in &snapshot.window {
        kinds.insert(name.clone(), "window");
    }
    for name in &snapshot.table {
        kinds.insert(name.clone(), "table");
    }
    for (name, aliases) in &snapshot.aliases {
        if let Some(kind) = kinds.get(name).copied() {
            for alias in aliases {
                kinds.insert(alias.clone(), kind);
            }
        }
    }
    kinds
}

fn append_list(builder: &mut ListBuilder<StringBuilder>, values: Option<&Vec<String>>) {
    if let Some(values) = values {
        for value in values {
            builder.values().append_value(value);
        }
    }
    builder.append(true);
}

fn append_argument_pairs(
    names_builder: &mut ListBuilder<StringBuilder>,
    desc_builder: &mut ListBuilder<StringBuilder>,
    values: Option<&Vec<(String, String)>>,
) {
    if let Some(values) = values {
        for (name, description) in values {
            names_builder.values().append_value(name);
            desc_builder.values().append_value(description);
        }
    }
    names_builder.append(true);
    desc_builder.append(true);
}

#[derive(Debug)]
struct RegistryTableFunction {
    table: Arc<dyn TableProvider>,
}

impl TableFunctionImpl for RegistryTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{REGISTRY_TABLE_FUNCTION} does not accept arguments"
            )));
        }
        Ok(Arc::clone(&self.table))
    }
}

#[derive(Debug)]
struct DocsTableFunction {
    table: Arc<dyn TableProvider>,
}

impl TableFunctionImpl for DocsTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{DOCS_TABLE_FUNCTION} does not accept arguments"
            )));
        }
        Ok(Arc::clone(&self.table))
    }
}
