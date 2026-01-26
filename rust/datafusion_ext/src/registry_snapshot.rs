use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::execution::session_state::SessionState;
use datafusion_expr::{Documentation, Signature, TypeSignature, Volatility, WindowUDF};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;

use crate::{udaf_builtin, udf_docs, udf_registry, udwf_builtin};

pub struct RegistrySnapshot {
    pub scalar: Vec<String>,
    pub aggregate: Vec<String>,
    pub window: Vec<String>,
    pub table: Vec<String>,
    pub aliases: BTreeMap<String, Vec<String>>,
    pub parameter_names: BTreeMap<String, Vec<String>>,
    pub volatility: BTreeMap<String, String>,
    pub rewrite_tags: BTreeMap<String, Vec<String>>,
    pub signature_inputs: BTreeMap<String, Vec<Vec<String>>>,
    pub return_types: BTreeMap<String, Vec<String>>,
    pub custom_udfs: Vec<String>,
}

impl RegistrySnapshot {
    pub fn new() -> Self {
        Self {
            scalar: Vec::new(),
            aggregate: Vec::new(),
            window: Vec::new(),
            table: Vec::new(),
            aliases: BTreeMap::new(),
            parameter_names: BTreeMap::new(),
            volatility: BTreeMap::new(),
            rewrite_tags: BTreeMap::new(),
            signature_inputs: BTreeMap::new(),
            return_types: BTreeMap::new(),
            custom_udfs: Vec::new(),
        }
    }
}

pub fn registry_snapshot(state: &SessionState) -> RegistrySnapshot {
    let table_signatures = custom_table_signatures();
    let custom_names = custom_udf_names(&table_signatures);
    let mut snapshot = RegistrySnapshot::new();
    snapshot.custom_udfs = custom_names.iter().cloned().collect();
    record_scalar_udfs(state, &mut snapshot);
    record_aggregate_udfs(state, &mut snapshot);
    record_window_udfs(state, &mut snapshot);
    record_table_functions(state, &mut snapshot);
    apply_table_signatures(&mut snapshot, &table_signatures);
    apply_docs_parameter_names(state, &mut snapshot);
    snapshot.scalar.sort();
    snapshot.aggregate.sort();
    snapshot.window.sort();
    snapshot.table.sort();
    snapshot.custom_udfs.sort();
    snapshot
}

fn record_scalar_udfs(state: &SessionState, snapshot: &mut RegistrySnapshot) {
    for (name, udf) in state.scalar_functions() {
        snapshot.scalar.push(name.clone());
        record_aliases(name, udf.aliases(), &mut snapshot.aliases);
        record_signature(name, udf.signature(), &mut snapshot.parameter_names, &mut snapshot.volatility);
        record_rewrite_tags(name, snapshot);
        record_signature_details(name, udf.signature(), snapshot, |arg_types| {
            udf.return_type(arg_types).ok()
        });
    }
}

fn record_aggregate_udfs(state: &SessionState, snapshot: &mut RegistrySnapshot) {
    for (name, udaf) in state.aggregate_functions() {
        snapshot.aggregate.push(name.clone());
        record_aliases(name, udaf.aliases(), &mut snapshot.aliases);
        record_signature(
            name,
            udaf.signature(),
            &mut snapshot.parameter_names,
            &mut snapshot.volatility,
        );
        record_rewrite_tags(name, snapshot);
        record_signature_details(name, udaf.signature(), snapshot, |arg_types| {
            udaf.return_type(arg_types).ok()
        });
    }
}

fn record_window_udfs(state: &SessionState, snapshot: &mut RegistrySnapshot) {
    for (name, udwf) in state.window_functions() {
        snapshot.window.push(name.clone());
        record_aliases(name, udwf.aliases(), &mut snapshot.aliases);
        record_signature(
            name,
            udwf.signature(),
            &mut snapshot.parameter_names,
            &mut snapshot.volatility,
        );
        record_rewrite_tags(name, snapshot);
        record_signature_details(name, udwf.signature(), snapshot, |arg_types| {
            window_return_type(udwf, arg_types, name).ok()
        });
    }
}

fn record_table_functions(state: &SessionState, snapshot: &mut RegistrySnapshot) {
    for (name, _udtf) in state.table_functions() {
        snapshot.table.push(name.clone());
        record_rewrite_tags(name, snapshot);
    }
}

fn record_aliases(name: &str, aliases: &[String], target: &mut BTreeMap<String, Vec<String>>) {
    if aliases.is_empty() {
        return;
    }
    target.insert(name.to_string(), aliases.to_vec());
}

fn record_signature(
    name: &str,
    signature: &Signature,
    params_target: &mut BTreeMap<String, Vec<String>>,
    volatility_target: &mut BTreeMap<String, String>,
) {
    if let Some(names) = signature.parameter_names.clone() {
        if !names.is_empty() {
            params_target.insert(name.to_string(), names);
        }
    }
    volatility_target.insert(name.to_string(), volatility_label(signature.volatility));
}

fn record_rewrite_tags(name: &str, snapshot: &mut RegistrySnapshot) {
    if let Some(tags) = rewrite_tags_for(name) {
        snapshot.rewrite_tags.insert(name.to_string(), tags);
    }
}

fn volatility_label(value: Volatility) -> String {
    match value {
        Volatility::Immutable => "immutable".to_string(),
        Volatility::Stable => "stable".to_string(),
        Volatility::Volatile => "volatile".to_string(),
    }
}

struct TableSignature {
    inputs: Vec<Vec<DataType>>,
    return_type: DataType,
}

fn custom_table_signatures() -> BTreeMap<String, TableSignature> {
    let mut signatures = BTreeMap::new();
    let cache_schema = DataType::Struct(Fields::from(vec![
        Field::new("cache_name", DataType::Utf8, false),
        Field::new("event_time_unix_ms", DataType::Int64, false),
        Field::new("entry_count", DataType::Int64, true),
        Field::new("hit_count", DataType::Int64, true),
        Field::new("miss_count", DataType::Int64, true),
        Field::new("eviction_count", DataType::Int64, true),
        Field::new("config_ttl", DataType::Utf8, true),
        Field::new("config_limit", DataType::Utf8, true),
    ]));
    signatures.insert(
        "range_table".to_string(),
        TableSignature {
            inputs: vec![vec![DataType::Int64, DataType::Int64]],
            return_type: DataType::Struct(Fields::from(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
        },
    );
    signatures.insert(
        "read_parquet".to_string(),
        TableSignature {
            inputs: vec![
                vec![DataType::Utf8],
                vec![DataType::Utf8, DataType::Int64],
                vec![DataType::Utf8, DataType::Int64, DataType::Binary],
                vec![DataType::Utf8, DataType::Int64, DataType::Utf8],
            ],
            return_type: DataType::Struct(Fields::from(Vec::<Field>::new())),
        },
    );
    signatures.insert(
        "read_csv".to_string(),
        TableSignature {
            inputs: vec![
                vec![DataType::Utf8],
                vec![DataType::Utf8, DataType::Int64],
                vec![DataType::Utf8, DataType::Int64, DataType::Binary],
                vec![DataType::Utf8, DataType::Int64, DataType::Utf8],
                vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Binary,
                    DataType::Boolean,
                ],
                vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Utf8,
                    DataType::Boolean,
                ],
                vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Binary,
                    DataType::Boolean,
                    DataType::Utf8,
                ],
                vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Utf8,
                    DataType::Boolean,
                    DataType::Utf8,
                ],
                vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Binary,
                    DataType::Boolean,
                    DataType::Utf8,
                    DataType::Utf8,
                ],
                vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Utf8,
                    DataType::Boolean,
                    DataType::Utf8,
                    DataType::Utf8,
                ],
            ],
            return_type: DataType::Struct(Fields::from(Vec::<Field>::new())),
        },
    );
    for name in [
        "list_files_cache",
        "metadata_cache",
        "predicate_cache",
        "statistics_cache",
    ] {
        signatures.insert(
            name.to_string(),
            TableSignature {
                inputs: vec![vec![]],
                return_type: cache_schema.clone(),
            },
        );
    }
    signatures.insert(
        "udf_registry".to_string(),
        TableSignature {
            inputs: vec![vec![]],
            return_type: DataType::Struct(Fields::from(vec![
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
            ])),
        },
    );
    signatures.insert(
        "udf_docs".to_string(),
        TableSignature {
            inputs: vec![vec![]],
            return_type: DataType::Struct(Fields::from(vec![
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
            ])),
        },
    );
    signatures
}

fn rewrite_tags_for(name: &str) -> Option<Vec<String>> {
    const REWRITE_TAGS: &[(&str, &[&str])] = &[
        ("stable_hash64", &["hash"]),
        ("stable_hash128", &["hash"]),
        ("prefixed_hash64", &["hash"]),
        ("stable_id", &["hash"]),
        ("sha256", &["hash"]),
        ("col_to_byte", &["position_encoding"]),
        ("list_unique", &["list"]),
        ("first_value_agg", &["aggregate"]),
        ("last_value_agg", &["aggregate"]),
        ("count_distinct_agg", &["aggregate"]),
        ("string_agg", &["aggregate", "string"]),
        ("row_number_window", &["window"]),
        ("lag_window", &["window"]),
        ("lead_window", &["window"]),
        ("range_table", &["table"]),
    ];
    REWRITE_TAGS
        .iter()
        .find(|(key, _)| *key == name)
        .map(|(_, tags)| tags.iter().map(|tag| tag.to_string()).collect())
}

fn custom_udf_names(table_signatures: &BTreeMap<String, TableSignature>) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for spec in udf_registry::all_udfs() {
        names.insert(spec.name.to_string());
    }
    for udaf in udaf_builtin::builtin_udafs() {
        names.insert(udaf.name().to_string());
    }
    for udwf in udwf_builtin::builtin_udwfs() {
        names.insert(udwf.name().to_string());
    }
    for name in table_signatures.keys() {
        names.insert(name.clone());
    }
    names
}

fn apply_table_signatures(
    snapshot: &mut RegistrySnapshot,
    table_signatures: &BTreeMap<String, TableSignature>,
) {
    for (name, signature) in table_signatures {
        let inputs = signature
            .inputs
            .iter()
            .map(|args| args.iter().map(format_data_type).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let returns = vec![format_data_type(&signature.return_type); inputs.len()];
        if !inputs.is_empty() {
            snapshot.signature_inputs.insert(name.clone(), inputs);
            snapshot.return_types.insert(name.clone(), returns);
        }
    }
}

fn apply_docs_parameter_names(state: &SessionState, snapshot: &mut RegistrySnapshot) {
    let mut docs = documentation_snapshot(state);
    for (name, doc) in udf_docs::docs_snapshot() {
        docs.insert(name.to_string(), doc);
    }
    for (name, doc) in docs {
        if snapshot.parameter_names.contains_key(&name) {
            continue;
        }
        let Some(args) = doc.arguments.as_ref() else {
            continue;
        };
        if args.is_empty() {
            continue;
        }
        let names = args.iter().map(|(arg, _)| arg.clone()).collect();
        snapshot.parameter_names.insert(name, names);
    }
}

fn documentation_snapshot(state: &SessionState) -> BTreeMap<String, &Documentation> {
    let mut docs: BTreeMap<String, &Documentation> = BTreeMap::new();
    for (name, udf) in state.scalar_functions() {
        if let Some(doc) = udf.documentation() {
            docs.entry(name.clone()).or_insert(doc);
        }
    }
    for (name, udaf) in state.aggregate_functions() {
        if let Some(doc) = udaf.documentation() {
            docs.entry(name.clone()).or_insert(doc);
        }
    }
    for (name, udwf) in state.window_functions() {
        if let Some(doc) = udwf.documentation() {
            docs.entry(name.clone()).or_insert(doc);
        }
    }
    docs
}

fn record_signature_details<F>(
    name: &str,
    signature: &Signature,
    snapshot: &mut RegistrySnapshot,
    mut return_type: F,
) where
    F: FnMut(&[DataType]) -> Option<DataType>,
{
    let arg_sets = signature_arg_sets(signature);
    if arg_sets.is_empty() {
        return;
    }
    let mut input_rows = Vec::with_capacity(arg_sets.len());
    let mut return_rows = Vec::with_capacity(arg_sets.len());
    for arg_types in arg_sets {
        input_rows.push(
            arg_types
                .iter()
                .map(format_data_type)
                .collect::<Vec<_>>(),
        );
        let resolved = return_type(&arg_types).unwrap_or(DataType::Null);
        return_rows.push(format_data_type(&resolved));
    }
    snapshot
        .signature_inputs
        .insert(name.to_string(), input_rows);
    snapshot.return_types.insert(name.to_string(), return_rows);
}

fn signature_arg_sets(signature: &Signature) -> Vec<Vec<DataType>> {
    arg_sets_from_type_signature(&signature.type_signature)
}

fn arg_sets_from_type_signature(signature: &TypeSignature) -> Vec<Vec<DataType>> {
    match signature {
        TypeSignature::Exact(types) => vec![types.clone()],
        TypeSignature::OneOf(variants) => variants
            .iter()
            .flat_map(arg_sets_from_type_signature)
            .collect(),
        TypeSignature::String(count) => vec![vec![DataType::Utf8; *count]],
        TypeSignature::Numeric(count) => vec![vec![DataType::Float64; *count]],
        TypeSignature::Uniform(count, valid_types) => valid_types
            .first()
            .map(|dtype| vec![vec![dtype.clone(); *count]])
            .unwrap_or_default(),
        TypeSignature::Any(count) => vec![vec![DataType::Null; *count]],
        TypeSignature::Nullary => vec![Vec::new()],
        _ => Vec::new(),
    }
}

fn window_return_type(udwf: &WindowUDF, arg_types: &[DataType], name: &str) -> datafusion_common::Result<DataType> {
    let arg_fields = build_arg_fields(arg_types);
    let field = udwf.field(WindowUDFFieldArgs::new(&arg_fields, name))?;
    Ok(field.data_type().clone())
}

fn build_arg_fields(arg_types: &[DataType]) -> Vec<FieldRef> {
    arg_types
        .iter()
        .enumerate()
        .map(|(idx, dtype)| {
            let name = format!("arg{}", idx + 1);
            Arc::new(Field::new(name, dtype.clone(), true))
        })
        .collect()
}

fn format_data_type(value: &DataType) -> String {
    match value {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Null => "null".to_string(),
        DataType::List(field) => format!("list<{}>", format_data_type(field.data_type())),
        DataType::Struct(fields) => {
            let parts = fields
                .iter()
                .map(|field| format!("{}:{}", field.name(), format_data_type(field.data_type())))
                .collect::<Vec<_>>();
            format!("struct<{}>", parts.join(","))
        }
        DataType::Map(field, _) => {
            let value = field.data_type();
            if let DataType::Struct(fields) = value {
                let key_type = fields
                    .first()
                    .map(|field| format_data_type(field.data_type()))
                    .unwrap_or_else(|| "null".to_string());
                let value_type = fields
                    .get(1)
                    .map(|field| format_data_type(field.data_type()))
                    .unwrap_or_else(|| "null".to_string());
                format!("map<{key_type},{value_type}>")
            } else {
                "map<null,null>".to_string()
            }
        }
        DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
        _ => value.to_string().to_lowercase(),
    }
}
