use std::collections::BTreeMap;
use std::sync::LazyLock;

use datafusion::execution::session_state::SessionState;
use datafusion_doc::DocSection;
use datafusion_expr::Documentation;

const DOC_SECTION_TABLE: DocSection = DocSection {
    include: true,
    label: "Table Functions",
    description: None,
};

static UDF_REGISTRY_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_TABLE,
        "Return metadata for all registered DataFusion functions.",
        "udf_registry()",
    )
    .build()
});

static UDF_DOCS_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_TABLE,
        "Return documentation metadata for all registered DataFusion functions.",
        "udf_docs()",
    )
    .build()
});

static READ_CSV_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_TABLE,
        "Read CSV files into a table provider.",
        "read_csv(path [, limit] [, schema_ipc] [, has_header] [, delimiter] [, compression])",
    )
    .with_argument("path", "Path or URL to CSV files.")
    .with_argument("limit", "Optional maximum rows to return.")
    .with_argument(
        "schema_ipc",
        "Optional Arrow schema IPC bytes (binary literal or hex string) to override inference.",
    )
    .with_argument("has_header", "Optional boolean indicating CSV header presence.")
    .with_argument(
        "delimiter",
        "Optional single-character delimiter (defaults to comma).",
    )
    .with_argument(
        "compression",
        "Optional compression codec name (e.g., gzip, bz2, zstd).",
    )
    .build()
});

static READ_PARQUET_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_TABLE,
        "Read Parquet files into a table provider.",
        "read_parquet(path [, limit] [, schema_ipc])",
    )
    .with_argument("path", "Path or URL to Parquet files.")
    .with_argument("limit", "Optional maximum rows to return.")
    .with_argument(
        "schema_ipc",
        "Optional Arrow schema IPC bytes (binary literal or hex string) to override inference.",
    )
    .build()
});

static RANGE_TABLE_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_TABLE,
        "Return a range of integers as a table.",
        "range_table(start, end)",
    )
    .with_argument("start", "Start of the range (inclusive).")
    .with_argument("end", "End of the range (exclusive).")
    .build()
});

static LIST_FILES_CACHE_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_TABLE,
        "Inspect the list-files cache snapshot.",
        "list_files_cache()",
    )
    .build()
});

static METADATA_CACHE_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_TABLE,
        "Inspect the file metadata cache snapshot.",
        "metadata_cache()",
    )
    .build()
});

static PREDICATE_CACHE_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_TABLE,
        "Inspect the predicate cache snapshot.",
        "predicate_cache()",
    )
    .build()
});

static STATISTICS_CACHE_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_TABLE,
        "Inspect the file statistics cache snapshot.",
        "statistics_cache()",
    )
    .build()
});

pub fn docs_snapshot() -> Vec<(&'static str, &'static Documentation)> {
    vec![
        ("udf_registry", &UDF_REGISTRY_DOC),
        ("udf_docs", &UDF_DOCS_DOC),
        ("read_csv", &READ_CSV_DOC),
        ("read_parquet", &READ_PARQUET_DOC),
        ("range_table", &RANGE_TABLE_DOC),
        ("list_files_cache", &LIST_FILES_CACHE_DOC),
        ("metadata_cache", &METADATA_CACHE_DOC),
        ("predicate_cache", &PREDICATE_CACHE_DOC),
        ("statistics_cache", &STATISTICS_CACHE_DOC),
    ]
}

pub fn registry_docs(state: &SessionState) -> BTreeMap<String, &Documentation> {
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
    for (name, doc) in docs_snapshot() {
        docs.insert(name.to_string(), doc);
    }
    docs
}
