use std::sync::LazyLock;

use datafusion_doc::DocSection;
use datafusion_expr::Documentation;

const DOC_SECTION_HASHING: DocSection = DocSection {
    include: true,
    label: "Hashing Functions",
    description: None,
};

const DOC_SECTION_OTHER: DocSection = DocSection {
    include: true,
    label: "Other Functions",
    description: None,
};

const DOC_SECTION_BUILTIN: DocSection = DocSection {
    include: true,
    label: "Built-in Functions",
    description: None,
};

static MAP_ENTRIES_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Return map entries as a list of key/value structs.",
        "map_entries(map_expr)",
    )
    .with_standard_argument("map_expr", Some("Map"))
    .build()
});

static MAP_KEYS_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Return map keys as an array.",
        "map_keys(map_expr)",
    )
    .with_standard_argument("map_expr", Some("Map"))
    .build()
});

static MAP_VALUES_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Return map values as an array.",
        "map_values(map_expr)",
    )
    .with_standard_argument("map_expr", Some("Map"))
    .build()
});

static MAP_EXTRACT_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Extract a value from a map by key.",
        "map_extract(map_expr, key)",
    )
    .with_standard_argument("map_expr", Some("Map"))
    .with_argument("key", "Map key to extract.")
    .build()
});

static LIST_EXTRACT_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Extract an element from a list by 1-based index.",
        "list_extract(list_expr, index)",
    )
    .with_standard_argument("list_expr", Some("List"))
    .with_argument("index", "1-based index to extract.")
    .build()
});

static LIST_UNIQUE_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Aggregate values into a list and remove duplicates.",
        "list_unique(value)",
    )
    .with_standard_argument("value", None)
    .build()
});

static FIRST_VALUE_AGG_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Return the first value in the aggregate window.",
        "first_value_agg(value)",
    )
    .with_standard_argument("value", None)
    .build()
});

static LAST_VALUE_AGG_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Return the last value in the aggregate window.",
        "last_value_agg(value)",
    )
    .with_standard_argument("value", None)
    .build()
});

static COUNT_DISTINCT_AGG_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Count distinct values in the aggregate window.",
        "count_distinct_agg(value)",
    )
    .with_standard_argument("value", None)
    .build()
});

static STRING_AGG_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Concatenate strings with a delimiter.",
        "string_agg(value, delimiter)",
    )
    .with_standard_argument("value", Some("String"))
    .with_standard_argument("delimiter", Some("String"))
    .build()
});

static ROW_NUMBER_WINDOW_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Return the row number within a window.",
        "row_number_window(value)",
    )
    .with_standard_argument("value", None)
    .build()
});

static LAG_WINDOW_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Return a value from a preceding row in the window.",
        "lag_window(value)",
    )
    .with_standard_argument("value", None)
    .build()
});

static LEAD_WINDOW_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Return a value from a following row in the window.",
        "lead_window(value)",
    )
    .with_standard_argument("value", None)
    .build()
});

static UNION_TAG_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Return the active variant tag from a union value.",
        "union_tag(union_expr)",
    )
    .with_standard_argument("union_expr", Some("Union"))
    .build()
});

static UNION_EXTRACT_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_BUILTIN,
        "Extract a value from a union by tag name.",
        "union_extract(union_expr, tag)",
    )
    .with_standard_argument("union_expr", Some("Union"))
    .with_argument("tag", "Union tag name to extract.")
    .build()
});

static ARROW_METADATA_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Extract Arrow field metadata. When a key is provided, returns a single metadata value; otherwise returns a map of all metadata entries.",
        "arrow_metadata(expr [, key])",
    )
    .with_standard_argument("expr", None)
    .with_argument("key", "Optional metadata key to extract.")
    .build()
});

static STABLE_HASH64_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_HASHING,
        "Compute a stable 64-bit hash of a string using Blake2b.",
        "stable_hash64(value)",
    )
    .with_standard_argument("value", Some("String"))
    .build()
});

static STABLE_HASH128_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_HASHING,
        "Compute a stable 128-bit hash of a string using Blake2b.",
        "stable_hash128(value)",
    )
    .with_standard_argument("value", Some("String"))
    .build()
});

static PREFIXED_HASH64_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_HASHING,
        "Compute a stable 64-bit hash of a string and prefix the result with a namespace.",
        "prefixed_hash64(prefix, value)",
    )
    .with_argument("prefix", "Namespace prefix to prepend to the hash.")
    .with_standard_argument("value", Some("String"))
    .build()
});

static STABLE_ID_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_HASHING,
        "Compute a stable identifier by prefixing a 128-bit hash of a string.",
        "stable_id(prefix, value)",
    )
    .with_argument("prefix", "Namespace prefix to prepend to the hash.")
    .with_standard_argument("value", Some("String"))
    .build()
});

static COL_TO_BYTE_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Convert a column offset to a byte offset for a line and encoding.",
        "col_to_byte(line_text, col, col_unit)",
    )
    .with_argument("line_text", "Line text to compute offsets within.")
    .with_argument("col", "Column offset within the line.")
    .with_argument("col_unit", "Encoding unit (BYTE, UTF8, UTF16, UTF32).")
    .build()
});

static POSITION_ENCODING_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Normalize a position encoding name to its numeric code.",
        "position_encoding_norm(value)",
    )
    .with_standard_argument("value", Some("String"))
    .build()
});

static CPG_SCORE_DOC: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_OTHER,
        "Pass-through CPG score function for compatibility.",
        "cpg_score(value)",
    )
    .with_standard_argument("value", None)
    .build()
});

pub fn arrow_metadata_doc() -> &'static Documentation {
    &ARROW_METADATA_DOC
}

pub fn stable_hash64_doc() -> &'static Documentation {
    &STABLE_HASH64_DOC
}

pub fn stable_hash128_doc() -> &'static Documentation {
    &STABLE_HASH128_DOC
}

pub fn prefixed_hash64_doc() -> &'static Documentation {
    &PREFIXED_HASH64_DOC
}

pub fn stable_id_doc() -> &'static Documentation {
    &STABLE_ID_DOC
}

pub fn col_to_byte_doc() -> &'static Documentation {
    &COL_TO_BYTE_DOC
}

pub fn position_encoding_doc() -> &'static Documentation {
    &POSITION_ENCODING_DOC
}

pub fn cpg_score_doc() -> &'static Documentation {
    &CPG_SCORE_DOC
}

pub fn docs_snapshot() -> Vec<(&'static str, &'static Documentation)> {
    vec![
        ("map_entries", &MAP_ENTRIES_DOC),
        ("map_keys", &MAP_KEYS_DOC),
        ("map_values", &MAP_VALUES_DOC),
        ("map_extract", &MAP_EXTRACT_DOC),
        ("list_extract", &LIST_EXTRACT_DOC),
        ("list_unique", &LIST_UNIQUE_DOC),
        ("first_value_agg", &FIRST_VALUE_AGG_DOC),
        ("last_value_agg", &LAST_VALUE_AGG_DOC),
        ("count_distinct_agg", &COUNT_DISTINCT_AGG_DOC),
        ("string_agg", &STRING_AGG_DOC),
        ("row_number_window", &ROW_NUMBER_WINDOW_DOC),
        ("lag_window", &LAG_WINDOW_DOC),
        ("lead_window", &LEAD_WINDOW_DOC),
        ("union_tag", &UNION_TAG_DOC),
        ("union_extract", &UNION_EXTRACT_DOC),
        ("arrow_metadata", arrow_metadata_doc()),
        ("stable_hash64", stable_hash64_doc()),
        ("stable_hash128", stable_hash128_doc()),
        ("prefixed_hash64", prefixed_hash64_doc()),
        ("stable_id", stable_id_doc()),
        ("col_to_byte", col_to_byte_doc()),
        ("position_encoding_norm", position_encoding_doc()),
        ("cpg_score", cpg_score_doc()),
    ]
}
