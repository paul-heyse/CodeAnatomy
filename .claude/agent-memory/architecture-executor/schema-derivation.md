# Schema Derivation (Phase H Gates 1-2)

## Extract Metadata Enrichment

### New Fields on ExtractMetadata (metadata.py)
- `field_types: Mapping[str, str]` - field_name -> PyArrow type string ("list<struct>", "struct", "map<string, string>", "string", "int64", etc.)
- `nested_shapes: Mapping[str, tuple[str, ...]]` - field_name -> child field names of struct type
- Both use `field(default_factory=dict)` default, backward compatible
- Parsed by `_field_types()` and `_nested_shapes()` helpers in `_metadata_from_record()`

### Template Field Population (templates.py)
- 5 templates had `"fields": None` - now populated with explicit column names
- All 7 base extract datasets (repo_files_v1, libcst_files_v1, ast_files_v1, symtable_files_v1, tree_sitter_files_v1, bytecode_files_v1, scip_index_v1) have field_types + nested_shapes
- `_string_tuple()` helper converts None -> () so previously-None fields became empty tuples

### Key Schema Facts
- `_attrs_field()` and `_bytecode_attrs_field()` BOTH produce field named "attrs" with type `ATTRS_T = pa.map_(pa.string(), pa.string())`
- tree_sitter "stats" is a STRUCT (not list<struct>) - the only non-list composite field
- SCIP metadata and index_stats are also struct (not list<struct>)
- SCIP identity columns: index_id, index_path (not repo/path/file_id/file_sha256)
- repo_files_v1 identity columns: file_id, path, file_sha256 (no "repo" column)
- Standard identity columns: repo, path, file_id, file_sha256

### Schema Derivation Module (derivation.py)
- `derive_extract_schema(metadata)` -> pa.Schema: identity columns + evidence columns with resolved types
- `derive_nested_dataset_schema(metadata, nested_path)` -> pa.Schema | None: child field names as string-typed schema
- `resolve_field_type(field_name, type_hint)` in field_types.py resolves string hints to pa.DataType

### Parity Test Results (test_schema_derivation.py)
- 56 tests, all passing: field names, field counts, type categories, nested shapes
- Covers all 7 base extract datasets
- Type category comparison: list, struct, map, string, integer, boolean, float, other

### Identity Column Dispatch
- _SCIP_DATASET_NAMES -> ("index_id", "index_path")
- _REPO_FILES_DATASET_NAMES -> ("file_id", "path", "file_sha256")
- Default -> ("repo", "path", "file_id", "file_sha256")
