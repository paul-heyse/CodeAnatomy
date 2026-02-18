**datafusion_common > file_options > file_type**

# Module: file_options::file_type

## Contents

**Traits**

- [`FileType`](#filetype) - Defines the functionality needed for logical planning for
- [`GetExt`](#getext) - Define each `FileType`/`FileCompressionType`'s extension

**Constants**

- [`DEFAULT_ARROW_EXTENSION`](#default_arrow_extension) - The default file extension of arrow files
- [`DEFAULT_AVRO_EXTENSION`](#default_avro_extension) - The default file extension of avro files
- [`DEFAULT_CSV_EXTENSION`](#default_csv_extension) - The default file extension of csv files
- [`DEFAULT_JSON_EXTENSION`](#default_json_extension) - The default file extension of json files
- [`DEFAULT_PARQUET_EXTENSION`](#default_parquet_extension) - The default file extension of parquet files

---

## datafusion_common::file_options::file_type::DEFAULT_ARROW_EXTENSION

*Constant*: `&str`

The default file extension of arrow files



## datafusion_common::file_options::file_type::DEFAULT_AVRO_EXTENSION

*Constant*: `&str`

The default file extension of avro files



## datafusion_common::file_options::file_type::DEFAULT_CSV_EXTENSION

*Constant*: `&str`

The default file extension of csv files



## datafusion_common::file_options::file_type::DEFAULT_JSON_EXTENSION

*Constant*: `&str`

The default file extension of json files



## datafusion_common::file_options::file_type::DEFAULT_PARQUET_EXTENSION

*Constant*: `&str`

The default file extension of parquet files



## datafusion_common::file_options::file_type::FileType

*Trait*

Defines the functionality needed for logical planning for
a type of file which will be read or written to storage.

**Methods:**

- `as_any`: Returns the table source as [`Any`] so that it can be



## datafusion_common::file_options::file_type::GetExt

*Trait*

Define each `FileType`/`FileCompressionType`'s extension

**Methods:**

- `get_ext`: File extension getter



