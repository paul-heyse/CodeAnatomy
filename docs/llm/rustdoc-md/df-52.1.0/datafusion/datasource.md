**datafusion > datasource**

# Module: datasource

## Contents

**Modules**

- [`dynamic_file`](#dynamic_file) - dynamic_file_schema contains an [`UrlTableFactory`] implementation that
- [`empty`](#empty) - [`EmptyTable`] useful for testing.
- [`file_format`](#file_format) - Module containing helper methods for the various file formats
- [`listing`](#listing) - A table that uses the `ObjectStore` listing capability
- [`listing_table_factory`](#listing_table_factory) - Factory for creating ListingTables with default options
- [`physical_plan`](#physical_plan) - Execution plans that read file formats
- [`provider`](#provider) - Data source traits

---

## Module: dynamic_file

dynamic_file_schema contains an [`UrlTableFactory`] implementation that
can create a [`ListingTable`] from the given url.



## Module: empty

[`EmptyTable`] useful for testing.



## Module: file_format

Module containing helper methods for the various file formats
See write.rs for write related helper methods



## Module: listing

A table that uses the `ObjectStore` listing capability
to get the list of files to process.



## Module: listing_table_factory

Factory for creating ListingTables with default options



## Module: physical_plan

Execution plans that read file formats



## Module: provider

Data source traits



