(import_statement
  name: (dotted_name) @import.module) @import.statement

(import_statement
  name: (aliased_import
    name: (dotted_name) @import.module
    alias: (identifier) @import.alias)) @import.statement

(import_from_statement
  module_name: (_) @import.from.module) @import.from.statement

(import_from_statement
  name: (dotted_name) @import.from.name) @import.from.statement

(import_from_statement
  name: (aliased_import
    name: (dotted_name) @import.from.name
    alias: (identifier) @import.from.alias)) @import.from.statement
