(call function: (identifier) @call.function.identifier)
(call function: (attribute) @call.function.attribute)

(function_definition name: (identifier) @def.function.name)
(class_definition name: (identifier) @class.definition.name)

(import_statement
  name: (dotted_name) @import.module)

(import_statement
  name: (aliased_import
    name: (dotted_name) @import.module
    alias: (identifier) @import.alias))

(import_from_statement
  module_name: (_) @import.from.module)

(import_from_statement
  name: (dotted_name) @import.from.name)

(import_from_statement
  name: (aliased_import
    name: (dotted_name) @import.from.name
    alias: (identifier) @import.from.alias))
