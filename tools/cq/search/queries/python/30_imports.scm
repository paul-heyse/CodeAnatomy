((import_statement
  name: (dotted_name) @import.module) @import.statement
 (#match? @import.module "^[A-Za-z_][A-Za-z0-9_\.]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "import")
 (#set! cq.anchor "import.module"))

((import_statement
  name: (aliased_import
    name: (dotted_name) @import.module
    alias: (identifier) @import.alias)) @import.statement
 (#match? @import.module "^[A-Za-z_][A-Za-z0-9_\.]*$")
 (#match? @import.alias "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "import_alias")
 (#set! cq.anchor "import.module"))

((import_from_statement
  module_name: (_) @import.from.module) @import.from.statement
 (#match? @import.from.module "^[A-Za-z_][A-Za-z0-9_\.]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "import_from")
 (#set! cq.anchor "import.from.module"))

((import_from_statement
  name: (dotted_name) @import.from.name) @import.from.statement
 (#match? @import.from.name "^[A-Za-z_][A-Za-z0-9_\.]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "import_from_name")
 (#set! cq.anchor "import.from.name"))

((import_from_statement
  name: (aliased_import
    name: (dotted_name) @import.from.name
    alias: (identifier) @import.from.alias)) @import.from.statement
 (#match? @import.from.name "^[A-Za-z_][A-Za-z0-9_\.]*$")
 (#match? @import.from.alias "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "import_from_alias")
 (#set! cq.anchor "import.from.name"))
