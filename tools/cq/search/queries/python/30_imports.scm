((import_statement
  name: (dotted_name) @import.module) @import.statement
 (#set! cq.emit "imports")
 (#set! cq.kind "import")
 (#set! cq.anchor "import.module"))

((import_statement
  name: (aliased_import
    name: (dotted_name) @import.module
    alias: (identifier) @import.alias)) @import.statement
 (#set! cq.emit "imports")
 (#set! cq.kind "import_alias")
 (#set! cq.anchor "import.module"))

((import_from_statement
  module_name: (_) @import.from.module) @import.from.statement
 (#set! cq.emit "imports")
 (#set! cq.kind "import_from")
 (#set! cq.anchor "import.from.module"))

((import_from_statement
  name: (dotted_name) @import.from.name) @import.from.statement
 (#set! cq.emit "imports")
 (#set! cq.kind "import_from_name")
 (#set! cq.anchor "import.from.name"))

((import_from_statement
  name: (aliased_import
    name: (dotted_name) @import.from.name
    alias: (identifier) @import.from.alias)) @import.from.statement
 (#set! cq.emit "imports")
 (#set! cq.kind "import_from_alias")
 (#set! cq.anchor "import.from.name"))
