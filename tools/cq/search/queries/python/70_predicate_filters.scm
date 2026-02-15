; Additional high-selectivity predicate filters.

((call function: (identifier) @call.target.identifier) @call
 (#match? @call.target.identifier "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "calls")
 (#set! cq.kind "call_filtered")
 (#set! cq.anchor "call.target.identifier"))

((import_statement name: (dotted_name) @import.module) @import.statement
 (#match? @import.module "^[A-Za-z_][A-Za-z0-9_\\.]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "import_filtered")
 (#set! cq.anchor "import.module"))
