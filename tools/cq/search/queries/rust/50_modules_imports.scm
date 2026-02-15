((use_declaration argument: (scoped_identifier) @import.path) @import.declaration
 (#match? @import.path "^([A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*)$")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration")
 (#set! cq.anchor "import.path"))

((use_declaration argument: (scoped_identifier
    path: (_) @import.path)) @import.declaration
 (#match? @import.path "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration")
 (#set! cq.anchor "import.path"))

((mod_item name: (identifier) @module.name) @module.item
 (#match? @module.name "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "modules")
 (#set! cq.kind "module_item")
 (#set! cq.anchor "module.name"))

((extern_crate_declaration name: (identifier) @import.extern.name) @import.extern
 (#match? @import.extern.name "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "extern_crate")
 (#set! cq.anchor "import.extern.name"))
