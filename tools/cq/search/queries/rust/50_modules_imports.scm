((use_declaration argument: (_) @import.path) @import.declaration
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration")
 (#set! cq.anchor "import.path"))

((mod_item name: (identifier) @module.name) @module.item
 (#set! cq.emit "modules")
 (#set! cq.kind "module_item")
 (#set! cq.anchor "module.name"))

((extern_crate_declaration name: (identifier) @import.extern.name) @import.extern
 (#set! cq.emit "imports")
 (#set! cq.kind "extern_crate")
 (#set! cq.anchor "import.extern.name"))
