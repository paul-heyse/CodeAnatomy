((use_declaration
   visibility: (visibility_modifier) @import.visibility
   argument: (scoped_identifier) @import.path) @import.declaration
 (#match? @import.path "^([A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*)$")
 (#match? @import.visibility "^pub")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_public")
 (#set! cq.anchor "import.path"))

((use_declaration
   !visibility
   argument: (scoped_identifier) @import.path) @import.declaration
 (#match? @import.path "^([A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*)$")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_private")
 (#set! cq.anchor "import.path"))

((use_declaration
   argument: (identifier) @import.path) @import.declaration
 (#match? @import.path "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_private")
 (#set! cq.anchor "import.path"))

((mod_item name: (identifier) @module.name) @module.item
 (#match? @module.name "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "modules")
 (#set! cq.kind "module_item")
 (#set! cq.anchor "module.name"))

((extern_crate_declaration
   visibility: (visibility_modifier) @import.visibility
   name: (identifier) @import.extern.name) @import.extern
 (#match? @import.extern.name "^[A-Za-z_][A-Za-z0-9_]*$")
 (#match? @import.visibility "^pub")
 (#set! cq.emit "imports")
 (#set! cq.kind "extern_crate_public")
 (#set! cq.anchor "import.extern.name"))

((extern_crate_declaration
   name: (identifier) @import.extern.name) @import.extern
 (#match? @import.extern.name "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "imports")
 (#set! cq.kind "extern_crate_private")
 (#set! cq.anchor "import.extern.name"))
