((use_declaration
   (visibility_modifier) @import.visibility
   argument: (scoped_identifier) @import.path) @import.declaration
 (#match? @import.path "^([A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*)$")
 (#match? @import.visibility "^pub")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_public")
 (#set! cq.anchor "import.path"))

((use_declaration
   argument: (scoped_identifier) @import.path) @import.declaration
 (#match? @import.path "^([A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*)$")
 (#not-match? @import.declaration "^\\s*pub\\b")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_private")
 (#set! cq.anchor "import.path"))

((use_declaration
   (visibility_modifier) @import.visibility
   argument: (identifier) @import.path) @import.declaration
 (#match? @import.path "^[A-Za-z_][A-Za-z0-9_]*$")
 (#match? @import.visibility "^pub")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_public")
 (#set! cq.anchor "import.path"))

((use_declaration
   argument: (identifier) @import.path) @import.declaration
 (#match? @import.path "^[A-Za-z_][A-Za-z0-9_]*$")
 (#not-match? @import.declaration "^\\s*pub\\b")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_private")
 (#set! cq.anchor "import.path"))

((use_declaration
   (visibility_modifier) @import.visibility
   argument: (scoped_use_list
              path: (scoped_identifier) @import.base
              list: (use_list (identifier) @import.leaf)) @import.path) @import.declaration
 (#match? @import.base "^([A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*)$")
 (#match? @import.leaf "^[A-Za-z_][A-Za-z0-9_]*$")
 (#match? @import.visibility "^pub")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_grouped_public")
 (#set! cq.anchor "import.leaf"))

((use_declaration
   argument: (scoped_use_list
              path: (scoped_identifier) @import.base
              list: (use_list (identifier) @import.leaf)) @import.path) @import.declaration
 (#match? @import.base "^([A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*)$")
 (#match? @import.leaf "^[A-Za-z_][A-Za-z0-9_]*$")
 (#not-match? @import.declaration "^\\s*pub\\b")
 (#set! cq.emit "imports")
 (#set! cq.kind "use_declaration_grouped_private")
 (#set! cq.anchor "import.leaf"))

((mod_item name: (identifier) @module.name) @module.item
 (#match? @module.name "^[A-Za-z_][A-Za-z0-9_]*$")
 (#set! cq.emit "modules")
 (#set! cq.kind "module_item")
 (#set! cq.anchor "module.name"))

((extern_crate_declaration
   (visibility_modifier) @import.visibility
   name: (identifier) @import.extern.name) @import.extern
 (#match? @import.extern.name "^[A-Za-z_][A-Za-z0-9_]*$")
 (#match? @import.visibility "^pub")
 (#set! cq.emit "imports")
 (#set! cq.kind "extern_crate_public")
 (#set! cq.anchor "import.extern.name"))

((extern_crate_declaration
   name: (identifier) @import.extern.name) @import.extern
 (#match? @import.extern.name "^[A-Za-z_][A-Za-z0-9_]*$")
 (#not-match? @import.extern "^\\s*pub\\b")
 (#set! cq.emit "imports")
 (#set! cq.kind "extern_crate_private")
 (#set! cq.anchor "import.extern.name"))
