((function_item name: (identifier) @def.function.name) @def.function
 (#set! cq.emit "definitions")
 (#set! cq.kind "function")
 (#set! cq.anchor "def.function.name"))

((struct_item name: (type_identifier) @def.struct.name) @def.struct
 (#set! cq.emit "definitions")
 (#set! cq.kind "struct")
 (#set! cq.anchor "def.struct.name"))

((enum_item name: (type_identifier) @def.enum.name) @def.enum
 (#set! cq.emit "definitions")
 (#set! cq.kind "enum")
 (#set! cq.anchor "def.enum.name"))

((trait_item name: (type_identifier) @def.trait.name) @def.trait
 (#set! cq.emit "definitions")
 (#set! cq.kind "trait")
 (#set! cq.anchor "def.trait.name"))

((mod_item name: (identifier) @def.module.name) @def.module
 (#set! cq.emit "definitions")
 (#set! cq.kind "module")
 (#set! cq.anchor "def.module.name"))

((macro_definition name: (identifier) @def.macro.name) @def.macro
 (#set! cq.emit "definitions")
 (#set! cq.kind "macro_definition")
 (#set! cq.anchor "def.macro.name"))
