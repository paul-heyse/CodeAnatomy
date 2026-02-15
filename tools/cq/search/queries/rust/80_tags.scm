((function_item
   name: (identifier) @name) @role.definition
 (#set! cq.emit "tags")
 (#set! cq.kind "definition.function")
 (#set! cq.anchor "name"))

((struct_item
   name: (type_identifier) @name) @role.definition
 (#set! cq.emit "tags")
 (#set! cq.kind "definition.struct")
 (#set! cq.anchor "name"))

((enum_item
   name: (type_identifier) @name) @role.definition
 (#set! cq.emit "tags")
 (#set! cq.kind "definition.enum")
 (#set! cq.anchor "name"))

((trait_item
   name: (type_identifier) @name) @role.definition
 (#set! cq.emit "tags")
 (#set! cq.kind "definition.trait")
 (#set! cq.anchor "name"))

((mod_item
   name: (identifier) @name) @role.definition
 (#set! cq.emit "tags")
 (#set! cq.kind "definition.module")
 (#set! cq.anchor "name"))

((macro_definition
   name: (identifier) @name) @role.definition
 (#set! cq.emit "tags")
 (#set! cq.kind "definition.macro")
 (#set! cq.anchor "name"))

((call_expression
   function: (identifier) @name) @role.reference
 (#set! cq.emit "tags")
 (#set! cq.kind "reference.call")
 (#set! cq.anchor "name"))

((call_expression
   function: (scoped_identifier
               name: (identifier) @name)) @role.reference
 (#set! cq.emit "tags")
 (#set! cq.kind "reference.call")
 (#set! cq.anchor "name"))

((macro_invocation
   macro: (identifier) @name) @role.reference
 (#set! cq.emit "tags")
 (#set! cq.kind "reference.macro")
 (#set! cq.anchor "name"))
