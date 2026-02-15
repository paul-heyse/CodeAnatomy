((function_item name: (identifier) @structure.function.name) @structure.function
 (#set! cq.emit "structure")
 (#set! cq.kind "function")
 (#set! cq.anchor "structure.function.name"))

((struct_item name: (type_identifier) @structure.struct.name) @structure.struct
 (#set! cq.emit "structure")
 (#set! cq.kind "struct")
 (#set! cq.anchor "structure.struct.name"))

