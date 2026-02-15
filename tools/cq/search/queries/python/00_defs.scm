((function_definition name: (identifier) @def.function.name) @def.function
 (#set! cq.emit "definitions")
 (#set! cq.kind "function")
 (#set! cq.anchor "def.function.name"))

((class_definition name: (identifier) @def.class.name) @def.class
 (#set! cq.emit "definitions")
 (#set! cq.kind "class")
 (#set! cq.anchor "def.class.name"))
