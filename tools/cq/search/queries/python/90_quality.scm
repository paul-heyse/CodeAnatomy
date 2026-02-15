((ERROR) @quality.error
 (#set! cq.emit "diagnostics")
 (#set! cq.kind "ERROR")
 (#set! cq.anchor "quality.error"))

((MISSING) @quality.missing
 (#set! cq.emit "diagnostics")
 (#set! cq.kind "MISSING")
 (#set! cq.anchor "quality.missing"))
