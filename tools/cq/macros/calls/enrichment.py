"""Call-site enrichment helpers (symtable + bytecode)."""

from __future__ import annotations


def enrich_call_site(
    source: str,
    containing_fn: str,
    rel_path: str,
) -> dict[str, dict[str, object] | None]:
    """Enrich call site with symtable and bytecode signals.

    Returns:
        dict[str, dict[str, object] | None]: Enrichment payload by source plane.
    """
    from tools.cq.query.enrichment import analyze_bytecode, analyze_symtable

    enrichment: dict[str, dict[str, object] | None] = {
        "symtable": None,
        "bytecode": None,
    }

    symtable_info = analyze_symtable(source, rel_path)
    if containing_fn in symtable_info:
        st_info = symtable_info[containing_fn]
        enrichment["symtable"] = {
            "is_closure": len(st_info.free_vars) > 0,
            "free_vars": list(st_info.free_vars),
            "globals_used": list(st_info.globals_used),
            "nested_scopes": st_info.nested_scopes,
        }

    try:
        code = compile(source, rel_path, "exec")
        for const in code.co_consts:
            if hasattr(const, "co_name") and const.co_name == containing_fn:
                bc_info = analyze_bytecode(const)
                enrichment["bytecode"] = {
                    "load_globals": list(bc_info.load_globals),
                    "load_attrs": list(bc_info.load_attrs),
                    "call_functions": list(bc_info.call_functions),
                }
                break
    except (SyntaxError, ValueError, TypeError):
        pass

    return enrichment


__all__ = ["enrich_call_site"]
