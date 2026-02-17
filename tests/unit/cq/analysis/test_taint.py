from __future__ import annotations

from tools.cq.analysis.taint import analyze_function_node, find_function_node


def test_find_function_node_top_level() -> None:
    source = """
def target(a):
    return a
"""
    node = find_function_node(
        source,
        function_name="target",
        function_line=2,
        class_name=None,
    )
    assert node is not None


def test_analyze_function_node_tracks_assign_call_and_return() -> None:
    source = """
def target(a):
    x = a
    helper(x)
    return x
"""
    node = find_function_node(
        source,
        function_name="target",
        function_line=2,
        class_name=None,
    )
    assert node is not None

    result = analyze_function_node(
        file="sample.py",
        function_node=node,
        tainted_params={"a"},
        depth=0,
    )

    kinds = [site.kind for site in result.sites]
    assert "assign" in kinds
    assert "call" in kinds
    assert "return" in kinds
    assert len(result.calls) == 1
