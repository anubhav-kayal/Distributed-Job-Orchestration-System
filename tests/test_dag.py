import pytest

from djos.workflows.dag import InvalidWorkflow, topological_order


def assert_valid_order(graph, order):
    position = {key: i for i, key in enumerate(order)}
    assert set(order) == set(graph)
    for key, deps in graph.items():
        for dep in deps:
            assert position[dep] < position[key], f"{dep} must come before {key}"


@pytest.mark.parametrize(
    "graph",
    [
        {"a": []},
        {"a": [], "b": ["a"], "c": ["b"]},  # chain
        {"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]},  # diamond
        {"x": [], "y": []},  # independent
        {"a": [], "b": ["a", "a"]},  # duplicate edge is harmless
    ],
)
def test_valid_graphs_get_a_dependency_respecting_order(graph):
    assert_valid_order(graph, topological_order(graph))


def test_diamond_order_is_deterministic():
    assert topological_order({"d": ["b", "c"], "c": ["a"], "b": ["a"], "a": []}) == [
        "a",
        "b",
        "c",
        "d",
    ]


def test_cycle_is_rejected_and_named():
    with pytest.raises(InvalidWorkflow, match=r"cycle among jobs \['a', 'b', 'c'\]"):
        topological_order({"a": ["c"], "b": ["a"], "c": ["b"]})


def test_nodes_downstream_of_a_cycle_are_reported_too():
    with pytest.raises(InvalidWorkflow, match=r"\['a', 'b', 'c'\]"):
        topological_order({"root": [], "a": ["b"], "b": ["a"], "c": ["a"]})


def test_self_dependency_is_rejected():
    with pytest.raises(InvalidWorkflow, match="depends on itself"):
        topological_order({"a": ["a"]})


def test_unknown_dependency_is_rejected():
    with pytest.raises(InvalidWorkflow, match="unknown job 'ghost'"):
        topological_order({"a": ["ghost"]})


def test_large_chain_is_linear_time():
    graph = {f"n{i}": [f"n{i - 1}"] if i else [] for i in range(5000)}
    assert topological_order(graph)[-1] == "n4999"
