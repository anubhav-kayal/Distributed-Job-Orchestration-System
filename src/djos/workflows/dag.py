"""Validating a workflow's dependency graph.

A workflow is a set of named jobs plus "B depends on A" edges. It must be a DAG (directed
acyclic graph): if A waits for B and B waits for A, neither can ever start.

Cycle detection uses Kahn's algorithm, which is also a topological sort:
  1. Count each node's incoming edges (its unfinished dependencies).
  2. Put every node with zero dependencies in a ready list.
  3. Repeatedly take a ready node, output it, and decrement each of its dependents;
     any that reach zero become ready.
If every node is output, the graph is acyclic and the output is a valid run order. Nodes
left over are exactly those on a cycle or downstream of one. It runs in O(V + E).
"""

from collections import deque
from collections.abc import Mapping, Sequence


class InvalidWorkflow(ValueError):
    pass


def topological_order(depends_on: Mapping[str, Sequence[str]]) -> list[str]:
    """Return the node keys in an order where every node comes after its dependencies.

    `depends_on` maps each node to the nodes it waits for. Raises InvalidWorkflow for an
    unknown dependency, a self-dependency, or a cycle.
    """
    dependents: dict[str, list[str]] = {key: [] for key in depends_on}
    remaining = {}
    for key, deps in depends_on.items():
        for dep in deps:
            if dep == key:
                raise InvalidWorkflow(f"job {key!r} depends on itself")
            if dep not in depends_on:
                raise InvalidWorkflow(f"job {key!r} depends on unknown job {dep!r}")
            dependents[dep].append(key)
        remaining[key] = len(set(deps))

    ready = deque(sorted(k for k, n in remaining.items() if n == 0))
    order = []
    while ready:
        key = ready.popleft()
        order.append(key)
        for child in sorted(set(dependents[key])):
            remaining[child] -= 1
            if remaining[child] == 0:
                ready.append(child)

    if len(order) != len(depends_on):
        stuck = sorted(k for k, n in remaining.items() if n > 0)
        raise InvalidWorkflow(f"dependency cycle among jobs {stuck}")
    return order
