/**
 * Layout for a workflow diagram: each job goes in the column after its deepest
 * dependency (longest-path layering), and within a column jobs keep submission order.
 * Assumes a DAG, which the API guarantees.
 */

export interface DagInput {
  key: string;
  depends_on: string[];
}

export interface Placed {
  key: string;
  column: number;
  row: number;
}

export function layout(nodes: DagInput[]): { placed: Placed[]; columns: number; rows: number } {
  const byKey = new Map(nodes.map((n) => [n.key, n]));
  const depth = new Map<string, number>();

  const depthOf = (key: string, seen: Set<string> = new Set()): number => {
    const known = depth.get(key);
    if (known !== undefined) return known;
    if (seen.has(key)) throw new Error(`cycle at ${key}`);
    seen.add(key);
    const deps = byKey.get(key)?.depends_on ?? [];
    const d = deps.length ? 1 + Math.max(...deps.map((dep) => depthOf(dep, seen))) : 0;
    depth.set(key, d);
    return d;
  };

  const rowsUsed: number[] = [];
  const placed = nodes.map((n) => {
    const column = depthOf(n.key);
    const row = rowsUsed[column] ?? 0;
    rowsUsed[column] = row + 1;
    return { key: n.key, column, row };
  });
  return {
    placed,
    columns: rowsUsed.length,
    rows: Math.max(0, ...rowsUsed),
  };
}
