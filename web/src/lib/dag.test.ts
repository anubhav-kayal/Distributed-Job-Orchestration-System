import { describe, expect, it } from "vitest";
import { layout } from "./dag";

describe("layout", () => {
  it("puts each job one column after its deepest dependency", () => {
    const { placed, columns, rows } = layout([
      { key: "extract", depends_on: [] },
      { key: "clean", depends_on: ["extract"] },
      { key: "enrich", depends_on: ["extract"] },
      { key: "load", depends_on: ["clean", "enrich"] },
      { key: "notify", depends_on: ["load", "extract"] },
    ]);
    const at = Object.fromEntries(placed.map((p) => [p.key, [p.column, p.row]]));
    expect(at).toEqual({ extract: [0, 0], clean: [1, 0], enrich: [1, 1], load: [2, 0], notify: [3, 0] });
    expect([columns, rows]).toEqual([4, 2]);
  });

  it("handles independent jobs and an empty graph", () => {
    expect(layout([{ key: "a", depends_on: [] }, { key: "b", depends_on: [] }]).rows).toBe(2);
    expect(layout([])).toEqual({ placed: [], columns: 0, rows: 0 });
  });

  it("refuses a cycle rather than looping", () => {
    expect(() => layout([{ key: "a", depends_on: ["b"] }, { key: "b", depends_on: ["a"] }])).toThrow(/cycle/);
  });
});
