import { describe, expect, it } from "vitest";
import { ago, duration, shortId } from "./time";

describe("duration", () => {
  it.each([
    [0, "0s"],
    [4_900, "4s"],
    [125_000, "2m 05s"],
    [4_320_000, "1h 12m"],
    [3 * 86_400_000, "3d"],
  ])("%i ms → %s", (ms, text) => expect(duration(ms)).toBe(text));
});

describe("ago", () => {
  const now = Date.parse("2030-01-01T12:00:00Z");
  it("is relative for recent times and absolute for old ones", () => {
    expect(ago("2030-01-01T11:59:59.5Z", now)).toBe("just now");
    expect(ago("2030-01-01T12:00:00.4Z", now)).toBe("just now"); // a hair in the future
    expect(ago("2030-01-01T11:59:48Z", now)).toBe("12s ago");
    expect(ago("2030-01-01T11:57:00Z", now)).toBe("3m ago");
    expect(ago("2030-01-01T12:05:00Z", now)).toBe("in 5m 00s");
    expect(ago(null, now)).toBe("—");
  });
});

it("shortens ids", () => expect(shortId("0238b68a-a918-4ff9")).toBe("0238b68a"));
