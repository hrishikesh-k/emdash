import { describe, expect, it } from "vitest";

import { postgresWeighting } from "../../../src/search/postgres.js";

describe("postgresWeighting", () => {
	it("returns null without weights or without a positive weight", () => {
		expect(postgresWeighting(["title"], undefined)).toBeNull();
		expect(postgresWeighting(["title", "body"], { title: 0, body: 0 })).toBeNull();
	});

	it("reproduces up to four distinct weights relative to the largest", () => {
		const weighting = postgresWeighting(["title", "body", "summary", "tags"], {
			title: 10,
			body: 5,
			summary: 2,
			tags: 1,
		});
		expect(Object.fromEntries(weighting!.classes)).toEqual({
			title: "A",
			body: "B",
			summary: "C",
			tags: "D",
		});
		expect(weighting!.rankWeights).toEqual([0.1, 0.2, 0.5, 1]);
	});

	it("gives fields without a configured weight a weight of 1", () => {
		const weighting = postgresWeighting(["title", "body"], { title: 4 });
		expect(Object.fromEntries(weighting!.classes)).toEqual({ title: "A", body: "B" });
		expect(weighting!.rankWeights).toEqual([0, 0, 0.25, 1]);
	});

	it("shares label D among the lowest weights beyond four, at their average", () => {
		const weighting = postgresWeighting(["a", "b", "c", "d", "e"], {
			a: 8,
			b: 6,
			c: 4,
			d: 2,
			e: 0,
		});
		expect(weighting!.classes.get("d")).toBe("D");
		expect(weighting!.classes.get("e")).toBe("D");
		expect(weighting!.rankWeights).toEqual([0.125, 0.5, 0.75, 1]);
	});

	it("never produces a rank weight outside 0 to 1", () => {
		const weighting = postgresWeighting(["a", "b"], { a: 1000, b: -5 });
		for (const weight of weighting!.rankWeights) {
			expect(weight).toBeGreaterThanOrEqual(0);
			expect(weight).toBeLessThanOrEqual(1);
		}
	});
});
