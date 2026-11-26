import { describe, expect, it } from "vitest";

import { fts5OperatorQueryToTsquery } from "../../../src/search/postgres.js";

describe("fts5OperatorQueryToTsquery", () => {
	it.each([
		["alpha OR beta", "'alpha' | 'beta'"],
		["alpha NOT beta", "'alpha' & !'beta'"],
		["(alpha OR beta) AND gamma", "('alpha' | 'beta') & 'gamma'"],
		["alpha NOT beta gamma", "'alpha' & !('beta' & 'gamma')"],
		["alpha NOT beta AND gamma", "'alpha' & !'beta' & 'gamma'"],
		["alpha OR beta gamma", "'alpha' | ('beta' & 'gamma')"],
		["NEAR(alpha beta, 5) OR soup", "('alpha' & 'beta') | 'soup'"],
		["alpha NEAR beta", "('alpha' & 'NEAR' & 'beta')"],
		['"alpha beta" OR soup', "'alpha beta' | 'soup'"],
		["alph* AND soup", "'alph':* & 'soup'"],
	])("translates %s", (query, expected) => {
		expect(fts5OperatorQueryToTsquery(query)).toBe(expected);
	});

	it.each([
		"NOT alpha",
		"alpha AND",
		"alpha OR (beta",
		"alpha OR beta)",
		"alpha, beta OR gamma",
		"NEAR() OR alpha",
		"NEAR(alpha, x) OR beta",
		'"" OR alpha',
	])("rejects malformed %s", (query) => {
		expect(fts5OperatorQueryToTsquery(query)).toBeNull();
	});

	it("keeps quotes and backslashes inside lexemes", () => {
		expect(fts5OperatorQueryToTsquery("it's OR a\\b")).toBe("'it''s' | 'a\\\\b'");
	});
});
