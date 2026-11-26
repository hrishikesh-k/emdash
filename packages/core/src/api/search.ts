import type { Kysely } from "kysely";

import { searchBackend } from "../database/dialect-helpers.js";
import type { Database } from "../database/types.js";
import { apiError } from "./error.js";

export function requireSearchSupport(db: Kysely<Database>): Response | null {
	if (searchBackend(db)) return null;
	return apiError(
		"SEARCH_UNAVAILABLE",
		"Built-in search is not enabled for this PostgreSQL database. " +
			"Pass `search: true` to `postgres()`, or wrap a custom dialect with `withPostgresSearch()`.",
		503,
	);
}
