import type { Dialect, DialectAdapter } from "kysely";

import type { PostgresSearchAdapter } from "../database/dialect-helpers.js";

/**
 * Enable EmDash's built-in full-text search on a PostgreSQL dialect.
 *
 * The built-in `postgres()` adapter does this when given `search: true`.
 * Custom adapters wrap the dialect their `createDialect()` returns:
 *
 * @example
 * ```ts
 * export function createDialect(config) {
 *   return withPostgresSearch(new PostgresDialect({ pool: new Pool(config) }));
 * }
 * ```
 */
export function withPostgresSearch<D extends Dialect>(dialect: D): D {
	const createAdapter = dialect.createAdapter.bind(dialect);
	dialect.createAdapter = (): DialectAdapter & PostgresSearchAdapter =>
		Object.assign(createAdapter(), { postgresSearch: true as const });
	return dialect;
}
