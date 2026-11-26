/**
 * Search Query Functions
 *
 * Programmatic API for searching content using FTS5.
 */

import type { Kysely, RawBuilder } from "kysely";
import { sql } from "kysely";

import { searchBackend } from "../database/dialect-helpers.js";
import { encodeCursor, decodeCursor, InvalidCursorError } from "../database/repositories/types.js";
import type { Database } from "../database/types.js";
import { validateIdentifier } from "../database/validate.js";
import { resolveConfiguredLocale } from "../i18n/config.js";
import { localeMatches } from "../i18n/query.js";
import { getDb } from "../loader.js";
import { FTSManager } from "./fts-manager.js";
import type { CollectionSearchMetadata } from "./fts-manager.js";
import {
	buildPostgresTsQuery,
	columnVector,
	PG_DOCUMENT_COLUMN,
	postgresTextSearchConfig,
	postgresWeighting,
} from "./postgres.js";
import type {
	SearchOptions,
	CollectionSearchOptions,
	SearchResult,
	SearchResponse,
	SuggestOptions,
	Suggestion,
	SearchStats,
} from "./types.js";

/**
 * Marker stored in the `id` slot of a search pagination cursor.
 *
 * Search results are merged from per-collection FTS queries and re-sorted by
 * score, so there is no single stable keyset column to encode (the way
 * `getEmDashCollection` encodes `(orderValue, id)`). We page over the merged,
 * score-sorted set by offset instead, carried as the cursor's `orderValue`.
 * Reusing `encodeCursor`/`decodeCursor` keeps the opaque base64-JSON shape and
 * the `InvalidCursorError` handling consistent with the rest of the API.
 */
const SEARCH_CURSOR_MARKER = "search";

/** Encode the next-page offset into an opaque search cursor. */
function encodeSearchCursor(offset: number): string {
	return encodeCursor(String(offset), SEARCH_CURSOR_MARKER);
}

/**
 * Decode a search cursor back to its offset. Throws `InvalidCursorError` for a
 * malformed cursor or one that doesn't carry a non-negative integer offset, so
 * a client pagination bug surfaces immediately rather than silently restarting
 * from the first page.
 */
/** Upper bound on a decoded search offset, to cap the per-collection row fetch a forged cursor can trigger. */
const MAX_SEARCH_OFFSET = 10_000;

function decodeSearchOffset(cursor: string): number {
	const { orderValue, id } = decodeCursor(cursor);
	if (id !== SEARCH_CURSOR_MARKER) {
		throw new InvalidCursorError(cursor);
	}
	const offset = Number(orderValue);
	if (!Number.isInteger(offset) || offset < 0 || offset > MAX_SEARCH_OFFSET) {
		throw new InvalidCursorError(cursor);
	}
	return offset;
}

/** Pattern to split on whitespace for query term extraction */
const WHITESPACE_SPLIT_PATTERN = /\s+/;
// FTS5 operators are uppercase-only; a case-insensitive match would treat
// ordinary words as syntax and skip the safe quoting path below.
const FTS_OPERATORS_PATTERN = /\b(AND|OR|NOT|NEAR)\b/;
const DOUBLE_QUOTE_PATTERN = /"/g;

/**
 * Detect search query syntax errors. Match specifically on the error
 * fingerprints for malformed user input rather than a broad "fts5" /
 * "syntax error" filter (which would also swallow internal table-corruption
 * errors). The fingerprints we care about are:
 *
 *  - "fts5: syntax error near …" — unbalanced quotes, stray operators,
 *    other malformed user input
 *  - "unknown special query: …" — bare special tokens like `^*` that
 *    parse but don't resolve to a real FTS5 directive
 *  - "syntax error in tsquery" / "no operand in tsquery" — the PostgreSQL
 *    equivalents for malformed operator queries
 */
function isSearchSyntaxError(error: unknown): boolean {
	if (!(error instanceof Error)) return false;
	const message = error.message.toLowerCase();
	return (
		message.includes("fts5: syntax error") ||
		message.includes("unknown special query") ||
		message.includes("syntax error in tsquery") ||
		message.includes("no operand in tsquery")
	);
}

// All queries must start before awaiting, but collection order determines tie
// ordering and which error is surfaced when several collections fail.
async function inCollectionOrder<T>(pending: Promise<T>[]): Promise<T[]> {
	const settled = await Promise.allSettled(pending);
	return settled.map((outcome) => {
		if (outcome.status === "rejected") throw outcome.reason;
		return outcome.value;
	});
}

/**
 * Search across multiple collections
 *
 * Public API that auto-injects the database.
 *
 * @param query - Search query (FTS5 syntax supported)
 * @param options - Search options
 * @returns Search results with pagination
 *
 * @example
 * ```typescript
 * import { search } from "emdash";
 *
 * const results = await search("hello world", {
 *   collections: ["posts", "pages"],
 *   limit: 20
 * });
 * ```
 */
export async function search(query: string, options: SearchOptions = {}): Promise<SearchResponse> {
	const db = await getDb();
	return searchWithDb(db, query, options);
}

/**
 * Search across multiple collections (with explicit db)
 *
 * @internal Use `search()` in templates. This variant is for admin routes
 * that already have a database handle.
 *
 * @param db - Kysely database instance
 * @param query - Search query (FTS5 syntax supported)
 * @param options - Search options
 * @returns Search results with pagination
 */
export async function searchWithDb(
	db: Kysely<Database>,
	query: string,
	options: SearchOptions = {},
): Promise<SearchResponse> {
	const ftsManager = new FTSManager(db);
	const limit = options.limit ?? 20;
	const status = options.status ?? "published";
	const offset = options.cursor ? decodeSearchOffset(options.cursor) : 0;

	if (!escapeQuery(query)) return { items: [] };

	const metadata = await ftsManager.getSearchMetadata(options.collections);
	const collections = options.collections?.length ? options.collections : [...metadata.keys()];

	// To rank the merged window [offset, offset + limit) correctly, each
	// collection must contribute its own top (offset + limit) rows — in the
	// worst case the entire window comes from one collection. The extra +1
	// row detects whether a further page exists.
	const perCollectionLimit = offset + limit + 1;

	const collectionResults = await inCollectionOrder(
		collections.map(async (collection) => {
			const collectionMetadata = metadata.get(collection);
			if (!collectionMetadata) return [];
			return searchSingleCollection(
				db,
				collection,
				query,
				{
					status,
					locale: options.locale,
					limit: perCollectionLimit,
					scope: options.scope,
				},
				collectionMetadata,
				true,
			);
		}),
	);
	const allResults = collectionResults.flat();

	allResults.sort(
		(a, b) =>
			b.titleMatchTier - a.titleMatchTier ||
			(a.titleMatchTier === 1 ? a.titleLength - b.titleLength : 0) ||
			b.score - a.score ||
			b.rawScore - a.rawScore ||
			a.titleLength - b.titleLength ||
			compareIds(a.id, b.id),
	);

	// Page the merged set by offset. A nextCursor is issued only when at least
	// one result exists past this page.
	const items = allResults.slice(offset, offset + limit).map(toSearchResult);
	const hasMore = allResults.length > offset + limit;
	const nextCursor = hasMore ? encodeSearchCursor(offset + limit) : undefined;

	return { items, nextCursor };
}

/**
 * Search within a single collection
 *
 * @param db - Kysely database instance
 * @param collection - Collection slug
 * @param query - Search query (FTS5 syntax supported)
 * @param options - Search options
 * @returns Search results with pagination
 *
 * @example
 * ```typescript
 * const results = await searchCollection(db, "posts", "hello world", {
 *   limit: 10
 * });
 * ```
 */
export async function searchCollection(
	db: Kysely<Database>,
	collection: string,
	query: string,
	options: CollectionSearchOptions = {},
): Promise<SearchResponse> {
	const limit = options.limit ?? 20;
	const offset = options.cursor ? decodeSearchOffset(options.cursor) : 0;
	if (!escapeQuery(query)) return { items: [] };

	const ftsManager = new FTSManager(db);
	const metadata = (await ftsManager.getSearchMetadata([collection])).get(collection);
	if (!metadata) return { items: [] };

	// Over-fetch the [offset, offset + limit) window plus one row to detect a
	// further page, then slice. Keeps the FTS SQL (LIMIT-only) unchanged and
	// the cursor contract identical to the cross-collection path.
	const fetched = await searchSingleCollection(
		db,
		collection,
		query,
		{
			status: options.status,
			locale: options.locale,
			limit: offset + limit + 1,
			scope: options.scope,
		},
		metadata,
	);

	const items = fetched.slice(offset, offset + limit).map(toSearchResult);
	const hasMore = fetched.length > offset + limit;
	const nextCursor = hasMore ? encodeSearchCursor(offset + limit) : undefined;

	return { items, nextCursor };
}

interface RankedSearchResult extends SearchResult {
	titleMatchTier: number;
	rawScore: number;
	titleLength: number;
}

function toSearchResult({
	titleMatchTier: _titleMatchTier,
	rawScore: _rawScore,
	titleLength: _titleLength,
	...result
}: RankedSearchResult): SearchResult {
	return result;
}

function compareIds(a: string, b: string): number {
	return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Internal function to search a single collection
 */
async function searchSingleCollection(
	db: Kysely<Database>,
	collection: string,
	query: string,
	options: CollectionSearchOptions,
	metadata: CollectionSearchMetadata,
	rankAcrossCollections = false,
): Promise<RankedSearchResult[]> {
	// Validate before any raw SQL interpolation
	validateIdentifier(collection, "collection slug");

	if (searchBackend(db) === "postgres") {
		return searchSingleCollectionPostgres(
			db,
			collection,
			query,
			options,
			metadata,
			rankAcrossCollections,
		);
	}

	const ftsManager = new FTSManager(db);
	const ftsTable = ftsManager.getFtsTableName(collection);
	const contentTable = ftsManager.getContentTableName(collection);
	const limit = options.limit ?? 20;
	const status = options.status ?? "published";
	const locale = options.locale ? resolveConfiguredLocale(options.locale) : undefined;

	// Escape the query for FTS5
	const escapedQuery = escapeQuery(query);
	if (!escapedQuery) {
		return [];
	}

	// Get searchable fields for snippet generation
	const { searchableFields, hasTitle, config } = metadata;
	const { weights, titleField } = config;

	// Title scope restricts the FTS5 match to the collection's title column.
	// The scoped column must be one of the indexed searchable fields; a
	// collection whose title is not indexed cannot match by title. Operators
	// are disabled so every term stays quoted inside the filter's group.
	let matchQuery = escapedQuery;
	if (options.scope === "title") {
		const titleColumn =
			titleField && searchableFields.includes(titleField)
				? titleField
				: searchableFields.includes("title")
					? "title"
					: undefined;
		if (!titleColumn) {
			return [];
		}
		validateIdentifier(titleColumn, "title field");
		matchQuery = `${titleColumn} : (${escapeQuery(query, false)})`;
	}

	// `title` is an optional user-defined field, not a system column. Only
	// select it when the collection actually has one; otherwise the query
	// errors with "no such column: c.title".
	// A configured titleField drives the result title so search shows
	// the same value as the content list. It's a validated existing field, so
	// the column exists; otherwise fall back to the optional `title` column.
	let titleExpr;
	if (titleField) {
		validateIdentifier(titleField, "title field");
		titleExpr = sql`c.${sql.ref(titleField)}`;
	} else {
		titleExpr = hasTitle ? sql`c.title` : sql`NULL`;
	}

	// Build weight string for bm25 if weights provided
	// Format: bm25(table, weight1, weight2, ...)
	// First two weights are for 'id' and 'locale' columns (UNINDEXED, so 0)
	let bm25Args = "";
	if (weights && searchableFields.length > 0) {
		const weightValues = ["0", "0"]; // id column, locale column
		for (const field of searchableFields) {
			weightValues.push(String(weights[field] ?? 1));
		}
		bm25Args = weightValues.join(", ");
	}

	// Build and execute the search query
	// Using raw SQL because Kysely doesn't have FTS5 support
	const bm25Expr = bm25Args ? `bm25("${ftsTable}", ${bm25Args})` : `bm25("${ftsTable}")`;

	// Materialization keeps bm25() in its FTS cursor context. The best-score
	// CTE computes the global per-collection maximum from every match, not
	// just the rows admitted by the title-ranked LIMIT, so normalized scores
	// stay identical across pages and result-window sizes.
	const titleColumn = titleField ?? "title";
	const titleMatchTier =
		rankAcrossCollections && searchableFields.includes(titleColumn)
			? buildTitleMatchTier(ftsTable, titleColumn, searchableFields.indexOf(titleColumn) + 2, query)
			: sql`0`;
	const titleLengthExpr = searchableFields.includes(titleColumn)
		? sql`coalesce(length(c.${sql.ref(titleColumn)}), 0)`
		: sql`0`;
	const fromMatches = sql`FROM "${sql.raw(ftsTable)}" f
		JOIN "${sql.raw(contentTable)}" c ON f.id = c.id
		WHERE "${sql.raw(ftsTable)}" MATCH ${matchQuery}
		AND c.status = ${status}
		AND c.deleted_at IS NULL
		${locale ? sql`AND ${localeMatches("c.locale", locale)}` : sql``}`;
	const displayFields = sql`c.id, c.slug, c.locale, ${titleExpr} AS title,
		snippet("${sql.raw(ftsTable)}", 2, '<mark>', '</mark>', '...', 32) AS snippet`;
	let results;
	try {
		// D1 coalesces only statements beginning with SELECT; keep the CTEs
		// inside a read-only outer SELECT so collection queries can batch.
		const rankedQuery = rankAcrossCollections
			? sql<SearchRow>`SELECT * FROM (WITH matches AS MATERIALIZED (
				SELECT c.id, ${sql.raw(bm25Expr)} AS score, ${titleMatchTier} AS title_match_tier, ${titleLengthExpr} AS title_length
				${fromMatches}
			), best AS MATERIALIZED (
				SELECT nullif(max(abs(score)), 0) AS best_score FROM matches
			), ranked AS MATERIALIZED (
				SELECT m.id, m.title_match_tier, m.title_length,
					abs(m.score) AS raw_score,
					coalesce(abs(m.score) / nullif(best.best_score, 0), 0) AS score
				FROM matches m CROSS JOIN best
				ORDER BY m.title_match_tier DESC,
					CASE WHEN m.title_match_tier = 1 THEN m.title_length ELSE 0 END ASC,
					score DESC, raw_score DESC, m.title_length ASC, m.id COLLATE BINARY
				LIMIT ${limit}
			)
			SELECT ${displayFields}, r.score, r.title_match_tier, r.raw_score, r.title_length
			FROM ranked r
			JOIN "${sql.raw(contentTable)}" c ON c.id = r.id
			JOIN "${sql.raw(ftsTable)}" f ON f.id = c.id
			WHERE "${sql.raw(ftsTable)}" MATCH ${matchQuery})
			ORDER BY title_match_tier DESC,
				CASE WHEN title_match_tier = 1 THEN title_length ELSE 0 END ASC,
				score DESC, raw_score DESC, title_length ASC, id COLLATE BINARY`
			: sql<SearchRow>`SELECT ${displayFields}, ${sql.raw(bm25Expr)} AS score, 0 AS title_match_tier, abs(${sql.raw(bm25Expr)}) AS raw_score, 0 AS title_length
				${fromMatches} ORDER BY score, c.id COLLATE BINARY LIMIT ${limit}`;
		results = await rankedQuery.execute(db);
	} catch (error) {
		// FTS5 returns syntax errors for queries with unbalanced quotes,
		// stray operators, or other malformed input. Treat these as
		// "no matches" so the user gets an empty result rather than an
		// internals-leaking error. Other errors (table missing, IO) still
		// propagate. Intentionally not logged: any anonymous client can
		// trigger this path, and the underlying error message embeds the
		// raw query, so logging would be both noisy and a log-injection
		// vector.
		if (isSearchSyntaxError(error)) {
			return [];
		}
		throw error;
	}

	return results.rows.map((row) => ({
		collection,
		id: row.id,
		slug: row.slug,
		locale: row.locale,
		title: row.title ?? undefined,
		// SQLite's snippet() returns NULL when the targeted column is
		// NULL for that row — even if the row matched via a different
		// searchable column. Skip sanitization in that case so we don't
		// throw on `null.replace`. The SearchResult.snippet field is
		// already optional, so omitting it is the documented contract.
		snippet: row.snippet === null ? undefined : sanitizeSnippet(row.snippet),
		score: Math.abs(row.score),
		titleMatchTier: row.title_match_tier,
		rawScore: row.raw_score,
		titleLength: row.title_length,
	}));
}

type SearchRow = {
	id: string;
	slug: string | null;
	locale: string;
	title: string | null;
	snippet: string | null;
	score: number;
	title_match_tier: number;
	raw_score: number;
	title_length: number;
};

function toRankedResult(collection: string, row: SearchRow): RankedSearchResult {
	return {
		collection,
		id: row.id,
		slug: row.slug,
		locale: row.locale,
		title: row.title ?? undefined,
		snippet: row.snippet === null ? undefined : sanitizeSnippet(row.snippet),
		score: Math.abs(Number(row.score)),
		titleMatchTier: Number(row.title_match_tier),
		rawScore: Number(row.raw_score),
		titleLength: Number(row.title_length),
	};
}

/**
 * Resolve the title column search matches and ranks against: the configured
 * titleField, else a `title` field, provided it is indexed.
 */
function indexedTitleColumn(metadata: CollectionSearchMetadata): string | undefined {
	const { titleField } = metadata.config;
	if (titleField && metadata.searchableFields.includes(titleField)) return titleField;
	if (metadata.searchableFields.includes("title")) return "title";
	return undefined;
}

/**
 * PostgreSQL counterpart of the FTS5 query in searchSingleCollection, with
 * the same result shape and ordering. Relevance is `ts_rank`, summed per
 * column with the configured weights when there are any.
 */
async function searchSingleCollectionPostgres(
	db: Kysely<Database>,
	collection: string,
	query: string,
	options: CollectionSearchOptions,
	metadata: CollectionSearchMetadata,
	rankAcrossCollections: boolean,
): Promise<RankedSearchResult[]> {
	const { searchableFields, hasTitle, config } = metadata;
	const textSearchConfig = postgresTextSearchConfig(config.tokenize ?? "porter unicode61");
	if (!textSearchConfig || searchableFields.length === 0) return [];

	const ftsManager = new FTSManager(db);
	const ftsTable = sql.ref(ftsManager.getFtsTableName(collection));
	const contentTable = sql.ref(ftsManager.getContentTableName(collection));
	const limit = options.limit ?? 20;
	const status = options.status ?? "published";
	const locale = options.locale ? resolveConfiguredLocale(options.locale) : undefined;
	const titleScope = options.scope === "title";

	const tsQuery = buildPostgresTsQuery(query, textSearchConfig, !titleScope);
	if (!tsQuery) return [];

	const titleColumn = indexedTitleColumn(metadata);
	if (titleScope && !titleColumn) return [];
	if (titleColumn) validateIdentifier(titleColumn, "title field");
	for (const field of searchableFields) validateIdentifier(field, "searchable field name");

	let titleExpr;
	if (config.titleField) {
		validateIdentifier(config.titleField, "title field");
		titleExpr = sql`c.${sql.ref(config.titleField)}`;
	} else {
		titleExpr = hasTitle ? sql`c.title` : sql`NULL`;
	}

	const document = sql`f.${sql.ref(PG_DOCUMENT_COLUMN)}`;
	// Field weights are stored as tsvector labels when the index is built, so
	// ranking reads only the stored vector.
	const weighting = postgresWeighting(searchableFields, config.weights);
	const rankExpr = weighting
		? sql`ts_rank(ARRAY[${sql.join(weighting.rankWeights)}]::float4[], ${document}, q.q)`
		: sql`ts_rank(${document}, q.q)`;

	let titleMatchTier = sql`0`;
	if (rankAcrossCollections && titleColumn) {
		const tier = buildPostgresTitleMatchTier(titleColumn, query, textSearchConfig);
		if (tier) titleMatchTier = tier;
	}
	const titleLengthExpr = titleColumn
		? sql`coalesce(length(c.${sql.ref(titleColumn)}), 0)`
		: sql`0`;

	const snippetField = sql.ref(searchableFields[0]);
	const snippetOptions =
		"StartSel=<mark>, StopSel=</mark>, MaxWords=32, MinWords=8, MaxFragments=1, FragmentDelimiter=...";

	// Scores are normalized against the best match among every matching row,
	// not just the LIMITed window, so they stay stable across pages.
	const normalizedScore = rankAcrossCollections
		? sql`coalesce(m.score / nullif(max(m.score) OVER (), 0), 0)`
		: sql`m.score`;

	const statement = sql<SearchRow>`
		WITH q AS (SELECT ${tsQuery} AS q),
		matches AS (
			SELECT c.id, (${rankExpr})::float8 AS score,
				${titleMatchTier} AS title_match_tier, ${titleLengthExpr} AS title_length
			FROM ${ftsTable} f
			JOIN ${contentTable} c ON f.id = c.id
			CROSS JOIN q
			WHERE ${document} @@ q.q
			${titleScope && titleColumn ? sql`AND ${columnVector(sql`f.${sql.ref(titleColumn)}`, textSearchConfig)} @@ q.q` : sql``}
			AND c.status = ${status}
			AND c.deleted_at IS NULL
			${locale ? sql`AND ${localeMatches("c.locale", locale)}` : sql``}
		),
		ranked AS (
			SELECT m.id, m.title_match_tier, m.title_length, m.score AS raw_score,
				${normalizedScore} AS score
			FROM matches m
			ORDER BY m.title_match_tier DESC,
				CASE WHEN m.title_match_tier = 1 THEN m.title_length ELSE 0 END ASC,
				score DESC, raw_score DESC, m.title_length ASC, m.id COLLATE "C"
			LIMIT ${limit}
		)
		SELECT c.id, c.slug, c.locale, ${titleExpr} AS title,
			ts_headline(${textSearchConfig}::regconfig, f.${snippetField}, q.q, ${snippetOptions}) AS snippet,
			r.score, r.title_match_tier, r.raw_score, r.title_length
		FROM ranked r
		JOIN ${contentTable} c ON c.id = r.id
		JOIN ${ftsTable} f ON f.id = c.id
		CROSS JOIN q
		ORDER BY r.title_match_tier DESC,
			CASE WHEN r.title_match_tier = 1 THEN r.title_length ELSE 0 END ASC,
			r.score DESC, r.raw_score DESC, r.title_length ASC, c.id COLLATE "C"
	`;
	let rows;
	try {
		rows = await statement.execute(db);
	} catch (error) {
		// Malformed operator queries are user input, not failures; see searchSingleCollection.
		if (isSearchSyntaxError(error)) return [];
		throw error;
	}

	return rows.rows.map((row) => toRankedResult(collection, row));
}

/**
 * Title tiers for cross-collection ranking on PostgreSQL: 2 when the title
 * equals the query (case-insensitively), 1 when the title alone matches it.
 */
function buildPostgresTitleMatchTier(
	titleColumn: string,
	query: string,
	textSearchConfig: string,
): RawBuilder<number> | null {
	const trimmed = query.trim();
	const phrase = trimmed.startsWith('"') && trimmed.endsWith('"');
	const exact = phrase ? trimmed.slice(1, -1).trim() : trimmed;
	const terms = phrase
		? trimmed
		: trimmed
				.split(WHITESPACE_SPLIT_PATTERN)
				.filter((term) => !TITLE_OPERATOR_PATTERN.test(term))
				.join(" ");
	const titleQuery = buildPostgresTsQuery(terms, textSearchConfig, false);
	if (!titleQuery) return null;
	const title = sql`c.${sql.ref(titleColumn)}`;
	return sql<number>`CASE
		WHEN lower(trim(${title})) = lower(${exact}) THEN 2
		WHEN ${columnVector(title, textSearchConfig)} @@ ${titleQuery} THEN 1
		ELSE 0
	END`;
}

// Module-scope regexes so the engine doesn't recompile per call —
// snippet sanitization runs on every search result.
const SNIPPET_AMP_RE = /&/g;
const SNIPPET_LT_RE = /</g;
const SNIPPET_GT_RE = />/g;
const SNIPPET_QUOT_RE = /"/g;
const SNIPPET_APOS_RE = /'/g;

/**
 * Make an FTS5 snippet safe to render with `set:html` / `innerHTML`.
 *
 * SQLite's `snippet()` function splices literal `<mark>` and `</mark>`
 * markers around matched terms but does not escape the surrounding
 * source text. Posts that legitimately contain `<`, `>`, `&`, `"` or
 * `'` would render as broken markup, and a `<script>` literal in a
 * title (or any other indexed field) would execute when displayed.
 *
 * The fix: HTML-escape the whole string, which turns the markers into
 * `&lt;mark&gt;` / `&lt;/mark&gt;`. Then restore those two patterns to
 * their original tag form. The result is "the indexed text with all
 * HTML metacharacters escaped, plus a small set of literal `<mark>`
 * highlight tags around matched terms" — which matches the API's
 * documented contract.
 */
function sanitizeSnippet(snippet: string): string {
	return snippet
		.replace(SNIPPET_AMP_RE, "&amp;")
		.replace(SNIPPET_LT_RE, "&lt;")
		.replace(SNIPPET_GT_RE, "&gt;")
		.replace(SNIPPET_QUOT_RE, "&quot;")
		.replace(SNIPPET_APOS_RE, "&#39;")
		.replaceAll("&lt;mark&gt;", "<mark>")
		.replaceAll("&lt;/mark&gt;", "</mark>");
}

const TITLE_OPERATOR_PATTERN = /^(AND|OR|NOT|NEAR)$/;

function buildTitleMatchTier(
	ftsTable: string,
	titleColumn: string,
	titleIndex: number,
	query: string,
): RawBuilder<number> {
	validateIdentifier(titleColumn, "title field");
	const trimmed = query.trim();
	const phrase = trimmed.startsWith('"') && trimmed.endsWith('"');
	const exact = phrase ? trimmed.slice(1, -1).trim() : trimmed;
	const terms = phrase
		? trimmed
		: trimmed
				.split(WHITESPACE_SPLIT_PATTERN)
				.filter((term) => !TITLE_OPERATOR_PATTERN.test(term))
				.join(" ");
	const titleQuery = escapeQuery(terms, false);
	if (!titleQuery) return sql`0`;
	const exactQuery = `${titleColumn} : "${exact.replace(DOUBLE_QUOTE_PATTERN, '""')}"`;
	const containsQuery = `${titleColumn} : (${titleQuery})`;
	// FTS tokenization supplies Unicode case folding. A whole-title phrase must
	// cover the complete title and have the same length, excluding prefix hits.
	return sql<number>`CASE
		WHEN c.id IN (
			SELECT id FROM "${sql.raw(ftsTable)}"
			WHERE "${sql.raw(ftsTable)}" MATCH ${exactQuery}
			AND trim(highlight("${sql.raw(ftsTable)}", ${titleIndex}, char(1), char(2))) =
				char(1) || trim(${sql.ref(titleColumn)}) || char(2)
			AND length(trim(${sql.ref(titleColumn)})) = length(${exact})
		) THEN 2
		WHEN c.id IN (
			SELECT id FROM "${sql.raw(ftsTable)}"
			WHERE "${sql.raw(ftsTable)}" MATCH ${containsQuery}
		) THEN 1
		ELSE 0
	END`;
}

/**
 * Get search suggestions for autocomplete
 *
 * @param db - Kysely database instance
 * @param query - Partial search query
 * @param options - Suggestion options
 * @returns Array of suggestions
 */
export async function getSuggestions(
	db: Kysely<Database>,
	query: string,
	options: SuggestOptions = {},
): Promise<Suggestion[]> {
	const limit = options.limit ?? 5;
	const locale = options.locale ? resolveConfiguredLocale(options.locale) : undefined;

	// escapeQuery already appends the prefix wildcard to each term.
	const prefixQuery = escapeQuery(query);
	if (!prefixQuery) return [];

	const ftsManager = new FTSManager(db);
	const metadata = await ftsManager.getSearchMetadata(options.collections);
	const collections = options.collections?.length ? options.collections : [...metadata.keys()];
	const collectionSuggestions = await inCollectionOrder(
		collections.map(async (collection): Promise<Suggestion[]> => {
			const collectionMetadata = metadata.get(collection);
			if (!collectionMetadata) return [];
			const { config, hasTitle } = collectionMetadata;

			// Suggestions are title-based (Suggestion.title is required). A configured
			// titleField supplies it -- a validated existing column -- so it
			// takes precedence and keeps autocomplete consistent with search results.
			// Otherwise fall back to the optional `title` field; collections with
			// neither can't produce a suggestion and are skipped (selecting a missing
			// column would error).
			let titleExpr;
			if (config.titleField) {
				validateIdentifier(config.titleField, "title field");
				titleExpr = sql`c.${sql.ref(config.titleField)}`;
			} else if (hasTitle) {
				titleExpr = sql`c.title`;
			} else {
				return [];
			}

			// Validate before raw SQL interpolation
			validateIdentifier(collection, "collection slug");

			const ftsTable = ftsManager.getFtsTableName(collection);
			const contentTable = ftsManager.getContentTableName(collection);

			if (searchBackend(db) === "postgres") {
				const textSearchConfig = postgresTextSearchConfig(config.tokenize ?? "porter unicode61");
				const tsQuery = textSearchConfig ? buildPostgresTsQuery(query, textSearchConfig) : null;
				if (!tsQuery) return [];
				const document = sql`f.${sql.ref(PG_DOCUMENT_COLUMN)}`;
				const statement = sql<{ id: string; slug: string | null; title: string }>`
					WITH q AS (SELECT ${tsQuery} AS q)
					SELECT c.id, c.slug, ${titleExpr} AS title
					FROM ${sql.ref(ftsTable)} f
					JOIN ${sql.ref(contentTable)} c ON f.id = c.id
					CROSS JOIN q
					WHERE ${document} @@ q.q
					AND c.status = 'published'
					AND c.deleted_at IS NULL
					AND ${titleExpr} IS NOT NULL
					${locale ? sql`AND ${localeMatches("c.locale", locale)}` : sql``}
					ORDER BY ts_rank(${document}, q.q) DESC, c.id COLLATE "C"
					LIMIT ${limit}
				`;
				let rows;
				try {
					rows = await statement.execute(db);
				} catch (error) {
					if (isSearchSyntaxError(error)) return [];
					throw error;
				}
				return rows.rows.map((row) => ({
					collection,
					id: row.id,
					slug: row.slug,
					title: row.title,
				}));
			}

			let results;
			try {
				results = await sql<{
					id: string;
					slug: string | null;
					title: string;
				}>`
				SELECT
					c.id,
					c.slug,
					${titleExpr} AS title
				FROM "${sql.raw(ftsTable)}" f
				JOIN "${sql.raw(contentTable)}" c ON f.id = c.id
				WHERE "${sql.raw(ftsTable)}" MATCH ${prefixQuery}
				AND c.status = 'published'
				AND c.deleted_at IS NULL
				AND ${titleExpr} IS NOT NULL
				${locale ? sql`AND ${localeMatches("c.locale", locale)}` : sql``}
				ORDER BY bm25("${sql.raw(ftsTable)}")
				LIMIT ${limit}
			`.execute(db);
			} catch (error) {
				// Same swallow as searchSingleCollection: malformed prefix
				// queries should yield no suggestions, not surface DB errors.
				// Intentionally not logged (anonymous-triggerable, echoes
				// user input -- see searchSingleCollection for rationale).
				if (isSearchSyntaxError(error)) {
					return [];
				}
				throw error;
			}

			return results.rows.map((row) => ({
				collection,
				id: row.id,
				slug: row.slug,
				title: row.title,
			}));
		}),
	);
	const suggestions = collectionSuggestions.flat();

	return suggestions.slice(0, limit);
}

/**
 * Get search statistics for all collections
 */
export async function getSearchStats(db: Kysely<Database>): Promise<SearchStats> {
	const ftsManager = new FTSManager(db);
	const collections = await getSearchableCollections(db);
	const stats: SearchStats = { collections: {} };

	for (const collection of collections) {
		const collectionStats = await ftsManager.getIndexStats(collection);
		if (collectionStats) {
			stats.collections[collection] = collectionStats;
		}
	}

	return stats;
}

/**
 * Get list of collections with search enabled
 */
async function getSearchableCollections(db: Kysely<Database>): Promise<string[]> {
	const results = await db
		.selectFrom("_emdash_collections")
		.select(["slug", "search_config"])
		.execute();

	return results
		.filter((r) => {
			if (!r.search_config) return false;
			try {
				const config = JSON.parse(r.search_config);
				return config.enabled === true;
			} catch {
				return false;
			}
		})
		.map((r) => r.slug);
}

/**
 * Escape a query string for FTS5
 *
 * Handles special characters and prevents injection.
 */
function escapeQuery(query: string, allowOperators = true): string {
	if (!query || typeof query !== "string") {
		return "";
	}

	// Trim whitespace
	query = query.trim();

	if (query.length === 0) {
		return "";
	}

	// If already a quoted phrase, escape only interior quotes and preserve phrase syntax
	if (query.startsWith('"') && query.endsWith('"') && query.length >= 2) {
		const inner = query.slice(1, -1);
		return `"${inner.replace(DOUBLE_QUOTE_PATTERN, '""')}"`;
	}

	// Escape any existing quotes
	const escaped = query.replace(DOUBLE_QUOTE_PATTERN, '""');

	// If the query contains FTS5 operators (AND, OR, NOT, NEAR),
	// pass through with quotes escaped but operators preserved. Not allowed
	// when the match string is embedded in a column filter: unquoted parens
	// would let a clause escape the filter's group.
	if (allowOperators && FTS_OPERATORS_PATTERN.test(query)) {
		return escaped;
	}

	// For simple queries, wrap each word to handle special chars
	const terms = escaped.split(WHITESPACE_SPLIT_PATTERN).filter((t) => t.length > 0);
	if (terms.length === 0) {
		return "";
	}

	// Join with implicit AND, add prefix matching (*) to all terms
	// This allows "hel wor" to match "hello world"
	return terms.map((t) => `"${t}"*`).join(" ");
}
