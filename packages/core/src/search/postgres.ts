/**
 * PostgreSQL full-text search backend.
 *
 * Mirrors the FTS5 layout: `_emdash_fts_<slug>` holds one row per non-deleted
 * content row with the extracted text of every searchable field, plus a
 * generated `tsvector` column with a GIN index. A plpgsql trigger function
 * keeps it in sync with `ec_<slug>`.
 */

import type { Kysely, RawBuilder } from "kysely";
import { sql } from "kysely";

import { withTransaction } from "../database/transaction.js";
import type { Database } from "../database/types.js";
import type { SearchTokenizer } from "./types.js";

/** Generated tsvector column. Field slugs cannot start with `_`, so it never collides. */
export const PG_DOCUMENT_COLUMN = "_emdash_document";

/** Columns of the index table that are not searchable fields. */
export const PG_SYSTEM_COLUMNS = ["id", "locale", PG_DOCUMENT_COLUMN] as const;

const PG_IDENTIFIER_MAX_LENGTH = 63;
const PG_INDEX_SUFFIX = "_doc_idx";

const TRIGGER_INSERT = "_emdash_fts_insert";
const TRIGGER_UPDATE = "_emdash_fts_update";
const TRIGGER_DELETE = "_emdash_fts_delete";

/**
 * Map an FTS5 tokenizer setting to a PostgreSQL text search configuration.
 * Returns null for tokenizers PostgreSQL cannot reproduce.
 */
export function postgresTextSearchConfig(tokenize: SearchTokenizer): string | null {
	switch (tokenize) {
		case "porter unicode61":
			return "english";
		case "unicode61":
			return "simple";
		case "trigram":
			return null;
	}
}

export function requirePostgresTextSearchConfig(tokenize: SearchTokenizer): string {
	const config = postgresTextSearchConfig(tokenize);
	if (!config) {
		throw new Error(`The "${tokenize}" search tokenizer is not supported on PostgreSQL`);
	}
	return config;
}

// Index objects are named `_emdash_fts_<slug>`, so a leading underscore is allowed.
const OBJECT_NAME_PATTERN = /^[a-z_][a-z0-9_]*$/;

function q(identifier: string): string {
	if (!OBJECT_NAME_PATTERN.test(identifier)) {
		throw new Error(`Invalid search index identifier: "${identifier}"`);
	}
	return `"${identifier}"`;
}

function assertIdentifierLength(ftsTable: string): void {
	if (ftsTable.length + PG_INDEX_SUFFIX.length > PG_IDENTIFIER_MAX_LENGTH) {
		throw new Error(
			`Collection slug is too long for PostgreSQL search: "${ftsTable}" leaves no room ` +
				`for its index name within PostgreSQL's ${PG_IDENTIFIER_MAX_LENGTH}-character limit`,
		);
	}
}

/**
 * Text indexed for one field. Portable Text is reduced to the strings under
 * `text`, `alt`, `caption`, and `code` keys at any depth, matching the FTS5
 * extraction. Other fields are indexed as their text representation.
 *
 * `ref` must be a validated column reference.
 */
function valueExpr(ref: string, fieldType: string | undefined): string {
	if (fieldType !== "portableText") return `(${ref})::text`;
	const doc = `(${ref})::jsonb`;
	return (
		`CASE WHEN ${ref} IS NULL THEN NULL ` +
		`WHEN jsonb_typeof(${doc}) IN ('array', 'object') THEN (` +
		`SELECT string_agg(kv.value #>> '{}', ' ') ` +
		`FROM jsonb_path_query(${doc}, 'strict $.** ? (@.type() == "object")') AS o(obj) ` +
		`CROSS JOIN LATERAL jsonb_each(o.obj) AS kv ` +
		`WHERE kv.key IN ('text', 'alt', 'caption', 'code') AND jsonb_typeof(kv.value) = 'string') ` +
		`ELSE ${doc} #>> '{}' END`
	);
}

type WeightClass = "A" | "B" | "C" | "D";

const WEIGHT_CLASSES: readonly WeightClass[] = ["A", "B", "C", "D"];

export interface PostgresWeighting {
	/** tsvector weight label stored for each field. */
	classes: Map<string, WeightClass>;
	/** `ts_rank` weights array, in PostgreSQL's `{D, C, B, A}` order, each within [0, 1]. */
	rankWeights: [number, number, number, number];
}

/**
 * Map per-field search weights onto PostgreSQL's four tsvector weight labels.
 *
 * Distinct weights are ranked from highest to lowest and take labels A, B, C,
 * then D. Up to four distinct weights are reproduced exactly (relative to the
 * largest); any further weights share label D at their average. `ts_rank`
 * rejects weights above 1, so all values are divided by the largest.
 * Fields without a configured weight count as 1, matching FTS5's bm25.
 * Returns null when no weights are configured or none is positive.
 */
export function postgresWeighting(
	fields: string[],
	weights: Record<string, number> | undefined,
): PostgresWeighting | null {
	if (!weights || fields.length === 0) return null;
	const fieldWeights = fields.map((field) => Math.max(0, weights[field] ?? 1));
	const max = Math.max(...fieldWeights);
	if (!(max > 0)) return null;

	const distinct = [...new Set(fieldWeights)].toSorted((a, b) => b - a);
	const head = distinct.slice(0, 3);
	const tail = distinct.slice(3);
	const labelWeight = new Map<WeightClass, number>();
	head.forEach((weight, index) => labelWeight.set(WEIGHT_CLASSES[index], weight / max));
	if (tail.length > 0) {
		const average = tail.reduce((sum, weight) => sum + weight, 0) / tail.length;
		labelWeight.set("D", average / max);
	}

	const classes = new Map<string, WeightClass>();
	fields.forEach((field, index) => {
		const position = distinct.indexOf(fieldWeights[index]);
		classes.set(field, WEIGHT_CLASSES[Math.min(position, 3)]);
	});
	const weightOf = (label: WeightClass) => labelWeight.get(label) ?? 0;
	return {
		classes,
		rankWeights: [weightOf("D"), weightOf("C"), weightOf("B"), weightOf("A")],
	};
}

function documentExpr(
	fields: string[],
	config: string,
	weighting: PostgresWeighting | null,
): string {
	return fields
		.map((f) => {
			const vector = `to_tsvector('${config}'::regconfig, coalesce(${q(f)}, ''))`;
			const label = weighting?.classes.get(f);
			return label ? `setweight(${vector}, '${label}')` : vector;
		})
		.join(" || ");
}

function upsertSet(fields: string[]): string {
	return ["locale", ...fields].map((f) => `${q(f)} = EXCLUDED.${q(f)}`).join(", ");
}

export interface PostgresIndexSpec {
	ftsTable: string;
	contentTable: string;
	fields: string[];
	fieldTypes: Map<string, string>;
	tokenize: SearchTokenizer;
	weights?: Record<string, number>;
}

/**
 * Run `fn` in a transaction holding a lock for one collection's search index,
 * so concurrent rebuilds from several server instances run one after another
 * instead of interleaving their DDL. PostgreSQL DDL is transactional, so a
 * rebuild that fails part-way rolls back to the previous index. The lock is
 * re-entrant within a transaction.
 */
export async function withPostgresIndexLock<T>(
	db: Kysely<Database>,
	ftsTable: string,
	fn: (trx: Kysely<Database>) => Promise<T>,
): Promise<T> {
	return withTransaction(db, async (trx) => {
		await sql`SELECT pg_advisory_xact_lock(hashtext('emdash_search_index'), hashtext(${ftsTable}))`.execute(
			trx,
		);
		return fn(trx);
	});
}

/**
 * Create the index table, its GIN index, the sync function, and triggers.
 * The function is named after the table; PostgreSQL keeps functions and
 * relations in separate namespaces.
 */
export async function createPostgresIndex(
	db: Kysely<Database>,
	spec: PostgresIndexSpec,
): Promise<void> {
	const { ftsTable, contentTable, fields, fieldTypes } = spec;
	const config = requirePostgresTextSearchConfig(spec.tokenize);
	assertIdentifierLength(ftsTable);
	const table = q(ftsTable);
	const content = q(contentTable);
	const columns = fields.map((f) => `${q(f)} text`).join(", ");

	await sql
		.raw(
			`CREATE TABLE IF NOT EXISTS ${table} (
				id text PRIMARY KEY,
				locale text,
				${columns},
				${q(PG_DOCUMENT_COLUMN)} tsvector GENERATED ALWAYS AS (${documentExpr(fields, config, postgresWeighting(fields, spec.weights))}) STORED
			)`,
		)
		.execute(db);
	await sql
		.raw(
			`CREATE INDEX IF NOT EXISTS ${q(`${ftsTable}${PG_INDEX_SUFFIX}`)}
			ON ${table} USING gin (${q(PG_DOCUMENT_COLUMN)})`,
		)
		.execute(db);

	const fieldList = fields.map(q).join(", ");
	const newValues = fields.map((f) => valueExpr(`NEW.${q(f)}`, fieldTypes.get(f))).join(", ");
	await sql
		.raw(
			`CREATE OR REPLACE FUNCTION ${table}() RETURNS trigger LANGUAGE plpgsql AS $emdash_fts$
			BEGIN
				IF TG_OP <> 'INSERT' THEN
					DELETE FROM ${table} WHERE id = OLD.id;
				END IF;
				IF TG_OP <> 'DELETE' AND NEW.deleted_at IS NULL THEN
					INSERT INTO ${table} (id, locale, ${fieldList})
					VALUES (NEW.id, NEW.locale, ${newValues})
					ON CONFLICT (id) DO UPDATE SET ${upsertSet(fields)};
				END IF;
				RETURN NULL;
			END
			$emdash_fts$`,
		)
		.execute(db);

	// The update trigger fires only when an indexed value, the locale, or the
	// trash state changed, so metadata-only saves skip re-tokenizing. Values
	// compare as text because the json type has no equality operator.
	const changed = ["deleted_at", "locale", ...fields]
		.map((f) => `OLD.${q(f)}::text IS DISTINCT FROM NEW.${q(f)}::text`)
		.join(" OR ");
	for (const trigger of [TRIGGER_INSERT, TRIGGER_UPDATE, TRIGGER_DELETE]) {
		await sql.raw(`DROP TRIGGER IF EXISTS ${q(trigger)} ON ${content}`).execute(db);
	}
	await sql
		.raw(
			`CREATE TRIGGER ${q(TRIGGER_INSERT)} AFTER INSERT ON ${content}
			FOR EACH ROW WHEN (NEW.deleted_at IS NULL) EXECUTE FUNCTION ${table}()`,
		)
		.execute(db);
	await sql
		.raw(
			`CREATE TRIGGER ${q(TRIGGER_UPDATE)} AFTER UPDATE ON ${content}
			FOR EACH ROW WHEN (${changed}) EXECUTE FUNCTION ${table}()`,
		)
		.execute(db);
	await sql
		.raw(
			`CREATE TRIGGER ${q(TRIGGER_DELETE)} AFTER DELETE ON ${content}
			FOR EACH ROW EXECUTE FUNCTION ${table}()`,
		)
		.execute(db);
}

/** Drop the index table and its sync function; CASCADE removes the triggers. */
export async function dropPostgresIndex(db: Kysely<Database>, ftsTable: string): Promise<void> {
	await sql.raw(`DROP FUNCTION IF EXISTS ${q(ftsTable)}() CASCADE`).execute(db);
	await sql.raw(`DROP TABLE IF EXISTS ${q(ftsTable)}`).execute(db);
}

type PopulateSpec = Pick<PostgresIndexSpec, "ftsTable" | "contentTable" | "fields" | "fieldTypes">;

/**
 * Run a populate statement while content writes are held off. A populate
 * reads content from its own snapshot; without the lock, an edit or trash
 * committed while it waits on a conflicting index row is overwritten with
 * the older values. SHARE mode waits for in-flight writes to commit and
 * blocks new ones until the transaction ends, without locking every row.
 */
async function withContentLocked<T>(
	db: Kysely<Database>,
	spec: PopulateSpec,
	fn: (trx: Kysely<Database>) => Promise<T>,
): Promise<T> {
	return withPostgresIndexLock(db, spec.ftsTable, async (trx) => {
		await sql.raw(`LOCK TABLE ${q(spec.contentTable)} IN SHARE MODE`).execute(trx);
		return fn(trx);
	});
}

export async function populatePostgresIndex(
	db: Kysely<Database>,
	spec: PopulateSpec,
): Promise<void> {
	const { ftsTable, contentTable, fields, fieldTypes } = spec;
	const content = q(contentTable);
	const values = fields.map((f) => valueExpr(`${content}.${q(f)}`, fieldTypes.get(f))).join(", ");
	await withContentLocked(db, spec, (trx) =>
		sql
			.raw(
				`INSERT INTO ${q(ftsTable)} (id, locale, ${fields.map(q).join(", ")})
				SELECT id, locale, ${values} FROM ${content}
				WHERE deleted_at IS NULL
				ON CONFLICT (id) DO UPDATE SET ${upsertSet(fields)}`,
			)
			.execute(trx),
	);
}

/**
 * Index up to `limit` content rows whose id sorts after `afterId` (null for
 * the first batch), in id order. Returns the last id covered, or null when no
 * rows remain.
 */
export async function populatePostgresIndexBatch(
	db: Kysely<Database>,
	spec: PopulateSpec,
	afterId: string | null,
	limit: number,
): Promise<string | null> {
	const { ftsTable, contentTable, fields, fieldTypes } = spec;
	const content = q(contentTable);
	const values = fields.map((f) => valueExpr(`${content}.${q(f)}`, fieldTypes.get(f))).join(", ");
	const result = await withContentLocked(db, spec, (trx) =>
		sql<{ last: string | null }>`
		WITH batch AS (
			SELECT id FROM ${sql.raw(content)}
			${afterId === null ? sql`` : sql`WHERE id > ${afterId}`}
			ORDER BY id
			LIMIT ${limit}
		),
		indexed AS (
			INSERT INTO ${sql.raw(q(ftsTable))} (id, locale, ${sql.raw(fields.map(q).join(", "))})
			SELECT id, locale, ${sql.raw(values)} FROM ${sql.raw(content)}
			WHERE deleted_at IS NULL AND id IN (SELECT id FROM batch)
			ON CONFLICT (id) DO UPDATE SET ${sql.raw(upsertSet(fields))}
		)
		SELECT max(id) AS last FROM batch
	`.execute(trx),
	);
	return result.rows[0]?.last ?? null;
}

// FTS5 operators are uppercase-only; see query.ts.
const OPERATORS_PATTERN = /\b(AND|OR|NOT|NEAR)\b/;
const WHITESPACE_PATTERN = /\s+/;
const OPERATOR_TOKEN_PATTERN = /"([^"]*)"|[(),]|[^\s(),"]+/g;
const BACKSLASH_PATTERN = /\\/g;
const SINGLE_QUOTE_PATTERN = /'/g;

function quoteLexeme(term: string): string {
	return `'${term.replace(BACKSLASH_PATTERN, "\\\\").replace(SINGLE_QUOTE_PATTERN, "''")}'`;
}

const NUMBER_PATTERN = /^\d+$/;
const OPERATOR_WORDS = new Set(["AND", "OR", "NOT"]);
const TSQUERY_OPERATORS: Record<string, string> = { AND: " & ", OR: " | ", NOT: " & !" };

class OperatorQuerySyntaxError extends Error {}

/** A word or phrase as a quoted lexeme; a trailing `*` on a word is a prefix match. */
function operatorLexeme(word: string): string {
	if (word.length > 1 && word.endsWith("*")) return `${quoteLexeme(word.slice(0, -1))}:*`;
	return quoteLexeme(word);
}

/**
 * Translate an FTS5 operator query into `to_tsquery` text, or null when the
 * query is malformed (FTS5 rejects the same input).
 *
 * FTS5 precedence, tightest first, is: adjacent operands (implicit AND), NOT,
 * AND, OR. tsquery's `!`, `&`, `|` give the last three, so each run of
 * adjacent operands is parenthesized. FTS5's NOT is binary (`a NOT b`) and
 * becomes `a & !b`. `NEAR(a b, N)` becomes `(a & b)`, ignoring the distance;
 * a NEAR not followed by `(` is an ordinary word, as in FTS5.
 */
export function fts5OperatorQueryToTsquery(query: string): string | null {
	const tokens = Array.from(query.matchAll(OPERATOR_TOKEN_PATTERN), ([token, phrase]) => ({
		token,
		phrase,
	}));
	let position = 0;
	const peek = (offset = 0) => tokens[position + offset];
	const take = () => {
		const next = tokens[position++];
		if (!next) throw new OperatorQuerySyntaxError();
		return next;
	};
	const expect = (token: string) => {
		if (take().token !== token) throw new OperatorQuerySyntaxError();
	};
	const startsOperand = () => {
		const next = peek();
		if (!next) return false;
		if (next.phrase !== undefined || next.token === "(") return true;
		return next.token !== ")" && next.token !== "," && !OPERATOR_WORDS.has(next.token);
	};

	const parseNearGroup = (): string => {
		expect("(");
		const parts: string[] = [];
		while (peek()?.phrase !== undefined || (startsOperand() && peek()?.token !== "(")) {
			const { token, phrase } = take();
			parts.push(phrase === undefined ? operatorLexeme(token) : quoteLexeme(phrase));
		}
		if (peek()?.token === ",") {
			take();
			if (!NUMBER_PATTERN.test(take().token)) throw new OperatorQuerySyntaxError();
		}
		expect(")");
		if (parts.length === 0) throw new OperatorQuerySyntaxError();
		return `(${parts.join(" & ")})`;
	};

	const parseOperand = (): string => {
		const { token, phrase } = take();
		if (phrase !== undefined) {
			if (!phrase.trim()) throw new OperatorQuerySyntaxError();
			return quoteLexeme(phrase);
		}
		if (token === "(") {
			const inner = parseExpression();
			expect(")");
			return `(${inner})`;
		}
		if (token === "NEAR" && peek()?.token === "(") return parseNearGroup();
		return operatorLexeme(token);
	};

	const parseTerm = (): string => {
		if (!startsOperand()) throw new OperatorQuerySyntaxError();
		const operands = [parseOperand()];
		while (startsOperand()) operands.push(parseOperand());
		return operands.length === 1 ? operands[0] : `(${operands.join(" & ")})`;
	};

	const parseExpression = (): string => {
		let text = parseTerm();
		while (peek() && OPERATOR_WORDS.has(peek().token)) {
			text += TSQUERY_OPERATORS[take().token];
			text += parseTerm();
		}
		return text;
	};

	try {
		const text = parseExpression();
		return position === tokens.length ? text : null;
	} catch (error) {
		if (error instanceof OperatorQuerySyntaxError) return null;
		throw error;
	}
}

/**
 * Translate the public search syntax into a tsquery expression.
 *
 * - A query that is one quoted phrase matches that phrase.
 * - A query containing uppercase AND/OR/NOT/NEAR keeps its operators and
 *   parentheses and FTS5's precedence, and matches whole words unless a word
 *   ends in `*`. Malformed operator queries return null (no matches).
 * - Anything else matches every term as a prefix.
 *
 * Terms are quoted lexemes and the text is a bound parameter, so user input
 * cannot inject tsquery operators. Returns null when nothing is searchable.
 */
export function buildPostgresTsQuery(
	query: string,
	config: string,
	allowOperators = true,
): RawBuilder<unknown> | null {
	const trimmed = query.trim();
	if (!trimmed) return null;
	const regconfig = sql`${config}::regconfig`;

	const inner = trimmed.slice(1, -1);
	if (
		trimmed.length >= 2 &&
		trimmed.startsWith('"') &&
		trimmed.endsWith('"') &&
		!inner.includes('"')
	) {
		const phrase = inner.trim();
		return phrase ? sql`phraseto_tsquery(${regconfig}, ${phrase})` : null;
	}

	if (allowOperators && OPERATORS_PATTERN.test(trimmed)) {
		const text = fts5OperatorQueryToTsquery(trimmed);
		return text === null ? null : sql`to_tsquery(${regconfig}, ${text})`;
	}

	const terms = trimmed
		.split(WHITESPACE_PATTERN)
		.map((term) => term.replaceAll('"', ""))
		.filter((term) => term.length > 0);
	if (terms.length === 0) return null;
	const text = terms.map((term) => `${quoteLexeme(term)}:*`).join(" & ");
	return sql`to_tsquery(${regconfig}, ${text})`;
}

/** Text search vector for one indexed column, for title scoping and weighting. */
export function columnVector(ref: RawBuilder<unknown>, config: string): RawBuilder<unknown> {
	return sql`to_tsvector(${config}::regconfig, coalesce(${ref}, ''))`;
}
