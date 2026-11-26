import type { Kysely } from "kysely";
import { sql } from "kysely";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { ContentRepository } from "../../../src/database/repositories/content.js";
import type { Database } from "../../../src/database/types.js";
import { SchemaRegistry } from "../../../src/schema/registry.js";
import { FTSManager } from "../../../src/search/fts-manager.js";
import {
	getSearchStats,
	getSuggestions,
	searchCollection,
	searchWithDb,
} from "../../../src/search/query.js";
import {
	PG_CONNECTION_STRING,
	type PgTestContext,
	setupTestPostgresDatabase,
	teardownTestPostgresDatabase,
} from "../../utils/test-db.js";

describe.skipIf(!PG_CONNECTION_STRING)("PostgreSQL search", () => {
	let ctx: PgTestContext;
	let db: Kysely<Database>;
	let repo: ContentRepository;
	let fts: FTSManager;

	async function ids(query: string, options: Parameters<typeof searchWithDb>[2] = {}) {
		const result = await searchWithDb(db, query, options);
		return result.items.map((item) => item.id);
	}

	beforeEach(async () => {
		ctx = await setupTestPostgresDatabase({ search: true });
		db = ctx.db;
		repo = new ContentRepository(db);
		fts = new FTSManager(db);
		const registry = new SchemaRegistry(db);
		await registry.createCollection({ slug: "posts", label: "Posts", supports: ["search"] });
		await registry.createField("posts", {
			slug: "body",
			label: "Body",
			type: "portableText",
			searchable: true,
		});
		await registry.createField("posts", {
			slug: "title",
			label: "Title",
			type: "string",
			searchable: true,
		});
		await registry.createField("posts", {
			slug: "summary",
			label: "Summary",
			type: "text",
			searchable: true,
		});
	});

	afterEach(async () => {
		await teardownTestPostgresDatabase(ctx);
	});

	function block(text: string) {
		return [
			{
				_type: "block",
				_key: "b1",
				style: "normal",
				markDefs: [],
				children: [{ _type: "span", _key: "s1", text }],
			},
			{ _type: "image", _key: "b2", alt: "festival poster" },
		];
	}

	async function createPost(slug: string, title: string, body: string, status = "published") {
		return repo.create({
			type: "posts",
			slug,
			status,
			data: { title, body: block(body), summary: "Short summary" },
		});
	}

	it("indexes existing content when search is enabled", async () => {
		const post = await createPost("cinema", "Opening Night", "The haunted cinema screens films.");
		await fts.enableSearch("posts");

		const result = await searchWithDb(db, "haunted");
		expect(result.items).toHaveLength(1);
		expect(result.items[0]).toMatchObject({ id: post.id, collection: "posts" });
		expect(result.items[0]!.snippet).toContain("<mark>");
		expect(result.items[0]!.score).toBeGreaterThan(0);
		expect(await getSearchStats(db)).toEqual({ collections: { posts: { indexed: 1 } } });
	});

	it("keeps the index in sync with inserts, updates, trash, and deletes", async () => {
		await fts.enableSearch("posts");
		const post = await createPost("draft", "Lighthouse", "A keeper watches the storm.");
		expect(await ids("keeper")).toEqual([post.id]);

		await repo.update("posts", post.id, { data: { title: "Harbor" } });
		expect(await ids("lighthouse")).toEqual([]);
		expect(await ids("harbor")).toEqual([post.id]);

		await repo.delete("posts", post.id);
		expect(await ids("harbor")).toEqual([]);

		await repo.restore("posts", post.id);
		expect(await ids("harbor", { status: "draft" })).toEqual([post.id]);

		await repo.delete("posts", post.id);
		await repo.permanentDelete("posts", post.id);
		expect(await fts.getIndexStats("posts")).toEqual({ indexed: 0 });
	});

	it("matches term prefixes and Portable Text prose, not its structure", async () => {
		const post = await createPost("p", "Opening Night", "The haunted cinema screens films.");
		await fts.enableSearch("posts");

		expect(await ids("haun cine")).toEqual([post.id]);
		expect(await ids("poster")).toEqual([post.id]);
		expect(await ids("normal")).toEqual([]);
		expect(await ids("span")).toEqual([]);
	});

	it("supports phrases and operators", async () => {
		const a = await createPost("a", "Red apples", "Fresh fruit from the orchard.");
		const b = await createPost("b", "Green pears", "Ripe fruit from the market.");
		await fts.enableSearch("posts");

		expect(await ids('"fresh fruit"')).toEqual([a.id]);
		expect((await ids('"fresh fruit" OR "ripe fruit"')).toSorted()).toEqual(
			[a.id, b.id].toSorted(),
		);
		expect((await ids("apples OR pears")).toSorted()).toEqual([a.id, b.id].toSorted());
		expect(await ids("fruit NOT orchard")).toEqual([b.id]);
		expect(await ids("(apples OR pears) AND orchard")).toEqual([a.id]);
		expect((await ids("apples OR pears AND market")).toSorted()).toEqual([a.id, b.id].toSorted());
		expect(await ids("fruit NOT ripe market")).toEqual([a.id]);
		expect(await ids("fruit NOT ripe AND market")).toEqual([]);
		expect(await ids("NEAR(fresh orchard, 5)")).toEqual([a.id]);
		for (const malformed of ["AND", "apples OR", "(apples OR pears", "NOT orchard"]) {
			expect(await ids(malformed)).toEqual([]);
		}
	});

	it("treats tsquery syntax in user input as literal text", async () => {
		await createPost("a", "Quotes", "Plain text.");
		await fts.enableSearch("posts");

		for (const query of ["'", "\\", "a:* & !b | (c", '"unbalanced', "<-> ()", "!!"]) {
			await expect(searchWithDb(db, query)).resolves.toMatchObject({ items: [] });
		}
	});

	it("filters by status and restricts title scope to the title field", async () => {
		const published = await createPost("a", "Mountain", "A trail through the valley.");
		await createPost("b", "Valley", "Draft about the mountain.", "draft");
		await fts.enableSearch("posts");

		expect(await ids("mountain")).toEqual([published.id]);
		expect(await ids("valley", { scope: "title" })).toEqual([]);
		expect(await ids("mountain", { scope: "title" })).toEqual([published.id]);
	});

	it("ranks an exact title match first across collections and pages results", async () => {
		const exact = await createPost("exact", "Garden", "Notes.");
		const other = await createPost(
			"other",
			"Notes",
			"A long piece about the garden and more garden.",
		);
		await fts.enableSearch("posts");

		const first = await searchWithDb(db, "garden", { limit: 1 });
		expect(first.items.map((item) => item.id)).toEqual([exact.id]);
		expect(first.nextCursor).toBeDefined();
		const second = await searchWithDb(db, "garden", { limit: 1, cursor: first.nextCursor });
		expect(second.items.map((item) => item.id)).toEqual([other.id]);
		expect(second.nextCursor).toBeUndefined();

		const single = await searchCollection(db, "posts", "garden");
		expect(single.items).toHaveLength(2);
	});

	it("applies field weights to ranking", async () => {
		const inTitle = await createPost("t", "Comet", "Nothing to see.");
		const inBody = await createPost("b", "Sky", "A comet crossed the sky.");
		await fts.enableSearch("posts", { weights: { title: 1, body: 10 } });

		expect(await ids("comet", { collections: ["posts"] })).toEqual([inTitle.id, inBody.id]);
		const single = await searchCollection(db, "posts", "comet");
		expect(single.items.map((item) => item.id)).toEqual([inBody.id, inTitle.id]);
	});

	it("returns title suggestions for a prefix", async () => {
		await createPost("a", "Astronomy basics", "Stars.");
		await fts.enableSearch("posts");

		const suggestions = await getSuggestions(db, "astro");
		expect(suggestions.map((s) => s.title)).toEqual(["Astronomy basics"]);
	});

	it("drops the index on disable and lets searchable fields be deleted", async () => {
		await createPost("a", "Atlas", "Maps.");
		await fts.enableSearch("posts");

		await new SchemaRegistry(db).deleteField("posts", "summary");
		expect(await ids("atlas")).toHaveLength(1);
		await repo.create({
			type: "posts",
			slug: "b",
			status: "published",
			data: { title: "Atlas two" },
		});
		expect(await ids("atlas")).toHaveLength(2);

		await fts.disableSearch("posts");
		expect(await fts.ftsTableExists("posts")).toBe(false);
		const leftovers = await sql<{ triggers: number; sync_function: string | null }>`
			SELECT
				(SELECT count(*)::int FROM pg_trigger
					WHERE tgrelid = ${"ec_posts"}::regclass AND NOT tgisinternal) AS triggers,
				to_regprocedure(${"_emdash_fts_posts()"})::text AS sync_function
		`.execute(db);
		expect(leftovers.rows[0]).toEqual({ triggers: 0, sync_function: null });
		await expect(
			repo.create({ type: "posts", slug: "c", status: "published", data: { title: "Atlas" } }),
		).resolves.toMatchObject({ slug: "c" });
	});

	it("rebuilds a missing index during repair", async () => {
		await createPost("a", "Beacon", "Light.");
		await fts.enableSearch("posts");
		await sql`DROP TABLE ${sql.ref("_emdash_fts_posts")}`.execute(db);

		expect(await fts.verifyAndRepairAll({ checkRowCounts: false })).toBe(1);
		expect(await ids("beacon")).toHaveLength(1);
	});

	it("leaves a complete index when several instances rebuild a missing index at once", async () => {
		await createPost("a", "Orchard", "Apples.");
		await fts.enableSearch("posts");
		const fields = await fts.getSearchableFields("posts");
		await fts.dropFtsTable("posts");

		await Promise.all(Array.from({ length: 4 }, () => fts.rebuildIndex("posts", fields)));

		const triggers = await sql<{ name: string }>`
			SELECT tgname AS name FROM pg_trigger
			WHERE tgrelid = ${"ec_posts"}::regclass AND NOT tgisinternal
			ORDER BY tgname
		`.execute(db);
		expect(triggers.rows.map((row) => row.name)).toEqual([
			"_emdash_fts_delete",
			"_emdash_fts_insert",
			"_emdash_fts_update",
		]);
		const added = await createPost("b", "Vineyard", "Grapes.");
		expect(await ids("vineyard")).toEqual([added.id]);
		expect(await ids("orchard")).toHaveLength(1);
	});

	it("populates the index in batches by content id", async () => {
		const posts = [];
		for (const slug of ["a", "b", "c", "d", "e"]) {
			posts.push(await createPost(slug, `Lantern ${slug}`, "Light."));
		}
		await repo.delete("posts", posts[2]!.id);
		await fts.enableSearch("posts");
		await sql`DELETE FROM ${sql.ref("_emdash_fts_posts")}`.execute(db);
		const fields = await fts.getSearchableFields("posts");

		const cursors: (string | null)[] = [];
		let cursor: string | null = null;
		do {
			cursor = await fts.populateBatch("posts", fields, cursor, 2);
			cursors.push(cursor);
		} while (cursor !== null);

		expect(cursors).toHaveLength(4);
		expect(await fts.getIndexStats("posts")).toEqual({ indexed: 4 });
		expect(await ids("lantern")).toHaveLength(4);
	});

	it("keeps an edit and a trash committed while a batch is populating", async () => {
		const edited = await createPost("a", "Old heading", "Body.");
		const trashed = await createPost("b", "Doomed heading", "Body.");
		await fts.enableSearch("posts");
		const fields = await fts.getSearchableFields("posts");

		let batch: Promise<string | null> | undefined;
		await db.transaction().execute(async (trx) => {
			const writer = new ContentRepository(trx);
			await writer.update("posts", edited.id, { data: { title: "New heading" } });
			await writer.delete("posts", trashed.id);
			batch = fts.populateBatch("posts", fields, null, 10);
			await new Promise((resolve) => setTimeout(resolve, 300));
		});
		await batch;

		expect(await ids("old", { scope: "title" })).toEqual([]);
		expect(await ids("new", { scope: "title" })).toEqual([edited.id]);
		expect(await fts.getIndexStats("posts")).toEqual({ indexed: 1 });
	});

	it("rejects the trigram tokenizer", async () => {
		await expect(fts.enableSearch("posts", { tokenize: "trigram" })).rejects.toThrow(
			/not supported on PostgreSQL/,
		);
	});
});
