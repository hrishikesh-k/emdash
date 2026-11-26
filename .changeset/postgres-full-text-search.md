---
"emdash": minor
---

Adds opt-in built-in search on PostgreSQL. Set `search: true` on `postgres()`. A custom database adapter can instead wrap the dialect its `createDialect()` returns with `withPostgresSearch()` from `emdash/db`:

```js
database: postgres({ connectionString: process.env.DATABASE_URL, search: true }),
```

With search turned on, the search, suggestion, enable, rebuild, and stats endpoints work on PostgreSQL. Site imports to a PostgreSQL target also rebuild search indexes instead of turning search off. Search uses PostgreSQL text search with a GIN-indexed table and triggers for each search-enabled collection, so it needs PostgreSQL 12 or later. The `trigram` tokenizer is not supported on PostgreSQL. PostgreSQL databases that don't opt in still return `503 SEARCH_UNAVAILABLE`, as before. Hyperdrive is not covered by this option.
