# Durable HTTP sources

HTTP sources connect explicitly configured JSON APIs to typed incremental streams. The connector performs direct GET requests, retains complete response entities, maps records or change events into the existing stream contract, applies quality gates and advances the opaque cursor in the same transaction as ingestion. Accepted batches enter the normal ordered catalog-publication outbox.

The included API is synthetic and loopback-only. No external provider is configured or contacted by these examples. The connector does not submit trades, deploy a service or write to the remote API.

## Run the synthetic control

From a checkout, start the example source in one terminal:

```sh
python3 examples/http_orders_server.py --port 8421
```

In another terminal:

```sh
python3 -m dagrivane.ingest_cli --database .local/http-ingestion.sqlite init
python3 -m dagrivane.ingest_cli --database .local/http-ingestion.sqlite register orders examples/incremental-stream.json
python3 -m dagrivane.http_source --database .local/http-ingestion.sqlite register feed orders examples/http-change-source.json
python3 -m dagrivane.http_source --database .local/http-ingestion.sqlite poll feed
python3 -m dagrivane.ingest_cli --database .local/http-ingestion.sqlite rows orders
python3 -m dagrivane.ingest_cli --database .local/http-ingestion.sqlite configure-target orders --catalog .local/http-catalog
python3 -m dagrivane.ingest_cli --database .local/http-ingestion.sqlite publish orders --catalog .local/http-catalog
python3 -m dagrivane.http_source --database .local/http-ingestion.sqlite audit feed
python3 -m dagrivane.ingest_cli --database .local/http-ingestion.sqlite audit orders --catalog .local/http-catalog
```

Installed packages include the same examples. Export them first with `python3 -m dagrivane.resources ./demo`, then replace `examples/` with `demo/` in these commands. The example server supports `--port 0` and prints its actual URL; update a copied source configuration when using a different port.

The change-feed control processes two pages and five changes. Its final live rows are order 1 at amount 15 and order 3 at amount 30, with one tombstone for order 2. The exact decimal total is 45. Two dataset generations are published, with the second referencing its predecessor. A subsequent HTTP 204 response creates no extra ingestion generation.

`examples/http-row-source.json` demonstrates nested-field mapping of row revisions. Use a separate stream/source for that configuration. Rows are upserts; a row absent from a page is not implicitly deleted. The row control therefore retains order 2 and ends with three rows totaling 65. Deletions require the explicit change-event mode.

## Source configuration

Version 1 requires all of the following fields:

- `url`: an absolute HTTP(S) endpoint without embedded credentials or a fragment. Static query filters are allowed, but the declared cursor parameter cannot already appear in that query.
- `allow_http`: explicit permission for plaintext HTTP. The included loopback example sets this true; HTTPS uses normal certificate verification.
- `cursor_parameter` and `initial_cursor`: the query parameter and starting opaque position. A cursor is null, bounded UTF-8 text, or a nonnegative signed-64-bit integer. It is encoded as a query value rather than appended as raw URL text.
- `response`: item, next-cursor and optional continuation pointers, plus `rows` or `events` mode. Pointers use JSON Pointer escaping (`~0`, `~1`) and can address object fields or array elements.
- `request`: explicit socket timeout, attempt count, exponential retry base, inline-wait bound and same-origin redirect bound.
- `authentication`: null, or an approved `Authorization`/`X-API-Key` header with an environment-variable name and a printable prefix. Actual environment values are read for the request and are not copied into configuration or request metadata.
- `limits`: per-entity and retained-journal byte budgets.

Definitions are immutable and tied to the stream's contract identity. One HTTP source owns each bound stream. The ordinary batch-ingestion entry point rejects writes to a bound stream; an older or external writer that bypasses the guard is detected before the connector can rebase or commit another page. Endpoint/mapping changes and movement to another stream require an explicit reviewed migration; automatic migration is not implemented.

The cursor and the stream offset are different things. The provider cursor remains opaque. Numeric ingestion offsets are allocated locally, one per mapped item, and are never represented as provider-native positions. Empty pages can advance an opaque cursor without inventing records or creating an empty ingestion generation.

## Row and event mapping

In `rows` mode, `columns` maps every contract column to an item-relative JSON pointer. The existing contract determines integer, exact-decimal, string, Boolean, date and timestamp semantics. JSON fractional numbers are parsed directly as Decimal, avoiding an intermediate binary float. Missing required pointers and invalid typed values reject the page.

An optional `event_id` pointer identifies an immutable provider revision, not merely a reusable business-row ID. With no pointer, the connector derives an identity from the typed upsert content. Provider revision identities are namespaced and hashed before entering the stream. A changed payload under an already accepted revision ID is rejected by the existing deduplication contract.

In `events` mode, each item has exactly `id`, `operation`, `key`, `at` and `row`. Upserts require a full typed row; deletes require `row: null`. Keys and event times must agree with the row. Provider IDs can be bounded text or integers, permitting identities that do not fit the stream's public ASCII token syntax. The connector allocates each item's numeric offset.

The continuation pointer must resolve to a Boolean when configured. Otherwise continuation is inferred from a non-null next cursor. Continuing pages require a different non-null cursor, and the durable journal detects longer cursor cycles across resumed bounded polls. A terminal response may still supply a resume token for a later incremental poll. HTTP 204 means no new entity and preserves the current cursor.

Duplicate JSON keys, excessive nesting/structure, non-finite values, null provider revision IDs, invalid item counts and conflicting source identities fail visibly. The connector never converts an arbitrary API response into a successful empty page.

## Crash and quality boundaries

A fenced lease gives one worker ownership of a source. Its heartbeat continues during network reads and retries. A stale worker cannot stage a response, apply a page, discard another worker's evidence or clear the current owner's state. The lease is checked again before checkpoint mutation; expiry during a long ingestion transaction rolls back its data and outbox updates while retaining the fetched page.

A complete entity accepted by the HTTP transport limits is saved before mapping/ingestion. Its identity covers the request checkpoint, raw bytes and response metadata. If the process stops after staging, a later worker processes those saved bytes before making another HTTP request. Cursor/data commit is atomic: if a process stops after that transaction, the next poll begins at the committed next cursor.

Mapping, event validation and quality failures retain the entity as a blocked page. The cursor, materialized table, event deduplication state and publication outbox remain unchanged. Further polls do not repeatedly fetch or replace that blocked response.

Inspect the page list, then use an actual returned page ID:

```sh
python3 -m dagrivane.http_source --database .local/http-ingestion.sqlite pages feed
python3 -m dagrivane.http_source --database .local/http-ingestion.sqlite page feed "<page-id>"
python3 -m dagrivane.http_source --database .local/http-ingestion.sqlite export-page feed "<page-id>" --output .local/reviewed-entity.json
python3 -m dagrivane.http_source --database .local/http-ingestion.sqlite discard feed "<page-id>" --reason "Upstream corrected the reviewed response"
```

Raw exports are create-only. Discard requires a reason and no active worker, preserves the original entity and leaves the source position unchanged. A later poll refetches that same position. Discard does not skip an invalid page or repair its contents. Retained raw entities contain source data and should be handled as such.

Empty pages evaluate the current materialized table's quality gates. Successful empty responses add journal evidence but no new dataset generation. Cancellation is cooperative between page operations, retries and body reads. A page already committed is retained; cancellation does not undo completed effects.

## HTTP behavior and retry boundaries

Only direct HTTP(S) GET requests are issued; inherited proxy settings and cookies are not used. Redirects must stay on the configured origin and preserve existing query values, including cursor and scope filters. Credentials are not forwarded to another origin. Gzip/other content encodings and ambiguous transfer framing are rejected. Ordinary and chunked JSON entity bodies are bounded before staging.

408, 429, 500, 502, 503 and 504 responses and transient transport failures can retry within the declared attempt/deadline limits. `Retry-After` supports integer seconds or an HTTP date. A server delay beyond the inline budget is stored as a cooldown rather than shortened to retry sooner. Polls during that cooldown make no request. Certificate-validation failures, cross-origin redirects and non-retryable HTTP statuses fail without cursor progress.

The poll deadline is checked between I/O operations; an active socket operation can last up to its configured timeout. Cancellation is not a hard process interrupt. Error diagnostics exclude request URLs, authorization values and remote error-body text. Raw successful response data remains available through the explicit journal/export path.

Each poll processes at most 100 pages and has a 0.05–300 second work budget. Socket timeouts are 0.05–30 seconds, attempts 1–5, redirects 0–5, entities at most 4 MiB and items at most 10,000 per page. JSON structure is capped at depth 32 and 250,000 nodes. Per-source retained-entity quota is explicit and at most 512 MiB; at most 10,000 pages and 1,000 sources are retained. The existing ingestion batch, table, state and event budgets also apply. Reaching a quota stops progress without deleting evidence. The `journal_bytes` counter and quota count raw HTTP entity bytes; metadata, receipts and ingestion/catalog bundles consume additional disk space. Compaction, retention and quota migration remain open work.

## Audit and exit status

`audit SOURCE --limit N` verifies raw bytes, metadata/checkpoint identities, applied/discarded page chains, deterministic event batches, current cursor/offset/generation and journal counters. It recomputes empty-page quality against the historical materialized generation and replays ingestion in the same SQLite read snapshot. A concurrent writer cannot silently mix two database generations in that audit.

Audits are limited to 1,000 pages/64 MiB of entity data and the existing ingestion-audit limits. Truncated audits have `complete: false` and `passed: false`. A fetched or blocked pending page can be valid retained state: an audit passing does not mean that a source is drained, a blocked page is acceptable, or all publication work is complete. Publication remains separately recoverable and ordered through the existing outbox.

CLI exit status is 0 for a drained poll or successful operation, 3 for a bounded/temporarily deferred poll (page limit, deadline, cooldown or another owner), 2 for invalid input/blocked/error state, 130 for cancellation, and 1 for a failed/incomplete audit. Poll results retain completed page summaries and the current source status. They do not present a partial scan as a complete result.

## Verification and remaining scope

The real loopback tests cover row/change feeds, native JSON decimal precision, retries/cooldowns, slow-response heartbeats, stale ownership, concurrency, chunked/oversized entities, redirects, blocked-page review, cancellation, crash recovery, direct CLI continuation/export, local publication and coherent replay. Clean installed controls retain raw entity bytes and independently reconstruct final rows and decimal totals before checking catalog bytes and parent lineage.

Evidence: [HTTP extraction and publication](http-source-verification.json) and [current package checks](verification.json). Provider-specific SDKs/CDC adapters, watched services, schema migration/backfills, retention/compaction, signed provenance, browser ingestion management and the full platform/20,000 useful-SLOC requirements remain open.

Current verification: 27 HTTP-source checks and 163 total Dagrivane checks passed; the complete portfolio has 445 passing checks. Clean installed controls passed outside the checkout. Current project-specific production source is 9,721 SLOC, below 20,000. No external publication was performed.
