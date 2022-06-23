# Durable incremental ingestion

An ingestion stream combines a typed contract, stable key columns, an event-time column, a lateness policy, quality gates and an artifact-catalog target. Accepted batches advance a durable numeric source checkpoint and queue one immutable table snapshot for publication. The interface is local CLI/library code; no remote connector, production daemon or browser ingestion screen is configured.

## Complete local example

Run from this checkout. From a wheel, first export examples with `python3 -m dagrivane.resources demo` and replace `examples/` with `demo/`:

```sh
python3 -m dagrivane.ingest_cli --database .local/ingestion.sqlite init
python3 -m dagrivane.ingest_cli --database .local/ingestion.sqlite register orders examples/incremental-stream.json
python3 -m dagrivane.ingest_cli --database .local/ingestion.sqlite csv-batch orders examples/incremental-orders.csv --after 0 --output initial-batch.json
python3 -m dagrivane.ingest_cli --database .local/ingestion.sqlite ingest orders initial-batch.json --request-key initial
python3 -m dagrivane.ingest_cli --database .local/ingestion.sqlite ingest orders examples/incremental-changes.json --request-key changes
python3 -m dagrivane.ingest_cli --database .local/ingestion.sqlite rows orders
python3 -m dagrivane.ingest_cli --database .local/ingestion.sqlite configure-target orders --catalog .local/ingest-artifacts
python3 -m dagrivane.ingest_cli --database .local/ingestion.sqlite publish orders --catalog .local/ingest-artifacts
python3 -m dagrivane.ingest_cli --database .local/ingestion.sqlite audit orders --catalog .local/ingest-artifacts
```

The initial CSV has order 1 amount 10 and order 2 amount 20. The change batch updates order 1 to 15, deletes order 2, and adds order 3 amount 30. The resulting snapshot has orders 1 and 3, total amount 45, one retained deletion tombstone, generation 2 and source offset 5. Two catalog versions publish in order; version 2 names version 1's artifact as a parent. All source data is a synthetic fixture.

Example export and derived-batch file publication are create-only. Repeating ingestion of the exact same batch bytes returns the original receipt with `reused: true`. It does not advance the checkpoint again. Do not rerun a whole example against an unrelated existing stream/catalog destination without reviewing its current status.

## Stream contract

The definition has exactly `version`, `contract`, `keys`, `event_time_column`, `allowed_lateness_seconds`, `gates`, `target`.

- `version` is integer 1. `contract` is the existing typed-table contract, with exact named fields and explicit scalar types.
- `keys` has 1–16 distinct non-nullable contract columns. Key JSON is bounded to 1 KiB. Decimal key scales normalize numerically; zero signs normalize; timestamps normalize to UTC. Text remains case-sensitive.
- `event_time_column` names a non-nullable timestamp column. It must agree with each upsert event's `at` value after UTC normalization.
- `allowed_lateness_seconds` is an integer from 0 to 366 days. It is evaluated against the greatest event time accepted earlier in source-offset order, including earlier events in the same batch.
- `gates` uses the existing quality rules and is evaluated against the complete materialized live table after the batch. Failure rejects the entire batch.
- `target` has `workspace`, `dataset`, `policy_version`. The policy version must match the contract version. Catalog policy rules and version limits continue to apply.

Definitions are immutable. Registering the same normalized definition is idempotent; different semantics require a new stream. This does not automatically migrate rows or reset offsets. `configure-target` creates the explicit workspace/policy or verifies an identical existing policy; it cannot invent missing earlier policy versions or override a conflicting one.

## Event batch contract

A batch contains integer `version: 1`, nonnegative signed-64-bit `after` and `through` offsets, `events`, and optional `source_sha256` (at most eight distinct source hashes). `through` must exceed `after`. Offsets are source-declared numeric positions, not inferred row counts. Events must have strictly increasing offsets greater than `after` and no greater than `through`. Gaps and an empty window that advances `through` are permitted; they assert a source window was examined, not that omitted source events were independently discovered.

Every event has exactly:

```json
{
  "id": "source-event-123",
  "offset": 123,
  "operation": "upsert",
  "key": {"id": 1},
  "at": "2020-01-02T12:00:00Z",
  "row": {"id": 1, "amount": "15", "updated_at": "2020-01-02T12:00:00Z"}
}
```

`delete` uses the same fields with `row: null`; it retains key, event time, source position and event ID as a tombstone. Upsert keys must agree with row keys. Input rows pass the full typed contract. Unknown fields, duplicate JSON keys, invalid timestamps/numbers, duplicate event IDs within a batch and offset-order violations are rejected.

A previously seen event ID with identical normalized operation/key/time/row is a duplicate even when redelivered at a new source offset. The offset is deliberately excluded from content identity. A changed payload under the same event ID fails the whole batch. Duplicate events do not update rows or watermark and are accepted even if their old event time is now outside the lateness boundary.

New events earlier than `watermark - allowed_lateness_seconds` reject the batch. Within that boundary, an event older than the current value for its key is recorded as stale and does not overwrite it. This includes tombstones, preventing late resurrection. At equal event times, a later source offset wins. Receipts count applied, duplicate and stale events separately. There is no silent quarantine or administrative override; older backfills need an explicitly separate reviewed stream/workflow.

## Atomic checkpoint and quality boundary

One SQLite write transaction validates the current source checkpoint, applies events, evaluates gates and stores rows/tombstones, deduplication IDs, the raw batch, receipt, output bundle and pending publication. Any validation, range, key, quality or storage error rolls back all of these together. Competing different batches from the same checkpoint cannot both advance it.

A request key is scoped to one stream and exact batch bytes. Reusing it for changed bytes fails, including whitespace-only changes. The same raw batch may be retried with another request key and still return its original receipt. Stream definitions and raw batches have separate identities. Repeated batches return their original generation, even if later generations have since committed.

Output rows are ordered by canonical key encoding, not by numeric key order. Decimal data remains exact through the table contract. A stream snapshot retains the current raw-batch hash, declared upstream file hashes and the previous bundle hash; the local raw-history chain provides deeper provenance. This does not authenticate a remote source or prove that an upstream file was available in the past.

## Recoverable catalog publication

Ingestion and catalog publication are separate transactions. Publication selects the earliest unacknowledged generation, obtains a durable claim, and uses a deterministic catalog request key based on store identity, stream and generation. The predecessor's acknowledged artifact becomes its parent. A later generation cannot skip an unacknowledged predecessor.

The publisher verifies the target policy before claiming, writes the saved exact bundle to a private temporary file, invokes catalog publication, independently verifies the catalog artifact and acknowledges its ID/hash/target/lineage. The acknowledgement requires the current claim token and an unexpired lease. An expired or superseded worker is fenced.

If the process stops after the catalog commit but before acknowledgement, its lease eventually expires. Another publisher retries the same bytes, request key and parents. The catalog returns the existing artifact rather than allocating another version. Ordinary caught errors release the current claim for retry and retain an error summary; an abrupt process stop leaves the lease until expiry. This is recoverable idempotency across these two local stores, not an end-to-end exactly-once guarantee for an arbitrary external source or deployment.

Default lease: 30 seconds. `publish --lease-seconds N` supports 1–300 seconds; choose a longer lease for larger local quality checks. There is no background publisher heartbeat. Each command publishes at most 100 generations (`--limit` can lower this). Output includes `remaining` and status counts; a successful command does not imply the queue is empty when another worker holds a lease or the limit was reached. The first claimed publication attempt binds the stream to the catalog's absolute local path. Moving or replacing that destination requires an explicit migration workflow that is not implemented here.

## CSV adapter

`csv-batch STREAM CSV --after N --output NEW.json` creates an upsert event for each of 1–10,000 CSV rows. Contract headers, values and the source hash use the existing bounded CSV loader. `--sha256 HASH` can pin the source bytes. Event IDs derive from exact CSV bytes and record number, so redelivering the same revision at new offsets is deduplicated. A changed file revision creates new IDs; event-time staleness rules still determine whether rows change.

A CSV is not treated as a full replacement: rows absent from it are not deleted. Deletions require explicit events. An empty CSV cannot infer an advancing source offset and is rejected by this adapter. The generated batch includes the original file SHA-256 and is validated before create-only output publication. No watched-directory daemon, automatic file deletion or network extraction is performed.

## Status and verification

`status STREAM` reports definition, checkpoint, watermark, event count, bound catalog and publication counts. `rows STREAM` checks the cached state fingerprint before returning live rows and the tombstone count. `batches STREAM --after-generation N --limit K` returns bounded summaries, omits per-event outcome arrays and includes a next-generation cursor. Full per-event outcomes are returned by ingestion receipts; there is no separate receipt-detail endpoint yet.

`audit STREAM` replays original batches and compares their identities, source-window chain, receipts, event deduplication ownership, output bundles, row cache and final checkpoint. `--catalog ROOT` additionally verifies acknowledged artifact bytes, policy and stored metadata in that catalog. A passed audit establishes local replay consistency, not that all pending work is published or that external source assertions are true. Pending/leased generations are valid ongoing state. Audits are bounded; truncated audits have `complete: false` and `passed: false`.

Limits: 1,000 streams per store; 4 MiB and 10,000 events per batch; 100,000 retained keys including tombstones; one million retained event IDs per stream; 32 MiB encoded materialized state. Existing typed-table row, cell and byte limits also apply. Listing summaries are capped to 4 MiB per page. Audit limits are 1,000 batches, 100,000 raw events and 64 MiB of raw input per call. Raw batches, deduplication records and tombstones are retained; automatic retention or garbage collection is not implemented.

The SQLite schema version is 1. New database files are owner-readable/writable, use transactional writer serialization and WAL, and reject unrelated or unsupported schemas. Store identity is persisted and must be retained for recovery. Commands print JSON; invalid input/storage exits 2, a completed failed/incomplete audit exits 1, successful operations exit 0. Read commands do not initialize a missing store.

## Remaining work

Remote CDC/API connectors, signed provenance, controlled schema migration/backfill, retention/compaction, a continuously running publisher, distributed ownership, browser ingestion management and deployment adapters remain open. No external service is configured by this block. Full platform scope and 20,000 useful production SLOC remain open.
