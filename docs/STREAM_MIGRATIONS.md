# Stream schema migrations

A migration forks a verified, quiescent stream into a named successor with the next
immutable contract version. The old definition and published artifacts stay intact.
The old stream rejects new writes; its saved rows, receipts and replay remain readable.
Consumers and catalog aliases must be changed explicitly by their operator.

## Run the synthetic example

Run from a checkout, or export the installed examples with
`python3 -m dagrivane.resources demo` and replace `examples/` below with `demo/`.
Use fresh database, catalog and output paths for this walkthrough.

```sh
python3 -m dagrivane.ingest_cli --database ingestion.sqlite init
python3 -m dagrivane.ingest_cli --database ingestion.sqlite register orders examples/incremental-stream.json
python3 -m dagrivane.ingest_cli --database ingestion.sqlite configure-target orders --catalog catalog
python3 -m dagrivane.ingest_cli --database ingestion.sqlite csv-batch orders examples/incremental-orders.csv --after 0 --output initial.json
python3 -m dagrivane.ingest_cli --database ingestion.sqlite ingest orders initial.json --request-key initial
python3 -m dagrivane.ingest_cli --database ingestion.sqlite ingest orders examples/incremental-changes.json --request-key changes
python3 -m dagrivane.ingest_cli --database ingestion.sqlite publish orders --catalog catalog
python3 -m dagrivane.migrate_stream --database ingestion.sqlite prepare examples/orders-schema-migration.json --catalog catalog --output reviewed.json
```

Inspect `reviewed.json`: `plan` contains the exact new definition and mappings;
`projection` contains quality checks, compatibility findings and projected row/state
hashes. The example adds a required `currency` column with the explicit value `USD`
to the retained rows. It leaves two live keys, one tombstone and checkpoint 5.
Adding a required column can break existing consumers: a passing projection does
not mean that those consumers accept the new schema.

After reviewing the plan, register its target policy and apply its checkpoint:

```sh
python3 -m dagrivane.migrate_stream policy reviewed.json --catalog catalog
python3 -m dagrivane.migrate_stream --database ingestion.sqlite apply reviewed.json --catalog catalog --request-key orders-schema-two
python3 -m dagrivane.ingest_cli --database ingestion.sqlite publish orders-v2 --catalog catalog
python3 -m dagrivane.ingest_cli --database ingestion.sqlite audit orders-v2 --catalog catalog
python3 -m dagrivane.migrate_stream verify reviewed.json
```

Policy registration is a separate catalog transaction with explicit permission
for the reviewed breaking change. If application fails afterwards, the registered
policy remains available. There is no transaction spanning both databases.

## Projection rules

Every destination column requires one mapping:

- `{"operation":"copy","source":"old_name"}` preserves a value and its type.
- `{"operation":"cast","source":"old_name"}` requests a lossless type change.
- `{"operation":"constant","value":"USD"}` supplies an explicit typed value.

`drop_columns` lists exactly the unused source columns. A renamed copied column
is still used and is not a drop. Keys retain their arity, order, types and values;
the key and event-time column may be renamed only through `copy`. Workspace,
dataset and lateness policy stay fixed. Contract and target policy advance by
one version. New column constraints and all new quality gates must pass.

Supported casts include integer to decimal, exact integral decimal to signed
64-bit integer, typed values to their canonical string, and canonical strings to
the declared destination type. For example, `"001"` is rejected as an integer
cast because it does not round-trip canonically. Fractional decimals are never
silently truncated. Constant mappings apply to existing live rows during this
migration; future events must supply the complete new contract themselves.

## Recovery and HTTP handoff

Application checks the reviewed source identity, full history, connector cursor
and publication chain again inside the ingestion write transaction. A changed
checkpoint, even with unchanged live rows, requires a fresh review. All old
publications must be acknowledged; HTTP sources must have no active lease or
unresolved staged page. A lease that expires during review is still checked at
application, so a newly active worker blocks the transition.

Successor state, inherited event hashes, source retirement, an empty bootstrap
batch, publication outbox and response receipt commit together. The bootstrap
starts successor generation 1 at the same source offset, without consuming a
synthetic source event. Its first catalog artifact names the last old artifact
as its parent. Publication uses the existing retryable outbox. Repeating an
application with the same key and plan returns its saved receipt; a changed plan
under that key is rejected. Applying under another key after retirement fails.

Tombstones and event clocks survive the transition, preventing older events from
resurrecting deleted rows. Inherited event IDs retain their original content
hashes. Reusing an old ID with rewritten new-schema content is a conflict.

An existing HTTP connector is frozen with its old stream. Register a successor
connector explicitly, add mappings for all new columns, and set `initial_cursor`
to the JSON value decoded from `origin.connector.cursor` in the reviewed proof.
Registration captures the successor's current offset/generation, including its
bootstrap. Review the provider's cursor semantics before resuming; this operation
does not rewrite or automatically migrate provider configuration or credentials.

## Evidence and limits

The create-only proof file is written with private permissions. It retains source
batch bytes, publication acknowledgements, predecessor bases, projected state,
deduplication identities and the local destination path. It may contain business
rows and provider cursors; review it before sharing.

Offline verification reconstructs adapted ingestion history and re-runs the typed
projection. It works without the original database or catalog, but reports
`source_authenticated: false`: content consistency does not authenticate an
external provider, original timestamps or catalog owner. Raw HTTP protocol replay
belongs to the separate HTTP source journal and is not included in this proof.

Proofs are bounded to 64 MiB, 1,000 batches and 100,000 events per retained source
level, with at most four predecessor migrations. Existing key, state and typed
table limits also apply; these budgets are cumulative constraints, not a promise
that every individually maximal input fits. Review and application perform full
bounded replay and can hold the local writer lock. No background/distributed
migration service, browser editor, general expression engine, retention cleanup
or automatic consumer rollout is provided.

Verification evidence: `stream-migration-verification.json` records installed CLI
and independent SQLite/catalog checks; `stream-migration-benchmark.json` records
a 5,000-key workload with 1,000 retained tombstones. The subsystem tests also cover
atomic rollback, concurrent retries, stale plans, invalid casts/gates, tampered
proof/cache data, chained migrations and a resumed successor HTTP connector.
