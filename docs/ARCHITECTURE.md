# Architecture

## Current implementation

A specification is validated and topologically ordered before any task runs. The executor records running state before launching each process, then commits its outcome. A run is tied to a specification and working-directory fingerprint. The workbench invokes only the bundled pipeline; it does not accept arbitrary command definitions over HTTP. Declared workflow inputs are checked against SHA-256 before a checkpoint can be reused. The control example pins both its CSV and task implementation. Undeclared files and external services are not captured by this identity; operators must declare relevant inputs and use a new run ID after changing them.

## Verification

Graph cycles, invalid commands, unknown dependencies, independent branches, retry counts, checkpoint reuse and interrupted-task outcomes.

## Local HTTP boundary

The workbench binds to loopback, validates the Host header, rejects foreign
Origin values and requires a custom header for state-changing requests. Static
files are served from an explicit allowlist. Request bodies are limited to 1 MiB.
These measures support a local development tool; there is no production identity,
authorization or organization model yet.

## Durable operations and local trust

`control` retains workspaces, immutable workflow versions, queue records, worker
leases and event cursors. `operations` snapshots declared inputs into a private
run directory and executes through the existing engine. Lease ownership fences
stale workers; an orphaned run needs explicit effect review before retry. The
Run queue workbench exposes these operator paths through reviewed examples.
Workspace keys partition records; they do not authenticate users or sandbox
commands. POSIX process-group termination is a timeout/cancellation mechanism,
not an isolation boundary. Remote executors and deployment adapters are absent.

## Typed data adapter

`contracts` defines explicit scalar types and version-compatibility reports. `tables` loads bounded, hashed CSV into immutable records. `transforms` compiles schemas and applies relational operations; join cardinality and output budgets are explicit. `quality` separates data acceptance from execution success. `dataflow` compiles the complete plan before input I/O and publishes one create-only JSON bundle after all gates pass.

`data_adapter` launches built-in plans through the graph's subprocess boundary, pinning imports to this installation. The normal checkpoint, timeout, cancellation and dependency semantics apply. The workbench selects between fixed bundled command and typed-data pipelines; it does not accept arbitrary process commands over HTTP. See DATAFLOWS.md for the exact scope and limits.

## Artifact and release boundary

The content-addressed object store verifies actual bytes independently of SQLite metadata. Immutable policies gate typed rows again at publication and verification. Dataset versions, build provenance, alias generations and pinned release manifests are transactional metadata; environment pointers require the exact prerequisite release and a fresh content check. Exported packages include the bounded parent closure and policies, allowing offline verification without the originating catalog. These are local selection primitives; authentication, signatures, retention and deployment adapters remain open. See ARTIFACTS.md.

## Incremental ingestion and publication recovery

`ingest_model` validates immutable stream contracts and reduces source-ordered events with event-time staleness, lateness and tombstones. `ingest_store` commits checkpoints, deduplication, materialized state and outbox bundles in one SQLite transaction. `ingest_publish` bridges the separate catalog transaction using ordered leased claims, deterministic request keys and fenced acknowledgement. `ingest_audit` replays raw batches and checks cached state, bundles and acknowledged catalog artifacts. `ingest_source` converts pinned CSV revisions to explicit upsert batches; `ingest_cli` supplies local operator commands. Remote source completeness and automatic schema migration are not inferred. See INGESTION.md.


## HTTP source checkpoints

`http_contract` validates direct endpoint, mapping, cursor and environment-backed
authentication declarations. `http_client` bounds transport bytes, redirects and
retry waits. `http_store` saves raw entities before mapping, then commits accepted
data and the next cursor coherently with ingestion state. A retained pending page
is processed before another fetch. Redirects preserve origin and query scope;
credentials are read from the declared environment variable, not saved in request
metadata. `http_audit` replays retained pages and refuses a green incomplete scan.
The ingestion workbench uses only reviewed loopback source profiles. See
[HTTP sources](HTTP_SOURCES.md) and [source workbench](INGESTION_WORKBENCH.md).

## Retained clocks and dispatch

`schedule_clock` resolves interval/cron definitions against retained timezone
rules; gaps, folds and missed slots have explicit policies. `schedule_store`
retains definitions and generations. `schedule_dispatch` commits queued runs,
slot outcomes, decision events and retry receipts in one transaction. Fair bounded
batches avoid one backlog starving every other due schedule. `schedule_watch` is
a foreground dispatcher; workers remain separate. `calendar_desk` provides saved
browser schedules, stale-view fencing, lost-response recovery and portable
snapshot evidence. See [schedules](SCHEDULES.md) and [calendar workbench](CALENDAR_WORKBENCH.md).

## Explicit stream evolution

`stream_migration_model` validates copy/cast/constant projections.
`stream_migration` prepares a frozen source checkpoint, checks projected data and
forks a successor with preserved tombstones and deduplication history. The apply
transaction fences the old writer and retains the publication handoff. HTTP
connectors move only through that reviewed successor transition. An offline
verifier reconstructs the proposed and accepted results. See
[stream migrations](STREAM_MIGRATIONS.md). Retention/compaction and automatic remote
deployment remain outside this implementation.
