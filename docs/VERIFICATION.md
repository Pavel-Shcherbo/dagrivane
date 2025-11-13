# Initial-core verification

Verified locally on 2026-11-04. This is not full product acceptance.

- 256 current domain, integration and negative-path tests passed.
- The documented CLI control scenario completed and its result was checked.
- Local HTTP success/failure scenarios and foreign-Origin rejection were checked.
- Desktop and mobile workbench screens were inspected; the review includes defect corrections.
- Broader product scope and the 20,000 useful production-SLOC requirement remain incomplete.

Run `python3 -m unittest discover -s tests -v` from this checkout. The examples and README describe supported behavior and limits. No remote publication or hosted deployment was performed.

Version 0.2 wheel installation was checked in an independent virtual environment from an unrelated working directory. Example export, CLI, static files and public workbench calculations passed. Installed code matched the source files, and runtime state remained outside the package. See installed-verification.json for the wheel identity.

Typed-data verification includes exact decimal controls, schema/range/null rejection, UTC normalization, CSV hash pins, no-overwrite publication races, relational comparison against SQLite, join growth limits, command-graph blocking/resume and isolated browser success/failure workspaces. The clean installed typed example and workbench API passed. Desktop and 390-pixel mobile screens were inspected with no document overflow or browser console errors. See browser-verification.json and DATAFLOWS.md; broader product acceptance remains open.

Verified increment: content-addressed artifacts, immutable dataset policies and versions, bounded provenance traversal, pinned local releases, prerequisite environment pointers and independently verifiable exports. 99 tests, clean installed CLI/HTTP checks and healthy/blocked desktop/mobile browser flows passed. The exported application was independently executed against its exported table: 4 orders, USD 112.00. Evidence: `docs/ARTIFACTS.md`, `docs/release-browser-verification.json`, `docs/release-download-verification.json`. Full product and 20,000-SLOC requirements remain open.

Verified increment: durable incremental streams, explicit source checkpoints, event/batch deduplication, temporal upserts/deletes with tombstones, atomic quality gates and an ordered leased catalog outbox. 23 new subsystem tests and 122 total Dagrivane tests passed. Clean installed CSV/change ingestion, duplicate delivery, late rejection, ordered publication, independent blob reading/hashing and full replay audit passed. Evidence: `docs/INGESTION.md`, `docs/incremental-ingestion-verification.json`. Remote connectors, retention, schema migration, browser ingestion management and full product/20,000 useful-SLOC scope remain open.

Incremental controls include an independent seeded per-key event-time oracle, atomic rejection, duplicate identity conflicts, retained deletions, lateness boundaries, decimal-key identity, competing source checkpoints, publisher races, expired claims, actual catalog-commit/pre-ack interruption recovery, outbox/raw/cache corruption detection and schema initialization. The new ingestion interface is CLI/library only; prior browser evidence applies to the existing workbench screens.

Verified schema-evolution increment: 26 new checks, 256 Dagrivane tests and 712 portfolio tests passed. Explicit copy/cast/constant mappings retain immutable definitions, keys, clocks, tombstones and original deduplication hashes. Complete source replay and checkpoint/publication/connector comparisons gate an atomic ingestion cutover, source retirement, bootstrap outbox and durable retry receipt. Installed CLI controls independently reconciled live keys 1/3/4, exact total 50, retained deletion, inherited IDs, unchanged old artifact bytes and catalog ancestry; exported proofs replay after removing the source database and catalog. A resumed successor HTTP connector retained its cursor and offset while the old worker was fenced. A retained 5,000-key workload preserved 1,000 tombstones and 6,000 IDs with independently reconciled total 120020.00. Evidence: docs/STREAM_MIGRATIONS.md, docs/stream-migration-verification.json and docs/stream-migration-benchmark.json. Current project-specific production source: 13772 SLOC. Schema/calendar browser controls, provider-specific and distributed services, retention, broader platform scope and the 20,000 useful-SLOC requirement remain open. No external publication.
