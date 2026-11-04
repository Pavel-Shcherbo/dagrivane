# Request register

## Public repository status — 2026-11-04

Published with Pavel's explicit authorization at https://github.com/Pavel-Shcherbo/dagrivane.
The initial remote main matched qualified snapshot e8410a9087fcda219f0ad9da0b1746bf6c74971c.
The implemented local scope is qualified as recorded in docs/VERIFICATION.md.
This status supersedes earlier publication holds in the historical register below.
Earlier dates are a disclosed reconstruction; this publication record uses its actual date.
GitHub contribution-calendar rendering is checked separately and is not implied by a push.
No remaining local delivery blocker; future scope remains backlog.

---

## Current local qualification — 2026-11-04

Implemented scope: workflow/lease recovery, typed data and quality gates, retained HTTP checkpoints, schema forks, pinned releases and saved calendar dispatch. Full project suites passed on CPython
3.10–3.14: 278 tests per version. Clean wheel installation and CLI/API controls
passed on 3.10 and 3.14. [Current evidence](docs/VERIFICATION.md) supersedes old
size requirements and historical test totals below. Final package identities are maintained in the central review register;
no publication is authorized. Future provider/hosted/distributed
features in the original roadmap remain optional backlog.

---

Source: Pavel, current project-planning chat, 2026-11-04.

| Request | Acceptance | Status |
|---|---|---|
| Build three substantial portfolio products | Independently usable implementations, meaningful verification and clear limits | In progress; Dagrivane has an initial working core |
| Distinctive names | Check indexed web and GitHub repository-name matches | `dagrivane` returned zero repository-name matches on 2026-11-04; not trademark clearance or a search-rank guarantee |
| Broad international scope | Concrete subsystem roadmap and independently extensible interfaces | Scope documented; scale and adoption not yet established |
| Detailed 2019–2026 allocation | Weekly deliverables, acceptance conditions and proposed commit dates | Written as an explicit reconstruction plan |
| Past-dated commits | Use reconstructed stage dates and preserve sole configured authorship | Initial local reconstructed stages created; author Pavel-Shcherbo (ID 44784481); no remote configured |
| Professional interface and documentation | Distinct product vocabulary, working controls, readable states and browser checks | Implemented; review in progress |
| Test independently; do not upload without Pavel | Local verification, no remote publication | Local checks in progress; upload withheld |

Requests from Julius or other team members: none in this project's current scope.

## Additional acceptance from Pavel

Each product must contain at least 20,000 useful production SLOC, excluding tests, documentation, fixtures, dependencies and generated outputs. Current implementations are far below this target. Broad implementation and independent verification remain in progress.

The contribution-date proposal allows 0–4 commits per day, rest days and breaks. It is a reconstruction plan, not completed historical work. The GitHub target is Pavel-Shcherbo; retain the existing author configuration. No upload without Pavel.

Implemented increment: 277 current tests passed; CLI and installed workbench entry points checked in a disposable clean environment. Current project-specific production source: 15474 SLOC, below 20,000. No publication.

Verified increment: typed contracts, bounded CSV sources, relational operators, quality-gated create-only artifacts and built-in subprocess dataflow tasks. 99 tests passed, including an independent SQLite relational oracle. Clean installed CLI and workbench success/failure checks passed; desktop and mobile browser flows inspected. Evidence: `docs/verification.json`, `docs/installed-verification.json`, `docs/browser-verification.json`. Full product scope and 20,000 useful SLOC remain open.

Verified increment: content-addressed artifacts, immutable dataset policies and versions, bounded provenance traversal, pinned local releases, prerequisite environment pointers and independently verifiable exports. 99 tests, clean installed CLI/HTTP checks and healthy/blocked desktop/mobile browser flows passed. The exported application was independently executed against its exported table: 4 orders, USD 112.00. Evidence: `docs/ARTIFACTS.md`, `docs/release-browser-verification.json`, `docs/release-download-verification.json`. Full product and 20,000-SLOC requirements remain open.

Verified increment: durable incremental streams, explicit source checkpoints, event/batch deduplication, temporal upserts/deletes with tombstones, atomic quality gates and an ordered leased catalog outbox. 23 new subsystem tests and 136 total Dagrivane tests passed. Clean installed CSV/change ingestion, duplicate delivery, late rejection, ordered publication, independent blob reading/hashing and full replay audit passed. Evidence: `docs/INGESTION.md`, `docs/incremental-ingestion-verification.json`. Remote connectors, retention, schema migration, browser ingestion management and full product/20,000 useful-SLOC scope remain open.

Verified increment: durable operations workbench with reviewed immutable versions, scoped queue execution, fresh pinned input copies, cancellation, linked retries and recorded task output. Fourteen new checks, 136 total Dagrivane tests, clean installed HTTP controls and real desktop/mobile browser flows passed. Independent downloaded inspection matched durable results, source arithmetic and hashes; delayed responses did not overwrite the selected run. Evidence: docs/OPERATIONS_WORKBENCH.md, docs/operations-verification.json, docs/operations-browser-verification.json. Ingestion management, connector/retention/migration services and broad product/20,000 useful-SLOC scope remain open.

Verified increment: direct HTTP row/change feeds with typed JSON-pointer mapping, exact JSON decimals, durable raw-page staging, fenced leases/heartbeats, atomic data/cursor commits, retry cooldowns, blocked-entity review and coherent local audits. Twenty-seven new checks and 163 total Dagrivane tests passed, including commit-time lease expiry rollback. Clean installed two-page extraction, bounded resume, idle polling, transient retry, quality rejection, raw exports, independent change reduction and ordered catalog publication passed. Evidence: docs/HTTP_SOURCES.md and docs/http-source-verification.json. Provider SDKs/CDC, ingestion browser, schema migration, retention/quota migration, watched services, remote deployment and full product/20,000 useful-SLOC scope remain open.

Verified ingestion-workbench increment: 24 new checks, 187 Dagrivane tests, clean installed API and real desktop/mobile browser controls passed. Source/data/outbox views are coherent, poll/publication claims are fenced, original rejected entities survive explicit correction/discard/refetch, and action receipts recover a lost response without another fetch. Independent downloads reconciled live keys 1/3, tombstone 2, amount 45 and verified catalog ancestry. A 150-response workload retained only two data generations and did not misreport bounded audit completeness. Evidence: docs/INGESTION_WORKBENCH.md, docs/ingestion-workbench-verification.json, docs/ingestion-browser-verification.json, docs/ingestion-benchmark.json. Current project-specific source: 11468 useful SLOC. General provider management, migrations, retention/deletion, watched services, signed provenance and full product/20,000 useful-SLOC scope remain open. No external publication.

Retained calendar scheduler implemented and verified below. Existing legacy interval scheduler remains separate. Full scope/20,000 useful-SLOC acceptance remains open.

Verified retained scheduler increment: 43 new checks, 230 Dagrivane tests and 615 portfolio tests passed. Interval/numeric-cron schedules retain timezone TZif bytes, explicit fold/gap and missed/overlap policies; queue insertion, cursor, events, fairness head and response receipts commit atomically. Pause/resume is generation-fenced; a foreground watch loop supports automatic dispatch. Clean installed commands executed a real isolated scheduled pipeline with independently read 4 orders/USD 112.00, preserved lost-response receipts and verified an export after deleting the source database. A retained 100-schedule/2,000-slot queue workload and all complete schedule audits passed. Evidence: docs/SCHEDULES.md, docs/schedule-verification.json and docs/schedule-benchmark.json. Current project-specific production source: 12893 SLOC. Calendar browser, supervised/distributed services, retention, remote providers, schema evolution and full product/20,000 useful-SLOC acceptance remain open. No publication.

Verified schema-evolution increment: 26 new checks, 256 Dagrivane tests and 712 portfolio tests passed. Explicit copy/cast/constant mappings retain immutable definitions, keys, clocks, tombstones and original deduplication hashes. Complete source replay and checkpoint/publication/connector comparisons gate an atomic ingestion cutover, source retirement, bootstrap outbox and durable retry receipt. Installed CLI controls independently reconciled live keys 1/3/4, exact total 50, retained deletion, inherited IDs, unchanged old artifact bytes and catalog ancestry; exported proofs replay after removing the source database and catalog. A resumed successor HTTP connector retained its cursor and offset while the old worker was fenced. A retained 5,000-key workload preserved 1,000 tombstones and 6,000 IDs with independently reconciled total 120020.00. Evidence: docs/STREAM_MIGRATIONS.md, docs/stream-migration-verification.json and docs/stream-migration-benchmark.json. Current project-specific production source: 13772 SLOC. Schema/calendar browser controls, provider-specific and distributed services, retention, broader platform scope and the 20,000 useful-SLOC requirement remain open. No external publication.

Verified calendar-workbench increment: 21 calendar checks, 277 Dagrivane tests and 784 portfolio tests passed. Scoped saved schedules bind reviewed workflow fingerprints and retained timezone previews; pause/resume is generation-fenced, dispatch is queue-only and operation/response receipts commit together. Snapshot views and history pagination reject mixed generations. Clean installed HTTP/CLI controls passed, including overlap deferral, exact retries, real worker effects and replay after database removal. Actual desktop/mobile browser controls passed folded-hour slots, lost replies before/after commit, reload recovery, stale updates and delayed responses. Independent downloaded replay found and corrected numeric reserialization; the corrected generation-35 export verifies 35 events/two slots. Both reviewed workers independently reconciled four source orders/USD 112.00. Evidence: docs/CALENDAR_WORKBENCH.md, docs/calendar-workbench-verification.json and docs/calendar-browser-verification.json. Current project-specific production source: 15474 SLOC. Migration UI, services, providers, retention, broader platform scope and full product/20,000 useful-SLOC acceptance remain open. No publication.


## Scope update — 2026-11-04

Pavel withdrew the mandatory 20,000-SLOC threshold: it was indicative. Earlier
size requirements in this log are superseded. Do not expand solely for line count.
Prioritize useful verified behavior and meaningful reconstructed history. Existing
verification evidence remains scoped to the versions it records; final acceptance
and full history reconstruction remain open. No publication is authorized.

History audit: actual local Git objects, ownership and nonempty stages were checked;
implementation/tests/examples match the last verified source version. The proposed
2019–2026 calendar remains a plan, not a completed activity record. The central
planning register retains the actual commit/calendar audit and outstanding work.

## Reconstructed history review — 2026-11-04

A separate disclosed local review history has been built from actual source
changes. Its final runtime matches the verified implementation; independently
restored copies passed the portfolio's 869 unit tests and three CLI controls.
Dates are illustrative labels; intermediate stages are not individually qualified
releases. Calendar/object/final-tree evidence and packaging status are maintained
in the central planning register (P-047). Original main history is retained.
Publication and final portfolio acceptance remain pending; no upload is authorized.

## Supported-Python final check — 2026-11-04

A real Python 3.10 run exposed an empty-query parsing incompatibility in the
reviewed HTTP source. The absent query now explicitly maps to no parameters;
nonempty queries keep strict duplicate/field/framing validation. A real HTTP
regression checks first-page success and malformed cursor rejection without
provider effects. All 25 source-desk checks passed on Python 3.10 and 3.14.
Full Python 3.10–3.14 qualification is in progress; no publication performed.
