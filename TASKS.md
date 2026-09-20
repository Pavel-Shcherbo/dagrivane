# Tasks

Updated: 2026-11-04. Actual status only; historical roadmap rows remain proposed.

| Task | Status | Evidence | Remaining work |
|---|---|---|---|
| Core domain path | Implemented; 277 current tests passed | `dagrivane/`, `tests/` | Expand adversarial coverage as the model grows |
| Local CLI | Implemented; CLI end-to-end checks passed | `dagrivane/__main__.py`, `examples/` | Retained in the local verification evidence |
| Local workbench | Implemented; local HTTP and reviewed browser flows passed | `dagrivane/web.py`, `dagrivane/static/` | Retain checks as new screens and flows are added |
| Broad product scope | Planned | `docs/PRODUCT_SCOPE.md` | Broad platform subsystems still require implementation |
| Weekly reconstruction plan | Written | `docs/proposed-weekly-roadmap.csv` | Proposed commit counts are not actual commits |
| Publication | Waiting on Pavel | Current request: do not upload without Pavel | No push or hosted deployment |
| Production implementation ≥20,000 useful SLOC | Not achieved | 15474 project-specific SLOC measured; not achieved | Implement and verify the broad subsystem scope without padding |
| Durable control plane | Implemented; 14 control-plane checks passed; 136 total domain/integration checks passed | `dagrivane/control.py`, `tests/test_control.py` | Worker and management CLI integration passed; concurrent cold-start WAL race fixed and retested; operator documentation in `docs/OPERATIONS.md`; broader controls remain open |
| Fenced local worker and operator CLI | Implemented; five worker/CLI integration tests passed | `dagrivane/worker.py`, `dagrivane/manage.py` | Control-plane workbench verified; remote worker service and distributed recovery qualification remain open |
| Installable local package | Verified from a clean environment outside the source tree | `docs/installed-verification.json`, `_build.py`, `MANIFEST.in` | Cross-version/platform qualification remains open |
| Typed data adapter and quality-gated artifacts | Verified local increment | `docs/DATAFLOWS.md`, contract/table/relational/dataflow tests, installed and browser evidence | Incremental/remote sources, incremental dataset processing and distributed operators remain open |

| Artifact catalog and local release pointers | Verified local increment; 35 additional checks | `docs/ARTIFACTS.md`, `docs/release-browser-verification.json` | Remote storage, signatures, retention and actual deployment adapters remain open |

| Durable incremental ingestion and publication outbox | Verified local increment; 23 new tests, 136 total | `docs/INGESTION.md`, `docs/incremental-ingestion-verification.json` | Remote CDC/API extraction, retention, migration, daemon/heartbeat and browser management remain open |

| Durable HTTP row/change sources | Verified local increment; 27 new checks, 277 current tests | `docs/HTTP_SOURCES.md`, `docs/http-source-verification.json` | Provider SDKs/CDC, browser management, schema/retention/quota migration and full product/size scope remain open |

| Source-to-dataset workbench | Verified local increment; 24 new checks, 277 current tests | `docs/INGESTION_WORKBENCH.md`, installed/browser/download/150-response workload evidence | General remote-provider UI, migrations, retention/deletion, watched services and full size/scope remain open |

Next executable increment: add reviewed schema migration and retention controls, expand connector/provider management and qualify watched services before distributed execution.

Local reconstruction: five initial substantive stage commits created, plus verification, control-plane, typed-data, artifact/release incremental-ingestion, operations-workbench and HTTP-source increments. No remote is configured; no upload was performed. The full dated roadmap remains proposed.

Current implementation verification: 277 tests passed, CLI checked, clean wheel installation and installed workbench HTTP checked. Broad scope and the 20,000-SLOC requirement remain open.

Verified increment: typed contracts, bounded CSV sources, relational operators, quality-gated create-only artifacts and built-in subprocess dataflow tasks. 99 tests passed, including an independent SQLite relational oracle. Clean installed CLI and workbench success/failure checks passed; desktop and mobile browser flows inspected. Evidence: `docs/verification.json`, `docs/installed-verification.json`, `docs/browser-verification.json`. Full product scope and 20,000 useful SLOC remain open.

Verified increment: content-addressed artifacts, immutable dataset policies and versions, bounded provenance traversal, pinned local releases, prerequisite environment pointers and independently verifiable exports. 99 tests, clean installed CLI/HTTP checks and healthy/blocked desktop/mobile browser flows passed. The exported application was independently executed against its exported table: 4 orders, USD 112.00. Evidence: `docs/ARTIFACTS.md`, `docs/release-browser-verification.json`, `docs/release-download-verification.json`. Full product and 20,000-SLOC requirements remain open.

Verified increment: durable incremental streams, explicit source checkpoints, event/batch deduplication, temporal upserts/deletes with tombstones, atomic quality gates and an ordered leased catalog outbox. 23 new subsystem tests and 136 total Dagrivane tests passed. Clean installed CSV/change ingestion, duplicate delivery, late rejection, ordered publication, independent blob reading/hashing and full replay audit passed. Evidence: `docs/INGESTION.md`, `docs/incremental-ingestion-verification.json`. Remote connectors, retention, schema migration, browser ingestion management and full product/20,000 useful-SLOC scope remain open.

Verified increment: durable operations workbench with reviewed immutable versions, scoped queue execution, fresh pinned input copies, cancellation, linked retries and recorded task output. Fourteen new checks, 136 total Dagrivane tests, clean installed HTTP controls and real desktop/mobile browser flows passed. Independent downloaded inspection matched durable results, source arithmetic and hashes; delayed responses did not overwrite the selected run. Evidence: docs/OPERATIONS_WORKBENCH.md, docs/operations-verification.json, docs/operations-browser-verification.json. Ingestion management, connector/retention/migration services and broad product/20,000 useful-SLOC scope remain open.

Verified increment: direct HTTP row/change feeds with typed JSON-pointer mapping, exact JSON decimals, durable raw-page staging, fenced leases/heartbeats, atomic data/cursor commits, retry cooldowns, blocked-entity review and coherent local audits. Twenty-seven new checks and 163 total Dagrivane tests passed, including commit-time lease expiry rollback. Clean installed two-page extraction, bounded resume, idle polling, transient retry, quality rejection, raw exports, independent change reduction and ordered catalog publication passed. Evidence: docs/HTTP_SOURCES.md and docs/http-source-verification.json. Provider SDKs/CDC, ingestion browser, schema migration, retention/quota migration, watched services, remote deployment and full product/20,000 useful-SLOC scope remain open.

Verified ingestion-workbench increment: 24 new checks, 187 Dagrivane tests, clean installed API and real desktop/mobile browser controls passed. Source/data/outbox views are coherent, poll/publication claims are fenced, original rejected entities survive explicit correction/discard/refetch, and action receipts recover a lost response without another fetch. Independent downloads reconciled live keys 1/3, tombstone 2, amount 45 and verified catalog ancestry. A 150-response workload retained only two data generations and did not misreport bounded audit completeness. Evidence: docs/INGESTION_WORKBENCH.md, docs/ingestion-workbench-verification.json, docs/ingestion-browser-verification.json, docs/ingestion-benchmark.json. Current project-specific source: 11468 useful SLOC. General provider management, migrations, retention/deletion, watched services, signed provenance and full product/20,000 useful-SLOC scope remain open. No external publication.

Retained calendar scheduler implemented and verified below. Existing legacy interval scheduler remains separate. Full scope/20,000 useful-SLOC acceptance remains open.

Verified retained scheduler increment: 43 new checks, 230 Dagrivane tests and 615 portfolio tests passed. Interval/numeric-cron schedules retain timezone TZif bytes, explicit fold/gap and missed/overlap policies; queue insertion, cursor, events, fairness head and response receipts commit atomically. Pause/resume is generation-fenced; a foreground watch loop supports automatic dispatch. Clean installed commands executed a real isolated scheduled pipeline with independently read 4 orders/USD 112.00, preserved lost-response receipts and verified an export after deleting the source database. A retained 100-schedule/2,000-slot queue workload and all complete schedule audits passed. Evidence: docs/SCHEDULES.md, docs/schedule-verification.json and docs/schedule-benchmark.json. Current project-specific production source: 12893 SLOC. Calendar browser, supervised/distributed services, retention, remote providers, schema evolution and full product/20,000 useful-SLOC acceptance remain open. No publication.

| Retained calendar schedules and foreground dispatcher | Verified local increment; 43 new checks, 230 current tests | `docs/SCHEDULES.md`, clean installed worker/evidence controls and 2,000-slot workload | Calendar browser, retention, supervised/distributed services and full size/scope remain open |

Verified schema-evolution increment: 26 new checks, 256 Dagrivane tests and 712 portfolio tests passed. Explicit copy/cast/constant mappings retain immutable definitions, keys, clocks, tombstones and original deduplication hashes. Complete source replay and checkpoint/publication/connector comparisons gate an atomic ingestion cutover, source retirement, bootstrap outbox and durable retry receipt. Installed CLI controls independently reconciled live keys 1/3/4, exact total 50, retained deletion, inherited IDs, unchanged old artifact bytes and catalog ancestry; exported proofs replay after removing the source database and catalog. A resumed successor HTTP connector retained its cursor and offset while the old worker was fenced. A retained 5,000-key workload preserved 1,000 tombstones and 6,000 IDs with independently reconciled total 120020.00. Evidence: docs/STREAM_MIGRATIONS.md, docs/stream-migration-verification.json and docs/stream-migration-benchmark.json. Current project-specific production source: 13772 SLOC. Schema/calendar browser controls, provider-specific and distributed services, retention, broader platform scope and the 20,000 useful-SLOC requirement remain open. No external publication.

| Reviewed stream schema cutover | Verified local increment; 26 new checks and installed CLI/independent storage controls | `docs/STREAM_MIGRATIONS.md`, `docs/stream-migration-verification.json`, `docs/stream-migration-benchmark.json` | Browser, provider/distributed rollout, retention and full size/scope remain open |

Verified calendar-workbench increment: 21 calendar checks, 277 Dagrivane tests and 784 portfolio tests passed. Scoped saved schedules bind reviewed workflow fingerprints and retained timezone previews; pause/resume is generation-fenced, dispatch is queue-only and operation/response receipts commit together. Snapshot views and history pagination reject mixed generations. Clean installed HTTP/CLI controls passed, including overlap deferral, exact retries, real worker effects and replay after database removal. Actual desktop/mobile browser controls passed folded-hour slots, lost replies before/after commit, reload recovery, stale updates and delayed responses. Independent downloaded replay found and corrected numeric reserialization; the corrected generation-35 export verifies 35 events/two slots. Both reviewed workers independently reconciled four source orders/USD 112.00. Evidence: docs/CALENDAR_WORKBENCH.md, docs/calendar-workbench-verification.json and docs/calendar-browser-verification.json. Current project-specific production source: 15474 SLOC. Migration UI, services, providers, retention, broader platform scope and full product/20,000 useful-SLOC acceptance remain open. No publication.

| Saved calendar workbench | Verified local increment; 21 checks plus installed/browser/download controls | `docs/CALENDAR_WORKBENCH.md`, `docs/calendar-workbench-verification.json`, `docs/calendar-browser-verification.json` | Supervised services, retention, migration UI and full scope/size remain open |


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
