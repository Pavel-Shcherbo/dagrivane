# Request register

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

Implemented increment: 163 current tests passed; CLI and installed workbench entry points checked in a disposable clean environment. Current project-specific production source: 9721 SLOC, below 20,000. No publication.

Verified increment: typed contracts, bounded CSV sources, relational operators, quality-gated create-only artifacts and built-in subprocess dataflow tasks. 99 tests passed, including an independent SQLite relational oracle. Clean installed CLI and workbench success/failure checks passed; desktop and mobile browser flows inspected. Evidence: `docs/verification.json`, `docs/installed-verification.json`, `docs/browser-verification.json`. Full product scope and 20,000 useful SLOC remain open.

Verified increment: content-addressed artifacts, immutable dataset policies and versions, bounded provenance traversal, pinned local releases, prerequisite environment pointers and independently verifiable exports. 99 tests, clean installed CLI/HTTP checks and healthy/blocked desktop/mobile browser flows passed. The exported application was independently executed against its exported table: 4 orders, USD 112.00. Evidence: `docs/ARTIFACTS.md`, `docs/release-browser-verification.json`, `docs/release-download-verification.json`. Full product and 20,000-SLOC requirements remain open.

Verified increment: durable incremental streams, explicit source checkpoints, event/batch deduplication, temporal upserts/deletes with tombstones, atomic quality gates and an ordered leased catalog outbox. 23 new subsystem tests and 136 total Dagrivane tests passed. Clean installed CSV/change ingestion, duplicate delivery, late rejection, ordered publication, independent blob reading/hashing and full replay audit passed. Evidence: `docs/INGESTION.md`, `docs/incremental-ingestion-verification.json`. Remote connectors, retention, schema migration, browser ingestion management and full product/20,000 useful-SLOC scope remain open.

Verified increment: durable operations workbench with reviewed immutable versions, scoped queue execution, fresh pinned input copies, cancellation, linked retries and recorded task output. Fourteen new checks, 136 total Dagrivane tests, clean installed HTTP controls and real desktop/mobile browser flows passed. Independent downloaded inspection matched durable results, source arithmetic and hashes; delayed responses did not overwrite the selected run. Evidence: docs/OPERATIONS_WORKBENCH.md, docs/operations-verification.json, docs/operations-browser-verification.json. Ingestion management, connector/retention/migration services and broad product/20,000 useful-SLOC scope remain open.

Verified increment: direct HTTP row/change feeds with typed JSON-pointer mapping, exact JSON decimals, durable raw-page staging, fenced leases/heartbeats, atomic data/cursor commits, retry cooldowns, blocked-entity review and coherent local audits. Twenty-seven new checks and 163 total Dagrivane tests passed, including commit-time lease expiry rollback. Clean installed two-page extraction, bounded resume, idle polling, transient retry, quality rejection, raw exports, independent change reduction and ordered catalog publication passed. Evidence: docs/HTTP_SOURCES.md and docs/http-source-verification.json. Provider SDKs/CDC, ingestion browser, schema migration, retention/quota migration, watched services, remote deployment and full product/20,000 useful-SLOC scope remain open.
