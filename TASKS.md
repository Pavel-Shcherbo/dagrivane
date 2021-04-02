# Tasks

Updated: 2026-11-04. Actual status only; historical roadmap rows remain proposed.

| Task | Status | Evidence | Remaining work |
|---|---|---|---|
| Core domain path | Implemented; 99 current tests passed | `dagrivane/`, `tests/` | Expand adversarial coverage as the model grows |
| Local CLI | Implemented; CLI end-to-end checks passed | `dagrivane/__main__.py`, `examples/` | Retained in the local verification evidence |
| Local workbench | Implemented; local HTTP and reviewed browser flows passed | `dagrivane/web.py`, `dagrivane/static/` | Retain checks as new screens and flows are added |
| Broad product scope | Planned | `docs/PRODUCT_SCOPE.md` | Most platform subsystems require implementation |
| Weekly reconstruction plan | Written | `docs/proposed-weekly-roadmap.csv` | Proposed commit counts are not actual commits |
| Publication | Waiting on Pavel | Current request: do not upload without Pavel | No push or hosted deployment |
| Production implementation ≥20,000 useful SLOC | Not achieved | 5408 project-specific SLOC measured; not achieved | Implement and verify the broad subsystem scope without padding |
| Durable control plane | Implemented; 14 control-plane checks passed; 99 total domain/integration checks passed | `dagrivane/control.py`, `tests/test_control.py` | Worker and management CLI integration passed; concurrent cold-start WAL race fixed and retested; operator documentation in `docs/OPERATIONS.md`; broader controls remain open |
| Fenced local worker and operator CLI | Implemented; five worker/CLI integration tests passed | `dagrivane/worker.py`, `dagrivane/manage.py` | Retain refreshed evidence and expose control-plane state in the workbench |
| Installable local package | Verified from a clean environment outside the source tree | `docs/installed-verification.json`, `_build.py`, `MANIFEST.in` | Cross-version/platform qualification remains open |
| Typed data adapter and quality-gated artifacts | Verified local increment | `docs/DATAFLOWS.md`, contract/table/relational/dataflow tests, installed and browser evidence | Incremental/remote sources, incremental dataset processing and distributed operators remain open |

| Artifact catalog and local release pointers | Verified local increment; 35 additional checks | `docs/ARTIFACTS.md`, `docs/release-browser-verification.json` | Remote storage, signatures, retention and actual deployment adapters remain open |

Next executable increment: expose workflow versions, queued runs and event inspection in the workbench; add incremental ingestion and artifact retention and signed provenance before distributed execution.

Local reconstruction: five initial substantive stage commits created, plus verification, control-plane, typed-data and artifact/release increments. No remote is configured; no upload was performed. The full dated roadmap remains proposed.

Current implementation verification: 99 tests passed, CLI checked, clean wheel installation and installed workbench HTTP checked. Broad scope and the 20,000-SLOC requirement remain open.

Verified increment: typed contracts, bounded CSV sources, relational operators, quality-gated create-only artifacts and built-in subprocess dataflow tasks. 99 tests passed, including an independent SQLite relational oracle. Clean installed CLI and workbench success/failure checks passed; desktop and mobile browser flows inspected. Evidence: `docs/verification.json`, `docs/installed-verification.json`, `docs/browser-verification.json`. Full product scope and 20,000 useful SLOC remain open.

Verified increment: content-addressed artifacts, immutable dataset policies and versions, bounded provenance traversal, pinned local releases, prerequisite environment pointers and independently verifiable exports. 99 tests, clean installed CLI/HTTP checks and healthy/blocked desktop/mobile browser flows passed. The exported application was independently executed against its exported table: 4 orders, USD 112.00. Evidence: `docs/ARTIFACTS.md`, `docs/release-browser-verification.json`, `docs/release-download-verification.json`. Full product and 20,000-SLOC requirements remain open.
