# Tasks

Updated: 2026-11-04. Actual status only; historical roadmap rows remain proposed.

| Task | Status | Evidence | Remaining work |
|---|---|---|---|
| Core domain path | Implemented; 26 current tests passed | `dagrivane/`, `tests/` | Expand adversarial coverage as the model grows |
| Local CLI | Implemented; CLI end-to-end checks passed | `dagrivane/__main__.py`, `examples/` | Retained in the local verification evidence |
| Local workbench | Implemented; local HTTP and reviewed browser flows passed | `dagrivane/web.py`, `dagrivane/static/` | Retain checks as new screens and flows are added |
| Broad product scope | Planned | `docs/PRODUCT_SCOPE.md` | Most platform subsystems require implementation |
| Weekly reconstruction plan | Written | `docs/proposed-weekly-roadmap.csv` | Proposed commit counts are not actual commits |
| Publication | Waiting on Pavel | Current request: do not upload without Pavel | No push or hosted deployment |
| Production implementation ≥20,000 useful SLOC | Not achieved | 1883 project-specific SLOC measured; not achieved | Implement and verify the broad subsystem scope without padding |
| Durable control plane | Implemented; 14 control-plane checks passed; 26 total domain/integration checks passed | `dagrivane/control.py`, `tests/test_control.py` | Worker and management CLI integration passed; concurrent cold-start WAL race fixed and retested; operator documentation pending |
| Fenced local worker and operator CLI | Implemented; five worker/CLI integration tests passed | `dagrivane/worker.py`, `dagrivane/manage.py` | Retain refreshed evidence and expose control-plane state in the workbench |
| Installable local package | Verified from a clean environment outside the source tree | `docs/installed-verification.json`, `_build.py`, `MANIFEST.in` | Cross-version/platform qualification remains open |


Next executable increment: Add immutable input/artifact identities to checkpoints, an attempt-level event log and process-tree cleanup before distributed execution.

Local reconstruction: five initial substantive stage commits created, plus the current verification record. No remote is configured; no upload was performed. The full dated roadmap remains proposed.

Current implementation verification: 26 tests passed, CLI checked, clean wheel installation and installed workbench HTTP checked. Broad scope and the 20,000-SLOC requirement remain open.
