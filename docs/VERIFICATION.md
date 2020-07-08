# Initial-core verification

Verified locally on 2026-11-04. This is not full product acceptance.

- 64 domain, integration and negative-path tests passed.
- The documented CLI control scenario completed and its result was checked.
- Local HTTP success/failure scenarios and foreign-Origin rejection were checked.
- Desktop and mobile workbench screens were inspected; the review includes defect corrections.
- Broader product scope and the 20,000 useful production-SLOC requirement remain incomplete.

Run `python3 -m unittest discover -s tests -v` from this checkout. The examples and README describe supported behavior and limits. No remote publication or hosted deployment was performed.

Version 0.2 wheel installation was checked in an independent virtual environment from an unrelated working directory. Example export, CLI, static files and public workbench calculations passed. Installed code matched the source files, and runtime state remained outside the package. See installed-verification.json for the wheel identity.

Typed-data verification includes exact decimal controls, schema/range/null rejection, UTC normalization, CSV hash pins, no-overwrite publication races, relational comparison against SQLite, join growth limits, command-graph blocking/resume and isolated browser success/failure workspaces. The clean installed typed example and workbench API passed. Desktop and 390-pixel mobile screens were inspected with no document overflow or browser console errors. See browser-verification.json and DATAFLOWS.md; broader product acceptance remains open.
