# Acceptance criteria

dagrivane is an independent product. The broad target is at least 20,000 useful production source lines, excluding tests, documentation, fixtures, examples, dependencies and build outputs. Formatting and duplication do not establish usefulness. The current initial version has not reached that target.

Every implemented subsystem must have an executable user path, explicit failure behavior and retained verification. Source size is reported separately from functional evidence.

Release qualification requires a clean setup, documented CLI/API flows, domain tests, integration checks, desktop/mobile review, sensitive-data boundary checks and representative performance measurements. Planned subsystems remain open until their own acceptance conditions are verified.

See PRODUCT_SCOPE.md, ARCHITECTURE.md and the repository task register for scope, current limitations and evidence.


## Scope update — 2026-11-04

Pavel withdrew the mandatory 20,000-SLOC threshold: it was indicative. Earlier
size requirements in this log are superseded. Do not expand solely for line count.
Prioritize useful verified behavior and meaningful reconstructed history. Existing
verification evidence remains scoped to the versions it records; final acceptance
and full history reconstruction remain open. No publication is authorized.
