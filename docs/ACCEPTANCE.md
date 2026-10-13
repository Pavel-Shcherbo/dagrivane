# Acceptance criteria

Dagrivane is qualified by its useful working behavior, not a source-line
quota. The earlier 20,000-SLOC target was withdrawn on 2026-11-04. Proposed
quarterly themes are backlog, not a claim that every subsystem exists.

## Required evidence

- Clean installation and example export outside the checkout.
- Real CLI/API flows and domain/integration tests, including failure and recovery.
- Desktop/mobile review of the implemented screens and downloaded evidence.
- Independent control values, input identities and reproducible exports.
- Explicit network, process, sensitive-data and storage boundaries.
- Representative bounded workloads with stated inputs and measured results.
- Documentation and retained evidence tied to the implementation they describe.

Tests and source size are separate measures. A passing test count is not proof of
unlimited scale, hosted tenant isolation or qualification of unimplemented features.
See [architecture](ARCHITECTURE.md), the subsystem guides, TASKS.md and REQUESTS.md.

## History and publication

The projects were assembled starting in November 2026. Earlier date labels belong
to a disclosed reconstruction. A separately retained local review history has
verified final source trees, restored final-version tests/CLI controls and portable
Git bundles. Intermediate stages are not individually release-qualified. Proposed
roadmap themes are not a completed-work record. The original main history remains
retained. Publication and deployment require Pavel's separate instruction.
