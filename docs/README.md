# Dagrivane guides

Start with a local workflow, then add typed data, retained sources or release gates.

[Install and run](../README.md#run-the-workbench) · [Architecture](ARCHITECTURE.md) · [Verified scope](VERIFICATION.md)

## Choose a task

| Task | Guide | What to expect |
|---|---|---|
| Run and recover command workflows | [Operations](OPERATIONS.md) | Register a version, submit work, inspect leases and explicitly retry reviewed failures. |
| Use the run-queue screen | [Operations Workbench](OPERATIONS_WORKBENCH.md) | Inspect saved runs, cancellation, pinned inputs and linked retries. |
| Transform typed CSV data | [Dataflows](DATAFLOWS.md) | Define contracts, decimal calculations, joins and quality gates. |
| Retain incremental source changes | [Ingestion](INGESTION.md) | Replay ordered batches, deduplicate delivery and retain tombstones. |
| Extract a JSON API | [Http Sources](HTTP_SOURCES.md) | Map raw pages to typed rows with cursor checkpoints and fenced leases. |
| Review rejected source data | [Ingestion Workbench](INGESTION_WORKBENCH.md) | Inspect checkpoints, correct rejected entities and verify catalog publication. |
| Pin an artifact or release | [Artifacts](ARTIFACTS.md) | Verify content hashes, lineage and prerequisite-gated environment pointers. |
| Schedule work from a local watcher | [Schedules](SCHEDULES.md) | Choose timezone, missed-run and overlap rules; dispatch and execution remain separate. |
| Use saved calendar controls | [Calendar Workbench](CALENDAR_WORKBENCH.md) | Preview retained timezone rules and export replayable decisions. |
| Change a stream schema | [Stream Migrations](STREAM_MIGRATIONS.md) | Review an explicit projection and atomically fence the old writer. |

## Scope and evidence

The [verification record](VERIFICATION.md) describes the actual platform, Python-version and installed-package checks. Subsystem guides record their own increment-specific evidence; an old test total is not a current total.

[Product scope](PRODUCT_SCOPE.md) and the weekly roadmap include future ideas. They are not a checklist of completed features. Earlier Git dates are explicitly reconstructed; see the [history note](../README.md#history-note).

[Contributing](../CONTRIBUTING.md) explains local checks and how to report a reproducible problem.
