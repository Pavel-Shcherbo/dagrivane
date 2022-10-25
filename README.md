# Dagrivane

Inspect local command workflows and verify data release gates.

## Run the workbench

From a source checkout with Python 3.10 or later:

```sh
python3 -m dagrivane.web
```

Open http://127.0.0.1:8411. No third-party runtime packages are required.
The workbench is a local development application, not a hosted multi-user service.
Bundled examples are included in the wheel. Runtime state is stored outside the installed package.

## Workbench preview

![Saved workflow operations](docs/operations-desktop.png)

## Command line

```sh
python3 -m dagrivane examples/pipeline.json --run-id walkthrough
python3 -m unittest discover -s tests -v
```

## Implemented

Validates command graphs before execution, runs dependency-ordered tasks, persists checkpoints in SQLite, verifies declared input content hashes, retries failed tasks within a declared limit and blocks dependent work when a prerequisite fails. The bundled four-stage pipeline extracts synthetic orders, checks them, calculates an exact decimal total and verifies a release gate.

## Saved workflow operations

The **Run queue** screen registers reviewed workflow versions, saves idempotent submissions and executes selected runs through the fenced local worker. Inspect retained task output and events, cancel a queued or running job, retry as a linked new run and export the current inspection. Each execution copies its pinned inputs into a fresh directory. See the [workbench operations guide](docs/OPERATIONS_WORKBENCH.md) for the three control scenarios and evidence boundaries.

## Typed data pipelines

Run `python3 -m dagrivane.dataflow examples/dataflow.json` for schema-checked CSV processing, or export a fresh example directory and run `typed-pipeline.json` through the normal graph executor. The browser's **Execution** selector includes the typed data pipeline and a quality-failure control.

The adapter provides typed contracts, explicit decimal calculations, filters, bounded joins, grouping, stable sorting, quality gates and create-only output bundles. See [dataflow guide](docs/DATAFLOWS.md) for commands, schemas, null semantics and resource limits.

## Artifact and release catalog

`python3 -m dagrivane.release_demo --output .local/release-control` executes a quality-checked dataset and an application package through content-addressed artifact recording, pinned release verification and prerequisite-gated local environment pointers. The workbench includes healthy, changed-data and missing-prerequisite controls.

The [artifact guide](docs/ARTIFACTS.md) covers immutable policies, dataset versions, lineage, generation conflicts and independently verifiable release exports. `python3 -m dagrivane.catalog_cli --help` exposes the persistent operator interface.

## Boundaries

Task execution within each workflow run is local and sequential. Commands are trusted local programs, not sandboxed workloads. Captured output is not a secret store. A process crash may leave external effects with an unknown outcome; the runner refuses to silently retry a task marked running. The durable control plane supports versioned workflow registration, idempotent submission, worker leases, cancellation, event cursors and pinned interval schedules through the operator CLI. Tasks within each run remain sequential. Remote executors, connector SDKs, CI promotion and authenticated multi-user operation remain planned.


## Durable execution

`python3 -m dagrivane.manage --help` exposes local workspaces, version registration, submission, worker execution, cancellation, explicit retry and interval scheduling. See [operator guide](docs/OPERATIONS.md) for a complete sequence. An expired lease becomes orphaned; the operator must review effects before explicitly retrying. Workspace IDs scope records but are not an authentication boundary.

## Installed examples

```sh
python3 -m dagrivane.resources ./demo
```

This exports the reviewed examples to a new directory without overwriting an existing destination. Use the exported paths with the CLI.

## Development scope

[Architecture](docs/ARCHITECTURE.md) explains the current execution path.
[Product scope](docs/PRODUCT_SCOPE.md) describes the broader proposed subsystems.
[Weekly roadmap](docs/proposed-weekly-roadmap.csv) is a planning scenario.
[Tasks](TASKS.md) and [request register](REQUESTS.md) track actual status.

## History note

This repository was assembled starting in November 2026. Earlier Git timestamps,
where used, represent reconstructed development stages, not verified original
work in those years. The 2019–2026 roadmap is proposed scope, not a completed-work record.

## Incremental ingestion

Durable streams accept ordered event batches or pinned CSV revisions, retain source checkpoints, deduplicate deliveries and apply event-time upserts/deletes with tombstones. Quality-gated snapshots enter a leased outbox and publish idempotently into the artifact catalog, with predecessor lineage and replay audits. See [incremental ingestion guide](docs/INGESTION.md) for a complete example and recovery boundaries.
