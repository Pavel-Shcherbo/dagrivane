# Architecture

## Current implementation

A specification is validated and topologically ordered before any task runs. The executor records running state before launching each process, then commits its outcome. A run is tied to a specification and working-directory fingerprint. The workbench invokes only the bundled pipeline; it does not accept arbitrary command definitions over HTTP. Declared workflow inputs are checked against SHA-256 before a checkpoint can be reused. The control example pins both its CSV and task implementation. Undeclared files and external services are not captured by this identity; operators must declare relevant inputs and use a new run ID after changing them.

## Verification

Graph cycles, invalid commands, unknown dependencies, independent branches, retry counts, checkpoint reuse and interrupted-task outcomes.

## Local HTTP boundary

The workbench binds to loopback, validates the Host header, rejects foreign
Origin values and requires a custom header for state-changing requests. Static
files are served from an explicit allowlist. Request bodies are limited to 1 MiB.
These measures support a local development tool; there is no production identity,
authorization or organization model yet.

## Next architectural increment

Expose existing workflow versions, queue, leases and events in management views; qualify remote storage and deployment adapters before distributed execution. Timeout handling terminates the process group on POSIX, but does not provide an operating-system sandbox.

## Typed data adapter

`contracts` defines explicit scalar types and version-compatibility reports. `tables` loads bounded, hashed CSV into immutable records. `transforms` compiles schemas and applies relational operations; join cardinality and output budgets are explicit. `quality` separates data acceptance from execution success. `dataflow` compiles the complete plan before input I/O and publishes one create-only JSON bundle after all gates pass.

`data_adapter` launches built-in plans through the graph's subprocess boundary, pinning imports to this installation. The normal checkpoint, timeout, cancellation and dependency semantics apply. The workbench selects between fixed bundled command and typed-data pipelines; it does not accept arbitrary process commands over HTTP. See DATAFLOWS.md for the exact scope and limits.

## Artifact and release boundary

The content-addressed object store verifies actual bytes independently of SQLite metadata. Immutable policies gate typed rows again at publication and verification. Dataset versions, build provenance, alias generations and pinned release manifests are transactional metadata; environment pointers require the exact prerequisite release and a fresh content check. Exported packages include the bounded parent closure and policies, allowing offline verification without the originating catalog. These are local selection primitives; authentication, signatures, retention and deployment adapters remain open. See ARTIFACTS.md.

## Incremental ingestion and publication recovery

`ingest_model` validates immutable stream contracts and reduces source-ordered events with event-time staleness, lateness and tombstones. `ingest_store` commits checkpoints, deduplication, materialized state and outbox bundles in one SQLite transaction. `ingest_publish` bridges the separate catalog transaction using ordered leased claims, deterministic request keys and fenced acknowledgement. `ingest_audit` replays raw batches and checks cached state, bundles and acknowledged catalog artifacts. `ingest_source` converts pinned CSV revisions to explicit upsert batches; `ingest_cli` supplies local operator commands. Remote source completeness and automatic schema migration are not inferred. See INGESTION.md.
