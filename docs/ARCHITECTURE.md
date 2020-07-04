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

Add immutable output artifacts and attempt-level events before introducing scheduling or distributed execution. Timeout handling terminates the process group on POSIX, but does not provide an operating-system sandbox.

## Typed data adapter

`contracts` defines explicit scalar types and version-compatibility reports. `tables` loads bounded, hashed CSV into immutable records. `transforms` compiles schemas and applies relational operations; join cardinality and output budgets are explicit. `quality` separates data acceptance from execution success. `dataflow` compiles the complete plan before input I/O and publishes one create-only JSON bundle after all gates pass.

`data_adapter` launches built-in plans through the graph's subprocess boundary, pinning imports to this installation. The normal checkpoint, timeout, cancellation and dependency semantics apply. The workbench selects between fixed bundled command and typed-data pipelines; it does not accept arbitrary process commands over HTTP. See DATAFLOWS.md for the exact scope and limits.
