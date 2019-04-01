# Product scope

Proposed scope. Completion is tracked in TASKS.md; the weekly roadmap is not evidence of historical work.

## 2019: proposed development stages

### Q1: Graph specification

Scope: task identifiers, dependencies and command arguments.

Acceptance: cycles and unknown dependencies are rejected before execution.

### Q2: Local execution

Scope: process exit codes, timeouts and working directories.

Acceptance: failed commands block descendants but preserve independent branches.

### Q3: Durable checkpoints

Scope: SQLite run state, task attempts and resume.

Acceptance: successful side effects are not repeated on an ordinary resume.

### Q4: Scheduling

Scope: cron-like intervals, explicit time zones and missed runs.

Acceptance: daylight-saving boundaries and duplicate triggers have defined outcomes.

## 2020: proposed development stages

### Q1: Command adapters

Scope: versioned Python and SQL task interfaces.

Acceptance: invalid parameters fail before a worker accepts a task.

### Q2: Source ingestion

Scope: HTTP pagination, cursors and incremental extraction.

Acceptance: repeated pages and interrupted loads do not duplicate records.

### Q3: Data quality gates

Scope: schema, uniqueness, null and aggregate checks.

Acceptance: quality failure is distinguished from execution failure.

### Q4: Run inspection

Scope: queryable execution events and structured log views.

Acceptance: event order and pagination remain stable across restarts.

## 2021: proposed development stages

### Q1: Worker leases

Scope: claiming tasks, heartbeats and expired ownership.

Acceptance: an expired worker cannot commit a newer owner's result.

### Q2: Container tasks

Scope: image digests, resource limits and isolated workspaces.

Acceptance: task cancellation and resource exhaustion leave inspectable state.

### Q3: Repository events

Scope: verified webhooks, event IDs and branch filters.

Acceptance: duplicate or invalid webhook deliveries do not create extra runs.

### Q4: Build artifacts

Scope: content hashes, manifests and immutable build outputs.

Acceptance: promotion references an exact artifact rather than a mutable tag.

## 2022: proposed development stages

### Q1: Environment promotion

Scope: development, test and release state transitions.

Acceptance: a failed prerequisite prevents promotion.

### Q2: Secrets and policies

Scope: scoped credentials, log filtering and task permissions.

Acceptance: credentials do not appear in persisted logs or exported bundles.

### Q3: Service adapters

Scope: CI integration, object storage and SQL warehouses.

Acceptance: provider-specific retry rules preserve idempotency boundaries.

### Q4: Recovery procedures

Scope: operator reconciliation and explicit retry decisions.

Acceptance: unknown external outcomes are not silently treated as success.

## 2023: proposed development stages

### Q1: Data contracts

Scope: schema versions, compatibility policies and producer checks.

Acceptance: breaking changes are identified before downstream execution.

### Q2: Lineage graph

Scope: code-to-dataset-to-service provenance.

Acceptance: every result resolves to the exact input and code versions.

### Q3: Impact analysis

Scope: dependency traversal and affected-service previews.

Acceptance: transitive consumers and cycles are handled consistently.

### Q4: Replayable runs

Scope: pinned inputs, task parameters and artifact manifests.

Acceptance: replay reports unavailable or changed inputs explicitly.

## 2024: proposed development stages

### Q1: Database evolution

Scope: expand-contract migrations and compatible readers.

Acceptance: old and new readers coexist during a staged migration.

### Q2: Backfill planning

Scope: time partitions, watermarks and incremental repair.

Acceptance: a backfill does not overwrite an unrelated live partition.

### Q3: Workflow editor

Scope: graph selection, validation and change previews.

Acceptance: keyboard operations match pointer-based graph editing.

### Q4: Release coordination

Scope: application rollout with dependent dataset versions.

Acceptance: code rollback does not pretend to undo irreversible data effects.

## 2025: proposed development stages

### Q1: Organization boundaries

Scope: tenant-scoped state, role grants and audit records.

Acceptance: cross-tenant IDs cannot reveal or mutate another workspace.

### Q2: Fair scheduling

Scope: quotas, concurrency pools and priority aging.

Acceptance: one busy workspace cannot indefinitely starve another.

### Q3: Operational objectives

Scope: queue latency, failure rates and run-duration budgets.

Acceptance: alerts distinguish dependency delays from worker failures.

### Q4: Metering

Scope: resource attribution and reproducible usage reports.

Acceptance: retries and cancelled tasks have unambiguous billing-free usage accounting.

## 2026: proposed development stages

### Q1: Disaster recovery

Scope: backup restore, reconciled leases and recovery checkpoints.

Acceptance: restored state is reconciled against external effects before retry.

### Q2: Extension SDK

Scope: adapter compatibility, capability declarations and fixtures.

Acceptance: an incompatible extension fails before a workflow starts.

### Q3: Capacity qualification

Scope: representative workloads and bottleneck analysis.

Acceptance: published capacity numbers come from retained benchmark evidence.

### Q4: Release qualification

Scope: upgrade rehearsal, operator handbook and signed manifests.

Acceptance: a clean installation can reproduce the documented end-to-end example.
