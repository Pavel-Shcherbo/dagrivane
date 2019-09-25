# Local control-plane operations

The control plane is for a trusted local operator. It does not authenticate users
or sandbox commands. Workspace identifiers scope records; a worker can claim any
eligible run in this local database.

## Register and submit

```sh
python3 -m dagrivane.resources ./demo
python3 -m dagrivane.manage workspace-create Research --concurrency 2
python3 -m dagrivane.manage register WORKSPACE_ID Orders demo/pipeline.json
python3 -m dagrivane.manage submit WORKSPACE_ID WORKFLOW_ID --version 1 --event-key manual-001
python3 -m dagrivane.manage worker --identity local-1
python3 -m dagrivane.manage run WORKSPACE_ID RUN_ID
python3 -m dagrivane.manage events WORKSPACE_ID RUN_ID
```

Replace uppercase identifiers with the IDs returned by preceding JSON responses.
The default control database is `.local/control.sqlite`; pass `--db PATH` before
the action to select another file. The exported example directory is writable.

An unchanged registration returns the existing version. A changed specification
or working directory creates a new immutable version. Submission references a
specific version. An event key is idempotent within its workspace; reusing it for
different submission parameters is rejected.

## Ownership and cancellation

Workers claim queued runs transactionally, respecting workspace concurrency.
Claims include an opaque ownership token and expiry. Heartbeats retain ownership;
events and terminal results are rejected if the token is stale or expired.

A queued cancellation removes the run from worker eligibility. A running
cancellation is observed by the worker and terminates its current process group
on POSIX. Already completed effects are retained. This is not a rollback or an
operating-system sandbox, and process-group cleanup is not a claim about arbitrary
escaped descendants or untested platforms.

Expired running claims become `orphaned` when the queue is inspected for work.
They are not automatically replayed. Inspect task state and external effects, then
use `retry WORKSPACE_ID RUN_ID --event-key REVIEWED_RETRY_KEY` to create a new run.
An operator's retry is an explicit decision and may repeat effects.

## Interval scheduling

```sh
python3 -m dagrivane.manage schedule-create WORKSPACE_ID WORKFLOW_ID --version 1 --interval 300 --first-due UNIX_TIMESTAMP
python3 -m dagrivane.manage tick
```

Intervals use Unix time. The scheduler pins the specified workflow version. A tick
coalesces missed intervals into one current submission per schedule, records the
missed count and advances the next due time. It does not backfill every missed
interval. Repeating a tick does not duplicate the same scheduled event.

The CLI does not install a background daemon. A caller must invoke ticks and
workers deliberately. Calendar/cron semantics, remote workers, authentication,
quotas and automatic reconciliation remain outside the current implementation.

## Database initialization

Cold initialization is serialized. Switching a new file into WAL mode uses a
bounded retry only for that idempotent setup operation. Workflow execution and
external side effects are not blindly retried after an uncertain outcome.

New database files use owner-only permissions. Existing database permissions are
preserved. Keep the database and task-state directory on a suitable local filesystem;
network-filesystem behavior and multi-host operation are not qualified.
