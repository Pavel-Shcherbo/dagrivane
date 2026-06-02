# Saved workflow schedules

Open `/calendar.html` from the local workbench. The Schedules link shares the
Run queue's workspace and workflow registry. Initialize the reviewed order
example, or select a workspace and a workflow version already registered through
the trusted local CLI. Nothing is registered from arbitrary browser-supplied paths
or commands.

## Review and create

Choose an immutable workflow version, then enter an interval or numeric cron
schedule. Cron controls expose timezone, repeated-hour fold policy, day matching
and skipped missing local times. Start/end instants, missed-slot policy, lateness,
overlap and priority are explicit. Preview the definition before creating it.
The returned definition retains the timezone's TZif bytes. Creation checks both
the preview identity and the reviewed workflow fingerprint. Editing the form
invalidates the preview; a delayed response cannot approve newer input.

Definitions are immutable. Pause/resume uses the displayed generation, preserves
the saved cursor and records the operator's reason. An obsolete generation is
rejected. Create another schedule to change its definition.

## Queue and execute

Queue due runs uses the declared UTC decision clock and maximum slot outcomes.
It inserts jobs; it does not execute commands. Inspect slot outcomes and their
linked runs. Use Run queue to execute reviewed order runs. For other registered
workflows, use the trusted local worker against the same database:

```sh
python3 -m dagrivane.manage --db .local/dagrivane/operations/control.sqlite worker --identity operator
```

The default browser state root is `.local/dagrivane`; `DAGRIVANE_STATE_DIR`
overrides that root. Under either root the shared database is
`operations/control.sqlite`. The CLI's default database is different, so specify
`--db` explicitly. The retained scheduler's foreground watch command is described
in [SCHEDULES.md](SCHEDULES.md); supervised and distributed service qualification
remains open.

## Recovery and evidence

Before a mutating request, the tab saves its exact parameters and request key in
session storage. If a response is lost, Recover first looks up the receipt. If
none exists, it retries the original parameters with the same key. The operation
and receipt share one database transaction. Reusing a key with changed parameters
is rejected. A successful write followed by a failed refresh stays a known saved
operation. Initializing the reviewed example is independently idempotent; preview
and reads do not create operation receipts.

Each schedule view reads its definition, workflow, decision events and outcomes
from one SQLite snapshot. When history paging discovers a changed generation,
plan or audit state, the page refreshes instead of appending incompatible rows.
Workspace/workflow/schedule lists use ID cursors; these separate page requests do
not establish a global snapshot across concurrent changes.

Export retains a receipt for the chosen snapshot. The server returns the complete
JSON document as text inside its response, and the browser saves that text
unchanged. Re-serializing parsed numbers in JavaScript would change `1.0` to `1`
and invalidate the Python canonical content hash. Verify the downloaded file:

```sh
python3 -m dagrivane.schedules verify downloaded-schedule.json
```

Replay checks retained clock decisions, occurrence/run references and event
integrity. It explicitly does not authenticate historical source data or establish
that worker side effects occurred. Verify those against the actual worker output.

## Bounds and trust

The HTTP request limit is 1 MiB. Responses and complete schedule evidence are
bounded at 16 MiB. Complete audits allow at most 5,000 events and 10,000 slot
outcomes; exceeding these bounds leaves paged inspection available and reports an
unavailable complete audit. There is no partial passing export. The calendar desk
retains at most 10,000 response receipts and 256 MiB of response content in total.
Receipt retention/deletion controls remain open.

Saved workflow specification/directory bytes must match their cached fingerprint
before review, creation or due dispatch. Workspace scoping partitions local state;
it is not an operating-system authentication boundary. Existing loopback Host,
Origin and request-header guards remain in force.

## Verified local increment

Twenty-one calendar checks cover rollback, concurrent retries, coherent reads,
scoping, stale generations, cached-content corruption, strict HTTP JSON and audit
bounds. The full local suite has 277 Dagrivane tests and 784 portfolio tests.
A clean installed wheel exercises creation, overlap deferral, pause/resume,
receipt recovery, real worker execution and replay after database removal.

The actual browser flow queued both UTC occurrences of New York's repeated
01:30 minute, ran both reviewed pipelines and independently reconciled each
artifact with four source orders totaling USD 112.00. It checked network loss
before and after commit, reload recovery, stale updates, delayed previews/views
and history paging across a generation change. Independent download replay found
and corrected the numeric serialization defect above. Final screenshots show
generation 35; the corrected downloaded example also captures generation 35.

Evidence: [installed controls](calendar-workbench-verification.json),
[browser and independent output checks](calendar-browser-verification.json),
[downloaded example](calendar-browser-example-evidence.json),
[desktop](calendar-desktop.png), [mobile](calendar-mobile.png).
This increment does not satisfy the full product or 20,000 useful-SLOC requirement.
