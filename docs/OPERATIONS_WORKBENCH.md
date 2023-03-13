# Durable workflow operations

The **Run queue** screen stores workflow versions, submissions, task results and events in SQLite. Reloading the page or restarting the web process preserves recorded history. Creating the workspace is explicit; merely reading the empty screen does not create an operations database.

Start `python3 -m dagrivane.web` and open `http://127.0.0.1:8411/#queue`. Choose **Create local workspace**, select a version, then **Queue run**. **Execute selected run** claims that exact run through the normal fenced worker. Queue submission alone does not start a background worker.

## Reviewable workflows

The browser offers three installation-owned order workflows:

- **Order checks** validates four synthetic orders, aggregates USD 112.00 and checks the release against pinned source identities.
- **Quality failure control** deliberately requires five source rows. Validation fails and dependent aggregation/release tasks remain blocked.
- **Cancellation control** inserts a four-second process before validation, allowing cancellation while a task is running.

Registering a different reviewed scenario creates a new immutable version. Existing submissions retain their original version. Registration checks the expected latest version to reject conflicting changes; re-registering the current identical scenario is idempotent. The browser never accepts arbitrary task specifications or commands. Versions altered outside the reviewed templates remain inspectable but cannot execute through this interface. A package/interpreter change can require registering a fresh reviewed version.

Submissions use an explicit request key, so retrying an uncertain network request does not create a duplicate. Priority affects selection by a general queue worker; executing a selected run requests that exact record. Two runs can own leases concurrently. A selected claim never expires leases or consumes work in another workspace. Workspace IDs organize records; they do not authenticate users.

## Inputs and execution

The `isolated_inputs` execution policy is supported by the ordinary worker as well as this screen. Before execution it reads and verifies pinned input bytes, then copies those bytes into a new per-run directory. Existing destinations are never reused or overwritten. Source templates remain unchanged, including during concurrent runs.

Copying accepts at most 64 files, 4 MiB per file and 16 MiB combined. Paths must be relative, remain inside the source directory, be distinct ignoring case and avoid file/directory overlap. Input symlinks are rejected. SHA-256 pins cover both `orders.csv` and the release-check script. The copy operation is an input-isolation mechanism; commands still run with the user's local process permissions.

The worker starts its lease heartbeat before preparing input files. Task output retained in the durable result is the UTF-8 tail, bounded to 8 KiB per task and 64 KiB combined. A partial leading UTF-8 character is omitted to preserve the byte limit; a truncation flag identifies shortened logs. Output is not a secret store.

## Cancellation and retry

Cancelling a queued run prevents execution. Cancelling a running run requests cooperative termination through the worker and records remaining tasks as cancelled. Completed effects are retained. Cancellation does not roll back completed tasks.

**Retry as new run** applies to failed, cancelled or orphaned records. It creates a linked submission pinned to the same approved version; the original record and its evidence remain intact. Review any external effects before retrying an orphaned run. Restarting an interrupted service does not prove that external effects were rolled back.

## Inspecting evidence

Select a run, then a task to inspect actual retained output. Events use an increasing sequence cursor; the queue uses a descending record cursor. Pages contain 25 runs or 50 events, with explicit continuation controls. The local operations workspace allows at most 100 versions and 1,000 runs. Retention, deletion and multi-user administration remain outside this increment.

The order summary is the **current file checked on refresh** against the registered quality gates. It is not an immutable output attestation. If the file is later changed, the check can fail while the historical execution result remains succeeded. Missing output is not replaced with a sample. Live task reconstruction reads up to 200 events and reports whether that history is complete; terminal task results are retained independently.

**Export inspection** downloads the selected state, currently loaded events, the event cursor and current output checks. Events not yet loaded are not claimed to be present. Stale responses from a previously selected run cannot replace the current inspection. Native buttons support keyboard selection; the queue and inspector stack on narrow screens.

## Verification

Fourteen operations/input-isolation tests cover concurrent initialization, independent parallel executions, version pinning, failures, queued/running cancellation, retries, scoped claims, metadata tampering, cursor continuation, request rejection before state changes, later output damage and multibyte log budgets. The complete Dagrivane suite contains 136 tests.

Clean installed HTTP checks exercise saved runs, duplicate requests, version changes, quality failures, cancellation and linked retries outside the checkout. Real browser checks cover successful output, failed dependencies, queued/running cancellation, retries, reload persistence, keyboard selection and a deliberately delayed inspection response. Desktop and 390-pixel mobile views were inspected. The downloaded inspection was independently matched against SQLite results, raw CSV arithmetic and input/output hashes.

Evidence: [installed controls](operations-verification.json), [browser and independent export checks](operations-browser-verification.json), [desktop](operations-desktop.png), [mobile](operations-mobile.png). These checks qualify this local increment; broad platform scope and the 20,000 useful-SLOC requirement remain open.
