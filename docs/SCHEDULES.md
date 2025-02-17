# Retained local schedules

Register a pinned workflow version, preview its clock, then dispatch due slots into the existing durable queue. Workers execute the same queued runs used by manual submissions. The schedule dispatcher does not execute commands itself.

```sh
python3 -m dagrivane.manage --db .local/control.sqlite workspace-create Research
python3 -m dagrivane.manage --db .local/control.sqlite register WORKSPACE Orders examples/pipeline.json
python3 -m dagrivane.schedules preview examples/daily-schedule.json --limit 5
python3 -m dagrivane.schedules --db .local/control.sqlite create WORKSPACE WORKFLOW \
  --version 1 --definition examples/daily-schedule.json --request-key orders-morning
python3 -m dagrivane.schedules --db .local/control.sqlite dispatch WORKSPACE \
  --request-key reviewed-november4 --now 2026-11-04T16:00:00Z
python3 -m dagrivane.manage --db .local/control.sqlite worker
```

Replace `WORKSPACE` and `WORKFLOW` with returned IDs. The example uses synthetic order inputs. After installation, export examples to a new directory with `python3 -m dagrivane.resources demo` and use `demo/` paths. Clock previews have no database side effects. Explicit dispatch clocks make recovery of a lost response unambiguous: reuse the same request key, clock and limit.

## Automatic local dispatch

```sh
python3 -m dagrivane.schedules --db .local/control.sqlite watch WORKSPACE \
  --identity research --every 5 --limit 100
```

This starts an explicit foreground loop, writing one JSON object per line. Stop it with Ctrl+C; `--max-ticks 1` runs one cycle. Keep a separate worker running or invoke `dagrivane.manage worker` as required. No daemon, login service or worker starts merely by registering a schedule.

Watch requests use identity, slot limit and whole clock seconds as their durable request key. A restart in the same second returns the retained receipt. The summary counts observed receipt outcomes, which may be replays, rather than claiming newly created runs. A backwards clock movement within one watch process stops the loop with `clock_regressed`. Batches and waits are bounded, with a 1–60-second period. This is a local foreground dispatcher; service supervision, retention maintenance and distributed clock/availability qualification remain open.

## Definition

Version 1 requires `name`, `trigger`, `start_at`, `end_at`, `misfire`, `overlap` and `priority`. Times accept whole Unix seconds or ISO timestamps with explicit timezone offsets. The supported range is 1970 through 2100. Start is inclusive and end is exclusive; null end means no declared end within that range. A schedule retains an exact workflow version and priority. Registering a newer workflow does not retarget existing schedules.

Interval triggers have `kind: "interval"` and `seconds` from 60 through 2,678,400 (31 days). The start is the anchor. These are elapsed-time intervals, so a 24-hour interval is distinct from a daily wall-clock time.

Cron triggers require five numeric fields in minute/hour/day-of-month/month/day-of-week order. They support comma lists, ascending ranges and steps on wildcards or ranges, such as `*/15` or `9-17/2`. Sunday is 0; day-of-week 7, names, aliases, wrapping ranges, seconds and a single-number step such as `5/10` are refused. `day_match: "and"` requires both day filters. `"or"` accepts either restricted day filter; when either field covers its full domain, the other still applies.

Each cron trigger explicitly declares `timezone`, `fold`, `gap` and `day_match`. `gap` currently must be `"skip"`: nonexistent local minutes do not fire. `fold` is `"first"`, `"second"` or `"both"` for repeated local minutes. On ordinary minutes these policies produce one occurrence. A timezone's TZif bytes and SHA-256 are embedded during compilation. Future previews and verification use those retained bytes rather than the current operating-system rules. The name labels the retained data; supplied bytes are not independently authenticated as an official zone distribution.

Forward and reverse expansion are bounded to about ten years and 200,000 matching local-minute candidates per search. An impossible unbounded expression fails explicitly when this horizon is exhausted. Finite schedules with no occurrence in their declared window can be exhausted immediately. A bounded day cache accelerates dense schedules without retaining an entire calendar in memory.

## Missed slots and overlap

`misfire` has `policy` and `max_lateness_seconds`. A null lateness permits any retained backlog for catch-up/coalescing. Otherwise the bound is 0–31 days; the boundary itself remains eligible.

- `catch_up` submits eligible slots individually across bounded dispatch calls.
- `coalesce` submits only the most recent eligible slot.
- `skip` also selects the latest eligible slot, but requires an explicit lateness bound. An old slot beyond that bound is omitted entirely.

Omitted ranges retain their endpoints and reason. Interval range counts are exact. Cron counts are explicitly null because endpoints are computed without expanding a potentially large backlog. The record does not invent an omitted-slot count.

Overlap is evaluated against this schedule's queued, running and cancellation-requested runs. `allow` can queue more work. `skip` records a skipped slot. `defer` retains the pending cursor until the blocker becomes terminal; lateness policy is reevaluated on the next dispatch. The first queued slot itself becomes an overlap blocker, so one catch-up batch with `skip` may submit its first slot and skip the rest. A failed, cancelled or orphaned run is terminal for this scheduling policy; deciding whether its effects are safe to retry remains the operator's responsibility.

## Atomicity, limits and operators

Queue insertion, occurrence records, decision events, due cursor, generation, round-robin head and response receipt share one SQLite write transaction. A process failure before commit rolls them all back. After commit, repeating the same dispatch key returns its saved response. Reusing a key with different clock/limit inputs is a conflict. Concurrent dispatchers serialize through SQLite and do not create duplicate slot records.

A dispatch visits at most 100 due schedules, emits at most 100 slot outcomes total and takes at most 20 outcomes from one schedule. A persistent round-robin cursor prevents a large backlog from repeatedly taking the first slice. `due_remaining` indicates unfinished work; it is not a failure or completion claim.

There are at most 1,000 schedules per workspace and 10,000 in one local store. List, event and occurrence pages have explicit cursors. Pause/resume requires the generation from a fresh inspection and a reason:

```sh
python3 -m dagrivane.schedules --db .local/control.sqlite inspect WORKSPACE SCHEDULE
python3 -m dagrivane.schedules --db .local/control.sqlite set-enabled WORKSPACE SCHEDULE \
  --no-enabled --generation 3 --reason "maintenance window"
```

Pause preserves the due cursor. Resume applies the declared missed-slot policy to the backlog. Definitions are immutable; register a new schedule and disable the old one to change its clock or pinned workflow. Creation keys deduplicate equal inputs; retain the compiled definition when retrying across a timezone-data update.

The retained scheduler uses additive `cp_timed_*` tables. Existing `manage schedule-create`, `schedule-set` and `tick` continue to manage the older UTC interval schedules independently. There is no automatic conversion or shared schedule ID between the two APIs. Existing control-plane readers remain usable.

## Evidence

```sh
python3 -m dagrivane.schedules --db .local/control.sqlite audit WORKSPACE SCHEDULE
python3 -m dagrivane.schedules --db .local/control.sqlite export WORKSPACE SCHEDULE \
  --output schedule-evidence.json
python3 -m dagrivane.schedules verify schedule-evidence.json
```

Export is create-only and reads one coherent database snapshot. It includes the retained definition, current state, hash-chained decisions, individual slot outcomes and referenced run records. Complete exports are bounded to 5,000 events, 10,000 occurrences and 16 MiB. Exceeding a bound is refused; it does not produce a green partial audit. The offline verifier checks recurrence membership, missed-slot policy, cursor/generation continuity, queued input references and content identities without the original database or system timezone files.

This proves local content and queue-reference consistency. It does not authenticate historical ownership or independently establish worker effects. `execution_verified` and `source_authenticated` are explicitly false. Runtime overlap observations are retained declarations, not reconstructed external execution traces. Exported run results can contain application output; review the complete raw content before sharing it.

The local control store's workspace boundaries partition records, not operating-system privileges. Workflows are trusted local commands. There is no remote scheduler endpoint, user authentication, hosted tenant isolation or production rollout in this increment. Receipts and histories persist; retention/deletion and long-running service capacity require further implementation.

## Verification retained with this version

`tests/test_schedules.py` covers 43 clock/store/watch/CLI cases: gaps, folds, half-hour shifts, date-line changes, leap days, frozen timezone replay, independent UTC enumeration, bounded backlog, overlap, pause/resume conflicts, concurrent dispatch/receipt retries, interrupted transactions, fairness, actual worker effects, tamper controls, legacy compatibility and watch stop/recovery behavior.

`schedule-verification.json` records clean-installed CLI use, a real scheduled worker run with independently read `4 orders / USD 112.00`, stale-generation rejection, foreground watch and portable export verification after deleting the original database. `schedule-benchmark.json` retains a 100-schedule/2,000-slot queue workload and complete schedule audits. Queue timings are not execution-throughput measurements or a production SLA. Runtime qualification currently covers the recorded local Python/macOS environment; broader platforms and the browser scheduler remain open.
