# Port-validation increment — 2026-11-05

The workbench now reports invalid `--port` values as usage errors (exit 2) rather than socket-binding tracebacks. Zero remains supported for automatic local port allocation. Three subprocess regressions cover out-of-range values, non-numeric input and an actual HTTP request to a server started with `--port 0`.

The complete current suite passed 281 tests on CPython 3.14.6 / macOS arm64. The older Python-version matrix below qualifies the previous source snapshot; it is not a claim that the new increment was rerun across every interpreter. No UI assets changed.

---

# Verified local delivery

This summary records the final local qualification on 2026-11-04. It covers
workflow/lease recovery, typed data and quality gates, retained HTTP checkpoints, schema forks, pinned releases and saved calendar dispatch. Synthetic inputs and explicit model assumptions remain marked in the
guides; no live provider, hosted service, market performance or deployment is claimed.

## Supported Python checks

| CPython minor | Tests | Result |
|---|---:|---|
| 3.10 | 278 | Passed |
| 3.11 | 278 | Passed |
| 3.12 | 278 | Passed |
| 3.13 | 278 | Passed |
| 3.14 | 278 | Passed |

These are actual unit/integration runs on macOS arm64. Every version's log and
runtime identity are retained in the central review evidence. The compact
[compatibility record](python-compatibility.json) contains their hashes. Windows,
other operating systems and future Python versions are not qualified by this matrix.

## Clean installed entry points

Wheels were installed outside the source checkouts under Python 3.10 and 3.14.
Checks confirmed actual installed import paths, matching source bytes, bundled
examples, refusal to overwrite an export destination, CLI results and HTTP controls.
Independent accounting/state/control values and portable replay are checked by
the retained subsystem verification. See
[Python 3.10 installation](installed-verification-python-3.10.json) and
[Python 3.14 installation](installed-verification-python-3.14.json).

## Browser and workload evidence

The subsystem guides retain actual desktop/mobile controls, delayed/lost-response
checks, downloaded replay evidence and bounded synthetic workloads. Their detailed
reports describe the version/increment originally exercised; old test totals in
those reports are historical, not the current project total of 278.
The final compatibility changes do not alter browser assets. No broad throughput,
production capacity or remote identity claim follows from these local workloads.

## Reproduce source tests

```sh
python3 -m unittest discover -s tests -v
```

The [architecture](ARCHITECTURE.md) and subsystem guides describe the local trust,
resource and model limits. Review histories have disclosed reconstructed dates;
intermediate commits are not individually qualified releases. No upload, push or
deployment was performed. Source-line volume is not an acceptance gate.
