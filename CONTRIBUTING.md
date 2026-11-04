# Contributing to Dagrivane

Start with the [quick start](README.md#run-the-workbench) and [guide index](docs/README.md). The application uses the Python standard library; packaging uses setuptools. Use a virtual environment and Python 3.10 or later.

## Local checks

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
python -m dagrivane --help
python -m unittest discover -s tests -v
```

Run commands from the repository root. The complete current suite contains 281 tests; [qualification](docs/VERIFICATION.md) records the platforms actually exercised. Passing on one local interpreter does not establish support for another operating system.

## Make a reviewable change

1. Describe the observed behavior and the desired result. Link an existing issue when one exists.
2. Keep changes focused on a runnable path. Add or update a regression test when behavior changes; documentation-only edits need link and command checks.
3. Retain a minimal synthetic fixture with independently checkable outcomes. Avoid committing local databases, generated reports or captured credentials.
4. Record the commands actually run and their results in the pull request. Call out untested paths and changed assumptions.
5. Update the relevant guide and task/request status when scope changes. Preserve unresolved work rather than marking a broad roadmap complete.

## Project-specific review

For workflow changes, include a blocked dependency or interrupted task. For ingestion changes, include a duplicate delivery, an expired lease and an interrupted publication. For schedules, retain the decision clock and timezone inputs; do not rely on the wall clock.

## Reporting a problem

Workflow or stream definition, run ID, input hashes and the last retained event or checkpoint. Include Python and operating-system versions, exact commands, expected behavior and actual output. Remove secrets and personal data before posting a public report. The application runs trusted commands locally; a fixture does not make an untrusted program safe to execute.

## Evidence and history

Use actual execution evidence for claims about correctness. The reconstructed Git timeline is disclosed in the README and does not establish when the implementation originally existed. Verification records apply to the versions and environments they name.
