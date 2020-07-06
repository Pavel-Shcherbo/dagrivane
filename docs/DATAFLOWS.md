# Typed local dataflows

Dagrivane can run CSV data preparation and application release checks through the same checkpointed workflow executor. The current adapter is a bounded, in-memory local implementation. It does not connect to a warehouse or stream unbounded datasets.

## Run the examples

```sh
python3 -m dagrivane.dataflow examples/dataflow.json
python3 -m dagrivane examples/typed-pipeline.json --state .local/typed-runs.sqlite --run-id typed-demo
```

The standalone example creates `examples/.local/typed-summary.json`. To run both examples in the same checkout, remove or archive that generated artifact before starting the graph with a new run ID, or export the bundled examples to a fresh directory first:

```sh
python3 -m dagrivane.resources ./typed-demo
python3 -m dagrivane typed-demo/typed-pipeline.json --state .local/typed-runs.sqlite --run-id typed-demo
```

The graph validates four synthetic order rows, aggregates USD 112.00, then checks the source identities and release total. A task can contain exactly one `command` or `dataflow` definition. Dataflow plans are compiled before any task starts, and execute in a separate process with the normal timeout/cancellation boundary. A failed quality gate returns exit code 3 and blocks dependent tasks. Invalid configuration or I/O returns exit code 2.

In the browser, select **Typed data pipeline**. The failure control deliberately changes the expected source row count; no summary is published in that case. Each browser run has a separate input directory and run ID.

## Plan format

```json
{
  "version": 1,
  "sources": [{
    "id": "orders",
    "path": "orders.csv",
    "contract": {
      "name": "orders",
      "version": 1,
      "columns": [
        {"name": "id", "type": "string"},
        {"name": "amount", "type": "decimal", "min": "0"}
      ]
    }
  }],
  "steps": [],
  "output": "orders",
  "gates": [{"id": "ids", "kind": "unique", "columns": ["id"]}],
  "destination": "artifacts/orders.json"
}
```

Sources are loaded first; each step references an earlier source or step through `input`. IDs are unique ASCII identifiers. Optional source `sha256` pins the original CSV bytes. Paths stay within the supplied root, including resolved symlinks. The CLI defaults that root to the specification's directory. The optional destination is one create-only JSON table bundle; omit it for validation without publication.

## Contracts

Supported types are `string`, signed 64-bit `integer`, `decimal`, `boolean`, `date` and `timestamp`. Fields are non-nullable unless `nullable: true` is explicit. CSV headers must match contract order exactly; rows must have exactly the declared fields. CSV booleans are `true` or `false`. Integers reject whitespace, leading plus signs and leading zeros. Dates use canonical `YYYY-MM-DD`; timestamps require an explicit UTC offset and are normalized to UTC.

CSV null uses `\N`, configurable per source through `null_token`. Empty string remains a string; an empty numeric cell is invalid. Optional `min`/`max` constrain numeric/temporal values, and `enum` limits permitted values.

Decimals reject binary float inputs, NaN and infinity. They are bounded to 38 coefficient digits, 18 fractional places and a bounded exponent. JSON outputs encode decimals as strings, preserving their scale. Arithmetic uses an 80-digit intermediate context, then validates results against the output contract. Overflow or an inadmissible result fails the operation. This is exact decimal data processing, not a monetary rounding policy; choose explicit `round` or divide `places` where needed.

`compatibility(previous, current)` reports type changes, removed/added fields, tightened nullability/ranges/enums and a non-increasing version. Its policy concerns exact named-field rows, without implicit migration. Even a nullable added column requires an explicit migration. It does not approve compatibility for positional CSV consumers or external systems.

## Operations

| Operation | Configuration and behavior |
|---|---|
| `select` | `columns` and optional `rename` map; name collisions fail compilation |
| `filter` | AND-list of `conditions`: `column`, `op`, optional typed `value` |
| `calculate` | New `column` contract and an `expression` with explicit operands |
| `aggregate` | `group_by` columns and named `metrics`; preserves first-seen group order |
| `join` | Explicit `right`, `left_on`, `right_on`, `right_columns`, `how`, `cardinality` |
| `sort` | Stable `keys` with `column`, boolean `descending` and `nulls_first` |
| `distinct` | Retains the first row for each key in `columns` |
| `limit` | Nonnegative bounded `count` and optional `offset` |

Filter operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`, `starts_with`, `contains`, `is_null`, `not_null`. Except explicit null predicates, a null input does not satisfy a condition. Membership values are typed and bounded. No expressions, SQL fragments or user Python are evaluated.

Calculated expressions support `add`, `subtract`, `multiply`, `divide`, `round`, `coalesce`, `concat`, `lower`, `upper`, `trim`. Each argument is either `{"column":"name"}` or `{"literal":"value"}`. Numeric/text signatures are checked against the declared output type. Null propagates except for `coalesce`. Division requires a decimal result and uses explicit `places` (default 2); `round` also defaults to 2. Both use half-even rounding. Division by zero fails.

Aggregates support `count`, `sum`, `avg`, `min`, `max`. `count` without a column counts rows; with a column it counts non-null values. Other aggregates skip nulls and return null for an empty group. A global aggregate over no input rows produces one row with count zero. Average is rounded half-even to 18 fractional places. Overflow fails output validation.

Joins support `inner` and `left`. Null keys never match. The default cardinality is `many_to_one`; right duplicates fail before producing output. `one_to_one` also rejects duplicate non-null left keys. `many_to_many` must be explicit and is still constrained by the output budget. Right output columns require explicit names, and collisions fail compilation.

## Quality, identities and publication

Rules support `row_count`, `not_null`, `unique` and numeric `sum`. A unique key includes nulls unless `ignore_nulls: true` is chosen. Reports contain failure counts and at most 20 offending record numbers, without copying row values into diagnostics. All configured gates must pass before publication.

Every node reports its schema, row count, original source hashes and a schema-plus-row SHA-256. Data identity preserves row order and decimal scale; original file identity is separate. The plan identity includes its full configuration, including destination. Node lineage follows all joined sources.

Bundles contain the final schema/report and JSON rows. Publication writes and fsyncs a temporary file, then atomically links it into a new destination without replacing an existing file. Concurrent publishers have one winner. This guarantees complete local file visibility; directory metadata is not fsynced, so power-loss durability is not claimed. There is no multi-file transaction or automatic overwrite/reconciliation.

## Resource and execution limits

A plan is at most 1 MiB, with 1–16 sources and at most 128 steps. Contracts have at most 256 columns. Each CSV is at most 16 MiB; source file sizes are preflighted against a 64 MiB total. Each table has at most 100,000 rows, 2 million cells and 16 MiB of encoded cell values. Up to 4 million retained cells are allowed during plan execution. A bundle is at most 64 MiB. These bounds constrain local workloads; this is not a formal process-memory guarantee. The graph's process timeout remains the outer execution limit.

Original inputs can change between file-stat preflight and reading; per-source byte limits and optional hashes are checked while loading. Run trusted commands and review sources before release. External network isolation and distributed execution are not implemented by this adapter.
