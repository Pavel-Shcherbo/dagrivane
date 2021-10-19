# Artifact catalog and local release manifests

The catalog connects typed datasets and application packages through explicit content identities. It stores local objects, immutable dataset policies, versioned artifact records, parent links and pinned release manifests. Environment promotion changes a local pointer; it does not execute a deployment.

## Executable control

```sh
python3 -m dagrivane.release_demo --output .local/release-control
python3 -m dagrivane.catalog_cli verify-export .local/release-control/release-export
python3 -m dagrivane.catalog_cli --store .local/release-control/catalog audit local-release
```

Use a new output directory. The control reads four synthetic orders, computes a USD 112.00 summary, builds a deterministic Python archive and executes it against the table. It records both artifacts, creates a release, verifies a staging prerequisite, selects the same release for a local production pointer and verifies an independent export.

Negative controls modify only their new isolated stores:

```sh
python3 -m dagrivane.release_demo --output .local/changed-data --scenario corrupt-data
python3 -m dagrivane.release_demo --output .local/missing-stage --scenario missing-staging
```

Both return 1 with a blocked result and leave the production pointer empty. Healthy verification returns 0. Invalid input/storage errors return 2. The workbench runs the same reviewed controls, retains each run separately and exports the JSON check result. It does not accept arbitrary application commands or filesystem paths over HTTP. Complete release exports use the CLI.

## Content identities and object storage

Object identity is SHA-256 of exact bytes. Objects live under `objects/<first-two-hex>/<remaining-hex>`. Publication stages and fsyncs a complete file before linking it into a new destination. Identical concurrent publication deduplicates only after verifying the winner. Existing corrupt objects are rejected and preserved for inspection. Reads check hashes; catalog verification also checks recorded byte counts.

Digest paths cannot escape the object root or use symlinked prefixes/objects. Objects are limited to 64 MiB. Object exports are create-only and preserve existing files. Directory metadata is not fsynced, so power-loss durability is not claimed.

Table identity is separate from file-byte identity. It covers the schema, row order and decimal scale; different JSON whitespace can change the file hash while preserving table identity. Producer source hashes are retained declarations, not automatically authenticated external-source records.

There is no object deletion or garbage-collection API. A failed database commit after object creation can leave an unreferenced object. Audit reports these without deleting them. A filesystem owner can rewrite bytes; verification detects discrepancies against recorded hashes, not a person consistently rewriting all hashes and metadata.

## Dataset policies and versions

Create a workspace first. All public metadata operations require its name. Workspaces are logical namespaces in one trusted local catalog, not multi-user authorization.

A policy definition has exactly `version`, `contract` and `gates`. Contract and policy version must match, start at one and increase consecutively. Identical re-registration is safe; changing existing content is rejected. Breaking schema evolution requires an explicit `allow_breaking=True` or CLI `--allow-breaking` decision. This records acceptance; it does not migrate old data.

Publishing a table rebuilds its typed rows, checks dimensions/data hash and producer success declarations, then independently evaluates the catalog's stored quality gates. A producer's `passed: true` cannot bypass duplicate-key, row-count, null or aggregate failures.

Dataset publication versions and policy versions are separate counters: the second dataset publication can still use policy one. Version allocation, request keys and metadata commit in the same transaction. Repeating identical normalized input/key returns the original receipt. Reusing a key for different content or a different operation is rejected.

```sh
python3 -m dagrivane.catalog_cli --store .local/artifacts workspace-create research
python3 -m dagrivane.catalog_cli --store .local/artifacts policy-register research orders policy.json
python3 -m dagrivane.catalog_cli --store .local/artifacts publish-table research orders table.json --policy-version 1 --run-id data-run-1 --request-key publish-1
python3 -m dagrivane.catalog_cli --store .local/artifacts artifacts research
```

`table.json` is a `dagrivane.table.v1` dataflow bundle. The local operator explicitly selects these source files. Read commands do not create an empty catalog accidentally.

## Builds, lineage and mutable aliases

A build record stores exact bytes, name, media type, run ID, source revision and optional parent IDs. Revision is a declared full 40-character Git-style hash or 64-character source hash. Generic registration does not fetch or prove a Git commit. The control uses SHA-256 of actual source bytes and records the executable archive separately.

Parents must already exist in the same workspace and cannot repeat. API-created provenance is acyclic. Traversal explicitly reports `complete: false` when node/depth limits truncate it. Release verification traverses all declared ancestors, detects manually corrupted cycles and checks ancestor content even for a release binding only a build.

Artifact/event cursors increase monotonically and resume after reopening. An alias is the explicit mutable layer: moving it requires the generation the caller observed. A competing change causes a conflict. A new alias uses expected generation zero.

## Pinned releases and local environments

Release definitions contain `bindings`: named references with exactly one `artifact` ID or `alias`. Optional `requirements` constrain kind, dataset name and exact policy version. Creation resolves aliases once, snapshots exact artifact metadata, verifies the full declared parent closure and stores an immutable manifest hash. Later alias movement does not change that release. Retrying the original request returns the original resolved result.

Environment definitions can require an already-created predecessor and cannot change that prerequisite. Promotion requires an observed generation, exact release ID and idempotency key. The predecessor must already point to that same release. Content and quality policies are checked again before committing the new pointer.

Receipts describe historical accepted operations. Repeating a key does not perform a new live verification. Use `verify-artifact`, `verify-release`, environment lookup or `audit` for current state. Local pointer movement is not an external rollout, automatic rollback or reversal of data effects.

## Independent release export

Exports contain `release.json` and content-named files under `objects/`. They include complete declared parent closure, artifact snapshots, table metadata and exact policy definitions. Duplicate bytes are stored once. An existing destination directory is rejected, even if empty.

Files are staged and verified first; object files arrive before the descriptor completion marker. Consumers should parse a valid descriptor and verify the set. Ordinary handled failures remove only the newly reserved export destination, preserving pre-existing directories.

`verify-export` needs no catalog database. It recomputes byte hashes/counts, typed-table identities and quality gates, checks requirements/binding snapshots, requires every parent and rejects cycles. Changed bytes, missing parents, boolean sizes, altered policies and symlinked objects fail verification. It does not establish publisher signatures, external-source truth or application runtime compatibility.

## Audit, concurrency and limits

Audit checks bounded workspace metadata, registered content, stored policy hashes and declared release provenance. Truncated audits return `complete: false` and cannot report full success. Shared object inventory reports unreferenced objects and invalid entries without deletion. Concurrent writers can create transient filesystem entries outside a reader's metadata snapshot; operational audit interpretation should account for that boundary.

SQLite transactions serialize dataset versions, idempotency records, alias generations and environment pointers. Object creation precedes metadata commit; filesystem and database are not a distributed transaction. Current limits include 100 parents per artifact, 32 bindings per release, 1,000 nodes in release verification, 256 artifacts/256 MiB per export, 64 MiB per object and the table/JSON bounds described in DATAFLOWS.md.

Authentication, remote storage, signatures, automatic retention, deployment adapters, migrations and capacity qualification remain open work.
