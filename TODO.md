# Implementation backlog

The repository contains the design and the completed native dependency build,
H3 filtering, bounded worker query/output validation, GeoParquet writer
compatibility, typed command configuration, CI build gates, catalog validation,
catalog observation, cache ownership/admission and persistence primitives, and
the tested HTTP endpoint contract and its request/operations controls. Remaining
items below are pending;
[DESIGN.md](DESIGN.md) defines the intended behavior.

## 1. Resolve implementation gates

- [x] Pin Go 1.27, ko, DuckDB Go/core, and H3 versions. Prototype a Linux native
  CGO build and ko image with required shared libraries and bundled extensions;
  verify non-root startup and no extension downloads at runtime.
- [x] Inspect the live STAC hierarchy and record fixtures for latest discovery,
  places manifest resolution, conditional validators, asset identity, and schema.
  Confirm immutable release assumptions and canonical version hashing.
- [x] Prove the DuckDB H3 scalar UDF integration and performance. Implement and
  compare conservative bbox pruning against unpruned exact membership at cell
  boundaries, pentagons, poles, and the antimeridian.
- [x] Prove isolated-worker RLIMIT_FSIZE behavior for DuckDB COPY, including
  buffered output, footer growth, huge individual rows, exact-limit success,
  SIGXFSZ handling, and cleanup. Verify bounded candidate materialization and
  disabling COPY-phase spill. No serving implementation ships without a hard
  output-size guard; resolve native integration failures explicitly.
- [x] Confirm pinned DuckDB can write GeoParquet 1.1-compatible WKB with zstd,
  correct CRS/geo metadata, nested selected fields, and empty results. Validate
  with an independent reader and store fixture files under testdata/.

## 2. Establish project and container build

- [x] Add the Go module, supervisor/worker command modes, typed kong config with
  documented defaults and environment overrides, validation, and slog `-v`.
- [x] Add Makefile lint/test/build/image/smoke targets and a pinned prek
  configuration; enable hooks. Use gotest.tools/v3 and table-driven tests.
- [x] Add ko configuration, native Linux build tooling, digest-pinned runtime,
  packaged extensions, and CI. Gate linux/amd64 on image smoke tests; add native
  arm64 only when its linkage and extension artifacts are verified.

## 3. Catalog manager

- [x] Implement deadline-bound latest/manifest revalidation, trusted URL checks,
  conditional requests, canonical version/projection hashes, and schema checks.
- [x] Implement serialized observations, overlapping-check coalescing, idle
  polling, startup readiness, generation fencing before replacement validation,
  cancellation of old jobs, and fail-closed behavior on check failures.
- [x] Test rollover on cache hits, during build and before headers; unavailable
  and malformed catalogs; unsupported schemas; same-release manifest changes;
  rollback observations; and stale persisted startup state.

## 4. Tile worker

- [x] Validate canonical H3 cells and selected source columns. Require id and
  geometry; quote identifiers and bind values; prohibit arbitrary client SQL.
- [x] Query pinned S3 assets using bbox/projection pushdown and exact H3
  membership. Fail visibly on invalid geometry. Materialize only the configured
  row allowance plus one, then reject or COPY a complete zstd GeoParquet file.
- [x] Apply worker memory/thread/deadline settings, scratch allocations, and
  output limits. Distinguish size errors, disk failures, OOM, timeout, and S3
  errors. Validate final footer/schema/size and calculate content digest.
- [x] Test empty cells, field typing/nulls, nested columns, H3 refinement edge
  cases, oversized single rows, row caps, byte caps, cancellation, corrupt input,
  and source failures. Benchmark dense cities and sparse regions; document S3
  transfer and latency rather than promising constant-time size rejection.

## 5. Disk cache and scheduling

- [x] Implement exclusive ownership, hashed paths, byte/entry reservations,
  bounded worker queue, same-key coalescing, caller detachment, and job cleanup.
- [x] Implement LRU eviction, open-reader pins, obsolete-generation priority,
  atomic file/sidecar publication, batched recency persistence, and startup
  reconciliation. Keep deleted/open files and failed cleanup accounted.
- [x] Implement bounded scratch with deployment quotas and separate bounded
  TTL negative-cache entries for proven size rejections only.
- [x] Test quota boundaries, all-pinned capacity failures, parallel admissions,
  cancellation of the last waiter, generation changes, disk-full errors,
  interrupted publication, orphan cleanup, corruption, and restart accounting.

## 6. HTTP and operations

- [x] Implement catalog/tile/health/metrics endpoints and the documented JSON
  errors, version headers, ETags, freshness checks, and cache-control policy.
  Require catalog_version; ignore ranges initially; revalidate before 304.
- [x] Implement finer-resolution guidance and resolution-15 terminal errors.
  Document viewport coverage rather than logical-child-only refinement, client
  deduplication, version replacement, and refresh after session resume.
- [x] Add request IDs, structured error logs, bounded-cardinality metrics,
  backpressure/Retry-After, slow-reader deadlines, and graceful shutdown.
- [x] Exercise the complete container with writable volume limits and read-only
  root, an independent GeoParquet reader, concurrent clients, release rollover,
  upstream outage, worker death, and restart. Keep live S3 tests opt-in.

## 7. Go CLI testing client

- [ ] Add cmd/overture-client and a reusable HTTP client package with kong flags,
  OVERTURE_CLIENT_ environment overrides, configuration validation, command-wide
  cancellation/deadlines, and slog verbose diagnostics. Keep it independent of
  DuckDB and server implementation packages.
- [ ] Implement catalog and tile commands, automatic catalog discovery, explicit
  version requests, and If-None-Match testing. Preserve raw failure behavior:
  no automatic retries, catalog replacement, or H3 refinement.
- [ ] Implement bounded streaming downloads, required-header/version/projection
  checks, Content-Length and SHA-256 ETag validation, Parquet magic checks,
  temporary-file cleanup, atomic publication, and explicit overwrite control.
  Standardize the server's digest ETag format to match the client contract.
- [ ] Provide stdout JSON results, stderr diagnostics, documented exit codes,
  bounded error-body parsing, and actionable 409/422/503/504 messages, including
  resolution-15 and boundary-cell refinement guidance.
- [ ] Add table-driven httptest coverage for discovery/download, explicit stale
  versions, 304 with no file mutation, oversized tiles, malformed responses,
  missing/mismatched headers, truncated/over-limit bodies, checksum failures,
  timeout/cancellation, cleanup errors, and preservation of existing files.
  Store response fixtures/goldens in testdata/.
- [ ] Build the CLI through Makefile build, run its tests through Makefile test,
  and exercise it against the container in smoke tests. Independently inspect
  downloaded GeoParquet; keep live upstream tests opt-in.

## 8. Delivery documentation

- [ ] Replace README's design-only status with real build/run/client examples
  when implemented. Document every flag/env override, resource sizing, freshness
  limits, client refinement, attribution, and native ko build requirements.
- [ ] Keep code, tests, DESIGN.md, README.md, and this backlog synchronized.
  Run Makefile lint/test/build and relevant image checks before completion.
