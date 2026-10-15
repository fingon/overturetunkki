# Implementation backlog

The repository contains the design and the completed native dependency build,
H3 filtering, bounded worker query/output validation, GeoParquet writer
compatibility, typed command configuration, CI build gates, catalog validation,
catalog observation, cache ownership/admission and persistence primitives, and
the tested HTTP endpoint contract and its request/operations controls. All
items below are completed;
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
- [x] Enforce bounded candidate rows before COPY, validate completed output and
  serving byte limits afterward, retain complete oversized files within cache
  quotas, and verify no host resource limits are changed.
- [x] Confirm pinned DuckDB can write GeoParquet 1.1-compatible WKB with zstd,
  correct CRS/geo metadata, nested selected fields, and empty results. Validate
  with an independent reader and store fixture files under testdata/.

## 2. Establish project and container build

- [x] Add the Go module, in-process HTTP executable, typed kong config with
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
  cancellation of old jobs, and immediate fencing on invalid catalogs.
- [x] Share daily catalog validation across requests, allow an additional
  24-hour transient-outage grace, honor Retry-After with exponential retry
  backoff, and enforce expiry and generation identity before tile headers.
  Expose refresh outcomes and last-success time; test cooldown, expiry,
  recovery, and same-release manifest changes during renewal.
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
  last-waiter detachment, detached publication, queued cancellation, generation
  changes, disk-full errors,
  interrupted publication, orphan cleanup, corruption, and restart accounting.

- [x] Count distinct tiles for HTTP admission, recheck caches after scheduling,
  and retain running builds for later callers after the last caller detaches.
- [x] Derive smaller XYZ squares from a compatible cached ancestor, with pinned
  readers, upstream fallback on a cache miss, and existing output limits and
  validation. H3 composition uses at most four square sources.
- [x] Configure minimum H3 resolution through flag/environment (default 2),
  reject coarser tiles before freshness/cache/build work, advertise the allowed
  range, and test configuration and fast rejection.

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
  upstream outage, failed-build recovery, and restart. Keep live S3 tests opt-in.

## 7. Go CLI testing client

- [x] Add cmd/overture-client and a reusable HTTP client package with kong flags,
  OVERTURE_CLIENT_ environment overrides, configuration validation, command-wide
  cancellation/deadlines, and slog verbose diagnostics. Keep it independent of
  DuckDB and server implementation packages.
- [x] Implement catalog and tile commands, automatic catalog discovery, explicit
  version requests, and If-None-Match testing. Preserve raw failure behavior:
  no automatic retries, catalog replacement, or H3 refinement.
- [x] Implement bounded streaming downloads, required-header/version/projection
  checks, Content-Length and SHA-256 ETag validation, Parquet magic checks,
  temporary-file cleanup, atomic publication, and explicit overwrite control.
  Standardize the server's digest ETag format to match the client contract.
- [x] Provide stdout JSON results, stderr diagnostics, documented exit codes,
  bounded error-body parsing, and actionable 409/422/503/504 messages, including
  resolution-15 and boundary-cell refinement guidance.
- [x] Add table-driven httptest coverage for discovery/download, explicit stale
  versions, 304 with no file mutation, oversized tiles, malformed responses,
  missing/mismatched headers, truncated/over-limit bodies, checksum failures,
  timeout/cancellation, cleanup errors, and preservation of existing files.
  Store response fixtures/goldens in testdata/.
- [x] Build the CLI through Makefile build, run its tests through Makefile test,
  and exercise it against the container in smoke tests. Independently inspect
  downloaded GeoParquet; keep live upstream tests opt-in.

## 8. Delivery documentation

- [x] Replace README's design-only status with real build/run/client examples
  when implemented. Document every flag/env override, resource sizing, freshness
  limits, client refinement, attribution, and native ko build requirements.
- [x] Keep code, tests, DESIGN.md, README.md, and this backlog synchronized.
  Run Makefile lint/test/build and relevant image checks before completion.

- [x] Support native macOS service/library/clients, Linux Podman builds with ko as a Go tool,
  and GitHub Actions host and container checks for amd64 and arm64.

- [x] Validate live tile performance for Helsinki cell `8a1126d33027fff` after
  catalog-bbox file selection. On release `2026-09-23.1`, selection reduces 16
  files to partition `00011` (632751442 source bytes). Use the pinned Linux
  service, a 300-second server deadline and a longer client deadline; record
  Parquet scan plan/profile, HTTP request count and transfer bytes, query/COPY
  timings, validated output IDs, and a repeated cache hit. The recorded
  [live measurements](testdata/performance/2026-09-23.1-helsinki/README.md)
  confirm projection pushdown and successful completion within 300 seconds.
  Disjoint bbox branches preserve exact H3 membership over valid bboxes and
  one global row limit; null/malformed bboxes are now excluded. The historical
  fallback run took 185 seconds, 775 GETs,
  and 95.1 MiB; its 58 IDs and digest match the 244-second baseline, and its
  cache hit took 273 ms. Removing fallback scans reduced the fresh request to
  10.9 seconds, 24 GETs, and 6.8 MiB with identical output. Current tiles cover
  only the valid-bbox subset, with new projection identity.

- [x] Add OpenStreetMap XYZ square queries (zooms 0–22) and the CLI square
  command. Cache only squares, deriving smaller squares from cached ancestors.
  Compose H3 responses from at most four square fetches with exact membership
  and bounded temporary storage. Reject oversized sources immediately without
  recursive subdivision or alternate-cover retries. Omit polar POIs outside
  the Web Mercator latitude range and return valid empty polar responses.

## 9. Embedded Go API

- [x] Expose typed options, catalog discovery, direct H3/XYZ tile retrieval with
  pinned paths, typed errors, and deterministic service/tile cleanup.
- [x] Share the in-process engine with the HTTP executable; remove worker
  subprocess modes, JSON protocol, and process-wide file-size guards.
- [x] Keep complete oversized XYZ/H3 files within cache limits without serving
  them; allow internal source reuse, atomic admission of actual sizes, and
  cleanup plus negative caching when retention cannot fit.
- [x] Add native macOS amd64/arm64 extensions, full package checks, public API
  fixtures, in-process cancellation/recovery, and unchanged-host-limit tests.
- [x] Use DuckDB installation during Go/native service open for missing extensions;
  retain signature verification and offline reuse, and preserve explicit
  directories and checksum-pinned ko bundled assets.
