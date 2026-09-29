# Overture caching container

## Status and scope

This document specifies the implementation. The native dependency, image build,
H3 filtering, worker cell/projection validation and bounded candidate query,
worker runtime/output validation, cache ownership/admission and keyed job
scheduling, the HTTP endpoint contract, GeoParquet writer compatibility, typed
command configuration, catalog validation, catalog observation, service wiring,
the CLI testing client, and the opt-in container harness are implemented.
Build a Go 1.27 HTTP service with ko. Query upstream Overture GeoParquet on S3
using DuckDB, return POIs for an H3 cell, and retain successful tiles in a
bounded disk LRU. The initial dataset is `theme=places/type=place`; other
themes, historical release serving, arbitrary SQL, and distributed cache
coordination are out of scope. Remaining implementation work and feasibility
gates are in [TODO.md](TODO.md).

The native probe in `cmd/native-probe` is the first executable artifact. It
links the pinned DuckDB and H3 libraries, loads the pinned `httpfs` and
`spatial` extensions from ko static assets, and refuses DuckDB's automatic
extension installation and loading. The extension fetch script verifies
architecture-specific SHA-256 checksums before producing the versioned
DuckDB extension layout. The ko runtime is the digest-pinned
`cgr.dev/chainguard/glibc-dynamic` image, which supplies glibc, libstdc++, CA
certificates, and a non-root user. The native Linux arm64 prototype was built
and executed with no network and a read-only filesystem; amd64 remains the
required CI image gate.

The `internal/h3filter` package registers `h3_cell_contains(latitude,
longitude, requested_cell)` as a DuckDB chunk scalar UDF. It accepts explicit
`DOUBLE`, `DOUBLE`, and `UBIGINT` inputs, preserves DuckDB's default NULL-in/
NULL-out behavior, and applies the pinned H3 `LatLngToCell` operation at the
requested cell's resolution. Registration and execution errors are returned to
DuckDB; invalid coordinates and cells are not silently discarded.

## Architecture

One container owns one writable cache directory and an exclusive process lock.
The Go supervisor handles HTTP, catalog discovery, cache accounting, and work
admission. A bounded scheduler launches one isolated worker process per admitted
build from the same binary and embeds DuckDB through
`github.com/duckdb/duckdb-go/v2`. Workers perform S3 reads and `COPY`; process
isolation provides enforceable output limits and cancellation without
interrupting unrelated queries. A dead worker affects only its build; the next
admission starts a fresh process.

Use `net/http`, `log/slog`, and `github.com/alecthomas/kong`. Configuration is
parsed directly into typed configuration structs with environment overrides.
Use named constants for repeated values and explicit error propagation. Proposed
packages are `catalog`, `tiles`, `cache`, `worker`, and `httpapi`, behind small
interfaces so catalog, clock, query execution, and filesystem failures are
controllable in tests.

## Catalog identity and freshness

Discover the advertised release from the official
[Overture STAC catalog](https://docs.overturemaps.org/blog/2026/02/11/stac/).
Read release metadata and resolve the places assets to the official
[S3 release location](https://docs.overturemaps.org/getting-data/cloud-sources/).
Do not guess the latest release from lexicographic bucket listings. Metadata
uses HTTPS; actual feature reads use anonymous S3 access in `us-west-2`.

Expose two identifiers:

- `release`: the upstream release name, treated as opaque.
- `catalog_version`: release plus a digest of the canonical places asset
  manifest and relevant schema metadata. Ignore unrelated catalog timestamps.

The manifest contains sorted asset URLs and published object checksums or
versions where available. Pin every job to that manifest; never resolve a
mutable `latest` path inside SQL. The design assumes published release assets
are immutable. Detecting silent in-place S3 mutation without upstream manifest
changes would require object-level validation and is outside this contract.

The live hierarchy is captured under `testdata/stac/`: the root `latest`
pointer resolves to release `2026-09-23.1`, then to `places/place`, whose
collection resolves sixteen AWS Parquet partition items. The current STAC
items publish partition URLs, sizes, row counts, row-group counts, and spatial
bounds but no object checksum field. HTTP ETags are retained as conditional
validators, not treated as content checksums because the observed multipart
ETags are not portable digest identities. The fixture's canonical input hashes
the trusted URL manifest and the selected schema metadata, with the expected
digest recorded in `testdata/stac/version.json`.

`internal/catalog` revalidates the root latest pointer, selected release,
places catalog, collection, and every partition item under one timeout. It
accepts only HTTPS links on the configured catalog host and AWS asset host,
requires the published `theme=places/type=place` prefix, and ignores alternate
providers. Response bodies are bounded; ETags and Last-Modified values are
retained for conditional revalidation, and a `304` is accepted only when a
cached body exists. Missing or incompatible collection metadata, selected
columns, partition links, bounds, or asset identities fail the refresh without
returning a prior snapshot.

Catalog and projection identities use compact canonical JSON with sorted object
keys, contract-ordered arrays, and a trailing LF before SHA-256 hashing. The
catalog hash includes the release, selected asset provider, sorted manifest,
and validated schema; the projection hash includes ordered selected columns,
their resolved metadata, H3 semantics, and the GeoParquet writer revision.

`catalog.Observer` serializes state publication and coalesces overlapping
refresh callers onto one check; caller cancellation only detaches that caller.
It performs an immediate startup check and continues bounded idle polling while
exposing readiness and the current generation. A newly observed release or
manifest fences the prior generation before replacement validation, canceling
its generation context. Any failed check also clears readiness and cancels the
active generation, so callers cannot fall back to a stale snapshot.

Freshness is fail closed and checked on every tile request, including cache
hits, negative hits, conditional requests, and range requests. Fetch the latest
pointer with cache revalidation, using conditional HTTP requests when upstream
validators exist. Revalidate the selected manifest as well. Requests may share
an overlapping in-flight check, but may not reuse a completed check merely
because it is recent. A background check improves discovery while idle; it does
not substitute for request checks.

A catalog manager serializes observations and publishes a generation atomically.
Once it observes a different advertised release or manifest, fence the old
generation immediately, even if validation of the new schema fails. Cancel old
builds, invalidate negative entries, and make old disk entries inaccessible.
Validate new asset paths, schema, selected columns, and geometry before marking
the new generation ready. Failure returns `503`, never fallback data. A change
back to an older release is also a new observation, not something to ignore.

Before sending tile headers, perform another freshness check after a build or
queue wait and compare its captured generation under the publication lock.
Discard obsolete results; return `409 catalog_changed` with the current version
when known. The same lock protects the final cache-hit generation comparison.
No old-generation response may begin after a change has been observed locally.
Already streaming responses retain their original version and may complete.
No service can promise detection of an upstream change after its last successful
check; this is a request-validation guarantee, not a global atomic snapshot.

Startup is unready until catalog validation succeeds. A failed freshness check
returns `503 catalog_unavailable`, including on a cache hit. Persisted metadata
helps recovery but never establishes currentness by itself. Clients must compare
versions, discard old tiles, and refresh on resuming a session; the server cannot
revoke data already downloaded by a client.

Deterministic tests cover unavailable and malformed STAC documents, unsupported
schemas, same-release manifest changes, release rollback, cold-start validators
without cached bodies, cached tile hits, builds, and the final pre-header
catalog check. A persisted tile keyed by an older catalog version cannot satisfy
a request keyed by a newer version.

## HTTP contract

| Endpoint | Behavior |
| --- | --- |
| `GET /v1/catalog` | Revalidate upstream; return release, catalog version, projection ID, selected fields, limits, supported H3 resolutions, and attribution links. |
| `GET /v1/tiles/places/{h3}?catalog_version=...` | Require the expected version; return one complete GeoParquet file or a structured error. |
| `GET /livez` | Supervisor is running. |
| `GET /readyz` | Catalog ready, workers healthy, cache writable, and resources available. |
| `GET /metrics` | Operational metrics, restricted by deployment networking. |

Reject a missing version with `400`; reject a different version with `409`
before looking up its tile. Validate a canonical lowercase hexadecimal H3 cell
index, cell mode, and resolution 0 through 15; resolution comes from the index.
Reject invalid indexes with `400`, without touching DuckDB.

Successful responses include `Content-Type: application/vnd.apache.parquet`,
`Content-Length`, a content-digest ETag, `Overture-Release`,
`Overture-Catalog-Version`, and `Overture-Projection-ID`. Send
`Cache-Control: no-cache, must-revalidate` so intermediaries must revalidate.
Do not enable stale-on-error caching. Evaluate `If-None-Match` only after the
catalog/version checks. Initially ignore Range and return the complete file
with `200`; range serving is not required. Return an empty, valid GeoParquet file
for a cell containing no POIs.

Errors are JSON with `code`, `message`, `retryable`, and known catalog context:

| Status | Code | Client action |
| --- | --- | --- |
| 400 | `invalid_request` | Correct parameters. |
| 409 | `catalog_changed` | Refresh catalog and discard old tiles. |
| 422 | `tile_too_large` | Request a finer H3 resolution. |
| 503 | `catalog_unavailable`, `capacity_unavailable`, `upstream_unavailable`, `server_shutting_down` | Retry with backoff; honor `Retry-After`. |
| 504 | `tile_timeout` | Retry later; this does not prove the tile is oversized. |
| 500 | `internal_error` | Report request ID; server logs the cause. |

Every response carries an `X-Request-ID`. A valid client-provided ID is echoed;
otherwise the server assigns a bounded process-local ID. HTTP failures are
logged with that ID and request metadata. Tile handlers use a bounded admission
semaphore (`TileConcurrency`, default 2); saturation returns
`503 capacity_unavailable` with `Retry-After: 1`. The metrics endpoint exposes
only fixed-cardinality counters for total, catalog, tile, 4xx, 5xx, and
capacity-rejection requests, plus catalog readiness. Complete tile responses
set a `30s` write deadline by default, configurable through `WriteTimeout`.
Shutdown rejects new requests, waits for accepted handlers to finish, and
returns the caller's deadline error if draining does not complete in time.

A size error includes the cell, resolution, `max_tile_bytes`, the failed limit
(`compressed_bytes` or `rows`), and `suggested_resolution = resolution + 1`.
This is advice, not a guarantee the next resolution fits. At resolution 15,
return `can_refine: false` with no suggested resolution; clients must omit the
tile or use a deployment with different limits/fields. Never return truncated
POI data as a successful tile. Guidance tells clients to cover their viewport
or original cell with every intersecting finer cell and deduplicate POI IDs;
logical children alone do not guarantee coverage of a footprint.

## Go CLI testing client

Provide `cmd/overture-client` as a separate Go 1.27 binary in the same module.
It exercises the public HTTP API without importing server internals or requiring
DuckDB, S3 credentials, or a running local database. Use `net/http`, kong for
typed flags/environment configuration, and slog with `-v` for debug diagnostics.
Keep the HTTP client package reusable by integration tests; share wire types
with the server, but test serialized responses against independent fixtures.

Initial commands:

| Command | Behavior |
| --- | --- |
| `overture-client catalog` | Fetch and print catalog JSON, including version, fields, limits, and attribution. |
| `overture-client tile <h3> --output tile.parquet` | Discover the current catalog, request one cell with its version, and save the complete response. |
| `overture-client tile <h3> --catalog-version <version> --output tile.parquet` | Send the supplied version unchanged, allowing stale-version rejection tests. |

Common flags are `--server-url` (default `http://localhost:8080`), `--timeout`
(default `60s`, covering the entire command), and `-v`. Use environment variables
prefixed `OVERTURE_CLIENT_`. Require an HTTP(S) server URL and a nonempty output
path for tile downloads. Reject invalid H3 input locally. An optional
`--if-none-match` sends an ETag for conditional-request testing. A `304` reports
the result without creating or changing an output file; it does not assert that
an existing local file is valid. Initially perform no automatic retries or
refinement: expose the server's behavior directly for reproducible tests.

For a `200`, check media type, catalog version, projection identity when obtained
from discovery, and required headers. Stream to a temporary file beside the
destination, bounded by the catalog's byte limit and a client
`--max-download-bytes` safety limit (default 64 MiB; use the smaller limit).
Verify Content-Length, the advertised content digest, and Parquet opening/closing
magic before atomic publication. Standardize the server ETag as a quoted
`sha256:<lowercase-hex>` digest for this verification. These checks establish
transport integrity; full GeoParquet/schema validation remains an independent
reader's responsibility in integration tests. Refuse overwriting an existing
destination unless `--force` is supplied, and preserve it on any failed download.
Delete partial files on error or cancellation and report cleanup failures.

Write catalog JSON and tile-result JSON to stdout; write diagnostics to stderr.
Tile results contain HTTP status, H3 cell, release, catalog version, projection
ID, ETag, downloaded bytes, elapsed time, and published path when applicable.
Never write binary Parquet to stdout. Bound error-response reads and display
structured API errors, including request ID and Retry-After when present;
malformed error bodies still report the HTTP status and a bounded diagnostic.

Exit codes are `0` for success/304, `1` for transport, integrity, filesystem, or
unexpected server failures, `2` for invalid CLI arguments or HTTP 400, `3` for
catalog change (409), `4` for oversized tiles (422), and `5` for temporary service
failure (503/504). On 409, show the current version and require an explicit new
invocation. On 422, print the suggested resolution and coverage guidance; at
resolution 15 explain that further refinement is unavailable. Do not silently
replace the requested cell with one child or claim logical children cover it.

Build both binaries with the normal Makefile build target and include CLI tests
in the normal test target. Container smoke tests use the CLI to discover a
catalog, download a tile, exercise ETag revalidation, and verify stale-version
and oversized-tile errors against controlled fixtures. The client is a local
testing tool; the server image need not contain it.

## H3 membership and projection

A POI belongs to a requested cell exactly when
`latLngToCell(latitude, longitude, requested_resolution) == requested_cell`.
The pinned implementation is exposed through the worker's DuckDB scalar UDF;
the callback uses DuckDB's chunk executor and keeps latitude/longitude order
explicit at the geometry-to-H3 boundary.

Use conservative cell bounds to push predicates into Overture `bbox` columns
before exact H3 evaluation. Account for curved edges, pentagons, poles, and the
antimeridian; split wrapped longitude intervals. Where conservative bounds
cannot be proved, scan a broader region rather than drop features. Bounds are
an optimization and never the membership rule. Ordinary single-face cells use
a spherical-cap envelope around the H3 center, expanded by the greatest
center-to-vertex distance, one greatest boundary-edge distance, and a floating-
point margin. Pentagon cells, cells spanning multiple icosahedron faces, and
pole-crossing caps use a global longitude range or a global envelope. The
generated DuckDB predicate also keeps NULL and malformed source bboxes as
candidates.

The table-driven integration test compares selected IDs from pruned and
unpruned DuckDB queries over 50,000 deterministic point bboxes plus centers and
boundary vertices for ordinary, pentagon, polar, and antimeridian cells. It
also covers wrapped and NULL bboxes. Run the performance comparison with:

```text
go test ./internal/h3filter -run '^$' -bench BenchmarkCellMembershipPruning -benchtime=1x
```

One arm64 run took about 23.5 ms unpruned versus 2.1 ms pruned and reduced the
candidate count from 50,000 to 5 for the benchmark cell. These timings are
machine-specific; the candidate-count metric and equality test are the gate,
not a latency promise.

[H3 hierarchy](https://h3geo.org/docs/) has approximate geometric containment.
Clients refining an oversized cell must cover their viewport or original cell
footprint at the finer resolution, including intersecting boundary cells; simply
requesting its logical children can miss POIs. Deduplicate overlapping requests
by Overture `id`. The API uses direct point membership at each resolution, not
parent membership derived from resolution 15.

`--fields` selects top-level source columns in a canonical order. Default to
`id,geometry,names,basic_category`; require `id` and `geometry` in every projection.
Reject missing required fields, duplicates, unknown columns, and SQL expressions
at startup and on schema change. Retain nested structures as typed columns;
nested-path projections are deferred. Geometry, bbox, and coordinates needed
for filtering may be read internally even when not selected for output.

Quote validated column identifiers; bind scalar values. Only the supervisor
provides validated S3 asset lists and generated output paths. Clients cannot
supply SQL, S3 URLs, column lists, or filesystem paths. Retain source nulls and
types. Invalid/null/non-point geometries are an upstream data error, not silently
skipped records. The projection ID hashes ordered columns, resolved types,
geometry encoding, H3 semantics version, and writer-format revision.

## DuckDB query and bounded output

Read only pinned places assets through DuckDB `httpfs`; use projection and bbox
predicate pushdown. Convert geometry to the pinned DuckDB geometry type and
apply exact H3 membership. Materialize at most `max_tile_rows + 1` matching rows
in bounded worker scratch. `internal/worker` now builds this statement only
from the validated catalog manifest and projection, binds all URLs and scalar
values, rejects null or non-point geometries through the query, and materializes
the bounded result in a temporary table. If the extra row exists, reject
immediately without sorting or serializing the whole cell; otherwise COPY that
table as one complete zstd GeoParquet file. This row limit is an explicit
additional admission limit, not an estimate of compressed bytes. LIMIT avoids
full result materialization but does not guarantee cheap S3 scans or early
completion.

For admitted rows, use DuckDB `COPY (SELECT <projection> FROM candidate)` to a
single staging file with `FORMAT PARQUET, COMPRESSION ZSTD`. Do not split output
into parts. Row order is unspecified; the ETag hashes the actual file. Use
`internal/geoparquet` to project geometry to WKB `BLOB` data (the production
query uses `ST_AsWKB`) and inject exactly one GeoParquet 1.1 `geo` metadata
value. The pinned DuckDB native `GEOMETRY` export currently writes GeoParquet
1.0 metadata, so native geometry columns must not be copied directly for this
output contract. The metadata declares the primary geometry column, Point
geometry type, WKB encoding, and WGS84 longitude/latitude semantics; omitting
`crs` selects the GeoParquet 1.1 default OGC:CRS84. Verify with Apache Arrow's
independent reader, including zero rows, zstd compression, raw WKB bytes, and
nested fields. Deterministic files are committed under
`testdata/geoparquet/`; plain Parquet containing WKB without metadata is
insufficient.

Set Linux `RLIMIT_FSIZE` in the isolated worker to `max_tile_bytes` before COPY.
The `internal/worker` output guard applies that limit, catches `EFBIG`-style
COPY errors, reports `ErrOutputTooLarge`, and removes partial output. The
supervisor must also remove the staging path when the worker terminates from
`SIGXFSZ`; the limit bounds regular-file output even when DuckDB buffers row
groups or grows the footer. `DisableCopySpill` sets both `temp_directory = ''`
and `max_temp_directory_size = '0B'` for the COPY phase, so the file-size limit
cannot be confused with a spill-file limit. Other write failures remain storage
errors, not size errors.

`internal/worker/output_test.go` proves this contract in subprocesses using the
pinned DuckDB build: a buffered 250,000-row Parquet COPY succeeds at its exact
measured size and fails one byte below it; a 2 MiB high-entropy individual row
fails under a 64 KiB limit; partial files are never accepted; and an ordered,
low-memory COPY fails when spilling is disabled. The parent accepts either a
returned file-size error or a worker exit signaled by `SIGXFSZ`, and always
cleans the staging path. The same package verifies that candidate materializing
queries use `LIMIT max_tile_rows + 1`, including zero and overflow limits.

After COPY, validate the footer, GeoParquet metadata, actual file size, and
expected schema before publication. `RuntimeSettings` applies DuckDB memory,
thread, scratch, and tile-deadline settings; `BuildTileWithSettings` validates
the independent footer/schema reader, size, and SHA-256 digest before returning
a result. Include footer bytes in the limit and allow exact equality. No tile
bytes are sent to HTTP clients until validation succeeds.
DuckDB's [COPY options](https://duckdb.org/docs/lts/sql/statements/copy) support
zstd, but `FILE_SIZE_BYTES` is a file-splitting target, not a hard single-file
size guard. Do not use it for this contract.

Exact zstd size cannot be known before encoding. Fast failure means an early
row-limit probe and interruption as soon as output would exceed the byte limit;
it cannot promise a constant-time decision or a bound on S3 bytes fetched.
Deadlines, DuckDB memory limits, and bounded concurrency also apply. If the
worker hard-limit prototype fails, resolve it before implementing serving; do
not replace it with an unbounded COPY followed only by stat.

Deterministic worker benchmarks cover dense-city and sparse-region query
planning without contacting S3. Any live upstream benchmark must report the
manifest asset bytes, rows examined, S3 transfer, query latency, COPY latency,
and whether rejection was caused by rows or compressed bytes. Neither the
deterministic benchmark nor a live result promises constant-time rejection or
constant S3 transfer.

## Disk LRU and concurrency

`internal/cache` owns the cache root with a nonblocking OS lock, derives final
and staging paths from a SHA-256 cache key, and accounts byte/entry reservations
under one mutex. Its keyed scheduler bounds the worker queue, coalesces callers
for one key, detaches canceled callers, cancels abandoned jobs, and releases
reservations after build cleanup. It publishes a file and sidecar atomically,
maintains a doubly linked LRU with reader pins and obsolete-generation priority,
batches recency writes, and reconciles corrupt/orphan state at startup. Quota
reservation and negative-cache policy remain separate. `ScratchPool` enforces
deployment-wide scratch reservations with caller cancellation. `NegativeCache`
accepts only constructed row/byte size proofs, bounds them with an LRU and TTL,
and supports catalog-version invalidation; transient failures have no insertion
path.

Cache key: `(catalog_version, projection_id, H3 cell, size_policy_id)`.
The size policy hashes byte/row limits, preventing reuse of outdated rejection
results. Compression and writer revisions are part of projection identity.
Files live under hash-derived paths, never raw request paths.

Maintain an in-memory map and doubly linked LRU with persistent sidecar metadata
containing key, byte size, content digest, and last-access timestamp. Metadata is
written atomically; access-time persistence can be batched, so recency after a
crash is approximate. On restart, remove unfinished staging files, validate
sidecars/files, rebuild accounting, and enforce the quota before readiness.
Log corrupt entries and remove them; unexpected filesystem errors fail visibly.

`cache_max_bytes` bounds the sum of complete tile file lengths, including old
generations and pinned files. Before a build, reserve `max_tile_bytes` in that
budget and evict least recently used unpinned entries until the reservation fits.
Replace the reservation with actual size on atomic publication. Evict obsolete
generations first. Open response files are pinned until transmission completes;
unlinked-but-open storage must remain accounted until the final reader closes.
If pinned files prevent admission, return `503 capacity_unavailable`.

Stage on the cache filesystem, sync completed data, rename atomically, and
publish metadata under the cache lock. Crash recovery removes orphan files or
metadata from interrupted publication. A second supervisor cannot share the
same directory; replicas use separate volumes and independently validate the
catalog.

Bound scratch/spill separately with `scratch_max_bytes`, per-worker allocations,
and a filesystem quota or size-limited volume. Provision disk for cache,
scratch, filesystem overhead, and metadata; logical file lengths are not physical
block usage. Bound metadata by `cache_max_entries`; reserve entry slots before
work as well. Staging consumes the byte reservation, not an untracked second
cache. Delete/cancellation failures retain accounting and are logged/retried.

Coalesce simultaneous requests for the same key into one build. Bound workers
and the waiting queue; return `503` when full. Caller cancellation detaches that
caller; cancel work when no callers remain, on generation invalidation, or on
job deadline. Cancel SQL then terminate an unresponsive isolated worker. Release
reservations only after cleanup. Bound slow-client write time so pinned files
cannot consume capacity indefinitely.

Deterministic cache tests cover exact byte/entry quota boundaries, pinned
capacity failures, parallel distinct-key admissions, last-waiter cancellation,
generation-key fencing, publication/storage failures, interrupted sidecar
publication, orphan cleanup, corruption, and restart accounting.

Cache `tile_too_large` decisions in a separate bounded in-memory LRU with a short
TTL, keyed identically. Do not cache transient upstream/storage failures as size
errors. Catalog freshness checks still precede negative hits.

## Configuration and deployment

The dependency build pins are recorded in `build/versions.env`: Go 1.27.1,
ko 0.19.1, prek 0.5.4, DuckDB 1.5.5 through duckdb-go v2.10505.0, and H3
Go v4.5.0. The pre-commit configuration pins golangci-lint v2.13.2 and uses
its configuration for formatting and linting.
The runtime image and DuckDB extension archives are pinned by digest/checksum;
the probe never installs extensions or downloads them at runtime.

`cmd/overturetunkki` now provides the supervisor and worker process modes. The
supervisor mode is the default and owns the catalog observer, HTTP server, disk
cache, bounded scheduler, and isolated workers. A worker process receives one
bounded JSON request over standard input, writes its complete tile into the
supervisor's staging volume, and returns typed failure metadata over standard
output. All flags have `OVERTURE_`-prefixed environment equivalents, and `-v`
sets the default `slog` level to debug.

These defaults are starting points to validate with representative POIs.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `:8080` | HTTP address. |
| `--catalog-url` | `https://stac.overturemaps.org/catalog.json` | Trusted catalog endpoint. |
| `--catalog-host` | `stac.overturemaps.org` | Exact trusted catalog host, including an optional test port. |
| `--asset-host` | `overturemaps-us-west-2.s3.us-west-2.amazonaws.com` | Exact trusted places asset host, including an optional test port. |
| `--catalog-poll-interval` | `1m` | Additional idle refresh. |
| `--catalog-timeout` | `10s` | Deadline for a complete freshness check. |
| `--fields` | `id,geometry,names,basic_category` | Output projection. |
| `--max-tile-bytes` | `8388608` | Maximum complete zstd Parquet size, 8 MiB. |
| `--max-tile-rows` | `100000` | Additional early rejection threshold. |
| `--cache-dir` | `/var/cache/overture` | Exclusive writable cache root. |
| `--cache-max-bytes` | `10737418240` | Complete files plus reservations, 10 GiB. |
| `--cache-max-entries` | `100000` | Bounds file/metadata count. |
| `--scratch-max-bytes` | `2147483648` | Total worker scratch allowance, 2 GiB. |
| `--worker-count` | `2` | Concurrent DuckDB jobs. |
| `--worker-memory-bytes` | `536870912` | DuckDB memory limit per worker, 512 MiB. |
| `--worker-threads` | `2` | DuckDB threads per worker. |
| `--queue-capacity` | `32` | Waiting builds. |
| `--tile-timeout` | `30s` | Queue plus query deadline. |
| `--negative-cache-entries` | `10000` | Maximum retained size rejections. |
| `--negative-cache-ttl` | `5m` | Rejection lifetime. |
| `--write-timeout` | `30s` | Maximum response transmission time. |
| `-v`, `--verbose` | `false` | Set default slog level to debug. |

Reject nonpositive limits, unsupported fields, an unwritable cache, and
`cache_max_bytes < max_tile_bytes`. Require `id` and `geometry`, reject duplicate
or unknown fields, and reject malformed listen or catalog URLs. Validate
concurrency against deployment memory and scratch allocations; DuckDB memory
limits do not bound total process RSS. Use container memory/CPU limits in
addition. Log failures with structured request ID, catalog version, H3 cell,
duration, and error metadata, without credentials. Metrics cover catalog
changes/check failures, cache hits/misses, negative hits, eviction,
bytes/reservations, workers/queue, latency, cancellation, and size rejection.
Avoid H3 cells or release IDs as unbounded metric labels.

The `Makefile` exposes `lint`, `test`, `build`, `build-linux`, `image`,
`service-image`, `container-test`, `smoke`, and `hooks` targets. `lint` runs
`prek run --all-files`, including the pinned standard hooks and full
golangci-lint configuration verification and linting; tests remain a separate
target. `build-linux` uses the pinned Go
container toolchain under Podman for Linux CGO artifacts. ko is a Go tool in
`build/tools/go.mod`, isolated from application dependencies; Podman runs it
in Linux and loads its image tarballs. No Docker daemon is required.
On macOS, build/test/vet select the CGO H3 client; the service remains Linux-only.
GitHub Actions tests both host architectures on Linux and macOS and gates
Linux images with smoke and lifecycle tests. Successful pushes to the default
branch build native amd64 and arm64 service images and publish their manifest
as `ghcr.io/fingon/overturetunkki/service:latest`. `image` and `smoke`
exercise the bundled native dependency probe. `service-image` packages the same bundled
extensions with the HTTP service. `container-test` is opt-in and runs a local
TLS STAC/asset fixture against the service image with a read-only root,
writable bounded cache volume, concurrent requests, conditional responses,
release rollover, catalog and asset outages, worker replacement, and restart.
The pre-commit hook is installed with `make hooks`.

Pin Go 1.27, ko, the DuckDB Go driver/core, H3, extensions, and runtime image.
The [DuckDB Go client](https://duckdb.org/docs/current/clients/go/overview) uses
`database/sql`. [ko defaults to CGO disabled](https://ko.build/advanced/limitations/),
so explicitly build with `CGO_ENABLED=1` in a Linux builder with a compatible
C/C++ toolchain. Use a digest-pinned glibc runtime base with the required C++
runtime and CA certificates; verify linkage in the resulting image. Start with
native linux/amd64 builds; add arm64 only after native build/smoke verification.
Do not assume macOS ko invocation can cross-compile this native dependency set.

Package pinned `httpfs` and any required spatial extension artifacts into the
image via ko data assets or the pinned base. Load from a known read-only path;
do not install extensions from the network at runtime. Verify H3 UDF registration
and GeoParquet output support against the pinned versions. Run as non-root with
a read-only root filesystem and writable cache/scratch mounts. Public Overture
S3 access requires no embedded AWS secrets. Restrict catalog asset URLs to the
configured trusted S3 bucket/prefix, including redirects and metadata links.

Graceful shutdown stops admission, drains HTTP within a deadline, cancels jobs,
cleans staging, persists recency, and releases the directory lock. Preserve
Overture/source attribution links in catalog responses and deployment docs.

## Verification and delivery

Provide Makefile targets for lint (`prek run --all-files`), test, build, image,
and container smoke tests. Enable prek hooks. Use table-driven Go tests with
`gotest.tools/v3/assert` and fixtures/goldens under `testdata/`. Run lint, tests,
and build for implementation changes; the opt-in `container-test` target gates
service-image changes without requiring live S3.

Required acceptance cases include release rollover during a hit/build/stream,
failed upstream checks, schema incompatibility without stale fallback, exact
byte-limit boundaries including footer, huge single rows, worker limit signals,
H3 boundaries/pentagons/poles/dateline, empty GeoParquet, selected nested columns,
negative invalidation, coalescing/cancellation, LRU pinning, disk exhaustion,
restart/crash recovery, and a ko image querying S3 without runtime extension
installation. Keep live upstream smoke tests separate from deterministic tests.
