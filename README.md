# Overture caching container

This repository builds a Go 1.27 service that serves bounded, zstd-compressed
GeoParquet POI tiles by H3 cell. The service discovers Overture's STAC catalog,
pins each request to a catalog version, queries the places assets through
DuckDB, selects source partitions by conservative spatial bounds, and keeps
successful tiles in a bounded disk cache. Tiles cover places with valid source
bboxes and exclude missing or malformed bboxes. The HTTP API reports request
IDs, freshness changes, finer-cell guidance, and retryable
capacity or upstream failures.

The repository includes `overture-client`, a small Go testing client that uses
only the public HTTP API. It prints catalog or tile-result JSON to stdout,
writes diagnostics to stderr, verifies streamed tile integrity, and never
writes Parquet to stdout.

## Build and run

Use the normal targets for local checks and binaries:

```sh
make lint
make test
make build
```

On macOS, install Go and Xcode Command Line Tools (`xcode-select --install`).
`make build` builds `bin/overture-client`; `make test` runs the client tests.
The client uses CGO for H3 validation and supports Apple Silicon and Intel Macs.
On Linux these targets build and test the service and native probe as well.
`make build-client` and `make test-client` select the client on either platform.

The service runs on Linux. A local Linux service needs a writable absolute cache
directory, for example:

```sh
mkdir -p "$PWD/.cache/overture"
OVERTURE_CACHE_DIR="$PWD/.cache/overture" ./bin/overturetunkki
```

Inspect the catalog and download one complete tile with the CLI:

```sh
./bin/overture-client catalog
./bin/overture-client tile 8928308280fffff --output tile.parquet
```

The tile command discovers the current catalog first. For reproducible stale
version or conditional-request tests, pass the version and ETag explicitly:

```sh
./bin/overture-client tile 8928308280fffff \
  --catalog-version '2026-09-23.1+sha256:...' \
  --if-none-match '"sha256:..."' \
  --output tile.parquet
```

A successful tile download is bounded by the smaller of the catalog's
`max_tile_bytes` and the client's `--max-download-bytes` limit (64 MiB by
default). It requires the server version, projection, release, media type,
Content-Length, and quoted `sha256:<lowercase-hex>` ETag; it validates both
Parquet magic markers before atomically publishing the file. Existing files
are preserved unless `--force` is supplied.

## Service configuration

Every service flag has the corresponding `OVERTURE_` environment variable.
The default mode is the supervisor; worker mode is an internal subprocess
protocol and is not a standalone HTTP service.

| Flag | Environment | Default |
| --- | --- | --- |
| `--mode` | `OVERTURE_MODE` | `supervisor` |
| `--listen` | `OVERTURE_LISTEN` | `:8080` |
| `--catalog-url` | `OVERTURE_CATALOG_URL` | `https://stac.overturemaps.org/catalog.json` |
| `--catalog-host` | `OVERTURE_CATALOG_HOST` | `stac.overturemaps.org` |
| `--asset-host` | `OVERTURE_ASSET_HOST` | `overturemaps-us-west-2.s3.us-west-2.amazonaws.com` |
| `--catalog-poll-interval` | `OVERTURE_CATALOG_POLL_INTERVAL` | `1m` |
| `--catalog-timeout` | `OVERTURE_CATALOG_TIMEOUT` | `10s` |
| `--fields` | `OVERTURE_FIELDS` | `id,geometry,names,basic_category` |
| `--max-tile-bytes` | `OVERTURE_MAX_TILE_BYTES` | `8388608` (8 MiB) |
| `--max-tile-rows` | `OVERTURE_MAX_TILE_ROWS` | `100000` |
| `--cache-dir` | `OVERTURE_CACHE_DIR` | `/var/cache/overture` |
| `--cache-max-bytes` | `OVERTURE_CACHE_MAX_BYTES` | `10737418240` (10 GiB) |
| `--cache-max-entries` | `OVERTURE_CACHE_MAX_ENTRIES` | `100000` |
| `--scratch-max-bytes` | `OVERTURE_SCRATCH_MAX_BYTES` | `2147483648` (2 GiB) |
| `--worker-count` | `OVERTURE_WORKER_COUNT` | `2` |
| `--worker-memory-bytes` | `OVERTURE_WORKER_MEMORY_BYTES` | `536870912` (512 MiB per worker) |
| `--worker-threads` | `OVERTURE_WORKER_THREADS` | `2` per worker |
| `--queue-capacity` | `OVERTURE_QUEUE_CAPACITY` | `32` waiting builds |
| `--tile-timeout` | `OVERTURE_TILE_TIMEOUT` | `30s` |
| `--negative-cache-entries` | `OVERTURE_NEGATIVE_CACHE_ENTRIES` | `10000` |
| `--negative-cache-ttl` | `OVERTURE_NEGATIVE_CACHE_TTL` | `5m` |
| `--write-timeout` | `OVERTURE_WRITE_TIMEOUT` | `30s` |
| `-v`, `--verbose` | `OVERTURE_VERBOSE` | `false` |

The service requires `id` and `geometry` in `--fields` and rejects unknown or
duplicate fields, invalid trusted hosts, nonpositive limits, unwritable cache
directories, and `cache-max-bytes` below `max-tile-bytes`.

Size resources together: `worker-count` must fit within
`scratch-max-bytes / max-tile-bytes`; `worker-memory-bytes` applies to each
DuckDB worker, while total process RSS also includes Go, DuckDB, extensions,
HTTP buffers, and scratch. `cache-max-bytes` includes complete files and
reservations, including old generations and pinned open readers. Set container
memory, CPU, cache, and scratch limits in addition to these application
limits. `max-tile-rows` is an early rejection bound; `max-tile-bytes` is the
hard complete compressed-output bound.

Catalog freshness is fail-closed. The poll interval is only an idle refresh;
each catalog or tile request performs its own bounded check using
`catalog-timeout`. A detected release or manifest change fences old work and
returns `409 catalog_changed` where appropriate; an unavailable check returns
`503` rather than serving stale data. Tile queue/query work is bounded by
`tile-timeout`, and complete HTTP responses have the `write-timeout` deadline.
Successful tile responses use `Cache-Control: no-cache, must-revalidate` and
304 responses are evaluated only after freshness validation.

The catalog response includes Overture's attribution URL
(`https://overturemaps.org`) along with the release, catalog version,
projection identity, selected fields, supported H3 resolutions, and limits.
Clients and deployments should preserve that attribution when presenting or
storing downloaded data.

## CLI flags and exit codes

Global CLI flags:

| Flag | Environment | Default |
| --- | --- | --- |
| `--server-url` | `OVERTURE_CLIENT_SERVER_URL` | `http://localhost:8080` |
| `--timeout` | `OVERTURE_CLIENT_TIMEOUT` | `60s` for the whole command |
| `-v`, `--verbose` | `OVERTURE_CLIENT_VERBOSE` | `false` |

`catalog` has no command-specific flags. `tile <h3>` accepts the following:

| Flag | Environment | Default |
| --- | --- | --- |
| `--catalog-version` | none | discover the current version |
| `--if-none-match` | none | no conditional validator |
| `--output` | none | required destination path |
| `--force` | none | `false`; refuse replacement |
| `--max-download-bytes` | `OVERTURE_CLIENT_MAX_DOWNLOAD_BYTES` | `67108864` (64 MiB) |

The client performs no automatic retries, catalog replacement, or H3
refinement. Exit codes are:

| Code | Meaning |
| --- | --- |
| `0` | Success or 304 Not Modified. |
| `1` | Transport, integrity, filesystem, timeout, or unexpected server failure. |
| `2` | Invalid CLI arguments or HTTP 400. |
| `3` | Catalog changed (HTTP 409); refresh and invoke again with the current version. |
| `4` | Tile too large (HTTP 422); request the suggested finer resolution. |
| `5` | Temporary service failure (HTTP 503/504); retry later. |

For a size rejection, cover the viewport or original cell with every finer
cell intersecting its footprint and deduplicate POIs by ID; logical children
alone do not guarantee coverage at boundaries. At resolution 15, refinement is
unavailable, so omit the tile or use a deployment with different limits.

## Images, smoke tests, and native dependencies

`make image` and `make smoke` build and run the native probe with bundled
DuckDB `httpfs` and `spatial` extensions. `make service-image` builds the
HTTP service image with an empty `/var/cache/overture` directory owned by the
service user (UID/GID 65532). With a writable root filesystem, it runs without
a cache mount; that cache lasts only as long as the container. Read-only
deployments still need a writable cache mount, as shown below.
`make container-test` first builds the CLI and service
image, then runs an opt-in Podman lifecycle test using deterministic local
STAC and GeoParquet fixtures; it checks the default cache without a mount,
then uses a read-only root, a writable bounded
cache volume, an independent GeoParquet reader, CLI download/304/stale-version
checks, rollover, outages, worker replacement, and restart. It does not
contact live S3. Live upstream tests, if added, must remain separately opt-in.

The native image build is pinned in `build/versions.env`: Go 1.27.1, ko
0.19.1, prek 0.5.4, DuckDB 1.5.5, duckdb-go v2.10505.0, H3 Go v4.5.0, the
glibc runtime digest, and architecture-specific extension checksums.
Images are built with the pinned `go tool ko` in a Linux Go/C++ builder run
by Podman, then loaded into Podman's local image store. No standalone ko or
Docker installation is needed. On macOS, initialize and start a Podman machine
(`podman machine init`, then `podman machine start`) before running:

```sh
make service-image
podman run --rm -p 8080:8080 --read-only --cap-drop=ALL \
  --security-opt=no-new-privileges --user=65532:65532 \
  --tmpfs /var/cache/overture:rw,uid=65532,gid=65532,size=1g \
  overturetunkki/service:dev
```

`make build-linux` builds Linux binaries through the same Podman toolchain.
Builds default to the host architecture; another `TARGET_ARCH` requires
Podman emulation support. GitHub Actions checks Linux amd64/arm64 and runs
Linux image smoke and lifecycle tests, caching Go dependencies and builds
for both host and Podman jobs, plus lint tool environments and results.
The service remains Linux-only; native macOS service builds are unsupported.

Successful pushes to `main` publish the service as a multi-platform image at
`ghcr.io/fingon/overturetunkki/service:latest` for Linux amd64 and arm64.

Tile build logs report selected source files and phase timings. Selected asset
bytes are full object sizes, not bytes downloaded. With `-v`, workers also
record the executed Parquet scan profile, including its selected columns and
HTTP request/transfer statistics. For timeout diagnosis, set
`OVERTURE_TILE_TIMEOUT` in the container and give `overture-client --timeout`
a longer deadline so it can receive the server's response. Increasing deadlines
alone does not fix slow remote reads.
The [recorded Helsinki run](testdata/performance/2026-09-23.1-helsinki/README.md)
records how malformed-bbox fallback scans dominated remote reads. The service
now excludes those rows and reads only the valid overlapping bbox subset.

Install repository hooks with `make hooks`. See [DESIGN.md](DESIGN.md) for the
architecture and HTTP contract, and [TODO.md](TODO.md) for the completed
implementation checklist.
