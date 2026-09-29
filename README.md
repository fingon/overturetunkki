# Overture caching container

Go 1.27 service, built with ko, serving zstd-compressed GeoParquet POI tiles by
H3 cell from Overture's S3 catalog through DuckDB. The design includes catalog
version checks, configurable output fields and tile limits, finer-cell retry
guidance, bounded HTTP admission with request IDs and write deadlines, and a
bounded disk LRU cache. A Go CLI testing client will inspect
catalog versions, download tiles, and exercise API error handling.

**Status:** native DuckDB/H3 build, ko image, exact H3 UDF, conservative bbox
pruning, validated worker projections and bounded candidate queries, worker
runtime/output validation, GeoParquet 1.1 writer compatibility, typed
supervisor/worker configuration, deadline-bound STAC catalog validation and
observation with rollover/fail-closed coverage, cache ownership/admission/
persistence with quota/restart coverage, and bounded scratch/
negative-cache primitives, and the tested HTTP endpoint contract are
implemented; HTTP request IDs, bounded metrics, backpressure, write deadlines,
and graceful shutdown are also covered. Service wiring and client implementation
are in progress.

The native probe verifies the pinned CGO libraries and bundled DuckDB `httpfs`
and `spatial` extensions in a non-root, network-disabled image. It uses
architecture-specific checksums and disables runtime extension downloads.

Use `make lint`, `make test`, and `make build` for local checks. `make image`
and `make smoke` build and run the native probe image; `make build-linux` uses a
Linux CGO builder. The CI workflow gates amd64 image smoke and verifies arm64
with the same bundled-extension check.

See [DESIGN.md](DESIGN.md) for the architecture and API, and [TODO.md](TODO.md)
for the implementation backlog.
