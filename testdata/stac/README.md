# Live STAC fixtures

These fixtures were captured from the public Overture STAC catalog on
2026-09-29. The root catalog advertised `2026-09-23.1` as `latest`; its
`places` catalog resolves to the `place` collection and sixteen AWS S3
GeoParquet assets. The raw JSON files preserve that hierarchy and the
`validators.json` file records response validators, representative asset HEAD
responses, and a successful `If-None-Match` request returning `304`.

The selected source is `theme=places/type=place`. Its collection advertises
GeoParquet 1.1.0, `geometry` as the primary geometry, 81,455,423 rows, and
sixteen partitions. The independent DuckDB schema fixture records WKB
`GEOMETRY('OGC:CRS84')`, Point-only GeoParquet metadata, the physical nested
columns, and the `theme`/`type` Hive partition columns.

`manifest.json` is the canonical version input. Asset entries are sorted by
the trusted AWS HTTPS URL, and schema column order is preserved from the
collection. Object ETags are recorded as HTTP validators but are not included
in the version input because multipart ETags are not content checksums and
silent in-place object mutation is outside the release contract. The expected
SHA-256 and example `catalog_version` are in `version.json`; canonical JSON
uses compact UTF-8 with lexicographically sorted object keys, explicitly
ordered arrays, and one trailing LF.

This capture confirms the published hierarchy and validators; it cannot prove
that a future release object is immutable. The catalog manager therefore treats
the immutable-release rule as an upstream contract and fails closed on release
or manifest changes rather than serving an old generation.

Release URLs are accepted only from the configured STAC endpoint and the
trusted Overture AWS bucket/prefix. The Azure alternate is recorded in the
raw item fixtures but is not part of this server's initial asset selection.
