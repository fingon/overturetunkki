# GeoParquet writer fixtures

These deterministic fixtures are written by the pinned DuckDB build through
`internal/geoparquet`. The selected `geometry` column is raw WKB `BYTE_ARRAY`
data compressed with zstd; the single `geo` key declares GeoParquet 1.1.0,
`geometry` as the primary Point column, and WGS84 longitude/latitude semantics.
The CRS is omitted because GeoParquet 1.1.0 defines the default as OGC:CRS84.

`places.parquet` contains two rows and the nested `names` struct. `empty.parquet`
contains the same schema and metadata with zero rows. Tests read both files with
Apache Arrow's independent Parquet reader.

Regenerate the files only when intentionally changing the writer contract:

```sh
OVERTURE_UPDATE_GEOPARQUET_FIXTURES=1 go test ./internal/geoparquet -run '^TestGenerateGeoParquetFixtures$' -count=1
```
