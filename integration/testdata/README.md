# Container source fixture

`places.parquet` is an upstream-shaped GeoParquet source for the container
lifecycle test. It contains the two rows from `testdata/geoparquet/places.parquet`
plus a conservative world-sized `bbox` struct. The service output fixture alone
cannot serve as upstream input because the query requires bounding boxes.

Generate with pinned DuckDB 1.5.5 and the bundled spatial extension, from the
repository root:

```sql
LOAD spatial;
COPY (
    SELECT *, {
        'xmin': -180.0::FLOAT, 'xmax': 180.0::FLOAT,
        'ymin': -90.0::FLOAT, 'ymax': 90.0::FLOAT
    } AS bbox
    FROM read_parquet('testdata/geoparquet/places.parquet')
) TO 'integration/testdata/places.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

`san-francisco.parquet` contains only `poi-2` and is served for partition `00002`;
`helsinki.parquet` contains only `poi-1` and is served for partition `00011`.
Other partitions serve `empty.parquet`, which preserves the same source schema.
The fixture supports HTTP HEAD and range reads and counts requests/bytes. A
world-bbox catalog variant forces an unpruned scan for output-ID comparison.

Generate the partition fixtures from `places.parquet` with pinned DuckDB 1.5.5:

```sql
SET autoload_known_extensions = false;
SET autoinstall_known_extensions = false;
COPY (SELECT * FROM read_parquet('integration/testdata/places.parquet')
      WHERE id = 'poi-2')
TO 'integration/testdata/san-francisco.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
COPY (SELECT * FROM read_parquet('integration/testdata/places.parquet')
      WHERE id = 'poi-1')
TO 'integration/testdata/helsinki.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
COPY (SELECT * FROM read_parquet('integration/testdata/places.parquet') WHERE FALSE)
TO 'integration/testdata/empty.parquet' (FORMAT PARQUET, COMPRESSION ZSTD,
    KV_METADATA {geo: '{"version":"1.0.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["Point"]}}}'});
```

The empty fixture must retain explicit GeoParquet metadata so DuckDB recognizes
its WKB geometry column even when no rows provide geometry-type inference.
