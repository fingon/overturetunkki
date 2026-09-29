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
