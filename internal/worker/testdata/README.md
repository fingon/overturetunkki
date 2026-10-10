# Cached tile source fixture

`cached-points.sql` generates a 41-by-41 grid over supplied geographic bounds,
with point geometry, nested names, and valid per-point bounding boxes. Its
parameters are an interval identifier, minimum longitude, longitude span,
minimum latitude, and latitude span.

The cached-tile test partitions this data into complete coarser GeoParquet
files, derives a finer tile, and compares its rows against direct H3 membership.
The ordinary case includes points outside the logical parent; the antimeridian
case uses separate longitude intervals. Empty input preserves the same schema.
