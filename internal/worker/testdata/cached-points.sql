WITH coordinates AS (
    SELECT
        CAST(? AS VARCHAR) || '-' || x.i::VARCHAR || '-' || y.i::VARCHAR AS id,
        ? + (? * x.i / 40) AS longitude,
        ? + (? * y.i / 40) AS latitude
    FROM range(41) AS x(i), range(41) AS y(i)
)
SELECT
    id,
    ST_Point(longitude, latitude) AS geometry,
    struct_pack("primary" := id, common := [id]) AS names,
    struct_pack(xmin := longitude, xmax := longitude,
                ymin := latitude, ymax := latitude) AS bbox
FROM coordinates
