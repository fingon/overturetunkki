SELECT
    id,
    ST_Point(longitude, latitude) AS geometry,
    struct_pack(primary_name := id, aliases := [id]) AS names
FROM (VALUES
    ('nw', -10.0, 10.0),
    ('ne', 10.0, 10.0),
    ('sw', -10.0, -10.0),
    ('se', 10.0, -10.0),
    ('origin', 0.0, 0.0),
    ('east', 180.0, 0.0),
    ('west', -180.0, 0.0),
    ('north-pole', 0.0, 90.0),
    ('south-pole', 0.0, -90.0)
) AS points(id, longitude, latitude)
