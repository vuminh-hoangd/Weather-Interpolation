-- queries.sql
-- kNN spatial queries for temperature prediction
-- Replace :target_lon, :target_lat, :target_time, :k with actual values.


UPDATE locations l
SET    is_test_zone = TRUE
WHERE  EXISTS (
    SELECT 1
    FROM   test_zones tz
    WHERE  ST_DWithin(l.geog, tz.center_geog, tz.radius_m)
);


SELECT
    l.name,
    l.lat,
    l.lon,
    wo.temperature,
    wo.humidity,
    wo.rain,
    wo.soil_temp,
    ST_Distance(l.geog,
        ST_MakePoint(:target_lon, :target_lat)::geography
    ) AS dist_m
FROM   training_observations wo
JOIN   locations l ON l.id = wo.location_id
WHERE  wo.observed_at = :target_time
ORDER  BY l.geog <-> ST_MakePoint(:target_lon, :target_lat)::geography
LIMIT  :k;


WITH neighbours AS (
    SELECT
        wo.temperature,
        GREATEST(
            ST_Distance(l.geog,
                ST_MakePoint(:target_lon, :target_lat)::geography),
            1.0     -- 1 m floor avoids division-by-zero on exact coordinate match
        ) AS dist_m
    FROM   training_observations wo
    JOIN   locations l ON l.id = wo.location_id
    WHERE  wo.observed_at = :target_time
    ORDER  BY l.geog <-> ST_MakePoint(:target_lon, :target_lat)::geography
    LIMIT  :k
)
SELECT
    ROUND(
        (SUM(temperature / dist_m) / SUM(1.0 / dist_m))::NUMERIC,
        2
    ) AS predicted_temperature_c
FROM neighbours;


WITH test_obs AS (
    SELECT
        wo.id,
        wo.observed_at,
        wo.temperature AS actual_temp,
        l.geog
    FROM   weather_observations wo
    JOIN   locations l ON l.id = wo.location_id
    WHERE  l.is_test_zone = TRUE
),
predictions AS (
    SELECT
        t.id          AS test_obs_id,
        t.actual_temp,
        (
            SELECT
                SUM(wo2.temperature /
                        GREATEST(ST_Distance(l2.geog, t.geog), 1.0))
                / SUM(1.0 /
                        GREATEST(ST_Distance(l2.geog, t.geog), 1.0))
            FROM   training_observations wo2
            JOIN   locations l2 ON l2.id = wo2.location_id
            WHERE  wo2.observed_at = t.observed_at
            ORDER  BY l2.geog <-> t.geog
            LIMIT  5
        ) AS predicted_temp
    FROM test_obs t
)
SELECT
    COUNT(*)                                                        AS evaluated_points,
    ROUND(AVG(ABS(actual_temp - predicted_temp))::NUMERIC,  3)     AS mae_c,
    ROUND(SQRT(AVG((actual_temp - predicted_temp)^2))::NUMERIC, 3) AS rmse_c
FROM predictions
WHERE predicted_temp IS NOT NULL;
