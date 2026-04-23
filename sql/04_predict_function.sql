-- 04_predict_function.sql
-- Adaptive kNN temperature prediction with lapse rate correction.

-- p_lon is longitude of query point, p_lat is latitude of query point
-- p_timestamp is UTC hour to predict
-- p_elevation is elevation of query point in metres (optional — if NULL, uses the nearest grid point's elevation as proxy)


CREATE OR REPLACE FUNCTION predict_temperature(
    p_lon         DOUBLE PRECISION,
    p_lat         DOUBLE PRECISION,
    p_timestamp   TIMESTAMPTZ,
    p_elevation   DOUBLE PRECISION DEFAULT NULL
)
RETURNS TABLE (
    predicted_temp_c  NUMERIC,
    k_used            INT,
    query_elevation_m NUMERIC,
    elev_stddev_m     NUMERIC,
    neighbours_found  BIGINT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_geog        GEOGRAPHY := ST_MakePoint(p_lon, p_lat)::geography;
    v_elev_stddev DOUBLE PRECISION;
    v_k           INT;
    v_query_elev  DOUBLE PRECISION;
    lapse_rate    CONSTANT DOUBLE PRECISION := 0.0065;
BEGIN
    IF p_elevation IS NOT NULL THEN
        v_query_elev := p_elevation;
    ELSE
        SELECT elevation INTO v_query_elev
        FROM   locations
        WHERE  elevation IS NOT NULL
        ORDER  BY geog <-> v_geog
        LIMIT  1;
    END IF;

    SELECT COALESCE(STDDEV(elevation), 0)
    INTO   v_elev_stddev
    FROM (
        SELECT elevation FROM locations
        WHERE  is_test_zone = FALSE AND elevation IS NOT NULL
        ORDER  BY geog <-> v_geog
        LIMIT  20
    ) n;

    v_k := GREATEST(3, LEAST(8, ROUND(8 - 5 * (v_elev_stddev / 400.0))));

    RETURN QUERY
    SELECT
        ROUND((
            SUM(
                (sub.temperature + lapse_rate * (sub.elevation - v_query_elev))
                / GREATEST(ST_Distance(sub.geog, v_geog), 1.0)
            )
            / SUM(1.0 / GREATEST(ST_Distance(sub.geog, v_geog), 1.0))
        )::NUMERIC, 2)                   AS predicted_temp_c,
        v_k                              AS k_used,
        ROUND(v_query_elev::NUMERIC, 1)  AS query_elevation_m,
        ROUND(v_elev_stddev::NUMERIC, 1) AS elev_stddev_m,
        COUNT(*)                         AS neighbours_found
    FROM (
        SELECT wo.temperature, l.elevation, l.geog
        FROM   training_observations wo
        JOIN   locations l ON l.id = wo.location_id
        WHERE  wo.observed_at = p_timestamp
          AND  wo.temperature IS NOT NULL
          AND  l.elevation IS NOT NULL
        ORDER  BY l.geog <-> v_geog
        LIMIT  v_k
    ) sub;
END;
$$;
