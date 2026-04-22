"""
predict.py
Predicts temperature at any (longitude, latitude) in France for a given UTC hour.

Uses adaptive kNN IDW with lapse rate correction:
  - Query point elevation fetched from Open-Meteo Elevation API
  - Elevation std dev of 20 nearest training points determines k (3-8)
  - Each neighbour's temperature is lapse-rate corrected to query elevation
  - k corrected temperatures are inverse-distance weighted

Lapse rate: 6.5 C per 1000m (standard environmental lapse rate)

Usage:
    python predict.py <longitude> <latitude> <YYYY-MM-DD HH:00>
"""

import sys
import requests
import pg8000.dbapi as pg
from datetime import datetime, timezone
from ingest import DB

K_MIN        = 3
K_MAX        = 8
ELEV_SCALE   = 400.0
LAPSE_RATE   = 0.0065  


def adaptive_k(elev_stddev: float) -> int:
    k = round(K_MAX - (K_MAX - K_MIN) * (elev_stddev / ELEV_SCALE))
    return int(max(K_MIN, min(K_MAX, k)))


def fetch_elevation(lon: float, lat: float) -> float:
    r = requests.get(
        "https://api.open-meteo.com/v1/elevation",
        params={"latitude": lat, "longitude": lon},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["elevation"][0]


def predict_temperature(lon: float, lat: float, observed_at: datetime) -> dict:
    query_elevation = fetch_elevation(lon, lat)

    conn = pg.connect(**DB)
    cur  = conn.cursor()

    geog = f"ST_MakePoint({lon}, {lat})::geography"

    cur.execute(f"""
        SELECT COALESCE(STDDEV(elevation), 0) FROM (
            SELECT elevation FROM locations
            WHERE is_test_zone = FALSE AND elevation IS NOT NULL
            ORDER BY geog <-> {geog}
            LIMIT 20
        ) n
    """)
    elev_stddev = float(cur.fetchone()[0])
    k = adaptive_k(elev_stddev)

    cur.execute(f"""
        SELECT wo.temperature, l.elevation, ST_Distance(l.geog, {geog}) AS dist_m
        FROM training_observations wo
        JOIN locations l ON l.id = wo.location_id
        WHERE wo.observed_at = %s
          AND wo.temperature IS NOT NULL
          AND l.elevation IS NOT NULL
        ORDER BY l.geog <-> {geog}
        LIMIT {k}
    """, (observed_at,))
    neighbours = cur.fetchall()
    conn.close()

    if not neighbours:
        return {"error": f"No training data found for {observed_at}. Only March 2026 is available."}

    total_weight = 0.0
    weighted_sum = 0.0
    for temp, neighbour_elev, dist_m in neighbours:
        corrected_temp = temp + LAPSE_RATE * (neighbour_elev - query_elevation)
        weight = 1.0 / max(dist_m, 1.0)
        weighted_sum += corrected_temp * weight
        total_weight += weight

    predicted = round(weighted_sum / total_weight, 2)

    return {
        "longitude":       lon,
        "latitude":        lat,
        "query_elevation": round(query_elevation, 1),
        "timestamp":       observed_at.strftime("%Y-%m-%d %H:%M UTC"),
        "predicted_temp":  predicted,
        "k_used":          k,
        "elev_stddev_m":   round(elev_stddev, 1),
        "neighbours":      len(neighbours),
    }


def main():
    if len(sys.argv) != 4:
        print("Usage: python predict.py <longitude> <latitude> <YYYY-MM-DD HH:00>")
        print('Example: python predict.py 2.352 48.857 "2026-03-15 12:00"')
        sys.exit(1)

    lon = float(sys.argv[1])
    lat = float(sys.argv[2])
    ts  = datetime.strptime(sys.argv[3], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

    result = predict_temperature(lon, lat, ts)

    if "error" in result:
        print(f"Error: {result['error']}")
        sys.exit(1)

    print(f"\nTemperature Prediction")
    print(f"  Location    : ({result['latitude']}N, {result['longitude']}E)")
    print(f"  Elevation   : {result['query_elevation']} m")
    print(f"  Timestamp   : {result['timestamp']}")
    print(f"  Prediction  : {result['predicted_temp']} C")
    print(f"  k used      : {result['k_used']}  (elev stddev = {result['elev_stddev_m']} m)")
    print(f"  Neighbours  : {result['neighbours']} training points")


if __name__ == "__main__":
    main()
