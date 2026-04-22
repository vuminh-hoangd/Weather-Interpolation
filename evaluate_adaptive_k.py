"""
evaluate_adaptive_k.py
Compares three prediction strategies on test zone cities for March 2026:
1. Fixed k=5, (baseline)
2. Adaptive k, no lapse rate correction
3. Adaptive k + lapse rate correction

Usage: python evaluate_adaptive_k.py
"""

import logging
import numpy as np
import pg8000.dbapi as pg
from ingest import DB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

K_MIN       = 3
K_MAX       = 8
ELEV_SCALE  = 400.0
LAPSE_RATE  = 0.0065


def adaptive_k(elev_stddev: float) -> int:
    k = round(K_MAX - (K_MAX - K_MIN) * (elev_stddev / ELEV_SCALE))
    return int(max(K_MIN, min(K_MAX, k)))


def get_elev_stddev(cur, geog_wkt: str) -> float:
    cur.execute(f"""
        SELECT COALESCE(STDDEV(elevation), 0) FROM (
            SELECT elevation FROM locations
            WHERE is_test_zone = FALSE AND elevation IS NOT NULL
            ORDER BY geog <-> '{geog_wkt}'::geography
            LIMIT 20
        ) n
    """)
    return float(cur.fetchone()[0])


def get_neighbours(cur, geog_wkt: str, observed_at, k: int) -> list[tuple]:
    """Return (temperature, elevation, distance_m) for k nearest training points."""
    cur.execute(f"""
        SELECT wo.temperature, l.elevation, ST_Distance(l.geog, '{geog_wkt}'::geography)
        FROM training_observations wo
        JOIN locations l ON l.id = wo.location_id
        WHERE wo.observed_at = %s
          AND wo.temperature IS NOT NULL
          AND l.elevation IS NOT NULL
        ORDER BY l.geog <-> '{geog_wkt}'::geography
        LIMIT {k}
    """, (observed_at,))
    return cur.fetchall()


def idw(neighbours: list[tuple], query_elev: float = None) -> float:
    total_w, weighted_sum = 0.0, 0.0
    for temp, elev, dist in neighbours:
        if query_elev is not None:
            temp = temp + LAPSE_RATE * (elev - query_elev)
        w = 1.0 / max(dist, 1.0)
        weighted_sum += temp * w
        total_w += w
    return weighted_sum / total_w


def evaluate() -> list[tuple]:
    conn = pg.connect(**DB)
    cur  = conn.cursor()

    cur.execute("""
        SELECT l.id, ST_AsText(l.geog) AS geog_wkt, l.elevation,
               (SELECT tz.name FROM test_zones tz ORDER BY tz.center_geog <-> l.geog LIMIT 1) AS city
        FROM locations l WHERE l.is_test_zone = TRUE AND l.elevation IS NOT NULL
    """)
    test_locs = cur.fetchall()
    log.info("Test locations: %d", len(test_locs))

    cur.execute("""
        SELECT DISTINCT observed_at FROM weather_observations
        WHERE observed_at >= '2026-03-01' AND observed_at < '2026-04-01'
        ORDER BY observed_at
    """)
    timestamps = [r[0] for r in cur.fetchall()]
    log.info("Timestamps: %d", len(timestamps))

    city_errors = {city: {"fixed": [], "adaptive": [], "lapse": [],
                          "fixed_sq": [], "adaptive_sq": [], "lapse_sq": []}
                   for city in set(r[3] for r in test_locs)}

    total = len(test_locs)
    for idx, (loc_id, geog_wkt, query_elev, city) in enumerate(test_locs, 1):
        if idx % 10 == 0:
            log.info("  [%d/%d] %s", idx, total, city)

        elev_std = get_elev_stddev(cur, geog_wkt)
        k_adapt  = adaptive_k(elev_std)

        cur.execute("""
            SELECT observed_at, temperature FROM weather_observations
            WHERE location_id = %s AND temperature IS NOT NULL
        """, (loc_id,))
        actuals = {r[0]: r[1] for r in cur.fetchall()}

        for ts, actual in actuals.items():
            nb_fixed    = get_neighbours(cur, geog_wkt, ts, 5)
            nb_adaptive = get_neighbours(cur, geog_wkt, ts, k_adapt)

            if nb_fixed:
                err_f = actual - idw(nb_fixed)
                err_l = actual - idw(nb_fixed, query_elev)
                city_errors[city]["fixed"].append(abs(err_f))
                city_errors[city]["fixed_sq"].append(err_f ** 2)
                city_errors[city]["lapse"].append(abs(err_l))
                city_errors[city]["lapse_sq"].append(err_l ** 2)
            if nb_adaptive:
                err_a = actual - idw(nb_adaptive)
                city_errors[city]["adaptive"].append(abs(err_a))
                city_errors[city]["adaptive_sq"].append(err_a ** 2)

    conn.close()

    results = []
    for city in sorted(city_errors.keys()):
        e = city_errors[city]
        mae_f  = round(float(np.mean(e["fixed"])), 3)
        mae_a  = round(float(np.mean(e["adaptive"]) if e["adaptive"] else mae_f), 3)
        mae_l  = round(float(np.mean(e["lapse"])), 3)
        rmse_f = round(float(np.sqrt(np.mean(e["fixed_sq"]))), 3)
        rmse_a = round(float(np.sqrt(np.mean(e["adaptive_sq"])) if e["adaptive_sq"] else rmse_f), 3)
        rmse_l = round(float(np.sqrt(np.mean(e["lapse_sq"]))), 3)
        results.append((city, mae_f, mae_a, mae_l, rmse_f, rmse_a, rmse_l))

    return results


def main():
    results = evaluate()
    print()
    print(f"{'City':<14} {'Fix MAE':<10} {'Adp MAE':<10} {'Lps MAE':<10} {'Fix RMSE':<10} {'Adp RMSE':<10} {'Lps RMSE':<10} Best")
    print("-" * 82)
    for city, mae_f, mae_a, mae_l, rmse_f, rmse_a, rmse_l in results:
        best = min(("Fixed", mae_f), ("Adaptive", mae_a), ("Lapse", mae_l), key=lambda x: x[1])[0]
        print(f"{city:<14} {mae_f:<10.3f} {mae_a:<10.3f} {mae_l:<10.3f} {rmse_f:<10.3f} {rmse_a:<10.3f} {rmse_l:<10.3f} {best}")
    all_mae_f  = [r[1] for r in results]
    all_mae_a  = [r[2] for r in results]
    all_mae_l  = [r[3] for r in results]
    all_rmse_f = [r[4] for r in results]
    all_rmse_a = [r[5] for r in results]
    all_rmse_l = [r[6] for r in results]
    print("-" * 82)
    print(f"{'AVERAGE':<14} {np.mean(all_mae_f):<10.3f} {np.mean(all_mae_a):<10.3f} {np.mean(all_mae_l):<10.3f} "
          f"{np.mean(all_rmse_f):<10.3f} {np.mean(all_rmse_a):<10.3f} {np.mean(all_rmse_l):<10.3f}")