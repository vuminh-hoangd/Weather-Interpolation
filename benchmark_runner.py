"""
benchmark_runner.py
Runs each spatial/temporal benchmark with and without indexes.
For every scenario it captures EXPLAIN ANALYZE timing and prints:
1. A formatted side-by-side comparison table
2. Raw EXPLAIN ANALYZE plan saved to benchmark_results/

Requirements: pip install pg8000
Usage:        python benchmark_runner.py
"""

import json
import os
import pg8000.dbapi as pg
from datetime import datetime


DB = dict(host="localhost", port=5432, database="imperial_db",
          user="postgres", password="Imperial")

TARGET_TIME = "2026-03-15 12:00:00+00"
RUNS        = 3
OUTPUT_DIR  = "benchmark_results"


BENCHMARKS = [
    {
        "id":   "knn",
        "name": "kNN Spatial Search",
        "desc": "Find 20 nearest grid points to a query location (lon, lat) to compute elevation stddev, then find k nearest training observations at the query timestamp — benchmarked at Paris (48.857°N, 2.352°E).",
        "scenarios": [
            {
                "label": "No index — step 1 only (seq scan)",
                "baseline_idx": None,
                "setup": ["DROP INDEX IF EXISTS idx_locations_geog_gist"],
                "query": """
                    SELECT l.name, l.lat, l.lon,
                           ST_Distance(l.geog,
                               ST_MakePoint(2.352, 48.857)::geography) AS dist_m
                    FROM   locations l
                    ORDER  BY l.geog <-> ST_MakePoint(2.352, 48.857)::geography
                    LIMIT  20
                """,
            },
            {
                "label": "No index — full pipeline (both searches)",
                "baseline_idx": None,
                "setup": [],
                "query": f"""
                    SELECT * FROM predict_temperature(
                        2.352::DOUBLE PRECISION,
                        48.857::DOUBLE PRECISION,
                        '{TARGET_TIME}'::TIMESTAMPTZ
                    )
                """,
            },
            {
                "label": "GiST — step 1 only (20 neighbours)",
                "baseline_idx": 0,
                "setup": [
                    "DROP INDEX IF EXISTS idx_locations_geog_gist",
                    "CREATE INDEX idx_locations_geog_gist ON locations USING GIST(geog)",
                ],
                "query": """
                    SELECT l.name, l.lat, l.lon,
                           ST_Distance(l.geog,
                               ST_MakePoint(2.352, 48.857)::geography) AS dist_m
                    FROM   locations l
                    ORDER  BY l.geog <-> ST_MakePoint(2.352, 48.857)::geography
                    LIMIT  20
                """,
            },
            {
                "label": "GiST — full pipeline (both searches)",
                "baseline_idx": 1,
                "setup": [],
                "query": f"""
                    SELECT * FROM predict_temperature(
                        2.352::DOUBLE PRECISION,
                        48.857::DOUBLE PRECISION,
                        '{TARGET_TIME}'::TIMESTAMPTZ
                    )
                """,
            },
        ],
    },
    {
        "id":   "radius",
        "name": "Radius Search (ST_DWithin)",
        "desc": "All grid points within 150 km of Lyon (45.764°N, 4.836°E)",
        "scenarios": [
            {
                "label": "No index (seq scan)",
                "setup": ["DROP INDEX IF EXISTS idx_locations_geog_gist"],
                "query": """
                    SELECT l.name, l.lat, l.lon,
                           ST_Distance(l.geog,
                               ST_MakePoint(4.836, 45.764)::geography) AS dist_m
                    FROM   locations l
                    WHERE  ST_DWithin(l.geog,
                               ST_MakePoint(4.836, 45.764)::geography, 150000)
                    ORDER  BY dist_m
                """,
            },
            {
                "label": "GiST index",
                "setup": [
                    "DROP INDEX IF EXISTS idx_locations_geog_gist",
                    "CREATE INDEX idx_locations_geog_gist ON locations USING GIST(geog)",
                ],
                "query": """
                    SELECT l.name, l.lat, l.lon,
                           ST_Distance(l.geog,
                               ST_MakePoint(4.836, 45.764)::geography) AS dist_m
                    FROM   locations l
                    WHERE  ST_DWithin(l.geog,
                               ST_MakePoint(4.836, 45.764)::geography, 150000)
                    ORDER  BY dist_m
                """,
            },
        ],
    },
    {
        "id":   "bbox",
        "name": "Bounding Box Search (&&)",
        "desc": "All grid points in southern France (lat 42–45°N, lon -2–8°E)",
        "scenarios": [
            {
                "label": "No index (seq scan)",
                "setup": ["DROP INDEX IF EXISTS idx_locations_geog_gist"],
                "query": """
                    SELECT l.name, l.lat, l.lon
                    FROM   locations l
                    WHERE  l.geog &&
                           ST_MakeEnvelope(-2.0, 42.0, 8.0, 45.0, 4326)::geography
                    ORDER  BY l.lat, l.lon
                """,
            },
            {
                "label": "GiST index",
                "setup": [
                    "DROP INDEX IF EXISTS idx_locations_geog_gist",
                    "CREATE INDEX idx_locations_geog_gist ON locations USING GIST(geog)",
                ],
                "query": """
                    SELECT l.name, l.lat, l.lon
                    FROM   locations l
                    WHERE  l.geog &&
                           ST_MakeEnvelope(-2.0, 42.0, 8.0, 45.0, 4326)::geography
                    ORDER  BY l.lat, l.lon
                """,
            },
        ],
    },
    {
        "id":   "time",
        "name": "Time-Range Query",
        "desc": "Row count for first week of March 2026",
        "scenarios": [
            {
                "label": "No index (seq scan)",
                "setup": [
                    "DROP INDEX IF EXISTS idx_obs_time_brin",
                    "DROP INDEX IF EXISTS idx_obs_time_btree",
                ],
                "query": """
                    SELECT COUNT(*)
                    FROM   weather_observations
                    WHERE  observed_at BETWEEN '2026-03-01 00:00:00+00'
                                           AND '2026-03-07 23:00:00+00'
                """,
            },
            {
                "label": "BRIN index",
                "setup": [
                    "DROP INDEX IF EXISTS idx_obs_time_brin",
                    "DROP INDEX IF EXISTS idx_obs_time_btree",
                    "CREATE INDEX idx_obs_time_brin ON weather_observations USING BRIN(observed_at)",
                ],
                "query": """
                    SELECT COUNT(*)
                    FROM   weather_observations
                    WHERE  observed_at BETWEEN '2026-03-01 00:00:00+00'
                                           AND '2026-03-07 23:00:00+00'
                """,
            },
            {
                "label": "B-tree index",
                "setup": [
                    "DROP INDEX IF EXISTS idx_obs_time_brin",
                    "DROP INDEX IF EXISTS idx_obs_time_btree",
                    "CREATE INDEX idx_obs_time_btree ON weather_observations(observed_at)",
                ],
                "query": """
                    SELECT COUNT(*)
                    FROM   weather_observations
                    WHERE  observed_at BETWEEN '2026-03-01 00:00:00+00'
                                           AND '2026-03-07 23:00:00+00'
                """,
            },
            {
                "label": "GiST + B-tree (combined)",
                "setup": [
                    "CREATE INDEX IF NOT EXISTS idx_locations_geog_gist ON locations USING GIST(geog)",
                ],
                "query": """
                    SELECT l.name, wo.observed_at, wo.temperature, wo.rain
                    FROM   weather_observations wo
                    JOIN   locations l ON l.id = wo.location_id
                    WHERE  ST_DWithin(l.geog,
                               ST_MakePoint(2.352, 48.857)::geography, 200000)
                      AND  wo.observed_at BETWEEN '2026-03-01 00:00:00+00'
                                              AND '2026-03-07 23:00:00+00'
                    ORDER  BY wo.observed_at
                """,
            },
        ],
    },
]


def run_explain(conn, setup_stmts: list[str], query_sql: str,
                runs: int = RUNS) -> tuple[float, str]:
    cur = conn.cursor()
    for stmt in setup_stmts:
        cur.execute(stmt)
    conn.commit()

    times    = []
    raw_plan = ""

    for i in range(runs):
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query_sql}")
        result = cur.fetchone()[0]
        plan   = result if isinstance(result, list) else json.loads(result)
        times.append(plan[0]["Execution Time"])

        if i == 0:
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {query_sql}")
            raw_plan = "\n".join(row[0] for row in cur.fetchall())

    times.sort()
    return times[len(times) // 2], raw_plan


def print_table(benchmark: dict, results: list[dict]) -> None:
    col_w  = [36, 18, 12]
    div    = "+" + "+".join("-" * w for w in col_w) + "+"
    header = (f"| {'Strategy':<{col_w[0]-2}} "
              f"| {'Exec time (ms)':<{col_w[1]-2}} "
              f"| {'Speedup':<{col_w[2]-2}} |")

    print(f"\n{'═'*70}")
    print(f"  {benchmark['name']}")
    print(f"  {benchmark['desc']}")
    print(div)
    print(header)
    print(div)

    for i, r in enumerate(results):
        scenario = benchmark["scenarios"][i]
        if "baseline_idx" in scenario:
            bi = scenario["baseline_idx"]
            speedup = "baseline" if bi is None else f"{results[bi]['ms'] / r['ms']:.1f}×"
        else:
            speedup = "baseline" if i == 0 else f"{results[0]['ms'] / r['ms']:.1f}×"
        print(f"| {r['label']:<{col_w[0]-2}} "
              f"| {r['ms']:>{col_w[1]-5}.2f} ms    "
              f"| {speedup:<{col_w[2]-2}} |")
    print(div)



def run_all() -> list[dict]:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_results = []
    conn        = pg.connect(**DB)

    try:
        for bench in BENCHMARKS:
            scenario_results = []
            raw_lines = [f"{'='*70}\n{bench['name']} — {bench['desc']}\n{'='*70}\n"]

            for scenario in bench["scenarios"]:
                print(f"  {bench['name']} / {scenario['label']} ...", end=" ", flush=True)
                ms, raw = run_explain(conn, scenario["setup"], scenario["query"])
                print(f"{ms:.2f} ms")
                scenario_results.append({"label": scenario["label"], "ms": ms})
                raw_lines.append(f"\n--- {scenario['label']} ---\n{raw}\n")

            print_table(bench, scenario_results)

            fname = os.path.join(OUTPUT_DIR, f"{timestamp}_{bench['id']}.txt")
            with open(fname, "w") as f:
                f.write("\n".join(raw_lines))
            print(f"  Raw plan → {fname}")

            all_results.append({"benchmark": bench, "results": scenario_results})
    finally:
        conn.close()

    return all_results


if __name__ == "__main__":
    print(f"{'═'*70}")
    print("  Query Benchmark — France Weather Grid (March 2026)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*70}")
    run_all()
    print(f"\nDone. Raw plans in ./{OUTPUT_DIR}/")
