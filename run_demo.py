"""
run_demo.py
Full end-to-end pipeline orchestrator. Run this once to set up everything.

Steps:
  1. Verify PostgreSQL + PostGIS connection
  2. Run SQL schema + seed files in order
  3. Backfill March 2026 data from Open-Meteo
  4. Run benchmark suite
  5. Generate HTML report
  6. Open france_grid.html map

Requirements: pip install psycopg requests schedule folium
Usage:        python run_demo.py
"""

import os
import sys
import subprocess
import pg8000.dbapi as pg

DB = dict(host="localhost", port=5432, database="imperial_db",
          user="postgres", password="Imperial")


PSQL = r"C:\Users\chwon\AppData\Local\Programs\pgAdmin 4\runtime\psql.exe"

SQL_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
SQL_FILES = [
    "01_schema.sql",
    "02_indexes.sql",
    "03_seed_france.sql",
    "04_monthly_partition.sql",
]


def step(n: int, title: str) -> None:
    print(f"\n{'─'*60}")
    print(f"  STEP {n}: {title}")
    print(f"{'─'*60}")


def run_sql_file(path: str) -> None:
    env = os.environ.copy()
    env["PGPASSWORD"] = DB["password"]
    result = subprocess.run(
        [PSQL,
         "-h", DB["host"],
         "-p", str(DB["port"]),
         "-U", DB["user"],
         "-d", DB["database"],
         "-f", path,
         "-v", "ON_ERROR_STOP=0"],
        capture_output=True, text=True, env=env,
    )
    if result.returncode != 0 and "already exists" not in result.stderr:
        print(f"  WARNING: {os.path.basename(path)}\n{result.stderr[:300]}")
    else:
        print(f"  ✓  {os.path.basename(path)}")


def main():
    step(1, "Verify PostgreSQL + PostGIS")
    try:
        conn = pg.connect(**DB)
        cur  = conn.cursor()
        cur.execute("SELECT PostGIS_Version()")
        version = cur.fetchone()[0]
        conn.close()
        print(f"  PostGIS: {version}")
        print("  Connection OK.")
    except Exception as e:
        print(f"\n  ERROR: {e}")
        print(
            "\n  Make sure:\n"
            "    • PostgreSQL is running\n"
            "    • PostGIS is installed (CREATE EXTENSION postgis;)\n"
            "    • Database 'imperial_db' exists"
        )
        sys.exit(1)

    step(2, "Create schema, indexes, seed France grid, create March 2026 partition")
    for filename in SQL_FILES:
        run_sql_file(os.path.join(SQL_DIR, filename))

    step(3, "Backfill March 2026 data from Open-Meteo (~2,000 grid points)")
    print("  This takes ~7 minutes. Press Ctrl+C to skip.")
    try:
        subprocess.run(
            [sys.executable, "ingest.py", "backfill", "2026-03"],
            check=True,
        )
    except KeyboardInterrupt:
        print("  Skipped.")
    except subprocess.CalledProcessError as e:
        print(f"  ERROR: {e}")

    step(4, "Run query benchmarks")
    from benchmark_runner import run_all
    run_all()

    step(5, "Generate HTML report")
    import generate_report
    generate_report.main()

    step(6, "Open France grid map")
    map_path = os.path.abspath("france_grid.html")
    if os.path.exists(map_path):
        os.startfile(map_path)
        print(f"  Opened {map_path}")
    else:
        print("  france_grid.html not found — run: python visualize.py")

    print(f"\n{'═'*60}")
    print("  Demo complete.")
    print(f"  • Report:  {os.path.abspath('report.html')}")
    print(f"  • Map:     {os.path.abspath('france_grid.html')}")
    print(f"  • Plans:   {os.path.abspath('benchmark_results/')}")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
