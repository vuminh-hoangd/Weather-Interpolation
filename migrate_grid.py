"""
migrate_grid.py
Migrates the database from 0.25° to 0.18° grid and applies city-proportional
exclusion radii, without dropping existing observations.

Steps:
  1. Insert new 0.18° grid locations (ON CONFLICT DO NOTHING skips existing ones)
  2. Update test_zones with new proportional radii
  3. Reset is_test_zone on all locations
  4. Re-apply is_test_zone with new radii
  5. Backfill March 2026 data for new locations only

Usage: python migrate_grid.py
"""

import time
import logging
import requests
import pg8000.dbapi as pg
from ingest import DB, france_grid, fetch_archive, ARCHIVE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TEST_ZONES = [
    ("Paris",       2.352,  48.857, 40000),
    ("Lyon",        4.836,  45.764, 28000),
    ("Grenoble",    5.724,  45.188, 15000),
    ("Toulouse",    1.444,  43.605, 24000),
    ("Bordeaux",   -0.579,  44.838, 24000),
    ("Lille",       3.057,  50.629, 20000),
    ("Nantes",     -1.554,  47.218, 20000),
    ("Rennes",     -1.678,  48.117, 16000),
    ("Strasbourg",  7.752,  48.573, 16000),
    ("Avignon",     4.805,  43.949, 24000),
]


def step1_insert_new_locations(conn):
    cur = conn.cursor()
    grid = france_grid()
    log.info("Step 1 — Inserting new 0.18 grid locations (%d total points) ...", len(grid))
    new_count = 0
    for lat, lon in grid:
        name = f"grid_{lat}_{lon}"
        cur.execute(
            "INSERT INTO locations (name, lat, lon) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            (name, lat, lon)
        )
        if cur.rowcount:
            new_count += 1
    conn.commit()
    log.info("  %d new locations inserted (%d already existed).",
             new_count, len(grid) - new_count)
    return new_count


def step2_update_test_zones(conn):
    cur = conn.cursor()
    log.info("Step 2 — Updating test zone radii ...")
    for name, lon, lat, radius in TEST_ZONES:
        cur.execute(
            "UPDATE test_zones SET radius_m = %s WHERE name = %s",
            (radius, name)
        )
        log.info("  %-12s radius -> %d m", name, radius)
    conn.commit()


def step3_reset_flags(conn):
    cur = conn.cursor()
    log.info("Step 3 — Resetting all is_test_zone flags ...")
    cur.execute("UPDATE locations SET is_test_zone = FALSE")
    conn.commit()
    log.info("  Done.")


def step4_reapply_flags(conn):
    cur = conn.cursor()
    log.info("Step 4 — Re-applying is_test_zone with new radii ...")
    cur.execute("""
        UPDATE locations l
        SET    is_test_zone = TRUE
        WHERE  EXISTS (
            SELECT 1 FROM test_zones tz
            WHERE  ST_DWithin(l.geog, tz.center_geog, tz.radius_m)
        )
    """)
    conn.commit()
    cur.execute("SELECT is_test_zone, COUNT(*) FROM locations GROUP BY is_test_zone ORDER BY is_test_zone")
    for row in cur.fetchall():
        label = "test zone" if row[0] else "training"
        log.info("  %s locations: %d", label, row[1])


def step5_backfill_new(conn):
    log.info("Step 5 — Backfilling March 2026 for new locations ...")
    cur = conn.cursor()
    cur.execute("""
        SELECT l.id, l.lat, l.lon
        FROM   locations l
        WHERE  NOT EXISTS (
            SELECT 1 FROM weather_observations wo
            WHERE  wo.location_id = l.id
            LIMIT  1
        )
    """)
    new_locs = cur.fetchall()
    log.info("  %d locations need backfill.", len(new_locs))

    for idx, (loc_id, lat, lon) in enumerate(new_locs, 1):
        log.info("  [%d/%d] (%s, %s)", idx, len(new_locs), lat, lon)
        try:
            rows = fetch_archive(lat, lon, "2026-03-01", "2026-03-31")
            if rows:
                cur.executemany(
                    "INSERT INTO weather_observations "
                    "(location_id, observed_at, temperature, humidity, rain, soil_temp) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    [(loc_id, r["observed_at"], r["temperature"],
                      r["humidity"], r["rain"], r["soil_temp"]) for r in rows]
                )
                conn.commit()
        except Exception as exc:
            log.warning("  SKIPPED (%s, %s): %s", lat, lon, exc)
        time.sleep(0.2)

    log.info("  Backfill complete.")


def main():
    conn = pg.connect(**DB)
    try:
        step1_insert_new_locations(conn)
        step2_update_test_zones(conn)
        step3_reset_flags(conn)
        step4_reapply_flags(conn)
        step5_backfill_new(conn)
    finally:
        conn.close()
    log.info("Migration complete. Run generate_report.py to regenerate the report.")


if __name__ == "__main__":
    main()
