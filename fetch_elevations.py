"""
fetch_elevations.py
Fetches elevation for every location in the DB using the Open-Meteo Elevation API
and stores it in the locations.elevation column.

API: https://api.open-meteo.com/v1/elevation
Batch size: 100 coordinates per request

Usage: python fetch_elevations.py
"""

import time
import logging
import requests
import pg8000.dbapi as pg
from ingest import DB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

ELEVATION_URL = "https://api.open-meteo.com/v1/elevation"
BATCH_SIZE = 100


def fetch_elevations(lats: list[float], lons: list[float]) -> list[float]:
    r = requests.get(ELEVATION_URL, params={
        "latitude":  ",".join(str(x) for x in lats),
        "longitude": ",".join(str(x) for x in lons),
    }, timeout=30)
    r.raise_for_status()
    return r.json()["elevation"]


def main():
    conn = pg.connect(**DB)
    cur = conn.cursor()

    cur.execute("SELECT id, lat, lon FROM locations WHERE elevation IS NULL ORDER BY id")
    locations = cur.fetchall()
    log.info("Fetching elevations for %d locations in batches of %d ...", len(locations), BATCH_SIZE)

    updated = 0
    for i in range(0, len(locations), BATCH_SIZE):
        batch = locations[i:i + BATCH_SIZE]
        ids  = [r[0] for r in batch]
        lats = [r[1] for r in batch]
        lons = [r[2] for r in batch]

        try:
            elevations = fetch_elevations(lats, lons)
            for loc_id, elev in zip(ids, elevations):
                cur.execute("UPDATE locations SET elevation = %s WHERE id = %s", (elev, loc_id))
            conn.commit()
            updated += len(batch)
            log.info("  [%d/%d] updated", updated, len(locations))
        except Exception as exc:
            log.warning("  Batch %d failed: %s", i // BATCH_SIZE, exc)
            conn.rollback()

        time.sleep(10.0)

    log.info("Done. %d locations updated.", updated)
    conn.close()


if __name__ == "__main__":
    main()
