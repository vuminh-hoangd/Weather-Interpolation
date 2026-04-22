"""
dedup_locations.py
Removes duplicate location rows (same lat/lon), keeping the one that has 
observations. Then adds a UNIQUE constraint on (lat, lon) to prevent future duplicates.
"""

import pg8000.dbapi as pg
from benchmark_runner import DB

conn = pg.connect(**DB)
cur  = conn.cursor()

cur.execute("SELECT COUNT(*) FROM locations")
before = cur.fetchone()[0]
print(f"Locations before: {before:,}")

cur.execute("""
    DELETE FROM locations
    WHERE id NOT IN (
        SELECT DISTINCT ON (lat, lon) id
        FROM   locations
        ORDER  BY lat, lon,
                  -- prefer the row that already has observations
                  (NOT EXISTS (
                      SELECT 1 FROM weather_observations wo
                      WHERE  wo.location_id = locations.id
                  )) ASC,
                  id ASC
    )
""")
deleted = cur.rowcount
conn.commit()
print(f"Deleted {deleted:,} duplicate rows.")

cur.execute("SELECT COUNT(*) FROM locations")
after = cur.fetchone()[0]
print(f"Locations after:  {after:,}")

cur.execute("""
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'locations_lat_lon_unique'
        ) THEN
            ALTER TABLE locations ADD CONSTRAINT locations_lat_lon_unique UNIQUE (lat, lon);
        END IF;
    END $$
""")
conn.commit()
print("UNIQUE constraint on (lat, lon) added.")


cur.execute("""
    SELECT is_test_zone, COUNT(*)
    FROM   locations
    GROUP  BY is_test_zone
    ORDER  BY is_test_zone
""")
for row in cur.fetchall():
    label = "test zone" if row[0] else "training "
    print(f"  {label}: {row[1]:,} locations")

cur.execute("""
    SELECT COUNT(*) FROM locations l
    WHERE NOT EXISTS (
        SELECT 1 FROM weather_observations wo WHERE wo.location_id = l.id LIMIT 1
    )
""")
print(f"  Locations needing backfill: {cur.fetchone()[0]:,}")

conn.close()
