# France Weather Grid — Query Optimisation Project

## Authors: Hoang Dung Vu Minh and Christopher Won

Hourly weather data (temperature, humidity, rain, soil temperature) is fetched from
**Open-Meteo** for a 0.18° latitude/longitude grid covering France (~3,800 points,
~20 km spacing) and stored in **PostgreSQL + PostGIS** with monthly table partitioning.

The core deliverable benchmarks GiST, B-tree, and BRIN indexes against unindexed
sequential scans across four spatial and temporal query patterns, and evaluates an
**adaptive kNN + lapse-rate temperature prediction** model implemented in SQL and Python.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Python environment](#2-python-environment)
3. [Database setup](#3-database-setup)
4. [Ingest data](#4-ingest-data)
5. [Fetch elevations](#5-fetch-elevations)
6. [Run benchmarks & generate report](#6-run-benchmarks--generate-report)
7. [Evaluate prediction strategies](#7-evaluate-prediction-strategies)
8. [Query a temperature prediction](#8-query-a-temperature-prediction)
9. [File reference](#9-file-reference)

---

## 1. Prerequisites

| Requirement | Version tested | Notes |
|---|---|---|
| PostgreSQL | 15 or 16 | Must include the `psql` CLI |
| PostGIS extension | 3.x | Install via Stack Builder or `apt install postgis` |
| Python | 3.11 or 3.12 | |
| Internet access | — | Open-Meteo APIs (weather + elevation) |

### Install PostGIS (if not already installed)

- **Windows**: run the PostgreSQL installer → Stack Builder → Spatial Extensions → PostGIS
- **macOS**: `brew install postgis`
- **Ubuntu/Debian**: `sudo apt install postgresql-15-postgis-3`

---

## 2. Python environment

```bash
# Create and activate a virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

# Install dependencies
pip install pg8000 requests schedule numpy
```

---

## 3. Database setup

All SQL files are in the `sql/` directory and must be run **in order**.

### 3a. Create the database

Open `psql` as the postgres superuser and run:

```sql
CREATE DATABASE imperial_db;
```

Then connect to it:

```bash
psql -U postgres -d imperial_db
```

### 3b. Run the SQL setup scripts

From inside `psql` (or using the `-f` flag), execute in this order:

```bash
psql -U postgres -d imperial_db -f sql/01_schema.sql
psql -U postgres -d imperial_db -f sql/04_monthly_partition.sql
psql -U postgres -d imperial_db -f sql/02_indexes.sql
psql -U postgres -d imperial_db -f sql/03_seed_france.sql
psql -U postgres -d imperial_db -f sql/04_predict_function.sql
```

What each file does:

| File | Purpose |
|---|---|
| `01_schema.sql` | Creates `locations`, `weather_observations` (partitioned), `test_zones`, and the `training_observations` view |
| `04_monthly_partition.sql` | Creates the March 2026 (and April 2026) monthly partitions |
| `02_indexes.sql` | Creates the GiST, BRIN, B-tree, and composite indexes |
| `03_seed_france.sql` | Inserts the ~3,800 France grid points and marks the 10 test-zone cities |
| `04_predict_function.sql` | Creates the `predict_temperature()` stored function |

### 3c. Database credentials

All Python scripts connect using these defaults (edit the `DB` dict at the top of
`ingest.py` if your setup differs):

```python
DB = dict(host="localhost", port=5432, database="imperial_db",
          user="postgres", password="Imperial")
```

---

## 4. Ingest data

This fetches hourly weather data from the Open-Meteo Archive API for the full
France grid and inserts it into PostgreSQL.

```bash
# Backfill one full month (run once — takes ~20–40 minutes for March 2026)
python ingest.py backfill 2026-03
```

> **Note**: The script fetches one grid point at a time with a short delay between
> requests to respect the Open-Meteo rate limit. A progress log is printed to the
> console and written to `backfill_resume.log`. If interrupted, re-run the same
> command — it skips grid points that already have data.

---

## 5. Fetch elevations

Populates the `elevation` column on the `locations` table using the
Open-Meteo Elevation API. Required for the adaptive kNN + lapse-rate prediction.

```bash
python fetch_elevations.py
```

> Processes ~3,800 grid points in batches of 100 with a 3-second delay between
> batches. Takes ~2 minutes. Safe to re-run — skips rows that already have an
> elevation value.

---

## 6. Run benchmarks & generate report

Runs all four benchmark suites (kNN spatial, radius, bounding box, time-range),
then writes a self-contained `report.html` that you can open in any browser.

```bash
python generate_report.py
```

The report opens automatically when done. If it does not open, open `report.html`
manually from the project folder.

To run the benchmarks alone (no HTML output):

```bash
python benchmark_runner.py
```

Raw EXPLAIN ANALYZE plans are saved to `benchmark_results/`.

---

## 7. Evaluate prediction strategies

Compares three temperature prediction strategies across all 10 test-zone cities
for every hour of March 2026 and prints a MAE / RMSE summary table.

```bash
python evaluate_adaptive_k.py
```

The three strategies compared are:

| Strategy | Description |
|---|---|
| Fixed k=5 | Always use 5 nearest training neighbours |
| Adaptive k | k chosen per query from local elevation std dev |
| Adaptive k + Lapse Rate | Lapse-rate temperature correction applied before IDW |

---

## 8. Query a temperature prediction

### From the terminal (Python)

```bash
python predict.py <longitude> <latitude> "YYYY-MM-DD HH:00"
```

Examples:

```bash
python predict.py 2.352 48.857 "2026-03-15 12:00"   # Paris
python predict.py 5.724 45.188 "2026-03-15 12:00"   # Grenoble
python predict.py -0.579 44.838 "2026-03-10 06:00"  # Bordeaux
```

> Fetches the query point's elevation from the Open-Meteo Elevation API, then
> runs the adaptive kNN + lapse-rate pipeline against the database.

### From psql (SQL function)

```sql
-- Automatic elevation lookup (nearest grid point used as proxy)
SELECT * FROM predict_temperature(2.352, 48.857, '2026-03-15 12:00+00');

-- Supply elevation explicitly (metres)
SELECT * FROM predict_temperature(5.724, 45.188, '2026-03-15 12:00+00', 212);
```

**Output columns**: `predicted_temp_c`, `k_used`, `query_elevation_m`,
`elev_stddev_m`, `neighbours_found`.

| Column | Meaning |
|---|---|
| `predicted_temp_c` | Predicted temperature in °C |
| `k_used` | Number of neighbours used (3–8, chosen adaptively from elevation std dev) |
| `query_elevation_m` | Elevation used for lapse-rate correction (supplied or proxied) |
| `elev_stddev_m` | Elevation std dev of the 20 nearest grid points (drives the choice of k) |
| `neighbours_found` | Number of training neighbours actually found at that timestamp 

> **Data availability**: predictions are only possible for timestamps that have
> been ingested. Only March 2026 data is available after following step 4.

---

## 9. File reference

```
weather-db/
├── sql/
│   ├── 01_schema.sql              Core tables and training view
│   ├── 02_indexes.sql             GiST, BRIN, B-tree, composite indexes
│   ├── 03_seed_france.sql         France grid + test zones
│   ├── 04_monthly_partition.sql   Monthly partition definitions
│   └── 04_predict_function.sql    predict_temperature() stored function
│
├── ingest.py                      Fetch weather data from Open-Meteo → DB
├── fetch_elevations.py            Fetch elevations from Open-Meteo → DB
├── benchmark_runner.py            Run EXPLAIN ANALYZE benchmarks
├── generate_report.py             Run benchmarks + write report.html
├── evaluate_adaptive_k.py         MAE/RMSE comparison of three strategies
├── predict.py                     CLI temperature prediction tool
├── visualize.py                   France grid map visualisation
├── migrate_grid.py                Grid migration utility
└── report.html                    Generated HTML report (auto-created)
```
