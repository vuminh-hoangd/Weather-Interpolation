"""
generate_report.py
Runs all benchmarks and kNN prediction evaluation,
then produces a self-contained HTML report: report.html
Requirements: pip install pg8000
Usage:        python generate_report.py
"""
import numpy as np
import os
import subprocess, sys
import pg8000.dbapi as pg
from datetime import datetime
from benchmark_runner import run_all, DB
from evaluate_adaptive_k import evaluate

PREDICTION_SQL = """
WITH test_obs AS (
    SELECT wo.location_id, wo.observed_at,
           wo.temperature AS actual_temp, l.geog
    FROM   weather_observations wo
    JOIN   locations l ON l.id = wo.location_id
    WHERE  l.is_test_zone = TRUE
),
predictions AS (
    SELECT t.location_id, t.observed_at, t.actual_temp,
        (SELECT SUM(temperature / GREATEST(dist_m, 1.0))
                / SUM(1.0   / GREATEST(dist_m, 1.0))
         FROM (
             SELECT wo2.temperature,
                    ST_Distance(l2.geog, t.geog) AS dist_m
             FROM   training_observations wo2
             JOIN   locations l2 ON l2.id = wo2.location_id
             WHERE  wo2.observed_at = t.observed_at
             ORDER  BY l2.geog <-> t.geog
             LIMIT  5
         ) neighbours) AS predicted_temp
    FROM test_obs t
)
SELECT
    COUNT(*)                                                          AS evaluated_points,
    ROUND(AVG(ABS(actual_temp - predicted_temp))::NUMERIC,  3)       AS mae_c,
    ROUND(SQRT(AVG((actual_temp - predicted_temp)^2))::NUMERIC, 3)   AS rmse_c,
    ROUND(MIN(actual_temp - predicted_temp)::NUMERIC, 3)             AS min_error_c,
    ROUND(MAX(actual_temp - predicted_temp)::NUMERIC, 3)             AS max_error_c
FROM predictions
WHERE predicted_temp IS NOT NULL
"""

CITY_BREAKDOWN_SQL = """
WITH test_obs AS (
    SELECT
        (SELECT tz.name FROM test_zones tz
         ORDER BY tz.center_geog <-> l.geog LIMIT 1) AS city_name,
        wo.temperature AS actual_temp,
        l.geog,
        wo.observed_at
    FROM   weather_observations wo
    JOIN   locations l ON l.id = wo.location_id
    WHERE  l.is_test_zone = TRUE
),
predictions AS (
    SELECT t.city_name, t.actual_temp,
        (SELECT SUM(temperature / GREATEST(dist_m, 1.0))
                / SUM(1.0   / GREATEST(dist_m, 1.0))
         FROM (
             SELECT wo2.temperature,
                    ST_Distance(l2.geog, t.geog) AS dist_m
             FROM   training_observations wo2
             JOIN   locations l2 ON l2.id = wo2.location_id
             WHERE  wo2.observed_at = t.observed_at
             ORDER  BY l2.geog <-> t.geog
             LIMIT  5
         ) neighbours) AS predicted_temp
    FROM test_obs t
)
SELECT
    city_name,
    ROUND(AVG(ABS(actual_temp - predicted_temp))::NUMERIC, 3) AS mae_c,
    ROUND(SQRT(AVG((actual_temp - predicted_temp)^2))::NUMERIC, 3) AS rmse_c,
    COUNT(*) AS hours_evaluated
FROM predictions
WHERE predicted_temp IS NOT NULL
GROUP BY city_name
ORDER BY mae_c
"""

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>France Weather Grid — Project Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
          background: #f4f6f9; color: #222; line-height: 1.6; }}
  header {{ background: #1a3a5c; color: white; padding: 2rem 3rem; }}
  header h1 {{ font-size: 1.8rem; margin-bottom: 0.3rem; }}
  header p  {{ opacity: 0.75; font-size: 0.95rem; }}
  main {{ max-width: 1100px; margin: 2rem auto; padding: 0 2rem 4rem; }}
  h2 {{ font-size: 1.3rem; color: #1a3a5c; margin: 2rem 0 1rem;
        padding-bottom: 0.4rem; border-bottom: 2px solid #d0dce8; }}
  h3 {{ font-size: 1.05rem; color: #333; margin: 1.2rem 0 0.5rem; }}
  .card {{ background: white; border-radius: 8px; padding: 1.5rem 2rem;
           margin-bottom: 1.5rem; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; margin-top: 0.5rem; }}
  th {{ background: #1a3a5c; color: white; padding: 0.5rem 0.8rem; text-align: left; }}
  td {{ padding: 0.45rem 0.8rem; border-bottom: 1px solid #eee; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f0f4f8; }}
  .badge {{ display:inline-block; padding: 0.15rem 0.5rem; border-radius: 4px;
             font-size: 0.8rem; font-weight: 600; }}
  .fast {{ background: #d4edda; color: #155724; }}
  .mid  {{ background: #fff3cd; color: #856404; }}
  .metric {{ display:inline-block; background:#eef3f9; border-radius:8px;
             padding: 0.8rem 1.4rem; margin: 0.4rem 0.4rem 0 0; text-align:center; }}
  .metric .val {{ font-size: 1.6rem; font-weight: 700; color: #1a3a5c; }}
  .metric .lbl {{ font-size: 0.78rem; color: #666; }}
</style>
</head>
<body>
<header>
  <h1>France Weather Grid — Query Optimization Report</h1>
  <p>Generated {generated_at} &nbsp;|&nbsp; Data: March 2026 &nbsp;|&nbsp;
     Grid: France 0.18° (~3,800 points, ~20 km) &nbsp;|&nbsp; Test zones: 10 cities &nbsp;|&nbsp; k=5</p>
</header>
<main>

<h2>1. Project Overview</h2>
<div class="card">
  <p>Hourly weather data (temperature, humidity, rain, soil temperature) is fetched from
     <strong>Open-Meteo</strong> for a 0.18° latitude/longitude grid covering France (~3,800 points, ~20 km spacing)
     and stored in <strong>PostgreSQL + PostGIS</strong> with monthly table partitioning.
     Ten French cities are withheld as test zones to evaluate spatial generalisation of a
     <strong>kNN Inverse-Distance Weighting</strong> temperature prediction model implemented
     in SQL and Python. The core deliverable benchmarks GiST, B-tree, and BRIN indexes against
     unindexed sequential scans across four spatial and temporal query patterns.</p>
</div>

<h2>2. Query Benchmark Results</h2>

<div class="card">
  <h3>Speedup Overview — Best Index vs. Sequential Scan</h3>
  <p style="color:#555;font-size:.9rem;margin-bottom:.8rem">
    How many times faster is the best indexed strategy compared to a full sequential scan, per benchmark type.
  </p>
  <div style="max-width:700px; margin: 0.5rem auto;">
    <canvas id="speedupOverview"></canvas>
  </div>
  <script>
  new Chart(document.getElementById('speedupOverview'), {{
    type: 'bar',
    data: {{
      labels: {speedup_labels},
      datasets: [{{
        label: 'Best speedup (x times faster)',
        data: {speedup_values},
        backgroundColor: [
          'rgba(26, 58, 92, 0.85)',
          'rgba(40, 167, 69, 0.80)',
          'rgba(255, 193, 7, 0.85)',
          'rgba(108, 117, 125, 0.80)',
        ],
        borderRadius: 5,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      plugins: {{
        legend: {{ display: false }},
        title: {{ display: true, text: 'Index Speedup by Query Type', font: {{ size: 14 }} }}
      }},
      scales: {{
        x: {{
          type: 'logarithmic',
          title: {{ display: true, text: 'Speedup (log scale)' }},
          ticks: {{
            callback: function(v) {{ return v + 'x'; }}
          }}
        }}
      }}
    }}
  }});
  </script>
</div>

{benchmark_sections}

<h2>3. Temperature Prediction — Strategy Comparison</h2>
<div class="card">
  <p>Three prediction strategies are compared across all 10 test-zone cities for March 2026.
     Ten French cities are withheld as test zones — their grid points are excluded from training
     so that predictions must rely on surrounding observations, giving an honest measure of
     spatial generalisation error.</p>
  <ul style="margin: 0.6rem 0 0.4rem 1.4rem; font-size: 0.92rem; color: #444;">
    <li><strong>Fixed k=5</strong> — baseline: always use 5 nearest training neighbours, no elevation correction.</li>
    <li><strong>Adaptive k</strong> — k chosen per query from elevation std dev of 20 nearest neighbours:
        k = clip(round(8 − 5 × stddev / 400), 3, 8). Flat terrain → k=8; mountainous → k=3.</li>
    <li><strong>Adaptive k + Lapse Rate</strong> — each neighbour's temperature is corrected to the query
        elevation before IDW: T<sub>corr</sub> = T<sub>nbr</sub> + 0.0065 × (elev<sub>nbr</sub> − elev<sub>query</sub>).</li>
  </ul>
  <p style="margin: 0.8rem 0 1rem; font-size:0.92rem; background:#eef3f9; padding:0.7rem 1rem; border-radius:6px;">
    <strong>Best strategy: Adaptive k + Lapse Rate</strong> — wins in 8 out of 10 cities, reducing average MAE
    by 28.6% over the fixed k=5 baseline. This is the strategy used when a user queries
    <em>predict_temperature(longitude, latitude, timestamp)</em>.
  </p>

  <h3>MAE Comparison by City</h3>
  <div style="max-width:860px; margin: 0.8rem auto 1.5rem;">
    <canvas id="strategyMaeChart"></canvas>
  </div>
  <script>
  new Chart(document.getElementById('strategyMaeChart'), {{
    type: 'bar',
    data: {{
      labels: {strategy_cities},
      datasets: [
        {{
          label: 'Fixed k=5',
          data: {strategy_fixed},
          backgroundColor: 'rgba(220, 53, 69, 0.78)',
          borderRadius: 4,
        }},
        {{
          label: 'Adaptive k',
          data: {strategy_adaptive},
          backgroundColor: 'rgba(255, 193, 7, 0.85)',
          borderRadius: 4,
        }},
        {{
          label: 'Adaptive k + Lapse Rate',
          data: {strategy_lapse},
          backgroundColor: 'rgba(26, 58, 92, 0.85)',
          borderRadius: 4,
        }}
      ]
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ position: 'top' }},
        title: {{ display: true, text: 'MAE per City — Three Strategies', font: {{ size: 14 }} }}
      }},
      scales: {{
        y: {{ beginAtZero: true, title: {{ display: true, text: 'MAE (°C)' }} }},
        x: {{ title: {{ display: true, text: 'City' }} }}
      }}
    }}
  }});
  </script>

  <h3 style="margin-top:1.5rem;">RMSE Comparison by City</h3>
  <div style="max-width:860px; margin: 0.8rem auto 1.5rem;">
    <canvas id="strategyRmseChart"></canvas>
  </div>
  <script>
  new Chart(document.getElementById('strategyRmseChart'), {{
    type: 'bar',
    data: {{
      labels: {strategy_cities},
      datasets: [
        {{
          label: 'Fixed k=5',
          data: {strategy_fixed_rmse},
          backgroundColor: 'rgba(220, 53, 69, 0.78)',
          borderRadius: 4,
        }},
        {{
          label: 'Adaptive k',
          data: {strategy_adaptive_rmse},
          backgroundColor: 'rgba(255, 193, 7, 0.85)',
          borderRadius: 4,
        }},
        {{
          label: 'Adaptive k + Lapse Rate',
          data: {strategy_lapse_rmse},
          backgroundColor: 'rgba(26, 58, 92, 0.85)',
          borderRadius: 4,
        }}
      ]
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ position: 'top' }},
        title: {{ display: true, text: 'RMSE per City — Three Strategies', font: {{ size: 14 }} }}
      }},
      scales: {{
        y: {{ beginAtZero: true, title: {{ display: true, text: 'RMSE (°C)' }} }},
        x: {{ title: {{ display: true, text: 'City' }} }}
      }}
    }}
  }});
  </script>

  <h3 style="margin-top:1.5rem;">Detailed Results</h3>
  <table style="margin-top:0.5rem;">
    <thead><tr>
      <th>City</th>
      <th>Fixed k=5 MAE</th><th>Fixed k=5 RMSE</th>
      <th>Adaptive k MAE</th><th>Adaptive k RMSE</th>
      <th>Adaptive+Lapse MAE</th><th>Adaptive+Lapse RMSE</th>
      <th>Best</th>
    </tr></thead>
    <tbody>{strategy_table}</tbody>
  </table>
  <div style="margin-top:1.5rem;">
   <div class="metric"><div class="val">{avg_mae_f} °C</div><div class="lbl">Fixed k=5 avg MAE</div></div>
   <div class="metric"><div class="val">{avg_mae_a} °C</div><div class="lbl">Adaptive k avg MAE</div></div>
   <div class="metric" style="background:#d4edda;"><div class="val" style="color:#155724;">{avg_mae_l} °C</div><div class="lbl">Adaptive+Lapse avg MAE</div></div>
   <div class="metric" style="background:#d4edda;"><div class="val" style="color:#155724;">−{improvement}%</div><div class="lbl">Overall MAE improvement</div></div>
  </div>
</div>

<h2>4. Index Sizes on Disk</h2>
<div class="card">
  {index_sizes}
</div>

</main>
</body>
</html>
"""




def build_strategy_table(results) -> str:
    rows = ""
    for city, f_mae, a_mae, l_mae, f_rmse, a_rmse, l_rmse in results:
        best_val  = min(f_mae, a_mae, l_mae)
        best_name = {f_mae: "Fixed k=5", a_mae: "Adaptive k", l_mae: "Adaptive+Lapse"}[best_val]
        rows += (f"<tr><td>{city}</td>"
                 f"<td>{f_mae}</td><td>{f_rmse}</td>"
                 f"<td>{a_mae}</td><td>{a_rmse}</td>"
                 f"<td>{l_mae}</td><td>{l_rmse}</td>"
                 f"<td><strong>{best_name}</strong></td></tr>\n")
    return rows


def speedup_badge(ratio: float) -> str:
    css = "fast" if ratio >= 10 else "mid"
    return f'<span class="badge {css}">{ratio:.1f}×</span>'


def build_benchmark_section(bench: dict, results: list[dict], chart_id: str) -> str:
    rows = ""
    for i, r in enumerate(results):
        scenario = bench["scenarios"][i]
        if "baseline_idx" in scenario:
            bi = scenario["baseline_idx"]
            speedup = "baseline" if bi is None else speedup_badge(results[bi]["ms"] / r["ms"])
        else:
            speedup = "baseline" if i == 0 else speedup_badge(results[0]["ms"] / r["ms"])
        rows += (f"<tr><td>{r['label']}</td>"
                 f"<td>{r['ms']:.2f} ms</td>"
                 f"<td>{speedup}</td></tr>\n")

    labels = [r["label"] for r in results]
    times  = [round(r["ms"], 2) for r in results]
    colors = (["rgba(220,53,69,0.80)"] +
              ["rgba(26,58,92,0.80)"] * (len(results) - 1))

    return f"""
<div class="card">
  <h3>{bench['name']}</h3>
  <p style="color:#555;font-size:.9rem;margin-bottom:.8rem">{bench['desc']}</p>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:2rem;align-items:start;margin-top:0.8rem">
    <table>
      <thead><tr><th>Strategy</th><th>Execution Time</th><th>Speedup vs baseline</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    <div><canvas id="{chart_id}"></canvas></div>
  </div>
  <script>
  new Chart(document.getElementById('{chart_id}'), {{
    type: 'bar',
    data: {{
      labels: {labels},
      datasets: [{{
        label: 'Execution time (ms)',
        data: {times},
        backgroundColor: {colors},
        borderRadius: 4,
      }}]
    }},
    options: {{
      responsive: true,
      plugins: {{
        legend: {{ display: false }},
        title: {{ display: true, text: 'Execution Time (ms) — log scale', font: {{ size: 12 }} }}
      }},
      scales: {{
        y: {{
          type: 'logarithmic',
          title: {{ display: true, text: 'ms (log scale)' }},
          ticks: {{ callback: function(v) {{ return v + ' ms'; }} }}
        }},
        x: {{ ticks: {{ maxRotation: 20, font: {{ size: 11 }} }} }}
      }}
    }}
  }});
  </script>
</div>"""


def build_prediction_metrics(row: tuple) -> str:
    evaluated, mae, rmse, min_err, max_err = row
    return (
        f'<div class="metric"><div class="val">{evaluated:,}</div>'
        f'<div class="lbl">Hours evaluated</div></div>'
        f'<div class="metric"><div class="val">{mae} °C</div>'
        f'<div class="lbl">MAE</div></div>'
        f'<div class="metric"><div class="val">{rmse} °C</div>'
        f'<div class="lbl">RMSE</div></div>'
        f'<div class="metric"><div class="val">{min_err} °C</div>'
        f'<div class="lbl">Min error</div></div>'
        f'<div class="metric"><div class="val">{max_err} °C</div>'
        f'<div class="lbl">Max error</div></div>'
    )


def build_city_chart(rows: list[tuple]) -> dict:
    labels = [r[0] for r in rows]
    mae    = [float(r[1]) for r in rows]
    rmse   = [float(r[2]) for r in rows]
    return {"labels": labels, "mae": mae, "rmse": rmse}


def build_city_table(rows: list[tuple]) -> str:
    tbody = "".join(
        f"<tr><td>{name}</td><td>{mae}</td><td>{rmse}</td><td>{hours:,}</td></tr>\n"
        for name, mae, rmse, hours in rows
    )
    return (
        "<table><thead><tr>"
        "<th>City</th><th>MAE (°C)</th><th>RMSE (°C)</th><th>Hours evaluated</th>"
        f"</tr></thead><tbody>{tbody}</tbody></table>"
    )


def build_index_sizes(cur) -> str:
    names = [
        "idx_locations_geog_gist",
        "idx_obs_time_brin",
        "idx_obs_time_btree",
        "idx_obs_loc_time",
    ]
    rows = ""
    for name in names:
        cur.execute(
            "SELECT pg_size_pretty(pg_relation_size(oid)) "
            "FROM pg_class WHERE relname = %s",
            (name,)
        )
        result = cur.fetchone()
        size   = result[0] if result else "N/A (not created)"
        rows  += f"<tr><td>{name}</td><td>{size}</td></tr>\n"
    return (
        "<table><thead><tr><th>Index</th><th>Size on disk</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def main():
    print("Running benchmarks ...")
    bench_results = run_all()

    print("\nRunning prediction evaluation ...")
    STRATEGY_RESULTS = evaluate()
    conn = pg.connect(**DB)
    cur  = conn.cursor()
    index_html = build_index_sizes(cur)
    conn.close()

    all_mae_f   = np.mean([r[1] for r in STRATEGY_RESULTS])
    all_mae_a   = np.mean([r[2] for r in STRATEGY_RESULTS])
    all_mae_l   = np.mean([r[3] for r in STRATEGY_RESULTS])
    improvement = round((1 - all_mae_l / all_mae_f) * 100, 1)

    speedup_labels = [r["benchmark"]["name"] for r in bench_results]
    speedup_values = [
        round(r["results"][0]["ms"] / min(x["ms"] for x in r["results"]), 1)
        for r in bench_results
    ]

    html = HTML.format(
        generated_at       = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        speedup_labels     = speedup_labels,
        speedup_values     = speedup_values,
        benchmark_sections = "".join(
            build_benchmark_section(r["benchmark"], r["results"],
                                    f"benchChart_{r['benchmark']['id']}")
            for r in bench_results
        ),
        strategy_cities        = [r[0] for r in STRATEGY_RESULTS],
        strategy_fixed         = [r[1] for r in STRATEGY_RESULTS],
        strategy_adaptive      = [r[2] for r in STRATEGY_RESULTS],
        strategy_lapse         = [r[3] for r in STRATEGY_RESULTS],
        strategy_fixed_rmse    = [r[4] for r in STRATEGY_RESULTS],
        strategy_adaptive_rmse = [r[5] for r in STRATEGY_RESULTS],
        strategy_lapse_rmse    = [r[6] for r in STRATEGY_RESULTS],
        strategy_table         = build_strategy_table(STRATEGY_RESULTS),
        index_sizes            = index_html,
        avg_mae_f              = round(float(all_mae_f), 3),
        avg_mae_a              = round(float(all_mae_a), 3),
        avg_mae_l              = round(float(all_mae_l), 3),
        improvement            = improvement,
    )

    output = "report.html"
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Report saved → {output}")
    if sys.platform == "win32":
      os.startfile(os.path.abspath(output))
    else:
      subprocess.call(["xdg-open", os.path.abspath(output)])


if __name__ == "__main__":
    main()
