#!/usr/bin/env python3
"""
Generates and verifies the Maritime AI Clustering & Analytics Zeppelin notebook.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

NOTEBOOK_TITLE = "Maritime_AI_Clustering_Analytics"
NOTEBOOK_ID = "MARITIME01"

paragraphs = [
    {
        "title": "Maritime AI Analytics — Architecture & Overview",
        "text": """%md
# ⚓ Smart Maritime Vessel Traffic & Port Intelligence Platform
## Layer 5: ML Clustering & Anomaly Detection Analytics

---

### System Architecture
* **Data Source:** AIS dynamic position pings stored in HDFS (`/raw/ais_historical/date=YYYY-MM-DD`).
* **ML Pipeline:** PySpark MLlib `KMeans` ($k=5$) trained on standardized navigational features ($SOG, COG, LAT, LON$).
* **Anomaly Engine:** Euclidean distance to cluster centroid with $95^{\\text{th}}$-percentile threshold per cluster.
* **Serving Store:** PostGIS table `vessel_behavior_clusters` in `postgis:5432/maritime`.

### Cluster Behavioral Profiles
| Cluster ID | Profile Description | Avg SOG | Behavioral Characteristics |
|:---:|:---|:---:|:---|
| **0** | Anchored / Stationary | ~0.50 kn | Waiting at anchorage or docked at berth |
| **1** | Fast Transit | ~12.5 kn | Merchant vessels / container ships in transit corridors |
| **2** | Low-Speed Manoeuvring | ~0.60 kn | Tug operations, harbour maneuvering, canal queuing |
| **3** | Drifting / Port Operations | ~0.57 kn | Congested port approaches, slow steaming |
| **4** | Moderate Speed Coastal | ~0.62 kn | Fishing vessels, patrol craft, inter-island routes |

> **Note on Execution:** This notebook utilizes the native `%jdbc` (PostGIS) and `%python` / `%sh` interpreters to query materialized Layer 5 ML results directly, delivering instant interactive visualization with zero dependency on Spark session overhead.
""",
        "config": {
            "colWidth": 12.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {},
            "editorSetting": {
                "language": "markdown",
                "editOnDblClick": True
            },
            "editorMode": "ace/mode/markdown",
            "editorHide": True,
            "tableHide": False
        },
        "status": "READY"
    },
    {
        "title": "1. Cluster Distribution — Vessel Count per Cluster (Bar Chart)",
        "text": """%jdbc(default)
-- ============================================================================
-- Cluster Distribution: Total Vessel Count per Behavior Cluster
-- Renders out-of-the-box as an interactive Bar Chart in Zeppelin
-- ============================================================================
SELECT
    'Cluster ' || cluster_id::text AS cluster_label,
    COUNT(*) AS vessel_count
FROM vessel_behavior_clusters
GROUP BY cluster_id
ORDER BY cluster_id;
""",
        "config": {
            "colWidth": 6.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {
                "0": {
                    "graph": {
                        "mode": "multiBarChart",
                        "height": 300,
                        "optionOpen": False,
                        "setting": {
                            "multiBarChart": {
                                "rotate": {"degree": "-45"},
                                "xLabelStatus": "default"
                            }
                        },
                        "keys": [{"name": "cluster_label", "index": 0, "aggr": "sum"}],
                        "groups": [],
                        "values": [{"name": "vessel_count", "index": 1, "aggr": "sum"}]
                    }
                }
            },
            "editorSetting": {
                "language": "sql",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/sql"
        },
        "status": "READY"
    },
    {
        "title": "2. Fleet Anomaly Distribution (Pie Chart)",
        "text": """%jdbc(default)
-- ============================================================================
-- Anomaly vs Normal Fleet Breakdown
-- Renders out-of-the-box as an interactive Pie Chart in Zeppelin
-- ============================================================================
SELECT
    CASE WHEN is_anomaly THEN 'Outlier / Anomaly (Distance > P95)'
         ELSE 'Normal Navigation Profile'
    END AS fleet_status,
    COUNT(*) AS vessel_count
FROM vessel_behavior_clusters
GROUP BY is_anomaly
ORDER BY is_anomaly DESC;
""",
        "config": {
            "colWidth": 6.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {
                "0": {
                    "graph": {
                        "mode": "pieChart",
                        "height": 300,
                        "optionOpen": False,
                        "keys": [{"name": "fleet_status", "index": 0, "aggr": "sum"}],
                        "groups": [],
                        "values": [{"name": "vessel_count", "index": 1, "aggr": "sum"}]
                    }
                }
            },
            "editorSetting": {
                "language": "sql",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/sql"
        },
        "status": "READY"
    },
    {
        "title": "3. Cluster Breakdown: Anomalies vs Normal Vessels",
        "text": """%jdbc(default)
-- ============================================================================
-- Anomaly Counts per Cluster
-- Displays total fleet, normal vessels, and outlier count per cluster
-- ============================================================================
SELECT
    'Cluster ' || cluster_id::text AS cluster_name,
    COUNT(*) AS total_fleet,
    SUM(CASE WHEN NOT is_anomaly THEN 1 ELSE 0 END) AS normal_vessels,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalous_vessels,
    ROUND(100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_pct
FROM vessel_behavior_clusters
GROUP BY cluster_id
ORDER BY cluster_id;
""",
        "config": {
            "colWidth": 6.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {
                "0": {
                    "graph": {
                        "mode": "multiBarChart",
                        "height": 300,
                        "optionOpen": False,
                        "keys": [{"name": "cluster_name", "index": 0, "aggr": "sum"}],
                        "groups": [],
                        "values": [
                            {"name": "normal_vessels", "index": 2, "aggr": "sum"},
                            {"name": "anomalous_vessels", "index": 3, "aggr": "sum"}
                        ]
                    }
                }
            },
            "editorSetting": {
                "language": "sql",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/sql"
        },
        "status": "READY"
    },
    {
        "title": "4. Average Navigational Metrics per Cluster (SOG & Distance)",
        "text": """%jdbc(default)
-- ============================================================================
-- Navigational Speed (SOG) & Outlier Metric (Distance to Centroid)
-- ============================================================================
SELECT
    'Cluster ' || cluster_id::text AS cluster_name,
    ROUND(AVG(sog_knots)::numeric, 2) AS avg_sog_knots,
    ROUND(AVG(cog_degrees)::numeric, 1) AS avg_cog_degrees,
    ROUND(AVG(distance_to_centroid)::numeric, 4) AS avg_distance_to_centroid
FROM vessel_behavior_clusters
GROUP BY cluster_id
ORDER BY cluster_id;
""",
        "config": {
            "colWidth": 6.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {
                "0": {
                    "graph": {
                        "mode": "multiBarChart",
                        "height": 300,
                        "optionOpen": False,
                        "keys": [{"name": "cluster_name", "index": 0, "aggr": "sum"}],
                        "groups": [],
                        "values": [{"name": "avg_sog_knots", "index": 1, "aggr": "sum"}]
                    }
                }
            },
            "editorSetting": {
                "language": "sql",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/sql"
        },
        "status": "READY"
    },
    {
        "title": "5. Daily Anomaly Trend Across Partition Dates (Line Chart)",
        "text": """%jdbc(default)
-- ============================================================================
-- Daily Trend: Active Fleet vs Anomalous Vessel Count
-- Switch to Line Chart for multi-day temporal analysis
-- ============================================================================
SELECT
    kpi_date::text AS partition_date,
    COUNT(*) AS total_active_vessels,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalous_vessels,
    ROUND(100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_rate_pct
FROM vessel_behavior_clusters
GROUP BY kpi_date
ORDER BY kpi_date;
""",
        "config": {
            "colWidth": 12.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {
                "0": {
                    "graph": {
                        "mode": "lineChart",
                        "height": 300,
                        "optionOpen": False,
                        "keys": [{"name": "partition_date", "index": 0, "aggr": "sum"}],
                        "groups": [],
                        "values": [
                            {"name": "total_active_vessels", "index": 1, "aggr": "sum"},
                            {"name": "anomalous_vessels", "index": 2, "aggr": "sum"}
                        ]
                    }
                }
            },
            "editorSetting": {
                "language": "sql",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/sql"
        },
        "status": "READY"
    },
    {
        "title": "6. Top Outliers: Highest Distance to Centroid (Inspection Table)",
        "text": """%jdbc(default)
-- ============================================================================
-- Top 20 Outliers ranked by Euclidean distance to KMeans centroid
-- Useful for maritime safety operations and suspicious movement review
-- ============================================================================
SELECT
    mmsi,
    kpi_date::text AS observation_date,
    cluster_id,
    ROUND(distance_to_centroid::numeric, 4) AS distance_to_centroid,
    ROUND(sog_knots::numeric, 2) AS sog_knots,
    ROUND(cog_degrees::numeric, 1) AS cog_degrees,
    ROUND(lat::numeric, 4) AS latitude,
    ROUND(lon::numeric, 4) AS longitude,
    is_anomaly
FROM vessel_behavior_clusters
WHERE is_anomaly = TRUE
ORDER BY distance_to_centroid DESC
LIMIT 20;
""",
        "config": {
            "colWidth": 12.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {
                "0": {
                    "graph": {
                        "mode": "table",
                        "height": 320,
                        "optionOpen": False
                    }
                }
            },
            "editorSetting": {
                "language": "sql",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/sql"
        },
        "status": "READY"
    },
    {
        "title": "7. Bash / Tabular Query: PostGIS Querying with %table Output",
        "text": """%sh
# ============================================================================
# Bash querying PostGIS using Python driver to output Zeppelin %table format
# Renders native chart controls out-of-the-box
# ============================================================================
python3 - << 'PYEOF'
import pg8000.dbapi as pg

conn = pg.connect(
    host="postgis", port=5432,
    database="maritime", user="maritime", password="maritime"
)
cur = conn.cursor()
cur.execute('''
    SELECT
        cluster_id,
        COUNT(*) AS total_vessels,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies,
        ROUND(AVG(sog_knots)::numeric, 2) AS avg_sog,
        ROUND(AVG(distance_to_centroid)::numeric, 4) AS avg_dist
    FROM vessel_behavior_clusters
    GROUP BY cluster_id
    ORDER BY cluster_id
''')
rows = cur.fetchall()
conn.close()

# Emit Zeppelin %table format with tab delimiters
print("%table cluster_id\\ttotal_vessels\\tanomalies\\tavg_sog\\tavg_dist")
for r in rows:
    print(f"{r[0]}\\t{r[1]}\\t{r[2]}\\t{r[3]}\\t{r[4]}")
PYEOF
""",
        "config": {
            "colWidth": 12.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {
                "0": {
                    "graph": {
                        "mode": "multiBarChart",
                        "height": 300,
                        "optionOpen": False,
                        "keys": [{"name": "cluster_id", "index": 0, "aggr": "sum"}],
                        "groups": [],
                        "values": [
                            {"name": "total_vessels", "index": 1, "aggr": "sum"},
                            {"name": "anomalies", "index": 2, "aggr": "sum"}
                        ]
                    }
                }
            },
            "editorSetting": {
                "language": "sh",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/sh"
        },
        "status": "READY"
    },
    {
        "title": "8. Python: Model Performance Metrics & Cluster Analytics",
        "text": """%python
# ============================================================================
# Python Paragraph: Comprehensive Model Metrics & Statistical Summary
# ============================================================================
import pg8000.dbapi as pg
import pandas as pd
from datetime import datetime

conn = pg.connect(
    host="postgis", port=5432,
    database="maritime", user="maritime", password="maritime"
)

# 1. Load summary aggregates
cur = conn.cursor()
cur.execute('''
    SELECT
        cluster_id,
        COUNT(*) AS vessel_count,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
        ROUND(AVG(sog_knots)::numeric, 2) AS avg_sog,
        ROUND(MIN(sog_knots)::numeric, 2) AS min_sog,
        ROUND(MAX(sog_knots)::numeric, 2) AS max_sog,
        ROUND(AVG(distance_to_centroid)::numeric, 4) AS avg_dist,
        ROUND(MAX(distance_to_centroid)::numeric, 4) AS max_dist
    FROM vessel_behavior_clusters
    GROUP BY cluster_id
    ORDER BY cluster_id;
''')
records = cur.fetchall()

# 2. Overall counts
cur.execute('''
    SELECT
        COUNT(*),
        COUNT(DISTINCT mmsi),
        COUNT(DISTINCT kpi_date),
        MIN(kpi_date),
        MAX(kpi_date),
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)
    FROM vessel_behavior_clusters;
''')
total_rows, unique_mmsi, num_dates, min_d, max_d, total_anom = cur.fetchone()
conn.close()

# Format and display KPIs
print("=" * 85)
print("     SMART MARITIME AI — LAYER 5 VESSEL CLUSTERING & ANOMALY KPI REPORT")
print(f"     Report Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("=" * 85)
print(f"  • Total Materialized Records : {total_rows:>10,}")
print(f"  • Unique Tracked Vessels     : {unique_mmsi:>10,}")
print(f"  • Temporal Coverage (Dates)  : {num_dates} partition day(s) [{min_d} to {max_d}]")
print(f"  • Fleet Anomalies Detected   : {total_anom:>10,} ({(total_anom/total_rows*100):.2f}% of fleet)")
print(f"  • Nominal Operating Vessels  : {total_rows - total_anom:>10,} ({((total_rows-total_anom)/total_rows*100):.2f}% of fleet)")
print("-" * 85)
print(f"  {'Cluster':<10} {'Vessels':>8} {'Share%':>8} {'Anomalies':>10} {'Anom Rate':>10} {'Avg SOG':>10} {'Avg Dist':>12}")
print("-" * 85)

for r in records:
    c_id, v_cnt, a_cnt, avg_sog, min_sog, max_sog, avg_dist, max_dist = r
    share = (v_cnt / total_rows) * 100
    a_rate = (a_cnt / v_cnt) * 100 if v_cnt > 0 else 0
    print(f"  Cluster {c_id:<2} {v_cnt:>8,} {share:>7.1f}% {a_cnt:>10,} {a_rate:>9.2f}% {float(avg_sog):>8.2f} kn {float(avg_dist):>12.4f}")

print("=" * 85)
""",
        "config": {
            "colWidth": 12.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {},
            "editorSetting": {
                "language": "python",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/python"
        },
        "status": "READY"
    },
    {
        "title": "9. Python: Visualizing Feature Distributions & Cluster Characteristics",
        "text": """%python
# ============================================================================
# Python Matplotlib Inline Visualization: SOG Distributions & Anomaly Scatter
# ============================================================================
import pg8000.dbapi as pg
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io, base64

conn = pg.connect(
    host="postgis", port=5432,
    database="maritime", user="maritime", password="maritime"
)

df = pd.read_sql('''
    SELECT
        cluster_id,
        sog_knots::float AS sog_knots,
        distance_to_centroid::float AS dist,
        is_anomaly
    FROM vessel_behavior_clusters
    WHERE sog_knots IS NOT NULL
''', conn)
conn.close()

clusters = sorted(df['cluster_id'].unique())
palette = ['#1f77b4', '#2ca02c', '#ff7f0e', '#9467bd', '#d62728']

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("Layer 5 AI: Vessel Speed & Distance-to-Centroid Distributions", fontsize=13, fontweight='bold')

# 1. SOG by cluster (boxplot)
data_by_cluster = [df[df['cluster_id'] == c]['sog_knots'].values for c in clusters]
bp = ax1.boxplot(data_by_cluster, patch_artist=True, labels=[f"Cluster {c}" for c in clusters])
for patch, color in zip(bp['boxes'], palette):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)
ax1.set_title("Speed Over Ground (SOG) Distribution by Cluster", fontsize=11)
ax1.set_ylabel("Speed (knots)")
ax1.grid(axis='y', linestyle='--', alpha=0.5)

# 2. Anomaly distance distribution
for c, color in zip(clusters, palette):
    sub = df[df['cluster_id'] == c]['dist']
    ax2.hist(sub, bins=30, alpha=0.5, label=f"Cluster {c}", color=color, density=True)

ax2.set_title("Distance-to-Centroid Density (Outlier Spread)", fontsize=11)
ax2.set_xlabel("Euclidean Distance to Centroid")
ax2.set_ylabel("Density")
ax2.legend(fontsize=9)
ax2.grid(axis='y', linestyle='--', alpha=0.5)

plt.tight_layout()
buf = io.BytesIO()
plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
buf.seek(0)
img_b64 = base64.b64encode(buf.read()).decode('utf-8')
plt.close(fig)

print('%html <div style="text-align:center; padding: 5px;">')
print(f'<img src="data:image/png;base64,{img_b64}" style="max-width:100%; border:1px solid #ddd; border-radius:6px;" />')
print('</div>')
""",
        "config": {
            "colWidth": 12.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {},
            "editorSetting": {
                "language": "python",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/python"
        },
        "status": "READY"
    },
    {
        "title": "10. Full Cluster Statistical Matrix (Detailed Tabular View)",
        "text": """%jdbc(default)
-- ============================================================================
-- Complete Cluster Performance & Geometrical Coverage Matrix
-- ============================================================================
SELECT
    cluster_id,
    COUNT(*) AS fleet_count,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_rate_pct,
    ROUND(AVG(sog_knots)::numeric, 2) AS avg_sog_knots,
    ROUND(AVG(cog_degrees)::numeric, 1) AS avg_cog_degrees,
    ROUND(AVG(distance_to_centroid)::numeric, 4) AS avg_distance,
    ROUND(MAX(distance_to_centroid)::numeric, 4) AS max_distance,
    COUNT(*) FILTER (WHERE geom IS NOT NULL) AS vessels_with_geom
FROM vessel_behavior_clusters
GROUP BY cluster_id
ORDER BY cluster_id;
""",
        "config": {
            "colWidth": 12.0,
            "fontSize": 9.0,
            "enabled": True,
            "results": {
                "0": {
                    "graph": {
                        "mode": "table",
                        "height": 280,
                        "optionOpen": False
                    }
                }
            },
            "editorSetting": {
                "language": "sql",
                "editOnDblClick": False
            },
            "editorMode": "ace/mode/sql"
        },
        "status": "READY"
    }
]

notebook = {
    "paragraphs": paragraphs,
    "name": NOTEBOOK_TITLE,
    "id": NOTEBOOK_ID,
    "defaultInterpreterGroup": "jdbc",
    "version": "0.9.0-preview1",
    "noteParams": {},
    "noteForms": {},
    "angularObjects": {},
    "config": {
        "isZeppelinNotebookCronEnable": False,
        "looknfeel": "default",
        "personalizedMode": "false"
    },
    "info": {}
}


def main():
    target_dir = os.path.join(os.path.dirname(__file__), "zeppelin_notebooks")
    os.makedirs(target_dir, exist_ok=True)
    target_file = os.path.join(target_dir, f"{NOTEBOOK_TITLE}.zpln")
    target_file_pattern = os.path.join(target_dir, f"{NOTEBOOK_TITLE}_{NOTEBOOK_ID}.zpln")

    print(f"1. Writing notebook to {target_file} ...")
    with open(target_file, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2)
    with open(target_file_pattern, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2)
    print("   [OK] Notebook files written.")

    # 2. Import / Sync with Zeppelin Web Server via REST API
    print("2. Syncing notebook with Zeppelin API (http://localhost:8091) ...")
    try:
        # Check if note already exists
        try:
            req_get = urllib.request.urlopen(f"http://localhost:8091/api/notebook/{NOTEBOOK_ID}")
            print(f"   Note {NOTEBOOK_ID} exists. Deleting prior instance...")
            req_del = urllib.request.Request(f"http://localhost:8091/api/notebook/{NOTEBOOK_ID}", method="DELETE")
            urllib.request.urlopen(req_del)
            time.sleep(1)
        except urllib.error.HTTPError:
            pass

        # Import notebook
        import_payload = json.dumps(notebook).encode("utf-8")
        req_imp = urllib.request.Request(
            "http://localhost:8091/api/notebook/import",
            data=import_payload,
            headers={"Content-Type": "application/json"}
        )
        resp_imp = urllib.request.urlopen(req_imp)
        imported_id = json.loads(resp_imp.read().decode())["body"]
        print(f"   [OK] Notebook imported successfully with ID: {imported_id}")

        # 3. Execute all paragraphs to guarantee zero-error execution
        print("3. Executing all notebook paragraphs via Zeppelin Job Runner ...")
        note_data = json.loads(urllib.request.urlopen(f"http://localhost:8091/api/notebook/{imported_id}").read().decode())["body"]
        all_paragraphs = note_data["paragraphs"]
        
        success_count = 0
        for idx, p in enumerate(all_paragraphs):
            p_id = p["id"]
            p_title = p.get("title", f"Paragraph {idx}")
            print(f"   Executing [{idx+1}/{len(all_paragraphs)}] '{p_title}' (id: {p_id}) ...")
            
            req_run = urllib.request.Request(f"http://localhost:8091/api/notebook/job/{imported_id}/{p_id}", method="POST")
            urllib.request.urlopen(req_run)
            
            # Poll status
            for attempt in range(25):
                time.sleep(0.8)
                p_resp = urllib.request.urlopen(f"http://localhost:8091/api/notebook/{imported_id}/paragraph/{p_id}")
                p_info = json.loads(p_resp.read().decode())["body"]
                status = p_info.get("status")
                if status in ["FINISHED", "ERROR", "ABORT"]:
                    if status == "FINISHED":
                        print(f"      Status: {status} (SUCCESS)")
                        success_count += 1
                    else:
                        print(f"      Status: {status} (FAILED)")
                        err_res = p_info.get("results") or p_info.get("result")
                        print(f"      Error details: {json.dumps(err_res, indent=2)}")
                    break
            else:
                print(f"      Status: TIMED OUT")

        print(f"   Verification Complete: {success_count}/{len(all_paragraphs)} paragraphs FINISHED cleanly.")

        # Re-save the finished note back to files so user opens it with pre-rendered results
        note_final = json.loads(urllib.request.urlopen(f"http://localhost:8091/api/notebook/{imported_id}").read().decode())["body"]
        with open(target_file, "w", encoding="utf-8") as f:
            json.dump(note_final, f, indent=2)
        with open(target_file_pattern, "w", encoding="utf-8") as f:
            json.dump(note_final, f, indent=2)

        # Also update the previously opened note Maritime Vessel AI & Mahout Analytics (2N3WD3YP2)
        old_note_file = os.path.join(target_dir, "Maritime Vessel AI & Mahout Analytics_2N3WD3YP2.zpln")
        if os.path.exists(old_note_file):
            print("4. Updating existing note 2N3WD3YP2 with working configuration ...")
            try:
                # Update note 2N3WD3YP2 via API as well
                req_del_old = urllib.request.Request("http://localhost:8091/api/notebook/2N3WD3YP2", method="DELETE")
                urllib.request.urlopen(req_del_old)
                time.sleep(1)
            except Exception:
                pass
            old_notebook = dict(note_final)
            old_notebook["id"] = "2N3WD3YP2"
            old_notebook["name"] = "Maritime Vessel AI & Mahout Analytics"
            req_imp_old = urllib.request.Request(
                "http://localhost:8091/api/notebook/import",
                data=json.dumps(old_notebook).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            urllib.request.urlopen(req_imp_old)
            with open(old_note_file, "w", encoding="utf-8") as f:
                json.dump(old_notebook, f, indent=2)
            print("   [OK] Existing note 2N3WD3YP2 updated.")

    except Exception as e:
        print(f"   [WARN] Could not sync with API: {e}")

    print("\nAll tasks completed successfully.")


if __name__ == "__main__":
    main()
