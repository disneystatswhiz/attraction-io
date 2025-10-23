# 🎢 ATTRACTION-IO — Fact Table Sync Branch

This branch powers the **EC2-ready, end-to-end wait-time pipeline** with a new Python-based fact-table refresh step (`src/fact_table/`) that runs automatically before modeling. It replaces legacy file-sync logic with a unified, incremental fact table built in Python and consumed by Julia for attraction-level forecasting.

---

## 🏁 Quick Start

### Local development
```bash
git clone https://github.com/<org>/attraction-io.git
cd attraction-io
git checkout fact_table_sync
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

### Run locally
# Linux / macOS / EC2
bash pipeline.sh
```

### Single-entity test
```bash
julia src/main_runner.jl AK85
```

---

## ⚙️  What’s New in `fact_table_sync`

| Area | Improvement |
|------|--------------|
| 🧩 **Step 0 – Fact-Table Refresh** | Four Python scripts (`report.py`, `update.py`, `latest.py`, `main.py`) now maintain a single incremental Parquet-based fact table and produce `latest_obs_report.csv`. |
| 🐍 **Python Bootstrap** | EC2’s `pipeline.sh` installs Python 3 + venv + minimal deps (`boto3`, `pandas`, `pyarrow`, `s3fs`) automatically. |
| 🚀 **Unified Runner** | `run_jobs.jl` now runs Step 0 (Python) → Step 1 (Julia setup) → Step 2 (forecast entities). |
| 🌿 **Branch Pinning** | The EC2 script can stay permanently on this branch (`fact_table_sync`) without affecting `main`. |
| 📈 **Performance Ready** | Designed for future parallel execution—`DATA_FACT` is read-only and thread-safe. |

---

## 🗂 Directory Highlights

```
src/
├── fact_table/           # 🐍 Python refresh step
│   ├── main.py
│   ├── report.py
│   ├── update.py
│   └── latest.py
├── main_setup.jl         # Julia setup and data prep
├── main_runner.jl        # Single-entity modeling
└── ...
scheduler/
└── run_jobs.jl           # Master job launcher (Step 0–2)
pipeline.sh               # EC2 boot script with Python bootstrap
```

---

## 🔄 Pipeline Flow

### Step 0 – Fact Table Refresh ( Python )
Runs automatically inside `run_jobs.jl`:
1. `report.py` identifies new raw wait-time files.  
2. `update.py` merges only new rows into the master Parquet.  
3. `latest.py` creates `latest_obs_report.csv` for Julia.  

### Step 1 – Julia Setup
`main_setup.jl` prepares working directories, metadata, and config.

### Step 2 – Model Runs
`run_jobs.jl` loops through all entities in `latest_obs_report.csv` and calls  
`run_entity(code)` from `main_runner.jl` to train and score forecasts.

### Outputs
- **Forecast files** → `output/<entity>/forecasts_<entity>.csv`  
- **Uploads** → `s3://touringplans_stats/stats_work/attraction-io/forecasts`  
- **Logs** → `logs/pipeline_*.log`

---

## ☁️ EC2 Automation

### Boot behavior
`/home/ubuntu/attraction-io/pipeline.sh`

1. Updates repo from the **fact_table_sync** branch.  
2. Ensures enough disk and rotates logs.  
3. Installs Python + venv + deps if needed.  
4. Bootstraps Julia packages (`Pkg.instantiate && precompile`).  
5. Launches `run_jobs.jl` with live stdout + 1-min heartbeat.  
6. Shuts down instance on success (override with `SHUTDOWN_ON_EXIT=0`).  

### Disable auto-run
Create an empty `.disable_on_boot` file in the repo root.

### Monitor logs
```bash
cd /home/ubuntu/attraction-io/logs
tail -f pipeline_latest.log
```

---

## 🧱 Tech Stack

- **Julia 1.10+** — DataFrames.jl, CSV.jl, TimeZones.jl  
- **Python 3.10+** — pandas, pyarrow, boto3, s3fs  
- **XGBoost.jl** (GPU enabled)  
- **AWS S3** for all I/O  
- **EC2** scheduled via cron (≈ 3 AM daily)

---

## 🧩 Dev Notes

- EC2 instance runs **only this branch** until merged to `main`.  
- Safe to merge back; only `pipeline.sh` differs materially.  
- Logs and outputs are idempotent—reruns won’t duplicate data.  
- Fact-table scripts are independent and can run locally with  
  ```bash
  cd src/fact_table
  python main.py
  ```

---

## ✨ Credits

Built and maintained by the TouringPlans Data Science Team — 2025
