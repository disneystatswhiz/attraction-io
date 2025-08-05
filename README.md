# 🎢 EC2 Master Wait Time Pipeline

This repository contains the full EC2-ready data pipeline for attraction wait time modeling, forecast logging, crowd calendar generation, and reporting.

---

## 📦 Overview

This project supports a modular pipeline to:

- ✅ Load and encode attraction wait time data
- ✅ Train and score forecasting models (XGBoost)
- ✅ Log and sync predictions to S3
- 🔜 Generate daily Crowd Calendar levels
- 🔜 Output reporting for internal and external use

---

## 🗂 Directory Structure

```
.
├── src/
│   ├── modelling/
│   │   ├── run_encodefeatures.jl
│   │   ├── run_trainer.jl
│   │   ├── run_predictions.jl
│   ├── data/
│   │   ├── run_sync.jl
│   └── utils/
│       ├── Structs.jl
│       ├── S3Utils.jl
├── work/
│   └── <entity_code>/
│       └── wait_times/
│           ├── to_be_modelled_<entity_code>.csv
│           └── scored_<entity_code>.csv
├── input/
│   └── forecasts/
├── output/$(uppercase(ATTRACTION.code))/
│   └── forecasts_<entity_code>.csv
├── README.md
```

---

## 🚀 Forecasting Pipeline

### Step 1: Encode Features

```julia
include("src/modelling/run_encodefeatures.jl")
df_encoded = main(attraction::Attraction)
```

- Reads wait time files from `output/$(uppercase(ATTRACTION.code))/`
- Applies feature encoding
- Writes encoded files to `work/<entity_code>/wait_times/`

### Step 2: Train & Score Model

```julia
include("src/modelling/run_trainer.jl")
main(attraction::Attraction)
```

- Loads encoded wait times
- Trains and saves XGBoost models
- Scores forecasts and writes them to `scored_<entity_code>.csv`

### Step 3: Log & Sync Forecasts

```julia
include("src/modelling/run_predictions.jl")
main(attraction::Attraction)
```

- Appends new scored predictions to existing forecast logs
- Saves result in `output/$(uppercase(ATTRACTION.code))/forecasts_<entity_code>.csv`
- Uploads to S3: `s3://touringplans_stats/stats_work/attraction/io/forecasts`

---

## 🔭 Coming Soon

### 🗓️ Crowd Calendar Processing

- Per-observation clustering to infer park-level crowd levels
- Public-facing 1–10 scale logic based on attraction thresholds

### 📊 Reporting Module

- Daily dashboard-friendly metrics
- Forecast accuracy tracking
- Slack / Google Sheets integration

---

## 🧱 Tech Stack

- Julia 1.10+
- XGBoost.jl (with GPU support)
- AWS S3 for I/O
- DataFrames.jl, CSV.jl, TimeZones.jl

---

## 🔧 Dev Notes

- No global config — everything uses the `Attraction` struct
- File paths follow strict naming conventions
- Logs are styled and printed with custom `log_info`, `log_warn`, `log_success`

---

## ✨ Credits

Built and maintained by the TouringPlans data science team.
