# 🎢 ATTRACTION-IO Wait Time Pipeline

This repository contains the full EC2-ready data pipeline for attraction wait time modeling, forecast logging, crowd calendar generation, and reporting.

---

## 🏁 Getting Started

1. `git clone ...` and `cd` into this repo.
2. Install dependencies: `julia --project=. -e 'using Pkg; Pkg.instantiate()'`
3. Run batch: `pipeline.bat` (local) or `pipeline.sh` (EC2).
4. To run a single attraction: `julia main_runner.jl <entity> <park> <property> <type>` all lowercase
5. Type is wait time type: `standby` or `priority`

See code comments for further details per module.

---

## 📦 Overview

This project supports a modular pipeline to:

- ✅ Load and encode attraction wait time data
- ✅ Train and score forecasting models (XGBoost)
- ✅ Log and sync predictions to S3
- ✅ Generate daily Crowd Calendar levels
- ✅ Output reporting for internal and external use

---

## 🗂 Directory Structure

```
.
├── input/
│   └── datasets imported from S3
├── output/
│   └── datasets queued for loading to S3
├── scheduler/
│   └── run_jobs.jl
├── src/
│   ├── calendar/
│   │   ├── run_assign_levels.jl
│   │   ├── run_dailyavgs.jl
│   │   ├── run_thresholds.jl
│   ├── data/
│   │   ├── run_features.jl
│   │   ├── run_futuredates.jl
│   │   ├── run_premodelling.jl
│   │   ├── run_setattraction.jl
│   │   ├── run_sync.jl
│   │   ├── run_tracking.jl
│   │   ├── run_wait_time_ingestion.jl
│   ├── dim/
│   │   ├── run_dimDate.jl
│   │   ├── run_dimDateGroupID.jl
│   │   ├── run_dimEntity.jl
│   │   ├── run_dimEvents.jl
│   │   ├── run_dimHolidays.jl
│   │   ├── run_dimMetatable.jl
│   │   ├── run_dimParkHours.jl
│   │   ├── run_dimSeason.jl
│   ├── donor/
│   │   ├── run_donorParkHours.jl
│   ├── modelling/
│   │   ├── run_encodefeatures.jl
│   │   ├── run_predictions.jl
│   │   ├── run_trainer.jl
│   │   ├── run_writer.jl # Currently Optional and not in production
│   ├── modules/
│   │   ├── mod_customloaders.jl
│   │   ├── mod_encoders.jl
│   ├── reporting/
│   │   ├── run_accuracyreports.jl
│   │   ├── run_descriptives.jl
│   │   ├── run_pipelinestatus.jl
│   ├── utilities/
│   │   ├── features.jl
│   │   ├── s3syncmanager.jl
│   │   ├── s3utils.jl
│   │   ├── structs.jl
│   │   ├── utility_setup.jl
│   │   ├── utils.jl
│   ├── main_runner.jl
│   └── main_setup.jl
├── temp/
│   └── temporary scripts and data
├── work/
│   └── holding folder for attraction files
├── .gitignore
├── Manifest.toml
├── pipeline.bat # batch runner for local job
├── pipeline.sh # batch runner for EC2 job
├── Project.toml
├── README.md
├── TODO.md
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
- Uploads to S3: `s3://touringplans_stats/stats_work/attraction-io/forecasts`

---

### 🔍 Viewing the live log on the EC2 Instance
- sudo -iu ubuntu
- cd /home/ubuntu/attraction-io/logs
- tail -f pipeline_latest.log

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
- to disable running the pipeline on boot up of the ec2 instance add a blank file in the root repo called ".disable_on_boot" - helpful for debugging if the disk is full

---

## ✨ Credits

Built and maintained by the TouringPlans data science team.
