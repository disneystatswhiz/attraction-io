# -----------------------------------------------------------
# run_raw_wait_sync.jl — Sync unified fact table + load to memory
# -----------------------------------------------------------
using DataFrames, Parquet

# We rely on:
# - LOC_INPUT           (from setup/utilities)
# - download_file_from_s3(s3file, localfile)::Bool

const FACT_S3_PATH = "s3://touringplans_stats/stats_work/fact_tables/wait_time_fact_table.parquet"
const OBS_REPORT_S3_PATH = "s3://touringplans_stats/stats_work/fact_tables/latest_obs_report.csv"
const FACT_LOCAL_PATH = joinpath(LOC_INPUT, "wait_times", "wait_time_fact_table.parquet")
const OBS_LOCAL_PATH  = joinpath(LOC_INPUT, "wait_times", "latest_obs_report.csv")

# -----------------------------------------------------------
# Sync helpers
# -----------------------------------------------------------
function sync_wait_time_fact_table!()::Bool
    mkpath(dirname(FACT_LOCAL_PATH))
    ok = download_file_from_s3(FACT_S3_PATH, FACT_LOCAL_PATH)
    if ok && isfile(FACT_LOCAL_PATH) && filesize(FACT_LOCAL_PATH) > 0
        # @info "📥 Synced fact table → $FACT_LOCAL_PATH ($(round(filesize(FACT_LOCAL_PATH)/1_000_000; digits=1)) MB)"
        return true
    else
        # @warn "❌ Fact table sync failed or file empty at $FACT_LOCAL_PATH"
        return false
    end
end

function sync_latest_obs_report!()::Bool
    mkpath(dirname(OBS_LOCAL_PATH))
    ok = download_file_from_s3(OBS_REPORT_S3_PATH, OBS_LOCAL_PATH)
    if ok && isfile(OBS_LOCAL_PATH) && filesize(OBS_LOCAL_PATH) > 0
        # @info "📥 Synced latest_obs_report → $OBS_LOCAL_PATH"
        return true
    else
        # @warn "❌ latest_obs_report sync failed or file empty at $OBS_LOCAL_PATH"
        return false
    end
end

# -----------------------------------------------------------
# Load fact table (into memory)
# -----------------------------------------------------------
function load_wait_time_fact_table()::DataFrame
    isfile(FACT_LOCAL_PATH) || throw(ArgumentError("Fact table missing locally at $FACT_LOCAL_PATH"))
    # @info "🧠 Loading wait_time_fact_table into memory..."
    df = DataFrame(Parquet.read_parquet(FACT_LOCAL_PATH))
    # @info "✅ Loaded $(nrow(df)) rows, $(ncol(df)) columns."
    return df
end

# -----------------------------------------------------------
# Run immediately when included
# -----------------------------------------------------------
if sync_wait_time_fact_table!()
    global DATA_FACT = load_wait_time_fact_table()
else
    error("❌ Could not sync fact table from S3 — aborting pipeline.")
end

if !sync_latest_obs_report!()
    # @warn "⚠️ Proceeding without updated latest_obs_report."
end
