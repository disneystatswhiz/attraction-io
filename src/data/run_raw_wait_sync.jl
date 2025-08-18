# ------------------------------------------------------------------------
# run_raw_wait_sync.jl — Call s3utils/s3manager to sync raw wait-time files
# ------------------------------------------------------------------------

using Dates

# Properties to sync once per main_setup run
const PROPERTIES = ["wdw", "dlr", "uor"]

# Toggle priority for UOR if/when needed
if !isdefined(@__MODULE__, :ENABLE_UOR_PRIORITY)
    @eval const ENABLE_UOR_PRIORITY = false
end

# @info "⏳ Syncing raw wait-time inputs from S3 → local..."

for prop in PROPERTIES

    # Standby (always)
    s3_wait   = "s3://touringplans_stats/export/wait_times/$prop/"
    loc_wait  = joinpath(LOC_INPUT, "wait_times", prop)
    mkpath(loc_wait)
    ok1 = sync_from_s3_folder(
        s3_wait, loc_wait;
        exclude = ["*"],
        include = ["*.csv"],   # narrow if desired, e.g., ["current_wait.csv", "*.csv"]
    )

    # Priority (respect UOR toggle)
    if prop != "uor" || ENABLE_UOR_PRIORITY
        s3_prio  = "s3://touringplans_stats/export/fastpass_times/$prop/"
        loc_prio = joinpath(LOC_INPUT, "wait_times", "priority", prop)
        mkpath(loc_prio)
        ok2 = sync_from_s3_folder(
            s3_prio, loc_prio;
            exclude = ["current_test*"],
            include = ["*.csv"],  # or just ["current_fastpass.csv"] if you want minimal
        )
    end
end

# @info "✅ Raw wait-time sync complete."
