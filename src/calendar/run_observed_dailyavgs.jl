# -----------------------------------------------------------------------------
# run_dailyavgs_observed_spike.jl  (Julia-style pseudocode, no functions)
# Goal: Per-day, per-entity average of OBSERVED POSTED wait times for 11:00–17:59
# -----------------------------------------------------------------------------

using Dates, DataFrames, CSV, Statistics, TimeZones

function main()
    
    # --- constants & paths --------------------------------------------------------
    START_HOUR = 11      # will be const in real code
    END_HOUR   = 17

    CODE    = uppercase(ATTRACTION.code)
    INPATH  = joinpath(LOC_WORK, CODE, "already_on_s3", "wait_times_$(CODE)_posted.csv")
    OUTPATH = joinpath(LOC_WORK,   CODE, "calendar", "observed_dailyavgs.csv")

    # --- idempotence / skip rules -------------------------------------------------
    if isfile(OUTPATH)
        # stop: output already exists; do nothing
        return
    end

    if ATTRACTION.queue_type == "priority"
        # stop: not applicable for priority queues
        return
    end

    if !isfile(INPATH)
        # stop: observed posted source file not found
        return
    end

    # --- load data ----------------------------------------------------------------
    df = CSV.read(INPATH, DataFrame)   # placeholder load
    required_cols = [:id_park_day_id, :id_entity_code, :meta_observed_at, :meta_wait_time_type, :target, :pred_mins_since_6am]
    missing_cols = setdiff(required_cols, names(df))
    if !isempty(missing_cols)
        # stop: missing required columns
    end

    # --- timestamp parsing --------------------------------------------------------
    df.observed_at = parse_zoneddatetimes_simple(df.meta_observed_at)

    # --- window filter (11:00–17:59 inclusive) -----------------------------------
    # For this filter use pred_mins_since_6am to figure out if the timestamp is in scope
    df_window = filter(row -> (row.pred_mins_since_6am ≥ (START_HOUR - 6) * 60 &&
                            row.pred_mins_since_6am ≤ (END_HOUR - 6) * 60), df)

    # Drop rows where target is missing
    df_window = filter(row -> !ismissing(row.target), df_window)

    # Drop rows where meta_wait_time_type is not "POSTED"
    df_window = filter(row -> row.meta_wait_time_type == "POSTED", df_window)

    # --- group & aggregate --------------------------------------------------------
    # group by day + entity; mean of target
    grouped = groupby(df_window, [:id_park_day_id, :id_entity_code])
    out = combine(grouped, :target => mean => :avg_posted_11am_to_5pm)

    # --- round, sort, rename ------------------------------------------------------
    out.avg_posted_11am_to_5pm = round.(out.avg_posted_11am_to_5pm, digits=1)
    sort!(out, [:id_park_day_id, :id_entity_code])
    rename!(out, :id_entity_code => :entity_code)

    # --- write output -------------------------------------------------------------
    mkpath(dirname(OUTPATH))
    CSV.write(OUTPATH, out)

    return
end

main()
