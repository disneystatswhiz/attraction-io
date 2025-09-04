# --------------------------------------------------------------------------
# run_thresholds.jl - Append future-dated thresholds to history (as of today)
# Steps:
# 1) Load thresholds history from S3 (if exists)
# 2) Using TOMORROW (today + 1) as effective_date and using forecasts for days >= TOMORROW,
#    calculate thresholds per entity
# 3) Append to thresholds history (durable) and upload back to S3
# 4) Also write a same-run snapshot to LOC_WORK for viewing
# --------------------------------------------------------------------------

using JSON3, DataFrames, CSV, Statistics, Clustering, Dates

# -----------------------------
# Helpers
# -----------------------------
function _mk_threshold_row(code::AbstractString, eff::Date, vals::AbstractVector{<:Real})
    @assert length(vals) == 9 "need 9 threshold values"
    return (; 
        entity_code = uppercase(String(code)),
        effective_date = eff,
        cl1_max = float(vals[1]), cl2_max = float(vals[2]), cl3_max = float(vals[3]),
        cl4_max = float(vals[4]), cl5_max = float(vals[5]), cl6_max = float(vals[6]),
        cl7_max = float(vals[7]), cl8_max = float(vals[8]), cl9_max = float(vals[9])
    )
end

function _ensure_history_schema()::DataFrame
    DataFrame(
        entity_code    = String[],
        effective_date = Date[],
        cl1_max=Float64[], cl2_max=Float64[], cl3_max=Float64[],
        cl4_max=Float64[], cl5_max=Float64[], cl6_max=Float64[],
        cl7_max=Float64[], cl8_max=Float64[], cl9_max=Float64[]
    )
end

# Compute thresholds from a vector of daily-average waits
function _compute_thresholds_from_waits(waits::Vector{<:Real})::Vector{Float64}
    if length(waits) < 10
        return fill(999.0, 9)
    end
    # KMeans on a row vector
    X = reshape(collect(waits), 1, :)
    result = kmeans(X, 10; maxiter=1000, display=:none)

    # Map each point to cluster, then get per-cluster max; order by cluster centroid rank
    levels  = [findfirst(==(c), sortperm(vec(result.centers))) for c in result.assignments]
    grouped = combine(groupby(DataFrame(level=levels, wait=waits), :level), :wait => maximum => :max_wait)
    sort!(grouped, :level)
    t = grouped.max_wait

    if length(t) < 9
        return fill(999.0, 9)
    else
        return round.(t[1:9], digits=1)
    end
end

# -----------------------------
# Main
# -----------------------------
function main()
    # Skip priority queues
    if ATTRACTION.queue_type == "priority"
        return
    end

    # Paths (no consts; computed per ATTRACTION)
    entity_uc = uppercase(ATTRACTION.code)
    work_cal_dir = joinpath(LOC_WORK, entity_uc, "calendar")
    out_cal_dir  = joinpath(LOC_OUTPUT, entity_uc)  # durable local mirror under OUTPUT
    avgs_path    = joinpath(work_cal_dir, "forecasts_dailyavgs.csv")

    # Durable history (local + S3)
    hist_local = joinpath(out_cal_dir, "forecasts_$(entity_uc)_thresholds.csv")
    hist_s3    = "s3://touringplans_stats/stats_work/attraction-io/forecasts/forecasts_$(entity_uc)_thresholds.csv"

    # Ephemeral/snapshot (kept in WORK for consistency & viewing this run)
    snapshot_path = joinpath(work_cal_dir, "forecasts_thresholds.csv")

    # Must have per-day averages (produced by run_dailyavgs.jl)
    if !isfile(avgs_path)
        return
    end

    mkpath(out_cal_dir)   # ensure durable folder exists
    mkpath(work_cal_dir)  # ensure work folder exists

    # (1) Load history from S3 if present (download to hist_local)
    try
        if s3_file_exists(hist_s3)             # <- your helper
            download_from_s3(hist_s3, hist_local)  # <- your helper
        end
    catch
        # non-fatal: proceed with empty history
    end

    hist = isfile(hist_local) ? CSV.read(hist_local, DataFrame) : _ensure_history_schema()
    if :effective_date ∈ names(hist) && !(eltype(hist.effective_date) <: Date)
        hist.effective_date = Date.(hist.effective_date)
    end

    # (2) Read daily averages and keep only FUTURE days (>= tomorrow)
    df = CSV.read(avgs_path, DataFrame)

    # Required columns
    req = ["entity_code", "avg_posted_11am_to_5pm", "id_park_day_id"]
    if !all(c -> c ∈ names(df), req)
        return
    end

    # Normalize types
    if !(eltype(df.id_park_day_id) <: Date)
        try
            df.id_park_day_id = Date.(df.id_park_day_id)
        catch
            return
        end
    end

    tomorrow = Dates.today() + Day(1)
    df_future = filter(r -> r.id_park_day_id >= tomorrow, df)

    # Compute thresholds per entity from FUTURE averages only, with effective_date = tomorrow
    todays_rows = NamedTuple[]
    if nrow(df_future) == 0
        # No future rows – emit a single row for this entity with sentinel thresholds
        push!(todays_rows, _mk_threshold_row(entity_uc, tomorrow, fill(999.0, 9)))
    else
        # Handle whatever entities are present in avgs (usually just this entity)
        for g in groupby(df_future, :entity_code)
            code  = String(g.entity_code[1])
            waits = collect(skipmissing(g.avg_posted_11am_to_5pm))
            vals  = _compute_thresholds_from_waits(waits)
            push!(todays_rows, _mk_threshold_row(code, tomorrow, vals))
        end
    end
    todays = DataFrame(todays_rows)

    # (3) Append to history and de-dupe by (entity_code, effective_date)
    all_thr = vcat(hist, todays)
    sort!(all_thr, [:entity_code, :effective_date], rev=true)
    all_thr = combine(groupby(all_thr, [:entity_code, :effective_date])) do g
        first(g, 1)
    end
    sort!(all_thr, [:entity_code, :effective_date])

    # Write durable history locally and to S3
    CSV.write(hist_local, all_thr)
    upload_file_to_s3(hist_local, hist_s3)  # <- your helper

    # Also write the snapshot (today's computation) to WORK for easy inspection
    CSV.write(snapshot_path, todays)
end

main()
