# --------------------------------------------------------------------------
# run_assign_levels.jl - Assign CL1–CL10 crowd levels to daily averages
# --------------------------------------------------------------------------

using JSON3, DataFrames, CSV

# -------------------------------------------------------
# Step 1: Assign crowd level based on thresholds
# -------------------------------------------------------
function assign_crowd_level(avg, row)
    if ismissing(avg)
        return missing
    end
    thresholds = [row[Symbol("cl$(i)_max")] for i in 1:9]
    if any(x -> ismissing(x) || x == 999.0, thresholds)
        return missing
    end
    for i in 1:9
        if avg <= thresholds[i]
            return i
        end
    end
    return 10
end

# -------------------------------------------------------
# Step 2: Main logic
# -------------------------------------------------------
function main()
    temp = joinpath(LOC_WORK, uppercase(ATTRACTION.code), "calendar")
    input_avgs = joinpath(temp, "forecasts_dailyavgs.csv")
    input_thresh = joinpath(temp, "forecasts_thresholds.csv")
    output_path = joinpath(temp, "forecasts_dailyavgs_w_levels.csv")

    # If queue_type is priority, skip this step
    if ATTRACTION.queue_type == "priority"
        # @info("🛑 Skipping forecasts_dailyavgs_w_levels.csv for priority queue.")
        return
    end

    if !isfile(input_avgs) || !isfile(input_thresh)
        # @warn("❌ Missing required input files.")
        return
    end

    # If output file already exists, skip this step
    if isfile(output_path)
        # @info("ℹ️ Output file already exists — skipping")
        return
    end

    df_avgs = CSV.read(input_avgs, DataFrame)
    df_thresh = CSV.read(input_thresh, DataFrame)

    required_avgs = ["entity_code", "avg_posted_11am_to_5pm"]
    required_thresh = ["entity_code", "cl1_max", "cl9_max"]

    if !all(x -> x in names(df_avgs), required_avgs) || !all(x -> x in names(df_thresh), required_thresh)
        # @warn"❌ Missing required columns in input files."
        return
    end

    df = innerjoin(df_avgs, df_thresh, on=:entity_code)
    df.crowd_level = [assign_crowd_level(avg, row) for (avg, row) in zip(df.avg_posted_11am_to_5pm, eachrow(df))]
    sort!(df, [:entity_code, :id_park_day_id])
    CSV.write(output_path, df)
    # @info("✅ Wrote forecasts_dailyavgs_w_levels.csv to $output_path")

    # --- Sync and send to S3 ---
    synced = sync_calendar_forecasts(ATTRACTION.code)

    # If previous file exists, append to it and re-upload
    if synced.exists
        df_prior = CSV.read(synced.local_file, DataFrame)
        df_combined = sort(vcat(df_prior, df), [:entity_code, :id_park_day_id, :effective_date], rev=true)
        df_combined = combine(groupby(df_combined, [:entity_code, :id_park_day_id])) do sub
            first(sub, 1)  # keep only most recent effective_date per entity/date
        end
        CSV.write(synced.local_file, df_combined)
    else
        # No prior file — use current as base
        CSV.write(synced.local_file, df)
    end

    # Upload to S3
    upload_file_to_s3(synced.local_file, "s3://touringplans_stats/stats_work/attraction-io/forecasts/$(basename(synced.local_file))")

    # Save final version to output/$(uppercase(ATTRACTION.code))/
    final_output_path = joinpath(LOC_OUTPUT, uppercase(ATTRACTION.code), "forecasts_$(uppercase(ATTRACTION.code))_calendar.csv")
    mkpath(dirname(final_output_path))
    mv(synced.local_file, final_output_path; force=true)

end

main()
