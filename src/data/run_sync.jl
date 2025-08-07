# ------------------------------------------------------------------------- #
# run_sync.jl — Sync from S3 the files we need for modelling 
# ------------------------------------------------------------------------- #

# ---------------------------------------------------------
# Step 1: Sync raw wait time files
# ---------------------------------------------------------
sync_wait_times(ATTRACTION.code, ATTRACTION.property, ATTRACTION.queue_type)

# ---------------------------------------------------------
# Step 2: Sync encoded wait times file(s) from stats_work
# ---------------------------------------------------------

for wt in getproperty(ATTRACTION, Val(:wait_time_types))
    filename = "wait_times_$(uppercase(ATTRACTION.code))_$(lowercase(wt)).csv"
    s3_path = "s3://touringplans_stats/stats_work/attraction-io/wait_times/$filename"
    local_folder = joinpath(LOC_WORK, uppercase(ATTRACTION.code), "already_on_s3")
    local_file = joinpath(LOC_WORK, uppercase(ATTRACTION.code), "already_on_s3", "wait_times_$(uppercase(ATTRACTION.code))_$(lowercase(wt)).csv")

    mkpath(local_folder)

    if file_exists_in_s3(s3_path)
        download_file_from_s3(s3_path, local_file)
    else
        # @info "⚠️ Encoded file not found in S3, will create from scratch: $s3_path"
    end
end

# ---------------------------------------------------------
# Step 3: Sync forecast file(s) from stats_work
# ---------------------------------------------------------

for wt in getproperty(ATTRACTION, Val(:wait_time_types))
    filename = "forecasts_$(uppercase(ATTRACTION.code))_$(lowercase(wt)).csv"
    s3_path = "s3://touringplans_stats/stats_work/attraction-io/forecasts/$filename"
    local_folder = joinpath(LOC_WORK, uppercase(ATTRACTION.code), "already_on_s3")
    local_file = joinpath(LOC_WORK, uppercase(ATTRACTION.code), "already_on_s3", "forecasts_$(uppercase(ATTRACTION.code))_$(lowercase(wt)).csv")

    mkpath(local_folder)

    if file_exists_in_s3(s3_path)
        download_file_from_s3(s3_path, local_file)
    else
        # @info "⚠️ Encoded file not found in S3, will create from scratch: $s3_path"
    end
end

