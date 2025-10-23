using JSON3, Dates

# -------------------------------------------------------
# Sync all entity files from S3
# -------------------------------------------------------
function sync_entity_files()
    s3path    = "s3://touringplans_stats/export/entities"
    localpath = joinpath(LOC_INPUT, "entities")

    mkpath(localpath)
    sync_from_s3_folder(s3path, localpath)
end


# -------------------------------------------------------
# Sync calendar forecasts from S3
# -------------------------------------------------------
function sync_calendar_forecasts(entity_id::String)
    filename     = "forecasts_$(uppercase(entity_id))_calendar.csv"
    s3_path      = "s3://touringplans_stats/stats_work/attraction-io/forecasts/$filename"
    local_folder = joinpath(LOC_WORK, uppercase(entity_id), "already_on_s3")
    local_file   = joinpath(local_folder, filename)

    mkpath(local_folder)

    exists = file_exists_in_s3(s3_path)
    if exists
        download_file_from_s3(s3_path, local_file)
    else
        # @info "⚠️ Encoded file not found in S3, will create from scratch: $s3_path"
    end

    return (local_file=local_file, exists=exists)
end


# -------------------------------------------------------
# Sync park hours from S3
# -------------------------------------------------------
function sync_parkhours_files()

    s3path     = "s3://touringplans_stats/export/park_hours"
    localpath  = joinpath(LOC_INPUT, "parkhours")

    mkpath(localpath)

    sync_from_s3_folder(s3path, localpath)

end

# -------------------------------------------------------
# Sync events from S3
# -------------------------------------------------------
function sync_event_files()

    filename   = "current_event_days.csv"
    s3file     = "s3://touringplans_stats/export/events/$filename"
    localpath  = joinpath(LOC_INPUT, "events")
    localfile  = joinpath(localpath, filename)
    mkpath(localpath)
    download_file_from_s3(s3file, localfile)

    filename   = "current_events.csv"
    s3file     = "s3://touringplans_stats/export/events/$filename"
    localpath  = joinpath(LOC_INPUT, "events")
    localfile  = joinpath(localpath, filename)
    mkpath(localpath)
    download_file_from_s3(s3file, localfile)

end