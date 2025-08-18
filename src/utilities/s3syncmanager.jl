using JSON3, Dates

const SYNC_PROPS_DEFAULT = ["wdw", "dlr", "uor"]

# ------------------------------------------------------------------ #
# Sync both standby and (optionally) priority files for one property #
# ------------------------------------------------------------------ #

function sync_property_wait_times!(property::String; enable_priority::Bool=true, delete::Bool=false)
    # Standby
    s3_wait   = "s3://touringplans_stats/export/wait_times/$property/"
    loc_wait  = joinpath(LOC_INPUT, "wait_times", property)
    mkpath(loc_wait)
    ok_wait = sync_from_s3_folder(s3_wait, loc_wait; exclude=["*"], include=["*.csv"], delete=delete)

    # Priority
    ok_prio = true
    if enable_priority
        s3_prio  = "s3://touringplans_stats/export/fastpass_times/$property/"
        loc_prio = joinpath(LOC_INPUT, "wait_times", "priority", property)
        mkpath(loc_prio)
        ok_prio = sync_from_s3_folder(s3_prio, loc_prio; exclude=["*"], include=["*.csv"], delete=delete)
    end

    return (standby=ok_wait, priority=ok_prio)
end

# ------------------------------------------------------- #
# Sync all wait times once per day #
# ------------------------------------------------------- #
function sync_all_wait_times_once_per_day!(; properties::Vector{String}=SYNC_PROPS_DEFAULT,
                                           enable_uor_priority::Bool=false,
                                           delete::Bool=false)
    # Sentinel + lock under LOC_INPUT to avoid permission surprises
    sent_dir = joinpath(LOC_INPUT, ".sentinels"); mkpath(sent_dir)
    stamp    = Dates.format(today(UTC), "yyyymmdd")
    sentinel = joinpath(sent_dir, "wait_sync_$stamp")
    lockfile = sentinel * ".lock"

    # Fast path: already synced today
    if isfile(sentinel)
        return Dict("status"=>"skipped", "sentinel"=>sentinel)
    end

    # Try to lock; if another process is running, wait briefly for completion
    io = try open(lockfile, "x") catch; nothing end
    if io === nothing
        for _ in 1:120
            isfile(sentinel) && return Dict("status"=>"skipped", "sentinel"=>sentinel)
            sleep(1)
        end
        return Dict("status"=>"skipped-timeout", "sentinel"=>sentinel)
    end

    try
        results = Dict{String,Any}()
        for prop in properties
            enable_pri = (prop != "uor") || enable_uor_priority
            res = sync_property_wait_times!(prop; enable_priority=enable_pri, delete=delete)
            results[prop] = res
        end
        touch(sentinel)                         # mark success for this UTC day
        results["status"]   = "ok"
        results["sentinel"] = sentinel
        return results
    finally
        close(io); rm(lockfile; force=true)
    end
end



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