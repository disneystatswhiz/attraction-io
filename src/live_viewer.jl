using Dates, TimeZones

function sync_from_s3_folder(s3path::String, localpath::String)
    s3path = startswith(s3path, "s3://") ? s3path : "s3://$s3path"
    isdir(localpath) || mkpath(localpath)
    cmd_parts = ["aws", "s3", "sync", s3path, localpath, "--only-show-errors"]
    try
        run(Cmd(cmd_parts))
        return true
    catch
        return false
    end
end

function latest_csv_mod(localpath::String, tz::TimeZone)
    files = filter(f -> endswith(f, ".csv"), readdir(localpath; join=true))
    if isempty(files)
        return nothing, nothing
    end
    max_file = nothing
    max_time = ZonedDateTime(0, tz)
    for file in files
        mtime_float = stat(file).mtime
        mtime_utc = unix2datetime(mtime_float)
        mtime_local = ZonedDateTime(mtime_utc, tz)
        if mtime_local > max_time
            max_time = mtime_local
            max_file = file
        end
    end
    return max_file, max_time
end

function main()
    s3path = "s3://touringplans_stats/stats_work/attraction-io/reporting/descriptive_summaries/"
    localpath = "d:\\GitHub\\attraction-io\\temp"
    tz = TimeZone("America/Toronto")

    last_file = nothing
    last_time = ZonedDateTime(0, tz)

    println("Starting S3 watcher loop - $s3path. Press Ctrl+C to stop.")
    while true
        sync_from_s3_folder(s3path, localpath)
        file, mtime = latest_csv_mod(localpath, tz)

        if isnothing(file)
            println("[", now(), "] No CSV files found in $localpath")
        elseif (file != last_file) || (mtime > last_time)
            println("[", now(), "] New or updated file detected: $(basename(file)), Time: $mtime")
            last_file = file
            last_time = mtime
        end

        sleep(5)
    end
end

main()
