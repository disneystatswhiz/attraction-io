using Dates

# --------------------------------------------------------------------------
# 1. Sync an entire S3 folder to a local directory
# --------------------------------------------------------------------------
"""
    sync_from_s3_folder(s3path, localpath; exclude=[], include=[], dryrun=false)

Sync all files from the given S3 folder (recursively) to a local directory.

Example:
    sync_from_s3_folder(
        "s3://bucket/wait_times/wdw",       # S3 path to sync from
        "input/wait_times/wdw";             # Local path to sync to
        exclude=["*.tmp"],                  # Exclude strings
        include=["*.csv"]                   # Include strings
    )
"""
function sync_from_s3_folder(
    s3path::String,
    localpath::String;
    exclude::Vector{String} = String[],
    include::Vector{String} = String[],
    dryrun::Bool = false
)::Bool
    s3path = startswith(s3path, "s3://") ? s3path : "s3://$s3path"
    isdir(localpath) || mkpath(localpath)

    cmd_parts = ["aws", "s3", "sync", s3path, localpath, "--only-show-errors"]
    dryrun && push!(cmd_parts, "--dryrun")
    foreach(ex -> push!(cmd_parts, "--exclude=$ex"), exclude)
    foreach(inc -> push!(cmd_parts, "--include=$inc"), include)

    try
        run(Cmd(cmd_parts))
        return true
    catch e
        # @warn "S3 folder sync failed" s3path localpath exception = e
        return false
    end
end


# --------------------------------------------------------------------------
# 2. Download a single file from S3 (skip if already present)
# --------------------------------------------------------------------------
"""
    download_file_from_s3(s3file, localfile)

Download one specific file from S3 to a local file path.
Skips download if the local file already exists.

Returns:
- `true` if file was downloaded
- `false` if skipped or failed

Example:
    download_file_from_s3("s3://bucket/path/file.csv", "input/file.csv")
"""
function download_file_from_s3(s3file::String, localfile::String)::Bool
    s3file = startswith(s3file, "s3://") ? s3file : "s3://$s3file"
    isdir(dirname(localfile)) || mkpath(dirname(localfile))

    # if isfile(localfile)
    #    return false
    # end

    try
        run(`aws s3 cp $s3file $localfile --only-show-errors`)
        return true
    catch e
        # @warn "❌ S3 file download failed" s3file localfile exception = e
        return false
    end
end



# --------------------------------------------------------------------------
# 3. Upload a single file to S3
# --------------------------------------------------------------------------
"""
    upload_file_to_s3(localfile, s3file)

Upload a local file to a given S3 path.

Example:
    upload_file_to_s3(
        "output/AK07/forecasts.csv",        # Local file to upload
        "s3://bucket/forecasts/AK07.csv"    # S3 file path to save as
    )
"""
function upload_file_to_s3(localfile::String, s3file::String)::Bool
    s3file = startswith(s3file, "s3://") ? s3file : "s3://$s3file"

    try
        run(`aws s3 cp $localfile $s3file --only-show-errors`)
        # @info "✅ S3 file uploaded" localfile s3file
        return true
    catch e
        # @warn "S3 file upload failed" localfile s3file exception = e
        return false
    end
end

# --------------------------------------------------------------------------
# 4. File exists in S3
# --------------------------------------------------------------------------
"""
    file_exists_in_s3(s3file)

Check if a file exists in S3.

Example:
    file_exists_in_s3("s3://bucket/path/file.csv")
"""
function file_exists_in_s3(s3file::String)::Bool
    s3file = startswith(s3file, "s3://") ? s3file : "s3://$s3file"

    try
        run(pipeline(`aws s3 ls $s3file`, stdout=devnull, stderr=devnull))
        return true
    catch
        return false
    end
end

# --------------------------------------------------------------------------
# 5. Get last modified date stamp for a file in S3
# --------------------------------------------------------------------------
"""
    get_last_modified_s3(bucket::String, key::String) -> Union{Date, Nothing}

Returns the last modified date (UTC) of an object in S3, as a `Date`.

Example:
    get_last_modified_s3("touringplans_stats", "stats_work/attraction-io/crowd_calendar/current_calendar.csv")
"""
function get_last_modified_s3(bucket::String, key::String)::Union{Date, Nothing}
    
    try
        output = read(`aws s3api head-object --bucket $bucket --key $key --query LastModified --output text`, String)
        date_part = first(split(output, "T"))  # Keep just the "YYYY-MM-DD" part
        return Date(date_part, dateformat"yyyy-mm-dd")
    catch e
        # @warn "Failed to get last modified date" bucket key exception = e
        return nothing
    end
end

# ---- End of S3Utils module