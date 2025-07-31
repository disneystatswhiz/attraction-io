# -----------------------------------------------------------
# run_tracking.jl — Track new wait time files for modeling
# -----------------------------------------------------------
using CSV, DataFrames, Glob

# -----------------------------------------------------------
# Get file’s last modified date (as Date, not DateTime)
# -----------------------------------------------------------
function get_file_date(file::String)::Date
    try
        dt = unix2datetime(stat(file).mtime)
        return Date(dt)
    catch e
        # @warn "⚠️ Failed to read file timestamp: $file — $e"
        return Date(1900, 1, 1)
    end
end

# -----------------------------------------------------------
# Get latest encoded file date (already processed)
# -----------------------------------------------------------
function get_latest_encoded_date(attraction::Attraction)::Date
    latest_dates = Date[]

    for wt in getproperty(attraction, Val(:wait_time_types))
        local_file = "work/$(ATTRACTION.code)/already_on_s3/wait_times_$(uppercase(ATTRACTION.code))_$(lowercase(wt)).csv"
        isfile(local_file) && push!(latest_dates, get_file_date(local_file))
    end

    return isempty(latest_dates) ? Date(1900, 1, 1) : maximum(latest_dates)
end

# -----------------------------------------------------------
# Determine folder for raw synced files
# -----------------------------------------------------------
function get_local_sync_folder(attraction::Attraction)::String
    
    return attraction.queue_type == "priority" ?
        "input/wait_times/priority/$(attraction.property)" :
        "input/wait_times/$(attraction.property)"
end

# -----------------------------------------------------------
# Main tracking function
# -----------------------------------------------------------
function run_tracking(attraction::Attraction)::Dict{String, Vector{String}}

    cutoff_date = get_latest_encoded_date(attraction)

    folder = get_local_sync_folder(attraction)
    all_files = filter(f -> endswith(f, ".csv"), readdir(folder; join=true))

    new_files_by_type = Dict{String, Vector{String}}()

    for wt in getproperty(attraction, Val(:wait_time_types))
        new_files_by_type[wt] = String[]
    end

    for file in all_files
        fdate = get_file_date(file)

        if fdate > cutoff_date
            for wt in keys(new_files_by_type)
                push!(new_files_by_type[wt], file)
            end
        end
    end

    return new_files_by_type
end

new_files_by_type = run_tracking(ATTRACTION)  # Run tracking for the defined attraction