# -----------------------------------------------------------
# run_tracking.jl — Track new wait time files for modeling
# -----------------------------------------------------------
using CSV, DataFrames, Glob, Dates

# -----------------------------------------------------------
# Get file’s last modified timestamp (as DateTime, not Date)
# -----------------------------------------------------------
function get_file_datetime(file::String)::DateTime
    try
        return unix2datetime(stat(file).mtime)
    catch e
        return DateTime(1900, 1, 1)
    end
end

# -----------------------------------------------------------
# Get latest encoded file timestamp (already processed)
# -----------------------------------------------------------
function get_latest_encoded_datetime(attraction::Attraction)::DateTime
    latest_times = DateTime[]
    for wt in getproperty(attraction, Val(:wait_time_types))
        local_file = joinpath(
            LOC_WORK,
            uppercase(attraction.code),
            "already_on_s3",
            "wait_times_$(uppercase(attraction.code))_$(lowercase(wt)).csv"
        )
        isfile(local_file) && push!(latest_times, get_file_datetime(local_file))
    end
    return isempty(latest_times) ? DateTime(1900, 1, 1) : maximum(latest_times)
end

# -----------------------------------------------------------
# Determine folder for raw synced files
# -----------------------------------------------------------
function get_local_sync_folder(attraction::Attraction)::String
    return attraction.queue_type == "priority" ?
        joinpath(LOC_INPUT, "wait_times", "priority", attraction.property) :
        joinpath(LOC_INPUT, "wait_times", attraction.property)
end

# -----------------------------------------------------------
# Main tracking function
# -----------------------------------------------------------
function run_tracking(attraction::Attraction)::Dict{String, Vector{String}}
    cutoff_dt = get_latest_encoded_datetime(attraction)
    folder = get_local_sync_folder(attraction)
    all_files = filter(f -> endswith(f, ".csv"), readdir(folder; join=true))
    new_files_by_type = Dict{String, Vector{String}}()
    for wt in getproperty(attraction, Val(:wait_time_types))
        new_files_by_type[wt] = String[]
    end
    for file in all_files
        fdt = get_file_datetime(file)
        if fdt > cutoff_dt
            for wt in keys(new_files_by_type)
                push!(new_files_by_type[wt], file)
            end
        end
    end
    return new_files_by_type
end

new_files_by_type = run_tracking(ATTRACTION)  # Run tracking for the defined attraction
