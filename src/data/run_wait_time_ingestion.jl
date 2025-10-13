# -------------------------------------------------------------
# 📦 Load new wait time data for a single entity
# -------------------------------------------------------------
using TimeZones

# ---- Main entry point ------------------------------------------------------
function run_wait_time_ingestion(
    new_files_by_type::Dict{String, Vector{String}},
    entity_code::String,
    queue_type::String
)

    output_folder = joinpath(LOC_WORK, entity_code, "wait_times")
    mkpath(output_folder)
    output_path = joinpath(output_folder, "wait_times.csv")

    if isfile(output_path)
        return
    end

    all_files = reduce(vcat, values(new_files_by_type))
    if isempty(all_files)
        return
    end

    combined_df = DataFrame()

    for (i, file) in enumerate(all_files)
        show_progress(i, length(all_files), file)

        try
            df = process_all_wait_time_files(file, entity_code, queue_type)
            if !isempty(df)
                append!(combined_df, df)
            end
        catch e
            @warn "⚠️ Failed to process $file: $e"
        end
    end

    if nrow(combined_df) == 0
        @warn "⚠️ No valid data ingested for $entity_code"
        return
    end

    combined_df.observed_at = [
        ZonedDateTime(DateTime(replace(string(x), r"\.0$" => "")), ATTRACTION.timezone, 2)
        for x in combined_df.observed_at
    ]

    sort!(combined_df, :observed_at)
    CSV.write(output_path, combined_df)
end


# ---- Utility: show status every 10 files -----------------------------------
function show_progress(i::Int, total::Int, file::String)
    if i == 1 || i % 10 == 0 || i == total
        @info "Processing file $i of $total: $file"
    end
end

# run it 
run_wait_time_ingestion(new_files_by_type, ATTRACTION.code, ATTRACTION.queue_type)
