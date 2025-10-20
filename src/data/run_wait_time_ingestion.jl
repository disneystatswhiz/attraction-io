# -------------------------------------------------------------
# 📦 Load new wait time data for a single entity (dedupe added)
# -------------------------------------------------------------
using CSV, DataFrames, Dates, Logging, TimeZones

# ---- Helpers ----------------------------------------------------------------
# Normalize timestamps and drop exact duplicates *in-place*.
function normalize_and_dedupe!(df::DataFrame)
    if nrow(df) == 0
        return df
    end
    # Normalize observed_at before dedupe so equality works as expected.
    df.observed_at = [
        ZonedDateTime(DateTime(replace(string(x), r"\.0$" => "")), ATTRACTION.timezone, 2)
        for x in df.observed_at
    ]
    unique!(df)
    return df
end

# Show status every 10 files (or first/last)
function show_progress(i::Int, total::Int, file::String)
    if i == 1 || i % 10 == 0 || i == total
        # @info "Processing file $i of $total: $file"
    end
end

# ---- Main entry point -------------------------------------------------------
function run_wait_time_ingestion(
    new_files_by_type::Dict{String, Vector{String}},
    entity_code::String,
    queue_type::String
)
    output_folder = joinpath(LOC_WORK, entity_code, "wait_times")
    mkpath(output_folder)
    output_path = joinpath(output_folder, "wait_times.csv")

    # If file already exists, skip (idempotent behavior)
    if isfile(output_path)
        return
    end

    all_files = reduce(vcat, values(new_files_by_type))
    if isempty(all_files)
        return
    end

    combined_df = DataFrame()

    # ---- Ingest each file, normalize & dedupe per-file, append --------------
    for (i, file) in enumerate(all_files)
        show_progress(i, length(all_files), file)

        try
            df = process_all_wait_time_files(file, entity_code, queue_type)

            if !isempty(df)
                normalize_and_dedupe!(df)                   # per-file dedupe
                append!(combined_df, df, promote = true)
            end

        catch e
            # @warn "⚠️ Failed to process $file: $e"
        end
    end

    # ---- Global checks & final dedupe ---------------------------------------
    if nrow(combined_df) == 0
        # @warn "⚠️ No valid data ingested for $entity_code"
        return
    end

    # Already normalized per-file; if inputs can mix types, you can re-normalize here.
    # unique! again to catch cross-file duplicates:
    unique!(combined_df)

    # ---- Final ordering & write ---------------------------------------------
    sort!(combined_df, :observed_at)
    CSV.write(output_path, combined_df)
end

# run it
run_wait_time_ingestion(new_files_by_type, ATTRACTION.code, ATTRACTION.queue_type)
