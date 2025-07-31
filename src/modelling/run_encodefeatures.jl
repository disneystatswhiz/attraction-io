# --------------------------------------------------------------------- #
# run_encodefeatures.jl - Feature encoding for modeling input           #
# --------------------------------------------------------------------- #

using Dates, CSV, DataFrames

# -------------------------------------------------------
# Step 1: Read and combine local wait time files
# -------------------------------------------------------
function read_wait_time_data(attraction::Attraction)
    entity_code = attraction.code
    wait_time_types = attraction.queue_type == "priority" ? ["priority"] : ["POSTED", "ACTUAL"]
    input_folder = "output"

    combined_df = DataFrame()

    for wait_time_type in wait_time_types
        wt_lower = lowercase(wait_time_type)
        file_path = joinpath(input_folder, "wait_times_$(entity_code)_$(wt_lower).csv")

        if isfile(file_path)
            df_wt = CSV.read(file_path, DataFrame)

            # Add meta_wait_time_type if not already present
            if !(:meta_wait_time_type in names(df_wt))
                df_wt.meta_wait_time_type .= wait_time_type
            end

            combined_df = vcat(combined_df, df_wt; cols = :union)
        else
            # @warn("Wait time file not found: $file_path")
        end
    end

    return combined_df
end

# -------------------------------------------------------
# Step 2: Encode features and write per-type outputs
# -------------------------------------------------------
function encode_and_save(df::DataFrame, attraction::Attraction)
    entity_code = attraction.code
    df_encoded = encode_features(df)

    for wt_type in unique(df_encoded.meta_wait_time_type)
        wt_lower = lowercase(wt_type)
        df_wt = df_encoded[df_encoded.meta_wait_time_type .== wt_type, :]
        output_path = "work/$(entity_code)/wait_times/to_be_modelled_$(wt_lower).csv"
        CSV.write(output_path, df_wt)
        # @info("📤 Encoded data written for $wt_type to $output_path")
    end

    return df_encoded
end

# -------------------------------------------------------
# Step 3: Main
# -------------------------------------------------------
function main(attraction::Attraction)
    entity_code = attraction.code
    wait_time_types = attraction.queue_type == "priority" ? ["priority"] : ["POSTED", "ACTUAL"]

    # Skip if all to_be_modelled files already exist
    all_exist = all(wait_time_type -> begin
        wt_lower = lowercase(wait_time_type)
        local_file_path = "work/$(entity_code)/wait_times/to_be_modelled_$(wt_lower).csv"
        isfile(local_file_path)
    end, wait_time_types)

    if all_exist
        # @info("✅ Encoded files already exist for $entity_code — skipping encoding.")
        return nothing
    end

    # Ensure at least one file exists
    any_file_exists = any(wait_time_type -> begin
        wt_lower = lowercase(wait_time_type)
        local_file_path = "output/wait_times_$(entity_code)_$(wt_lower).csv"
        isfile(local_file_path)
    end, wait_time_types)

    if !any_file_exists
        # @info("No wait time files found for $entity_code. Cannot proceed with encoding.")
        return nothing
    end

    df_all = read_wait_time_data(attraction)
    df_encoded = encode_and_save(df_all, attraction)

    return df_encoded
end

# Run the encoding
global df_encoded = main(ATTRACTION)
