# -------------------------------------------------------------------- #
# run_predictions.jl - Log and sync scored forecasts                   #
# -------------------------------------------------------------------- #

using Dates, CSV, DataFrames

# Round wait times based on wait time type, with floor at 0
function round_wait_time(value, wait_time_type::String)
    isnothing(value) && return missing
    if isnan(value)
        return missing
    end

    if wait_time_type == "POSTED" || wait_time_type == "posted"
        rounded = ceil(Int, value / 5) * 5
        if rounded == 100
            rounded = 105
        end
    else
        rounded = round(Int, value)
    end

    return max(0, rounded)
end

function run_entity_forecast_logger(attraction::Attraction)
    entity_code = attraction.code
    wait_time_types = attraction.queue_type == "priority" ? ["priority"] : ["POSTED", "ACTUAL"]
    model_name = "XGBoost"
    today_str = string(Dates.today())

    for wt_type in wait_time_types
        wt_lower = lowercase(wt_type)
        base_filename = "forecasts_$(entity_code)_$(wt_lower).csv"

        # --- Paths ---
        download_path = joinpath("input", "forecasts")
        scored_path   = joinpath("work", entity_code, "wait_times", "scored_$(wt_lower).csv")
        output_path   = joinpath("output", base_filename)
        local_synced  = joinpath(download_path, base_filename)

        # --- Ensure input/forecasts folder exists ---
        isdir(download_path) || mkpath(download_path)

        # ----------------------------------------------------
        # Step 1. Download existing forecast file from S3
        # ----------------------------------------------------
        
        try
            sync_from_s3("s3://touringplans_stats/stats_work/attraction-io/forecasts", local_synced)
        catch e
        end

        # Load old forecasts if they exist
        df_existing = isfile(local_synced) ? CSV.read(local_synced, DataFrame) : DataFrame()

        # ----------------------------------------------------
        # Step 2. Load newly scored forecasts
        # ----------------------------------------------------
        if isfile(scored_path)
            df_new = CSV.read(scored_path, DataFrame)
            df_new.meta_observed_at = parse_zoneddatetimes_simple(df_new.meta_observed_at)

            # Round and annotate
            df_new.predicted_wait_time = round_wait_time.(df_new.predicted_wait_time, wt_lower)
            df_new.model_name = fill(model_name, nrow(df_new))
            df_new.model_run_date = fill(today_str, nrow(df_new))

            # Combine and dedupe
            df_combined = vcat(df_existing, df_new; cols = :union)
            df_combined.model_run_date = Date.(df_combined.model_run_date)
            sort!(df_combined, [:meta_observed_at, :model_run_date], rev = [false, true])
            df_combined = unique(df_combined, [:meta_observed_at])
            sort!(df_combined, "meta_observed_at", rev = true)

            # Save and send
            CSV.write(output_path, df_combined)
                    upload_file_to_s3(output_path, "s3://touringplans_stats/stats_work/attraction-io/forecasts/$(base_filename)")
        end
    end

end

function main(attraction::Attraction)
    entity_code = attraction.code
    wait_time_types = attraction.queue_type == "priority" ? ["priority"] : ["POSTED", "ACTUAL"]

    # Ensure at least one file exists
    any_file_exists = any(wait_time_type -> begin
        wt_lower = lowercase(wait_time_type)
        local_file_path = "output/wait_times_$(entity_code)_$(wt_lower).csv"
        isfile(local_file_path)
    end, wait_time_types)

    if !any_file_exists
        # @info("No wait time files found for $entity_code. Cannot proceed with predictions.")
        return nothing
    end

    already_logged = true
    for wt_type in wait_time_types
        wt_lower = lowercase(wt_type)
        base_filename = "forecasts_$(entity_code)_$(wt_lower).csv"
        output_path = joinpath("output", base_filename)
        if !isfile(output_path)
            already_logged = false
            break
        end
    end

    if already_logged
        return
    end

    run_entity_forecast_logger(attraction)
end

main(ATTRACTION)