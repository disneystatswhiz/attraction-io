# -------------------------------------------------------------------- #
# run_predictions.jl - Log and sync scored forecasts                   #
# -------------------------------------------------------------------- #

using Dates, CSV, DataFrames
const POSTED_ACTUAL_RATIO = 0.78

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

function get_forecast_df(entity_code::String)::Dict{String, Union{DataFrame, Nothing}}
    temp_folder = joinpath(LOC_WORK, entity_code, "wait_times")
    result = Dict{String, Union{DataFrame, Nothing}}(
        "POSTED" => nothing, 
        "ACTUAL" => nothing, 
        "PRIORITY" => nothing
    )

    for wt in ["POSTED", "ACTUAL", "PRIORITY"]
        path = joinpath(temp_folder, "scored_$(lowercase(wt)).csv")
        if isfile(path)
            df = CSV.read(path, DataFrame)
            df.meta_observed_at = parse_zoneddatetimes_simple(df.meta_observed_at)
            df.predicted_wait_time = round_wait_time.(df.predicted_wait_time, lowercase(wt))
            df.meta_wait_time_type = fill(wt, nrow(df))
            result[wt] = df
        end
    end

    # Impute ACTUAL if missing, using POSTED (classic logic)
    if result["ACTUAL"] === nothing && result["POSTED"] !== nothing
        df_posted = deepcopy(result["POSTED"])
        df_posted.predicted_wait_time .= round_wait_time.(df_posted.predicted_wait_time .* POSTED_ACTUAL_RATIO, "actual")
        df_posted.meta_wait_time_type .= "ACTUAL"
        result["ACTUAL"] = df_posted
    end

    # Impute POSTED if missing, using ACTUAL (classic logic)
    if result["POSTED"] === nothing && result["ACTUAL"] !== nothing
        df_actual = deepcopy(result["ACTUAL"])
        df_actual.predicted_wait_time .= round_wait_time.(df_actual.predicted_wait_time ./ POSTED_ACTUAL_RATIO, "posted")
        df_actual.meta_wait_time_type .= "POSTED"
        result["POSTED"] = df_actual
    end

    return result
end

function run_entity_forecast_logger(attraction::Attraction)
    entity_code = attraction.code
    model_name = "XGBoost"
    today_str = string(TODAY_DATE)

    forecast_dict = get_forecast_df(entity_code)

    # If all forecast types are missing, skip this entity
    if all(x -> x === nothing, values(forecast_dict))
        # @warn "⏭️ Skipping $entity_code — no forecasts available or imputed."
        return
    end

    for wt_type in keys(forecast_dict)
        df_new = forecast_dict[wt_type]
        isnothing(df_new) && continue  # No new forecasts of this type

        wt_lower = lowercase(wt_type)
        base_filename = "forecasts_$(entity_code)_$(wt_lower).csv"
        already_on_s3_file = joinpath(LOC_WORK, entity_code, "already_on_s3", "forecasts_$(uppercase(ATTRACTION.code))_$(wt_lower).csv")
        output_path = joinpath(LOC_OUTPUT, entity_code, base_filename)

        # Load existing forecasts from already_on_s3 (just synced earlier in pipeline)
        df_existing = isfile(already_on_s3_file) ? CSV.read(already_on_s3_file, DataFrame) : DataFrame()
        if !isempty(df_existing)
            df_existing.meta_observed_at = parse_zoneddatetimes_simple(df_existing.meta_observed_at)
        end

        # Annotate new rows
        df_new.model_name = fill(model_name, nrow(df_new))
        df_new.model_run_date = fill(today_str, nrow(df_new))

        # Combine and dedupe: keep the most recent model_run_date for each meta_observed_at
        df_combined = vcat(df_existing, df_new; cols = :union)
        df_combined.model_run_date = Date.(df_combined.model_run_date)
        sort!(df_combined, [:meta_observed_at, :model_run_date], rev = [false, true])
        df_combined = unique(df_combined, [:meta_observed_at])
        sort!(df_combined, "meta_observed_at", rev = true)

        # Write combined file and upload to S3
        CSV.write(output_path, df_combined)
        upload_file_to_s3(output_path, "s3://touringplans_stats/stats_work/attraction-io/forecasts/$(base_filename)")
    end
end


function main(attraction::Attraction)
    entity_code = attraction.code
    wait_time_types = attraction.queue_type == "priority" ? ["PRIORITY"] : ["POSTED", "ACTUAL"]

    # Ensure at least one file exists
    any_file_exists = any(wait_time_type -> begin
        wt_lower = lowercase(wait_time_type)
        local_file_path = joinpath(LOC_WORK, entity_code, "wait_times", "scored_$(wt_lower).csv")
        isfile(local_file_path)
    end, wait_time_types)

    if !any_file_exists
        return nothing
    end

    already_logged = true
    for wt_type in wait_time_types
        wt_lower = lowercase(wt_type)
        base_filename = "forecasts_$(entity_code)_$(wt_lower).csv"
        output_path = joinpath(LOC_OUTPUT, entity_code, base_filename)
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