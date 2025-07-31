# ----------------------------------------------------------------------------- #
# run_dailyavgs.jl - Calculate daily average posted times between 11am and 5pm  #
# ----------------------------------------------------------------------------- #

using JSON3, Dates, DataFrames, CSV, Statistics, TimeZones

const START_HOUR = 11
const END_HOUR = 17

# -------------------------------------------------------
# Step 1: Compute daily averages
# -------------------------------------------------------
function compute_daily_averages(outpath::String)

    forecast_path = "output/forecasts_$(uppercase(ATTRACTION.code))_posted.csv"
    output_path = outpath
    mkpath(dirname(output_path))

    if !isfile(forecast_path)
        # @warn("❌ Forecast file not found at $forecast_path")
        return
    end

    df = CSV.read(forecast_path, DataFrame)
    df.meta_observed_at = parse_zoneddatetimes_simple(df.meta_observed_at)

    required_cols = ["meta_observed_at", "id_entity_code", "predicted_wait_time", "id_park_day_id"]
    if !all(col -> col in names(df), required_cols)
        # @warn("❌ Forecast file missing one or more required columns.")
        return
    end

    # Filter to 11am–5pm window
    df = filter(row -> (hour(row.meta_observed_at) ≥ START_HOUR && hour(row.meta_observed_at) ≤ END_HOUR), df)

    # Group by day + entity and compute mean
    grouped = combine(groupby(df, [:id_park_day_id, :id_entity_code]),
                      :predicted_wait_time => mean => :avg_posted_11am_to_5pm)

    # Round to 1 decimal
    grouped.avg_posted_11am_to_5pm = round.(grouped.avg_posted_11am_to_5pm, digits=1)

    # Sort + rename
    sort!(grouped, [:id_park_day_id, :id_entity_code])
    rename!(grouped, :id_entity_code => "entity_code")

    CSV.write(output_path, grouped)
    # @info("📤 Wrote daily averages to $output_path")

end

# -------------------------------------------------------
# Step 3: Main
# -------------------------------------------------------
function main(outpath::String)
    
    # Check if forecasts_dailyavgs.csv already exists
    if isfile(outpath)
        # @info("🛑 forecasts_dailyavgs.csv already exists. Skipping sync.")
        return
    end

    compute_daily_averages(outpath)
end

outpath = "work/$(uppercase(ATTRACTION.code))/calendar/forecasts_dailyavgs.csv"
main(outpath)
