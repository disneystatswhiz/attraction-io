# --------------------------------------------------------------------------------------- #
# run_futuredates.jl - Add missing wait times for future dates to be featured and encoded
# --------------------------------------------------------------------------------------- #

using Dates, CSV, DataFrames, TimeZones

# ---------------------------------------------------------
# Helper: Generate future forecast times for a given property
# ---------------------------------------------------------
# This function generates a DataFrame with forecast times for the next 2 years
# based on the property's timezone and the current date.
# It creates 15-minute intervals starting from 6 AM of the current day or the next day
# if the current time is past 6 AM.
# ----------------------------------------------------------
# arg: property::String - The property code (e.g., "WDW", "UOR", "DLR", "USH", "TDR")
# returns: DataFrame with columns `park_day_id` and `observed_at`
#          `park_day_id` is the date in YYYY-MM-DD format   
#          `observed_at` is the DateTime in the property's timezone
# ----------------------------------------------------------
function generate_future_forecast_times(attraction::Attraction)::DataFrame
    tz = attraction.timezone

    # Determine date range
    today_local = Date(ZonedDateTime(now(), tz))
    start_date = (hour(ZonedDateTime(now(), tz)) < 6) ? today_local : today_local + Day(1)
    end_date = today_local > Date(year(today_local), 8, 31) ?
        Date(year(today_local) + 2, 12, 31) :
        Date(year(today_local) + 1, 12, 31)

    # Generate all 15-min intervals across all days
    observed_at = ZonedDateTime[]
    park_day_id = Date[]

    for day in start_date:end_date
        base_time = ZonedDateTime(DateTime(day, Time(6, 0)), tz)

        # Cover until 3:00 AM the following day
        final_time = ZonedDateTime(DateTime(day + Day(1), Time(3, 0)), tz)

        while base_time <= final_time
            push!(observed_at, base_time)
            push!(park_day_id, Date(base_time))  # Use local date, not UTC date
            base_time += Minute(15)
        end
    end

    return DataFrame(park_day_id = park_day_id, observed_at = observed_at)
end

function main(attraction::Attraction)
    input_folder = joinpath(LOC_WORK, uppercase(attraction.code), "wait_times")
    input_file = joinpath(input_folder, "wait_times.csv")
    output_file = joinpath(input_folder, "future.csv")
    wait_time_types = attraction.queue_type == "priority" ? ["PRIORITY"] : ["POSTED", "ACTUAL"]

    if !isfile(input_file)
        # @info("❌ Now new rows for $input_file")
        return
    end

    # Load existing
    df_existing = CSV.read(input_file, DataFrame)
    df_existing.observed_at = parse_zoneddatetimes_simple(df_existing.observed_at)

    # Generate future rows (all with ZonedDateTime observed_at)
    forecast_times = generate_future_forecast_times(attraction)

    future_rows = vcat([
        DataFrame(
            entity_code = fill(attraction.code, nrow(forecast_times)),
            observed_at = forecast_times.observed_at,  # Already ZonedDateTime
            observed_wait_time = fill(missing, nrow(forecast_times)),
            wait_time_type = fill(wt_type, nrow(forecast_times))
        ) for wt_type in wait_time_types
    ]...)

    # Combine + dedupe
    common_cols = intersect(names(df_existing), names(future_rows))
    df_existing = select(df_existing, common_cols)
    future_rows = select(future_rows, common_cols)
    df_all = vcat(df_existing, future_rows)
    df_all = unique(df_all, [:entity_code, :observed_at, :wait_time_type])
    sort!(df_all, :observed_at)

    # Write
    CSV.write(output_file, df_all)
    return
end

main(ATTRACTION)
