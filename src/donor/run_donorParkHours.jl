# =======================================================================================
# 🏰 run_donorParkHours.jl — Build Donors Table for Park Hours
# =======================================================================================

using Dates, CSV, DataFrames, TimeZones

# -------------------------------------------------------
# Default constants for fallback imputation
# -------------------------------------------------------
const DEFAULT_OPEN_TIME = Time(9, 0)
const DEFAULT_CLOSE_TIME = Time(21, 0)
const DEFAULT_OPEN_TIME_WITH_EMH = Time(9, 0)
const DEFAULT_CLOSE_TIME_WITH_EMH_OR_PARTY = Time(21, 0)
const DEFAULT_EMH_MORNING = 0
const DEFAULT_EMH_EVENING = 0


# -------------------------------------------------------
# Helper: Row-wise donor imputation by park_code + date_group_id
# -------------------------------------------------------
function impute_missing_park_hours!(df::DataFrame)::DataFrame
    sort!(df, [:park_code, :park_day_id])
    group = groupby(df, :park_code)
    total_imputed = 0

    donor_cols = [:opening_time, :closing_time,
                  :opening_time_with_emh, :closing_time_with_emh_or_party,
                  :emh_morning, :emh_evening]

    for subdf in group
        for i in 1:nrow(subdf)
            if ismissing(subdf.opening_time[i])
                this_date = subdf.park_day_id[i]
                this_dgid = subdf.date_group_id[i]

                donor_idx = findlast(j -> !ismissing(subdf.opening_time[j]) &&
                                         subdf.date_group_id[j] == this_dgid &&
                                         subdf.park_day_id[j] < this_date &&
                                         subdf.is_valid_donor[j],
                                     1:i-1)

                if !isnothing(donor_idx)
                    for col in donor_cols
                        subdf[i, col] = subdf[donor_idx, col]
                    end
                else
                    tz = get_timezone_for_park(subdf.park_code[i])
                    subdf[i, :opening_time]                   = ZonedDateTime(DateTime(this_date + DEFAULT_OPEN_TIME), tz)
                    subdf[i, :closing_time]                   = ZonedDateTime(DateTime(this_date + DEFAULT_CLOSE_TIME), tz)
                    subdf[i, :opening_time_with_emh]          = ZonedDateTime(DateTime(this_date + DEFAULT_OPEN_TIME_WITH_EMH), tz)
                    subdf[i, :closing_time_with_emh_or_party] = ZonedDateTime(DateTime(this_date + DEFAULT_CLOSE_TIME_WITH_EMH_OR_PARTY), tz)
                    subdf[i, :emh_morning]                    = DEFAULT_EMH_MORNING
                    subdf[i, :emh_evening]                    = DEFAULT_EMH_EVENING
                end
                total_imputed += 1
            end
        end
    end

    return vcat(group...)
end

function reset_datetime_to_datepart(dt::ZonedDateTime, new_date::Date)
    t = Time(dt)
    return ZonedDateTime(DateTime(new_date + t), dt.zone)
end

function main()

    output_folder = "work/donor"
    if !isdir(output_folder)
        mkdir(output_folder)
    end
    output_file = "work/donor/donorparkhours.csv"
    
    # If the output file already exists, skip processing
    # TODO: Remove this check if you want to always regenerate
    if isfile(output_file)
        return
    else 
        # @info("Creating a donor park hours table...")
    end

    dimdate    = CSV.read("work/dim/dimdate.csv", DataFrame)
    park_hours = CSV.read("work/dim/dimparkhours.csv", DataFrame)
    dgid       = CSV.read("work/dim/dimdategroupid.csv", DataFrame)

    # Parse ISO8601 datetime columns
    for col in [:opening_time, :closing_time, :opening_time_with_emh, :closing_time_with_emh_or_party]
        park_hours[!, col] = parse_zoneddatetimes_simple(park_hours[!, col])
    end

    parks = unique(park_hours.park_code)
    dimdate_expanded = DataFrame(
        park_day_id = repeat(dimdate.park_day_id, outer = length(parks)),
        park_code   = repeat(parks, inner = nrow(dimdate))
    )

    park_hours_expanded = leftjoin(dimdate_expanded, park_hours, on = [:park_day_id, :park_code])
    df = leftjoin(park_hours_expanded, dgid, on = :park_day_id)

    # --------------------------------------------------------------------
    # 🛠️ Promote park hour columns to accept ZonedDateTime + Missing
    # --------------------------------------------------------------------
    for col in [:opening_time, :closing_time, :opening_time_with_emh, :closing_time_with_emh_or_party]
        if eltype(df[!, col]) == Missing
            df[!, col] = Vector{Union{ZonedDateTime, Missing}}(df[!, col])
        end
    end

    for col in [:emh_morning, :emh_evening]
        if eltype(df[!, col]) == Missing
            df[!, col] = Vector{Union{Int, Missing}}(df[!, col])
        end
    end

    # Flag valid donors
    df.is_valid_donor = map((open, close) ->
        ismissing(open) || ismissing(close) ? false :
        (hour(open) ≥ 8) && !(14 ≤ hour(close) < 18),
        df.opening_time, df.closing_time)

    # Impute
    df = impute_missing_park_hours!(df)

    # Normalize datetime to use park_day_id as date part
    df.opening_time                   = reset_datetime_to_datepart.(df.opening_time, df.park_day_id)
    df.closing_time                   = reset_datetime_to_datepart.(df.closing_time, df.park_day_id)
    df.opening_time_with_emh          = reset_datetime_to_datepart.(df.opening_time_with_emh, df.park_day_id)
    df.closing_time_with_emh_or_party = reset_datetime_to_datepart.(df.closing_time_with_emh_or_party, df.park_day_id)

    # Fix closing times after midnight
    df.closing_time[df.closing_time .< df.opening_time] .+= Day(1)
    df.closing_time_with_emh_or_party[df.closing_time_with_emh_or_party .< df.opening_time] .+= Day(1)

    # Compute float hours
    df.opening_hour = [ismissing(x) ? missing : hour(x) + minute(x)/60 for x in df.opening_time]
    df.closing_hour = [ismissing(x) ? missing : hour(x) + minute(x)/60 for x in df.closing_time]
    df.hours_open   = round.([(close - open) / Hour(1)
                              for (open, close) in zip(df.opening_time, df.closing_time)]; digits=2)

    select!(df, Not(:date_group_id))
    CSV.write(output_file, df)
end

main()
