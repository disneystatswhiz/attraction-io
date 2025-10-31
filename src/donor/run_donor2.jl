# =======================================================================================
# 🏰 run_donorParkHours.jl — Build Donors Table for Park Hours (+ single sources file)
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
# Helper: Reset a ZonedDateTime's date-part to a given Date
# -------------------------------------------------------
function reset_datetime_to_datepart(dt::ZonedDateTime, new_date::Date)
    t = Time(dt)
    return ZonedDateTime(DateTime(new_date + t), dt.zone)
end

# -------------------------------------------------------
# Core imputation with source logging
#   - df: working DataFrame containing park hours + date_group_id
#   - sources_log: DataFrame to be populated with one row per (park_code, park_date)
#                  with columns: park_code, park_date, donor_date, source
# -------------------------------------------------------
function impute_missing_park_hours!(df::DataFrame, sources_log::DataFrame)::DataFrame
    sort!(df, [:park_code, :park_date])
    group = groupby(df, :park_code)

    donor_cols = [:opening_time, :closing_time,
                  :opening_time_with_emh, :closing_time_with_emh_or_party,
                  :emh_morning, :emh_evening]

    # For rows that ALREADY have published hours (both opening/closing present), log PUBLISHED up front.
    # We log once per row overall; imputation only runs for rows with missing opening_time, so no double-logging.
    for i in 1:nrow(df)
        if !ismissing(df.opening_time[i]) && !ismissing(df.closing_time[i])
            push!(sources_log, (
                park_code  = String(df.park_code[i]),
                park_date  = Date(df.park_date[i]),
                donor_date = missing,
                source     = "OFFICIAL",
            ))
        end
    end

    # Impute rows that are missing opening_time (matches your original behavior)
    for subdf in group
        for i in 1:nrow(subdf)
            if ismissing(subdf.opening_time[i])
                this_date = subdf.park_date[i]
                this_dgid = subdf.date_group_id[i]

                donor_idx = findlast(j ->
                        !ismissing(subdf.opening_time[j]) &&
                        subdf.date_group_id[j] == this_dgid &&
                        subdf.park_date[j] < this_date &&
                        subdf.is_valid_donor[j],
                    1:i-1)

                if !isnothing(donor_idx)
                    # Copy donor fields
                    for col in donor_cols
                        subdf[i, col] = subdf[donor_idx, col]
                    end
                    # Log as DONOR
                    push!(sources_log, (
                        park_code  = String(subdf.park_code[i]),
                        park_date  = Date(this_date),
                        donor_date = Date(subdf.park_date[donor_idx]),
                        source     = "DONOR",
                    ))
                else
                    # Defaults in local park timezone
                    tz = get_timezone_for_park(subdf.park_code[i])
                    subdf[i, :opening_time]                   = ZonedDateTime(DateTime(this_date + DEFAULT_OPEN_TIME), tz)
                    subdf[i, :closing_time]                   = ZonedDateTime(DateTime(this_date + DEFAULT_CLOSE_TIME), tz)
                    subdf[i, :opening_time_with_emh]          = ZonedDateTime(DateTime(this_date + DEFAULT_OPEN_TIME_WITH_EMH), tz)
                    subdf[i, :closing_time_with_emh_or_party] = ZonedDateTime(DateTime(this_date + DEFAULT_CLOSE_TIME_WITH_EMH_OR_PARTY), tz)
                    subdf[i, :emh_morning]                    = DEFAULT_EMH_MORNING
                    subdf[i, :emh_evening]                    = DEFAULT_EMH_EVENING

                    # Log as DEFAULT (no donor)
                    push!(sources_log, (
                        park_code  = String(subdf.park_code[i]),
                        park_date  = Date(this_date),
                        donor_date = missing,
                        source     = "DEFAULT",
                    ))
                end
            end
        end
    end

    return vcat(group...)
end

# -------------------------------------------------------
# Main
# -------------------------------------------------------
function main()

    # Output locations
    output_folder = LOC_DONOR
    if !isdir(output_folder)
        mkdir(output_folder)
    end

    output_file_hours   = joinpath(LOC_DONOR, "donorparkhours2.csv")
    output_file_sources = joinpath(LOC_DONOR, "donorparkhours_sources.csv")

    # If hours table already exists, skip (keeps your current short-circuit)
    if isfile(output_file_hours)
        return
    end

    # --- Read inputs
    dimdate    = CSV.read(joinpath(LOC_DIM, "dimdate.csv"), DataFrame)
    park_hours = CSV.read(joinpath(LOC_DIM, "dimparkhours.csv"), DataFrame)
    dgid       = CSV.read(joinpath(LOC_DIM, "dimdategroupid.csv"), DataFrame)

    # Parse ISO8601 datetime columns (helper assumed to exist)
    for col in [:opening_time, :closing_time, :opening_time_with_emh, :closing_time_with_emh_or_party]
        park_hours[!, col] = parse_zoneddatetimes_simple(park_hours[!, col])
    end

    # --- Expand to full grid (park_code x park_date)
    parks = unique(park_hours.park_code)
    dimdate_expanded = DataFrame(
        park_date = repeat(dimdate.park_date, outer = length(parks)),
        park_code = repeat(parks, inner = nrow(dimdate))
    )

    park_hours_expanded = leftjoin(dimdate_expanded, park_hours, on = [:park_date, :park_code])
    df = leftjoin(park_hours_expanded, dgid, on = :park_date)

    # --- Promote columns to accept Missing where needed
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

    # --- Flag valid donors (exclude partial or abnormal days)
    df.is_valid_donor = map((open, close) ->
        ismissing(open) || ismissing(close) ? false :
        (hour(open) ≥ 8) && !(14 ≤ hour(close) < 18),
        df.opening_time, df.closing_time)

    # --- Prepare sources log (single file you asked for)
    sources_log = DataFrame(
        park_code  = String[],
        park_date  = Date[],
        donor_date = Union{Missing,Date}[],
        source     = String[],   # PUBLISHED, DONOR, DEFAULT
    )

    # --- Impute (and log sources)
    df = impute_missing_park_hours!(df, sources_log)

    # --- Normalize datetime to park_date
    df.opening_time                   = reset_datetime_to_datepart.(df.opening_time, df.park_date)
    df.closing_time                   = reset_datetime_to_datepart.(df.closing_time, df.park_date)
    df.opening_time_with_emh          = reset_datetime_to_datepart.(df.opening_time_with_emh, df.park_date)
    df.closing_time_with_emh_or_party = reset_datetime_to_datepart.(df.closing_time_with_emh_or_party, df.park_date)

    # --- Fix cross-midnight closes
    df.closing_time[df.closing_time .< df.opening_time] .+= Day(1)
    df.closing_time_with_emh_or_party[df.closing_time_with_emh_or_party .< df.opening_time] .+= Day(1)

    # --- Derive convenience metrics
    df.opening_hour = [ismissing(x) ? missing : hour(x) + minute(x)/60 for x in df.opening_time]
    df.closing_hour = [ismissing(x) ? missing : hour(x) + minute(x)/60 for x in df.closing_time]
    df.hours_open   = round.([(close - open) / Hour(1)
                              for (open, close) in zip(df.opening_time, df.closing_time)]; digits=2)

    # --- Write outputs
    select!(df, Not(:date_group_id))  # drop dgid from final hours table
    CSV.write(output_file_hours, df)

    # Ensure the sources log has exactly one row per (park_code, park_date).
    # Because we only log PUBLISHED up-front and DONOR/DEFAULT during imputation (for missing only),
    # this will already be true; but we’ll dedupe defensively just in case.
    unique!(sources_log, [:park_code, :park_date])

    # Write the single sources file you requested
    CSV.write(output_file_sources, sources_log)

    # --- Upload to S3
    #upload_file_to_s3(output_file_hours,   "s3://touringplans_stats/stats_work/dimension_tables/donorparkhours2.csv")
    #upload_file_to_s3(output_file_sources, "s3://touringplans_stats/stats_work/dimension_tables/donorparkhours_sources.csv")

end

main()
