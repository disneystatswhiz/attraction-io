# -------------------------------------------------------------------- #
# run_writer.jl - Format & send forecast logs to S3 for site use       #
# Modern, explicit pipeline version                                    #
# -------------------------------------------------------------------- #

using Dates, CSV, DataFrames, TimeZones

function get_site_timezone(property::String)::TimeZone
    return property == "WDW" || property == "UOR" ? TimeZone("America/New_York") :
           property == "DLR" || property == "USH" ? TimeZone("America/Los_Angeles") :
           property == "TDR"                     ? TimeZone("Asia/Tokyo") :
           error("❌ Unknown property: $property")
end

function format_with_tz(dt::DateTime, tz::TimeZone)::String
    return string(ZonedDateTime(dt, tz))
end

# Merge posted + actual into standby site ingest format
function merge_standby_forecasts(df_posted::DataFrame, df_actual::DataFrame)
    df_posted.meta_observed_at = DateTime.(df_posted.meta_observed_at)
    df_actual.meta_observed_at = DateTime.(df_actual.meta_observed_at)
    df_merged = innerjoin(df_posted, df_actual, on = [:meta_observed_at, :id_entity_code], makeunique=true)
    rename!(df_merged, Dict(
        :predicted_wait_time => :posted_time,
        :predicted_wait_time_1 => :actual_time,
        :id_entity_code => :entity_code
    ))
    df_merged.status_code = fill("", nrow(df_merged))
    df_merged.forecast_at = df_merged.meta_observed_at
    df_merged.date = Date.(df_merged.id_park_day_id)
    return df_merged
end

function write_standby_site_files(attraction::Attraction, output_folder::String)
    entity_code = attraction.code
    property = attraction.property
    tz = get_site_timezone(property)

    file_posted = joinpath(output_folder, "forecasts_$(entity_code)_posted.csv")
    file_actual = joinpath(output_folder, "forecasts_$(entity_code)_actual.csv")
    if !(isfile(file_posted) && isfile(file_actual))
        # @warn "⚠️ Missing posted or actual forecast file for $entity_code — skipping standby site output."
        return
    end

    df_posted = CSV.read(file_posted, DataFrame)
    df_actual = CSV.read(file_actual, DataFrame)
    df = merge_standby_forecasts(df_posted, df_actual)
    sort!(df, [:meta_observed_at])

    df.__month = month.(df.id_park_day_id)
    df.__year  = year.(df.id_park_day_id)

    for g in groupby(df, [:__year, :__month])
        y, m = first(g.__year), first(g.__month)
        mm_str = lpad(string(m), 2, '0')
        filename = "forecast_$(lowercase(entity_code))_$(mm_str)_$(y).csv"
        path = joinpath(output_folder, filename)

        site_df = select(g,
            :date,
            :entity_code,
            :posted_time,
            :actual_time,
            :status_code,
            :forecast_at => ByRow(dt -> format_with_tz(dt, tz)) => :forecast_at
        )

        # Enforce schema
        expected_cols = [:date, :entity_code, :posted_time, :actual_time, :status_code, :forecast_at]
        @assert names(site_df) == expected_cols "❌ Columns do not match required ingest schema!"

        CSV.write(path, site_df)
        s3_path = "s3://touringplans_stats/stats_work/attraction-io/site_ingest/$(filename)"
        upload_file_to_s3(path, s3_path)
        # @info "💾 Wrote and uploaded standby site forecast chunk: $path → $s3_path"
    end
end

function write_priority_site_files(attraction::Attraction, output_folder::String)
    entity_code = attraction.code
    property = attraction.property
    tz = get_site_timezone(property)
    file_priority = joinpath(output_folder, "forecasts_$(entity_code)_priority.csv")
    if !isfile(file_priority)
        # @warn "⚠️ No priority forecast file for $entity_code — skipping priority site output."
        return
    end

    df = CSV.read(file_priority, DataFrame)
    sort!(df, [:meta_observed_at])
    df.meta_observed_at = DateTime.(df.meta_observed_at)
    df.__month = month.(df.id_park_day_id)
    df.__year  = year.(df.id_park_day_id)

    for g in groupby(df, [:__year, :__month])
        y, m = first(g.__year), first(g.__month)
        mm_str = lpad(string(m), 2, '0')
        filename = "forecast_$(lowercase(entity_code))_$(mm_str)_$(y).csv"
        path = joinpath(output_folder, filename)

        site_df = DataFrame(
            date = Date.(g.id_park_day_id),
            entity_code = g.id_entity_code,
            minutes_until_return = g.predicted_wait_time,
            status_code = fill("", nrow(g)),
            forecast_at = format_with_tz.(g.meta_observed_at, tz)
        )
        site_df = select(site_df, :date, :entity_code, :minutes_until_return, :status_code, :forecast_at)
        expected_cols = [:date, :entity_code, :minutes_until_return, :status_code, :forecast_at]
        @assert names(site_df) == expected_cols "❌ Columns do not match required ingest schema!"

        CSV.write(path, site_df)
        s3_path = "s3://touringplans_stats/stats_work/attraction-io/site_ingest/$(filename)"
        upload_file_to_s3(path, s3_path)
        # @info "💾 Wrote and uploaded priority site forecast chunk: $path → $s3_path"
    end
end

function main(attraction::Attraction, output_folder::String)
    # @info "📝 Formatting final site outputs for $(attraction.code) ($(attraction.queue_type))"
    if attraction.queue_type == "standby"
        write_standby_site_files(attraction, output_folder)
    elseif attraction.queue_type == "priority"
        write_priority_site_files(attraction, output_folder)
    else
        error("❌ Unknown queue_type: $(attraction.queue_type). Must be 'standby' or 'priority'")
    end
    # @info "✅ All site forecast files written and uploaded for $(attraction.code)"
end

# Usage (example):
# main(ATTRACTION, LOC_OUTPUT)

