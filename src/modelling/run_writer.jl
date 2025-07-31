# --------------------------------------------------------------------
# run_writer.jl - Format & send forecast logs to S3 for site use
# --------------------------------------------------------------------

using Dates, CSV, DataFrames, JSON3, TimeZones

# --------------------------------------------------
# Resolve S3 upload target from config
# --------------------------------------------------
function resolve_s3_target(config::Dict, queue_type::String, park_code::String)::String
    root_key   = config["_PARAMETERS"]["send_forecasts_to"]
    queue_type = uppercase(queue_type)
    park_code  = lowercase(park_code)

    base_path = get(config["_S3_OUTPUTS"], "forecasts_to_$(lowercase(root_key))", nothing)
    base_path === nothing && error("❌ Unknown send_forecasts_to root: $root_key")

    s3_path =
        if root_key == "STAGING"
            queue_type == "STANDBY"  ? join([base_path, "standby"], "/") :
            queue_type == "PRIORITY" ? join([base_path, "fp_next_available", park_code], "/") :
            error("❌ Unknown queue_type: $queue_type")

        elseif root_key == "PRODUCTION"
            queue_type == "STANDBY"  ? join([base_path, "standby", park_code], "/") :
            queue_type == "PRIORITY" ? join([base_path, "fp_next_available", park_code], "/") :
            error("❌ Unknown queue_type: $queue_type")

        else
            base_path  # Fallback
        end

    return replace(s3_path, "\\" => "/")
end


# --------------------------------------------------
# Upload local forecast file to a full S3 path using send_to_s3
# --------------------------------------------------
function send_forecast_to_full_s3_path(config::Dict, local_group::String, local_key::String, s3_path::String, filename::String)
    temp_key = "__TEMP_S3_TARGET__"
    config["_S3_OUTPUTS"][temp_key] = s3_path
    send_to_s3(config, local_group, local_key, temp_key, filename)
    delete!(config["_S3_OUTPUTS"], temp_key)  # Clean up
end

# --------------------------------------------------
# Map property to correct TimeZone object
# --------------------------------------------------
function get_timezone(property::String)::TimeZone
    return property == "WDW" || property == "UOR" ? TimeZone("America/New_York") :
           property == "DLR" || property == "USH" ? TimeZone("America/Los_Angeles") :
           property == "TDL"                     ? TimeZone("Asia/Tokyo") :
           error("❌ Unknown property: $property")
end

# --------------------------------------------------
# Format datetime in ISO 8601 format with timezone
# --------------------------------------------------
function format_with_tz(dt::DateTime, tz::TimeZone)::String
    return string(ZonedDateTime(dt, tz))
end

# --------------------------------------------------
# Merge posted + actual forecasts into standby format
# --------------------------------------------------
function merge_standby_forecasts(df_posted::DataFrame, df_actual::DataFrame)::DataFrame
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

# --------------------------------------------------
# Write standby forecast files
# --------------------------------------------------
function write_standby_forecasts(config::Dict, tz::TimeZone)
    entity_code   = config["_ENTITY"]
    output_folder = config["_LOCAL"]["output_folder"]
    queue_type = config["_PARSING"]["queue_type"]
    park_code  = config["_PARSING"]["park_code"]
    target_s3  = resolve_s3_target(config, queue_type, park_code)

    # @info("🔀 Merging POSTED and ACTUAL forecasts for standby queue...")

    file_posted = joinpath(output_folder, "forecasts_$(entity_code)_posted.csv")
    file_actual = joinpath(output_folder, "forecasts_$(entity_code)_actual.csv")

    if !isfile(file_posted) || !isfile(file_actual)
        # @warn("⚠️ Missing posted or actual forecast file — skipping standby merge.")
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

        CSV.write(path, site_df)
        # @success("💾 Wrote standby forecast chunk to $path")
        send_forecast_to_full_s3_path(config, "_LOCAL", "output_folder", target_s3, filename)
    end
end

# --------------------------------------------------
# Write priority forecast files
# --------------------------------------------------
function write_priority_forecasts(config::Dict, tz::TimeZone)
    entity_code   = config["_ENTITY"]
    output_folder = config["_LOCAL"]["output_folder"]
    queue_type = config["_PARSING"]["queue_type"]
    park_code  = config["_PARSING"]["park_code"]
    target_s3  = resolve_s3_target(config, queue_type, park_code)

    # @info("📦 Handling PRIORITY forecasts for $entity_code...")

    file_priority = joinpath(output_folder, "forecasts_$(entity_code)_priority.csv")
    if !isfile(file_priority)
        # @warn("⚠️ No priority forecast file found — skipping.")
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

        CSV.write(path, site_df)
        # @success("💾 Wrote priority forecast chunk to $path")
        send_forecast_to_full_s3_path(config, "_LOCAL", "output_folder", target_s3, filename)

    end
end

# --------------------------------------------------
# Entrypoint
# --------------------------------------------------
function main(config::Dict)
    entity_code = config["_ENTITY"]
    queue_type  = config["_PARSING"]["queue_type"]
    property    = config["_PROPERTY"]
    tz          = get_timezone(property)

    # @header("📝 Formatting final site outputs", config=config)

    if queue_type == "STANDBY"
        write_standby_forecasts(config, tz)
    elseif queue_type == "PRIORITY"
        write_priority_forecasts(config, tz)
    else
        error("❌ Unknown queue_type: $queue_type. Must be 'STANDBY' or 'PRIORITY'")
    end

    # @success("✅ All site forecast files written and uploaded.")
end

# --------------------------------------------------
# Run it
# --------------------------------------------------
main(config)
