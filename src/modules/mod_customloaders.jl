module CustomLoaders

using CSV, DataFrames, Dates, Statistics, TimeZones

export process_all_wait_time_files,
       process_new_fastpass_file,
       process_old_fastpass_file,
       process_standby_file,
       get_wait_time_filetype,
       format_columns!

# ----------------------------------------------------------- #
# Format columns for consistency                              #
# ----------------------------------------------------------- #
function format_columns!(df::DataFrame)::DataFrame
    # --- Step 0: Ensure entity_code is uppercase
    if :entity_code in names(df)
        df.entity_code = uppercase.(df.entity_code)
    end

    # --- Step 1: If observed_at is a string, try to parse it to DateTime
    if :observed_at in names(df) && eltype(df.observed_at) <: AbstractString
        df.observed_at = [
            try
                DateTime(obs[1:4] * "-" * obs[6:7] * "-" * obs[9:10] * "T" * obs[12:13] * ":" * obs[15:16])
            catch
                missing
            end
            for obs in df.observed_at
        ]
    end
    return df
end


# ----------------------------------------------------------- #
# 🔍 Identify wait time file type based on queue type + name
# ----------------------------------------------------------- #
function get_wait_time_filetype(csv_filename_path::String, queue_type::String)::Union{String, Nothing}
    filename = lowercase(basename(csv_filename_path))

    if queue_type == "standby"
        return "Standby"

    elseif queue_type == "priority"
        if occursin("_2012", filename) || 
           occursin("_2013", filename) || 
           occursin("_2014", filename) || 
           occursin("_2015", filename) || 
           occursin("_2016", filename) || 
           occursin("_2017", filename) || 
           occursin("_2018", filename) || 
           occursin("_2019_01", filename) || 
           occursin("_2019_02", filename) || 
           occursin("_201901", filename) || 
           occursin("_201902", filename)
            return "Old Fastpass"
        else
            return "New Fastpass"
        end
    end

    @warn "❓ Could not determine file type from: $csv_filename_path"
    return nothing
end


# ---------------------------------------------------- #
# Main entry point to process any fastpass file type   #
# ---------------------------------------------------- #
function process_all_wait_time_files(path::String, entity_code::String, queue_type::String)::DataFrame
    file_type = get_wait_time_filetype(path, queue_type)

    if file_type == "New Fastpass"
        return process_new_fastpass_file(path, entity_code)
    elseif file_type == "Old Fastpass"
        return process_old_fastpass_file(path, entity_code)
    elseif file_type == "Standby"
        return process_standby_file(path, entity_code)
    else
        @warn "⚠️ Skipping unrecognized file type for $queue_type: $path"
        return DataFrame()
    end
end


# ----------------------------------------- #
# Function to process the new fastpass file #
# ----------------------------------------- #
function process_new_fastpass_file(path::String, target_entity::AbstractString)::DataFrame
    cols = [:FATTID, :FDAY, :FMONTH, :FYEAR, :FHOUR, :FMIN, :FWINHR, :FWINMIN, :FUSERID]
    df = CSV.read(path, DataFrame, select=cols, types=Dict(:FUSERID => Int64))

    # Filter to the target entity
    df = df[df.FATTID .== target_entity, :]

    df.FUSERID = coalesce.(df.FUSERID, -1)
    df.wait_time_source = ifelse.(df.FUSERID .== 361784 .|| df.FUSERID .== 20176, "scraped", "lines")

    # Split valid and special
    df_valid = df[df.FWINHR .< 8000, :]
    df_special = df[df.FWINHR .>= 8000, :]

    # --- Valid rows: normal processing
    df_valid.observed_at_datetime = DateTime.(df_valid.FYEAR, df_valid.FMONTH, df_valid.FDAY, df_valid.FHOUR, df_valid.FMIN)
    df_valid.return_at_datetime   = DateTime.(df_valid.FYEAR, df_valid.FMONTH, df_valid.FDAY, df_valid.FWINHR, df_valid.FWINMIN)
    df_valid.observed_wait_time   = (df_valid.return_at_datetime .- df_valid.observed_at_datetime) ./ Minute(1)

    # Build observed and return timestamps
    df_valid.observed_at_datetime = DateTime.(df_valid.FYEAR, df_valid.FMONTH, df_valid.FDAY, df_valid.FHOUR, df_valid.FMIN)
    df_valid.return_at_datetime   = DateTime.(df_valid.FYEAR, df_valid.FMONTH, df_valid.FDAY, df_valid.FWINHR, df_valid.FWINMIN)

    # Correct return times if more than 15 minutes earlier than observed time
    df_valid.return_at_datetime = ifelse.(
        (df_valid.return_at_datetime .- df_valid.observed_at_datetime) .< Minute(-15),
        df_valid.return_at_datetime .+ Day(1),
        df_valid.return_at_datetime
    )

    # df_valid.observed_at_datetime = string.(df_valid.observed_at_datetime)

    df_valid = df_valid[:, [:FATTID, :observed_at_datetime, :observed_wait_time, :wait_time_source]]

    # --- Special rows: preserve sellout info
    df_special.observed_at_datetime = DateTime.(df_special.FYEAR, df_special.FMONTH, df_special.FDAY, df_special.FHOUR, df_special.FMIN)
    df_special.return_at_datetime = fill(DateTime(2099, 12, 31, 0, 0), nrow(df_special))
    df_special.observed_wait_time = fill(8888, nrow(df_special))  # sentinel value for sellouts  
    # df_special.observed_at_datetime = string.(df_special.observed_at_datetime)

    df_special = df_special[:, [:FATTID, :observed_at_datetime, :observed_wait_time, :wait_time_source]]

    # --- Combine
    rename!(df_valid, :FATTID => :entity_code, :observed_at_datetime => :observed_at)
    rename!(df_special, :FATTID => :entity_code, :observed_at_datetime => :observed_at)

    df_combined = vcat(df_valid, df_special)
    df_combined[!, :wait_time_type] .= "PRIORITY"

    # Ensure all columns are in the correct format
    df_combined = format_columns!(df_combined)

    return df_combined
end


# ------------------------------------------ #
# Function to process the old fastpass file  #
# ------------------------------------------ #
function process_old_fastpass_file(path::String, target_entity::AbstractString)::DataFrame
    col_indices = [1,2,3,4,5,6,7,8,25]
    df = CSV.read(path, DataFrame, select=col_indices, header=false, skipto=2)

    if ncol(df) == 0 || nrow(df) == 0
        return DataFrame()
    end

    rename!(df, Symbol.([
        "FATTID", "FDAY", "FMONTH", "FYEAR", "FHOUR", "FMIN", "FWINHR", "FWINMIN", "FUSERID"
    ]))

    # Filter to the target entity
    df = df[df.FATTID .== target_entity, :]

    df.FUSERID = coalesce.(df.FUSERID, -1)
    df.wait_time_source = ifelse.(ismissing.(df.FUSERID) .|| df.FUSERID .== "tnalexander" .|| df.FUSERID .== "marie23", "scraped", "lines")

    # Split into valid (parseable) and special (sellout) rows
    df_valid = df[df.FWINHR .< 8000, :]
    df_special = df[df.FWINHR .>= 8000, :]

    # --- Valid rows
    df_valid.observed_at_datetime = DateTime.(df_valid.FYEAR, df_valid.FMONTH, df_valid.FDAY, df_valid.FHOUR, df_valid.FMIN)
    df_valid.return_at_datetime   = DateTime.(df_valid.FYEAR, df_valid.FMONTH, df_valid.FDAY, df_valid.FWINHR, df_valid.FWINMIN)
    df_valid.observed_wait_time   = (df_valid.return_at_datetime .- df_valid.observed_at_datetime) ./ Dates.Minute(1)
    # df_valid.observed_at_datetime = string.(df_valid.observed_at_datetime)
    df_valid = df_valid[:, [:FATTID, :observed_at_datetime, :observed_wait_time]]

    # --- Special (unavailable / sellout) rows
    df_special.observed_at_datetime = DateTime.(df_special.FYEAR, df_special.FMONTH, df_special.FDAY, df_special.FHOUR, df_special.FMIN)
    df_special.return_at_datetime = fill(DateTime(2099, 12, 31, 0, 0), nrow(df_special))  # dummy datetime
    df_special.observed_wait_time = fill(8888, nrow(df_special))  # sentinel value for sellouts
    # df_special.observed_at_datetime = string.(df_special.observed_at_datetime)
    df_special = df_special[:, [:FATTID, :observed_at_datetime, :observed_wait_time]]

    # --- Combine and finalize
    rename!(df_valid, :FATTID => :entity_code, :observed_at_datetime => :observed_at)
    rename!(df_special, :FATTID => :entity_code, :observed_at_datetime => :observed_at)

    df_combined = vcat(df_valid, df_special)
    df_combined[!, :wait_time_type] .= "PRIORITY"

    # Ensure all columns are in the correct format
    df_combined = format_columns!(df_combined)

    return df_combined
end

# ------------------------------------- #
# Function to process the standby files #
# ------------------------------------- #
function process_standby_file(file_path::String, target_entity::AbstractString)::DataFrame
    columns_to_keep = [:entity_code, :user_id, :observed_at, :submitted_posted_time, :submitted_actual_time]

    df = CSV.read(file_path, DataFrame; select=columns_to_keep,
                types=Dict(:user_id => String, :submitted_actual_time => Int64,
                            :submitted_posted_time => Int64, :observed_at => String,
                            :entity_code => String))
                        
    # Filter to the target entity
    df = df[df.entity_code .== target_entity, :]

    # Drop rows with both times missing
    df = df[.!((ismissing.(df.submitted_actual_time)) .& (ismissing.(df.submitted_posted_time))), :]

    # Default any missing user_id
    df.user_id = coalesce.(df.user_id, "default_value")

    # Add wait_time_source
    df[!, :wait_time_source] = ifelse.(df.user_id .== "361784" .|| df.user_id .== "20176", "Scraped", "Lines")

    # Split into posted and actual
    actual_df = df[.!ismissing.(df.submitted_actual_time), :]
    actual_df[!, :wait_time_type] .= "ACTUAL"
    actual_df[!, :observed_wait_time] = actual_df.submitted_actual_time

    posted_df = df[.!ismissing.(df.submitted_posted_time), :]
    posted_df[!, :wait_time_type] .= "POSTED"
    posted_df[!, :observed_wait_time] = posted_df.submitted_posted_time

    combined = vcat(actual_df, posted_df)

    # Rename and parse :observed_at → :date_time
    parsed_year   = parse.(Int, SubString.(combined.observed_at, 1, 4))
    parsed_month  = parse.(Int, SubString.(combined.observed_at, 6, 7))
    parsed_day    = parse.(Int, SubString.(combined.observed_at, 9, 10))
    parsed_hour   = parse.(Int, SubString.(combined.observed_at, 12, 13))
    parsed_minute = parse.(Int, SubString.(combined.observed_at, 15, 16))
    combined[!, :date_time] = DateTime.(parsed_year, parsed_month, parsed_day, parsed_hour, parsed_minute)

    # Clean columns
    combined.observed_wait_time = round.(Int, combined.observed_wait_time)
    filter!(row -> 0 ≤ row.observed_wait_time ≤ 1000, combined)
    filter!(row -> row.wait_time_type ∈ ["ACTUAL", "POSTED"], combined)
    filter!(row -> row.wait_time_source ∈ ["Scraped", "Lines"], combined)

    # Final select and rename
    select!(combined, [:entity_code, :date_time, :observed_wait_time, :wait_time_type])
    rename!(combined, :entity_code => :entity_code, :date_time => :observed_at)

    # Ensure all columns are in the correct format
    combined = format_columns!(combined)
    
    return combined
end

end # module
