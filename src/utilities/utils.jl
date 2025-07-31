using DataFrames, TimeZones

function log_header(msg::AbstractString; pad::Int = 2, char::Char = '-')
    width = length(msg) + 2 * pad
    bar = repeat(string(char), width)
    println()
    println(bar)
    println(" "^pad * msg)
    println(bar)
    println()
end

"""
    build_attraction_struct(row::DataFrameRow, entity_code::String, wait_type::String) -> Attraction

Given a row from dimEntity and wait_type, build an Attraction struct with inferred park, property, and queue_type.

- entity_code: should already be lowercase
- wait_type: optionally overridden, but typically inferred from fastpass_booth
"""
function build_attraction_struct(row::DataFrameRow, entity_code::String)::Attraction
    code = uppercase(entity_code)
    name = row.name
    park_code = lowercase(code[1:2])

    property_code = if park_code in ["ak", "mk", "hs", "ep", "bb", "tl"]
        "wdw"
    elseif park_code in ["dl", "ca"]
        "dlr"
    elseif park_code in ["uf", "ia", "eu"]
        "uor"
    elseif park_code in ["tdl", "tds"]
        "tdr"
    elseif park_code == "uh"
        "ush"
    else
        error("Unknown park code: $park_code")
    end

    # Infer queue type
    queue_type = row.fastpass_booth ? "priority" : "standby"

    # Use dimEntity field directly
    is_in_calendar = row.crowd_calendar_group > 0

    # Optional field
    extinct_on = ismissing(row.extinct_on) ? nothing : Date(row.extinct_on)

    # Optional field
    is_single_rider = row.single_rider

    # Assign timezone based on property_code
    timezone = if property_code in ["wdw", "uor"]
        TimeZone("America/New_York")
    elseif property_code in ["dlr", "ush"]
        TimeZone("America/Los_Angeles")
    elseif property_code == "tdr"
        TimeZone("Asia/Tokyo")
    else
        error("No timezone assigned for property: $property_code")
    end

    return Attraction(code, name, park_code, property_code, queue_type, extinct_on, is_in_calendar, is_single_rider, timezone)
end

"""
    parse_zoneddatetimes(input_vector::AbstractVector)

Use to parse David's ISO8601 date strings into ZonedDateTime.

Converts a vector of values (strings, numbers, or other convertible types) into
ZonedDateTime values using the ISO8601 format "yyyy-mm-ddTHH:MM:SSzzz".

Returns a vector of `Union{ZonedDateTime, Missing}`.
Invalid or missing inputs become `missing`.
"""
function parse_zoneddatetimes(input_vector::AbstractVector)
    fmt = DateFormat("yyyy-MM-ddTHH:MM:SSzzz")
    return [try
        clean_str = replace(string(x), r"([+-]\d{2}):(\d{2})" => s"\1\2")
        ZonedDateTime(clean_str, fmt)
    catch
        missing
    end for x in input_vector]
end

"""
    parse_zoneddatetimes_simple(input_vector::AbstractVector)

Use for CSVS that we created

Similar to `parse_zoneddatetimes`, but assumes input is already in a simple format
without timezone offsets, e.g., "2023-10-01T12:00:00".
Returns a vector of `ZonedDateTime` values.
"""
function parse_zoneddatetimes_simple(input_vector::AbstractVector)
    fmt = DateFormat("yyyy-mm-ddTHH:MM:SS.ssszzz")
    return [ZonedDateTime(String(x), fmt) for x in input_vector]
end


"""
    get_timezone_for_park(park_code::AbstractString) -> TimeZone
"""
function get_timezone_for_park(park_code::AbstractString)::TimeZone
    park_code = lowercase(park_code)
    property_code = if park_code in ["ak", "mk", "hs", "ep", "bb", "tl"]
        "wdw"
    elseif park_code in ["dl", "ca"]
        "dlr"
    elseif park_code in ["uf", "ia", "eu", "vb"]
        "uor"
    elseif park_code in ["tdl", "tds"]
        "tdr"
    elseif park_code == "uh"
        "ush"
    else
        error("Unknown park code: $park_code")
    end

    return if property_code in ["wdw", "uor"]
        tz"America/New_York"
    elseif property_code in ["dlr", "ush"]
        tz"America/Los_Angeles"
    elseif property_code == "tdr"
        tz"Asia/Tokyo"
    else
        error("No timezone for property: $property_code")
    end
end

function get_timezone_for_property(property_code::AbstractString)::TimeZone
    property_code = lowercase(property_code)

    return if property_code in ["wdw", "uor"]
        tz"America/New_York"
    elseif property_code in ["dlr", "ush"]
        tz"America/Los_Angeles"
    elseif property_code == "tdr"
        tz"Asia/Tokyo"
    else
        error("No timezone for property: $property_code")
    end
end

# --------------------------------------------------------------------- #
# Generates a park_day_id based on the datetime column.
# The park_day_id is the date of the observation, adjusted to the previous day
# if the observation time is before 6 AM.
# Args:
# - df::DataFrame: The DataFrame containing the datetime column
# - dt_col_name::Symbol: The name of the datetime column
# Returns:
# - Vector{Date}: A vector of park_day_ids corresponding to each row in the DataFrame
# --------------------------------------------------------------------- #
function get_park_day_id(df::DataFrame, dt_col_name::Symbol)

    park_day_ids = Vector{Date}(undef, nrow(df))
    
    for i in 1:nrow(df)
        dt_col = df[i, dt_col_name]
        
        # Extract the date from the datetime column
        date = Dates.Date(dt_col)
        
        # Extract the hour part from the datetime column
        hour = Dates.hour(dt_col)
        
        # If the hour is less than 6, then the date is the previous day
        if hour < 6
            date = date - Dates.Day(1)
        end
        
        park_day_ids[i] = date
    end
    
    return park_day_ids
end

# --------------------------------------------------------------------- #
# Fully remove a folder and all of its contents.                       #
# If the folder exists, delete it recursively.                         #
# If the folder does not exist, do nothing.                            #
# Args:                                                                 #
# - folder   → the folder to delete                                     #
# --------------------------------------------------------------------- #
function cleanup_folders(folder::String)
    if isdir(folder)
        try
            rm(folder; recursive=true)
            # @info("🧹 Deleted folder and all contents: $folder")
        catch e
            # @warn("⚠️  Failed to delete folder: $folder — $(e.msg)")
        end
    end
end


# --------------------------------------------------------------------- #
# Round a ZonedDateTime to the nearest specified number of minutes
# -- Args:
# - dt::ZonedDateTime: The datetime to round
# - rounder::Int: Number of minutes to round to (e.g., 15)
# -- Returns:
# - ZonedDateTime: Rounded ZonedDateTime
# --------------------------------------------------------------------- #
function round_datetime(dt::ZonedDateTime, rounder::Int)
    local_dt = DateTime(dt)  # Strip zone to do rounding math
    minutes_past_midnight = hour(local_dt) * 60 + minute(local_dt)
    rminutes = round(minutes_past_midnight / rounder) * rounder
    hour_value = div(rminutes, 60)
    if hour_value == 24
        hour_value = 0
    end
    rounded_local = DateTime(year(local_dt), month(local_dt), day(local_dt), hour_value, rem(rminutes, 60))
    return ZonedDateTime(rounded_local, dt.zone)  # ← FIX: use zone object directly, not string
end

# --------------------------------------------------------------------- #
# Fallback method for non-ZonedDateTime inputs
# --------------------------------------------------------------------- #
function round_datetime(dt::Any, rounder::Int)
    throw(ArgumentError("round_datetime requires a ZonedDateTime input. Got $(typeof(dt))"))
end
