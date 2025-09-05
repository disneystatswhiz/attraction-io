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
    park_code = lowercase(join(filter(c -> !isdigit(c), entity_code)))

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

# --------------------------------------------------------
# 🚫 Legacy version — assumes all values are strings
# --------------------------------------------------------
function parse_zoneddatetimes_simple(input_vector::AbstractVector)
    fmt = DateFormat("yyyy-mm-ddTHH:MM:SS.ssszzz")
    return [ZonedDateTime(String(x), fmt) for x in input_vector]
end

"""
    parse_zoneddatetimes_smart(input_vector::AbstractVector; timezone=TimeZone("America/New_York"))

Smart parser for mixed-format ISO8601 strings into `ZonedDateTime`.

- Supports clean and dirty formats (`.sss` fractional seconds or not, with or without offset)
- Fixes offset formatting (`+hh:mm` → `+hhmm`)
- Gracefully handles already-parsed values, `missing`, and bad strings

Returns `Vector{Union{ZonedDateTime, Missing}}`.
"""
# THIS FUNCTION NEEDS TESTING - JUST A PLACEHOLDER FOR FUTURE DEV #
function parse_zoneddatetimes_smart(input_vector::AbstractVector; timezone=TimeZone("America/New_York"))

    fmt_simple = DateFormat("yyyy-mm-ddTHH:MM:SS.ssszzz")
    fmt_clean  = DateFormat("yyyy-MM-ddTHH:MM:SSzzz")

    return [try
        if x isa Missing
            missing
        elseif x isa ZonedDateTime
            x
        else
            str = string(x)
            str_fixed = replace(str, r"([+-]\d{2}):(\d{2})" => s"\1\2")
            if occursin('.', str_fixed)
                ZonedDateTime(str_fixed, fmt_simple, timezone)
            else
                ZonedDateTime(str_fixed, fmt_clean)
            end
        end
    catch
        missing
    end for x in input_vector]
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

    return if property_code in ["wdw", "uor", "dc"]
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

    return if property_code in ["wdw", "uor", "dc"]
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
# Remove all files or folders whose name contains the given pattern.   #
# Matching is case-insensitive.                                        #
# - Searches recursively in the base folder (default: pwd())           #
# - Deletes any matching file or directory                             #
# Args:                                                                 #
# - pattern   → substring to match (e.g. entity_code like "ak07")      #
# - base_dir  → where to search (default: current working dir)         #
# --------------------------------------------------------------------- #
function cleanup_folders(pattern::String; base_dir::String = pwd(), delete_files::Bool = true)
    pat = lowercase(pattern)
    files_to_del = String[]
    dirs_to_del  = String[]

    # Collect matches
    for (root, dirs, files) in walkdir(base_dir)
        if delete_files
            for name in files
                occursin(pat, lowercase(name)) && push!(files_to_del, joinpath(root, name))
            end
        end
        for name in dirs
            occursin(pat, lowercase(name)) && push!(dirs_to_del, joinpath(root, name))
        end
    end

    # Dedupe
    files_to_del = unique(files_to_del)
    dirs_to_del  = unique(dirs_to_del)

    # 1) Delete files
    if delete_files
        for p in files_to_del
            if isfile(p)
                try
                    rm(p; force=true)
                    # @info "🧹 Deleted file: $p"
                catch e
                    # @warn "⚠️ Could not delete file $p — $(e.msg)"
                end
            end
        end
    end

    # 2) Delete directories (deepest first)
    # Depth = number of path separators; handles Windows and POSIX.
    depth(p) = count(c -> c == '/' || c == '\\', p)
    sort!(dirs_to_del, by=depth, rev=true)

    for p in dirs_to_del
        if isdir(p)
            try
                rm(p; recursive=true, force=true)
                # @info "🧹 Deleted folder: $p"
            catch e
                # @warn "⚠️ Could not delete folder $p — $(e.msg)"
            end
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

# --------------------------------------------------------------------- #
# Get last modified timestamp of an S3 object
# --------------------------------------------------------------------- #
function get_last_modified_s3_ts(bucket::String, key::String)::DateTime
    cmd = `aws s3api head-object --bucket $bucket --key $key --query LastModified --output text`
    iso = strip(read(cmd, String))  # e.g. "2025-08-14T12:34:56.000Z" or "+00:00"

    # RFC3339: yyyy-mm-ddTHH:MM:SS[.fraction](Z|±HH:MM)
    m = match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+\-]\d{2}:\d{2})$", iso)
    m === nothing && error("Unexpected LastModified format: $iso")

    base  = m.captures[1]
    frac  = something(m.captures[2], "")
    tzstr = m.captures[3]

    # Normalize fraction to milliseconds for Dates.DateTime
    frac3 = isempty(frac) ? "" : "." * rpad(first(frac, 3), 3, '0')

    # Parse naive local datetime part
    dt = DateTime(base * frac3, dateformat"yyyy-mm-ddTHH:MM:SS.sss")

    # Adjust to UTC based on offset
    if tzstr == "Z" || tzstr == "+00:00"
        return dt
    else
        mm = match(r"^([+\-])(\d{2}):(\d{2})$", tzstr)
        sign = mm.captures[1] == "+" ? 1 : -1
        h    = parse(Int, mm.captures[2])
        mi   = parse(Int, mm.captures[3])
        return dt - sign*(Hour(h) + Minute(mi))  # convert to UTC
    end
end