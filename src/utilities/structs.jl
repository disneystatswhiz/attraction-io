module Structs

using Dates
using TimeZones

export Attraction, ParkEvent, EventDay, WaitObs, get_wait_time_types

# ── Attraction ───────────────────────────────────────────────────────────────

struct Attraction
    code::String
    name::String
    park::String
    property::String         # e.g., "wdw", "dlr", "uor", "ush", "tdl"
    queue_type::String       # e.g., "standby", "priority"
    extinct_on::Union{Date, Nothing}
    is_in_calendar::Bool
    is_single_rider::Bool
    timezone::TimeZone       # e.g., "America/New_York"
end

function get_wait_time_types(queue_type::String)::Vector{String}
    uppercase(queue_type) == "PRIORITY" ? ["PRIORITY"] : ["POSTED", "ACTUAL"]
end

Base.getproperty(a::Attraction, ::Val{:wait_time_types}) = get_wait_time_types(a.queue_type)

# ── Event ────────────────────────────────────────────────────────────────────

struct ParkEvent
    code::String
    name::String
    property::String
    is_hard_ticket::Bool
    timezone::TimeZone
end

# ── Helpers for EventDay ─────────────────────────────────────────────────────

_is_blankish(s::AbstractString) = isempty(strip(String(s))) ||
                                  lowercase(strip(String(s))) in ("na","null","none")

# Parse a *single* ISO8601 datetime string into ZonedDateTime, tolerating "+hh:mm" offsets.
# Mirrors your vector helpers but scoped for one value.
function _parse_one_zdt(s::AbstractString)::Union{ZonedDateTime,Missing}
    s_fixed = replace(String(s), r"([+-]\d{2}):(\d{2})" => s"\\1\\2")  # "+05:00" -> "+0500"
    # Try with and without fractional seconds
    for fmt in (DateFormat("yyyy-MM-ddTHH:MM:SSzzz"),
                DateFormat("yyyy-mm-ddTHH:MM:SS.ssszzz"))
        try
            return ZonedDateTime(s_fixed, fmt)
        catch
            # try next format
        end
    end
    return missing
end

# Convert many string shapes into Time (or missing).
# Supported:
#   Time                       -> returned as-is
#   "", "NA", "null", missing  -> missing
#   "1995-10-31T19:00:00-05:00" (ISO datetime w/ offset) -> Time part
#   "7pm", "7:30 pm"           -> 12h clock
#   "HH:MM[:SS]"               -> 24h clock with colons
#   "HHMM" / "HMM"             -> compact 24h
#   "HH.MM" / "H.MM"           -> dots allowed (treated like colons)
#   Any AbstractString (incl. InlineStrings.StringXX)
function parse_time(x)::Union{Time,Missing}
    x === missing && return missing
    x === nothing && return missing
    x isa Time && return x

    if x isa AbstractString
        s = strip(String(x))
        _is_blankish(s) && return missing

        # 0) Full ISO datetime? (starts with YYYY-MM-DDT…)
        if occursin(r"^\d{4}-\d{2}-\d{2}T", s)
            z = _parse_one_zdt(s)
            z !== missing && return Time(z)
            # If it looked ISO but didn't parse, continue with other shapes
        end

        # normalize separators, collapse internal spaces
        s = replace(s, '.' => ':')
        s = replace(s, r"\s+" => " ")
        ls = lowercase(s)

        # 1) 12-hour forms: "7pm", "7:30 pm", "12 AM"
        if occursin(r"\b(am|pm)\b", ls) || endswith(ls, "am") || endswith(ls, "pm")
            m = match(r"^\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*$"i, s)
            if m !== nothing
                hour = parse(Int, m.captures[1])
                minute = m.captures[2] === nothing ? 0 : parse(Int, m.captures[2])
                mer = lowercase(m.captures[3])
                hour = mer == "pm" ? (hour == 12 ? 12 : hour + 12) : (hour == 12 ? 0 : hour)
                return Time(hour, minute)
            end
        end

        # 2) Compact 24h "HHMM" or "HMM"
        m = match(r"^\s*(\d{1,2})(\d{2})\s*$", s)
        if m !== nothing
            hour = parse(Int, m.captures[1])
            minute = parse(Int, m.captures[2])
            return Time(hour, minute)
        end

        # 3) 24h with colons "HH:MM[:SS]"
        m = match(r"^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$", s)
        if m !== nothing
            hour = parse(Int, m.captures[1])
            minute = parse(Int, m.captures[2])
            second = m.captures[3] === nothing ? 0 : parse(Int, m.captures[3])
            return Time(hour, minute, second)
        end

        # 4) Last resort: try DateTime (no TZ) and take time part
        try
            dt = DateTime(s)
            return Time(dt)
        catch
            # fall through to unified error
        end

        throw(ArgumentError("Invalid time string for EventDay: '$s'"))
    end

    throw(ArgumentError("Unsupported time type for EventDay: $(typeof(x))"))
end

# ── EventDay ─────────────────────────────────────────────────────────────────

struct EventDay
    date::Date
    property::Union{String, Missing}
    event::ParkEvent
    start_time::Union{Time, Missing}
    end_time::Union{Time, Missing}
end

# Accept AbstractString/InlineString/Missing/Time for start/end
EventDay(date::Date, property, event::ParkEvent, start_time, end_time) =
    EventDay(date, property, event, parse_time(start_time), parse_time(end_time))

# ── Wait Observation ─────────────────────────────────────────────────────────

struct WaitObs
    attraction_id::String       # e.g. "AK07"
    observed_at::ZonedDateTime  # fully parsed ISO8601 datetime with timezone
    wait_time_minutes::Int      # raw wait time in minutes (can be 0)
    wait_time_type::String      # e.g. "posted", "actual", ""
end

end # module Structs
