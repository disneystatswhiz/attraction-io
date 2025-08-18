module Structs

using Dates, TimeZones
export Attraction, Event, EventDay, WaitObs 

struct Attraction
    code::String
    name::String
    park::String
    property::String # e.g., "wdw", "dlr", "uor", "ush", "tdl"
    queue_type::String # e.g., "standby", "priority"
    extinct_on::Union{Date, Nothing}
    is_in_calendar::Bool
    is_single_rider::Bool
    timezone::TimeZone # e.g., "America/New_York"
end

function get_wait_time_types(queue_type::String)::Vector{String}
    uppercase(queue_type) == "PRIORITY" ? ["PRIORITY"] : ["POSTED", "ACTUAL"]
end

Base.getproperty(a::Attraction, ::Val{:wait_time_types}) = get_wait_time_types(a.queue_type)

struct Event
    code::String
    name::String
    property::String
    is_hard_ticket::Bool
    timezone::TimeZone
end

struct EventDay
    date::Date
    property::Union{String, Missing}
    event::Event
    start_time::Union{Time, Missing}
    end_time::Union{Time, Missing}
end

struct WaitObs
    attraction_id::String            # e.g. "AK07"
    observed_at::ZonedDateTime       # fully parsed ISO8601 datetime with timezone
    wait_time_minutes::Int           # raw wait time in minutes (can be 0)
    wait_time_type::String           # e.g. "posted", "actual", ""
end


end # module Structs
