# --------------------------------------------
# run_dimEvents.jl — Build structured Event data from unified file
# --------------------------------------------

using Dates, CSV, DataFrames, TimeZones
using .Structs  # Attraction, Event, EventDay
# assumes: sync_event_files, get_timezone_for_property, upload_file_to_s3, LOC_INPUT, LOC_DIM
# assumes: parse_zoneddatetimes(::AbstractVector) already defined elsewhere in your codebase

# -- Small helper: ZonedDateTime -> Time (in a target TZ)
to_time_of_day(z::Union{ZonedDateTime,Missing}, tz::TimeZone)::Union{Time,Missing} =
    z === missing ? missing : Time(astimezone(z, tz))

# -- Sync inputs
sync_event_files()

# -- Load files
event_days_path = joinpath(LOC_INPUT, "events", "current_event_days.csv")
events_path     = joinpath(LOC_INPUT, "events", "current_events.csv")

event_days_df = CSV.read(event_days_path, DataFrame; missingstring="")
events_df     = CSV.read(events_path, DataFrame;   missingstring="")

# -- Build Event lookup: Dict{String, Event}
event_lookup = Dict{String, Event}()

for row in eachrow(events_df)
    abbrev        = row.event_abbreviation
    name          = row.event_name
    property      = row.property_abbrev
    is_hard_ticket = row.event_hard_ticket == 1
    tz            = get_timezone_for_property(property)

    event_lookup[abbrev] = Event(abbrev, name, property, is_hard_ticket, tz)
end

# -- Pre-parse opening/closing into ZonedDateTime (handles ISO with offsets)
#    Your helper turns weird offset "+hh:mm" -> "+hhmm" and returns missing on bad values.
start_zdt = parse_zoneddatetimes(Vector(event_days_df.event_opening_time))
end_zdt   = parse_zoneddatetimes(Vector(event_days_df.event_closing_time))

# -- Build structured Vector{EventDay}
event_day_structs = EventDay[]

for (i, row) in enumerate(eachrow(event_days_df))
    date   = Date(row.date)
    abbrev = row.event_abbreviation
    ev     = get(event_lookup, abbrev, nothing)
    isnothing(ev) && continue

    # convert parsed ZonedDateTime -> local Time in the event's timezone
    st = to_time_of_day(start_zdt[i], ev.timezone)
    et = to_time_of_day(end_zdt[i],   ev.timezone)

    push!(event_day_structs, EventDay(date, ev.property, ev, st, et))
end

# -- Output for inspection or downstream use
df_struct = DataFrame(
    date        = [e.date for e in event_day_structs],
    event_name  = [e.event.name for e in event_day_structs],
    event_code  = [e.event.code for e in event_day_structs],
    start_time  = [e.start_time for e in event_day_structs],
    end_time    = [e.end_time for e in event_day_structs],
    hard_ticket = [e.event.is_hard_ticket for e in event_day_structs],
    property    = [e.event.property for e in event_day_structs]
)

out_path = joinpath(LOC_DIM, "dimevents.csv")
CSV.write(out_path, df_struct)

# --- Upload to S3 ---
upload_file_to_s3(out_path, "s3://touringplans_stats/stats_work/dimension_tables/dimevents.csv")
