# --------------------------------------------
# run_dimEvents.jl — Build structured Event data from unified file
# --------------------------------------------

using Dates, CSV, DataFrames

# -- Sync inputs
sync_event_files()

# -- Load files
event_days_path = "input/events/current_event_days.csv"
events_path     = "input/events/current_events.csv"

event_days_df = CSV.read(event_days_path, DataFrame; missingstring="")
events_df     = CSV.read(events_path, DataFrame; missingstring="")

# -- Build Event lookup: Dict{String, Event}
event_lookup = Dict{String, Event}()

for row in eachrow(events_df)
    abbrev = row.event_abbreviation
    name   = row.event_name
    property = row.property_abbrev
    is_hard_ticket = row.event_hard_ticket == 1
    tz = get_timezone_for_property(property)

    event_lookup[abbrev] = Event(abbrev, name, property, is_hard_ticket, tz)
end

# -- Build structured Vector{EventDay}
event_day_structs = EventDay[]

for row in eachrow(event_days_df)
    date     = Date(row.date)
    abbrev   = row.event_abbreviation
    event    = get(event_lookup, abbrev, nothing)
    isnothing(event) && continue

    start_time = row.event_opening_time === nothing ? missing : row.event_opening_time
    end_time   = row.event_closing_time === nothing ? missing : row.event_closing_time

    push!(event_day_structs, EventDay(date, event.property, event, start_time, end_time))
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

CSV.write("work/dim/dimevents.csv", df_struct)