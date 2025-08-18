# scheduler/mtimes.jl
# Helpers for locating current_* files and checking freshness. No side effects.

using Dates

# Reuse existing constants if already defined by the including script
const ROOT  = @isdefined(ROOT)  ? ROOT  : abspath(dirname(Base.active_project()))
const PROPS = @isdefined(PROPS) ? PROPS : ["wdw","dlr","uor"]

get_current_path(prop::String, typ::String) =
    typ == "standby"  ? joinpath(ROOT, "input", "wait_times", prop, "current_wait.csv") :
    typ == "priority" ? joinpath(ROOT, "input", "wait_times", "priority", prop, "current_fastpass.csv") :
                        error("unknown type: $typ")

# Age in hours (Float64)
file_age_hours(path::AbstractString)::Float64 = begin
    m = unix2datetime(stat(path).mtime)
    secs = Dates.datetime2unix(now(UTC)) - Dates.datetime2unix(m)
    secs / 3600
end

# Freshness check against a window (hours)
is_fresh(prop::String, typ::String; window_hours::Real=12.0)::Bool = begin
    path = get_current_path(prop, typ)
    isfile(path) || return false
    file_age_hours(path) ≤ window_hours
end

# Optional: print a small table (useful as a CLI tool)
fmt_age(dt::DateTime) = begin
    secs = Dates.datetime2unix(now(UTC)) - Dates.datetime2unix(dt)
    secs < 3600 ? "$(round(secs/60; digits=1))m" : "$(round(secs/3600; digits=2))h"
end

function print_mtimes()
    println("Current file mtimes (UTC) — ", Dates.format(now(UTC), "yyyy-mm-dd HH:MM:SS"))
    println(rpad("prop",5), rpad("type",10), rpad("exists",8), rpad("mtime_utc",23), rpad("age",8), "path")
    for prop in PROPS, typ in ("standby","priority")
        path = get_current_path(prop, typ)
        if isfile(path)
            m = unix2datetime(stat(path).mtime)
            println(rpad(prop,5), rpad(typ,10), rpad("yes",8), rpad(string(m),23), rpad(fmt_age(m),8), path)
        else
            println(rpad(prop,5), rpad(typ,10), rpad("no",8), rpad("-",23), rpad("-",8), path)
        end
    end
end
