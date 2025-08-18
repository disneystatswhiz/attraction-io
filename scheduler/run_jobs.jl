# ==============================================================================
# Simple Startup Job Runner (clean logs + non-blocking per-group waits)
# ==============================================================================

using CSV, DataFrames, Dates, Logging, Random

# --- Config (overridable) -----------------------------------------------------
if !@isdefined(ROOT);                   const ROOT  = abspath(dirname(Base.active_project())); end
if !@isdefined(PROPS);                  const PROPS = ["wdw","dlr","uor"]; end
if !@isdefined(MAX_PARALLEL_PER_GROUP); const MAX_PARALLEL_PER_GROUP = 3; end
if !@isdefined(FRESHNESS_WINDOW_HOURS); const FRESHNESS_WINDOW_HOURS = 12.0; end
if !@isdefined(MAX_WAIT_MINUTES);       const MAX_WAIT_MINUTES = 360; end     # set 0 to disable waiting
if !@isdefined(POLL_SECONDS);           const POLL_SECONDS = 300; end

# --- Setup --------------------------------------------------------------------
include(joinpath(ROOT, "src", "main_setup.jl"))

# Show current_* mtimes (uses your existing script)
include(joinpath(ROOT, "scheduler", "run_get_current_ts.jl"))

# --- Helpers ------------------------------------------------------------------
get_current_path(prop::String, typ::String) =
    typ == "standby"  ? joinpath(ROOT, "input", "wait_times", prop, "current_wait.csv") :
    typ == "priority" ? joinpath(ROOT, "input", "wait_times", "priority", prop, "current_fastpass.csv") :
                        error("unknown type: $typ")

function is_fresh_group(prop::String, typ::String; window_hours::Real=FRESHNESS_WINDOW_HOURS, verbose::Bool=true)::Bool
    path = get_current_path(prop, typ)
    if !isfile(path)
        verbose && @warn "missing current_* file" group="$prop/$typ" path=path
        return false
    end
    m = unix2datetime(stat(path).mtime)
    age_h = (Dates.datetime2unix(now(UTC)) - Dates.datetime2unix(m)) / 3600
    if age_h ≤ window_hours
        verbose && @info "group is fresh; will run" group="$prop/$typ" age_h=round(age_h; digits=2)
        return true
    else
        verbose && @warn "group is stale; will retry if configured" group="$prop/$typ" age_h=round(age_h; digits=2) window_h=window_hours
        return false
    end
end

function wait_until_fresh(prop::String, typ::String;
                          window_hours::Real=FRESHNESS_WINDOW_HOURS,
                          max_wait_minutes::Int=MAX_WAIT_MINUTES,
                          poll_seconds::Int=POLL_SECONDS)::Bool
    max_wait_minutes ≤ 0 && return false  # no waiting
    deadline = time() + max_wait_minutes * 60
    while time() ≤ deadline
        if is_fresh_group(prop, typ; window_hours=window_hours, verbose=false)
            @info "became fresh; proceeding" group="$prop/$typ"
            return true
        end
        remaining = Int(clamp(round(deadline - time()), 0, typemax(Int)))
        @info "waiting to become fresh" group="$prop/$typ" retry_in_s=poll_seconds time_left_s=remaining
        sleep(poll_seconds)
    end
    @warn "did not become fresh in time; skipping" group="$prop/$typ" waited_min=max_wait_minutes
    return false
end

derive_park(entity::AbstractString, fallback::String) = begin
    m = match(r"^[A-Za-z]{2}", entity)
    m === nothing ? lowercase(fallback) : uppercase(m.match)
end

# --- Entity loaders -----------------------------------------------------------
function get_standby_entities(prop)
    f = joinpath(ROOT, "input", "wait_times", prop, "current_wait.csv")
    isfile(f) || return String[]
    df = CSV.read(f, DataFrame)
    collect(String.(unique(skipmissing(df.entity_code))))
end

function get_priority_entities(prop)
    f = joinpath(ROOT, "input", "wait_times", "priority", prop, "current_fastpass.csv")
    isfile(f) || return String[]
    df = CSV.read(f, DataFrame)
    collect(String.(unique(skipmissing(df.FATTID))))
end

# --- Run group with tiny worker pool -----------------------------------------
function run_group!(entities::Vector{String}, prop::String, typ::String; max_parallel::Int=MAX_PARALLEL_PER_GROUP)
    if isempty(entities)
        @warn "no entities found" group="$prop/$typ"
        return
    end
    @info "launching group" group="$prop/$typ" total=length(entities)

    ch = Channel{String}(length(entities))
    for e in entities; put!(ch, e); end
    close(ch)

    @sync begin
        for _ in 1:max_parallel
            @async begin
                for e in ch
                    park = derive_park(e, prop)
                    run(`julia --project=$ROOT $(joinpath(ROOT,"src","main_runner.jl")) $e $park $prop $typ`)
                end
            end
        end
    end
    @info "group complete" group="$prop/$typ"
end

# --- Runner (non-blocking across groups; skips uor/priority) ------------------
function run_all()
    @sync begin
        for prop in PROPS
            for typ in ("standby","priority")
                if prop == "uor" && typ == "priority"
                    @info "skipping non-existent group" group="uor/priority"
                    continue
                end
                @async begin
                    if is_fresh_group(prop, typ) || wait_until_fresh(prop, typ)
                        ents = typ == "standby" ? get_standby_entities(prop) : get_priority_entities(prop)
                        run_group!(ents, prop, typ)
                    else
                        @warn "skipped after waiting (not fresh)" group="$prop/$typ"
                    end
                end
            end
        end
    end
end

run_all()
@info "all done"
