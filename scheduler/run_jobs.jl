# ==============================================================================
# Focused Group Scheduler: cross‑platform (Windows + Linux/EC2)
# Runs one fresh group at a time (parallel per-entity), then moves to the next.
# ==============================================================================

using CSV, DataFrames, Dates, Logging

# --- Config (overridable from CLI or include) ---------------------------------
if !@isdefined(ROOT);                   const ROOT  = abspath(dirname(Base.active_project())); end
if !@isdefined(PROPS);                  const PROPS = ["wdw","dlr","uor","ush","tdr"]; end
if !@isdefined(MAX_PARALLEL_PER_GROUP); const MAX_PARALLEL_PER_GROUP = 6; end
if !@isdefined(FRESHNESS_WINDOW_HOURS); const FRESHNESS_WINDOW_HOURS = 8.0; end
if !@isdefined(MAX_WAIT_MINUTES);       const MAX_WAIT_MINUTES = 420; end     # 7h
if !@isdefined(POLL_SECONDS);           const POLL_SECONDS = 300; end         # 5m
if !@isdefined(ENTITY_TIMEOUT_S);       const ENTITY_TIMEOUT_S = 25*60; end   # 25m

# --- Setup --------------------------------------------------------------------
include(joinpath(ROOT, "src", "main_setup.jl"))  # paths, logging, etc.
let; include(joinpath(ROOT, "scheduler", "run_get_current_ts.jl")); end

# --- Helpers ------------------------------------------------------------------
get_current_path(prop::String, typ::String) =
    typ == "standby"  ? joinpath(ROOT, "input", "wait_times", prop, "current_wait.csv") :
    typ == "priority" ? joinpath(ROOT, "input", "wait_times", "priority", prop, "current_fastpass.csv") :
                        error("unknown type: $typ")

is_priority_supported(prop::String) = !(prop in ("uor","ush","tdr"))

derive_park(entity::AbstractString, fallback::String) = begin
    chars_only = replace(entity, r"\d+" => "")
    isempty(chars_only) ? lowercase(fallback) : uppercase(chars_only[1:2])
end

function is_fresh_group(prop::String, typ::String; window_hours::Real=FRESHNESS_WINDOW_HOURS, verbose=true)::Bool
    path = get_current_path(prop, typ)
    if !isfile(path)
        verbose && @warn "missing current_* file" group="$prop/$typ" path=path
        return false
    end
    m     = unix2datetime(stat(path).mtime)
    age_h = (Dates.datetime2unix(now(UTC)) - Dates.datetime2unix(m)) / 3600
    fresh = age_h ≤ window_hours
    if verbose
        if fresh
            @info "group is fresh" group="$prop/$typ" age_h=round(age_h; digits=2)
        else
            @warn "group is stale" group="$prop/$typ" age_h=round(age_h; digits=2) window_h=window_hours
        end
    end
    return fresh
end

# Centralized “raw” sync (single call per poll when needed)
sync_raw_files() = include(joinpath(ROOT, "src", "data", "run_raw_wait_sync.jl"))

# --- Entity loaders -----------------------------------------------------------
function get_standby_entities(prop::String)
    f = joinpath(ROOT, "input", "wait_times", prop, "current_wait.csv")
    isfile(f) || return String[]
    df = CSV.read(f, DataFrame)
    df = df[.!ismissing.(df.submitted_posted_time) .| .!ismissing.(df.submitted_actual_time), :]
    # df = df[df.entity_code .!= "AK07", :]
    return collect(String.(unique(skipmissing(df.entity_code))))
end

function get_priority_entities(prop::String)
    f = joinpath(ROOT, "input", "wait_times", "priority", prop, "current_fastpass.csv")
    isfile(f) || return String[]
    df = CSV.read(f, DataFrame)
    # df = df[df.FATTID .!= "AK06", :]
    return collect(String.(unique(skipmissing(df.FATTID))))
end

# --- Safe child runner (portable cmd + env; timeout + retry) ------------------
# Wait for a process with a wall-clock timeout (works on Windows & Linux)
function run_with_timeout(cmd::Cmd; timeout::Int=ENTITY_TIMEOUT_S)::Int
    p = run(cmd; wait=false)                 # start child, don't block
    deadline = time() + timeout
    # Poll until the process exits or we hit the deadline
    while time() < deadline && !process_exited(p)
        sleep(0.25)                          # light wait
    end
    if !process_exited(p)
        @error "entity timed out; killing process" timeout_s=timeout cmd=cmd
        try
            Base.kill(p)                     # send SIGTERM on *nix, TerminateProcess on Windows
        catch
        end
        try
            wait(p)                          # reap the process
        catch
        end
        return 124                           # conventional timeout code
    end
    # If it exited naturally, wait to populate exitcode reliably, then return it
    wait(p)
    return p.exitcode
end


# Build a portable Julia command with env limits (Windows & Linux)
function build_entity_cmd(e::String, prop::String, typ::String)::Cmd
    park = derive_park(e, prop)
    jcmd = Base.julia_cmd()  # e.g., C:\...\julia.exe on Windows; /usr/bin/julia on Linux
    script_path = joinpath(ROOT, "src", "main_runner.jl")
    if !isfile(script_path)
        @error "main_runner.jl not found" path=script_path
    end
    base = `$jcmd --project=$ROOT $script_path $e $park $prop $typ`
    return Base.addenv(base,
        "JULIA_NUM_THREADS" => "1",
        "OPENBLAS_NUM_THREADS" => "1",
        "OMP_NUM_THREADS" => "1",
    )
end

function run_entity_once(e::String, prop::String, typ::String)::Int
    cmd = build_entity_cmd(e, prop, typ)
    return run_with_timeout(cmd)
end

function run_entity(e::String, prop::String, typ::String; max_retries::Int=1)::Bool
    for attempt in 1:(max_retries+1)
        code = run_entity_once(e, prop, typ)
        if code == 0
            return true
        else
            @warn "entity nonzero exit" group="$prop/$typ" entity=e attempt=attempt code=code
            attempt ≤ max_retries || return false
            sleep(min(60, 5 * attempt))  # small backoff
        end
    end
    return false
end

# --- Run one group to completion (focused worker pool) ------------------------
function run_group!(prop::String, typ::String; max_parallel::Int=MAX_PARALLEL_PER_GROUP, max_retries::Int=1)
    ents = typ == "standby" ? get_standby_entities(prop) : get_priority_entities(prop)
    if isempty(ents)
        @warn "no entities found" group="$prop/$typ"
        return (ok=0, fail=0, total=0)
    end

    @info "starting focused group" group="$prop/$typ" total=length(ents) max_parallel=max_parallel
    successes = Threads.Atomic{Int}(0)
    failures  = Threads.Atomic{Int}(0)

    ch = Channel{String}(length(ents))
    for e in ents; put!(ch, e); end
    close(ch)

    @sync begin
        for _ in 1:max_parallel
            @async begin
                for e in ch
                    (run_entity(e, prop, typ; max_retries=max_retries) ?
                        Threads.atomic_add!(successes, 1) :
                        Threads.atomic_add!(failures, 1))
                end
            end
        end
    end

    @info "group complete" group="$prop/$typ" ok=successes[] fail=failures[] total=length(ents)
    return (ok=successes[], fail=failures[], total=length(ents))
end

# --- Main loop: pick ONE fresh group, run it, repeat --------------------------
function run_all_focused()
    deadline = time() + MAX_WAIT_MINUTES * 60
    pairs = [(prop, "standby") for prop in PROPS]
    append!(pairs, [(prop, "priority") for prop in PROPS if is_priority_supported(prop)])
    completed = Set{Tuple{String,String}}()

    while time() ≤ deadline
        # 1) choose any fresh, not-yet-completed group
        fresh = nothing
        for (prop, typ) in pairs
            (prop, typ) in completed && continue
            if is_fresh_group(prop, typ; verbose=false)
                fresh = (prop, typ); break
            end
        end

        if fresh === nothing
            # 2) none fresh — do a single global sync, then poll
            @info "no fresh groups; running global raw sync then polling"
            sync_raw_files()
            sleep(POLL_SECONDS)
            continue
        end

        # 3) Run that group to completion
        prop, typ = fresh
        @info "running focused group" group="$prop/$typ"
        run_group!(prop, typ)
        push!(completed, (prop, typ))

        # 4) done?
        if length(completed) == length(pairs)
            @info "all groups processed (focused mode) — done"
            return
        end
    end

    @warn "max wait reached; exiting focused scheduler" waited_min=MAX_WAIT_MINUTES
end

# --- Kick it off --------------------------------------------------------------
run_all_focused()
