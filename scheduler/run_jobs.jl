# ===================================================================================== #
#            Attraction-IO Parallel Polling Launcher (Relative Cutoff Version)          #
# ===================================================================================== #
using DataFrames, Dates, CSV, Logging, Random

# --- Paths & runtime context --------------------------------------------------------- #
if !isdefined(@__MODULE__, :ROOT)
    @eval const ROOT = abspath(joinpath(@__DIR__, ".."))
end
if !isdefined(@__MODULE__, :start_time_pipeline)
    @eval const start_time_pipeline = time_ns()
end

println("-----------------------------------------")
println("Starting pipeline at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))")
println("-----------------------------------------")

include(joinpath(ROOT, "src", "utilities", "utility_setup.jl"))

# --- Tunables ----------------------------------------------------------------------- #
const POLL_INTERVAL     = Minute(10)     # poll every 10 minutes
const MAX_POLL_MINUTES  = 600            # stop after 600 minutes (10 hours)
const REFRESH_WINDOW    = Hour(12)
const BUCKET            = "touringplans_stats"
const MAX_PARALLEL      = 3
const ENABLE_UOR_PRIORITY = false

# Compute relative deadline
const POLL_DEADLINE_UTC = now(UTC) + Minute(MAX_POLL_MINUTES)

# ===================================================================================== #
# Freshness helper
# ===================================================================================== #

function refreshed_within(bucket::String, key::String; window::Period=REFRESH_WINDOW)::Bool
    now_utc = now(UTC)
    try
        lm = get_last_modified_s3_ts(bucket, key)
        age = now_utc - lm
        # @info "Freshness check (ts)" key last_modified_utc=lm now_utc=now_utc age=age window=window
        return (age ≥ Second(0)) && (age ≤ window)
    catch err
        # @warn "Timestamp check failed; falling back to date-only" key error=err
        try
            lm_date = get_last_modified_s3(bucket, key)
            same_day = lm_date == Date(now_utc)
            # @info "Freshness check (date-only)" key last_modified_date=lm_date today_utc=Date(now_utc) same_day=same_day
            return same_day
        catch err2
            # @error "Both freshness checks failed; launching defensively" key error=err2
            return true
        end
    end
end

# ===================================================================================== #
# Utilities
# ===================================================================================== #

@inline function derive_park(entity::AbstractString, fallback_prop::AbstractString)
    m = match(r"^[A-Za-z]{2}", entity)
    return m === nothing ? lowercase(fallback_prop) : lowercase(m.match)
end

const PARK_PREFIXES = Dict(
    "wdw" => Set(["AK", "MK", "EP", "HS"]),
    "dlr" => Set(["DL", "CA"]),
    "uor" => Set(["IA", "EU", "UF"])
)

function filter_by_prop_prefix(entities::AbstractVector{<:AbstractString}, prop::AbstractString)
    prefixes = get(PARK_PREFIXES, prop, nothing)
    prefixes === nothing && return String.(entities)
    kept = String[]; dropped = String[]
    for e in entities
        e2 = String(e)
        if length(e2) ≥ 2 && uppercase(e2[1:2]) in prefixes
            push!(kept, e2)
        else
            push!(dropped, e2)
        end
    end
    if !isempty(dropped)
        # @warn "Filtered out entities not matching prop prefix" prop dropped_sample=first(dropped, min(5, length(dropped)))
    end
    return kept
end

# ===================================================================================== #
# Entity discovery
# ===================================================================================== #

function get_standby_entities(prop::String)::Vector{String}
    local_dir = joinpath(LOC_TEMP, "$(prop)_standby")
    isdir(local_dir) && rm(local_dir; force=true, recursive=true)
    mkpath(local_dir)

    s3_path = "s3://$BUCKET/export/wait_times/$prop"
    sync_from_s3_folder(s3_path, local_dir; exclude=["*"], include=["current_wait.csv"])

    df = CSV.read(joinpath(local_dir, "current_wait.csv"), DataFrame)
    filter!(row -> !ismissing(row.submitted_posted_time) || !ismissing(row.submitted_actual_time), df)

    if "entity_code" ∉ names(df)
        error("Required column 'entity_code' not found in current_wait.csv; columns=$(names(df))")
    end

    vals = String.(strip.(String.(coalesce.(df[!, "entity_code"], ""))))
    ents = unique(filter(!isempty, vals))
    ents = filter(!=("AK07"), ents)
    ents = filter_by_prop_prefix(ents, prop)

    # @info "Discovered entities" prop queue_type="standby" count=length(ents) sample=first(ents, min(5, length(ents)))
    return ents
end

# Replace your current get_priority_entities with this:

function get_priority_entities(prop::String)::Vector{String}
    if prop == "uor" && !ENABLE_UOR_PRIORITY
        # @info "UOR priority disabled; skipping entity discovery" prop
        return String[]
    end

    local_dir = joinpath(LOC_TEMP, "$(prop)_fastpass")
    isdir(local_dir) && rm(local_dir; force=true, recursive=true)
    mkpath(local_dir)

    s3_path = "s3://$BUCKET/export/fastpass_times/$prop"
    sync_from_s3_folder(s3_path, local_dir; exclude=["*"], include=["current_fastpass.csv"])

    f = joinpath(local_dir, "current_fastpass.csv")
    if !isfile(f)
        # @warn "current_fastpass.csv not found after sync" prop s3_path
        return String[]
    end

    df = CSV.read(f, DataFrame)

    # --- Deep visibility on headers / size ---
    headers = names(df)
    # @info "Priority feed loaded" prop rows=nrow(df) cols=length(headers)
    # @info "Priority headers (repr)" headers=repr(headers)

    if nrow(df) == 0
        # @warn "Priority feed has zero rows" prop file=f
        return String[]
    end

    # --- Robust header match for FATTID (handles BOM/space) ---
    normalize = s -> uppercase(strip(replace(String(s), '\ufeff' => "")))
    target    = "FATTID"
    col = nothing
    for h in headers
        if normalize(h) == target
            col = h
            break
        end
    end
    if col === nothing
        # @error "FATTID column not found after normalization" prop normalized_headers=map(normalize, headers)
        return String[]
    end

    # --- Extract raw values + quick diagnostics ---
    raw = df[!, col]
    miss_ct = count(ismissing, raw)
    nonmiss = collect(skipmissing(raw))
    preview = map(string, nonmiss[1:min(end, 10)])
    # @info "Priority ID diagnostics" column=String(col) missing=miss_ct nonmissing=length(nonmiss) preview=preview

    # Coerce → String, strip, drop empties
    vals = String.(strip.(String.(coalesce.(raw, ""))))
    ents_pre = unique(filter(!isempty, vals))
    # @info "IDs before prefix filter" count=length(ents_pre) sample=first(ents_pre, min(5, length(ents_pre)))

    # Optional: filter by property prefix
    ents = filter_by_prop_prefix(ents_pre, prop)
    # @info "IDs after prefix filter"  count=length(ents)     sample=first(ents,     min(5, length(ents)))

    return ents
end


# ===================================================================================== #
# Job Launcher
# ===================================================================================== #

function run_one_job(prop::String, typ::String; max_parallel::Int=MAX_PARALLEL)
    entities = (typ == "standby") ? get_standby_entities(prop) :
               (typ == "priority") ? get_priority_entities(prop) : String[]

    if isempty(entities)
        # @info "No entities discovered; nothing to launch" prop typ
        return
    end

    shuffle!(entities)
    active_jobs = Vector{Base.Process}()

    for entity in entities
        entity_s = String(entity)
        park     = derive_park(entity_s, prop)
        cmd      = `julia --project=. src/main_runner.jl $entity_s $park $prop $typ`

        # NEW: log the launch
        # @info "Launching entity job" entity=entity_s park=park prop=prop queue_type=typ cmd=cmd

        process = run(cmd; wait=false)
        push!(active_jobs, process)

        if length(active_jobs) == max_parallel
            wait(active_jobs[1])
            popfirst!(active_jobs)
        end
    end

    foreach(wait, active_jobs)
end

# ===================================================================================== #
# main_setup — once per day
# ===================================================================================== #

let lockfile = joinpath(ROOT, "temp", "main_setup_done_$(Dates.format(today(), "yyyymmdd")).lock")
    if !isfile(lockfile)
        include(joinpath(ROOT, "src", "main_setup.jl"))
        mkpath(dirname(lockfile))
        open(lockfile, "w") do io end
        # @info "main_setup completed for today"
    else
        # @info "main_setup already completed today; continuing"
    end
end

# ===================================================================================== #
# Polling loop — relative cutoff
# ===================================================================================== #

pending = Set([
    ("wdw", "standby"),
    ("wdw", "priority"),
    ("dlr", "standby"),
    ("dlr", "priority"),
    ("uor", "standby")
])

job_tasks = Dict{Tuple{String,String}, Task}()

while !isempty(pending) && now(UTC) ≤ POLL_DEADLINE_UTC
    for (prop, typ) in copy(pending)
        key = (typ == "standby") ?
            "export/wait_times/$prop/current_wait.csv" :
            "export/fastpass_times/$prop/current_fastpass.csv"

        if refreshed_within(BUCKET, key)
            # @info "Launching job (fresh within window)" prop typ key window=REFRESH_WINDOW
            job_tasks[(prop, typ)] = @async try
                run_one_job(prop, typ; max_parallel=MAX_PARALLEL)
            catch err
                bt = catch_backtrace()
                # @error "Group task failed" prop typ error=err stacktrace=bt
                rethrow()
            end
            delete!(pending, (prop, typ))
        else
            last_mod = try get_last_modified_s3_ts(BUCKET, key) catch; nothing end
            mins_left = round(Int, (POLL_DEADLINE_UTC - now(UTC)) / Minute(1))
            # @info "Not fresh yet; will poll again" prop typ key window=REFRESH_WINDOW \
            #    last_modified_utc=last_mod now_utc=now(UTC) minutes_left=mins_left
        end
    end
    isempty(pending) || sleep(Dates.value(POLL_INTERVAL) * 60)
end

if !isempty(pending)
    # @warn "Stopped polling because deadline hit; some groups never became fresh" remaining=collect(pending)
end

# ===================================================================================== #
# Wait for all launched groups to complete
# ===================================================================================== #

for ((prop, typ), task) in job_tasks
    wait(task)
end

elapsed = round((time_ns() - start_time_pipeline) / 1e9 / 60, digits=2)
# @info "Launcher complete" elapsed_min=elapsed

# =============================================== End ================================= #
