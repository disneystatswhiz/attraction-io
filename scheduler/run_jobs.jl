# ===================================================================================== #
#            Attraction-IO Parallel Polling Launcher (Relative Cutoff Version)          #
# ===================================================================================== #
using DataFrames, Dates, CSV, Logging, Random

# --- Paths & runtime context --------------------------------------------------------- #
const ROOT                = abspath(joinpath(@__DIR__, ".."))
const start_time_pipeline = time_ns()

println("--------------------------------------------------------------------------------")
println("Starting pipeline at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))")
println("--------------------------------------------------------------------------------")

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
        @info "Freshness check (ts)" key last_modified_utc=lm now_utc=now_utc age=age window=window
        return (age ≥ Second(0)) && (age ≤ window)
    catch err
        @warn "Timestamp check failed; falling back to date-only" key error=err
        try
            lm_date = get_last_modified_s3(bucket, key)
            same_day = lm_date == Date(now_utc)
            @info "Freshness check (date-only)" key last_modified_date=lm_date today_utc=Date(now_utc) same_day=same_day
            return same_day
        catch err2
            @error "Both freshness checks failed; launching defensively" key error=err2
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
        @warn "Filtered out entities not matching prop prefix" prop dropped_sample=first(dropped, min(5, length(dropped)))
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

    @info "Discovered entities" prop queue_type="standby" count=length(ents) sample=first(ents, min(5, length(ents)))
    return ents
end

function get_priority_entities(prop::String)::Vector{String}
    if prop == "uor" && !ENABLE_UOR_PRIORITY
        @info "UOR priority disabled; skipping entity discovery" prop
        return String[]
    end

    local_dir = joinpath(LOC_TEMP, "$(prop)_fastpass")
    isdir(local_dir) && rm(local_dir; force=true, recursive=true)
    mkpath(local_dir)

    s3_path = "s3://$BUCKET/export/fastpass_times/$prop"
    sync_from_s3_folder(s3_path, local_dir; exclude=["*"], include=["current_fastpass.csv"])

    df = CSV.read(joinpath(local_dir, "current_fastpass.csv"), DataFrame)

    colidx = findfirst(n -> n === :FATTID || n === "FATTID", names(df))
    colidx === nothing && error("Required column 'FATTID' not found in current_fastpass.csv; columns=$(names(df))")

    vals = String.(strip.(String.(coalesce.(df[!, colidx], ""))))
    ents = unique(filter(!isempty, vals))
    ents = filter_by_prop_prefix(ents, prop)

    @info "Discovered entities" prop queue_type="priority" count=length(ents) sample=first(ents, min(5, length(ents)))
    return ents
end

# ===================================================================================== #
# Job Launcher
# ===================================================================================== #

function run_one_job(prop::String, typ::String; max_parallel::Int=MAX_PARALLEL)
    entities = (typ == "standby") ? get_standby_entities(prop) :
               (typ == "priority") ? get_priority_entities(prop) : String[]

    if isempty(entities)
        @info "No entities discovered; nothing to launch" prop typ
        return
    end

    shuffle!(entities)
    active_jobs = Vector{Base.Process}()

    for entity in entities
        entity_s = String(entity)
        park     = derive_park(entity_s, prop)
        cmd      = `julia --project=. src/main_runner.jl $entity_s $park $prop $typ`

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
        @info "main_setup completed for today"
    else
        @info "main_setup already completed today; continuing"
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
            @info "Launching job (fresh within window)" prop typ key window=REFRESH_WINDOW
            job_tasks[(prop, typ)] = @async try
                run_one_job(prop, typ; max_parallel=MAX_PARALLEL)
            catch err
                bt = catch_backtrace()
                @error "Group task failed" prop typ error=err stacktrace=bt
                rethrow()
            end
            delete!(pending, (prop, typ))
        else
            last_mod = try get_last_modified_s3_ts(BUCKET, key) catch; nothing end
            mins_left = round(Int, (POLL_DEADLINE_UTC - now(UTC)) / Minute(1))
            @info "Not fresh yet; will poll again" prop typ key window=REFRESH_WINDOW \
                  last_modified_utc=last_mod now_utc=now(UTC) minutes_left=mins_left
        end
    end
    isempty(pending) || sleep(Dates.value(POLL_INTERVAL) * 60)
end

if !isempty(pending)
    @warn "Stopped polling because deadline hit; some groups never became fresh" remaining=collect(pending)
end

# ===================================================================================== #
# Wait for all launched groups to complete
# ===================================================================================== #

for ((prop, typ), task) in job_tasks
    wait(task)
end

elapsed = round((time_ns() - start_time_pipeline) / 1e9 / 60, digits=2)
@info "Launcher complete" elapsed_min=elapsed

# =============================================== End ================================= #
