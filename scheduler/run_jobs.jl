# ===================================================================================== #
#            Attraction‑IO Parallel Polling Launcher (Minimal‑Fix Reuse)               #
# ===================================================================================== #
# This keeps your original structure and function names, but fixes the issues we saw:
# - Only the outer loop checks freshness; getters never re-check (avoid double-gating)
# - Priority uses FATTID reliably (Symbol/String tolerant) and logs discovery
# - Standby uses entity_code and logs discovery
# - Local sync dirs are wiped per call to avoid cross‑park bleed
# - Park derivation is robust; entities coerced to String (no SubString issues)
# - main_setup lockfile is per‑day (not once‑ever)
# - Poll loop is unchanged, but now each group launches only once and logs cleanly
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
const POLL_INTERVAL   = Minute(10)
const CUT_OFF_TIME    = Time(09, 00)
const REFRESH_WINDOW  = Hour(12)
const BUCKET          = "touringplans_stats"
const MAX_PARALLEL    = 3

# Optional: allow/deny UOR priority
const ENABLE_UOR_PRIORITY = false

# ===================================================================================== #
# Freshness helper (unchanged from yours)
# ===================================================================================== #

function refreshed_within(bucket::String, key::String; window::Period=REFRESH_WINDOW)::Bool
    now_utc = now(UTC)
    try
        lm = get_last_modified_s3_ts(bucket, key)
        age = now_utc - lm
        return (age ≥ Second(0)) && (age ≤ window)
    catch err
        @warn "Falling back to date-only last_modified check; add get_last_modified_s3_ts for robustness" key
        lm_date = get_last_modified_s3(bucket, key)
        return lm_date == Date(now_utc)
    end
end

# ===================================================================================== #
# Utilities
# ===================================================================================== #

# Robust park derivation (AK07, MK139, CA119, IA06, etc.)
@inline function derive_park(entity::AbstractString, fallback_prop::AbstractString)
    m = match(r"^[A-Za-z]{2}", entity)
    return m === nothing ? lowercase(fallback_prop) : lowercase(m.match)
end

# Light sanity map to avoid cross‑park mixes (optional; adjust as needed)
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
# Entity discovery (single‑source of truth: no freshness checks inside)
# ===================================================================================== #

function get_standby_entities(prop::String)::Vector{String}
    # Clean per‑prop scratch to avoid stale files
    local_dir = joinpath(LOC_TEMP, "$(prop)_standby")
    isdir(local_dir) && rm(local_dir; force=true, recursive=true)
    mkpath(local_dir)

    s3_path = "s3://$BUCKET/export/wait_times/$prop"
    sync_from_s3_folder(s3_path, local_dir; exclude=["*"], include=["current_wait.csv"])

    df = CSV.read(joinpath(local_dir, "current_wait.csv"), DataFrame)
    # Keep your filter: we only care about rows with a submitted wait
    filter!(row -> !ismissing(row.submitted_posted_time) || !ismissing(row.submitted_actual_time), df)

    if "entity_code" ∉ names(df)
        error("Required column 'entity_code' not found in current_wait.csv; columns=$(names(df))")
    end

    vals = String.(strip.(String.(coalesce.(df[!, "entity_code"], ""))))

    ents = unique(filter(!isempty, vals))
    ents = filter(!=("AK07"), ents)                 # exclude dev/test
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

    # Accept either Symbol or String column name
    colidx = findfirst(n -> n === :FATTID || n === "FATTID", names(df))
    colidx === nothing && error("Required column 'FATTID' not found in current_fastpass.csv; columns=$(names(df))")

    vals = String.(strip.(String.(coalesce.(df[!, colidx], ""))))
    ents = unique(filter(!isempty, vals))
    ents = filter_by_prop_prefix(ents, prop)

    @info "Discovered entities" prop queue_type="priority" count=length(ents) sample=first(ents, min(5, length(ents)))
    return ents
end

# ===================================================================================== #
# Job Launcher (parallel per entity) — unchanged structure
# ===================================================================================== #

function run_one_job(prop::String, typ::String; max_parallel::Int=MAX_PARALLEL)
    if typ == "standby"
        entities   = get_standby_entities(prop)
        queue_type = "standby"
    elseif typ == "priority"
        entities   = get_priority_entities(prop)
        queue_type = "priority"
    else
        return
    end

    if isempty(entities)
        @info "No entities discovered; nothing to launch" prop typ
        return
    end

    shuffle!(entities)

    active_jobs = Vector{Base.Process}()
    for entity in entities
        entity_s = String(entity)
        park     = derive_park(entity_s, prop)
        cmd      = `julia --project=. src/main_runner.jl $entity_s $park $prop $queue_type`

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
# Polling loop — unchanged shape, but single freshness gate
# ===================================================================================== #

pending = Set([
    ("wdw", "standby"),
    ("wdw", "priority"),
    ("dlr", "standby"),
    ("dlr", "priority"),
    ("uor", "standby")
])

job_tasks = Dict{Tuple{String,String}, Task}()

while !isempty(pending) && Time(now()) ≤ CUT_OFF_TIME
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
            @info "Not fresh yet; will poll again" prop typ key window=REFRESH_WINDOW last_modified_utc=last_mod now_utc=now(UTC)
        end
    end
    isempty(pending) || sleep(Dates.value(POLL_INTERVAL) * 60)  # sleep seconds
end

if !isempty(pending)
    @warn "Some property/types never became fresh within window before CUT_OFF_TIME" remaining=collect(pending)
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
