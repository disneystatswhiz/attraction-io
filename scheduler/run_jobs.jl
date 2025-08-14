# ===================================================================================== #
#                  Attraction-IO Parallel Polling Launcher (Async)                      #
# ===================================================================================== #
using DataFrames, Dates, CSV, Logging, Random

ROOT                = abspath(joinpath(@__DIR__, ".."))
start_time_pipeline = time_ns()

println("--------------------------------------------------------------------------------")
flush(stdout)
println("Starting pipeline at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))")
flush(stdout)
println("--------------------------------------------------------------------------------")
flush(stdout)

include(joinpath(ROOT, "src", "utilities", "utility_setup.jl"))

const POLL_INTERVAL   = Minute(10)
const CUT_OFF_TIME    = Time(17, 00)
const BUCKET          = "touringplans_stats"
const REFRESH_WINDOW  = Hour(12)               # <— NEW: rolling window

# ===================================================================================== #
# ------------------------ Helper: last-modified within window ------------------------ #
# ===================================================================================== #

"""
    refreshed_within(bucket, key; window=REFRESH_WINDOW) -> Bool

Returns true if the S3 object at `bucket/key` was last modified within `window`
hours from now (UTC). Requires `get_last_modified_s3_ts(bucket, key)::DateTime` (UTC).
If that function is missing, it falls back to a date-only check (less robust).
"""
function refreshed_within(bucket::String, key::String; window::Period=REFRESH_WINDOW)::Bool
    now_utc = now(UTC)
    try
        lm = get_last_modified_s3_ts(bucket, key)   # should be UTC DateTime
        age = now_utc - lm
        return (age ≥ Second(0)) && (age ≤ window)
    catch err
        # ---- Fallback (date-only): keeps you running until you expose the TS helper.
        # NOTE: This is less robust around midnights; remove once TS helper is available.
        @warn "Falling back to date-only last_modified check; add get_last_modified_s3_ts for robustness" key
        lm_date = get_last_modified_s3(bucket, key)  # your existing Date-returning helper
        # Treat any object with last_modified date equal to today(UTC) as "refreshed"
        return lm_date == Date(now_utc)
    end
end

# ===================================================================================== #
# ------------------------- Utility: Entity Detection Functions ------------------------ #
# ===================================================================================== #

function get_standby_entities(prop::String)::Vector{String}
    s3_key = "export/wait_times/$prop/current_wait.csv"
    if !refreshed_within(BUCKET, s3_key)
        @info "Standby current_wait.csv not fresh enough; skipping entity discovery" prop s3_key
        return String[]
    end

    s3_path   = "s3://$BUCKET/export/wait_times/$prop"
    local_dir = joinpath(LOC_TEMP, "$(prop)_standby")
    sync_from_s3_folder(s3_path, local_dir; exclude=["*"], include=["current_wait.csv"])

    df = CSV.read(joinpath(local_dir, "current_wait.csv"), DataFrame)
    filter!(row -> !ismissing(row.submitted_posted_time) || !ismissing(row.submitted_actual_time), df)
    entities = unique(df.entity_code)
    return filter(x -> x != "AK07", entities)  # exclude dev/test
end

function get_priority_entities(prop::String)::Vector{String}
    s3_key = "export/fastpass_times/$prop/current_fastpass.csv"
    if !refreshed_within(BUCKET, s3_key)
        @info "Priority current_fastpass.csv not fresh enough; skipping entity discovery" prop s3_key
        return String[]
    end

    s3_path   = "s3://$BUCKET/export/fastpass_times/$prop"
    local_dir = joinpath(LOC_TEMP, "$(prop)_fastpass")
    sync_from_s3_folder(s3_path, local_dir; exclude=["*"], include=["current_fastpass.csv"])

    df = CSV.read(joinpath(local_dir, "current_fastpass.csv"), DataFrame)
    return unique(df.FATTID)
end

# ===================================================================================== #
# ----------------------- Job Launcher: Parallelized Per-Property/Type ---------------- #
# ===================================================================================== #

function run_one_job(prop::String, typ::String; max_parallel::Int=3)
    if typ == "standby"
        entities   = get_standby_entities(prop)
        queue_type = "standby"
    elseif typ == "priority"
        if prop == "uor"
            return
        end
        entities   = get_priority_entities(prop)
        queue_type = "priority"
    else
        return
    end

    # entities = intersect(entities, ["AK07"])  # for targeted testing

    if isempty(entities)
        @info "No entities discovered; nothing to launch" prop typ
        return
    end

    shuffle!(entities)

    active_jobs = Vector{Base.Process}()
    for entity in entities
        park = lowercase(first(entity, 2))
        cmd  = `julia --project=. src/main_runner.jl $entity $park $prop $queue_type`

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
# -------------------------- Optional: Run main_setup Once Per Day -------------------- #
# ===================================================================================== #

lockfile = joinpath(ROOT, "temp", "main_setup_done.lock")
if !isfile(lockfile)
    include(joinpath(ROOT, "src", "main_setup.jl"))
    mkpath(dirname(lockfile))
    open(lockfile, "w") do io end
end

# ===================================================================================== #
# -------------------------- Main Polling Loop for Job Launching ---------------------- #
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
            job_tasks[(prop, typ)] = @async run_one_job(prop, typ; max_parallel=3)
            delete!(pending, (prop, typ))
        else
            @info "Not fresh yet; will poll again" prop typ key window=REFRESH_WINDOW
        end
    end
    isempty(pending) || sleep(Dates.value(POLL_INTERVAL) * 60)
end

if !isempty(pending)
    @warn "Some property/types never became fresh within window before CUT_OFF_TIME" remaining=collect(pending)
end

# ===================================================================================== #
# ------------------------ Wait for All Jobs to Complete and Log ---------------------- #
# ===================================================================================== #

for ((prop, typ), task) in job_tasks
    wait(task)
end

elapsed = round((time_ns() - start_time_pipeline) / 1e9 / 60, digits=2)
@info "Launcher complete" elapsed_min=elapsed

# ===================================================================================== #
#                                End of Polling Launcher                               #
# ===================================================================================== #
