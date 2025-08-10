# ===================================================================================== #
#                  Attraction-IO Parallel Polling Launcher (Async)                      #
# ===================================================================================== #
using DataFrames, Dates, CSV, Logging, Random

ROOT                = abspath(joinpath(@__DIR__, ".."))
start_time_pipeline = time_ns()

# Quick note for log test
println("--------------------------------------------------------------------------------")
flush(stdout)
println("Starting pipeline at $(Dates.format(now(), "yyyy-mm-dd HH:MM:SS"))")
flush(stdout)
println("--------------------------------------------------------------------------------")
flush(stdout)

include(joinpath(ROOT, "src", "utilities", "utility_setup.jl"))

const POLL_INTERVAL = Minute(10)
const CUT_OFF_TIME  = Time(23, 59)
const BUCKET        = "touringplans_stats"
today_date          = today()

# ===================================================================================== #
# ------------------------- Utility: Entity Detection Functions ------------------------ #
# ===================================================================================== #

function get_standby_entities(prop::String)::Vector{String}
    s3_key = "export/wait_times/$prop/current_wait.csv"
    if get_last_modified_s3(BUCKET, s3_key) != today_date
        return String[]
    end

    s3_path   = "s3://$BUCKET/export/wait_times/$prop"
    local_dir = joinpath(LOC_TEMP, "$(prop)_standby")
    sync_from_s3_folder(s3_path, local_dir; exclude=["*"], include=["current_wait.csv"])

    df = CSV.read(joinpath(local_dir, "current_wait.csv"), DataFrame)
    filter!(row -> !ismissing(row.submitted_posted_time) || !ismissing(row.submitted_actual_time), df)
    return unique(df.entity_code)
end

function get_priority_entities(prop::String)::Vector{String}
    s3_key = "export/fastpass_times/$prop/current_fastpass.csv"
    if get_last_modified_s3(BUCKET, s3_key) != today_date
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
    # Identify entities for this property/type (skip if none)
    if typ == "standby"
        entities = get_standby_entities(prop)
        queue_type = "standby"
    elseif typ == "priority"
        if prop == "uor"
            return
        end
        entities = get_priority_entities(prop)
        queue_type = "priority"
    else
        return
    end

    # >>> ADD THIS LINE to run ONLY the test entity
    # entities = intersect(entities, ["AK07"])  # Replace "AK07" with your desired entity
    # log_header("Running job for test entity: $(entities)")
    # >>> ADD THIS LINE to run ONLY the test entity

    if isempty(entities)
        return
    end

    shuffle!(entities)  # Randomize order to avoid systematic bias

    active_jobs = Vector{Base.Process}()

    for entity in entities
        park = lowercase(first(entity, 2))
        cmd  = `julia --project=. src/main_runner.jl $entity $park $prop $queue_type`

        # Start the job as an external process, don't wait yet
        process = run(cmd; wait=false)
        push!(active_jobs, process)

        # If max_parallel jobs are running, wait for the first to finish
        if length(active_jobs) == max_parallel
            wait(active_jobs[1])
            popfirst!(active_jobs)
        end
    end

    # Wait for any remaining jobs to finish
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
        last_mod = get_last_modified_s3(BUCKET, key)
        if last_mod == today_date
            # Launch job as a Julia Task (async), passing the property/type
            job_tasks[(prop, typ)] = @async run_one_job(prop, typ; max_parallel=3)
            delete!(pending, (prop, typ))
        end
    end
    isempty(pending) || sleep(Dates.value(POLL_INTERVAL) * 60)
end

if !isempty(pending)
    # Optionally log warning about missing data
end

# ===================================================================================== #
# ------------------------ Wait for All Jobs to Complete and Log ---------------------- #
# ===================================================================================== #

for ((prop, typ), task) in job_tasks
    wait(task)
end

elapsed = round((time_ns() - start_time_pipeline) / 1e9 / 60, digits=2)

# ===================================================================================== #
#                                End of Polling Launcher                               #
# ===================================================================================== #
