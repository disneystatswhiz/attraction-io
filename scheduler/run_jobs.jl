# ===================================================================================== #
#                  Attraction-IO Parallel Polling Launcher (Async)                      #
# ===================================================================================== #
using DataFrames, Dates, CSV, Logging, Random

# ===================================================================================== #
# -------------------------- Initial Setup and Configuration -------------------------- #
# ===================================================================================== #

ROOT                = abspath(joinpath(@__DIR__, ".."))
start_time_pipeline = time_ns()

include(joinpath(ROOT, "src", "utilities", "utility_setup.jl"))

const POLL_INTERVAL = Minute(10)
const CUT_OFF_TIME  = Time(07, 00)
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
# ---------------------------- Job Launcher: Per-Property/Type ------------------------ #
# ===================================================================================== #

function run_one_job(prop::String, typ::String)
    if typ == "standby"
        entities = get_standby_entities(prop)
        queue_type = "standby"
    elseif typ == "priority"
        if prop == "uor"
            @info "ℹ️  UOR has no priority job—skipping."
            return
        end
        entities = get_priority_entities(prop)
        queue_type = "priority"
    else
        @warn "Unknown job type: $typ"
        return
    end

    if isempty(entities)
        @info "⚠️  No $typ entities found for $prop — skipping."
        return
    end

    shuffle!(entities)  # <-- RANDOMIZE ORDER!

    for entity in entities
        park = lowercase(first(entity, 2))
        cmd  = `julia --project=. src/main_runner.jl $entity $park $prop $queue_type`
        try
            run(cmd)
        catch e
            @warn "❌ [$prop] $entity failed: $e"
        end
    end

    # @info "✅ [$prop $typ] Completed all jobs."
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
    ("uor", "standby"),
])

job_tasks = Dict{Tuple{String,String}, Task}()

# @info "⏱  Started polling at $(now()); will cut off at $(CUT_OFF_TIME). Pending: $(pending)"

while !isempty(pending) && Time(now()) ≤ CUT_OFF_TIME
    for (prop, typ) in copy(pending)
        key = (typ == "standby") ?
            "export/wait_times/$prop/current_wait.csv" :
            "export/fastpass_times/$prop/current_fastpass.csv"
        last_mod = get_last_modified_s3(BUCKET, key)
        if last_mod == today_date
            @info "✅ Data ready for $prop $typ — launching async job"
            job_tasks[(prop, typ)] = @async run_one_job(prop, typ)
            delete!(pending, (prop, typ))
        else
            @info "⏳ Waiting for $prop $typ data to be ready... (last_mod = $last_mod, today = $today_date)"
        end
    end
    isempty(pending) || sleep(Dates.value(POLL_INTERVAL) * 60)
end

if !isempty(pending)
    @warn "⚠️  Cutoff reached—these jobs never had data: $(collect(pending))"
end

# ===================================================================================== #
# ------------------------ Wait for All Jobs to Complete and Log ---------------------- #
# ===================================================================================== #

# @info "⌛ Waiting for all launched jobs to finish..."
for ((prop, typ), task) in job_tasks
    wait(task)
    @info "🎉 Completed job for $prop $typ"
end

elapsed = round((time_ns() - start_time_pipeline) / 1e9 / 60, digits=2)
@info "🏁 All done. Total elapsed time: $(elapsed) minutes."

# ===================================================================================== #
#                                End of Polling Launcher                               #
# ===================================================================================== #
