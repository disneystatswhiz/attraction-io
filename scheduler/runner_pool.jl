# ===================================================================================== #
#                                Attraction IO Runner                                   #
#                          (Parallel, process-based pool)                               #
# ===================================================================================== #

using Distributed
using LinearAlgebra
using JLD2
using FilePathsBase: rm, isdir
using CSV, DataFrames, Dates

# --------------------------------- Config -------------------------------------------- #
const THREADS_PER_WORKER = 2
const UTILIZATION        = 0.85
const CLEAN_ON_START     = true

# --------------------------------- Sizing -------------------------------------------- #
const CPU             = Sys.CPU_THREADS
const MAX_WORKERS     = 6
const TARGET_THREADS  = max(THREADS_PER_WORKER, floor(Int, CPU * UTILIZATION))
const N_WORKERS_RAW   = max(1, floor(Int, TARGET_THREADS ÷ THREADS_PER_WORKER))
const N_WORKERS       = min(MAX_WORKERS, N_WORKERS_RAW)

# --------------------------------- Spawn Workers ------------------------------------- #
addprocs(
    N_WORKERS;
    exeflags = ["--project=@.", "-t", string(THREADS_PER_WORKER)],
    env = Dict(
        "JULIA_NUM_THREADS"    => string(THREADS_PER_WORKER),
        "OMP_NUM_THREADS"      => "1",
        "OPENBLAS_NUM_THREADS" => "1",
        "MKL_NUM_THREADS"      => "1",
    )
)

@everywhere begin
    using LinearAlgebra, Dates, JLD2
    BLAS.set_num_threads(1)
end

# ------------------------------ Optional Clean Start --------------------------------- #
if CLEAN_ON_START
    for folder in ["output", "input", "work", "temp"]
        if isdir(folder); rm(folder; force=true, recursive=true); end
    end
end


# ------------------------------ Global Setup (MASTER ONLY) --------------------------- #
include("../src/main_setup.jl")     # builds Main.DATA_FACT, syncs S3
local_dfact = Main.DATA_FACT

# ------------------------------ Entity List ------------------------------------------ #

# ---- Load and filter entity codes dynamically ----
const LATEST_OBS_PATH = "input/wait_times/latest_obs_report.csv"
latest_obs_df = CSV.read(LATEST_OBS_PATH, DataFrame)

# Parse dates
latest_obs_df.latest_observation_date = (latest_obs_df.latest_observation_date)

# Filter: keep entities observed in the last 3 days
cutoff_date = today() - Day(3)
recent_obs_df = filter(row -> row.latest_observation_date >= cutoff_date, latest_obs_df)

# Extract codes
const CODES = sort(unique(recent_obs_df.entity_code))

# @info "Loaded $(length(CODES)) entities observed within the last 3 days"
# @debug "Entity codes: $(CODES)"

# Filter DATA_FACT to include only relevant entities
if :entity_code in propertynames(local_dfact)
    n0 = nrow(local_dfact)
    local_dfact = filter(row -> row[:entity_code] in CODES, local_dfact)
    # @info "Filtered DATA_FACT" before=n0 after=nrow(local_dfact)
else
    # @warn "DATA_FACT has no entity_code column to filter"
end

# ---------------- Split DATA_FACT into per-code caches (MASTER) ---------------------- #
const DF_SPLIT_DIR = joinpath(@__DIR__, "..", "temp", "cache", "data_fact_split")
mkpath(DF_SPLIT_DIR)

for code in CODES
    df_sub = filter(row -> row[:entity_code] == code, local_dfact)
    if nrow(df_sub) == 0
        @warn "No DATA_FACT rows for $code; skipping split"
        continue
    end
    JLD2.@save joinpath(DF_SPLIT_DIR, "data_fact_$(lowercase(code)).jld2") DATA_FACT=df_sub
end

# Free memory on master (optional)
local_dfact = nothing
GC.gc()

# Make split dir path visible on workers
@everywhere const DF_SPLIT_DIR = $(DF_SPLIT_DIR)

# ------------------------------ Load runner code on WORKERS -------------------------- #
@everywhere begin
    include("../src/main_runner.jl")
    isdefined(Main, :run_entity) || error("run_entity not loaded on worker $(myid())")
end

# ------------------------------ Safe Wrapper ----------------------------------------- #
@everywhere function safe_run_entity(code::AbstractString)
    logdir = joinpath("work", code)
    try
        # Load only this entity's DATA_FACT slice
        split_path = joinpath(DF_SPLIT_DIR, "data_fact_$(lowercase(code)).jld2")
        if !isfile(split_path)
            throw(ArgumentError("Missing split file: $(split_path)"))
        end

        # Set per-worker global (safe: each worker is its own process)
        Main.DATA_FACT = JLD2.load(split_path, "DATA_FACT")

        # Still pass it explicitly (future-proof & clearer)
        run_entity(code; data_fact = Main.DATA_FACT)

        return (code, :ok, nothing)

    catch err
        try
            mkpath(logdir)
            open(joinpath(logdir, "_error.txt"), "w") do io
                println(io, "timestamp=$(Dates.now())")
                println(io, "code=$code")
                println(io, "err=$(err)")
            end
        catch end
        try cleanup_folders(code) catch end
        return (code, :error, string(err))
    finally
        # Optional: free memory on this worker between entities
        # Main.DATA_FACT = nothing; GC.gc()
    end
end

# ------------------------------ Execute Pool ----------------------------------------- #
results = pmap(safe_run_entity, CODES; batch_size=1)

# ------------------------------ Summary Output --------------------------------------- #
ok   = count(r -> r[2] === :ok, results)
errs = filter(r -> r[2] === :error, results)
for (code, _, msg) in errs
    # @info("  - $code: $msg")
end
