# -------------------------------------------------------------
# run_jobs.jl — Launch attraction modelling for entities
# needing (re)runs based on latest_obs_report.csv
# -------------------------------------------------------------
using Dates, CSV, DataFrames, TimeZones, Parquet

# --- Anchor all paths at the repo root (one level up from scheduler) ---
const ROOT         = abspath(joinpath(@__DIR__, ".."))
const TZ_LOCAL     = TimeZone("America/Toronto")

# Files/dirs relative to ROOT
const LATEST_OBS_REPORT = joinpath(ROOT, "input", "wait_times", "latest_obs_report.csv")
const WORK_DIRS         = [joinpath(ROOT, d) for d in ("input","output","temp","work")]

# Script entrypoints (absolute)
const MAIN_SETUP  = joinpath(ROOT, "src", "main_setup.jl")
const MAIN_RUNNER = joinpath(ROOT, "src", "main_runner.jl")

# How many days back counts as "needs modelling"
const FRESHNESS_WINDOW_DAYS = 2

# Include the main_runner only once and call run_entity repeatedly
include(joinpath(ROOT, "src", "main_runner.jl"))

# -------------------------------------------------------------
function must_exist(path::AbstractString)
    isfile(path) || error("Required file not found: $path")
end

function ensure_clean_dirs!(dirs::Vector{String})
    for d in dirs
        if isdir(d)
            try
                rm(d; recursive = true, force = true)
            catch e
                @warn "Failed to remove directory $d" error = e
            end
        end
        try
            mkpath(d)
        catch e
            @warn "Failed to recreate directory $d" error = e
        end
    end
end

# Only used for MAIN_SETUP because we want a fresh Julia process for that one step
function run_script(script::AbstractString, args::Vector{String}=String[])
    cmd = `$(Base.julia_cmd()) --project=. $script $(args...)`
    #println("→ Running: ", cmd)
    t0 = time()
    ok = Base.success(cmd)
    dt = round(time() - t0; digits = 1)
    #println(ok ? "✓ Completed $script in $(dt)s" : "✗ FAILED $script after $(dt)s")
    return ok
end

function load_latest_obs_report(path::AbstractString)
    df = CSV.read(path, DataFrame)

    # Ensure names are strings (your convention)
    rename!(df, Dict(n => String(n) for n in names(df)))

    # Validate required columns (as strings)
    for req in ["entity_code", "latest_observation_date"]
        req in names(df) || error("latest_obs_report is missing required column: $req")
    end

    # Coerce date column to Date (supports String/DateTime/ZonedDateTime)
    col = df[!, "latest_observation_date"]
    if eltype(col) <: Date
        # already fine
    elseif eltype(col) <: DateTime
        df[!, "latest_observation_date"] = Date.(col)
    elseif eltype(col) <: TimeZones.ZonedDateTime
        df[!, "latest_observation_date"] = Date.(col)
    else
        df[!, "latest_observation_date"] = Date.(string.(col))
    end

    # Ensure entity_code is String
    df[!, "entity_code"] = String.(df[!, "entity_code"])
    return df
end

function select_entities_needing_runs(df::DataFrame; window_days::Int=2)
    # now(TZ_LOCAL) already returns a ZonedDateTime — convert directly to Date
    today_local = Date(now(TZ_LOCAL))
    cutoff = today_local - Day(window_days)

    dates = df[!, "latest_observation_date"]
    mask  = (dates .>= cutoff) .& (dates .<= today_local)

    # De-dup; ensure lowercase for runner
    return unique(lowercase.(df[!, "entity_code"][mask]))
end

# ---------- Python fact-table refresh (Step 0) ----------
# Detect a venv if you have one; otherwise use system python.
const PY_BIN = let v = joinpath(ROOT, "venv")
    if Sys.iswindows()
        isfile(joinpath(v, "Scripts", "python.exe")) ? joinpath(v, "Scripts", "python.exe") : "python"
    else
        isfile(joinpath(v, "bin", "python")) ? joinpath(v, "bin", "python") : "python3"
    end
end

const FACT_DIR = joinpath(ROOT, "src", "fact_table")
const PY_MAIN  = joinpath(FACT_DIR, "main.py")

function run_py_main!()
    isfile(PY_MAIN) || error("Missing Python orchestrator: $PY_MAIN")
    # run from the fact_table folder so relative paths inside Python match
    cd(FACT_DIR) do
        cmd = `$(PY_BIN) $(PY_MAIN)`
        ok = Base.success(pipeline(cmd, stdout=stdout, stderr=stderr))
        ok || error("Python fact-table refresh failed (main.py).")
    end
end

function main()
    #println("===== run_jobs.jl started @ ", Dates.format(now(TZ_LOCAL), "yyyy-mm-dd HH:MM:SS zzz"), " =====")

    # Safety: ensure we're in a project root
    if !isfile("Project.toml") && !isfile("Manifest.toml")
        @warn "Project.toml/Manifest.toml not found in current directory. You're not at project root?"
    end

    # 0) Refresh fact tables via Python (optional; comment out if not needed)
    #println("Step 0/5 — Refreshing fact tables via Python")
    run_py_main!()
    
    # 1) Clean working folders (commented during testing)
    #println("Step 1/5 — Cleaning working folders: ", join(WORK_DIRS, ", "))
    ensure_clean_dirs!(WORK_DIRS)

    # 2) Run main_setup.jl to (re)sync data, folders, and specs
    #println("Step 2/5 — Running main_setup.jl")
    must_exist(MAIN_SETUP)
    Base.include(Main, MAIN_SETUP)   # <-- instead of run_script(MAIN_SETUP)

    # 3) Load latest_obs_report.csv and decide which entities to model
    #println("Step 3/5 — Loading latest_obs_report and selecting entities")
    must_exist(LATEST_OBS_REPORT)
    report   = load_latest_obs_report(LATEST_OBS_REPORT)
    entities = select_entities_needing_runs(report; window_days=FRESHNESS_WINDOW_DAYS)

    if isempty(entities)
        #println("No entities need modelling (latest observations older than $(FRESHNESS_WINDOW_DAYS) days). Done.")
        return
    end

    #println("Entities to model (sequential): ", join(entities, ", "))

    failures = String[]
    total = length(entities)

    for (i, code) in enumerate(entities)
        #println("Step 4/5 — Calling run_entity ($i/$total) for entity: $code")
        t0 = time()
        ok = try
            run_entity(code; data_fact = Main.DATA_FACT)
            true
        catch e
            @error "run_entity failed" entity=code error=e
            false
        end

        dt = round(time() - t0; digits=1)
        #println(ok ? "✓ Completed $code in $(dt)s" : "✗ FAILED $code after $(dt)s")
        ok || push!(failures, code)
        sleep(0.2)
    end

    # 5) Summary
    #println("Step 5/5 — Summary")
    if isempty(failures)
        #println("All runs completed successfully. 🎉")
    else
        #println("Some entities failed: ", join(failures, ", "))
        exit(1)  # non-zero to signal partial failure if used by a scheduler
    end
end

# -------------------------------------------------------------
main()
