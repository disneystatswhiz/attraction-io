# ===================================================================================== #
# ---------------------- Skip setup if already run today ----------------------------- #
# ===================================================================================== #

using Dates

if !isdefined(@__MODULE__, :ROOT)
    const ROOT = dirname(@__DIR__)
end

SETUP_INPUT = joinpath(ROOT, "input", "dimEntity.csv")  # or another stable output

if isfile(SETUP_INPUT)
    @info "🔧 Checking setup status..."
    if Date(Dates.unix2datetime(stat(SETUP_INPUT).mtime)) == TODAY_DATE
        println("✅ Setup already completed today — skipping.")
        exit(0)
    end
end

# ===================================================================================== #
# ------------------------- Setup attraction-io pipeline ------------------------------ #
# ===================================================================================== #

# --- Start timer
start_time_pipeline = time_ns()

# --- Include shared utility setup logic
include(joinpath(ROOT, "src", "utilities", "utility_setup.jl"))

# --- Include dimension table setup scripts
include(joinpath(ROOT, "src", "dim", "run_dimDate.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimEntity.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimParkHours.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimEvents.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimHolidays.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimMetatable.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimDateGroupID.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimSeason.jl"))

# --- Donor table scripts
include(joinpath(ROOT, "src", "donor", "run_donorParkHours.jl"))

# --- Script to sync all wait time files
include(joinpath(ROOT, "src", "data", "run_raw_wait_sync.jl"))

# --- End timer and report
elapsed_dim = (time_ns() - start_time_pipeline) / 1e9
log_header("✅ Added Dimension & Donor tables & Wait Time syncs in $(round(elapsed_dim / 60, digits=2)) minutes.")
# ===================================================================================== #