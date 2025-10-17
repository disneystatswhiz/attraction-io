# ===================================================================================== #
# -------------------------- Parse and Validate Arguments ----------------------------- #
# ===================================================================================== #

if length(ARGS) < 4
    # @error "❌ ARGS missing. Usage: julia main_runner.jl <entity> <park> <property> <type>"
    exit(1)
end

const ENTITY_CODE = lowercase(ARGS[1])
const PARK        = lowercase(ARGS[2])
const PROPERTY    = lowercase(ARGS[3])
const QUEUE_TYPE  = lowercase(ARGS[4])

# ===================================================================================== #
# ------------------------- Initial Setup and Modules --------------------------------- #
# ===================================================================================== #
ROOT = abspath(joinpath(@__DIR__, ".."))
start_time_pipeline = time_ns()

include(joinpath(ROOT, "src", "utilities", "utility_setup.jl"))
include(joinpath(ROOT, "src", "modules", "mod_customloaders.jl"))
include(joinpath(ROOT, "src", "modules", "mod_encoders.jl"))
using .CustomLoaders
using .EncodeFeatures

# ===================================================================================== #
# -------------------- Preprocessing & Feature Engineering ---------------------------- #
# ===================================================================================== #

include(joinpath(ROOT, "src", "data", "run_set_attraction.jl"))
include(joinpath(ROOT, "src", "data", "run_sync.jl"))
include(joinpath(ROOT, "src", "data", "run_tracking.jl"))
include(joinpath(ROOT, "src", "data", "run_wait_time_ingestion.jl"))
include(joinpath(ROOT, "src", "data", "run_futuredates.jl"))
include(joinpath(ROOT, "src", "data", "run_features.jl"))
include(joinpath(ROOT, "src", "data", "run_premodelling.jl"))

# ===================================================================================== #
# -------------------------- Forecasting + Scoring ------------------------------------ #
# ===================================================================================== #

include(joinpath(ROOT, "src", "modelling", "run_encodefeatures.jl"))
include(joinpath(ROOT, "src", "modelling", "run_trainer.jl"))
include(joinpath(ROOT, "src", "modelling", "run_predictions.jl"))
# include(joinpath(ROOT, "src", "modelling", "run_writer.jl"))  # Optional S3 writer

# ===================================================================================== #
# ------------------------ Crowd Calendar Assignment ---------------------------------- #
# ===================================================================================== #

include(joinpath(ROOT, "src", "calendar", "run_dailyavgs.jl"))
include(joinpath(ROOT, "src", "calendar", "run_thresholds.jl"))
include(joinpath(ROOT, "src", "calendar", "run_assign_levels.jl"))
include(joinpath(ROOT, "src", "calendar", "run_observed_dailyavgs.jl"))

# ===================================================================================== #
# ----------------------------- Reporting Outputs ------------------------------------- #
# ===================================================================================== #


include(joinpath(ROOT, "src", "reporting", "run_descriptives.jl"))
include(joinpath(ROOT, "src", "reporting", "run_accuracyreports.jl"))
include(joinpath(ROOT, "src", "reporting", "run_daily_wait_time_curve.jl"))

# ===================================================================================== #
# ---------------------------- Final Logging and Cleanup ------------------------------ #
# ===================================================================================== #

elapsed_modelling = (time_ns() - start_time_pipeline) / 1e9
@info "✅ Modelling completed for $(ATTRACTION.code) - $(ATTRACTION.name) in $(round(elapsed_modelling / 60, digits=2)) minutes."
flush(stdout)
flush(stderr)

if !(ATTRACTION.code in [""])
    cleanup_folders(ATTRACTION.code, base_dir=ROOT)
end
# ====================================================================================== #
# ----------------------------- End of Main Runner Script ------------------------------ #