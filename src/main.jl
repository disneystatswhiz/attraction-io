# ===================================================================================== #
# ---------------------- Parse Command-Line Arguments --------------------------------- #
# ===================================================================================== #

if length(ARGS) >= 4
    const ENTITY_CODE = lowercase(ARGS[1])
    const PARK        = lowercase(ARGS[2])
    const PROPERTY    = lowercase(ARGS[3])
    const QUEUE_TYPE  = lowercase(ARGS[4])
else
    # @info "No ARGS passed. Falling back to hardcoded values (VS Code mode)."
    const ENTITY_CODE = "ia16"
    const PARK        = "ia"
    const PROPERTY    = "uor"
    const QUEUE_TYPE  = "standby"
end

println("🔧 Running with: ENTITY=$ENTITY_CODE | PARK=$PARK | PROPERTY=$PROPERTY | TYPE=$QUEUE_TYPE")

# ===================================================================================== #

# --- Start timer
start_time_pipeline = time_ns()
const ROOT = dirname(@__DIR__)

# ===================================================================================== #
# ------------------------- Setup attraction-io pipeline ------------------------------ #
# ===================================================================================== #

# --- Define our custom type structures only if Structs module is not already defined
if !@isdefined Structs
    include(joinpath(ROOT, "src", "utilities", "Structs.jl"))
end
using .Structs: Attraction, Event, EventDay


# --- Include necessary functions
include(joinpath(ROOT, "src", "utilities", "utils.jl"))
include(joinpath(ROOT, "src", "utilities", "s3utils.jl"))
include(joinpath(ROOT, "src", "utilities", "s3syncmanager.jl"))
include(joinpath(ROOT, "src", "utilities", "features.jl"))

# --- Include dimension table setup scripts ---
include(joinpath(ROOT, "src", "dim", "run_dimDate.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimEntity.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimParkHours.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimEvents.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimHolidays.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimMetatable.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimDateGroupID.jl"))
include(joinpath(ROOT, "src", "dim", "run_dimSeason.jl"))

# --- Include scripts to create donor tables ---
include(joinpath(ROOT, "src", "donor", "run_donorParkHours.jl"))

# ===================================================================================== #
# -------------------- Modelling Setup for Attraction of Interest --------------------- #
# ===================================================================================== #

# --- Load in the module that knows how to identify and process each input file type
if !@isdefined CustomLoaders
    include(joinpath(ROOT, "src", "modules", "mod_customLoaders.jl"))
end
using ..CustomLoaders
# ---

# --- Include the pre-modelling scripts ---
include(joinpath(ROOT, "src", "data", "run_set_attraction.jl"))
include(joinpath(ROOT, "src", "data", "run_sync.jl"))
include(joinpath(ROOT, "src", "data", "run_tracking.jl"))
include(joinpath(ROOT, "src", "data", "run_wait_time_ingestion.jl"))
include(joinpath(ROOT, "src", "data", "run_futuredates.jl"))
include(joinpath(ROOT, "src", "data", "run_features.jl"))
include(joinpath(ROOT, "src", "data", "run_premodelling.jl"))

# ================================================================================ #
# -------------------- Forecasting for Attraction of Interest -------------------- #
# ================================================================================ #

# --- Load in the module that encodes features in prep for XGboost
if !@isdefined EncodeFeatures
    include(joinpath(ROOT, "src", "modules", "mod_encoders.jl"))
end
using ..EncodeFeatures
# ---

include(joinpath(ROOT, "src", "modelling", "run_encodefeatures.jl"))
include(joinpath(ROOT, "src", "modelling", "run_trainer.jl"))
include(joinpath(ROOT, "src", "modelling", "run_predictions.jl"))
#include(joinpath(ROOT, "src", "modelling", "run_writer.jl")) # Currently not used, this sends the predictions to S3 PROD or STAGING

# ================================================================================ #
# ----------- Crowd Calendar Calculations for Attraction of Interest ------------- #
# ================================================================================ #

include(joinpath(ROOT, "src", "calendar", "run_dailyavgs.jl"))
include(joinpath(ROOT, "src", "calendar", "run_thresholds.jl"))
include(joinpath(ROOT, "src", "calendar", "run_assign_levels.jl"))


# ======================================================================= #
# ----------- Reporting Regimend for Attraction of Interest ------------- #
# ======================================================================= #

include(joinpath(ROOT, "src", "data", "run_sync.jl")) # Refresh the data from S3 before running reporting
include(joinpath(ROOT, "src", "reporting", "run_descriptives.jl"))
#include(joinpath(ROOT, "src", "reporting", "run_accuracyreports.jl"))

# --- Lap time for calendar calculations
elapsed_modelling = (time_ns() - start_time_pipeline) / 1e9
log_header("✅ Modelling completed for $(ATTRACTION.code) - $(ATTRACTION.name) in $(round(elapsed_modelling / 60, digits=2)) minutes.")

# -------------------- Cleanup and Finalization ----------------------------------- #
cleanup_folders(ATTRACTION.code, base_dir=ROOT)
