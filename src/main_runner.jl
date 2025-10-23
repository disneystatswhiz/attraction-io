# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

const ENTITY_CODE = Ref{String}("")
if !isdefined(@__MODULE__, :ROOT)
    const ROOT = abspath(joinpath(@__DIR__, ".."))
end

include(joinpath(ROOT, "src", "utilities", "utility_setup.jl"))
include(joinpath(ROOT, "src", "modules", "mod_encoders.jl"))
using .EncodeFeatures

function run_entity(entity_code::AbstractString; data_fact = nothing)
    # Start timing per run
    start_time_pipeline = time_ns()

    ENTITY_CODE[] = lowercase(String(entity_code))

    # Load or validate the in-memory fact table
    local_df = data_fact === nothing ? (isdefined(Main, :DATA_FACT) ? Main.DATA_FACT : nothing) : data_fact
    local_df === nothing && error("DATA_FACT not available. Run main_setup.jl first or pass data_fact=...")

    # ================= Preprocessing & Feature Engineering ==================
    include(joinpath(ROOT, "src", "data", "run_set_attraction.jl"))
    include(joinpath(ROOT, "src", "data", "run_sync.jl"))
    include(joinpath(ROOT, "src", "data", "run_wait_time_ingestion.jl"))
    include(joinpath(ROOT, "src", "data", "run_futuredates.jl"))
    include(joinpath(ROOT, "src", "data", "run_features.jl"))
    include(joinpath(ROOT, "src", "data", "run_premodelling.jl"))

    # ======================= Forecasting + Scoring ==========================
    include(joinpath(ROOT, "src", "modelling", "run_encodefeatures.jl"))
    include(joinpath(ROOT, "src", "modelling", "run_trainer.jl"))
    include(joinpath(ROOT, "src", "modelling", "run_predictions.jl"))
    # include(joinpath(ROOT, "src", "modelling", "run_writer.jl"))

    # =================== Crowd Calendar Assignment =========================
    include(joinpath(ROOT, "src", "calendar", "run_dailyavgs.jl"))
    include(joinpath(ROOT, "src", "calendar", "run_thresholds.jl"))
    include(joinpath(ROOT, "src", "calendar", "run_assign_levels.jl"))
    include(joinpath(ROOT, "src", "calendar", "run_observed_dailyavgs.jl"))

    # ========================= Reporting Outputs ===========================
    include(joinpath(ROOT, "src", "reporting", "run_descriptives.jl"))
    include(joinpath(ROOT, "src", "reporting", "run_accuracyreports.jl"))
    include(joinpath(ROOT, "src", "reporting", "run_daily_wait_time_curve.jl"))

    # ===================== Final Logging and Cleanup =======================
    elapsed_modelling = (time_ns() - start_time_pipeline) / 1e9
    log_header("✅ Completed run for entity '$(ENTITY_CODE[])' in $(round(elapsed_modelling / 60, digits=2)) minutes.")
    flush(stdout); flush(stderr)

    # Only cleanup if ATTRACTION exists
    if isdefined(Main, :ATTRACTION)
        if !(ATTRACTION.code in (ENTITY_CODE[],))
            cleanup_folders(ATTRACTION.code, base_dir=ROOT)
        end
    end

    return nothing
end

if abspath(PROGRAM_FILE) == @__FILE__
    if length(ARGS) < 1
        @error "❌ ARGS missing. Usage: julia src/main_runner.jl <entity>"
        exit(1)
    end
    run_entity(ARGS[1])
end
