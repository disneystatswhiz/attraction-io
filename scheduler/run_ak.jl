# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

include("../src/main_setup.jl")  # ensures Main.DATA_FACT
include("../src/main_runner.jl")

# -------------------------------------------------------------------
# Full list of entity codes grouped by property (with comments)
# -------------------------------------------------------------------
CODES_AK = [

    # ==== Animal Kingdom (AK) ====
    "AK07",   # Kilimanjaro Safaris
    "AK11",   # Expedition Everest
    "AK18",   # DINOSAUR
    "AK85",   # Na'vi River
    "AK86"    # Flight of Passage

]

# -------------------------------------------------------------------------------------
# Sequential calls 
# -------------------------------------------------------------------------------------
for code in CODES_AK
    try
        run_entity(code; data_fact = Main.DATA_FACT)
    catch err
        @warn "run_entity failed" code exception=(err, catch_backtrace())
        cleanup_folders(code)
    end
end
