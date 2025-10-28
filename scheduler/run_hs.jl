# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

include("../src/main_setup.jl")  # ensures Main.DATA_FACT
include("../src/main_runner.jl")

# -------------------------------------------------------------------
# Full list of entity codes grouped by property (with comments)
# -------------------------------------------------------------------
CODES_HS = [

    # ==== Hollywood Studios (HS) ====
    "HS103",  # Slinky Dog
    "HS104",  # Alien Saucers
    "HS111",  # Runaway Railway
    "HS112",  # Millennium Falcon
    "HS113",  # Rise of Resistance
    "HS12",   # Rock Coaster
    "HS15",   # Star Tours
    "HS20",   # Toy Story Mania!
    "HS22"   # Tower of Terror

]

# -------------------------------------------------------------------------------------
# Sequential calls 
# -------------------------------------------------------------------------------------
for code in CODES_HS
    try
        run_entity(code; data_fact = Main.DATA_FACT)
    catch err
        @warn "run_entity failed" code exception=(err, catch_backtrace())
        cleanup_folders(code)
    end
end
