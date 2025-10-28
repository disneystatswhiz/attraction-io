# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

include("../src/main_setup.jl")  # ensures Main.DATA_FACT
include("../src/main_runner.jl")

# -------------------------------------------------------------------
# Full list of entity codes grouped by property (with comments)
# -------------------------------------------------------------------
CODES_EP = [

    # ==== EPCOT (EP) ====
    "EP02",   # Spaceship Earth
    "EP04",   # Seas with Nemo
    "EP07",   # Living w/ Land
    "EP09",   # Soarin'
    "EP13",   # Jrny Imagination
    "EP14",   # Test Track
    "EP155",  # Frozen Ever After
    "EP16",   # Msn: SPACE Orange
    "EP186",  # Remy's Adventure
    "EP20"   # Gran Fiesta Tour

]

# -------------------------------------------------------------------------------------
# Sequential calls 
# -------------------------------------------------------------------------------------
for code in CODES_EP
    try
        run_entity(code; data_fact = Main.DATA_FACT)
    catch err
        @warn "run_entity failed" code exception=(err, catch_backtrace())
        cleanup_folders(code)
    end
end
