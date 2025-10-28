# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

include("../src/main_setup.jl")  # ensures Main.DATA_FACT
include("../src/main_runner.jl")

# -------------------------------------------------------------------
# Full list of entity codes grouped by property (with comments)
# -------------------------------------------------------------------
CODES_MK = [

    # ==== Magic Kingdom (MK) ====
    "MK01",   # Space Mountain
    "MK05",   # Peter Pan's Flight
    "MK06",   # Winnie the Pooh
    "MK13",   # Jungle Cruise
    "MK141",  # 7 Dwarfs Train
    "MK142",  # Under the Sea
    "MK15",   # Magic Carpets
    "MK16",   # Pirates of Caribbean
    "MK23",   # Haunted Mansion
    "MK27",   # Dumbo
    "MK28",   # it's a small world
    "MK29",   # Mad Tea Party
    "MK34",   # Barnstormer
    "MK39",   # Astro Orbiter
    "MK43"    # Tom'land Speedway

]

# -------------------------------------------------------------------------------------
# Sequential calls 
# -------------------------------------------------------------------------------------
for code in CODES_MK
    try
        run_entity(code; data_fact = Main.DATA_FACT)
    catch err
        @warn "run_entity failed" code exception=(err, catch_backtrace())
        cleanup_folders(code)
    end
end
