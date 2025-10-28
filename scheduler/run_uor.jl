# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

include("../src/main_setup.jl")  # ensures Main.DATA_FACT
include("../src/main_runner.jl")

# -------------------------------------------------------------------
# Full list of entity codes grouped by property (with comments)
# -------------------------------------------------------------------
CODES_UOR = [

    # ==== Islands of Adventure (IA) ====
    "IA01",   # Spider-Man
    "IA06",   # Seuss Trolley
    "IA07",   # Incredible Hulk
    "IA08",   # JP River Adventure
    "IA09",   # One Fish
    "IA13",   # Storm Force
    "IA14",   # Cat in the Hat
    "IA15",   # Hippogriff
    "IA16",   # Forbidden Journey
    "IA65",   # Hagrid's Adventure
    "IA69",   # VelociCoaster

    # ==== Universal Studios Florida (UF) ====
    "UF02",   # E.T. Adventure
    "UF06",   # MEN IN BLACK
    "UF07",   # Mummy
    "UF12",   # Simpsons Ride
    "UF30",   # Despicable Me
    "UF48",   # Transformers
    "UF62",   # Twirl 'n' Hurl 
    "UF63",   # Gringotts

    # ==== Epic Universe (EU) ====
    "EU01",   # Stardust Racers
    "EU04",   # Mario Kart
    "EU05",   # Yoshi's Adventure
    "EU06",   # Mine-Cart Madness
    "EU07",   # Battle at the Ministry
    "EU09",   # Fyre Drill
    "EU10",   # Dragon Racer's Rally
    "EU11"   # Hiccup's Wing Gliders

]

# -------------------------------------------------------------------------------------
# Sequential calls 
# -------------------------------------------------------------------------------------
for code in CODES_UOR
    try
        run_entity(code; data_fact = Main.DATA_FACT)
    catch err
        @warn "run_entity failed" code exception=(err, catch_backtrace())
        cleanup_folders(code)
    end
end
