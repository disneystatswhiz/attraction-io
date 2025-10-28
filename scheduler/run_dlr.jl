# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

include("../src/main_setup.jl")  # ensures Main.DATA_FACT
include("../src/main_runner.jl")

# -------------------------------------------------------------------
# Full list of entity codes grouped by property (with comments)
# -------------------------------------------------------------------
CODES_DLR = [
    # ==== Disney California Adventure (CA) ====
    "CA03",   # Monsters, Inc.
    "CA09",   # Soarin'
    "CA109",  # Radiator Racers
    "CA110",  # Mater's Jamboree
    "CA148",  # Luigi's Roadsters
    "CA155",  # Guardians BREAKOUT
    "CA167",  # Incredicoaster
    "CA180",  # Emotional Whirlwind
    "CA188",  # WEB SLINGERS
    "CA22",   # Jumpin' Jellyfish
    "CA28",   # Pixar Pal-A-Round - Swing
    "CA30",   # Toy Story Mania!
    "CA39",   # Pixar Pal-A-Round - No Swing
    "CA56",   # Silly Swings - Single
    "CA67",   # Goofy's Sky School
    "CA68",   # Little Mermaid

    # ==== Disneyland (DL) ====
    "DL01",   # Alice in Wonderland
    "DL02",   # Astro Orbitor
    "DL03",   # Autopia
    "DL07",   # Buzz Lightyear
    "DL16",   # Dumbo
    "DL179",  # Millennium Falcon
    "DL18",   # Finding Nemo Subs
    "DL180",  # Rise of Resistance
    "DL24",   # Indiana Jones Adv
    "DL27",   # it's a small world
    "DL28",   # Jungle Cruise
    "DL33",   # Matterhorn
    "DL34",   # Mickey's House
    "DL37",   # Mr Toad's Wild Ride
    "DL38",   # Peter Pan's Flight
    "DL39",   # Pinocchio's Journey
    "DL40",   # Pirates of Caribbean
    "DL42",   # Roger Rabbit's Spin
    "DL45",   # Snow White
    "DL46",   # Space Mountain
    "DL50",   # Star Tours
    "DL53"   # Haunted Mansion

]

# -------------------------------------------------------------------------------------
# Sequential calls 
# -------------------------------------------------------------------------------------
for code in CODES_DLR
    try
        run_entity(code; data_fact = Main.DATA_FACT)
    catch err
        @warn "run_entity failed" code exception=(err, catch_backtrace())
        cleanup_folders(code)
    end
end
