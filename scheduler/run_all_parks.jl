# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

include("../src/main_setup.jl")  # ensures Main.DATA_FACT
include("../src/main_runner.jl")

# -------------------------------------------------------------------
# Full list of entity codes grouped by property (with comments)
# -------------------------------------------------------------------
CODES = [

    # ==== Animal Kingdom (AK) ====
    "AK07",   # Kilimanjaro Safaris
    "AK11",   # Expedition Everest
    "AK18",   # DINOSAUR
    "AK85",   # Na'vi River
    "AK86",   # Flight of Passage

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
    "EP20",   # Gran Fiesta Tour

    # ==== Hollywood Studios (HS) ====
    "HS103",  # Slinky Dog
    "HS104",  # Alien Saucers
    "HS111",  # Runaway Railway
    "HS112",  # Millennium Falcon
    "HS113",  # Rise of Resistance
    "HS12",   # Rock Coaster
    "HS15",   # Star Tours
    "HS20",   # Toy Story Mania!
    "HS22",   # Tower of Terror

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
    "MK43",    # Tom'land Speedway

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
    "DL53",   # Haunted Mansion

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
    "EU11"    # Hiccup's Wing Gliders

]

# -------------------------------------------------------------------------------------
# Sequential calls 
# -------------------------------------------------------------------------------------
for code in CODES
    try
        run_entity(code; data_fact = Main.DATA_FACT)
    catch err
        @warn "run_entity failed" code exception=(err, catch_backtrace())
        cleanup_folders(code)
    end
end
