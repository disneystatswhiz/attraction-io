# ===================================================================================== #
#                                Attraction IO Runner                                   #
#                          (Parallel, process-based pool)                               #
# ===================================================================================== #

using Distributed
using LinearAlgebra
using JLD2
using FilePathsBase: rm, isdir
# (mkpath comes from Base)

# --------------------------------- Config -------------------------------------------- #
const THREADS_PER_WORKER = 2
const UTILIZATION        = 0.80
const CLEAN_ON_START     = false

# --------------------------------- Sizing -------------------------------------------- #
const CPU             = Sys.CPU_THREADS
const MAX_WORKERS     = 6
const TARGET_THREADS  = max(THREADS_PER_WORKER, floor(Int, CPU * UTILIZATION))
const N_WORKERS_RAW   = max(1, floor(Int, TARGET_THREADS ÷ THREADS_PER_WORKER))
const N_WORKERS       = min(MAX_WORKERS, N_WORKERS_RAW)

# --------------------------------- Spawn Workers ------------------------------------- #
addprocs(
    N_WORKERS;
    exeflags = ["--project=@.", "-t", string(THREADS_PER_WORKER)],
    env = Dict(
        "JULIA_NUM_THREADS"    => string(THREADS_PER_WORKER),
        "OMP_NUM_THREADS"      => "1",
        "OPENBLAS_NUM_THREADS" => "1",
        "MKL_NUM_THREADS"      => "1",
    )
)

@everywhere begin
    using LinearAlgebra, Dates, JLD2
    BLAS.set_num_threads(1)
end

# ------------------------------ Optional Clean Start --------------------------------- #
if CLEAN_ON_START
    for folder in ["output", "input", "work", "temp"]
        if isdir(folder); rm(folder; force=true, recursive=true); end
    end
end

# ------------------------------ Entity List ------------------------------------------ #
const CODES = [

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

# ------------------------------ Global Setup (MASTER ONLY) --------------------------- #
include("../src/main_setup.jl")     # builds Main.DATA_FACT, syncs S3
local_dfact = Main.DATA_FACT

# Filter DATA_FACT to include only relevant entities
if :entity_code in propertynames(local_dfact)
    n0 = nrow(local_dfact)
    local_dfact = filter(row -> row[:entity_code] in CODES, local_dfact)
    # @info "Filtered DATA_FACT" before=n0 after=nrow(local_dfact)
else
    # @warn "DATA_FACT has no entity_code column to filter"
end

# ---------------- Split DATA_FACT into per-code caches (MASTER) ---------------------- #
const DF_SPLIT_DIR = joinpath(@__DIR__, "..", "temp", "cache", "data_fact_split")
mkpath(DF_SPLIT_DIR)

for code in CODES
    df_sub = filter(row -> row[:entity_code] == code, local_dfact)
    if nrow(df_sub) == 0
        @warn "No DATA_FACT rows for $code; skipping split"
        continue
    end
    JLD2.@save joinpath(DF_SPLIT_DIR, "data_fact_$(lowercase(code)).jld2") DATA_FACT=df_sub
end

# Free memory on master (optional)
local_dfact = nothing
GC.gc()

# Make split dir path visible on workers
@everywhere const DF_SPLIT_DIR = $(DF_SPLIT_DIR)

# ------------------------------ Load runner code on WORKERS -------------------------- #
@everywhere begin
    include("../src/main_runner.jl")
    isdefined(Main, :run_entity) || error("run_entity not loaded on worker $(myid())")
end

# ------------------------------ Safe Wrapper ----------------------------------------- #
@everywhere function safe_run_entity(code::AbstractString)
    logdir = joinpath("work", code)
    try
        # Load only this entity's DATA_FACT slice
        split_path = joinpath(DF_SPLIT_DIR, "data_fact_$(lowercase(code)).jld2")
        if !isfile(split_path)
            throw(ArgumentError("Missing split file: $(split_path)"))
        end

        # Set per-worker global (safe: each worker is its own process)
        Main.DATA_FACT = JLD2.load(split_path, "DATA_FACT")

        # Still pass it explicitly (future-proof & clearer)
        run_entity(code; data_fact = Main.DATA_FACT)

        return (code, :ok, nothing)

    catch err
        try
            mkpath(logdir)
            open(joinpath(logdir, "_error.txt"), "w") do io
                println(io, "timestamp=$(Dates.now())")
                println(io, "code=$code")
                println(io, "err=$(err)")
            end
        catch end
        try cleanup_folders(code) catch end
        return (code, :error, string(err))
    finally
        # Optional: free memory on this worker between entities
        # Main.DATA_FACT = nothing; GC.gc()
    end
end

# ------------------------------ Execute Pool ----------------------------------------- #
results = pmap(safe_run_entity, CODES; batch_size=1)

# ------------------------------ Summary Output --------------------------------------- #
ok   = count(r -> r[2] === :ok, results)
errs = filter(r -> r[2] === :error, results)
for (code, _, msg) in errs
    # @info("  - $code: $msg")
end
