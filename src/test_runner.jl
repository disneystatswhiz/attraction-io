# ===================================================================================== #
# ------------------------------ Attraction IO Runner --------------------------------- #
# ===================================================================================== #

include("../src/main_setup.jl")  # ensures Main.DATA_FACT
include("../src/main_runner.jl")

import Base.Threads: @spawn, @sync

# -------------------------------------------------------------------
# Function to run entities with a fixed-size thread pool
# -------------------------------------------------------------------
function run_entities_pool(codes::Vector{String}, data_fact; nworkers::Int=6)
    jobs = Channel{String}(length(codes))
    for c in codes
        put!(jobs, c)
    end
    close(jobs)

    @sync for _ in 1:nworkers
        @spawn begin
            for code in jobs
                try
                    run_entity(code; data_fact=data_fact)
                catch err
                    @warn "run_entity failed" code exception=(err, catch_backtrace())
                    cleanup_folders(code)
                end
            end
        end
    end
end

# -------------------------------------------------------------------
# Full list of entity codes grouped by property (with comments)
# -------------------------------------------------------------------
CODES = [
    "CA03",   # Monsters, Inc.
    "DL24",   # Indiana Jones Adv
    "IA06",   # Seuss Trolley
    "UF07",   # Mummy
    "EU04",   # Mario Kart
    "AK11",   # Expedition Everest
    "EP155",  # Frozen Ever After
    "HS104",  # Alien Saucers
    "MK05"   # Peter Pan's Flight
]

# -------------------------------------------------------------------
# Run entities (limit to 6 concurrent threads)
# -------------------------------------------------------------------
run_entities_pool(CODES, Main.DATA_FACT; nworkers=3)
