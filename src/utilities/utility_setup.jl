# src/utilities/utility_setup.jl
using CSV, DataFrames, Dates

const LOC_WORK   = joinpath(ROOT, "work")
const LOC_INPUT  = joinpath(ROOT, "input")
const LOC_OUTPUT = joinpath(ROOT, "output")
const LOC_DIM    = joinpath(LOC_WORK, "_dim")
const LOC_DONOR  = joinpath(LOC_WORK, "_donor")
const LOC_TEMP   = joinpath(ROOT, "temp")

const TODAY_DATE = Dates.today()

# Include Structs module once
if !isdefined(Main, :Structs)
    include(joinpath(ROOT, "src", "utilities", "structs.jl"))
end

# Make selected structs globally available
using .Structs: Attraction, ParkEvent, EventDay

# Include other utilities
include(joinpath(ROOT, "src", "utilities", "utils.jl"))
include(joinpath(ROOT, "src", "utilities", "s3utils.jl"))
include(joinpath(ROOT, "src", "utilities", "s3syncmanager.jl"))
include(joinpath(ROOT, "src", "utilities", "features.jl"))
