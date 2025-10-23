# ------------------------------------------------------------------ #
# run_set_attraction.jl - Given an attraction code, set the          #
# corresponding attraction struct for processing                     #
# ------------------------------------------------------------------ #

using CSV, DataFrames

# ENTITY_CODE is a Ref{String}; unwrap it to a plain String
@assert isdefined(Main, :ENTITY_CODE) && !isempty(ENTITY_CODE[]) "ENTITY_CODE not set"
code    = String(ENTITY_CODE[])       # e.g., "ak06" (lowercase in your runner)
code_up = uppercase(code)             # match against DIM which stores uppercase codes

dim_path    = joinpath(LOC_DIM, "dimentity.csv")
entities_df = CSV.read(dim_path, DataFrame)

# Robust match: coerce to String and uppercase before comparing
codes_up = uppercase.(String.(entities_df.code))
idx = findfirst(==(code_up), codes_up)
idx === nothing && error("❌ Attraction not found in dim: $code (looked for $code_up in $dim_path)")

row = entities_df[idx, :]

# Build ATTRACTION with the lowercase code (consistent with the rest of the pipeline)
ATTRACTION = build_attraction_struct(row, code)
