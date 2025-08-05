# ------------------------------------------------------------------ #
# run_set_attraction.jl - Given an attraction code, set the          #
# corresponding attraction struct for processing                     #
# ------------------------------------------------------------------ #

dim_path = joinpath(LOC_DIM, "dimentity.csv")
entities_df = CSV.read(dim_path, DataFrame)
attraction_row = entities_df[entities_df.code .== uppercase(ENTITY_CODE), :]
if nrow(attraction_row) == 0
    error("❌ Attraction not found: $ENTITY_CODE")
end
row = attraction_row[1, :]
ATTRACTION = build_attraction_struct(row, ENTITY_CODE)
