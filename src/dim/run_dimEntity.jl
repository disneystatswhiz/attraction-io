# -------------------------------------------------------
# run_dimEntity.jl
# Build and distribute dimEntity
# -------------------------------------------------------

using JSON3, Dates, DataFrames, CSV, Glob

# --- Sync all entity files from S3 ---
sync_entity_files()  # ✅ Pulls entire s3://.../entities/ to input/entities/

# --- Read and combine all current_*_entities.csv files ---
entity_files = glob("input/entities/current_*_entities.csv")
df_all_entities = DataFrame()

for file in entity_files
    df = CSV.read(file, DataFrame)

    # Patch: Force :land column to have consistent type
    if :land in names(df)
        df.land = convert(Vector{Union{Missing, String}}, df.land)
    else
        df.land = Vector{Union{Missing, String}}(missing, nrow(df))  # Create empty land column
    end

    append!(df_all_entities, df; cols = :union)
end

# --- Write master dimEntity table ---
CSV.write("work/dim/dimentity.csv", df_all_entities)
