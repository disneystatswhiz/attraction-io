# -------------------------------------------------------
# run_dimMetatable.jl - Download current metatable from S3
# -------------------------------------------------------

s3path = "s3://touringplans_stats/export/metatable/current_metatable.csv"
locpath = joinpath(LOC_INPUT, "metatable", "current_metatable.csv")

# Ensure the local directory exists
mkpath(dirname(locpath))

# Download the current metatable file from S3
download_file_from_s3(s3path, locpath)

# --- Upload to S3 ---
upload_file_to_s3(locpath, "s3://touringplans_stats/stats_work/dimension_tables/dimmetatable.csv")

# -------------------------------------------------------