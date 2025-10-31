# -------------------------------------------------------
# run_dimParkHours.jl
# Script to build and distribute the dimParkHours table
# -------------------------------------------------------

using JSON3, Dates, DataFrames, CSV, TimeZones

# --- Sync park hour file from S3 ---
sync_parkhours_files()

# --- Construct full file path ---
parkhours_folder = joinpath(LOC_INPUT, "parkhours")

# --- Check file exists ---
if !isdir(parkhours_folder)
    error("❌ Park hours folder not found: $parkhours_folder")
end

# --- Read and combine all park hour CSVs ---
csv_files = filter(f -> endswith(f, ".csv"), readdir(parkhours_folder, join=true))

if isempty(csv_files)
    error("❌ No park hour CSV files found in: $parkhours_folder")
end

dfs = DataFrame[]

for file in csv_files
    # @info "🔧 Reading park hours from $file"
    df = redirect_stderr(devnull) do
        CSV.read(file, DataFrame)
    end
    push!(dfs, df)
end

parkhours_df = vcat(dfs...)

# --- Process park hours data ---
wanted_cols = [:date, :park, :opening_time, :opening_time_with_emh, :closing_time, :closing_time_with_emh_or_party, :emh_morning, :emh_evening]
df_parkhours = select(parkhours_df, wanted_cols)

# Rename columns to match expected schema
rename!(df_parkhours, Dict(
    :date => :park_date,
    :park => :park_code
))

dropmissing!(df_parkhours)

# Ensure opening_time, opening_time_with_emh, closing_time, closing_time_with_emh_or_party are DateTime in ISO8601 format
df_parkhours.opening_time = parse_zoneddatetimes(df_parkhours.opening_time)
df_parkhours.opening_time_with_emh = parse_zoneddatetimes(df_parkhours.opening_time_with_emh)
df_parkhours.closing_time = parse_zoneddatetimes(df_parkhours.closing_time)
df_parkhours.closing_time_with_emh_or_party = parse_zoneddatetimes(df_parkhours.closing_time_with_emh_or_party)

# Float hours since midnight (e.g., 8:30 AM → 8.5)
df_parkhours.opening_hour = [ismissing(x) ? missing : hour(x) + minute(x)/60 for x in df_parkhours.opening_time]
df_parkhours.closing_hour = [ismissing(x) ? missing : hour(x) + minute(x)/60 for x in df_parkhours.closing_time]

# Total open time in hours (via minutes / 60)
df_parkhours.hours_open = [ismissing(open) || ismissing(close) ? missing :
    (close - open) / Hour(1)
    for (open, close) in zip(df_parkhours.opening_time, df_parkhours.closing_time)]

# Save as dim_parkhours.csv
dim_parkhours_file = joinpath("work", "_dim", "dimparkhours.csv")
CSV.write(dim_parkhours_file, df_parkhours)

# --- Upload to S3 ---
upload_file_to_s3(dim_parkhours_file, "s3://touringplans_stats/stats_work/dimension_tables/dimparkhours.csv")

# --- end of run_dimParkHours.jl ---