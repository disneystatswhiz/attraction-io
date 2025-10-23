# -------------------------------------------------------------
# run_wait_time_ingestion.jl — write ONE combined file per entity
# -------------------------------------------------------------
using CSV, DataFrames, Dates, Logging, TimeZones

# Assumes:
#   - DATA_FACT in memory (from run_raw_wait_sync.jl)
#   - Utils in scope: parse_zoneddatetimes_smart, log_header (optional)
#   - ATTRACTION has .code, .wait_time_types, .timezone

# Filter DATA_FACT to our attraction code 
df = filter(row -> uppercase(string(row.entity_code)) == uppercase(ATTRACTION.code), DATA_FACT)

# Rename wait_time_minutes → observed_wait_time
rename!(df, :wait_time_minutes => :observed_wait_time)

# Output the filtered and renamed DataFrame to a CSV file
output_folder = joinpath(LOC_WORK, uppercase(ATTRACTION.code), "wait_times")
mkpath(output_folder)
output_file = joinpath(output_folder, "wait_times.csv")
CSV.write(output_file, df)
# @info "Filtered DATA_FACT and wrote $(nrow(df)) rows to $output_file"
