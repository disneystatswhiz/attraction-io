# -----------------------------------------------------------
# run_tracking.jl — Freshness via latest_obs_report (date-level)
# -----------------------------------------------------------
using CSV, DataFrames, Dates, TimeZones

# Safe rename: only rename if the old name exists
function safe_rename!(df::AbstractDataFrame, mapping::Dict{Symbol,Symbol})
    for (old, new) in mapping
        if old in names(df) && old != new
            rename!(df, old => new)
        end
    end
    return df
end

# Parse to DateTime safely
parse_dt(x)::DateTime = x isa DateTime ? x :
                        x isa ZonedDateTime ? DateTime(x) :
                        DateTime(string(x))

# Latest encoded OBSERVED DATE for this entity+type from already_on_s3
function latest_encoded_observed_date(entity_code::String, wt::String)::Date
    f = joinpath(
        LOC_WORK,
        uppercase(entity_code),
        "already_on_s3",
        "wait_times_$(uppercase(entity_code))_$(lowercase(wt)).csv"
    )
    if !isfile(f) || filesize(f) == 0
        return Date(1900,1,1)
    end
    try
        df = CSV.read(f, DataFrame; select=["observed_at"])
        if nrow(df) == 0
            return Date(1900,1,1)
        end
        return maximum(Date.(parse_dt.(df.observed_at)))
    catch
        return Date(unix2datetime(stat(f).mtime))
    end
end

# Local paths for synced inputs
fact_table_path() = joinpath(LOC_INPUT, "wait_times", "wait_time_fact_table.parquet")
latest_report_path() = joinpath(LOC_INPUT, "wait_times", "latest_obs_report.csv")

"""
Returns Dict{String,Vector{String}} mapping each wait_time_type to:
- [fact_table_path] if latest_obs_report shows a newer **date** than we've encoded
- [] otherwise
"""
function run_tracking(attraction::Attraction)::Dict{String, Vector{String}}
    rep_fp = latest_report_path()
    isfile(rep_fp) || throw(ArgumentError("latest_obs_report not found at $rep_fp"))

    rep = CSV.read(rep_fp, DataFrame)

    # Normalize expected names
    safe_rename!(rep, Dict(
        :attraction_code => :entity_code,
        :entity          => :entity_code,
        :latest_observed_date => :latest_observation_date,
        :latest_obs_date      => :latest_observation_date
    ))

    for col in ("entity_code", "latest_observation_date")
        col in names(rep) || throw(ArgumentError("latest_obs_report missing column: $col"))
    end

    # Find this entity's latest date in the report
    uc = uppercase(attraction.code)
    row = rep[uppercase.(rep.entity_code) .== uc, :]
    latest_in_report_date =
        nrow(row) == 0 ? nothing : maximum(Date.(string.(row.latest_observation_date)))

    # Prepare return shape
    enqueue = Dict{String, Vector{String}}()
    for wt in getproperty(attraction, Val(:wait_time_types))
        enqueue[wt] = String[]
    end

    ft = fact_table_path()
    if latest_in_report_date === nothing || !isfile(ft)
        return enqueue
    end

    # Compare per type (even though report is entity-level)
    for wt in keys(enqueue)
        latest_already_encoded = latest_encoded_observed_date(uc, wt)
        if latest_in_report_date > latest_already_encoded
            push!(enqueue[wt], ft)
        end
    end

    return enqueue
end

# Execute for current ATTRACTION
new_files_by_type = run_tracking(ATTRACTION)
