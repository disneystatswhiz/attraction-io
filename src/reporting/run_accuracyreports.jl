using CSV, DataFrames, Dates, Statistics, Plots, TimeZones

# ---------------------------------------------------------
# functions
# ---------------------------------------------------------

function clean_reporting_columns(df::DataFrame)::DataFrame
    renames = Dict{Symbol, Symbol}()

    for col in names(df)
        col_str = String(col)
        newname = if col_str == "target"
            :wait_time_minutes
        elseif startswith(col_str, "meta_") || startswith(col_str, "pred_") || startswith(col_str, "id_")
            Symbol(replace(col_str, r"^(meta_|pred_|id_)" => ""))
        else
            Symbol(col_str)  # keep as is
        end

        if newname != col  # only rename if different
            renames[Symbol(col)] = newname
        end
    end

    return rename(df, renames)
end

function load_observed(path::String)::DataFrame
    df = CSV.read(path, DataFrame) |> clean_reporting_columns
    filter!(:wait_time_minutes => !ismissing, df)
    df[!, :observed_at] = parse_zoneddatetimes_simple(df.observed_at)
    df[!, :observed_at_r15] = round_datetime.(df.observed_at, 15)
    select!(df, Not(:wgt_geo_decay))
    return df
end

function load_forecast(path::String)::DataFrame
    df = CSV.read(path, DataFrame) |> clean_reporting_columns
    rename!(df, :observed_at => :observed_at_r15)
    filter!(:predicted_wait_time => !ismissing, df)
    df[!, :observed_at_r15] = parse_zoneddatetimes_simple(df.observed_at_r15)
    return select(df, :observed_at_r15, :wait_time_type, :predicted_wait_time)
end

function compute_errors(df::DataFrame)::DataFrame
    df[!, :wait_time_minutes] = Int.(df.wait_time_minutes)
    df[!, :abs_error] = abs.(df.wait_time_minutes .- df.predicted_wait_time)
    df[!, :rel_error] = df.abs_error ./ df.wait_time_minutes
    df.rel_error = ifelse.(isnan.(df.rel_error), missing, df.rel_error)
    return df
end

function summarize_errors(df::DataFrame)::DataFrame
    combine(groupby(df, :wait_time_type),
        :wait_time_minutes   => mean => :wait_time_mean,
        :predicted_wait_time => mean => :predicted_mean,
        :abs_error           => mean => :mae,
        :rel_error           => mean => :mre
    )
end

# ---------------------------------------------------------
# Main Logic
# ---------------------------------------------------------

function run_accuracy_reports(attraction_code::String)
    # Load observed wait times
    obs_posted = load_observed(joinpath(LOC_WORK, attraction_code, "already_on_s3", "wait_times_posted.csv"))
    obs_actual = load_observed(joinpath(LOC_WORK, attraction_code, "already_on_s3", "wait_times_actual.csv"))
    df_obs = vcat(obs_posted, obs_actual)

    # Load forecasts
    fcast_posted = load_forecast(joinpath(LOC_WORK, attraction_code, "already_on_s3", "forecasts_posted.csv"))
    fcast_actual = load_forecast(joinpath(LOC_WORK, attraction_code, "already_on_s3", "forecasts_actual.csv"))
    df_forecast = vcat(fcast_posted, fcast_actual)

    # Join on timestamp and type
    df_joined = innerjoin(df_obs, df_forecast, on=[:observed_at_r15, :wait_time_type])
    filter!(row -> !ismissing(row.wait_time_minutes) && !ismissing(row.predicted_wait_time), df_joined)

    # Compute errors
    compute_errors(df_joined)

    # Summary stats
    mae = mean(skipmissing(df_joined.abs_error))
    mre = mean(skipmissing(df_joined.rel_error))

    @info "Overall MAE for $attraction_code: $mae"
    @info "Overall MRE for $attraction_code: $mre"

    df_summary = summarize_errors(df_joined)
    display(df_summary)

    return df_summary
end

# ---------------------------------------------------------
# Run it!
# ---------------------------------------------------------

df_summary = run_accuracy_reports(ATTRACTION.code)
