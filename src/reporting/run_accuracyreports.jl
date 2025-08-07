using CSV, DataFrames, Dates, Statistics, TimeZones

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

# Helper for safe quantiles
safe_quantile(v, q) = isempty(v) ? missing : quantile(v, q)

function process_accuracy_report(
    attraction_code::String,
    wait_time_type::String,
    df_joined::DataFrame
)
    upper_code = uppercase(attraction_code)
    suffix = lowercase(wait_time_type)

    # Only include relevant rows
    filter!(row -> !ismissing(row.abs_error), df_joined)
    if nrow(df_joined) == 0
        # @warn "No joined accuracy data for $attraction_code [$wait_time_type]."
        return nothing
    end

    # Mode helper
    function mode_skipmissing(v)
        v_clean = collect(skipmissing(v))
        isempty(v_clean) && return missing
        counts = Dict{eltype(v_clean), Int}()
        for x in v_clean
            counts[x] = get(counts, x, 0) + 1
        end
        max_count = maximum(values(counts))
        modes = [k for (k, cnt) in counts if cnt == max_count]
        return maximum(modes)
    end

    # Basic summary stats (Abs/Rel Error)
    abs_errors = skipmissing(df_joined.abs_error)
    rel_errors = skipmissing(df_joined.rel_error)

    overall_stats = DataFrame(
        attraction_code    = upper_code,
        wait_time_type     = uppercase(wait_time_type),
        n_obs              = nrow(df_joined),
        mean_abs_error     = mean(abs_errors),
        median_abs_error   = median(abs_errors),
        mode_abs_error     = mode_skipmissing(abs_errors),
        p25_abs_error      = safe_quantile(abs_errors, 0.25),
        p75_abs_error      = safe_quantile(abs_errors, 0.75),
        p95_abs_error      = safe_quantile(abs_errors, 0.95),
        stddev_abs_error   = std(abs_errors),
        max_abs_error      = maximum(abs_errors),
        min_abs_error      = minimum(abs_errors)
    )

    # Round numeric floats
    for col in names(overall_stats)
        if eltype(overall_stats[!, col]) <: AbstractFloat
            overall_stats[!, col] .= round.(overall_stats[!, col], digits=3)
        end
    end

    # --- Hourly profile for errors ---
    df_joined[!, :hour_of_day] = mod.(floor.(Int, df_joined.mins_since_6am ./ 60) .+ 6, 24)
    
    function hour_label(hour::Int)
        hour = mod(hour, 24)
        if hour == 0
            return "hour_midnight"
        elseif hour == 12
            return "hour_12pm"
        elseif hour < 12
            return "hour_$(hour)am"
        else
            return "hour_$(hour - 12)pm"
        end
    end

    # Group by hour_of_day and compute mean absolute error
    df_hour = combine(groupby(df_joined, :hour_of_day), :abs_error => mean => :mean_abs_error)
    df_hour.hour_label = hour_label.(df_hour.hour_of_day)
    df_hour.mean_abs_error .= round.(df_hour.mean_abs_error, digits=1)
    hour_dict = Dict(df_hour.hour_label .=> df_hour.mean_abs_error)

    ordered_labels = [
        "hour_6am", "hour_7am", "hour_8am", "hour_9am", "hour_10am", "hour_11am",
        "hour_12pm", "hour_1pm", "hour_2pm", "hour_3pm", "hour_4pm", "hour_5pm",
        "hour_6pm", "hour_7pm", "hour_8pm", "hour_9pm", "hour_10pm", "hour_11pm",
        "hour_midnight", "hour_1am", "hour_2am", "hour_3am", "hour_4am", "hour_5am"
    ]

    hour_stats = DataFrame([NamedTuple{Tuple(Symbol.(ordered_labels))}(Tuple(get(hour_dict, label, missing) for label in ordered_labels))])


    # Final combine
    overall_stats = hcat(overall_stats, hour_stats)
    overall_stats.summary_date = fill(today(), 1)

    # Write output
    out_name   = "accuracy_summary_$(upper_code)_$(suffix).csv"
    local_path = joinpath(LOC_WORK, uppercase(upper_code), "already_on_s3", out_name)
    out_path   = joinpath(LOC_OUTPUT, uppercase(upper_code), out_name)
    s3_path    = "s3://touringplans_stats/stats_work/attraction-io/reporting/$(out_name)"

    CSV.write(local_path, overall_stats; append=isfile(local_path))
    mkpath(dirname(out_path))  # <-- Ensures the folder exists!
    cp(local_path, out_path; force=true)
    upload_file_to_s3(out_path, s3_path)

    # @info "✅ Finished accuracy report for $upper_code [$suffix]"
end

# ---------------------------------------------------------
# Run it!
# ---------------------------------------------------------

for wait_type in wait_time_types
    wt_lower = lowercase(wait_type)
    obs_path = joinpath(LOC_WORK, ATTRACTION.code, "already_on_s3", "wait_times_$(uppercase(ATTRACTION.code))_$(wt_lower).csv")
    fcast_path = joinpath(LOC_WORK, ATTRACTION.code, "already_on_s3", "forecasts_$(uppercase(ATTRACTION.code))_$(wt_lower).csv")

    if isfile(obs_path) && isfile(fcast_path)
        df_obs = load_observed(obs_path)
        df_fcast = load_forecast(fcast_path)
        df_joined = innerjoin(df_obs, df_fcast, on=[:observed_at_r15, :wait_time_type])
        filter!(row -> !ismissing(row.wait_time_minutes) && !ismissing(row.predicted_wait_time), df_joined)
        compute_errors(df_joined)
        process_accuracy_report(ATTRACTION.code, wt_lower, df_joined)
    end
end
