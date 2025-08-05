using CSV, DataFrames, Statistics, Dates, Plots

# ----------------------------
# Load and clean observations for a given wait_time_type
# ----------------------------
function process_descriptives(attraction_code::String, wait_time_type::String)
    upper_code = uppercase(attraction_code)
    suffix = lowercase(wait_time_type)

    path = joinpath(LOC_OUTPUT, uppercase(upper_code), "wait_times_$(upper_code)_$(suffix).csv")
    if !isfile(path)
        # @warn "Skipping $path — file not found."
        return nothing
    end

    df = CSV.read(path, DataFrame)
    filter!(row -> !ismissing(row.target), df)
    df.meta_observed_at = parse_zoneddatetimes_simple(df.meta_observed_at)

    # --- Basic summary stats ---
    function mode_skipmissing(v)
        # Remove missings
        v_clean = collect(skipmissing(v))
        isempty(v_clean) && return missing

        # Count frequencies
        counts = Dict{eltype(v_clean), Int}()
        for x in v_clean
            counts[x] = get(counts, x, 0) + 1
        end

        # Find the maximum count
        max_count = maximum(values(counts))

        # All values that hit that count
        modes = [k for (k, cnt) in counts if cnt == max_count]

        # Return the highest mode
        return maximum(modes)
    end

    overall_stats = DataFrame(
        attraction_code = first(df.id_entity_code),
        wait_time_type  = first(df.meta_wait_time_type),
        n_obs           = nrow(df),
        n_days          = length(unique(df.id_park_day_id)),
        first_obs       = minimum(df.meta_observed_at),
        last_obs        = maximum(df.meta_observed_at),
        missing_pct     = 100 * count(ismissing, df.target) / nrow(df),
        zero_pct        = 100 * count(==(0), df.target) / nrow(df),
        mean_wait       = mean(skipmissing(df.target)),
        median_wait     = median(skipmissing(df.target)),
        mode_wait       = mode_skipmissing(df.target),
        p25_wait        = quantile(skipmissing(df.target), 0.25),
        p75_wait        = quantile(skipmissing(df.target), 0.75),
        p95_wait        = quantile(skipmissing(df.target), 0.95),
        stddev_wait     = std(skipmissing(df.target)),
        max_wait        = maximum(skipmissing(df.target)),
        min_wait        = minimum(skipmissing(df.target)),
    )

    # Round numeric floats
    for col in names(overall_stats)
        if eltype(overall_stats[!, col]) <: AbstractFloat
            overall_stats[!, col] .= round.(overall_stats[!, col], digits=1)
        end
    end

    # --- Hourly profile ---
    df.hour_of_day = floor.(Int, df.pred_mins_since_6am ./ 60)

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

    df_hour = combine(groupby(df, :hour_of_day), :target => mean => :mean_wait)
    df_hour.hour_label = hour_label.(df_hour.hour_of_day)
    df_hour.mean_wait .= round.(df_hour.mean_wait, digits=1)
    hour_dict = Dict(df_hour.hour_label .=> df_hour.mean_wait)

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

    # Write output (append only)
    out_name   = "descriptive_summary_$(upper_code)_$(suffix).csv"
    local_path = joinpath(LOC_WORK, uppercase(upper_code), "already_on_s3", out_name)
    out_path   = joinpath(LOC_OUTPUT, uppercase(upper_code), out_name)
    s3_path    = "s3://touringplans_stats/stats_work/attraction-io/reporting/$(out_name)"

    CSV.write(local_path, overall_stats; append=isfile(local_path))
    cp(local_path, out_path; force=true)
    upload_file_to_s3(out_path, s3_path)

    # @info "✅ Finished descriptives for $upper_code [$suffix]"
end

# ----------------------------
# Loop through all wait time types for this attraction
# ----------------------------
wait_time_types = ATTRACTION.queue_type == "priority" ? ["PRIORITY"] : ["POSTED", "ACTUAL"]

for wait_type in wait_time_types
    wt_lower = lowercase(wait_type)
    model_path = joinpath(LOC_WORK, ATTRACTION.code, "wait_times", "model_$(wt_lower).bst")

    if isfile(model_path)
        process_descriptives(ATTRACTION.code, wt_lower)
    else
        # @info "⏭️ Skipping descriptives for $(ATTRACTION.code) [$wt_lower] — model not found at $model_path"
    end
end
