# --------------------------------------------------------------------------
# run_thresholds.jl - Generate per-entity crowd level thresholds
# --------------------------------------------------------------------------

using JSON3, DataFrames, CSV, Statistics, Clustering, Dates

# -------------------------------------------------------
# Generate per-entity thresholds
# -------------------------------------------------------
function generate_thresholds(df::DataFrame)::DataFrame
    cols = [:entity_code, :effective_date] ∪ [Symbol("cl$(i)_max") for i in 1:9]
    out = DataFrame(; (c => Any[] for c in cols)...)
    today_str = today()

    for subdf in groupby(df, :entity_code)
        code = subdf.entity_code[1]
        waits = collect(skipmissing(subdf.avg_posted_11am_to_5pm))

        if length(waits) < 10
            # @warn("⚠️ Skipping $code — <10 days")
            push!(out, [code, today_str, fill(999.0, 9)...])
            continue
        end

        try
            X = reshape(waits, 1, :)
            result = kmeans(X, 10; maxiter=1000, display=:none)
            levels = [findfirst(==(c), sortperm(vec(result.centers))) for c in result.assignments]
            grouped = combine(groupby(DataFrame(level=levels, wait=waits), :level), :wait => maximum => :max_wait)
            sort!(grouped, :level)
            t = grouped.max_wait
            if length(t) < 9
                # @warn("⚠️ $code had <9 thresholds — fallback")
                push!(out, [code, today_str, fill(999.0, 9)...])
            else
                push!(out, [code, today_str, round.(t[1:9], digits=1)...])
            end
        catch e
            # @warn("⚠️ Error clustering $code: $e — fallback")
            push!(out, [code, today_str, fill(999.0, 9)...])
        end
    end

    sort!(out, [:entity_code, :effective_date])
    return out
end

# -------------------------------------------------------
# Main logic
# -------------------------------------------------------
function main()
    temp = "work/$(uppercase(ATTRACTION.code))/calendar"
    input_path = joinpath(temp, "forecasts_dailyavgs.csv")
    output_path = joinpath(temp, "forecasts_thresholds.csv")

    if !isfile(input_path)
        return
    end

    if isfile(output_path)
        return
    end

    df = CSV.read(input_path, DataFrame)
    required_cols = ["entity_code", "avg_posted_11am_to_5pm"]
    if !all(x -> x in names(df), required_cols)
        return
    end

    thresholds = generate_thresholds(df)
    CSV.write(output_path, thresholds)
    # @info("✅ Wrote per-entity thresholds to $output_path")
end

main()
