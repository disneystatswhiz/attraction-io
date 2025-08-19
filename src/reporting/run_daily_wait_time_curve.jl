# --------------------------------------------------------------------------------- #
# run_daily_wait_time_curve.jl
# PNGs for wait-time curves, EC2-safe, strictly keyed by id_park_day_id.
# - PRIORITY: 2 series (Observed vs Predicted posted)
# - STANDBY : 4 series (Posted Observed/Predicted + Actual Observed/Predicted)
#             with labels for Actual Observed points: "25 @ 9:32 a.m."
#
# After saving, each PNG is uploaded with upload_file_to_s3().
# --------------------------------------------------------------------------------- #
using CSV, DataFrames, Dates
using CairoMakie
using Logging

# ---------------- Config ----------------
const MIN_POINTS     = 10      # min rows per series for a day to qualify
const POSITIVE_ONLY  = false    # ignore observed rows where target <= 0
const LABEL_TEXTSIZE = 10      # size for actual-observed data labels

CODE  = ATTRACTION.code
QTYPE = lowercase(strip(String(ATTRACTION.queue_type)))  # "priority" | "standby"

# Where to send images in S3 (folder-style)
const S3_BASE = "s3://touringplans_stats/stats_work/attraction-io/reporting/"

# Canonical column names (resolve to actual key per-DF)
const COL_ID  = "id_park_day_id"
const COL_X   = "pred_mins_since_6am"
const COL_OBS = "target"
const COL_FC  = "predicted_wait_time"

# ---------------- Paths + Load (queue-type aware) ----------------
base = joinpath(LOC_WORK, CODE, "already_on_s3")

read_required(p) = (isfile(p) ? CSV.read(p, DataFrame) : error("Missing file: $p"))

if QTYPE == "priority"
    # Priority uses its own pair; no "actual" series for this queue type
    path_priority_obs = joinpath(base, "wait_times_$(CODE)_priority.csv")
    path_priority_fc  = joinpath(base, "forecasts_$(CODE)_priority.csv")

    df_posted_obs = read_required(path_priority_obs)   # normalize name to "posted" for reuse downstream
    df_posted_fc  = read_required(path_priority_fc)
    df_actual_obs = DataFrame()  # empty — not used for priority plots
    df_actual_fc  = DataFrame()

    # Remove target >= 8000 for this analysis, only if target is not missing
    obs_key = obscol(df_posted_obs)
    df_posted_obs = df_posted_obs[.!ismissing.(df_posted_obs[!, obs_key]) .& (df_posted_obs[!, obs_key] .< 8000), :]

else
    # Standby uses posted + actual (4 files)
    path_posted_obs = joinpath(base, "wait_times_$(CODE)_posted.csv")
    path_actual_obs = joinpath(base, "wait_times_$(CODE)_actual.csv")
    path_posted_fc  = joinpath(base, "forecasts_$(CODE)_posted.csv")
    path_actual_fc  = joinpath(base, "forecasts_$(CODE)_actual.csv")

    df_posted_obs = read_required(path_posted_obs)
    df_actual_obs = read_required(path_actual_obs)
    df_posted_fc  = read_required(path_posted_fc)
    df_actual_fc  = read_required(path_actual_fc)
end

# ---------------- Column resolution helpers ----------------
"Resolve a column by name (case-insensitive). Returns the actual key (Symbol or String)."
function resolve_col(df::DataFrame, wanted::AbstractString)
    for c in names(df)
        s = String(c)
        if s == wanted || lowercase(s) == lowercase(wanted)
            return c
        end
    end
    error("Column '$wanted' not found. Available columns: $(names(df))")
end

idcol(df)  = resolve_col(df, COL_ID)
xcol(df)   = resolve_col(df, COL_X)
obscol(df) = resolve_col(df, COL_OBS)
fccol(df)  = resolve_col(df, COL_FC)

# Ensure id_park_day_id exists and is Date (NO derivation from other columns)
function coerce_idday!(df::DataFrame)
    c = idcol(df)  # must exist, or resolve_col throws
    T = eltype(df[!, c])
    if T <: Date
        return df
    elseif T <: Integer
        df[!, c] = Date.(string.(df[!, c]), dateformat"yyyymmdd")
    elseif T <: AbstractString
        v = df[!, c]
        if all(s -> length(s) == 8 && !isnothing(tryparse(Int, s)), v)
            df[!, c] = Date.(v, dateformat"yyyymmdd")
        else
            df[!, c] = Date.(v)  # assume ISO yyyy-mm-dd
        end
    else
        error("Unsupported eltype for $COL_ID: $(T).")
    end
    return df
end

# helper: case-insensitive column existence
has_col(df::DataFrame, wanted::AbstractString) =
    any(lowercase(String(c)) == lowercase(wanted) for c in names(df))

# safe coercion loop (handles empty DFs)
for df in (df_posted_obs, df_posted_fc, df_actual_obs, df_actual_fc)
    has_col(df, COL_ID) && coerce_idday!(df)
end

# ---------------- Day selection (strictly by id_park_day_id) ----------------
"Return Vector{Date} of days with at least min_points rows matching cond."
function days_meeting(df::DataFrame; cond::Function = _ -> true, min_points::Int=1)
    nrow(df) > 0 || return Date[]
    c = idcol(df)
    filtered = cond === (_ -> true) ? df : df[cond.(eachrow(df)), :]
    nrow(filtered) > 0 || return Date[]
    days = Date[]
    for sub in groupby(filtered, c)
        if nrow(sub) >= min_points
            push!(days, sub[1, c])
        end
    end
    return days
end

"Pick most recent day with observed POSTED + forecast POSTED; else obs-only; else fc-only."
function choose_plot_day(df_obs_posted::DataFrame, df_fc_posted::DataFrame;
                         positive_only::Bool=true, min_points::Int=1)
    c_obs = obscol(df_obs_posted)
    cond_obs = positive_only ?
        (r -> !ismissing(r[c_obs]) && r[c_obs] > 0) :
        (r -> !ismissing(r[c_obs]))
    obs_days = days_meeting(df_obs_posted; cond=cond_obs, min_points=min_points)
    fc_days  = days_meeting(df_fc_posted;   cond = _ -> true, min_points=min_points)

    common = intersect(obs_days, fc_days)
    if !isempty(common)
        return maximum(common), :ok
    elseif !isempty(obs_days)
        return maximum(obs_days), :obs_only
    elseif !isempty(fc_days)
        return maximum(fc_days), :fc_only
    else
        return nothing, :none
    end
end

# ---------------- Utilities ----------------
_xy(df, xkey, ykey) = (Float64.(df[!, xkey]), Float64.(df[!, ykey]))
function ensure_outdir(code::AbstractString)
    d = joinpath(LOC_WORK, code, "plots")  # save in plots/
    isdir(d) || mkpath(d)
    d
end
function sort_by_x!(df::DataFrame)
    xc = xcol(df)
    (xc ∈ names(df) && nrow(df) > 0) && sort!(df, xc)
    return df
end

# Nice "9:32 a.m." formatter for minutes since 6am
function fmt_ampm(mins_since_6::Real)
    t = Time(6) + Minute(round(Int, mins_since_6))
    h24 = hour(t); m = minute(t)
    h12 = h24 % 12; h12 = (h12 == 0 ? 12 : h12)
    ampm = h24 < 12 ? "a.m." : "p.m."
    return string(h12, ":", lpad(string(m), 2, '0'), " ", ampm)
end

# ---------------- Plotters (return local file path) ----------------
# PRIORITY (2 series, blue)
function make_priority_plot(df_obs::DataFrame, df_fc::DataFrame;
                            code::String, date_for_title::Date, outfile::String)::String
    sort_by_x!(df_obs); sort_by_x!(df_fc)
    x_obs, y_obs = _xy(df_obs, xcol(df_obs), obscol(df_obs))
    x_fc , y_fc  = _xy(df_fc,  xcol(df_fc),  fccol(df_fc))

    f = Figure(size=(1100, 650))
    ax = Axis(f[1,1],
        title    = "$code — $(Dates.format(date_for_title, dateformat"yyyy-mm-dd"))",
        subtitle = "Priority: Observed vs Predicted",
        xlabel   = "Minutes since 6am", ylabel = "Wait time (minutes)")

    handles = Any[]; labels = String[]
    if !isempty(x_obs)
        h = lines!(ax, x_obs, y_obs; linewidth=2, color=:blue)
        band!(ax, x_obs, zeros(length(y_obs)), y_obs; color=:blue, alpha=0.12)
        push!(handles, h); push!(labels, "Observed")
    end
    if !isempty(x_fc)
        h = lines!(ax, x_fc, y_fc; linewidth=3, linestyle=:dot, color=:blue)
        push!(handles, h); push!(labels, "Predicted")
    end
    !isempty(handles) && axislegend(ax, handles, labels; position=:rt)

    x_all = vcat(x_obs, x_fc); y_all = vcat(y_obs, y_fc)
    if !isempty(x_all) && !isempty(y_all)
        xlims!(ax, 0, maximum(x_all))
        ylims!(ax, 0, 1.1 * max(1.0, maximum(y_all)))
    end

    outdir  = ensure_outdir(code)
    outpath = joinpath(outdir, outfile)
    save(outpath, f)
    return outpath
end

# STANDBY (4 series: posted blue + actual orange, with labels on Actual Observed)
function make_standby_combined_plot(df_pobs::DataFrame, df_pfc::DataFrame,
                                    df_aobs::DataFrame, df_afc::DataFrame;
                                    code::String, date_for_title::Date, outfile::String)::String
    sort_by_x!(df_pobs); sort_by_x!(df_pfc); sort_by_x!(df_aobs); sort_by_x!(df_afc)
    xp_obs, yp_obs = _xy(df_pobs, xcol(df_pobs), obscol(df_pobs))
    xp_fc , yp_fc  = _xy(df_pfc,  xcol(df_pfc),  fccol(df_pfc))
    xa_obs, ya_obs = _xy(df_aobs, xcol(df_aobs), obscol(df_aobs))
    xa_fc , ya_fc  = _xy(df_afc,  xcol(df_afc),  fccol(df_afc))

    f = Figure(size=(1100, 650))
    ax = Axis(f[1,1],
        title    = "$code — $(Dates.format(date_for_title, dateformat"yyyy-mm-dd"))",
        subtitle = "Standby: Posted & Actual (Observed vs Predicted)",
        xlabel   = "Minutes since 6am", ylabel = "Wait time (minutes)")

    handles = Any[]; labels = String[]

    if !isempty(xp_obs)
        h = lines!(ax, xp_obs, yp_obs; linewidth=2, color=:blue)
        band!(ax, xp_obs, zeros(length(yp_obs)), yp_obs; color=:blue, alpha=0.12)
        push!(handles, h); push!(labels, "Posted Observed")
    end
    if !isempty(xp_fc)
        h = lines!(ax, xp_fc, yp_fc; linewidth=3, linestyle=:dot, color=:blue)
        push!(handles, h); push!(labels, "Posted Predicted")
    end
    if !isempty(xa_obs)
        h = scatter!(ax, xa_obs, ya_obs; marker=:circle, markersize=10, color=:orange)
        push!(handles, h); push!(labels, "Actual Observed")
    end
    if !isempty(xa_fc)
        h = lines!(ax, xa_fc, ya_fc; linewidth=3, linestyle=:dot, color=:orange)
        push!(handles, h); push!(labels, "Actual Predicted")
    end

    !isempty(handles) && axislegend(ax, handles, labels; position=:rt)

    # limits (before labels)
    x_all = vcat(xp_obs, xp_fc, xa_obs, xa_fc)
    y_all = vcat(yp_obs, yp_fc, ya_obs, ya_fc)
    if !isempty(x_all) && !isempty(y_all)
        xlims!(ax, 0, maximum(x_all))
        y_max = 1.1 * max(1.0, maximum(y_all))
        ylims!(ax, 0, y_max)

        # ---- label Actual Observed points: "25 @ 9:32 a.m." with rounded orange box ----
        if !isempty(xa_obs)
            dy = 0.02 * y_max  # vertical offset in data units
            positions = Point2f.(xa_obs, ya_obs .+ dy)
            labels = string.(round.(Int, ya_obs), " @ ", fmt_ampm.(xa_obs))

            textlabel!(ax, positions;
                text = labels,
                # box style
                background_color = :orange,
                cornerradius     = 6,
                padding          = 6,
                strokecolor      = :transparent,
                alpha            = 0.9,
                # text style
                text_color       = :black,
                fontsize         = LABEL_TEXTSIZE,
                # placement
                text_align       = (:left, :bottom),
                offset           = (4, 2),
                space            = :data
            )
        end
    end

    outdir  = ensure_outdir(code)
    outpath = joinpath(outdir, outfile)
    save(outpath, f)
    return outpath
end

# ---------------- Pick day & plot + S3 upload ----------------
plot_day, status = choose_plot_day(df_posted_obs, df_posted_fc;
                                   positive_only=POSITIVE_ONLY,
                                   min_points=MIN_POINTS)

if plot_day === nothing
    # Diagnostics: what days exist in each file?
    if !isempty(names(df_posted_obs))
        obs_days_all = unique(df_posted_obs[!, idcol(df_posted_obs)])
        @info "Observed posted days (min..max)" min_obs=(isempty(obs_days_all) ? missing : minimum(obs_days_all)) max_obs=(isempty(obs_days_all) ? missing : maximum(obs_days_all))
    end
    if !isempty(names(df_posted_fc))
        fc_days_all  = unique(df_posted_fc[!,  idcol(df_posted_fc)])
        @info "Forecast posted days (min..max)" min_fc=(isempty(fc_days_all) ? missing : minimum(fc_days_all)) max_fc=(isempty(fc_days_all) ? missing : maximum(fc_days_all))
    end
    @warn "No suitable day found in observed or forecast — skipping plot" CODE QTYPE

else
    filedate = Dates.format(plot_day, dateformat"yyyymmdd")

    if QTYPE == "priority"
        # ---- posted only (do NOT touch df_actual_*) ----
        obs_key_p = obscol(df_posted_obs)
        id_key_p  = idcol(df_posted_obs)
        id_key_pf = idcol(df_posted_fc)

        mask_pobs = (df_posted_obs[!, id_key_p] .== plot_day) .& .!ismissing.(df_posted_obs[!, obs_key_p])
        POSITIVE_ONLY && (mask_pobs .&= df_posted_obs[!, obs_key_p] .> 0)

        f_posted_obs = df_posted_obs[mask_pobs, :]
        f_posted_fc  = df_posted_fc[df_posted_fc[!, id_key_pf] .== plot_day, :]

        localpath = make_priority_plot(f_posted_obs, f_posted_fc;
            code=CODE, date_for_title=plot_day,
            outfile="plot_daily_curve_$(CODE)_priority.png")

        s3path = S3_BASE * basename(localpath)
        upload_file_to_s3(localpath, s3path) || @warn("Upload failed", localpath, s3path)

    elseif QTYPE == "standby"
        # ---- posted + actual ----
        obs_key_p = obscol(df_posted_obs); id_key_p = idcol(df_posted_obs)
        obs_key_a = obscol(df_actual_obs); id_key_a = idcol(df_actual_obs)
        id_key_pf = idcol(df_posted_fc);   id_key_af = idcol(df_actual_fc)

        mask_pobs = (df_posted_obs[!, id_key_p] .== plot_day) .& .!ismissing.(df_posted_obs[!, obs_key_p])
        mask_aobs = (df_actual_obs[!, id_key_a] .== plot_day) .& .!ismissing.(df_actual_obs[!, obs_key_a])
        if POSITIVE_ONLY
            mask_pobs .&= df_posted_obs[!, obs_key_p] .> 0
            mask_aobs .&= df_actual_obs[!, obs_key_a] .> 0
        end

        f_posted_obs = df_posted_obs[mask_pobs, :]
        f_actual_obs = df_actual_obs[mask_aobs, :]
        f_posted_fc  = df_posted_fc[df_posted_fc[!, id_key_pf] .== plot_day, :]
        f_actual_fc  = df_actual_fc[df_actual_fc[!, id_key_af] .== plot_day, :]

        localpath = make_standby_combined_plot(f_posted_obs, f_posted_fc, f_actual_obs, f_actual_fc;
            code=CODE, date_for_title=plot_day,
            outfile="plot_daily_curve_$(CODE)_standby.png")

        s3path = S3_BASE * basename(localpath)
        upload_file_to_s3(localpath, s3path) || @warn("Upload failed", localpath, s3path)
    else
        @warn "Unknown queue type" CODE QTYPE
    end

    if status != :ok
        @warn "Plotted most recent day without full obs+fc intersection" CODE QTYPE plot_day status
    end
end

