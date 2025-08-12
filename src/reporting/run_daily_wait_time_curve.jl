using CSV, DataFrames, Dates, TimeZones, PyPlot, Statistics

CODE = ATTRACTION.code
yesterday_id = today() - Day(1)

# STEP A load and clean data

# Get paths
path_for_observed_posted = joinpath(LOC_WORK, CODE, "already_on_s3", "wait_times_$(CODE)_posted.csv")
path_for_observed_actual = joinpath(LOC_WORK, CODE, "already_on_s3", "wait_times_$(CODE)_actual.csv")
path_for_forecast_posted = joinpath(LOC_WORK, CODE, "already_on_s3", "forecasts_$(CODE)_posted.csv")
path_for_forecast_actual = joinpath(LOC_WORK, CODE, "already_on_s3", "forecasts_$(CODE)_actual.csv")

# Load data
df_posted_obs = CSV.read(path_for_observed_posted, DataFrame)
df_actual_obs = CSV.read(path_for_observed_actual, DataFrame)
df_forecast_posted = CSV.read(path_for_forecast_posted, DataFrame)
df_forecast_actual = CSV.read(path_for_forecast_actual, DataFrame)

# Filter for id_park_day_id is yesterday
df_posted_obs_y = filter(row -> row.id_park_day_id == yesterday_id, df_posted_obs)
df_actual_obs_y = filter(row -> row.id_park_day_id == yesterday_id, df_actual_obs)
df_forecast_posted_y = filter(row -> row.id_park_day_id == yesterday_id, df_forecast_posted)
df_forecast_actual_y = filter(row -> row.id_park_day_id == yesterday_id, df_forecast_actual)

# For the two observed files, remove any rows where target is missing
df_posted_obs_y = filter(row -> !ismissing(row.target), df_posted_obs_y)
df_actual_obs_y = filter(row -> !ismissing(row.target), df_actual_obs_y)

# Get yesterday's pred_dategroupid for each df (if present)
pred_group_posted_obs = unique(df_posted_obs_y.pred_dategroupid)
pred_group_actual_obs = unique(df_actual_obs_y.pred_dategroupid)
pred_group_forecast_posted = unique(df_forecast_posted_y.pred_dategroupid)
pred_group_forecast_actual = unique(df_forecast_actual_y.pred_dategroupid)

# Helper to filter by pred_dategroupid (excluding yesterday)
function filter_by_pred_group(df, pred_groups)
    filter(row -> (row.pred_dategroupid in pred_groups) && (row.id_park_day_id != yesterday_id), df)
end

# Keep all rows for days with the same pred_dategroupid as yesterday (excluding yesterday itself)
df_posted_obs_similar = filter_by_pred_group(df_posted_obs, pred_group_posted_obs)
df_actual_obs_similar = filter_by_pred_group(df_actual_obs, pred_group_actual_obs)
df_forecast_posted_similar = filter_by_pred_group(df_forecast_posted, pred_group_forecast_posted)
df_forecast_actual_similar = filter_by_pred_group(df_forecast_actual, pred_group_forecast_actual)

# Combine all eight dataframes into a single dataframe
df_all = vcat(df_posted_obs_similar, df_actual_obs_similar, df_forecast_posted_similar, df_forecast_actual_similar, df_posted_obs_y, df_actual_obs_y, df_forecast_posted_y, df_forecast_actual_y)


"""
main():
  cfg = load_config_or_defaults()
  log("Starting daily curve plot for", cfg.ATTRACTION.code)

  # STEP A: load + clean data
  df_posted_obs = load_observed_csv(path_for("posted"))
  df_posted_fc  = load_forecast_csv(path_for("posted"))
  df_actual_obs = load_observed_csv(path_for("actual"))
  df_actual_fc  = load_forecast_csv(path_for("actual"))

  # STEP B: join obs + fc by observed_at_r15, per type
  df_posted = inner_join_on_r15(df_posted_obs, df_posted_fc)
  df_actual = inner_join_on_r15(df_actual_obs, df_actual_fc)

  # STEP C: pick yesterday (with timezone) and filter
  yd = get_yesterday_date(cfg.default_tz)
  df_posted_y = filter_to_date(df_posted, yd)
  df_actual_y = filter_to_date(df_actual, yd)

  guard_nonempty(df_posted_y, "No POSTED rows for yesterday")
  # ACTUAL can be sparse; allow empty but warn.

  # STEP D: determine yesterday's pred_date_group_id
  yd_group = mode_or_first_nonmissing(df_posted_y.pred_date_group_id)
  guard_present(yd_group, "Missing pred_date_group_id for yesterday")

  # STEP E: fetch historical POSTED curves with same yd_group (excluding yesterday)
  df_posted_hist = filter_similar_days(df_posted, yd_group, before_date=yd)
  curves_hist = split_into_daily_curves(df_posted_hist; limit=cfg.max_hist_days)

  # STEP F: assemble plotting series
  series = build_plot_series(
             posted_y=df_posted_y,
             actual_y=df_actual_y,
             hist_curves=curves_hist)

  # STEP G: plot + save
  fig = plot_daily_curve(series; title=title_from(cfg.ATTRACTION.code, yd))
  outpath = output_path(cfg.ATTRACTION.code, yd)
  save_png(fig, outpath)

  log("Saved:", outpath)
"""