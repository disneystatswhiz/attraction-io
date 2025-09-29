# =============================================================================
# run_observed_assign_levels.jl
# Goal: Assign crowd levels to observed daily averages using precomputed thresholds
# Inputs:
#   - observed_dailyavgs.csv  (id_park_day_id, entity_code, avg_posted_11am_to_5pm)
#   - thresholds file         (per entity [+ optional cohort keys] with level breakpoints)
# Output:
#   - observed_crowdlevels.csv (id_park_day_id, entity_code, avg_posted_11am_to_5pm, crowd_level, crowd_label)
# =============================================================================

using Dates, DataFrames, CSV, Statistics

# --- constants & paths --------------------------------------------------------
CODE = uppercase(ATTRACTION.code)

AVG_PATH   = joinpath(LOC_WORK, CODE, "calendar", "observed_dailyavgs.csv")
THRESH_PATH = joinpath(LOC_WORK, CODE, "already_on_s3", "forecasts_$(CODE)_calendar.csv")   # adjust to your actual thresholds file
OUT_PATH   = joinpath(LOC_WORK, CODE, "calendar", "observed_crowdlevels.csv")

# --- idempotence / skip rules -------------------------------------------------
if isfile(OUT_PATH)
    # stop: already assigned
end

if ATTRACTION.queue_type == "priority"
    # stop: not applicable
end

if !isfile(AVG_PATH) || !isfile(THRESH_PATH)
    # stop: missing inputs
end

# --- load inputs --------------------------------------------------------------
avg_df = CSV.read(AVG_PATH, DataFrame)
thr_df = CSV.read(THRESH_PATH, DataFrame)

# Ensure expected columns exist
# avg_df must have: :id_park_day_id, :entity_code, :avg_posted_11am_to_5pm
# thr_df must have at minimum: :entity_code + ordered breakpoint columns
#   Option A (percentiles): :p10, :p20, ..., :p90  (in minutes)
#   Option B (levels): :lvl1_max, :lvl2_max, ... :lvl10_max (in minutes)
# Optionally, thresholds may include cohort keys (e.g., :season_year, :day_type, :bucket)

# --- (optional) derive a join key --------------------------------------------
# If thresholds are cohort-specific, compute the same cohort keys on avg_df to match thr_df
# e.g., avg_df.season_year = derive from id_park_day_id
#      avg_df.day_type     = join from a dim table earlier in pipeline
# Otherwise, entity-level thresholds only.

# --- join avgs with thresholds ------------------------------------------------
# Join on [:entity_code] and any cohort keys used by thr_df (e.g., [:season_year, :day_type])
joined = leftjoin(avg_df, thr_df, on=[:entity_code])   # add more keys if applicable

# --- assign crowd level per row ----------------------------------------------
# Define ordered breakpoints vector per row.
# Example for lvl*_max columns (inclusive upper bounds):
#   if avg <= lvl1_max → level = 1
#   elseif avg <= lvl2_max → level = 2
#   …
#   else → level = N (max band)

# PSEUDOCODE (row-wise):
# for each row in `joined`:
#     avg = row.avg_posted_11am_to_5pm
#     # build ordered list of breakpoints for this entity/cohort
#     bps = [row.lvl1_max, row.lvl2_max, ..., row.lvl10_max]   # or from :p10,:p20,... if your file uses percentiles
#     # find first index where avg ≤ breakpoint
#     idx = first index i where avg ≤ bps[i]
#     if idx exists:
#         level = idx
#     else:
#         level = length(bps)
#     row.crowd_level = level
#     # optional label
#     row.crowd_label = "CL$(level)"   # or map to text labels if you have them

# (If using percentile thresholds p10..p90 → map those to level bands in order.)

# --- finalize output ----------------------------------------------------------
# Keep only desired columns in a stable order
# out = select(joined, [:id_park_day_id, :entity_code, :avg_posted_11am_to_5pm, :crowd_level, :crowd_label])

# --- validations --------------------------------------------------------------
# - check for rows with missing thresholds (no matching entity/cohort) → decide: drop? default to max? log?
# - ensure crowd_level is Int and within expected range (e.g., 1–10)
# - option
