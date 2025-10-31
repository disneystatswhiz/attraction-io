# ------------------------------------------------------------------------- #
# run_features.jl - Add needed columns and prepare data for modelling
# ------------------------------------------------------------------------- #

using Dates, CSV, DataFrames

function main()
    # Load key paths
    entity_code = ATTRACTION.code
    local_input_folder = joinpath(LOC_WORK, entity_code, "wait_times")

    # Construct input and output paths
    wait_time_filename = "future.csv"
    local_file_path = joinpath(local_input_folder, wait_time_filename)
    temp_file_path = joinpath(local_input_folder, "features.csv")

    # -------------------------------------------------------------------------------------
    # Skip feature generation if pre_model file already exists
    # -------------------------------------------------------------------------------------
    if isfile(temp_file_path)
        return
    end

    # -------------------------------------------------------------------------------------
    # Get the wait time data 
    # -------------------------------------------------------------------------------------
    if !isfile(local_file_path)
        return  # or `continue` if inside a loop
    end

    df = CSV.read(local_file_path, DataFrame)
    parsed = parse_zoneddatetimes_smart(df.observed_at; timezone = ATTRACTION.timezone)
    bad = ismissing.(parsed)
    if any(bad)
        # @info "features: dropping $(count(bad)) rows with unparsable observed_at"
    end
    df = df[.!bad, :]
    df.observed_at = convert(Vector{ZonedDateTime}, parsed[.!bad])

    # -------------------------------------------------------------------------------------
    # Add features
    # -------------------------------------------------------------------------------------
    df = add_mins_since_6am(df, :observed_at)
    df = add_park_hours(df, ATTRACTION)
    df = add_dategroupid(df)
    df = add_season(df)
    df = add_geometric_decay(df)

    # Reorder and rename columns
    df = select(df,
        :park_date        => :id_park_day_id,
        :entity_code        => :id_entity_code,
        :property_code      => :meta_property_code,
        :park_code          => :meta_park_code,
        :observed_at        => :meta_observed_at,
        :wait_time_type     => :meta_wait_time_type,
        :observed_wait_time => :target,
        :pred_dategroupid,
        :pred_season,
        :pred_season_year,
        :pred_mins_since_6am,
        :pred_mins_since_park_open,
        :pred_park_open_hour,
        :pred_park_close_hour,
        :pred_park_hours_open,
        :pred_emh_morning,
        :pred_emh_evening,
        :wgt_geo_decay
    )

    # Save the updated DataFrame to a temporary file
    CSV.write(temp_file_path, df)
end

main()
