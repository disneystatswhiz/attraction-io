# --------------------------------------------------------------------- #
# Adds a column to the DataFrame that calculates the number of minutes
# since 6 AM for each datetime in the specified column.
# If the time is before 6 AM, it wraps around to the previous day.
# Args:
# - df::DataFrame: The DataFrame containing the datetime column
# - datetime_col::Symbol: The column name containing datetime values
# Returns:
# - DataFrame with an additional column `pred_mins_since_6am`
# --------------------------------------------------------------------- #
function add_mins_since_6am(df::DataFrame, datetime_col::Symbol)::DataFrame
    hours   = Dates.hour.(df[!, datetime_col])
    minutes = Dates.minute.(df[!, datetime_col])
    offset  = (hours .- 6) .* 60 .+ minutes
    df.pred_mins_since_6am = ifelse.(offset .< 0, offset .+ 1440, offset)
    return df
end

# --------------------------------------------------------------------- #
# Adds park hours to the DataFrame by joining with a CSV file.
# The CSV file should contain columns: park_date, park_code, park_open, park_close.
# If the DataFrame already has park_open and park_close columns, they are dropped.
# Args:
# - df::DataFrame: The DataFrame to which park hours will be added
# - config::AbstractDict: Configuration dictionary containing paths
# Returns:
# - DataFrame with park hours added
# --------------------------------------------------------------------- #
function add_park_hours(df::DataFrame, attraction::Attraction)::DataFrame

    # Ensure the DataFrame has a park_date column
    if "park_date" ∉ names(df)
        df.park_date = get_park_day_id(df, :observed_at)
    end

    # Ensure the DataFrame has a park_code column
    if "park_code" ∉ names(df)
        df[!, :park_code] = fill(uppercase(attraction.park), nrow(df))
        df[!, :property_code] = fill(uppercase(attraction.property), nrow(df))
    end

    # Load park_hours file
    hours_path = joinpath(ROOT, "work", "_donor", "donorparkhours.csv")
    park_hours_df = CSV.read(hours_path, DataFrame)
    park_hours_df.opening_time = parse_zoneddatetimes_simple(park_hours_df.opening_time)
    park_hours_df.closing_time = parse_zoneddatetimes_simple(park_hours_df.closing_time)
    park_hours_df.opening_time_with_emh = parse_zoneddatetimes_simple(park_hours_df.opening_time_with_emh)
    park_hours_df.closing_time_with_emh_or_party = parse_zoneddatetimes_simple(park_hours_df.closing_time_with_emh_or_party)

    # Join park hours onto the main DataFrame
    df = leftjoin(df, park_hours_df, on = [:park_date, :park_code])

    # Compute minutes since park open, if both columns are present
    df.pred_mins_since_park_open = round.(Int, Dates.value.(df.observed_at .- df.opening_time) ./ 60_000)
    
    # Rename columns for clarity
    rename!(df, 
        :emh_morning => :pred_emh_morning, 
        :emh_evening => :pred_emh_evening,
        :opening_hour => :pred_park_open_hour,
        :closing_hour => :pred_park_close_hour,
        :hours_open => :pred_park_hours_open
    )

    # Drop unneeded park hours columns
    select!(df, Not([:opening_time, :opening_time_with_emh, :closing_time, :closing_time_with_emh_or_party, :is_valid_donor]))

    return df
end


# --------------------------------------------------------------------- #
# Adds a dategroupid column to the DataFrame by joining with a CSV file.
# The CSV file should contain columns: park_date, date_group_id.
# The dategroupid column is renamed to pred_dategroupid.
# Args:
# - df::DataFrame: The DataFrame to which dategroupid will be added
# - config::AbstractDict: Configuration dictionary containing paths
# Returns:
# - DataFrame with dategroupid added
# --------------------------------------------------------------------- #
function add_dategroupid(df::DataFrame)::DataFrame

    # Load dategroupid file
    dgid_path = joinpath(ROOT, "work", "_dim", "dimdategroupid.csv")
    dgid_df = CSV.read(dgid_path, DataFrame)

    # Add "pred_" prefix to the dategroupid column
    rename!(dgid_df, :date_group_id => :pred_dategroupid)
    
    # Join and return
    return leftjoin(df, dgid_df, on = [:park_date])
end


# --------------------------------------------------------------------- #
# Adds season column predictors to the DataFrame 
# by joining with a CSV file.
# The CSV file should contain columns: park_date, season, season_year.
# The season column is renamed to pred_season and pred_season_year.
# Args:
# - df::DataFrame: The DataFrame to which season will be added
# - config::AbstractDict: Configuration dictionary containing paths
# Returns:
# - DataFrame with season added
# --------------------------------------------------------------------- #
function add_season(df::DataFrame)::DataFrame

    # Load season file
    season_path = joinpath(ROOT, "work", "_dim", "dimseason.csv")
    season_df = CSV.read(season_path, DataFrame)

    # Add "pred_" prefix to the dategroupid column
    rename!(season_df, :season => :pred_season)
    rename!(season_df, :season_year => :pred_season_year)
    
    # Join and return
    return leftjoin(df, season_df, on = [:park_date])
end

# --------------------------------------------------------------------- #
# Adds a geometric decay column to the DataFrame.
# The geometric decay is calculated as 2^(1 - days since observed_at).
# More recent observations have higher weights.
# Args:
# - df::DataFrame: The DataFrame to which the geometric decay will be added
# Returns:
# - DataFrame with a new column `wgt_geo_decay` added
# Note: The DataFrame must contain an `observed_at` column with DateTime values.
# --------------------------------------------------------------------- #
function add_geometric_decay(df::DataFrame; half_life_days::Real = 730.0)::DataFrame
    
    # Convert ZonedDateTime to DateTime for subtraction
    observed_dt = DateTime.(df.observed_at)
    now_dt = Dates.now()

    # Compute age in days
    age_in_days = (now_dt .- observed_dt) ./ Dates.Day(1)

    # Apply geometric decay
    df.wgt_geo_decay = round.(0.5 .^ (age_in_days ./ half_life_days), digits=4)

    return df
end
