# -------------------------------------------------------
# run_dimDate.jl
# Script to build and distribute the dimDate table
# -------------------------------------------------------

using JSON3, Dates, DataFrames, CSV
# --- begin code ---

function build_dimdate()::DataFrame
    if today() > Date(year(today()), 8, 31)
        end_date = Date(year(today()) + 2, 12, 31)
    else
        end_date = Date(year(today()) + 1, 12, 31)
    end

    df = DataFrame(park_day_id = collect(Date(2005, 1, 1):end_date))
    df.year = year.(df.park_day_id)
    df.month = month.(df.park_day_id)
    df.day = day.(df.park_day_id)
    df.day_of_week = dayofweek.(df.park_day_id)
    df.quarter = quarterofyear.(df.park_day_id)
    df.week_of_year = week.(df.park_day_id)
    df.day_of_year = dayofyear.(df.park_day_id)
    df.month_name = monthname.(df.park_day_id)
    df.month_mmm = monthabbr.(df.park_day_id)
    df.month_m = first.(df.month_mmm)
    df.day_of_week_name = dayname.(df.park_day_id)
    df.day_of_week_ddd = dayabbr.(df.park_day_id)
    df.day_of_week_d = first.(df.day_of_week_ddd)
    df.month_year_mmm_yyyy = string.(df.month_mmm, "-", df.year)
    df.quarter_year_q_yyyy = string.("Q", df.quarter, "-", df.year)
    df.year_yy = string.("'", lpad.(string.(df.year .% 100), 2, "0"))

    offsets = map(date -> begin
        today = Dates.today()
        (
            Dates.value(date - today),
            (year(date) - year(today)) * 12 + month(date) - month(today),
            (year(date) - year(today)) * 4 + ceil(Int, month(date)/3) - ceil(Int, month(today)/3),
            year(date) - year(today),
            date > today ? "Future" : "Past"
        )
    end, df.park_day_id)

    df.cur_day_offset      = getindex.(offsets, 1)
    df.cur_month_offset    = getindex.(offsets, 2)
    df.cur_quarter_offset  = getindex.(offsets, 3)
    df.cur_year_offset     = getindex.(offsets, 4)
    df.future_date         = getindex.(offsets, 5)

    current = now()
    df.ytd_flag = df.day_of_year .<= dayofyear(current)
    df.mtd_flag = df.day .<= day(current)
    df.output_file_label = uppercase.(string.(df.year, "_", lpad.(string.(df.month), 2, "0"), df.month_mmm))

    return df
end

using CSV, DataFrames, Dates, Logging

function save_dimdate()

    input_folder = LOC_DIM
    target_path = joinpath(input_folder, "dimdate.csv")

    if isfile(target_path)
        return
    end

    if !isdir(input_folder)
        mkpath(input_folder)
    end

    df = build_dimdate()  # <-- assumes this function is already defined
    CSV.write(target_path, df)

    # Send the dimDate dataset to S3
    upload_file_to_s3(target_path,"s3://touringplans_stats/stats_work/dimension_tables/dimdate.csv")

end

# Run it!
save_dimdate()
