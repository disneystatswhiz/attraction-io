# -------------------------------------------------------
# run_dimHolidays.jl - Build and save holiday code table
# -------------------------------------------------------

# ------------------------------------------------------------------
# Easter Calculation Helper
function easter_date(year)
    a = year % 19
    b = year ÷ 100
    c = year % 100
    d = b ÷ 4
    e = b % 4
    f = (b + 8) ÷ 25
    g = (b - f + 1) ÷ 3
    h = (19 * a + b - d - g + 15) % 30
    i = c ÷ 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) ÷ 451
    month = (h + l - 7 * m + 114) ÷ 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return Date(year, month, day)
end

# ------------------------------------------------------------------
# Main Processing
function main()
    holidays_path = joinpath(LOC_DIM, "dimholidays.csv")
    if isfile(holidays_path)
        return
    end

    df = CSV.read(joinpath(LOC_DIM, "dimdate.csv"), DataFrame)
    sort!(df, :park_day_id)

    df[!, :holidaycode] .= "NONE"
    df[!, :holidayname] .= "None"

    easter_dates = easter_date.(df.year)

    df[(df.month .== 1) .& (df.day .== 1), :holidaycode] .= "NYD"
    df[(df.month .== 1) .& (df.day_of_week_name .== "Monday") .& (15 .<= df.day .<= 21), :holidaycode] .= "MLK"
    df[(df.month .== 2) .& (df.day_of_week_name .== "Monday") .& (15 .<= df.day .<= 21), :holidaycode] .= "PRS"
    df[df.park_day_id .== (easter_dates .- Day(47)), :holidaycode] .= "MGR"
    df[df.park_day_id .== (easter_dates .- Day(46)), :holidaycode] .= "ASH"
    df[df.park_day_id .== (easter_dates .- Day(2)), :holidaycode] .= "GFR"
    df[df.park_day_id .== (easter_dates .- Day(1)), :holidaycode] .= "EST"
    df[df.park_day_id .== easter_dates, :holidaycode] .= "ESS"
    df[df.park_day_id .== (easter_dates .+ Day(1)), :holidaycode] .= "ESM"
    df[(df.month .== 5) .& (df.day_of_week_name .== "Monday") .& (25 .<= df.day .<= 31), :holidaycode] .= "MEM"
    df[(df.month .== 7) .& (df.day .== 4), :holidaycode] .= "IND"
    df[(df.month .== 9) .& (df.day_of_week_name .== "Monday") .& (1 .<= df.day .<= 7), :holidaycode] .= "LAB"
    df[(df.month .== 10) .& (df.day_of_week_name .== "Monday") .& (8 .<= df.day .<= 14), :holidaycode] .= "COL"
    df[(df.month .== 10) .& (df.day .== 31), :holidaycode] .= "HAL"
    df[(df.month .== 11) .& (df.day_of_week_name .∈ Ref(["Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"])) .& (2 .<= df.day .<= 8), :holidaycode] .= "NJC"
    df[(df.month .== 11) .& (df.day .== 11), :holidaycode] .= "VET"
    df[(df.month .== 11) .& (df.day_of_week_name .== "Thursday") .& (22 .<= df.day .<= 28), :holidaycode] .= "THK"
    df[(df.month .== 12) .& (df.day .== 24), :holidaycode] .= "CME"
    df[(df.month .== 12) .& (df.day .== 25), :holidaycode] .= "CMD"
    df[(df.month .== 12) .& (df.day .== 26), :holidaycode] .= "BOX"
    df[(df.month .== 12) .& (df.day .== 31), :holidaycode] .= "NYE"

    indices_prs = findall(==("PRS"), df.holidaycode)
    indices_mgr = findall(==("MGR"), df.holidaycode)
    both = intersect(indices_prs .+ 1, indices_mgr)
    df[both .- 1, :holidaycode] .= "PMP"
    df[both, :holidaycode] .= "PMM"

    name_map = Dict(
        "NYD" => "New Year's Day", "MLK" => "Martin Luther King Jr. Day", "PRS" => "Presidents' Day",
        "MGR" => "Mardi Gras", "PMP" => "Presidents' Day / Mardi Gras (Presidents' Day)",
        "PMM" => "Presidents' Day / Mardi Gras (Mardi Gras)", "ASH" => "Ash Wednesday",
        "GFR" => "Good Friday", "EST" => "Easter Saturday", "ESS" => "Easter Sunday",
        "ESM" => "Easter Monday", "MEM" => "Memorial Day", "IND" => "Independence Day",
        "LAB" => "Labor Day", "COL" => "Columbus Day", "HAL" => "Halloween",
        "NJC" => "Jersey Week", "VET" => "Veterans Day", "THK" => "Thanksgiving",
        "CME" => "Christmas Eve", "CMD" => "Christmas Day", "BOX" => "Boxing Day",
        "NYE" => "New Year's Eve"
    )
    for (code, name) in name_map
        df[df.holidaycode .== code, :holidayname] .= name
    end

    out = select(df, [:park_day_id, :holidaycode, :holidayname])
    CSV.write(holidays_path, out)

    # --- Upload to S3 ---
    upload_file_to_s3(holidays_path, "s3://touringplans_stats/stats_work/dimension_tables/dimholidays.csv")

end

main()

