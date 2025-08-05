# =======================================================================================
# 🎯 run_dimDateGroupID.jl — Generate Date Group ID Table (WDW or DLR)
# =======================================================================================

# --- Main Function ---
function main()
    output_file = joinpath(LOC_DIM, "dimdategroupid.csv")
    if isfile(output_file)
        return
    end

    df_date = CSV.read(joinpath(LOC_DIM, "dimdate.csv"), DataFrame)
    df_holiday = CSV.read(joinpath(LOC_DIM, "dimholidays.csv"), DataFrame)

    select_date = select(df_date, [:park_day_id, :year, :month, :day, :day_of_week, :day_of_week_ddd, :week_of_year, :month_mmm])
    df = innerjoin(select_date, df_holiday, on = :park_day_id)

    df.date_group_id .= "...needs assigning..."
    df.is_easter_over .= 0

    for i in findall(df.holidaycode .== "GFR")
        y = year(df[i, :park_day_id])
        df[i:end, :is_easter_over] .= ifelse.(year.(df[i:end, :park_day_id]) .== y, 1, df[i:end, :is_easter_over])
    end

    df.easter_prefix .= ""
    is_mar_apr = (3 .<= month.(df.park_day_id) .<= 4)
    df.easter_prefix[is_mar_apr .& (df.is_easter_over .== 0)] .= "Before_Easter_"
    df.easter_prefix[is_mar_apr .& (df.is_easter_over .== 1)] .= "After_Easter_"

    df.week_of_month = floor.(Int, (dayofmonth.(df.park_day_id) .- 1) ./ 7) .+ 1
    df.week = "week" .* string.(df.week_of_month) .* "_"
    df.week[in.(df.week_of_month, Ref([4, 5]))] .= "week4or5_"

    df.date_group_id .= df.easter_prefix .* df.month_mmm .* "_" .* df.week .* df.day_of_week_ddd 

    direct_map = Dict(
        "ASH" => "Ash_Wednesday", "EST" => "Easter_Saturday", "ESS" => "Easter_Sunday",
        "ESM" => "Easter_Monday", "GFR" => "Good_Friday", "HAL" => "Halloween",
        "IND" => "Independence_Day", "LAB" => "Labor_Day", "MEM" => "Memorial_Day",
        "MLK" => "Martin_Luther_King_Day", "NYD" => "New_Years_Day", "NYE" => "New_Years_Eve",
        "PRS" => "Presidents_Day", "THK" => "Thanksgiving", "VET" => "Veterans_Day",
        "BOX" => "Boxing_Day", "COL" => "Columbus_Day", "CMD" => "Christmas_Day",
        "CME" => "Christmas_Eve", "MGR" => "Mardi_Gras", "PMP" => "Presidents_Day_With_Mardi_Gras",
        "PMM" => "Mardi_Gras_With_Presidents_Day"
    )

    for (code, label) in direct_map
        df.date_group_id[df.holidaycode .== code] .= label
    end

    jersey_idx = findall(df.holidaycode .== "NJC")
    df.date_group_id[jersey_idx] .= "Jersey_Week_" .* df.day_of_week_ddd[jersey_idx]

    # Assign dates between christmas and new years directly
    dec27_idx = findall((df.month .== 12) .& (df.day .== 27))
    df.date_group_id[dec27_idx] .= "Dec27"
    dec28_idx = findall((df.month .== 12) .& (df.day .== 28))
    df.date_group_id[dec28_idx] .= "Dec28"
    dec29_idx = findall((df.month .== 12) .& (df.day .== 29))
    df.date_group_id[dec29_idx] .= "Dec29"
    dec30_idx = findall((df.month .== 12) .& (df.day .== 30))
    df.date_group_id[dec30_idx] .= "Dec30"

    rename!(df, :date_group_id => Symbol("date_group_id"))

    # Ensure date_group_id is uppercase
    df.date_group_id .= uppercase.(df.date_group_id)

    output_df = sort(select(df, [:park_day_id, Symbol("date_group_id")]), :park_day_id)
    CSV.write(output_file, output_df)

end


main()
