# =======================================================================================
# 🍂 run_dimSeason.jl — Assign Seasons based on DateGroupID
# =======================================================================================

function main()

    # --- Skip if already exists
    season_path = "work/dim/dimseason.csv"
    if isfile(season_path)
        return
    end

    # --- Load Inputs
    path_dategroupid = "work/dim/dimdategroupid.csv"
    df = CSV.read(path_dategroupid, DataFrame)

    # --- Prep
    df.date_group_id = uppercase.(df.date_group_id)
    df.season = fill("", nrow(df))

    # --- Priority override: CHRISTMAS PEAK (Dec 27 – Jan 1 inclusive)
    for (i, date) in enumerate(df.park_day_id)
        if (month(date) == 12 && day(date) ≥ 27) || (month(date) == 1 && day(date) ≤ 1)
            df.season[i] = "CHRISTMAS_PEAK"
        end
    end

    # --- Preserve index mapping for carry logic
    day_index = Dict(date => i for (i, date) in enumerate(df.park_day_id))
    date_list = df.park_day_id

    # --- High-priority: Specific holidays with carry logic
    holiday_patterns = [
        ("MARTIN_LUTHER|MLK", "MLK_JR_DAY", 3, 2),
        ("PRESIDENTS", "PRESIDENTS_DAY", 3, 2),
        ("MARDI_GRAS", "MARDI_GRAS", 3, 2),
        ("MEMORIAL", "MEMORIAL_DAY", 3, 2),
        ("LABOR", "LABOR_DAY", 3, 2),
        ("THANKSGIVING", "THANKSGIVING", 1, 1),
        ("CHRISTMAS|NEW_YEAR|BOXING", "CHRISTMAS", 1, 1),
        ("EASTER_MONDAY|EASTER_SATURDAY|EASTER_SUNDAY|GOOD_FRIDAY", "EASTER", 1, 1),
        ("JERSEY", "JERSEY_WEEK", 1, 1),
        ("HALLOWEEN", "HALLOWEEN", 1, 1),
        ("VETERANS", "VETERANS_DAY", 1, 1),
        ("COLUMBUS", "COLUMBUS_DAY", 1, 1),
        ("MARATHON", "MARATHON", 1, 1)
    ]

    for (pattern, season_label, carry_before, carry_after) in holiday_patterns
        regex = Regex(pattern)
        match_idx = findall(x -> occursin(regex, x), df.date_group_id)
        for idx in match_idx
            if df.season[idx] == ""
                df.season[idx] = season_label
            end
            for offset in 1:carry_before
                i = idx - offset
                if i ≥ 1 && df.season[i] == ""
                    df.season[i] = season_label
                end
            end
            for offset in 1:carry_after
                i = idx + offset
                if i ≤ nrow(df) && df.season[i] == ""
                    df.season[i] = season_label
                end
            end
        end
    end

    # --- Special Handling: Combined Presidents Day and Mardi Gras week
    combined_label = "PRESIDENTS_DAY_MARDI_GRAS"
    presidents_dates = Set(df.park_day_id[df.season .== "PRESIDENTS_DAY"])
    mardi_dates = Set(df.park_day_id[df.season .== "MARDI_GRAS"])

    for date in presidents_dates
        window = date - Day(3):date + Day(3)
        if any(in(mardi_dates), window)
            for d in window
                idx = findfirst(==(d), df.park_day_id)
                if !isnothing(idx)
                    df.season[idx] = combined_label
                end
            end
        end
    end

    # --- Lower-priority general seasonal patterns (only assign if still blank)
    seasonal_patterns = [
        ("AFTER_EASTER", "AFTER_EASTER"),
        ("BEFORE_EASTER", "BEFORE_EASTER"),
        ("MAY_WEEK", "SPRING"),
        ("JUN_WEEK|JUL_WEEK|INDEPENDENCE|AUG_WEEK", "SUMMER"),
        ("SEP_WEEK|OCT_WEEK|NOV_WEEK", "AUTUMN"),
        ("DEC_WEEK|JAN_WEEK|FEB_WEEK", "WINTER")
    ]

    for (pattern, season_label) in seasonal_patterns
        regex = Regex(pattern)
        df.season = ifelse.((df.season .== "") .& occursin.(regex, df.date_group_id), season_label, df.season)
    end

    # --- Add season_year
    df.season_year = [
        (season in ("CHRISTMAS", "CHRISTMAS_PEAK") && month(date) == 1 ?
            string(season, "_", year(date) - 1) :
            string(season, "_", year(date))
        ) for (season, date) in zip(df.season, df.park_day_id)
    ]

    # --- Finalize + Save
    output = select(df, [:park_day_id, :season, :season_year])
 
    # --- Distribute to input folders
    CSV.write(season_path, output)

end

main()
