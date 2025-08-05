# ------------------------------------------------------------------------- #
# run_premodelling.jl - Split pre_model file by wait time type
# ------------------------------------------------------------------------- #

using Dates, CSV, DataFrames

function main()
    entity_code       = ATTRACTION.code
    wait_time_types   = ATTRACTION.queue_type == "priority" ? ["PRIORITY"] : ["POSTED", "ACTUAL"]

    # Load the feature-enriched DataFrame
    pre_model_path = joinpath(LOC_WORK, entity_code, "wait_times", "features.csv")

    if !isfile(pre_model_path)
        # No new data
        return
    end

    pre_model_df = CSV.read(pre_model_path, DataFrame)
    pre_model_df.meta_observed_at = parse_zoneddatetimes_simple(pre_model_df.meta_observed_at)

    # ---------------------------------------------------
    # Split into separate DataFrames by wait_time_type
    # ---------------------------------------------------
    wait_time_dfs = Dict{String, DataFrame}()
    for wait_time_type in wait_time_types
        wait_time_dfs[wait_time_type] = pre_model_df[pre_model_df.meta_wait_time_type .== wait_time_type, :]
    end

    # ---------------------------------------------------
    # Save and upload each DataFrame to S3
    # ---------------------------------------------------
    for (wt_type, df_new) in wait_time_dfs
        wt_lower = lowercase(wt_type)
        file_basename = "wait_times_$(entity_code)_$(wt_lower).csv"

        # Paths
        input_path  = joinpath(LOC_WORK, entity_code, "already_on_s3", file_basename)
        output_path = joinpath(LOC_OUTPUT, entity_code, file_basename)
        mkpath(dirname(output_path))

        # Skip if no new data to append and existing file already exists
        if nrow(df_new) == 0 && isfile(input_path)
            # @info "⚠️ No new $wt_type data for $entity_code — skipping write and upload."
            continue
        end

        # If existing input file found, append and dedup
        if isfile(input_path)
            df_existing = CSV.read(input_path, DataFrame)
            df_existing.meta_observed_at = parse_zoneddatetimes_simple(df_existing.meta_observed_at)

            df_combined = vcat(df_existing, df_new; cols = :union)
            df_final = unique(df_combined)
        else
            df_final = df_new
        end


        # ---------------------------------------------------
        # Reorder columns: id_* → meta_* → target → pred_* → wgt_* → others
        # ---------------------------------------------------
        all_cols = names(df_final)

        id_cols   = filter(col -> startswith(col, "id_"), all_cols)
        meta_cols = filter(col -> startswith(col, "meta_"), all_cols)
        target_col = filter(col -> startswith(col, "target"), all_cols)
        pred_cols = filter(col -> startswith(col, "pred_"), all_cols)
        wgt_cols  = filter(col -> startswith(col, "wgt_"), all_cols)

        # Everything else (not matching any of the above), sorted alphabetically
        known_cols = Set(vcat(id_cols, meta_cols, target_col, pred_cols, wgt_cols))
        other_cols = sort(filter(col -> !(col in known_cols), all_cols))

        # Apply the order
        ordered_cols = vcat(id_cols, meta_cols, target_col, pred_cols, wgt_cols, other_cols)
        df_final = df_final[:, ordered_cols]

        # Sort the rows by descending meta_observed_at column
        df_final = sort(df_final, :meta_observed_at, rev=true)

        # If target is a non-empty number, filter out extreme values (less than -100 or greater than 1000)
        if !isempty(target_col) && eltype(df_final[!, target_col[1]]) <: Number
            df_final = filter(row -> row.target >= -100 && row.target <= 1000, df_final)
        end

        # Save to output and upload
        CSV.write(output_path, df_final)
                
        # Upload to S3
        upload_file_to_s3(output_path,"s3://touringplans_stats/stats_work/attraction-io/wait_times/$file_basename")
    end
end
main()