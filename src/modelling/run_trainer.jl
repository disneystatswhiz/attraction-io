# -------------------------------------------------------------------- #
# run_trainer.jl - Train XGBoost model on encoded wait time data       #
# -------------------------------------------------------------------- #

using Dates, CSV, DataFrames, XGBoost

# ---------------------------------------------------------
# Train and optionally score using XGBoost
# ---------------------------------------------------------
function train_model(df::DataFrame, attraction::Attraction, wait_type::String)::Union{Booster, Nothing}

    entity_code = attraction.code
    temp_folder = joinpath(LOC_WORK, entity_code, "wait_times")
    wait_type_lower = lowercase(wait_type)

    # Split into train and score
    df_train = filter(row -> !ismissing(row.target) && -100 ≤ row.target ≤ 7000, df)
    df_score = filter(row -> ismissing(row.target) && row.meta_observed_at > ZonedDateTime(now(), tz"UTC"), df)

    if isempty(df_train)
        # @error("❌ No training data for $entity_code ($wait_type)")
        return nothing
    end

    predictors = filter(col -> startswith(col, "pred_"), names(df))
    weights    = filter(col -> startswith(col, "wgt_"), names(df))
    w          = isempty(weights) ? nothing : Vector{Float32}(df_train[:, weights[1]])

    X = Matrix{Float32}(df_train[:, predictors])
    y = Vector{Float32}(df_train.target)

    dtrain = isnothing(w) ? DMatrix(X, label=y) : DMatrix(X, label=y, weight=w)

    # GPU setting (optional — assume false unless overridden in future)
    use_gpu = false
    booster = xgboost(
        dtrain;
        num_round = 2000,
        eta = 0.1,
        max_depth = 6,
        subsample = 0.5,
        min_child_weight = 10,
        objective = "reg:absoluteerror",
        tree_method = use_gpu ? "gpu_hist" : "hist",
        verbosity = 0,
        watchlist = ()
    )

    # # @info("📊 Trained model on $(nrow(df_train)) rows | GPU: $use_gpu | $(length(predictors)) predictors")

    # Save feature importance
    importance_df = DataFrame(
        feature = keys(XGBoost.importance(booster)),
        importance = values(XGBoost.importance(booster))
    )
    imp_path = joinpath(temp_folder, "feature_importance_$(wait_type_lower).csv")
    CSV.write(imp_path, importance_df)
    # # @info("📈 Feature importance saved to $imp_path")

    # Score future rows
    if !isempty(df_score)
        X_score = Matrix{Float32}(df_score[:, predictors])
        df_score.predicted_wait_time = predict(booster, DMatrix(X_score))
        select!(df_score, Not(:target))  # Drop target
        df_score.meta_wait_time_type = fill(wait_type, nrow(df_score))  # ✅ ADD THIS
        scored_path = joinpath(temp_folder, "scored_$(wait_type_lower).csv")
        CSV.write(scored_path, df_score)
        # # @info("🔮 Predictions saved to $scored_path")
    else
        # @warn("⚠️ No scoring rows for $entity_code ($wait_type)")
    end

    # # @info("✅ Training complete for $entity_code ($wait_type)")
    return booster
end

# ---------------------------------------------------------
# Main function - loop through wait types and train
# ---------------------------------------------------------
function main(attraction::Attraction)
    entity_code = attraction.code
    wait_time_types = attraction.queue_type == "priority" ? ["PRIORITY"] : ["POSTED", "ACTUAL"]
    temp_folder = joinpath(LOC_WORK, entity_code, "wait_times")

    for wait_type in wait_time_types
        wt_lower = lowercase(wait_type)
        input_path = joinpath(temp_folder, "to_be_modelled_$(wt_lower).csv")
        model_path = joinpath(temp_folder, "model_$(wt_lower).bst")

        if !isfile(input_path)
            # @warn("⚠️ Skipping $wait_type — input file not found: $input_path")
            continue
        end

        df = CSV.read(input_path, DataFrame)

        if "meta_wait_time_type" ∉ names(df)
            # @warn("⚠️ Skipping $wait_type — no 'meta_wait_time_type' column in input file.")
            continue
        end

        df.meta_observed_at = parse_zoneddatetimes_simple(df.meta_observed_at)
        df = filter(row -> lowercase(row.meta_wait_time_type) == lowercase(wait_type), df)

        if isempty(df)
            # @warn("⚠️ Skipping $wait_type — no matching rows for 'meta_wait_time_type == $wait_type'")
            continue
        end

        booster = train_model(df, attraction, wait_type)
        if booster !== nothing
            XGBoost.save(booster, model_path)
            # @info("💾 Model saved to $model_path")
        end
    end
end


# ✅ Call main with ATTRACTION defined elsewhere
main(ATTRACTION)
