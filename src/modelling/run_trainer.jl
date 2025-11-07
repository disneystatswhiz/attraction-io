# -------------------------------------------------------------------- #
# run_trainer.jl - Train XGBoost model on encoded wait time data       #
# -------------------------------------------------------------------- #

using Dates, CSV, DataFrames, XGBoost
using Base.Threads
using LinearAlgebra
using FilePathsBase: mkpath          # CHANGED

# --- Threading (keep aligned with worker pool) ---
const XGB_THREADS = 1
BLAS.set_num_threads(1)

# ---------------------------------------------------------
# Train and optionally score using XGBoost
# ---------------------------------------------------------
function train_model(df::DataFrame, attraction::Attraction, wait_type::String)::Union{Booster, Nothing}

    entity_code = attraction.code
    temp_folder = joinpath(LOC_WORK, entity_code, "wait_times")
    mkpath(temp_folder)                              # CHANGED
    wait_type_lower = lowercase(wait_type)

    # Split into train and score
    df_train = filter(row -> !ismissing(row.target) && -100 ≤ row.target ≤ 7000, df)
    df_score = filter(row -> ismissing(row.target), df)   # CHANGED

    # @info "trainer" code=entity_code wt=wait_type n_train=nrow(df_train) n_score=nrow(df_score) nthread=XGB_THREADS  # optional

    if isempty(df_train)
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
        dtrain;                                 # ← semicolon means keywords follow
        num_round = 2000,
        eta = 0.1,
        max_depth = 6,
        subsample = 0.5,
        min_child_weight = 10,
        objective = "reg:absoluteerror",
        tree_method = use_gpu ? "gpu_hist" : "hist",
        nthread = XGB_THREADS,                  # ← was Threads.nthreads(); keep aligned to pool/env
        verbosity = 0,
        watchlist = ()
    )

    # Save feature importance
    importance = XGBoost.importance(booster)
    importance_df = DataFrame(feature = collect(keys(importance)),
                              importance = collect(values(importance)))
    imp_path = joinpath(temp_folder, "feature_importance_$(wait_type_lower).csv")
    CSV.write(imp_path, importance_df)

    # Score rows with missing target
    if !isempty(df_score)
        X_score = Matrix{Float32}(df_score[:, predictors])
        df_score.predicted_wait_time = predict(booster, DMatrix(X_score))
        select!(df_score, Not(:target))  # Drop target
        df_score.meta_wait_time_type = fill(wait_type, nrow(df_score))
        scored_path = joinpath(temp_folder, "scored_$(wait_type_lower).csv")
        CSV.write(scored_path, df_score)
    end

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
            continue
        end

        df = CSV.read(input_path, DataFrame)
        if "meta_wait_time_type" ∉ names(df)
            continue
        end

        df.meta_observed_at = parse_zoneddatetimes_simple(df.meta_observed_at)
        df = filter(row -> lowercase(row.meta_wait_time_type) == lowercase(wait_type), df)
        if isempty(df)
            continue
        end

        booster = train_model(df, attraction, wait_type)
        if booster !== nothing
            XGBoost.save(booster, model_path)
        end
    end
end

# ✅ Call main with ATTRACTION defined elsewhere
main(ATTRACTION)
