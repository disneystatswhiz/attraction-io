module EncodeFeatures

using DataFrames, CategoricalArrays, MLJ, Dates

export encode_features, fit_encoders, transform_with_encoders, FeatureEncoders

# ------------------------------
# 🔧 Struct to hold encoders
# ------------------------------
mutable struct FeatureEncoders
    encoders::Dict{Symbol, Any}
end

# ------------------------------
# 🧠 Fit encoders for all pred_* columns
# ------------------------------
function fit_encoders(df::DataFrame)::FeatureEncoders
    encoders = Dict{Symbol, Any}()

    for col in names(df)
        col_sym = Symbol(col)
        if !startswith(string(col), "pred_")
            continue
        end

        col_data = df[!, col_sym]

        if eltype(col_data) <: Union{Missing, AbstractString} || eltype(col_data) <: CategoricalValue
            col_data = categorical(col_data)
            encoders[col_sym] = union(String.(levels(col_data)), ["__unknown__"])
        elseif eltype(col_data) <: Date || eltype(col_data) <: DateTime
            encoders[col_sym] = :datetime_numeric
        end
    end

    return FeatureEncoders(encoders)
end

# ------------------------------
# 🔁 Apply fitted encoders
# ------------------------------
function transform_with_encoders(df::DataFrame, enc::FeatureEncoders)
    df_new = copy(df)

    for (col, encoder) in enc.encoders
        if encoder isa Vector{String}
            # Add "__unknown__" fallback category if not already present
            levels_with_unknown = Set(encoder)
            push!(levels_with_unknown, "__unknown__")
            levels_vector = collect(levels_with_unknown)

            # Convert values to string, map unknowns, and apply encoder
            col_vals = string.(df[!, col])
            col_vals_mapped = [val in encoder ? val : "__unknown__" for val in col_vals]

            df_new[!, col] = levelcode.(categorical(col_vals_mapped; levels=levels_vector))
        elseif encoder == :datetime_numeric
            df_new[!, col] = Dates.value.(df[!, col])
        end
    end

    return df_new
end


# ------------------------------
# 🚀 Fit + Transform Convenience
# ------------------------------
function encode_features(df::DataFrame)
    enc = fit_encoders(df)
    df_encoded = transform_with_encoders(df, enc)
    return df_encoded
end

end # module
