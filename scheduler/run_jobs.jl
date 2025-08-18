# ==============================================================================
# Simple Startup Job Runner (per-group max concurrency = 5 + freshness gate)
# 1) Run main_setup.jl
# 2) Show current_* mtimes (via scheduler/print_current_mtimes.jl)
# 3) If group is fresh (≤ 12h), gather entities and run main_runner in parallel
# ==============================================================================

using CSV, DataFrames, Dates

if !@isdefined(ROOT)
    const ROOT = abspath(dirname(Base.active_project()))
end
if !@isdefined(MAX_PARALLEL_PER_GROUP)
    const MAX_PARALLEL_PER_GROUP = 3
end
if !@isdefined(FRESHNESS_WINDOW_HOURS)
    const FRESHNESS_WINDOW_HOURS = 12.0
end
if !@isdefined(PROPS)
    const PROPS = ["wdw","dlr","uor"]
end

# 1) Run main_setup once
include(joinpath(ROOT, "src", "main_setup.jl"))

# 2) Print current_* file mtimes and expose get_current_path()
include(joinpath(ROOT, "scheduler", "run_get_current_ts.jl"))

# ----- Freshness gate ----------------------------------------------------------
function is_fresh_group(prop::String, typ::String; window_hours::Real=FRESHNESS_WINDOW_HOURS)::Bool
    path = get_current_path(prop, typ)  # from run_get_current_ts.jl
    if !isfile(path)
        println("⏭️  $prop/$typ: missing ", path)
        return false
    end
    m = unix2datetime(stat(path).mtime)
    secs = Dates.datetime2unix(now(UTC)) - Dates.datetime2unix(m)
    age_h = secs / 3600
    if age_h ≤ window_hours
        println("✅ $prop/$typ is fresh (age=$(round(age_h; digits=2))h) → running")
        return true
    else
        println("⏭️  $prop/$typ is stale (age=$(round(age_h; digits=2))h > $(window_hours)h) → skipping")
        return false
    end
end

# ----- Park derivation ---------------------------------------------------------
function derive_park(entity::AbstractString, fallback::String)
    m = match(r"^[A-Za-z]{2}", entity)
    return m === nothing ? lowercase(fallback) : uppercase(m.match)
end

# ----- Entity loaders ----------------------------------------------------------
function get_standby_entities(prop)
    f = joinpath(ROOT, "input", "wait_times", prop, "current_wait.csv")
    if !isfile(f); return String[]; end
    df = CSV.read(f, DataFrame)
    return collect(String.(unique(skipmissing(df.entity_code))))
end

function get_priority_entities(prop)
    f = joinpath(ROOT, "input", "wait_times", "priority", prop, "current_fastpass.csv")
    if !isfile(f); return String[]; end
    df = CSV.read(f, DataFrame)
    return collect(String.(unique(skipmissing(df.FATTID))))
end

# ----- Run a group with a tiny worker pool (caps concurrency) -----------------
function run_group!(entities::Vector{String}, prop::String, typ::String; max_parallel::Int=MAX_PARALLEL_PER_GROUP)
    if isempty(entities)
        println("⏭️  No $prop/$typ entities found.")
        return
    end
    println("▶ Running $prop $typ entities (max $max_parallel at once): ", entities)

    ch = Channel{String}(length(entities))
    for e in entities
        put!(ch, e)
    end
    close(ch)

    @sync begin
        for _ in 1:max_parallel
            @async begin
                for e in ch
                    park = derive_park(e, prop)
                    run(`julia --project=$ROOT $(joinpath(ROOT,"src","main_runner.jl")) $e $park $prop $typ`)
                end
            end
        end
    end
end

# ----- Runner ------------------------------------------------------------------
function run_all()
    for prop in PROPS
        if is_fresh_group(prop, "standby")
            run_group!(get_standby_entities(prop), prop, "standby")
        end
        if is_fresh_group(prop, "priority")
            run_group!(get_priority_entities(prop), prop, "priority")
        end
    end
end

run_all()
println("🏁 Done.")
