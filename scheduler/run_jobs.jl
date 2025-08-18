# =====================================================================================
# Attraction-IO Scheduler (simple, local-first, freshness-gated)
# - Runs main_setup once (idempotent)
# - Periodically refreshes only current_* files from S3 -> local
# - Launches property/type groups when local current_* is NEW and FRESH (<= 12h)
# - Caps parallel entity launches per group
# =====================================================================================

using Dates, CSV, DataFrames, JSON3

# ----- Paths & includes --------------------------------------------------------------
const ROOT = abspath(dirname(Base.active_project()))   # project root (works when started with --project)
include(joinpath(ROOT, "src", "utilities", "utility_setup.jl"))
include(joinpath(ROOT, "src", "utilities", "s3utils.jl"))  # provides sync_from_s3_folder

# ----- Knobs -------------------------------------------------------------------------
const PROPERTIES           = ["wdw","dlr","uor"]
const ENABLE_UOR_PRIORITY  = false
const POLL_INTERVAL        = Minute(5)                  # how often to poll for arrivals
const MAX_ENTITY_PAR       = 3                          # per-group entity parallelism
const MAX_PROP_GROUPS      = 5                          # optional cap across property/type groups
const FRESH_WINDOW         = Hour(12)                   # require current_* to be <= 12h old
const MIN_FILE_AGE         = Minute(0)                  # set >0 if you want to avoid "file still being written"
const REGISTRY_PATH        = joinpath(ROOT, "temp", "scheduler_registry.json")

# ----- Local paths for current files --------------------------------------------------
get_local_current_path(prop::String, typ::String) =
    typ == "standby"  ? joinpath(LOC_INPUT, "wait_times", prop, "current_wait.csv") :
    typ == "priority" ? joinpath(LOC_INPUT, "wait_times", "priority", prop, "current_fastpass.csv") :
                        error("unknown type: $typ")

# ----- Tiny refresh sync (only current_*.csv) ----------------------------------------
function refresh_current!(prop::String, typ::String)::Bool
    if typ == "standby"
        s3  = "s3://touringplans_stats/export/wait_times/$prop/"
        loc = joinpath(LOC_INPUT, "wait_times", prop)
        mkpath(loc)
        return sync_from_s3_folder(s3, loc; exclude=["*"], include=["current_wait.csv"])
    else
        s3  = "s3://touringplans_stats/export/fastpass_times/$prop/"
        loc = joinpath(LOC_INPUT, "wait_times", "priority", prop)
        mkpath(loc)
        return sync_from_s3_folder(s3, loc; exclude=["*"], include=["current_fastpass.csv"])
    end
end

# ----- Registry (remember last local mtime we launched on) ---------------------------
load_registry() =
    isfile(REGISTRY_PATH) ?
        (try JSON3.read(read(REGISTRY_PATH, String)) catch; Dict{String,Any}() end) :
        Dict{String,Any}()

function save_registry(reg)
    isdir(dirname(REGISTRY_PATH)) || mkpath(dirname(REGISTRY_PATH))
    open(REGISTRY_PATH, "w") do io
        JSON3.write(io, reg; indent=2)
    end
end

last_mtime(reg, prop, typ) =
    haskey(reg, "$prop/$typ") ?
        (try DateTime(String(reg["$prop/$typ"]["local_mtime"])) catch; DateTime(0) end) :
        DateTime(0)

function mark_launched!(reg, prop, typ, m::DateTime)
    reg["$prop/$typ"] = Dict(
        "local_mtime" => string(m),
        "launched_at" => string(now(UTC)),
    )
end

# ----- Freshness helpers --------------------------------------------------------------
local_mtime(file::AbstractString)::DateTime =
    isfile(file) ? unix2datetime(stat(file).mtime) : DateTime(0)

"""
Return (fresh?, mtime, age).
fresh? means file age is within FRESH_WINDOW and >= MIN_FILE_AGE (both vs now(UTC)).
"""
function is_fresh(file::AbstractString; window::Period=FRESH_WINDOW, min_age::Period=MIN_FILE_AGE)
    if !isfile(file)
        return (false, DateTime(0), Second(0))
    end
    m = local_mtime(file)                # Naive DateTime; we treat it as UTC-based epoch
    age = now(UTC) - m
    fresh = (age ≥ min_age) && (age ≤ window)
    return (fresh, m, age)
end

# ----- Entity discovery from local current_* ------------------------------------------
const PARK_PREFIXES = Dict(
    "wdw" => Set(["AK","MK","EP","HS"]),
    "dlr" => Set(["DL","CA"]),
    "uor" => Set(["IA","EU","UF"])
)

filter_by_prop_prefix(ents::Vector{String}, prop::String) =
    [e for e in ents if length(e) ≥ 2 && uppercase(e[1:2]) in get(PARK_PREFIXES, prop, Set{String}())]

function get_standby_entities(prop::String)::Vector{String}
    f = get_local_current_path(prop, "standby"); @assert isfile(f) "Missing current_wait.csv for $prop"
    df = CSV.read(f, DataFrame)
    filter!(r -> !ismissing(r.submitted_posted_time) || !ismissing(r.submitted_actual_time), df)
    ents = unique(String.(strip.(String.(coalesce.(df.entity_code, "")))))
    ents = filter(!isempty, ents)
    ents = setdiff(ents, ["AK07"])  # keep your exception if needed
    return filter_by_prop_prefix(ents, prop)
end

function get_priority_entities(prop::String)::Vector{String}
    f = get_local_current_path(prop, "priority"); isfile(f) || return String[]
    df = CSV.read(f, DataFrame)
    norm = s -> uppercase(strip(replace(String(s), '\ufeff' => "")))
    col = findfirst(h -> norm(h) == "FATTID", names(df)); col === nothing && return String[]
    vals = String.(strip.(String.(coalesce.(df[!, col], ""))))
    ents = unique(filter(!isempty, vals))
    return filter_by_prop_prefix(ents, prop)
end

derive_park(entity::AbstractString, fallback::AbstractString) =
    (m = match(r"^[A-Za-z]{2}", entity); m === nothing ? lowercase(fallback) : lowercase(m.match))

# ----- Launch main_runner directly, with per-entity parallelism -----------------------
function launch_property_type(prop::String, typ::String; max_parallel::Int=MAX_ENTITY_PAR)
    entities = typ == "standby"  ? get_standby_entities(prop) :
               typ == "priority" ? get_priority_entities(prop) : String[]
    isempty(entities) && (@warn "No entities for $prop/$typ"; return)
    shuffle!(entities)
    active = Base.Process[]
    for e in entities
        park = derive_park(e, prop)
        cmd  = `julia --project=$ROOT $(joinpath(ROOT, "src", "main_runner.jl")) $e $park $prop $typ`
        p = run(pipeline(cmd; stdout=devnull, stderr=devnull); wait=false)
        push!(active, p)
        while length(active) ≥ max_parallel
            wait(active[1]); popfirst!(active)
            filter!(process_running, active)
        end
    end
    foreach(wait, active)
end

# ----- Boot setup (idempotent; will skip if already done today) -----------------------
run(`julia --project=$ROOT $(joinpath(ROOT, "src", "main_setup.jl"))`)

@info "🕒 scheduler running… ", Dates.format(now(), "yyyy-mm-dd HH:MM:SS")
reg = load_registry()
active_groups = Task[]   # track property/type group launches as Tasks

while true
    # prune finished group tasks
    filter!(t -> !istaskdone(t), active_groups)

    for prop in PROPERTIES
        for typ in ("standby", "priority")
            
            # respect UOR priority toggle
            if typ == "priority" && !(prop != "uor" || ENABLE_UOR_PRIORITY)
                continue
            end

            # optional global cap across groups
            if length(active_groups) >= MAX_PROP_GROUPS
                break
            end

            # 1) refresh just the current_* file so local cache sees new drops
            refresh_current!(prop, typ) || continue
            path = get_local_current_path(prop, typ)
            isfile(path) || continue

            # 2) require freshness (<= FRESH_WINDOW and >= MIN_FILE_AGE)
            fresh, mtime, age = is_fresh(path)
            fresh || continue

            # 3) launch only if newer than what we last launched on
            if mtime > last_mtime(reg, prop, typ)
                hrs = round(Dates.value(age) / 3600; digits=2)
                @info "🚀 launching $prop/$typ (entities)… [age=$(hrs)h]"
                push!(active_groups, @async launch_property_type(prop, typ))
                mark_launched!(reg, prop, typ, mtime); save_registry(reg)
            end
        end
    end

    sleep(Dates.value(POLL_INTERVAL) * 60)
end
