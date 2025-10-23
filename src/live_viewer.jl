# ============================================
# S3 → Local CSV Watcher (no size-only fallback)
# - exact timestamps + delete
# - last-resort direct cp of newest CSV
# - 1s polling
# ============================================

using Dates
using TimeZones

# ---------- EDIT THESE ----------
const S3PATH   = "s3://touringplans_stats/stats_work/attraction-io/reporting/accuracy_summaries/"
const LOCALDIR = raw"d:\GitHub\attraction-io\temp"
const TZ       = TimeZone("America/Toronto")
const POLL_SECS = 1
const DIRECTCP_EVERY = 90   # ~90s if POLL_SECS=1
# --------------------------------

# ---- Helpers ---------------------------------------------------------

function run_cmd(cmd::Cmd)
    out = IOBuffer(); err = IOBuffer()
    proc = run(pipeline(ignorestatus(cmd), stdout=out, stderr=err))
    return success(proc), String(take!(out)), String(take!(err))
end

build_cmd(args::Vector{String}) = Cmd(args)

function sync_from_s3_folder(s3path::String, localpath::String; flags::Vector{String}=String[])
    isdir(localpath) || mkpath(localpath)
    base = ["aws","s3","sync", s3path, localpath, "--only-show-errors"]
    cmd  = build_cmd(vcat(base, flags))
    ok, _out, err = run_cmd(cmd)
    if !ok && !isempty(strip(err))
        # @warn "aws s3 sync failed" cmd=join(cmd.exec," ") error=err
    end
    return ok
end

function latest_csv_mod(localpath::String, tz::TimeZone)
    files = filter(f -> endswith(lowercase(f), ".csv"), readdir(localpath; join=true))
    isempty(files) && return (nothing, nothing)
    best_file, best_time = nothing, ZonedDateTime(DateTime(0), tz)
    for f in files
        mt = ZonedDateTime(unix2datetime(stat(f).mtime), tz)
        if mt > best_time
            best_time = mt; best_file = f
        end
    end
    return best_file, best_time
end

function s3_latest_csv_key(s3path::String)
    cmd = build_cmd(["aws","s3","ls", s3path, "--recursive"])
    ok, out, err = run_cmd(cmd)
    if !ok
        # @warn "aws s3 ls failed" error=err
        return nothing
    end
    latest_dt = DateTime(0); latest_key = nothing
    for line in eachline(IOBuffer(out))
        occursin(".csv", lowercase(line)) || continue
        parts = split(strip(line))
        length(parts) < 4 && continue
        dt_str = string(parts[1]," ",parts[2])
        key    = join(parts[4:end]," ")
        try
            dt = DateTime(dt_str, dateformat"yyyy-mm-dd HH:MM:SS")
            if dt > latest_dt
                latest_dt = dt; latest_key = key
            end
        catch; end
    end
    return latest_key
end

function s3_cp_single(s3path::String, key::String, localdir::String)
    isdir(localdir) || mkpath(localdir)
    s3uri  = s3path * key
    target = joinpath(localdir, basename(key))
    cmd    = build_cmd(["aws","s3","cp", s3uri, target, "--only-show-errors"])
    ok, _out, err = run_cmd(cmd)
    if !ok
        # @warn "aws s3 cp failed" obj=s3uri error=err
    end
    return ok, target
end

# ---- Main loop -------------------------------------------------------

function main()
    isdir(LOCALDIR) || mkpath(LOCALDIR)
    println("Starting S3 watcher: $S3PATH → $LOCALDIR  (poll=$(POLL_SECS)s)")    
    last_file, last_time = latest_csv_mod(LOCALDIR, TZ)
    if !isnothing(last_file)
        println("Initial latest local: $(basename(last_file)) # @ $last_time")
    end

    stagnant = 0
    while true
        # 1) normal sync
        sync_from_s3_folder(S3PATH, LOCALDIR; flags=["--exact-timestamps","--delete"])

        file, mtime = latest_csv_mod(LOCALDIR, TZ)
        updated = false
        if isnothing(file)
            println("[$(now())] No CSVs in $LOCALDIR")
        elseif (file != last_file) || (isnothing(last_time) || mtime > last_time)
            println("[$(now())] New/updated: $(basename(file))  mtime=$mtime")
            last_file, last_time = file, mtime
            stagnant = 0
            updated = true
        end

        if !updated
            stagnant += 1
            if stagnant % DIRECTCP_EVERY == 0
                println("[$(now())] Still stagnant; fetching newest S3 key directly…")
                key = s3_latest_csv_key(S3PATH)
                if !isnothing(key)
                    ok, _ = s3_cp_single(S3PATH, key, LOCALDIR)
                    if ok
                        file2, mtime2 = latest_csv_mod(LOCALDIR, TZ)
                        if !isnothing(file2) && ((file2 != last_file) || (mtime2 > (last_time === nothing ? ZonedDateTime(DateTime(0), TZ) : last_time)))
                            println("[$(now())] Updated after direct cp: $(basename(file2))  mtime=$mtime2")
                            last_file, last_time = file2, mtime2
                            stagnant = 0
                        end
                    end
                else
                    println("[$(now())] Could not determine newest S3 key.")
                end
            end
        end

        sleep(POLL_SECS)
    end
end

main()
