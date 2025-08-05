@echo off
title 🧩 Master Pipeline Launcher (Git Bash)

REM --- Full paths for Git Bash and Julia ---
set "GITBASH=C:\Program Files\Git\git-bash.exe"
set "WORKDIR=/d/GitHub/attraction-io"
set "JULIA=/c/Users/fred/AppData/Local/Programs/Julia-1.10.0/bin/julia.exe"

REM --- Launch Git Bash explicitly using start ---
start "" "%GITBASH%" -c "cd %WORKDIR% && %JULIA% scheduler/run_jobs_polling.jl"
