#Requires -Version 7
Param(
  [switch]$NoShutdown
)

$ErrorActionPreference = "Stop"
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectDir
New-Item -ItemType Directory -Path "$ProjectDir\logs","$ProjectDir\input","$ProjectDir\output","$ProjectDir\temp","$ProjectDir\work" -Force | Out-Null

# Lock (simple PID file)
$LockFile = Join-Path $ProjectDir ".pipeline.lock"
if (Test-Path $LockFile) {
  try {
    $pidExisting = Get-Content $LockFile | ForEach-Object {[int]$_}
    if (Get-Process -Id $pidExisting -ErrorAction SilentlyContinue) {
      Write-Host "Another run is active (pid=$pidExisting). Exiting."
      exit 0
    }
  } catch {}
}
$PID | Out-File $LockFile -Encoding ascii -Force

# Logging
$ts = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$hostShort = $env:COMPUTERNAME
$Log = Join-Path $ProjectDir "logs\pipeline_${ts}_${hostShort}.log"
Start-Transcript -Path $Log -Append | Out-Null

function Finish {
  param([int]$rc)
  try { Stop-Transcript | Out-Null } catch {}
  if (Test-Path $LockFile) { Remove-Item $LockFile -Force -ErrorAction SilentlyContinue }
  if (-not $NoShutdown) {
    Write-Host "Windows: not powering off by default (safety). Use OS tools if desired."
  } else {
    Write-Host "NoShutdown flag set; not powering off."
  }
  exit $rc
}
$rcGlobal = 1
try {
  Write-Host "==== PIPELINE BOOT CONTEXT ===="
  Write-Host (Get-Date -Format o)
  Write-Host "User: $env:USERNAME"
  Write-Host "PWD:  $((Get-Location).Path)"
  Write-Host "==============================="

  # Git (optional on Windows)
  if (Get-Command git -ErrorAction SilentlyContinue) {
    git config --global --add safe.directory "$ProjectDir" | Out-Null
    if ($env:GIT_TAG) {
      git fetch --all --tags --prune
      git checkout --detach "tags/$env:GIT_TAG"
    } else {
      $branch = $env:GIT_BRANCH; if (-not $branch) { $branch = "fact_table_sync" }
      git fetch --all --prune
      git checkout -B $branch "origin/$branch"
      git reset --hard "origin/$branch"
      git pull --ff-only origin $branch
    }
    git log -1 --pretty=format:"git msg: %s`n" | Write-Host
  } else {
    Write-Host "git not found; skipping update."
  }

  # Python venv (optional on Windows for your Step 0)
  if (Get-Command python -ErrorAction SilentlyContinue) {
    $Venv = Join-Path $ProjectDir "venv"
    if (-not (Test-Path $Venv)) { python -m venv $Venv }
    $act = Join-Path $Venv "Scripts\Activate.ps1"
    if (Test-Path $act) { . $act }
    python -m pip install --upgrade pip
    if (Test-Path "$ProjectDir\requirements.txt") {
      python -m pip install -r "$ProjectDir\requirements.txt"
    } else {
      python -m pip install boto3 pandas pyarrow s3fs
    }
  } else {
    Write-Host "Python not found; continue without it."
  }

  # Julia optional on Windows — if your job needs it:
  if (Get-Command julia -ErrorAction SilentlyContinue) {
    Write-Host "julia: $(Get-Command julia).Source"
    & julia --version
    & julia --project="$ProjectDir" -e 'using Pkg; Pkg.instantiate(); Pkg.precompile();'
    $jobScript = "$ProjectDir\scheduler\run_jobs.jl"
    if (-not (Test-Path $jobScript)) { throw "Job script not found: $jobScript" }
    & julia --project="$ProjectDir" "$jobScript"
    $rcGlobal = $LASTEXITCODE
  } else {
    Write-Host "Julia not found; nothing to run. (Install Julia or point to a Windows-native job.)"
    $rcGlobal = 1
  }

} catch {
  Write-Host "ERROR: $($_.Exception.Message)"
  $rcGlobal = 1
} finally {
  Finish -rc $rcGlobal
}
