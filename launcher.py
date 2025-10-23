#!/usr/bin/env python3
# launcher.py — runs the OS-appropriate pipeline script and normalizes flags/logs.

import os, sys, platform, shutil, subprocess, datetime, argparse

PROJECT_DIR = os.path.abspath(os.path.dirname(__file__))
LOGS_DIR = os.path.join(PROJECT_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

def ts():
    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def run(cmd, cwd=PROJECT_DIR):
    print(f">>> {cmd}")
    p = subprocess.Popen(cmd, cwd=cwd, shell=True)
    return p.wait()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-shutdown", action="store_true", help="Skip poweroff at exit (dev mode).")
    args, _ = ap.parse_known_args()

    os_name = platform.system().lower()
    logfile = os.path.join(LOGS_DIR, f"launcher_{ts()}_{os_name}.log")
    print(f"OS={os_name}\nLogs: {logfile}")

    if "windows" in os_name:
        script = os.path.join(PROJECT_DIR, "pipeline.ps1")
        if not os.path.exists(script):
            print("ERROR: pipeline.ps1 not found")
            return 2

        # Prefer PowerShell 7 (pwsh) if available, else fall back to legacy powershell
        ps7 = shutil.which("pwsh")
        ps51 = shutil.which("powershell")
        shell = ps7 or ps51
        if not shell:
            print("ERROR: PowerShell not found (expected pwsh or powershell in PATH)")
            return 2

        cmd = f'"{shell}" -NoProfile -ExecutionPolicy Bypass -File "{script}"'
        if args.no_shutdown:
            cmd += " -NoShutdown"

        print(f"Using shell: {shell}")
        return run(cmd)


    # POSIX (Linux, macOS, WSL, Git Bash)
    script = os.path.join(PROJECT_DIR, "pipeline.sh")
    if not os.path.exists(script):
        print("ERROR: pipeline.sh not found"); return 2
    bash = shutil.which("bash") or "/bin/bash"
    cmd = f'{bash} "{script}"'
    if args.no_shutdown: cmd += " --no-shutdown"
    return run(cmd)

if __name__ == "__main__":
    sys.exit(main())
