import os
import subprocess
import time
import shutil

def run_scripts_in_order():

    start_time = time.perf_counter()

    # Run report.py
    subprocess.run(["python", "src/fact_table/report.py"], check=True)
    # Run update.py
    subprocess.run(["python", "src/fact_table/update.py"], check=True)
    # Run latest.py
    subprocess.run(["python", "src/fact_table/latest.py"], check=True)

    # Delete the input and work directories to ensure clean slate
    if os.path.exists("input"):
        shutil.rmtree("input")
    if os.path.exists("work"):
        shutil.rmtree("work")

    end_time = time.perf_counter()
    elapsed_minutes = (end_time - start_time) / 60
    print(f"\n[OK] Updated wait_time_fact_table in {elapsed_minutes:.1f} minutes.\n")

if __name__ == "__main__":
    run_scripts_in_order()
