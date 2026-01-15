import subprocess
import sys
from pathlib import Path

# SETUP:
# Calculates the project root: /Users/<NAME>/Developer/crypto-project/src/pipeline/
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Directory that points to: /Users/<NAME>/Developer/crypto-project/src/pipeline/
PIPELINE_DIR = BASE_DIR / "src" / "pipeline"

def run_step(step_name, script_path):
    # Runs a single Python script and check for errors
    print(f"--------- Starting {step_name} ---------")

    # Attempt to run the pipeline's automation, if it fails, it will show an error message
    try:
        # Run the script using the current Python interpreter
        result = subprocess.run(
            [sys.executable, str(script_path)], 
            check=True,  # Raises error if script fails
            capture_output=False # Print statements show in terminal
        )
        print(f"--------- {step_name} Completed ---------\n")

    except subprocess.CalledProcessError as called_process_error:
        print(f"--------- {step_name} Failed ---------\n")
        print(f"Error code: {called_process_error.returncode}")
        sys.exit(1) # Stops the entire pipeline

if __name__ == "__main__":
    print(f"Project Root: {BASE_DIR}")
    print("Initializing Data Pipeline.\n")

    # Runs the bronze layer code logic for extracting data
    run_step("Bronze Layer (Ingestion)", PIPELINE_DIR / "bronze" / "ingest.py")

    # Runs the silver layer code logic for cleaning data
    run_step("Silver Layer (Transformation)", PIPELINE_DIR / "silver" / "clean.py")

    # Runs the gold layer code logic for analyzing data
    run_step("Gold Layer (Analytics)", PIPELINE_DIR / "gold" / "analyze.py")

    print("Pipeline run successfully.")
