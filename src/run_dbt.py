import os
import logging
import subprocess

# Ensure logs folder exists
os.makedirs("../logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../logs/dbt_run.log"),
        logging.StreamHandler()
    ]
)

def run_dbt_command(command):
    """Runs a DBT command and logs the output."""
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logging.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running DBT command: {e.stderr}")

if __name__ == "__main__":
    logging.info("Starting DBT transformations...")
    run_dbt_command("dbt run")
    run_dbt_command("dbt test")
    run_dbt_command("dbt docs generate")
