import os
import subprocess
import shutil
from dagster import op, job, schedule, get_dagster_logger, In, Out, ConfigurableResource

logger = get_dagster_logger()

# -------- Dagster ConfigurableResource Example --------
class PipelineConfig(ConfigurableResource):
    scraper_script: str
    loader_script: str
    dbt_project_dir: str
    yolo_script: str

# -------- OPS --------

@op(config_schema={"script_path": str})
def scrape_telegram_data(context):
    script_path = context.op_config["script_path"]
    logger.info(f"Starting Telegram scraping using: {script_path}")
    try:
        subprocess.run(["python", script_path], check=True, cwd=os.path.dirname(script_path))
        logger.info("✅ Telegram data scraping completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error during Telegram scraping: {e}")
        raise

@op(config_schema={"script_path": str})
def load_raw_to_postgres(context):
    script_path = context.op_config["script_path"]
    logger.info(f"Starting loading data to PostgreSQL using: {script_path}")
    try:
        subprocess.run(["python", script_path], check=True, cwd=os.path.dirname(script_path))
        logger.info("✅ Raw data loading completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error loading data to PostgreSQL: {e}")
        raise

@op(config_schema={"dbt_project_path": str})
def run_dbt_transformations(context):
    dbt_project_path = context.op_config["dbt_project_path"]
    if not shutil.which("dbt"):
        raise RuntimeError("❌ 'dbt' not found in PATH. Please install dbt.")
    logger.info(f"Starting dbt run in: {dbt_project_path}")
    try:
        subprocess.run(["dbt", "run"], check=True, cwd=dbt_project_path)
        logger.info("✅ dbt transformations completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error during dbt run: {e}")
        raise

@op(config_schema={"script_path": str})
def run_yolo_enrichment(context):
    script_path = context.op_config["script_path"]
    logger.info(f"Starting YOLO enrichment using: {script_path}")
    try:
        subprocess.run(["python", script_path], check=True, cwd=os.path.dirname(script_path))
        logger.info("✅ YOLO enrichment completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error during YOLO enrichment: {e}")
        raise

# -------- JOB --------

@job
def telegram_pipeline():
    scrape = scrape_telegram_data()
    load = load_raw_to_postgres()
    dbt = run_dbt_transformations()
    yolo = run_yolo_enrichment()
    # Simple linear pipeline
    load.alias("load_after_scrape").after(scrape)
    dbt.alias("dbt_after_load").after(load)
    yolo.alias("yolo_after_dbt").after(dbt)

# -------- SCHEDULE --------

@schedule(cron_schedule="0 2 * * *", job=telegram_pipeline, execution_timezone="Africa/Addis_Ababa")
def daily_telegram_schedule(_context):
    return {
        "ops": {
            "scrape_telegram_data": {"config": {"script_path": "/absolute/path/to/scraper.py"}},
            "load_raw_to_postgres": {"config": {"script_path": "/absolute/path/to/loader.py"}},
            "run_dbt_transformations": {"config": {"dbt_project_path": "/absolute/path/to/dbt_project"}},
            "run_yolo_enrichment": {"config": {"script_path": "/absolute/path/to/yolo_processor.py"}},
        }
    }