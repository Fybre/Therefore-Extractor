import argparse
import logging
import time
import datetime
import os
import shutil

import therefore_document_gatherer
import therefore_document_processor

# ----------------------------
# Logging configuration
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ----------------------------
# Helpers
# ----------------------------
def parse_interval(interval_str):
    """Convert interval string like '3h', '30m', '20s' to seconds."""
    if not interval_str:
        return None
    s = interval_str.strip().lower()
    try:
        if s.endswith("h"):
            return int(float(s[:-1]) * 3600)
        elif s.endswith("m"):
            return int(float(s[:-1]) * 60)
        elif s.endswith("s"):
            return int(float(s[:-1]))
        else:
            return int(s)  # assume plain seconds
    except ValueError:
        logging.error(f"Invalid interval format: {interval_str}")
        return None

def ensure_directories(config_path, db_dir, vectordb_dir, output_dir="output"):
    """Ensure required directories exist for config, dbs, vectordb and output."""
    dirs_to_create = [
        os.path.dirname(config_path),
        db_dir,
        vectordb_dir,
        output_dir
    ]
    for d in dirs_to_create:
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
            logging.info(f"Created missing directory: {d}")

def clear_output_dir(output_dir="output"):
    """Delete all contents of the output directory, but keep the folder."""
    if os.path.exists(output_dir):
        for filename in os.listdir(output_dir):
            file_path = os.path.join(output_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # remove file or symlink
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # remove subdir
            except Exception as e:
                logging.error(f"Failed to delete {file_path}: {e}")
        logging.info(f"Cleared output directory: {output_dir}")
    else:
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Created output directory: {output_dir}")

# ----------------------------
# Pipeline runner
# ----------------------------
def run_pipeline(config_path, tenant_name, db_dir, vectordb_dir):
    """Run a single gather + process cycle."""
    # Clear output folder before each run
    clear_output_dir("output")

    pipeline_start = time.time()
    logging.info("=== Starting pipeline run ===")

    # ------ Gather Phase ------
    logging.info("--- Starting document gather phase ---")
    gather_start = time.time()
    therefore_document_gatherer.get_therefore_documents_for_processing(
        config_path=config_path,
        tenant_name=tenant_name,
        db_dir=db_dir
    )
    gather_elapsed = time.time() - gather_start
    logging.info(f"Gather phase complete in {str(datetime.timedelta(seconds=int(gather_elapsed)))}")

    # ------ Process Phase ------
    logging.info("--- Starting document process phase ---")
    process_start = time.time()
    therefore_document_processor.process(
        config_path=config_path,
        tenant_name=tenant_name,
        db_dir=db_dir,
        vectordb_dir=vectordb_dir
    )
    process_elapsed = time.time() - process_start
    logging.info(f"Process phase complete in {str(datetime.timedelta(seconds=int(process_elapsed)))}")

    # ------ Totals ------
    pipeline_elapsed = time.time() - pipeline_start
    logging.info(
        f"=== Pipeline run finished in {str(datetime.timedelta(seconds=int(pipeline_elapsed)))} "
        f"(Gather: {str(datetime.timedelta(seconds=int(gather_elapsed)))}, "
        f"Process: {str(datetime.timedelta(seconds=int(process_elapsed)))}) ==="
    )

# ----------------------------
# Main entry point
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Therefore document gather + process pipeline.")
    parser.add_argument("--tenant", dest="tenant_name", default=None, help="Tenant name to process (or env TENANT_ARG)")
    parser.add_argument("--config", dest="config_path", default=None, help="Path to config file (or env CONFIG_PATH)")
    parser.add_argument("--db-dir", dest="db_dir", default=None, help="Directory for tenant SQLite DBs (or env DB_DIR_ARG)")
    parser.add_argument("--vectordb-dir", dest="vectordb_dir", default=None, help="Directory for Chroma DBs (or env VECTORDB_DIR_ARG)")
    parser.add_argument("--interval", dest="interval", type=str, default=None,
                        help="Run interval, e.g. '3h', '30m', '10s' (or env INTERVAL). If omitted, run once and exit.")
    args = parser.parse_args()

    # Resolve args from env vars if not provided via CLI
    tenant_name = args.tenant_name or os.environ.get("TENANT_ARG")
    config_path = args.config_path or os.environ.get("CONFIG_PATH", "config/config.json")
    db_dir = args.db_dir or os.environ.get("DB_DIR_ARG", "db/docs")
    vectordb_dir = args.vectordb_dir or os.environ.get("VECTORDB_DIR_ARG", "db/vectordb")
    interval_str = args.interval or os.environ.get("INTERVAL")

    # Ensure all directories exist
    ensure_directories(config_path, db_dir, vectordb_dir, output_dir="output")

    # Parse interval and run
    interval_seconds = parse_interval(interval_str)

    if interval_seconds is None:
        run_pipeline(config_path, tenant_name, db_dir, vectordb_dir)
    else:
        while True:
            run_pipeline(config_path, tenant_name, db_dir, vectordb_dir)
            logging.info(f"Next run will start after {interval_str}...")
            time.sleep(interval_seconds)
