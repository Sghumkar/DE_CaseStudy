from datetime import datetime
import os
import pandas as pd
from src.logger import get_logger
from config.settings import DATA_DIR, LOG_DIR, QUARANTINE_DIR,PROCESSED_DIR

logger = get_logger(__name__)

def save_valid_data(df, aggregated_metrics, file_path):
    
    valid_folder = PROCESSED_DIR
    os.makedirs(valid_folder, exist_ok=True)

    base_name = os.path.basename(file_path)
    valid_file = os.path.join(valid_folder, f"valid_{base_name}")
    metrics_file = os.path.join(valid_folder, f"metrics_{base_name.replace('.csv', '')}_aggregated.csv")

    logger.info(f"Saving valid data to: {valid_file}")
    df.to_csv(valid_file, index=False)
    logger.info(f"Valid data saved to: {valid_file}")

    aggregated_metrics.to_csv(metrics_file, index=False)
    logger.info(f"Aggregated metrics saved to: {metrics_file}")


def save_failed_rows(failed_df, reasons, file_path):
    os.makedirs(QUARANTINE_DIR, exist_ok=True)

    failed_df['Reason for Failure'] = reasons
    base_name = os.path.basename(file_path)
    quarantine_file = os.path.join(QUARANTINE_DIR, f"failed_{base_name}")

    logger.info(f"Saving failed rows to quarantine: {quarantine_file}")
    failed_df.to_csv(quarantine_file, index=False)
    logger.info(f"Failed rows saved to: {quarantine_file}")


def log_error(file_path, error_message):
   
    os.makedirs(LOG_DIR, exist_ok=True)
    error_log_file = os.path.join(LOG_DIR, "error_log.txt")

    with open(error_log_file, 'a') as log:
        log.write(f"{datetime.now()} - {file_path} - {error_message}\n")

    logger.error(f"Error logged: {error_message}")
