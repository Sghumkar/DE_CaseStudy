from datetime import datetime
import os
import pandas as pd
from src.logger import setup_logger
from config.settings import  DATA_DIR,LOG_DIR,QUARANTINE_DIR

logger = setup_logger()

def save_valid_data(df, aggregated_metrics, file_path):
    """Save valid data and aggregated metrics to separate files."""
    # Set processed folder to the absolute path you want
    valid_folder = r'C:\Users\GhumkaS\Desktop\de2\processed'
    os.makedirs(valid_folder, exist_ok=True)

    base_name = os.path.basename(file_path)
    valid_file = os.path.join(valid_folder, f"valid_{base_name}")
    metrics_file = os.path.join(valid_folder, f"metrics_{base_name.replace('.csv', '')}_aggregated.csv")

    print(f"Saving valid data to: {valid_file}")  # Debugging output
    df.to_csv(valid_file, index=False)
    print(f"Valid data saved to: {valid_file}")  # Debugging output

    print(f"Saving aggregated metrics to: {metrics_file}")  # Debugging output
    aggregated_metrics.to_csv(metrics_file, index=False)
    print(f"Aggregated metrics saved to: {metrics_file}")  # Debugging output


def save_failed_rows(failed_df, reasons, file_path):
    """Save failed rows to a quarantine directory with failure reasons."""

    os.makedirs(QUARANTINE_DIR, exist_ok=True)

    failed_df['Reason for Failure'] = reasons
    base_name = os.path.basename(file_path)
    quarantine_file = os.path.join(QUARANTINE_DIR, f"failed_{base_name}")
    
    print(f"Saving failed rows to quarantine: {quarantine_file}")  # Debugging output
    failed_df.to_csv(quarantine_file, index=False)
    print(f"Failed rows saved to: {quarantine_file}")  # Debugging output

def log_error(file_path, error_message):
    """Log errors to a log file."""
    
    os.makedirs(LOG_DIR, exist_ok=True)
    error_log_file = os.path.join(LOG_DIR, "error_log.txt")

    with open(error_log_file, 'a') as log:
        log.write(f"{datetime.now()} - {file_path} - {error_message}\n")
    logger.error(f"Error logged: {error_message}")
