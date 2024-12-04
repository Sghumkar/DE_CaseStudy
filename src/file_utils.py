from datetime import datetime
import os
import pandas as pd
from src.logger import get_logger
from config.settings import DATA_DIR, LOG_DIR, QUARANTINE_DIR
from db.db_utils import get_db_connection, release_db_connection,create_tables_if_not_exist,insert_raw_data,insert_aggregated_data

logger = get_logger(__name__)

def save_valid_data(df, aggregated_metrics_temp):

    aggregated_metrics=[
        (
            row['Source File'],
            row['Station Name'],
            row['min_temp'],
            row['max_temp'],
            row['avg_temp'],
            row['std_temp'],
            row['min_humidity'],
            row['max_humidity'],
            row['avg_humidity'],
            row['std_humidity'],
            row['min_pressure'],
            row['max_pressure'],
            row['avg_pressure'],
            row['std_pressure']
        )
        for _, row in aggregated_metrics_temp.iterrows()
    ]

    raw_data = [
        (
            row['Station Name'],
            row['Measurement ID'],
            row['Measurement Timestamp'],
            row['Air Temperature'],
            row['Barometric Pressure'],
            row['Humidity']
        )
        for _, row in df.iterrows()
    ]


    conn = None
    try:
        conn = get_db_connection()

        logger.info('connection established')

        create_tables_if_not_exist(conn)

        cursor = conn.cursor()
        insert_raw_data(conn, cursor, raw_data)
        insert_aggregated_data(conn, cursor, aggregated_metrics)
        logger.info('Data insertion to db completed')

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error saving data to the database: {e}")
        raise

    finally:
        if conn:
            release_db_connection(conn)





def save_failed_rows(failed_df, reasons, file_path):
    os.makedirs(QUARANTINE_DIR, exist_ok=True)

    failed_df['Reason for Failure'] = reasons
    base_name = os.path.basename(file_path)
    quarantine_file = os.path.join(QUARANTINE_DIR, f"failed_{base_name}")

    failed_df.to_csv(quarantine_file, index=False)
    logger.info(f"Failed rows saved to: {quarantine_file}")


def log_error(file_path, error_message):
   
    os.makedirs(LOG_DIR, exist_ok=True)
    error_log_file = os.path.join(LOG_DIR, "error_log.txt")

    with open(error_log_file, 'a') as log:
        log.write(f"{datetime.now()} - {file_path} - {error_message}\n")

    logger.error(f"Error logged: {error_message}")
