import os
import pandas as pd
from datetime import datetime
from config.settings import VALIDATION_RULES, DATA_DIR, LOG_DIR, QUARANTINE_DIR
from src.logger import setup_logger

logger = setup_logger()

def validate_and_transform(file_path):
    try:
        logger.info(f"Starting to process the file: {file_path}")
        df = read_csv(file_path)

        if df is None or df.empty:
            logger.warning(f"The file {file_path} contains no data.")
            return None

        failed_rows, reasons = [], []
        df, failed_rows, reasons = check_missing_values(df, failed_rows, reasons)
        logger.info(f"Checking missing values complete")
        
        if df is None or df.empty:
            return None

        df, failed_rows, reasons = validate_numeric_columns(df, failed_rows, reasons)
        
        if df is None or df.empty:
            return None

        df, failed_rows, reasons = validate_timestamp_format(df, failed_rows, reasons)

        df = add_metadata(df, file_path)

        if failed_rows:
            logger.warning(f"Found {len(failed_rows)} rows with validation errors.")
            save_failed_rows(pd.DataFrame(failed_rows), reasons, file_path)

        if df.empty:
            logger.warning(f"After validation, no valid rows remain in {file_path}.")
            return None

        logger.info(f"Data transformation complete. Returning valid rows.")
        return df

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        log_error(file_path, str(e))
        return None


def read_csv(file_path):
    """Read the CSV file and return a DataFrame."""
    try:
        logger.info("Reading CSV file...")
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None


def check_missing_values(df, failed_rows, reasons):
    """Check for missing critical fields in the DataFrame."""
    logger.info(f"Checking for missing critical fields: {VALIDATION_RULES.keys()}")
    for index, row in df.iterrows():
        missing_fields = [col for col in VALIDATION_RULES.keys() if pd.isnull(row[col])]
        if missing_fields:
            failed_rows.append(row.to_dict())
            reasons.append(f"Missing critical fields: {', '.join(missing_fields)}")
    
    valid_rows = df.dropna(subset=VALIDATION_RULES.keys())
    if valid_rows.empty:
        logger.warning(f"No rows left after removing those with missing critical fields.")
        if failed_rows:
            save_failed_rows(pd.DataFrame(failed_rows), reasons, df)
        return None, failed_rows, reasons

    logger.info(f"Removed rows with missing critical fields. Remaining valid rows: {len(valid_rows)}")
    return valid_rows, failed_rows, reasons


def validate_numeric_columns(df, failed_rows, reasons):
    logger.info("Validating numeric columns and checking for out-of-range values...")
    
    # Create an empty set to keep track of all invalid indices across columns
    invalid_indices = set()

    for column, (min_val, max_val) in VALIDATION_RULES.items():
        logger.info(f"Validating column: {column}")
        
        # Convert the column to numeric, coercing errors to NaN
        df[column] = pd.to_numeric(df[column], errors='coerce')
        
        # Find rows with invalid or out-of-range values
        invalid_rows = df[(df[column].isnull()) | (~df[column].between(min_val, max_val))]

        if not invalid_rows.empty:
            logger.info(f"Found {len(invalid_rows)} invalid or out-of-range entries in column: {column}")
            for index, row in invalid_rows.iterrows():
                failed_rows.append(row.to_dict())
                reasons.append(f"Invalid or out-of-range value in column: {column}")
                logger.info(f"Processed row {index} in column {column} - Invalid value")
        else:
            logger.info(f"No invalid rows found for column {column}")

        # Add invalid row indices to the invalid_indices set
        invalid_indices.update(invalid_rows.index)

    # Filter out the invalid rows once after processing all columns
    valid_rows = df.drop(index=invalid_indices, errors='ignore')

    # Log the DataFrame's data types and return the valid rows
    logger.info(f"df is {valid_rows.dtypes}")

    return valid_rows, failed_rows, reasons



def validate_timestamp_format(df, failed_rows, reasons):
    """Validate the timestamp column format."""
    logger.info("Validating timestamp format...")
    df['Measurement Timestamp'] = pd.to_datetime(df['Measurement Timestamp'], errors='coerce')
    invalid_timestamps = df['Measurement Timestamp'].isnull()

    if invalid_timestamps.sum() > 0:
        logger.info(f"Found {invalid_timestamps.sum()} invalid timestamps.")
        for index, row in df[invalid_timestamps].iterrows():
            failed_rows.append(row.to_dict())
            reasons.append("Invalid timestamp format")
        df = df[~invalid_timestamps]
    else:
        logger.info("All timestamps are valid.")

    return df, failed_rows, reasons


def add_metadata(df, file_path):
    """Add metadata columns (Processed At, Source File) to valid rows."""
    logger.info("Adding metadata columns to valid rows...")
    df['Processed At'] = datetime.now()
    df['Source File'] = os.path.basename(file_path)
    return df


def save_failed_rows(failed_df, reasons, file_path):
    """Save failed rows to a quarantine directory with failure reasons."""
    quarantine_folder = QUARANTINE_DIR
    os.makedirs(quarantine_folder, exist_ok=True)

    failed_df['Reason for Failure'] = reasons
    base_name = os.path.basename(file_path)
    quarantine_file = os.path.join(quarantine_folder, f"failed_{base_name}")
    
    logger.info(f"Saving failed rows to quarantine: {quarantine_file}")
    failed_df.to_csv(quarantine_file, index=False)
    logger.info(f"Failed rows saved to: {quarantine_file}")


def log_error(file_path, error_message):
    """Log errors to a log file."""
    log_folder = LOG_DIR
    os.makedirs(log_folder, exist_ok=True)
    error_log_file = os.path.join(log_folder, "error_log.txt")

    with open(error_log_file, 'a') as log:
        log.write(f"{datetime.now()} - {file_path} - {error_message}\n")
    logger.error(f"Error logged: {error_message}")
