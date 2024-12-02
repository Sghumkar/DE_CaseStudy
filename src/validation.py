
import pandas as pd
from config.settings import VALIDATION_RULES

def check_missing_values(df, failed_rows, reasons):
    """Check for missing critical fields in the DataFrame."""
    for index, row in df.iterrows():
        missing_fields = [col for col in VALIDATION_RULES.keys() if pd.isnull(row[col])]
        if missing_fields:
            failed_rows.append(row.to_dict())
            reasons.append(f"Missing critical fields: {', '.join(missing_fields)}")

    valid_rows = df.dropna(subset=VALIDATION_RULES.keys())
    if valid_rows.empty:
        return None, failed_rows, reasons
    return valid_rows, failed_rows, reasons

def validate_numeric_columns(df, failed_rows, reasons):
    """Validate numeric columns based on range specified in VALIDATION_RULES."""
    invalid_indices = set()
    for column, (min_val, max_val) in VALIDATION_RULES.items():
        df[column] = pd.to_numeric(df[column], errors='coerce')
        invalid_rows = df[(df[column].isnull()) | (~df[column].between(min_val, max_val))]
        for index, row in invalid_rows.iterrows():
            failed_rows.append(row.to_dict())
            reasons.append(f"Invalid or out-of-range value in column: {column}")
        invalid_indices.update(invalid_rows.index)

    valid_rows = df.drop(index=invalid_indices, errors='ignore')
    return valid_rows, failed_rows, reasons

def validate_timestamp_format(df, failed_rows, reasons):
    """Validate the timestamp column format."""
    df['Measurement Timestamp'] = pd.to_datetime(df['Measurement Timestamp'], errors='coerce')
    invalid_timestamps = df['Measurement Timestamp'].isnull()
    for index, row in df[invalid_timestamps].iterrows():
        failed_rows.append(row.to_dict())
        reasons.append("Invalid timestamp format")
    df = df[~invalid_timestamps]
    return df, failed_rows, reasons



# import os
# import pandas as pd
# from datetime import datetime
# from config.settings import VALIDATION_RULES, DATA_DIR, LOG_DIR, QUARANTINE_DIR
# from src.logger import setup_logger

# logger = setup_logger()


# def validate_and_transform(file_path):
#     """Main function to validate, transform, and process the file."""
#     try:
#         logger.info(f"Starting to process the file: {file_path}")
#         df = read_csv(file_path)

#         if df is None or df.empty:
#             logger.warning(f"The file {file_path} contains no data.")
#             return None

#         # Validate and clean the data
#         failed_rows, reasons = [], []
#         df, failed_rows, reasons = check_missing_values(df, failed_rows, reasons)

#         if df is None or df.empty:
#             return None

#         df, failed_rows, reasons = validate_numeric_columns(df, failed_rows, reasons)

#         if df is None or df.empty:
#             return None

#         df, failed_rows, reasons = validate_timestamp_format(df, failed_rows, reasons)

#         # Add metadata to valid rows
#         df = add_metadata(df, file_path)

#         # Save any failed rows
#         if failed_rows:
#             save_failed_rows(pd.DataFrame(failed_rows), reasons, file_path)

#         # If valid rows exist, process them
#         if not df.empty:
#             # Calculate aggregated metrics
#             aggregated_metrics = calculate_aggregated_metrics(df)

#             # Save valid data and metrics
#             save_valid_data(df, aggregated_metrics, file_path)

#         logger.info(f"Processing complete for file: {file_path}")
#         return df

#     except Exception as e:
#         logger.error(f"Error processing file {file_path}: {e}")
#         log_error(file_path, str(e))
#         return None


# def read_csv(file_path):
#     """Read the CSV file and return a DataFrame."""
#     try:
#         logger.info("Reading CSV file...")
#         return pd.read_csv(file_path)
#     except Exception as e:
#         logger.error(f"Error reading file {file_path}: {e}")
#         return None


# def check_missing_values(df, failed_rows, reasons):
#     """Check for missing critical fields in the DataFrame."""
#     logger.info(f"Checking for missing critical fields: {VALIDATION_RULES.keys()}")
#     for index, row in df.iterrows():
#         missing_fields = [col for col in VALIDATION_RULES.keys() if pd.isnull(row[col])]
#         if missing_fields:
#             failed_rows.append(row.to_dict())
#             reasons.append(f"Missing critical fields: {', '.join(missing_fields)}")
    
#     valid_rows = df.dropna(subset=VALIDATION_RULES.keys())
#     if valid_rows.empty:
#         logger.warning("No rows left after removing those with missing critical fields.")
#         if failed_rows:
#             save_failed_rows(pd.DataFrame(failed_rows), reasons, df)
#         return None, failed_rows, reasons

#     logger.info(f"Removed rows with missing critical fields. Remaining valid rows: {len(valid_rows)}")
#     return valid_rows, failed_rows, reasons


# def validate_numeric_columns(df, failed_rows, reasons):
#     """Validate numeric columns based on range specified in VALIDATION_RULES."""
#     logger.info("Validating numeric columns and checking for out-of-range values...")
#     invalid_indices = set()

#     for column, (min_val, max_val) in VALIDATION_RULES.items():
#         logger.info(f"Validating column: {column}")
#         df[column] = pd.to_numeric(df[column], errors='coerce')
#         invalid_rows = df[(df[column].isnull()) | (~df[column].between(min_val, max_val))]

#         if not invalid_rows.empty:
#             logger.info(f"Found {len(invalid_rows)} invalid or out-of-range entries in column: {column}")
#             for index, row in invalid_rows.iterrows():
#                 failed_rows.append(row.to_dict())
#                 reasons.append(f"Invalid or out-of-range value in column: {column}")
#         else:
#             logger.info(f"No invalid rows found for column {column}")

#         invalid_indices.update(invalid_rows.index)

#     valid_rows = df.drop(index=invalid_indices, errors='ignore')
#     return valid_rows, failed_rows, reasons


# def validate_timestamp_format(df, failed_rows, reasons):
#     """Validate the timestamp column format."""
#     logger.info("Validating timestamp format...")
#     df['Measurement Timestamp'] = pd.to_datetime(df['Measurement Timestamp'], errors='coerce')
#     invalid_timestamps = df['Measurement Timestamp'].isnull()

#     if invalid_timestamps.sum() > 0:
#         logger.info(f"Found {invalid_timestamps.sum()} invalid timestamps.")
#         for index, row in df[invalid_timestamps].iterrows():
#             failed_rows.append(row.to_dict())
#             reasons.append("Invalid timestamp format")
#         df = df[~invalid_timestamps]
#     else:
#         logger.info("All timestamps are valid.")

#     return df, failed_rows, reasons


# def add_metadata(df, file_path):
#     """Add metadata columns (Processed At, Source File) to valid rows."""
#     logger.info("Adding metadata columns to valid rows...")
#     df['Processed At'] = datetime.now()
#     df['Source File'] = os.path.basename(file_path)
#     return df


# def calculate_aggregated_metrics(df):
#     """Calculate aggregated metrics for each sensor type."""
#     logger.info("Calculating aggregated metrics for each sensor type...")
#     metrics = df.groupby('Source File').agg(
#         min_temp=('Air Temperature', 'min'),
#         max_temp=('Air Temperature', 'max'),
#         avg_temp=('Air Temperature', 'mean'),
#         std_temp=('Air Temperature', 'std'),
#         min_humidity=('Humidity', 'min'),
#         max_humidity=('Humidity', 'max'),
#         avg_humidity=('Humidity', 'mean'),
#         std_humidity=('Humidity', 'std'),
#     ).reset_index()

#     logger.info("Aggregation complete.")
#     return metrics


# def save_valid_data(df, aggregated_metrics, file_path):
#     """Save valid data and aggregated metrics to separate files."""
#     valid_folder = os.path.join(DATA_DIR, 'processed')
#     os.makedirs(valid_folder, exist_ok=True)

#     base_name = os.path.basename(file_path)
#     valid_file = os.path.join(valid_folder, f"valid_{base_name}")
#     metrics_file = os.path.join(valid_folder, f"metrics_{base_name.replace('.csv', '')}_aggregated.csv")

#     logger.info(f"Saving valid data to: {valid_file}")
#     df.to_csv(valid_file, index=False)
#     logger.info(f"Valid data saved to: {valid_file}")

#     logger.info(f"Saving aggregated metrics to: {metrics_file}")
#     aggregated_metrics.to_csv(metrics_file, index=False)
#     logger.info(f"Aggregated metrics saved to: {metrics_file}")


# def save_failed_rows(failed_df, reasons, file_path):
#     """Save failed rows to a quarantine directory with failure reasons."""
#     quarantine_folder = QUARANTINE_DIR
#     os.makedirs(quarantine_folder, exist_ok=True)

#     failed_df['Reason for Failure'] = reasons
#     base_name = os.path.basename(file_path)
#     quarantine_file = os.path.join(quarantine_folder, f"failed_{base_name}")
    
#     logger.info(f"Saving failed rows to quarantine: {quarantine_file}")
#     failed_df.to_csv(quarantine_file, index=False)
#     logger.info(f"Failed rows saved to: {quarantine_file}")


# def log_error(file_path, error_message):
#     """Log errors to a log file."""
#     log_folder = LOG_DIR
#     os.makedirs(log_folder, exist_ok=True)
#     error_log_file = os.path.join(log_folder, "error_log.txt")

#     with open(error_log_file, 'a') as log:
#         log.write(f"{datetime.now()} - {file_path} - {error_message}\n")
#     logger.error(f"Error logged: {error_message}")
