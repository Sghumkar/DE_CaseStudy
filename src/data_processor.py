# src/data_processor.py
import pandas as pd
from src.validation import check_missing_values, validate_numeric_columns, validate_timestamp_format
from src.metadata import add_metadata
from src.aggregation import calculate_aggregated_metrics
from src.file_utils import save_valid_data, save_failed_rows, log_error
from src.logger import setup_logger

logger = setup_logger()

def validate_and_transform(file_path):
    """Main function to validate, transform, and process the file."""
    try:
        logger.info(f"Starting to process the file: {file_path}")
        df = pd.read_csv(file_path)

        if df is None or df.empty:
            logger.warning(f"The file {file_path} contains no data.")
            return None

        # Validate and clean the data
        failed_rows, reasons = [], []
        df, failed_rows, reasons = check_missing_values(df, failed_rows, reasons)

        if df is None or df.empty:
            return None

        df, failed_rows, reasons = validate_numeric_columns(df, failed_rows, reasons)

        if df is None or df.empty:
            return None

        df, failed_rows, reasons = validate_timestamp_format(df, failed_rows, reasons)

        # Add metadata to valid rows
        df = add_metadata(df, file_path)

        # Save any failed rows
        if failed_rows:
            save_failed_rows(pd.DataFrame(failed_rows), reasons, file_path)

        # If valid rows exist, process them
        if not df.empty:
            # Calculate aggregated metrics
            aggregated_metrics = calculate_aggregated_metrics(df)

            # Save valid data and metrics
            save_valid_data(df, aggregated_metrics, file_path)

        logger.info(f"Processing complete for file: {file_path}")
        return df

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        log_error(file_path, str(e))
        return None
