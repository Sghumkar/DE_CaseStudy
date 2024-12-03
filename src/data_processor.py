from src.validation import check_missing_values, validate_numeric_columns, validate_timestamp_format
from src.aggregation import calculate_aggregated_metrics
from src.file_utils import save_valid_data, save_failed_rows, log_error
from src.logger import get_logger
import pandas as pd
from config.settings import CRITICAL_COLUMNS

logger = get_logger(__name__)

def validate_and_transform(file_path, quarantine_folder_path):
    try:
        logger.info(f"Starting to process the file: {file_path}")
        df = pd.read_csv(file_path,usecols=CRITICAL_COLUMNS)
        
        
        if df is None or df.empty:
            logger.warning(f"The file {file_path} contains no data.")
            return None

        failed_rows, reasons = [], []
        df, failed_rows, reasons = check_missing_values(df, failed_rows, reasons, file_path, quarantine_folder_path, logger)
        
        if df is None or df.empty:
            return None

        df, failed_rows, reasons = validate_numeric_columns(df, failed_rows, reasons, file_path, quarantine_folder_path, logger)

        if df is None or df.empty:
            return None

        df, failed_rows, reasons = validate_timestamp_format(df, failed_rows, reasons, file_path, quarantine_folder_path, logger)


        # Save any failed rows
        if failed_rows:
            save_failed_rows(pd.DataFrame(failed_rows), reasons, file_path)

        # If valid rows exist, process them
        if not df.empty:
            aggregated_metrics = calculate_aggregated_metrics(df,file_path)
            save_valid_data(df, aggregated_metrics, file_path)
            df=aggregated_metrics

        logger.info(f"Processing complete for file: {file_path}")
        return df

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        log_error(file_path, str(e))
        return None
