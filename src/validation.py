import os
import pandas as pd
from config.settings import VALIDATION_RULES

def save_invalid_rows_to_quarantine(invalid_rows, file_path, quarantine_folder, logger):
    # Save invalid rows to a quarantine file
    if not os.path.exists(quarantine_folder):
        os.makedirs(quarantine_folder)  # Ensure quarantine folder exists

    file_name = os.path.basename(file_path)
    quarantine_file = os.path.join(quarantine_folder, f"quarantine_{file_name}")

    if not invalid_rows.empty:
        try:
            invalid_rows.to_csv(quarantine_file, index=False)
            logger.info(f"Invalid rows saved to quarantine: {quarantine_file}")
        except Exception as e:
            logger.error(f"Error saving invalid rows to quarantine: {e}")
    else:
        logger.info("No invalid rows to save to quarantine.")


def check_missing_values(df, failed_rows, reasons, file_path, quarantine_folder, logger):
    
    logger.info("Starting check for missing critical fields...")
    invalid_rows = pd.DataFrame()

    for index, row in df.iterrows():
        missing_fields = [col for col in VALIDATION_RULES.keys() if pd.isnull(row[col])]
        if missing_fields:
            logger.warning(f"Row {index} has missing critical fields: {', '.join(missing_fields)}")
            failed_rows.append(row.to_dict())
            reasons.append(f"Missing critical fields: {', '.join(missing_fields)}")
            invalid_rows = pd.concat([invalid_rows, pd.DataFrame([row])])

    valid_rows = df.dropna(subset=VALIDATION_RULES.keys())

    if invalid_rows.empty:
        logger.info("No rows with missing critical fields found.")
    else:
        save_invalid_rows_to_quarantine(invalid_rows, file_path, quarantine_folder, logger)

    if valid_rows.empty:
        logger.warning("No valid rows left after checking for missing critical fields.")
        return None, failed_rows, reasons

    logger.info(f"Valid rows remaining after checking missing values: {len(valid_rows)}")
    logger.info("Check for missing critical fields completed")
    return valid_rows, failed_rows, reasons


def validate_numeric_columns(df, failed_rows, reasons, file_path, quarantine_folder, logger):

    # Validate numeric columns based on range specified in VALIDATION_RULES
    logger.info("Starting validation for numeric columns...")
    invalid_rows = pd.DataFrame()

    for column, (min_val, max_val) in VALIDATION_RULES.items():
        logger.info(f"Validating column: {column} with min: {min_val}, max: {max_val}")
        df[column] = pd.to_numeric(df[column], errors='coerce')
        out_of_range_rows = df[(df[column].isnull()) | (~df[column].between(min_val, max_val))]

        if not out_of_range_rows.empty:
            logger.warning(f"Found {len(out_of_range_rows)} invalid or out-of-range entries in column: {column}")
            for index, row in out_of_range_rows.iterrows():
                failed_rows.append(row.to_dict())
                reasons.append(f"Invalid or out-of-range value in column: {column}")
            invalid_rows = pd.concat([invalid_rows, out_of_range_rows])
        else:
            logger.info(f"No invalid rows found for column: {column}")

    valid_rows = df.drop(index=invalid_rows.index, errors='ignore')

    if not invalid_rows.empty:
        save_invalid_rows_to_quarantine(invalid_rows, file_path, quarantine_folder, logger)

    if valid_rows.empty:
        logger.warning("No valid rows left after numeric validation.")
        return None, failed_rows, reasons

    logger.info(f"Valid rows remaining after numeric validation: {len(valid_rows)}")
    logger.info("Validation for numeric columns done")

    return valid_rows, failed_rows, reasons


def validate_timestamp_format(df, failed_rows, reasons, file_path, quarantine_folder, logger):
    # Validate the timestamp column format
       
    logger.info("Starting timestamp format validation...")
    
    # Identify invalid rows
    invalid_rows = df[pd.to_datetime(df['Measurement Timestamp'], errors='coerce').isnull()]
    
    if not invalid_rows.empty:
        logger.warning(f"Found {len(invalid_rows)} invalid timestamps.")
        for index, row in invalid_rows.iterrows():
            failed_rows.append(row.to_dict())
            reasons.append("Invalid timestamp format")
        save_invalid_rows_to_quarantine(invalid_rows, file_path, quarantine_folder, logger)
    
    valid_rows = df[~df.index.isin(invalid_rows.index)]
    
    if valid_rows.empty:
        logger.warning("No valid rows left after timestamp validation.")
        return None, failed_rows, reasons
    
    try:
        valid_rows['Measurement Timestamp'] = pd.to_datetime(valid_rows['Measurement Timestamp'])
        logger.info("Converted 'Measurement Timestamp' to datetime format for valid rows.")
    except Exception as e:
        logger.error(f"Error while converting timestamps: {e}")
        raise

    logger.info(f"Remaining valid rows after timestamp validation: {len(valid_rows)}")
    return valid_rows, failed_rows, reasons
