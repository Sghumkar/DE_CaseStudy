import pandas as pd
from config.settings import VALIDATION_RULES,CRITICAL_COLUMNS


def check_missing_values(df, failed_rows, reasons,logger):
    logger.info("Starting check for missing critical fields...")
    invalid_rows = pd.DataFrame()

    for index,row in df.iterrows():
        missing_fields = [col for col in CRITICAL_COLUMNS if pd.isnull(row[col])]
        if missing_fields:
            failed_rows.append(row.to_dict())
            reasons.append(f"Missing critical fields: {', '.join(missing_fields)}")
            invalid_rows = pd.concat([invalid_rows, pd.DataFrame([row])])

    valid_rows = df.dropna(subset=CRITICAL_COLUMNS)

    if valid_rows.empty:
        logger.warning("No valid rows left after checking for missing critical fields.")
        return None, failed_rows, reasons

    logger.info(f"Valid rows remaining after checking missing values: {len(valid_rows)}")
    return valid_rows, failed_rows, reasons


def validate_numeric_columns(df, failed_rows, reasons, logger):
    logger.info("Starting validation for numeric columns...")
    invalid_rows = pd.DataFrame()

    for column, (min_val, max_val) in VALIDATION_RULES.items():
        df[column] = pd.to_numeric(df[column], errors='coerce')
        out_of_range_rows = df[(df[column].isnull()) | (~df[column].between(min_val, max_val))]

        if not out_of_range_rows.empty:
            logger.warning(f"Found {len(out_of_range_rows)} invalid or out-of-range entries in column: {column}")
            for  index,row in out_of_range_rows.iterrows():
                failed_rows.append(row.to_dict())
                reasons.append(f"Invalid or out-of-range value in column: {column}")
            invalid_rows = pd.concat([invalid_rows, out_of_range_rows])

    valid_rows = df.drop(index=invalid_rows.index, errors='ignore')

    if valid_rows.empty:
        logger.warning("No valid rows left after numeric validation.")
        return None, failed_rows, reasons

    logger.info(f"Valid rows remaining after numeric validation: {len(valid_rows)}")
    return valid_rows, failed_rows, reasons


def validate_timestamp_format(df, failed_rows, reasons, logger):
    logger.info("Starting timestamp format validation...")

    invalid_rows = df[pd.to_datetime(df['Measurement Timestamp'], errors='coerce').isnull()]
    
    if not invalid_rows.empty:
        logger.warning(f"Found {len(invalid_rows)} invalid timestamps.")
        for  index,row in invalid_rows.iterrows():
            failed_rows.append(row.to_dict())
            reasons.append("Invalid timestamp format")
    
    valid_rows = df[~df.index.isin(invalid_rows.index)]
    
    if valid_rows.empty:
        logger.warning("No valid rows left after timestamp validation.")
        return None, failed_rows, reasons
    
    valid_rows['Measurement Timestamp'] = pd.to_datetime(valid_rows['Measurement Timestamp'])
    logger.info("Converted 'Measurement Timestamp' to datetime format for valid rows.")
    

    logger.info(f"Remaining valid rows after timestamp validation: {len(valid_rows)}")
    return valid_rows, failed_rows, reasons


