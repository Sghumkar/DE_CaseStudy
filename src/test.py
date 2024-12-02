import pandas as pd
import logging

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sample Validation Rules
VALIDATION_RULES = {
    "Air Temperature": (-50, 50),
    "Wet Bulb Temperature": (-50, 50),
    "Humidity": (0, 100),
}

def validate_and_transform(df, VALIDATION_RULES):
    failed_rows = []
    for column, (min_val, max_val) in VALIDATION_RULES.items():
        logger.info(f"Validation start for column: {column}")

        # Check if the column exists in the DataFrame
        if column not in df.columns:
            logger.warning(f"Column '{column}' is missing.")
            continue

        # Step 1: Convert the column to numeric (coerce errors to NaN)
        df[column] = pd.to_numeric(df[column], errors='coerce')
        logger.info(f"Validation step 1 completed for column: {column}")

        # Step 2: Check for invalid (NaN) or out-of-range values
        invalid_rows = df[(df[column].isnull()) | (~df[column].between(min_val, max_val))]
        #logger.info(f"Validation step 2 completed for column: {column}")

        if not invalid_rows.empty:
            #logger.info(f"Invalid rows found in column {column}: {invalid_rows}")
            failed_rows.extend(invalid_rows.to_dict('records'))

    return df, failed_rows

def test_validate_and_transform():
    file_path = r'C:\Users\GhumkaS\Desktop\de2\data\humidity.csv'  # Path to your CSV file
    df = pd.read_csv(file_path)

    # Apply validation and transformation
    transformed_df, failed_rows = validate_and_transform(df, VALIDATION_RULES)

    # Log the transformed DataFrame
    logger.info("Transformed DataFrame:")
    #logger.info(transformed_df)

    # Log the failed rows
    # if failed_rows:
    #     logger.info(f"Failed rows: {len(failed_rows)}")
    #     for row in failed_rows:
    #         logger.info('row')

# Run the test
test_validate_and_transform()
