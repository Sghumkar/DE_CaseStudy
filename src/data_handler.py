from src.validation import validate_and_transform
from src.logger import setup_logger

logger = setup_logger()

def process_file(file_path):
    logger.info(f"Processing file: {file_path}")
    transformed_data = validate_and_transform(file_path)
    if transformed_data is not None:
        logger.info(f"Transformed data preview:\n{transformed_data.head()}")
    else:
        logger.error(f"Failed to process file: {file_path}")

