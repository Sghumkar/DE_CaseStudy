from src.data_processor import validate_and_transform
from src.logger import get_logger
from config.settings import QUARANTINE_DIR

logger = get_logger(__name__)

def process_file(file_path):
    logger.info(f"Processing file: {file_path}")
    transformed_data = validate_and_transform(file_path, QUARANTINE_DIR)
    
    if transformed_data is not None:
        logger.info(f"Transformed data schema:\n{transformed_data.dtypes}")
    else:
        logger.error(f"No rows to display: {file_path}")
