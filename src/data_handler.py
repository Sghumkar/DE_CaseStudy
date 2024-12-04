from src.data_processor import validate_and_transform
from src.logger import get_logger

logger = get_logger(__name__)

processing_completed = False

def process_file(file_path):
    global processing_completed
    
    logger.info(f"Processing file: {file_path}")
    transformed_data = validate_and_transform(file_path)
    
    if transformed_data is not None:
        logger.info(f"Processing of file: {file_path} completed.")
    else:
        logger.info(f"No rows to display: {file_path}")
    
    logger.info(f"Processing complete for file: {file_path}")
    
    processing_completed = True
