from src.data_processor import validate_and_transform
from src.logger import get_logger
from src.file_utils import log_error

logger = get_logger(__name__)

def process_file(file_path):
    
    logger.info(f"Processing file: {file_path}")
    try:
        validate_and_transform(file_path)

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        log_error(file_path, str(e))  
    finally:
        logger.info(f"Waithing for new File")
      
