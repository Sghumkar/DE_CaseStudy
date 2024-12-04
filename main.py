from src.monitor import monitor_directory
from config.settings import  DATA_DIR
from src.logger import get_logger
import os
logger = get_logger(__name__)
if __name__ == "__main__":
    try:
        logger.info("Starting the file monitoring pipeline.")
        os.makedirs(DATA_DIR, exist_ok=True) 
        monitor_directory(DATA_DIR)
    except Exception as e:
        logger.critical(f"Critical error in pipeline: {e}", exc_info=True)