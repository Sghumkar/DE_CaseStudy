import logging
import os
from config.settings import LOG_DIR

os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(name="WeatherDataPipeline"):
    
    logger = logging.getLogger(name)
    if not logger.hasHandlers():  # Check if the logger already has handlers
        logger.setLevel(logging.DEBUG)
        log_file = os.path.join(LOG_DIR, "app.log")

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter('%(asctime)s - %(filename)s - %(funcName)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter('%(asctime)s - %(filename)s - %(funcName)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
