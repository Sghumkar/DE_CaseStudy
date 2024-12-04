import time
import logging

logger = logging.getLogger(__name__)

def retry_operation(func, max_retries=3, delay=2, backoff=2, file_path=None):

    attempts = 0
    while attempts < max_retries:
        try:
            return func()  # This will run the file processing or DB operations
        except Exception as e:
            attempts += 1
            logger.error(f"Attempt {attempts} failed for {func.__name__} with file {file_path}: {e}")
            if attempts >= max_retries:
                logger.error(f"Max retries reached for {func.__name__} with file {file_path}. Operation failed.")
                if func.__name__ == "file_processing":
                    raise  # File processing should raise error after max retries
                else:
                    # If DB operation failed, do not retry the file processing, raise the error
                    raise  
            # Exponential backoff
            sleep_time = delay * (backoff ** (attempts - 1))
            logger.info(f"Retrying {func.__name__} for file {file_path} in {sleep_time} seconds...")
            time.sleep(sleep_time)
