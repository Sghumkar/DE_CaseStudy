import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.data_handler import process_file
from src.logger import get_logger

from src.file_utils import log_error
logger = get_logger(__name__)

class DataHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.src_path.endswith('.csv'):
            logger.info(f"New file detected: {event.src_path}")
            process_file(event.src_path)

def monitor_directory(directory):
    logger.info(f"Monitoring directory: {directory}")
    event_handler = DataHandler()
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Stopping directory monitor due to keyboard interrupt.")
        observer.stop()
    except Exception as e:
        logger.error(f"Unexpected error in monitor: {e}")
        log_error(directory, str(e))  
    finally:
        observer.join()
