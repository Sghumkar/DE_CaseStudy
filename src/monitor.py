import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.data_handler import process_file
from src.logger import setup_logger

logger = setup_logger()

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
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
