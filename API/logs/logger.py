import logging
from pathlib import Path

def new_logger():
    log_file = Path(__file__).with_name('logs.log')
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers = [logging.FileHandler(log_file, encoding='utf-8')], force=True)
    return logging.getLogger('logs')
