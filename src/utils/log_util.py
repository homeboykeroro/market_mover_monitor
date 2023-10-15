from datetime import datetime
import logging
import os

def get_logger(name: str='root', 
                log_parent_directory: str = 'C:/Users/John/Downloads/Trade History/Scanner', 
                level: int=logging.DEBUG,
                console_log: bool=True,
                display_format: str='\r%(asctime)s - %(message)s (%(levelname)s)',
                date_format: str='%m/%d/%Y %I:%M:%S %p'):
    log_date = datetime.now().strftime('%Y%m%d')
    log_filename = 'scanner_log_' + log_date + '.txt'
    log_dir = log_parent_directory + "/" + log_filename
    if not os.path.exists(os.path.dirname(log_dir)) and os.path.dirname(log_dir):
        os.makedirs(os.path.dirname(log_dir))

    logger = logging.getLogger(name)
    handler = logging.FileHandler(log_dir)
    logger.setLevel(level)

    if not len(logger.handlers):
        formatter = logging.Formatter(display_format, datefmt=date_format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        if console_log:
            console = logging.StreamHandler()
            console.setLevel(level)
            logger.addHandler(console)

    return logger

if __name__ == '__main__':
    logger = get_logger()