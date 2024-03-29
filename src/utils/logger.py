from datetime import datetime
import logging
import os

from utils.text_to_speech_engine import TextToSpeechEngine

class Logger:
    def __init__(self):
        self.__logger = self.__get_logger()
        self.__text_to_speech_engine = TextToSpeechEngine()

    def log_debug_msg(self, msg: str, with_speech: bool = False, with_log_file: bool = True, with_std_out: bool = False):
        if with_std_out:
            print(msg)
        
        if with_log_file:
            self.__logger.debug(msg)
        
        if with_speech:
            self.__text_to_speech_engine.speak(msg)
            
    def log_error_msg(self, msg: str, with_speech: bool = False, with_log_file: bool = True, with_std_out: bool = False):
        if with_std_out:
            print(msg)
        
        if with_log_file:
            self.__logger.exception(msg)
        
        if with_speech:
            self.__text_to_speech_engine.speak(msg)

    def __get_logger(self, name: str = 'root',
                   log_parent_directory: str = 'C:/Users/John/Downloads/Trade History/Scanner',
                   level: int = logging.DEBUG,
                   display_format: str = '\r%(asctime)s - %(message)s (%(levelname)s)',
                   date_format: str = '%m/%d/%Y %I:%M:%S %p'):
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

        return logger