import re
import time
import pytz
import datetime
import pandas as pd

from ibapi.common import BarData
from ibapi.contract import ContractDetails

from scanner.scanner_connector_callback import ScannerConnectorCallBack

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.scanner_to_request_id import ScannerToRequestId
from constant.halt_reason import HaltReason

from utils.trade_halt_info_retrieval_util import retrieve_trade_halt_info
from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class HaltScannerEnd(ScannerConnectorCallBack):
    TRADE_HALT_NOTIFY_TIMES = 4
    TRADE_HALT_CODE_LIST = [halt_reason.name for halt_reason in HaltReason]
    
    def __init__(self):
        self.__start_time = None
        self.__halt_ticker_list = []
        
    def execute_scanner_data(self, req_id: int, rank: int, contract_details: ContractDetails) -> None:
        logger.log_debug_msg(f'Halt scanner data, reqId: {req_id}', with_speech = False)
        
        logger.log_debug_msg('Get scanner data for Halt', with_speech = False)
        if rank == 0:
            if self.__start_time != None:
                logger.log_debug_msg(f'Halt scanner refresh interval time: {time.time() - self.__start_time} seconds', with_speech = False)
                
            self.__start_time = time.time()
            self.__halt_ticker_list = []

        if re.match('^[a-zA-Z]{1,4}$', contract_details.contract.symbol): 
            self.__halt_ticker_list.append(contract_details.contract.symbol)
        else:
            logger.log_debug_msg(f'Exclude invalid ticker of {contract_details.contract.symbol} from halt scanner result', with_speech = False)      
        
    def execute_scanner_end(self, req_id: int, ticker_to_previous_close_dict: dict, scanner_connector) -> None:
        logger.log_debug_msg(f'Halt scannerDataEnd, reqId: {req_id}, Result length: {len(self.__halt_ticker_list)}, Result: {self.__halt_ticker_list}', with_speech = False)
        us_current_datetime = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
        ticker_to_trade_halt_info_dict = retrieve_trade_halt_info()
        
        for ticker in self.__halt_ticker_list:
            if ticker in ticker_to_trade_halt_info_dict:
                trade_halt_record = ticker_to_trade_halt_info_dict[ticker]
                halt_date = trade_halt_record.halt_date
                halt_time = trade_halt_record.halt_time
                resumption_quote_time = trade_halt_record.resumption_quote_time
                reason_code = trade_halt_record.reason
                
                halt_datetime = datetime.datetime.strptime(f'{halt_date} {halt_time}', '%m/%d/%Y %H:%M:%S')
                halt_datetime = pytz.timezone('US/Eastern').localize(halt_datetime)
                
                halt_hour = halt_datetime.hour
                halt_minute = halt_datetime.minute
                display_halt_time = halt_datetime.strftime("%H:%M:%S")
                display_resumption_quote_time = resumption_quote_time if resumption_quote_time else 'Unknown'

                read_time_str = f'{halt_hour} {halt_minute}' if (halt_minute > 0) else f'{halt_hour} o clock' 
                read_ticker_str = " ".join(ticker)
                read_reason_code_str = " ".join(reason_code)
                
                time_interval = (us_current_datetime - halt_datetime).total_seconds() / 60
                if time_interval <= self.TRADE_HALT_NOTIFY_TIMES and reason_code in self.TRADE_HALT_CODE_LIST:
                    logger.log_debug_msg(f'{read_ticker_str} {read_reason_code_str} halt at {read_time_str}')
                    logger.log_debug_msg(f'{ticker} {reason_code} ({HaltReason[reason_code].value}) at {display_halt_time}, Quoted resumption time: {display_resumption_quote_time}', with_speech = False)
    
    def execute_historical_data(self, req_id: int, bar: BarData, ticker_to_previous_close_dict: dict) -> None:
        pass 
    
    def execute_historical_data_end(self, req_id: int, ticker_to_previous_close_dict: dict) -> None:
        pass