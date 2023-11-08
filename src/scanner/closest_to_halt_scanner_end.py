import re
import time
import pytz
import datetime
import pandas as pd

from ibapi.common import BarData
from ibapi.contract import ContractDetails

from datasource.scanner_connector import ScannerConnector
from scanner.scanner_connector_callback import ScannerConnectorCallBack

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.scanner_to_request_id import ScannerToRequestId
from constant.timeframe import Timeframe

from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class ClosestToHaltScannerEnd(ScannerConnectorCallBack):
    def __init__(self):
           self.__start_time = None
           self.__closest_to_halt_ticker_list = []
           self.__closest_to_halt_contract_detail_list = []
           self.__previous_close_retrieval_counter = 0
           self.__previous_close_retrieval_size = 0
        
    def execute_scanner_data(self, req_id: int, rank: int, contract_details: ContractDetails, scanner_connector: ScannerConnector) -> None:
        logger.log_debug_msg(f'Closest to halt scanner data, reqId: {req_id}', with_speech = False)
        
        if req_id == ScannerToRequestId.CLOSEST_TO_HALT.value:
            logger.log_debug_msg('Get scanner data for closest to halt', with_speech = False)
            if rank == 0:
                if self.__start_time != None:
                    logger.log_debug_msg(f'Closest to halt scanner refresh interval time: {time.time() - self.__start_time} seconds', with_speech = False)
                    
                self.__start_time = time.time()
                self.__closest_to_halt_ticker_list = []
                self.__closest_to_halt_contract_detail_list = []
                self.__previous_close_retrieval_counter = 0
                self.__previous_close_retrieval_size = 0

                if re.match('^[a-zA-Z]{1,4}$', contract_details.contract.symbol): 
                    self.__closest_to_halt_ticker_list.append(contract_details.contract.symbol)
                    self.__closest_to_halt_contract_detail_list.append(contract_details)
                else:
                    logger.log_debug_msg(f'Exclude invalid ticker of {contract_details.contract.symbol} from closest to halt scanner result', with_speech = False)      
        
    def execute_scanner_end(self, req_id: int, scanner_connector: ScannerConnector) -> None:
        logger.log_debug_msg(f'Closest to halt scannerDataEnd, reqId: {req_id}, Result length: {len(self.__closest_to_halt_ticker_list)}, Result: {self.__closest_to_halt_ticker_list}', with_speech = False)
        
        # Limit up down would only happen in normal trading hours
        if len(self.__closest_to_halt_ticker_list) > 0:
            self.__get_previous_close(scanner_connector)
            self.__get_one_minute_candle(scanner_connector)
        else:
            logger.log_debug_msg('Skip market data retrieval', with_speech = False)    
        
    def execute_historical_data(self, req_id: int, bar: BarData, scanner_connector: ScannerConnector) -> None:
        logger.log_debug_msg(f'Closest to halt scanner get historical data, req_id: {req_id}', with_speech = False)
        
        # Retrieve previous close
        if 0 <= req_id - scanner_connector.CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX <= scanner_connector.MAXIMUM_SCANNER_RESULT_SIZE - 1:
            logger.log_debug_msg(f'0 <= reqId - CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX of 1000 <= {scanner_connector.MAXIMUM_SCANNER_RESULT_SIZE - 1}', with_speech = False)
            rank = req_id % scanner_connector.CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX
            ticker = self.__closest_to_halt_ticker_list[rank]
            previous_close = bar.open
            scanner_connector.ticker_to_previous_close_dict[ticker] = previous_close
            self.__previous_close_retrieval_counter += 1
            logger.log_debug_msg(f'{ticker} previous close: {previous_close}, rank: {rank}', with_speech = False)
            logger.log_debug_msg(f'Previous close retrieval counter: {self.__previous_close_retrieval_counter}', with_speech = False)
        
        # Retrieve minute candle
        if 0 <= req_id - scanner_connector.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX <= scanner_connector.MAXIMUM_SCANNER_RESULT_SIZE - 1:
            logger.log_debug_msg(f'0 <= reqId - CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX of 1100 <= {scanner_connector.MAXIMUM_SCANNER_RESULT_SIZE - 1}', with_speech = False)
            open = bar.open
            high = bar.high
            low = bar.low
            close = bar.close
            volume = bar.volume * 100
            dt = bar.date.replace(" US/Eastern", "")
            logger.log_debug_msg(f'reqId: {req_id}, datetime: {dt}', with_speech = False)

            self.__single_ticker_ohlcv_list.append([open, high, low, close, volume])
            self.__datetime_list.append(dt)    
    
    def execute_historical_data_end(self, req_id: int, scanner_connector: ScannerConnector) -> None:
        logger.log_debug_msg(f'Closest to halt scanner get historical data end, req_id: {req_id}', with_speech = False)
        
        if 0 <= req_id - scanner_connector.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX <= scanner_connector.MAXIMUM_SCANNER_RESULT_SIZE - 1:
            rank = req_id - scanner_connector.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX

            datetime_index = pd.DatetimeIndex(self.__datetime_list)
            ticker_to_indicator_column = pd.MultiIndex.from_product([[self.__closest_to_halt_ticker_list[rank]], [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.VOLUME]])
            single_ticker_candle_df = pd.DataFrame(self.__single_ticker_ohlcv_list, columns=ticker_to_indicator_column, index=datetime_index)
            self.__candle_df_list.append(single_ticker_candle_df)
            logger.log_debug_msg(f'{self.__closest_to_halt_ticker_list[rank]} datetime list: {self.__datetime_list}, dataframe: {single_ticker_candle_df}', with_speech = False)

            self.__single_ticker_ohlcv_list = []
            self.__datetime_list = []

        is_all_previous_close_retrieved = self.__previous_close_retrieval_counter == self.__previous_close_retrieval_size
        is_all_minute_candle_retrieved = len(self.__candle_df_list) == len(self.__closest_to_halt_ticker_list)

        # Check if all candle data is retrieved by last rank (req_id)
        if is_all_previous_close_retrieved and is_all_minute_candle_retrieved:
            concat_df_list = self.__candle_df_list
            all_ticker_one_minute_candle_df = pd.concat(concat_df_list, axis=1)
            
            close_df = all_ticker_one_minute_candle_df.loc[:, idx[:, Indicator.CLOSE]]
            close_pct_df = close_df.pct_change().mul(100).rename(columns={Indicator.CLOSE: CustomisedIndicator.CLOSE_CHANGE})
            previous_close_list = [float(scanner_connector.ticker_to_previous_close_dict[ticker]) for ticker in self.__closest_to_halt_ticker_list]
            previous_close_pct_df = (((close_df.sub(previous_close_list))
                                                .div(previous_close_list))
                                                .mul(100)).rename(columns={Indicator.CLOSE: CustomisedIndicator.PREVIOUS_CLOSE_CHANGE})
            
            complete_df = pd.concat([all_ticker_one_minute_candle_df, 
                                     close_pct_df,
                                     previous_close_pct_df], axis=1)
                
            for ticker in self.__closest_to_halt_ticker_list:
                read_ticker_str = " ".join(ticker)
                
                display_close = complete_df.loc[complete_df.index[-1], idx[ticker, Indicator.CLOSE]]
                display_volume = complete_df.loc[complete_df.index[-1], idx[ticker, Indicator.VOLUME]]
                display_close_pct = complete_df.loc[complete_df.index[-1], idx[ticker, CustomisedIndicator.CLOSE_CHANGE]]
                display_previous_close_pct = complete_df.loc[complete_df.index[-1], idx[ticker, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]]
                
                pop_up_datetime = complete_df.index[-1]
                pop_up_hour = pd.to_datetime(pop_up_datetime).hour
                pop_up_minute = pd.to_datetime(pop_up_datetime).minute
                display_hour = ('0' + str(pop_up_hour)) if pop_up_hour < 10 else pop_up_hour
                display_minute = ('0' + str(pop_up_minute)) if pop_up_minute < 10 else pop_up_minute
                display_time_str = f'{display_hour}:{display_minute}'
                read_time_str = f'{pop_up_hour} {pop_up_minute}' if (pop_up_minute > 0) else f'{pop_up_hour} o clock' 
                
                logger.log_debug_msg(f'{read_ticker_str} closest to halt at {read_time_str}', with_std_out = False)
                logger.log_debug_msg(f'{read_ticker_str} closest to halt at {display_time_str}, Close: {display_close}, Volume: {display_volume}, Close change: {display_close_pct}, Previous close change: {display_previous_close_pct}', with_speech = False)
                    
    def __get_previous_close(self, scanner_connector: ScannerConnector):
        logger.log_debug_msg('Get previous close for closest to halt ticker', with_speech = False)
        
        # Calculate how many ticker's previous close should get
        for contract_rank, contract_detail in enumerate(self.__closest_to_halt_contract_detail_list):
            if contract_detail.contract.symbol not in scanner_connector.ticker_to_previous_close_dict:
                self.__previous_close_retrieval_size += 1
        
        logger.log_debug_msg(f'No. of previous close request for closest to halt: {self.__previous_close_retrieval_size}', with_speech = False)
        
        # Send get previous close requests
        for contract_rank, contract_detail in enumerate(self.__closest_to_halt_contract_detail_list):
            if contract_detail.contract.symbol not in scanner_connector.ticker_to_previous_close_dict:
                logger.log_debug_msg(f'Get {contract_detail.contract.symbol} previous close for closest to halt, rank: {contract_rank}', with_speech = False)
                get_previous_close_req_id = ScannerConnector.CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX + contract_rank
                scanner_connector.reqHistoricalData(get_previous_close_req_id, contract_detail.contract, '', '2 D', Timeframe.ONE_DAY.value, 'TRADES', 0, 1, False, [])
                
    def __get_one_minute_candle(self, scanner_connector: ScannerConnector):
        self.__single_ticker_ohlcv_list = []
        self.__datetime_list = []
        self.__candle_df_list = []
        
        #If no durationStr unit is specified, seconds is used.
        for rank, contract_detail in enumerate(self.__closest_to_halt_contract_detail_list):
            candle_req_id = scanner_connector.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX + rank
            logger.log_debug_msg(f'Send get closest to halt ticker {contract_detail.contract.symbol} data at {datetime.datetime.now().astimezone(pytz.timezone("US/Eastern"))}', with_speech = False)
            scanner_connector.reqHistoricalData(candle_req_id, contract_detail, '', '120 S', Timeframe.ONE_MINUTE.value, 'TRADES', 0, 1, False, [])
