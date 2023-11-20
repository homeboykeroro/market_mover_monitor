import re
import time
import pytz
import datetime
import numpy as np
import pandas as pd

from ibapi.common import BarData
from ibapi.contract import ContractDetails

from scanner.scanner_connector_callback import ScannerConnectorCallBack

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.scanner_to_request_id import ScannerToRequestId
from constant.timeframe import Timeframe
from constant.request_id_prefix import RequestIdPrefix

from utils.datetime_util import get_trading_session_start_time_by_current_datetime
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
        
    def execute_scanner_data(self, req_id: int, rank: int, contract_details: ContractDetails) -> None:
        logger.log_debug_msg(f'Closest to halt scanner data, reqId: {req_id}')
        
        logger.log_debug_msg('Get scanner data for closest to halt')
        if rank == 0:
            if self.__start_time != None:
                logger.log_debug_msg(f'Closest to halt scanner refresh interval time: {time.time() - self.__start_time} seconds')
                
            self.__start_time = time.time()
            self.__closest_to_halt_ticker_list = []
            self.__closest_to_halt_contract_detail_list = []
            self.__previous_close_retrieval_counter = 0
            self.__previous_close_retrieval_size = 0

        if re.match('^[a-zA-Z]{1,4}$', contract_details.contract.symbol): 
            self.__closest_to_halt_ticker_list.append(contract_details.contract.symbol)
            self.__closest_to_halt_contract_detail_list.append(contract_details)
        else:
            logger.log_debug_msg(f'Exclude invalid ticker of {contract_details.contract.symbol} from closest to halt scanner result')      
        
    def execute_scanner_end(self, req_id: int, ticker_to_previous_close_dict: dict, scanner_connector) -> None:
        logger.log_debug_msg(f'Closest to halt scannerDataEnd, reqId: {req_id}, Result length: {len(self.__closest_to_halt_ticker_list)}, Result: {self.__closest_to_halt_ticker_list}')
        
        # Limit up down would only happen in normal trading hours
        if len(self.__closest_to_halt_ticker_list) == 0:
            logger.log_debug_msg('No items to retrieve in closest to halt scanner')
            return
        
        logger.log_debug_msg('Get candlesticks in closest to halt scanner end')
            
        self.__get_previous_close(ticker_to_previous_close_dict, scanner_connector)
        self.__get_one_minute_candle(scanner_connector)
        
    def execute_historical_data(self, req_id: int, bar: BarData, ticker_to_previous_close_dict: dict) -> None:
        logger.log_debug_msg(f'Closest to halt scanner get historical data, req_id: {req_id}')
        
        # Retrieve previous close
        if 0 <= req_id - RequestIdPrefix.CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1:
            logger.log_debug_msg(f'0 <= reqId - CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX of 1000 <= {RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1}')
            rank = req_id % RequestIdPrefix.CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX.value
            ticker = self.__closest_to_halt_ticker_list[rank]
            previous_close = bar.close
            ticker_to_previous_close_dict[ticker] = previous_close
            self.__previous_close_retrieval_counter += 1
            logger.log_debug_msg(f'{ticker} previous close: {previous_close}, rank: {rank}')
            logger.log_debug_msg(f'Previous close retrieval counter: {self.__previous_close_retrieval_counter}')
        
        # Retrieve minute candle
        if 0 <= req_id - RequestIdPrefix.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1:
            logger.log_debug_msg(f'0 <= reqId - CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX of 1100 <= {RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value- 1}')
            open = bar.open
            high = bar.high
            low = bar.low
            close = bar.close
            volume = bar.volume
            dt = bar.date.replace(" US/Eastern", "")
            logger.log_debug_msg(f'reqId: {req_id}, datetime: {dt}')

            self.__single_ticker_ohlcv_list.append([open, high, low, close, volume])
            self.__datetime_list.append(dt)    
    
    def execute_historical_data_end(self, req_id: int, ticker_to_previous_close_dict: dict) -> None:
        logger.log_debug_msg(f'Closest to halt scanner get historical data end, req_id: {req_id}')
        
        if 0 <= req_id - RequestIdPrefix.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1:
            rank = req_id - RequestIdPrefix.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX.value

            datetime_index = pd.DatetimeIndex(self.__datetime_list)
            ticker_to_indicator_column = pd.MultiIndex.from_product([[self.__closest_to_halt_ticker_list[rank]], [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.VOLUME]])
            single_ticker_candle_df = pd.DataFrame(self.__single_ticker_ohlcv_list, columns=ticker_to_indicator_column, index=datetime_index)
            self.__candle_df_list.append(single_ticker_candle_df)
            
            self.__single_ticker_ohlcv_list = []
            self.__datetime_list = []

        is_all_previous_close_retrieved = self.__previous_close_retrieval_counter == self.__previous_close_retrieval_size
        is_all_minute_candle_retrieved = len(self.__candle_df_list) == len(self.__closest_to_halt_ticker_list)

        # Check if all candle data is retrieved by last rank (req_id)
        if is_all_previous_close_retrieved and is_all_minute_candle_retrieved:
            concat_df_list = self.__candle_df_list
            all_ticker_one_minute_candle_df = pd.concat(concat_df_list, axis=1)
            
            close_df = all_ticker_one_minute_candle_df.loc[:, idx[:, Indicator.CLOSE]]
            vol_df = all_ticker_one_minute_candle_df.loc[:, idx[:, Indicator.VOLUME]].astype(float, errors = 'raise')
            close_pct_df = close_df.pct_change().mul(100).rename(columns={Indicator.CLOSE: CustomisedIndicator.CLOSE_CHANGE})
            previous_close_list = [[float(ticker_to_previous_close_dict[ticker]) for ticker in self.__closest_to_halt_ticker_list]]
            previous_close_df = pd.DataFrame(np.repeat(previous_close_list, 
                                                                len(all_ticker_one_minute_candle_df), 
                                                                axis=0),
                                                 columns=pd.MultiIndex.from_product([self.__closest_to_halt_ticker_list, [CustomisedIndicator.PREVIOUS_CLOSE]]),
                                                 index=all_ticker_one_minute_candle_df.index)
            
            previous_close_pct_df = (((close_df.sub(previous_close_df.values))
                                                .div(previous_close_df.values))
                                                .mul(100)).rename(columns={Indicator.CLOSE: CustomisedIndicator.PREVIOUS_CLOSE_CHANGE})
            vol_cumsum_df = vol_df.cumsum().rename(columns={Indicator.VOLUME: CustomisedIndicator.TOTAL_VOLUME})
            
            complete_df = pd.concat([all_ticker_one_minute_candle_df, 
                                     close_pct_df,
                                     previous_close_df, 
                                     previous_close_pct_df,
                                     vol_cumsum_df], axis=1)
            
            with pd.option_context('display.max_rows', None,
                       'display.max_columns', None,
                       'display.precision', 3,
                       ):
                    logger.log_debug_msg(f'Closest to halt completed dataframe: {complete_df}', with_log_file = False, with_std_out = False)
            
            for ticker in self.__closest_to_halt_ticker_list:
                read_ticker_str = " ".join(ticker)
                
                display_close = complete_df.loc[complete_df.index[-1], idx[ticker, Indicator.CLOSE]]
                display_volume = "{:,}".format(complete_df.loc[complete_df.index[-1], idx[ticker, Indicator.VOLUME]])
                display_total_volume = "{:,}".format(complete_df.loc[complete_df.index[-1], idx[ticker, CustomisedIndicator.TOTAL_VOLUME]])
                
                display_close_pct = round(complete_df.loc[complete_df.index[-1], idx[ticker, CustomisedIndicator.CLOSE_CHANGE]], 2)
                display_previous_close_pct = round(complete_df.loc[complete_df.index[-1], idx[ticker, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]], 2)
                
                pop_up_datetime = complete_df.index[-1]
                pop_up_hour = pd.to_datetime(pop_up_datetime).hour
                pop_up_minute = pd.to_datetime(pop_up_datetime).minute
                display_hour = ('0' + str(pop_up_hour)) if pop_up_hour < 10 else pop_up_hour
                display_minute = ('0' + str(pop_up_minute)) if pop_up_minute < 10 else pop_up_minute
                display_time_str = f'{display_hour}:{display_minute}'
                read_time_str = f'{pop_up_hour} {pop_up_minute}' if (pop_up_minute > 0) else f'{pop_up_hour} o clock' 
                
                logger.log_debug_msg(f'{read_ticker_str} closest to halt at {read_time_str}', with_speech = True, with_log_file = False, with_std_out = False)
                logger.log_debug_msg(f'{ticker} closest to halt at {display_time_str}, Close: {display_close}, Volume: {display_volume}, Total volume: {display_total_volume}, Close change: {display_close_pct}%, Previous close change: {display_previous_close_pct}%', with_std_out = True)
                    
    def __get_previous_close(self, ticker_to_previous_close_dict: dict, scanner_connector):
        logger.log_debug_msg('Get previous close for closest to halt ticker')
        
        # Calculate how many ticker's previous close should get
        for contract_rank, contract_detail in enumerate(self.__closest_to_halt_contract_detail_list):
            if contract_detail.contract.symbol not in ticker_to_previous_close_dict:
                self.__previous_close_retrieval_size += 1
        
        us_current_datetime = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
        trading_session_start_time = get_trading_session_start_time_by_current_datetime(us_current_datetime)
        
        pre_market_trading_hour_start_time = datetime.time(4, 0, 0)
        normal_trading_hour_start_time = datetime.time(9, 30, 0)
        
        if ((trading_session_start_time == pre_market_trading_hour_start_time) 
                or (trading_session_start_time == normal_trading_hour_start_time)):
            previous_close_duration_str = '2 D'
            self.__previous_close_retrieval_size = self.__previous_close_retrieval_size * 2
        else: 
            previous_close_duration_str = '1 D'
        
        logger.log_debug_msg(f'Previous close duration string: {previous_close_duration_str}, trading session start time: {trading_session_start_time}')
        logger.log_debug_msg(f'No. of previous close request for closest to halt: {self.__previous_close_retrieval_size}')
        
        # Send get previous close requests
        for contract_rank, contract_detail in enumerate(self.__closest_to_halt_contract_detail_list):
            if contract_detail.contract.symbol not in ticker_to_previous_close_dict:
                logger.log_debug_msg(f'Get {contract_detail.contract.symbol} previous close for closest to halt, rank: {contract_rank}')
                get_previous_close_req_id = RequestIdPrefix.CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX.value + contract_rank
                scanner_connector.reqHistoricalData(get_previous_close_req_id, contract_detail.contract, '', previous_close_duration_str, Timeframe.ONE_DAY.value, 'TRADES', 1, 1, False, [])
                
    def __get_one_minute_candle(self, scanner_connector):
        self.__single_ticker_ohlcv_list = []
        self.__datetime_list = []
        self.__candle_df_list = []
        
        #If no durationStr unit is specified, seconds is used.
        for rank, contract_detail in enumerate(self.__closest_to_halt_contract_detail_list):
            candle_req_id = RequestIdPrefix.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX.value + rank
            logger.log_debug_msg(f'Send get closest to halt ticker {contract_detail.contract.symbol} data at {datetime.datetime.now().astimezone(pytz.timezone("US/Eastern"))}')
            scanner_connector.reqHistoricalData(candle_req_id, contract_detail.contract, '', '120 S', Timeframe.ONE_MINUTE.value, 'TRADES', 0, 1, False, [])
