import re
import time
import pytz
import datetime
import numpy as np
import pandas as pd

from ibapi.common import BarData
from ibapi.contract import ContractDetails

from factory.pattern_analyser_factory import PatternAnalyserFactory
from scanner.scanner_connector_callback import ScannerConnectorCallBack

from constant.candle.candle_colour import CandleColour
from constant.filter.pattern import Pattern
from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.timeframe import Timeframe
from constant.request_id_prefix import RequestIdPrefix
from constant.scanner_to_timeframes import ScannerToTimeframes
from constant.timeframe_to_patterns import ScannerToTimeframePatterns

from utils.datetime_util import get_trading_session_start_time_by_current_datetime
from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class TopGainerScannerEnd(ScannerConnectorCallBack):
    def __init__(self):
        self.__start_time = None
        self.__top_gainer_ticker_list = []
        self.__top_gainer_contract_detail_list = []
        self.__previous_close_retrieval_counter = 0
        self.__previous_close_retrieval_size = 0
        
    def execute_scanner_data(self, req_id: int, rank: int, contract_details: ContractDetails) -> None:
        logger.log_debug_msg(f'Top gainer scanner data, reqId: {req_id}')
        
        logger.log_debug_msg('Get scanner data for top gainer')
        if rank == 0:
            if self.__start_time != None:
                logger.log_debug_msg(f'Top gainer scanner refresh interval time: {time.time() - self.__start_time} seconds')
            
            self.__start_time = time.time()
            self.__top_gainer_ticker_list = []
            self.__top_gainer_contract_detail_list = []
            self.__previous_close_retrieval_counter = 0
            self.__previous_close_retrieval_size = 0

        if re.match('^[a-zA-Z]{1,4}$', contract_details.contract.symbol): 
            self.__top_gainer_ticker_list.append(contract_details.contract.symbol)
            self.__top_gainer_contract_detail_list.append(contract_details)
        else:
            logger.log_debug_msg(f'Exclude invalid ticker of {contract_details.contract.symbol} from top gainer scanner result')      
        
    def execute_scanner_end(self, req_id: int, ticker_to_previous_close_dict: dict, scanner_connector) -> None:
        logger.log_debug_msg(f'Top gainer scannerDataEnd, reqId: {req_id}, Result length: {len(self.__top_gainer_ticker_list)}, Result: {self.__top_gainer_ticker_list}')
        
        if len(self.__top_gainer_ticker_list) == 0:
            logger.log_debug_msg('No items to retrieve in top gainer scanner')
            return
        
        logger.log_debug_msg('Get candlesticks in top gainer scanner end')

        self.__get_previous_close(ticker_to_previous_close_dict, scanner_connector)
        self.__get_timeframe_candle(scanner_connector)
        
    def execute_historical_data(self, req_id: int, bar: BarData, ticker_to_previous_close_dict: dict) -> None:
        logger.log_debug_msg(f'Top gainer scanner get historical data, req_id: {req_id}')
        
        # Retrieve previous close
        if 0 <= req_id - RequestIdPrefix.TOP_GAINER_DAY_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1:
            logger.log_debug_msg(f'0 <= reqId - TOP_GAINER_DAY_CANDLE_REQ_ID_PREFIX of 100 <= {RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1}')
            rank = req_id % RequestIdPrefix.TOP_GAINER_DAY_CANDLE_REQ_ID_PREFIX.value
            ticker = self.__top_gainer_ticker_list[rank]
            previous_close = bar.close
            ticker_to_previous_close_dict[ticker] = previous_close
            self.__previous_close_retrieval_counter += 1
            logger.log_debug_msg(f'{ticker} previous close: {previous_close}, rank: {rank}')
            logger.log_debug_msg(f'Previous close retrieval counter: {self.__previous_close_retrieval_counter}')
        
        # Retrieve minute candle
        if RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value <= req_id <= ((RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value * len(ScannerToTimeframes.TOP_GAINER.value)) + RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1):
            logger.log_debug_msg(f'{RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value} <= reqId <= {((RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value * len(ScannerToTimeframes.TOP_GAINER.value)) + RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1)}')
            open = bar.open
            high = bar.high
            low = bar.low
            close = bar.close
            volume = bar.volume
            dt = bar.date.replace(" US/Eastern", "")
            logger.log_debug_msg(f'reqId: {req_id}, datetime: {dt}')

            timeframe_idx = (req_id // RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value) - 1
            self.__timeframe_idx_to_single_ticker_ohlcv_list_dict[timeframe_idx].append([open, high, low, close, volume])
            self.__timeframe_idx_to_datetime_list_dict[timeframe_idx].append(dt)
            
    def execute_historical_data_end(self, req_id: int, ticker_to_previous_close_dict: dict) -> None:
        logger.log_debug_msg(f'Top gainer scanner get historical data end, req_id: {req_id}')
        
        if RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value <= req_id <= ((RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value * len(ScannerToTimeframes.TOP_GAINER.value)) + RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1):
            logger.log_debug_msg(f'{RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value} <= reqId <= {((RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value * len(ScannerToTimeframes.TOP_GAINER.value)) + RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1)}')
            timeframe_idx = (req_id // RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value) - 1
            rank = req_id - (RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value * (timeframe_idx + 1))

            ohlcv_list = self.__timeframe_idx_to_single_ticker_ohlcv_list_dict[timeframe_idx]
            datetime_list = self.__timeframe_idx_to_datetime_list_dict[timeframe_idx]
            datetime_index = pd.DatetimeIndex(datetime_list)
            ticker_to_indicator_column = pd.MultiIndex.from_product([[self.__top_gainer_ticker_list[rank]], [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.VOLUME]])
            single_ticker_candle_df = pd.DataFrame(ohlcv_list, columns=ticker_to_indicator_column, index=datetime_index)
            self.__timeframe_idx_to_candle_df_list_dict[timeframe_idx].append(single_ticker_candle_df)

            self.__timeframe_idx_to_single_ticker_ohlcv_list_dict[timeframe_idx] = []
            self.__timeframe_idx_to_datetime_list_dict[timeframe_idx] = []

        is_all_previous_close_retrieved = self.__previous_close_retrieval_counter == self.__previous_close_retrieval_size
        
        if not self.__timeframe_idx_to_candle_df_list_dict:
            is_all_minute_candle_retrieved = False
        else:
            is_all_minute_candle_retrieved = all([len(candle_df_list) == len(self.__top_gainer_ticker_list) for candle_df_list in self.__timeframe_idx_to_candle_df_list_dict.values()])

        # Check if all candle data is retrieved by last rank (req_id)
        if is_all_previous_close_retrieved and is_all_minute_candle_retrieved:
            for timeframe_idx, timeframe in enumerate(ScannerToTimeframes.TOP_GAINER.value):
                logger.log_debug_msg(f'Append customised indicators for {timeframe.name} dataframe')
                concat_df_list = self.__timeframe_idx_to_candle_df_list_dict[timeframe_idx]
                all_ticker_individual_timeframe_candle_df = pd.concat(concat_df_list, axis=1)
                previous_close_list = [[float(ticker_to_previous_close_dict[ticker]) for ticker in self.__top_gainer_ticker_list]]
                previous_close_df = pd.DataFrame(np.repeat(previous_close_list, 
                                                                len(all_ticker_individual_timeframe_candle_df), 
                                                                axis=0),
                                                 columns=pd.MultiIndex.from_product([self.__top_gainer_ticker_list, [CustomisedIndicator.PREVIOUS_CLOSE]]),
                                                 index=all_ticker_individual_timeframe_candle_df.index)
                
                open_df = all_ticker_individual_timeframe_candle_df.loc[:, idx[:, Indicator.OPEN]].rename(columns={Indicator.OPEN: RuntimeIndicator.COMPARE})
                high_df = all_ticker_individual_timeframe_candle_df.loc[:, idx[:, Indicator.HIGH]]
                low_df = all_ticker_individual_timeframe_candle_df.loc[:, idx[:, Indicator.LOW]]
                close_df = all_ticker_individual_timeframe_candle_df.loc[:, idx[:, Indicator.CLOSE]].rename(columns={Indicator.CLOSE: RuntimeIndicator.COMPARE})
                vol_df = all_ticker_individual_timeframe_candle_df.loc[:, idx[:, Indicator.VOLUME]].astype(float, errors = 'raise')

                previous_close_pct_df = (((close_df.sub(previous_close_df.values))
                                                   .div(previous_close_df.values))
                                                   .mul(100)).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.PREVIOUS_CLOSE_CHANGE})
                
                close_pct_df = close_df.pct_change().mul(100).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.CLOSE_CHANGE})
                close_pct_df.iloc[[0]] = previous_close_pct_df.iloc[[0]]
                
                green_candle_df = (close_df > open_df).replace({True: CandleColour.GREEN, False: np.nan})
                red_candle_df = (close_df < open_df).replace({True: CandleColour.RED, False: np.nan})
                flat_candle_df = (close_df == open_df).replace({True: CandleColour.GREY, False: np.nan})
                colour_df = ((green_candle_df.fillna(red_candle_df))
                                             .fillna(flat_candle_df)
                                             .rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.CANDLE_COLOUR}))
                
                vol_cumsum_df = vol_df.cumsum().rename(columns={Indicator.VOLUME: CustomisedIndicator.TOTAL_VOLUME})
                vol_20_ma_df = vol_df.rolling(window=20, min_periods=1).mean().rename(columns={Indicator.VOLUME: CustomisedIndicator.MA_20_VOLUME})
                vol_50_ma_df = vol_df.rolling(window=50, min_periods=1).mean().rename(columns={Indicator.VOLUME: CustomisedIndicator.MA_50_VOLUME})

                typical_price_df = ((high_df.add(low_df.values)
                                            .add(close_df.values))
                                            .div(3))
                tpv_cumsum_df = typical_price_df.mul(vol_df.values).cumsum()
                vwap_df = tpv_cumsum_df.div(vol_cumsum_df.values).rename(columns={Indicator.HIGH: CustomisedIndicator.VWAP})

                close_above_open_boolean_df = (close_df > open_df)
                high_low_diff_df = high_df.sub(low_df.values)
                close_above_open_upper_body_df = close_df.where(close_above_open_boolean_df.values)
                open_above_close_upper_body_df = open_df.where((~close_above_open_boolean_df).values)
                upper_body_df = close_above_open_upper_body_df.fillna(open_above_close_upper_body_df)

                close_above_open_lower_body_df = open_df.where(close_above_open_boolean_df.values)
                open_above_close_lower_body_df = close_df.where((~close_above_open_boolean_df).values)
                lower_body_df = close_above_open_lower_body_df.fillna(open_above_close_lower_body_df)

                body_diff_df = upper_body_df.sub(lower_body_df.values)
                marubozu_ratio_df = (body_diff_df.div(high_low_diff_df.values)).mul(100).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.MARUBOZU_RATIO})

                complete_df = pd.concat([all_ticker_individual_timeframe_candle_df, 
                                    close_pct_df,
                                    previous_close_df,
                                    previous_close_pct_df,
                                    colour_df,
                                    marubozu_ratio_df,
                                    vwap_df, 
                                    vol_20_ma_df,
                                    vol_50_ma_df,
                                    vol_cumsum_df], axis=1)
                
                with pd.option_context('display.max_rows', None,
                       'display.max_columns', None,
                       'display.precision', 3,
                       ):
                    logger.log_debug_msg(f'Top gainer completed dataframe: {complete_df}', with_log_file = False, with_std_out = False)

                for pattern in ScannerToTimeframePatterns.TOP_GAINER.value[timeframe_idx]:
                    logger.log_debug_msg(f'Scan {pattern.name} in {timeframe.name}')
                    pattern_analyzer = PatternAnalyserFactory.get_pattern_analyser(pattern.value, complete_df)
                    pattern_analyzer.analyse()
    
    def __get_previous_close(self, ticker_to_previous_close_dict: dict, scanner_connector):
        logger.log_debug_msg('Get previous close for top gainer ticker')
        
        # Calculate how many ticker's previous close should get
        for contract_rank, contract_detail in enumerate(self.__top_gainer_contract_detail_list):
            if contract_detail.contract.symbol not in ticker_to_previous_close_dict:
                self.__previous_close_retrieval_size += 1
        
        us_current_datetime = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
        trading_session_start_time = get_trading_session_start_time_by_current_datetime(us_current_datetime)
        
        pre_market_trading_hour_start_time = datetime.time(4, 0, 0)
        normal_trading_hour_start_time = datetime.time(9, 30, 0)
        
        if ((trading_session_start_time == pre_market_trading_hour_start_time) 
                or (trading_session_start_time == normal_trading_hour_start_time)):
            self.__previous_close_retrieval_size = self.__previous_close_retrieval_size * 2
            previous_close_duration_str = '2 D'
        else: 
            previous_close_duration_str = '1 D'
        
        logger.log_debug_msg(f'Previous close duration string: {previous_close_duration_str}, trading session start time: {trading_session_start_time}')
        logger.log_debug_msg(f'No. of previous close request for top gainer: {self.__previous_close_retrieval_size}')
        
        # Send get previous close requests
        for contract_rank, contract_detail in enumerate(self.__top_gainer_contract_detail_list):
            if contract_detail.contract.symbol not in ticker_to_previous_close_dict:
                logger.log_debug_msg(f'Get {contract_detail.contract.symbol} previous close for top gainer, rank: {contract_rank}')
                get_previous_close_req_id = RequestIdPrefix.TOP_GAINER_DAY_CANDLE_REQ_ID_PREFIX.value + contract_rank
                scanner_connector.reqHistoricalData(get_previous_close_req_id, contract_detail.contract, '', previous_close_duration_str, Timeframe.ONE_DAY.value, 'TRADES', 1, 1, False, [])
                
    def __get_timeframe_candle(self, scanner_connector):
            us_current_datetime = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
            candle_start_time = get_trading_session_start_time_by_current_datetime(us_current_datetime)
            candle_start_datetime = us_current_datetime.replace(hour=candle_start_time.hour, minute=candle_start_time.minute, second=candle_start_time.second)
            timeframe_interval = (us_current_datetime - candle_start_datetime).total_seconds()
            truncate_seconds = timeframe_interval % 60
            timeframe_interval = timeframe_interval - truncate_seconds
            logger.log_debug_msg(f'US current datetime: {us_current_datetime}, Candle start time: {candle_start_datetime}, Timeframe interval: {timeframe_interval} seconds')

            # Minimum timeframe interval is less than 60 seconds 
            if timeframe_interval < 60:
                logger.log_debug_msg('Timeframe interval less than 60 seconds')
                return
            
            timeframe_interval = str(int(timeframe_interval))
            
            self.__timeframe_idx_to_single_ticker_ohlcv_list_dict = {}
            self.__timeframe_idx_to_datetime_list_dict = {}
            self.__timeframe_idx_to_candle_df_list_dict = {}
            
            for timeframe_idx, timeframe in enumerate(ScannerToTimeframes.TOP_GAINER.value):
                self.__timeframe_idx_to_single_ticker_ohlcv_list_dict[timeframe_idx] = []
                self.__timeframe_idx_to_datetime_list_dict[timeframe_idx] = []
                self.__timeframe_idx_to_candle_df_list_dict[timeframe_idx] = []

                #If no durationStr unit is specified, seconds is used.
                for rank, contract_detail in enumerate(self.__top_gainer_contract_detail_list):
                    candle_req_id = ((timeframe_idx + 1) * RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value) + rank
                    logger.log_debug_msg(f'Get {contract_detail.contract.symbol} {ScannerToTimeframes.TOP_GAINER.value[timeframe_idx].name} minute candles')
                    scanner_connector.reqHistoricalData(candle_req_id, contract_detail.contract, '', timeframe_interval, timeframe.value, 'TRADES', 0, 1, False, [])
                