import time

from pandas.core.frame import DataFrame
import pandas as pd

from pattern.pattern_analyser import PatternAnalyser

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour

from utils.logger import Logger
from utils.dataframe_util import derive_idx_df

idx = pd.IndexSlice
logger = Logger()

class ClosestToNewHighOrNewHigh(PatternAnalyser):
    MAX_BELOW_HIGHEST_PCT = 5
    
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        logger.log_debug_msg('Closest to new high or new high scan')
        start_time = time.time()
        
        # close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE]].rename(columns={Indicator.CLOSE: RuntimeIndicator.COMPARE})
        # high_df = self.__historical_data_df.loc[:, idx[:, Indicator.HIGH]].rename(columns={Indicator.HIGH: RuntimeIndicator.COMPARE})
        # previous_close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
        # close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
        
        # max_close_df = self.__historical_data_df.max()
        # max_high_df = self.__historical_data_df.max()
        
        # max_close_first_occurrence_idx_df = close_df.rename(columns={Indicator.CLOSE: RuntimeIndicator.COMPARE}).idxmax()
        # max_high_first_occurrence_idx_df = high_df.rename(columns={Indicator.HIGH: RuntimeIndicator.COMPARE}).idxmax()
        # timeframe_idx_df = derive_idx_df(close_df, numeric_idx = False).rename(columns={RuntimeIndicator.INDEX: RuntimeIndicator.COMPARE})
        # timeframe_idx_df.eq(max_close_first_occurrence_idx_df, axis=1)
        # last_row_timeframe_idx = timeframe_idx_df.iloc[-1].name
        
        
        # last_row_timeframe_idx = len(close_df)
        
        
        # close_highest_close_pct_diff_df = ((max_close_df - close_df) / close_df) * 100
        # close_highest_high_diff_pct_df = ((max_high_df - close_df) / close_df) * 100
        
        # if 0 <= close_highest_close_pct_diff_df <= self.MAX_BELOW_HIGHEST_PCT:
            
            
        
        
        
        