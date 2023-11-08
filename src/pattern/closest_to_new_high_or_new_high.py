import time

from pandas.core.frame import DataFrame
import pandas as pd

from pattern.pattern_analyser import PatternAnalyser

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour

from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class ClosestToNewHighOrNewHigh(PatternAnalyser):
    MAX_BELOW_HIGHEST_PCT = 5
    
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        logger.log_debug_msg('Closest to new high or new high scan', with_speech = False)
        start_time = time.time()
        
        # close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE]].rename(columns={Indicator.CLOSE: RuntimeIndicator.COMPARE})
        # high_df = self.__historical_data_df.loc[:, idx[:, Indicator.HIGH]].rename(columns={Indicator.HIGH: RuntimeIndicator.COMPARE})
        # previous_close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
        # close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
        
        # max_close_df = self.__historical_data_df.max()
        # max_high_df = self.__historical_data_df.max()
        
        # close_highest_close_pct_diff_df = ((max_close_df - close_df) / close_df) * 100
        # close_highest_high_diff_pct_df = ((max_high_df - close_df) / close_df) * 100
        
        # if 0 <= close_highest_close_pct_diff_df <= self.MAX_BELOW_HIGHEST_PCT:
            
        
        
        
        