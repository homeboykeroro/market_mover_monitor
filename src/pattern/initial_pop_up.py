import time
import pandas as pd
from pandas.core.frame import DataFrame

from pattern.pattern_analyser import PatternAnalyser

from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator

from utils.dataframe_util import derive_idx_df
from utils.logger import Logger

idx = pd.IndexSlice
logger = Logger()

class InitialPopUp(PatternAnalyser):
    MIN_CLOSE_PCT = 6
    MIN_PREVIOUS_CLOSE_PCT = 15
    MAX_RAMP_OCCURRENCE = 5
    NOTIFY_PERIOD = 2
        
    def __init__(self, historical_data_df: DataFrame):
        self.__historical_data_df = historical_data_df

    def analyse(self) -> None:
        logger.log_debug_msg('Initial pop up scan', with_speech = False)
        start_time = time.time()

        close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
        previous_close_pct_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]].rename(columns={CustomisedIndicator.PREVIOUS_CLOSE_CHANGE: RuntimeIndicator.COMPARE})

        candle_close_pct_boolean_df = (close_pct_df >= self.MIN_CLOSE_PCT)
        previous_close_pct_boolean_df = (previous_close_pct_df >= self.MIN_PREVIOUS_CLOSE_PCT)

        ramp_up_boolean_df = (candle_close_pct_boolean_df) & (previous_close_pct_boolean_df)
        ramp_up_occurrence_df = ((ramp_up_boolean_df.cumsum()
                                                    .where(ramp_up_boolean_df.values)))
        result_boolean_df = (ramp_up_occurrence_df.iloc[-self.NOTIFY_PERIOD:] <= self.MAX_RAMP_OCCURRENCE)
        new_gainer_result_series = result_boolean_df.any()   
        new_gainer_ticker_list = new_gainer_result_series.index[new_gainer_result_series].get_level_values(0).tolist()

        if len(new_gainer_ticker_list) > 0:
            datetime_idx_df = derive_idx_df(ramp_up_occurrence_df, numeric_idx=False)
            close_df = self.__historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
            previous_close_df = self.__historical_data_df.loc[:, idx[:, CustomisedIndicator.PREVIOUS_CLOSE_CHANGE]]
            volume_df = self.__historical_data_df.loc[:, idx[:, Indicator.VOLUME]]
            
            pop_up_close_df = close_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]
            pop_up_close_pct_df = close_pct_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]
            pop_up_previous_close_pct_df = previous_close_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]
            pop_up_volume_df = volume_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]
            pop_up_datetime_idx_df = datetime_idx_df.where(ramp_up_boolean_df.values).ffill().iloc[[-1]]

            for ticker in new_gainer_ticker_list:
                display_close = pop_up_close_df.loc[:, ticker].iat[0, 0]
                display_volume = pop_up_volume_df.loc[:, ticker].iat[0, 0]
                display_close_pct = round(pop_up_close_pct_df.loc[:, ticker].iat[0, 0], 2)
                display_previous_close_pct = round(pop_up_previous_close_pct_df.loc[:, ticker].iat[0, 0], 2)

                pop_up_datetime = pop_up_datetime_idx_df.loc[:, ticker].iat[0, 0]
                pop_up_hour = pd.to_datetime(pop_up_datetime).hour
                pop_up_minute = pd.to_datetime(pop_up_datetime).minute
                display_hour = ('0' + str(pop_up_hour)) if pop_up_hour < 10 else pop_up_hour
                display_minute = ('0' + str(pop_up_minute)) if pop_up_minute < 10 else pop_up_minute
                display_time_str = f'{display_hour}:{display_minute}'
                read_time_str = f'{pop_up_hour} {pop_up_minute}' if (pop_up_minute > 0) else f'{pop_up_hour} o clock' 
                read_ticker_str = " ".join(ticker)

                logger.log_debug_msg(f'{read_ticker_str} is popping up {display_previous_close_pct} percent at {read_time_str}', with_std_out = False)
                logger.log_debug_msg(f'{ticker} is popping up {display_previous_close_pct}%, Time: {display_time_str}, Close: ${display_close}, Change: {display_close_pct}%, Volume: {display_volume}', with_speech = False)

        logger.log_debug_msg(f'Initial pop up analysis time: {time.time() - start_time} seconds', with_speech = False)