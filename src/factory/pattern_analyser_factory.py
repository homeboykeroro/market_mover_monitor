from pandas.core.frame import DataFrame

from constant.filter.pattern import Pattern

from pattern.pattern_analyser import PatternAnalyser
from pattern.unusual_volume_ramp_up import UnusualVolumeRampUp
from pattern.initial_pop_up import InitialPopUp
from pattern.closest_to_new_high_or_new_high import ClosestToNewHighOrNewHigh

class PatternAnalyserFactory:
    @staticmethod
    def get_pattern_analyser(analyser: str, historical_data_df: DataFrame) -> PatternAnalyser:
        if Pattern.INITIAL_POP_UP == analyser:
            return InitialPopUp(historical_data_df)
        elif Pattern.UNUSUAL_VOLUME_RAMP_UP == analyser:
            return UnusualVolumeRampUp(historical_data_df)
        elif Pattern.CLOSEST_TO_NEW_HIGH_OR_NEW_HIGH == analyser:
            return ClosestToNewHighOrNewHigh(historical_data_df)
        else:
            raise Exception(f'Pattern analyser of {analyser} not found')