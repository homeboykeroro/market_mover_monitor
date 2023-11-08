from enum import Enum

class Timeframe(str, Enum):
    ONE_MINUTE = '1 min'
    ONE_DAY = '2 D'
    FIVE_MINUTE = '5 mins'