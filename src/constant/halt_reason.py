from enum import Enum

class HaltReason(str, Enum):
    T1 = 'Pending Release of Material News'
    T2 = 'News Released (Through a Regulation FD Compliant Methods)'
    # 5-10% for Tier 1 stocks and 10-20% for Tier 2 stocks.
    T5 = 'Trade Pause Due to 10% or more price move in the security in a five-minute period'
    LUDP = 'Volatility Trade Pause'
    LUDS = 'Volatility Trading Pause - Straddle Condition'

    def has_key(key):
        return key in HaltReason.__members__