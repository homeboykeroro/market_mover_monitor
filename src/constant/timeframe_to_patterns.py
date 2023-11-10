from enum import Enum

from constant.filter.pattern import Pattern

class ScannerToTimeframePatterns(Enum):
    TOP_GAINER = [[Pattern.INITIAL_POP_UP, Pattern.UNUSUAL_VOLUME_RAMP_UP, Pattern.CLOSEST_TO_NEW_HIGH_OR_NEW_HIGH]]
