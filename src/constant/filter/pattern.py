from enum import Enum

class Pattern(str, Enum):
    UNUSUAL_VOLUME_RAMP_UP = 'UNUSUAL_VOLUME_RAMP_UP',
    INITIAL_POP_UP = 'INITIAL_POP_UP',
    CLOSEST_TO_NEW_HIGH_OR_NEW_HIGH = 'CLOSEST_TO_NEW_HIGH_OR_NEW_HIGH',
    