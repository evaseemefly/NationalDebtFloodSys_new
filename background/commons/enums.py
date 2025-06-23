from enum import unique, Enum


@unique
class TYGroupTypeEnum(Enum):
    """
        台风路径类型枚举
    """
    CENTER = 4101
    SLOW = 4102
    FAST = 4103
    RIGHT = 4104
    LEFT = 4105
    NONE = -1


@unique
class FloodLevelEnum(Enum):
    GTE100 = 4201
    GTE150 = 4202
    GTE200 = 4203
