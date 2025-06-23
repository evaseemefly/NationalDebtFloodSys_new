import enum


class TaskStatus(enum.Enum):
    """
        任务枚举类
    """
    pending = 4001
    in_progress = 4002
    completed = 4003
    failed = 4004


@enum.unique
class TyphoonForecastSourceEnum(enum.Enum):
    """
        台风预报机构
    """
    CMA = 201


@enum.unique
class TyphoonOffsetEnum(enum.Enum):
    """
        台风偏移枚举
    """
    CENTER = 5010
    FAST = 5011
    SLOW = 5012
    RIGHT = 5013
    LEFT = 5014
