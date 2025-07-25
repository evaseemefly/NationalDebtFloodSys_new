from enum import unique, Enum


@unique
class NullEnum(Enum):
    NULL = -1


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


@unique
class TyphoonForecastInstitutionEnum(Enum):
    """
        台风预报机构代码
    """
    CMA = 201


class TaskStatusEnum(Enum):
    """
        任务枚举类
    """
    pending = 4002
    in_progress = 4003
    completed = 4004
    failed = 4005


class TyphoonGroupEnum(Enum):
    """
        台风集合路径枚举
    """
    GROUP_CENTER = 4101
    GROUP_SLOW = 4102
    GROUP_FAST = 4103
    GROUP_RIGHT = 4104
    GROUP_LEFT = 4105

    @classmethod
    def _missing_(cls, value):
        return cls.GROUP_CENTER  # 设定默认值为 CENTER


@unique
class RasterFileType(Enum):
    """
        场文件类型
    """
    NETCDF = 6101
    """netcdf"""
    GEOTIFF = 6102
    """geotiff"""

    @classmethod
    def _missing_(cls, value):
        return cls.GEOTIFF  # 设定默认值为 GEOTIFF


@unique
class ElementTypeEnum(Enum):
    """
        预报要素种类枚举
    """
    SURGE = 6002
    """预报-逐时增水"""
    SURGE_MAX = 6003
    """预报-最大增水"""
    # TODO:[-] 24-12-03
    SURGE_MERGE = 6004
    """预报-合并后的逐时增水"""
