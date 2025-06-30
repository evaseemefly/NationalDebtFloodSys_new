from typing import List

from pydantic import BaseModel, ConfigDict

from common.enums import TyphoonGroupEnum


class StionInfoSchema(BaseModel):
    station_name: str
    station_code: str
    lat: float
    lon: float
    desc: str

    # class Config:
    #     orm_mode = True  # 旧版本

    model_config = ConfigDict(from_attributes=True)  # 新版本


class StationSurgeSchema(BaseModel):
    station_code: str
    forecast_ts: int
    issue_time: int
    surge: float

    model_config = ConfigDict(from_attributes=True)  # 新版本


class StationTideSchema(BaseModel):
    """
        StationAstronomicTide
    """
    station_code: str
    ts: int
    tide: float

    model_config = ConfigDict(from_attributes=True)  # 新版本


class StationGroupSurgeSchema(BaseModel):
    group_type: TyphoonGroupEnum
    surge_list: List[StationSurgeSchema]
