from datetime import datetime
from typing import List, ForwardRef, Any, Optional, Union

from pydantic import BaseModel, Field


# 请求数据模型
# class TyphoonPathSchema(BaseModel):
#     forecastDt: datetime = Field(..., description="预报时间")
#     lat: float = Field(..., ge=-90, le=90, description="纬度")
#     lon: float = Field(..., ge=-180, le=180, description="经度")
#     bp: float = Field(..., gt=0, description="气压")
#     isForecast: bool = Field(..., description="是否为预报数据")
#     tyType: Optional[str] = Field(None, description="台风类型")
#
#     class Config:
#         json_encoders = {
#             datetime: lambda v: v.isoformat()
#         }
#         schema_extra = {
#             "example": {
#                 "forecastDt": "2024-04-28T10:30:00",
#                 "lat": 23.5,
#                 "lon": 121.5,
#                 "bp": 998.5,
#                 "isForecast": True,
#                 "tyType": "TY"
#             }
#         }
#

class TyphoonDistGroupSchema(BaseModel):
    tyCode: str
    timestamp: int

    class Config:
        orm_mode = True  # 旧版本


class TyphoonPointSchema(BaseModel):
    forecastDt: datetime
    lat: float
    lon: float
    bp: int
    isForecast: bool
    tyType: str


class TyphoonPathSchema(BaseModel):
    # tyId: str
    params: List[TyphoonPointSchema]


class TyphoonPathDetail(BaseModel):
    tyCode: str
    tyNameCh: str
    tyNameEn: str
    timeStamp: int


class TyphoonPathComplexSchema(BaseModel):
    tyCode: str
    issueTs: int
    groupType: str
    taskId: Optional[int]
    tyPathList: Optional[List[TyphoonPointSchema]]
