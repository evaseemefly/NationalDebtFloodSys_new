from datetime import datetime
from typing import List, ForwardRef, Any, Optional, Union

from pydantic import BaseModel, Field

from commons.enums import TYGroupTypeEnum


class TyphoonDistGroupSchema(BaseModel):
    tyCode: str
    timestamp: int

    class Config:
        orm_mode = True  # 旧版本


class TyphoonPointSchema(BaseModel):
    forecastDt: datetime
    lat: float
    lon: float
    bp: float
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
    # taskId: Optional[int]
    tyPathList: Optional[List[TyphoonPointSchema]]


class TyphoonDetailInfoSchema(BaseModel):
    """
        只包含提交的台风基础信息(名称及编号等)
    """
    timeStamp: int
    tyCode: int
    tyNameCh: str
    tyNameEn: str


class TyphoonPathComplexDetailSchema(BaseModel):
    """
       接收前台提交 filter 后的请求体
       {
        tyDetail:{
                    "tyCode": 2504,
                    "tyNameCh": "丹娜丝",
                    "tyNameEn": "DANAS",
                    "timeStamp": 1752461190841
                }
        tyPathList: [{
                    "forecastDt": "2025-07-06T15:00:00.000Z",
                    "lat": 23.3,
                    "lon": 120,
                    "bp": 950,
                    "isForecast": false,
                    "tyType": "STY"
                }]
       }

    """
    tyDetail: TyphoonDetailInfoSchema
    tyPathList: List[TyphoonPointSchema]


group_path_switch = {
    'center': TYGroupTypeEnum.CENTER,
    'fast': TYGroupTypeEnum.FAST,
    'left': TYGroupTypeEnum.LEFT,
    'right': TYGroupTypeEnum.RIGHT,
    'slow': TYGroupTypeEnum.SLOW
}


def get_group_type(val: str) -> TYGroupTypeEnum:
    return group_path_switch.get(val)


class StationSurgeFileSchema(BaseModel):
    ty_code: str
    issue_ts: int
    forecast_ts: int
    group_id: int
    file_name: str

    # group_path_stamp: str

    @property
    def group_path_stamp(self) -> str:
        # {AttributeError}AttributeError("type object 'StationSurgeFileSchema' has no attribute 'file_name'")
        path_stamp: str = self.file_name.split('.')[0].split('_')[-1]
        return path_stamp

    @property
    def group_path_type(self) -> TYGroupTypeEnum:
        path_type: TYGroupTypeEnum = get_group_type(self.group_path_stamp())
        return path_type
