from datetime import datetime
from typing import List, ForwardRef, Any, Optional, Union

from pydantic import BaseModel, Field


# 使用 ForwardRef 来解决循环引用问题
# TyRealDataChildrenSchema = ForwardRef('TyRealDataChildrenSchema')


class TyRealDataChildrenSchema(BaseModel):
    lat: Union[float, int]
    lon: Union[float, int]
    bp: int
    ts: int
    ty_type: str
    forecast_dt: datetime

    # forecast_ty_path_list: List['TyRealDataChildrenSchema'] = []

    class Config:
        arbitrary_types_allowed = True
        orm_mode = True


# 解析 ForwardRef
# TyRealDataChildrenSchema.update_forward_refs()


class TyRealDataMidSchema(BaseModel):
    """
        对应 TyForecastRealDataMidModel
    """
    lat: Union[float, int]
    lon: Union[float, int]
    bp: int
    ts: int
    ty_type: str
    forecast_dt: datetime

    forecast_ty_path_list: List[TyRealDataChildrenSchema] = []

    class Config:
        orm_mode = True


class TyPathSchema(BaseModel):
    """
        对应 TyPathMidModel
    """
    ty_id: int
    ty_code: int
    ty_name_en: str
    ty_name_ch: str
    ty_path_list: List[TyRealDataMidSchema]

    class Config:
        orm_mode = True
