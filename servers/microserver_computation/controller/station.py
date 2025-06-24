from datetime import datetime, timedelta
from typing import List, Dict, Optional

import arrow
import requests
from lxml import etree
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from pydantic import ValidationError

from common.default import DEFAULT_CODE
from common.enums import TyphoonGroupEnum
from common.exceptions import NoExistTargetTyphoon
from dao.stations import StationDao
from schema.common import ResponseModel
from schema.stations import StionInfoSchema, StationGroupSurgeSchema
from schema.task import TyGroupTaskSchema
from schema.typhoon import TyphoonPathSchema, TyphoonPathComplexSchema, TyphoonDistGroupSchema

app = APIRouter()


def get_station_dao():
    return StationDao()


@app.get('/all/list',
         summary="获取所有站点信息", response_model=List[StionInfoSchema])
async def get(station_dao: StationDao = Depends(get_station_dao)):
    """
        获取所有站点信息
    :param params:
    :return:
    """
    try:
        #
        # station_dao = StationDao()
        res = station_dao.get_all_stations()
        return res

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@app.get('/surge/group', summary="获取所有站点信息", response_model=List[StationGroupSurgeSchema])
async def get_station_group_surgelist(station_code: str, ty_code: str, group: int,
                                      station_dao: StationDao = Depends(get_station_dao)):
    """
        获取对应站点的增水集合
    @return:
    """
    group_type: TyphoonGroupEnum = TyphoonGroupEnum(group)
    # station_dao = StationDao()
    station_groups = station_dao.get_station_groupsurge(station_code, ty_code, group_type)
    return station_groups
