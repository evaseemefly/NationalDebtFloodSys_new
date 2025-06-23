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
from core.jobs import JobGenerateTyphoonPathFile
from dao.jobs import TaskDao
from dao.typhoon import TyphoonDao
from models.mid_models import TyDetailMidModel, TyPathMidModel
from schema.common import ResponseModel
from schema.task import TyGroupTaskSchema
from schema.typhoon import TyphoonPathSchema, TyphoonPathComplexSchema, TyphoonDistGroupSchema

app = APIRouter()


@app.get('/typhoon/group/dist',
         summary="获取指定台风的不同的集合", response_model=List[TyphoonDistGroupSchema])
async def get(ty_code: str):
    """
        根据 ty_code 获取对应台风的路径(实况|预报)
    :param params:
    :return:
    """
    try:
        #
        typhoon_dao = TyphoonDao()
        res = typhoon_dao.get_group_list(ty_code)
        return res

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@app.get('/typhoon/grouppath/list',
         summary="获取指定任务创建的所有集合路径集合", response_model=List[TyphoonPathComplexSchema])
async def get(ty_code: str, issue_ts):
    """

    @param ty_code:
    @param issue_ts:
    @return:
    """
    try:
        #

        typhoon_dao = TyphoonDao()
        res = typhoon_dao.get_grouppath_list(ty_code, issue_ts)
        return res

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@app.get('/typhoon/grouppath/detail/list',
         summary="根据台风编号以及发布时间戳获取5中集合路径的信息", response_model=List[TyphoonPathComplexSchema])
async def get(ty_code: str, issue_ts):
    """
        获取指定case的5种集合路径信息
    @param ty_code:
    @param issue_ts:
    @return:
    """
    try:
        #

        typhoon_dao = TyphoonDao()
        res = typhoon_dao.get_dist_grouppath_list(ty_code, issue_ts)
        return res

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
