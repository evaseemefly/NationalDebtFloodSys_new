from datetime import datetime, timedelta
from typing import List, Dict, Optional

import arrow
import requests
from lxml import etree
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from pydantic import ValidationError

from common.default import DEFAULT_CODE
from common.exceptions import NoExistTargetTyphoon
from core.jobs import JobGenerateTyphoonPathFile
from dao.jobs import TaskDao
from models.mid_models import TyDetailMidModel, TyPathMidModel
from schema.common import ResponseModel
from schema.task import TyGroupTaskSchema
from schema.typhoon import TyphoonPathSchema, TyphoonPathComplexSchema

app = APIRouter()


@app.post('/create/typhoon/path',
          summary="获取提交的作业请求")
async def get(params: TyphoonPathComplexSchema):
    """
        根据 ty_code 获取对应台风的路径(实况|预报)
    :param params:
    :return:
    """
    try:
        #
        print(f"Received typhoon path data: {params.dict()}")
        now_ts = arrow.utcnow().int_timestamp
        job_dao = TaskDao()
        job_dao.submit_task(1, params)
        # 测试——返回提交的数据集
        return ResponseModel(
            code=200,
            message="数据提交成功",
            data=params.dict()
        )

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@app.post('/create/typhoon/surge',
          summary="获取提交的作业请求")
async def get(params: TyphoonPathComplexSchema):
    """
        根据 ty_code 获取对应台风的路径(实况|预报)
    :param params:
    :return:
    """
    try:
        #
        print(f"Received typhoon path data: {params.dict()}")
        now_ts = arrow.utcnow().int_timestamp
        job_dao = TaskDao()
        job_dao.submit_surge_task(1, params)
        # 测试——返回提交的数据集
        return ResponseModel(
            code=200,
            message="数据提交成功",
            data=params.dict()
        )

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@app.get('/get/task/list', response_model=List[TyGroupTaskSchema],
         summary="爬取中央气象台的台风路径")
def get(code: str):
    """
        获取台风对应的任务列表
        TODO:[*] 25-05-13 此处需要修改 url 为 group/list 目前不从 task 获取对应的 group
    @param code:
    @return:
    """
    job_dao = TaskDao()
    # issue_ts: int = 1747125125
    res = job_dao.get_task_list(code)
    return res


@app.get('/get/grouppath/list', response_model=List[TyGroupTaskSchema],
         summary="爬取中央气象台的台风路径")
def get(code: str, issue_ts: int):
    """
        根据 台风编号获取集合路径的 发布时间戳以及code等集合
    @param code:
    @param issue_ts:
    @return:
    """
