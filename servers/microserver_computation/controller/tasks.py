from datetime import datetime, timedelta
from typing import List, Dict, Optional

import arrow
import requests
from lxml import etree
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from pydantic import ValidationError

from common.default import DEFAULT_CODE
from common.exceptions import NoExistTargetTyphoon
from config.celery_config import celery_app
from core.jobs import JobGenerateTyphoonPathFile
from dao.jobs import TaskDao, execute_ty_model
from models.mid_models import TyDetailMidModel, TyPathMidModel
from schema.common import ResponseModel
from schema.task import TyGroupTaskSchema
from schema.typhoon import TyphoonPathSchema, TyphoonPathComplexSchema, TyphoonPathComplexDetailSchema

app = APIRouter()


@app.post('/create/typhoon/path',
          summary="获取提交的作业请求")
async def post(params: TyphoonPathComplexDetailSchema):
    """
        根据 ty_code 获取对应台风的路径(实况|预报)
    :param params:
    :return:
    """
    try:
        #
        print(f"Received typhoon path data: {params.dict()}")
        now_ts = arrow.utcnow().int_timestamp
        # job_dao = TaskDao()
        # job_dao.submit_task(1, params)
        # 环境部署于docker容器中
        # ('Error 99 connecting to localhost:6379. Cannot assign requested address.',)
        try:
            id = execute_ty_model(params)
        except Exception as e:
            print(f'当前提交作业执行时发生错误:ERROR_CODE:{e.args}')
        return {
            "task_id": id,
            "message": "任务已提交，正在后台执行..."
        }
        # 测试——返回提交的数据集
        return ResponseModel(
            code=200,
            message="数据提交成功",
            data=params.dict()
        )

    # ImportError('Missing redis library (pip install redis)')
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


@app.post('/create/agent',
          summary="创建任务代理")
async def post(code: str):
    """
        根据 ty_code 获取对应台风的路径(实况|预报)
    :param params:
    :return:
    """
    try:
        #
        print(code)
        # TODO:[*] 25-07-03
        # 'NoneType' object has no attribute 'Redis'
        task = celery_app.send_task(
            'ty_group',  # 必须与 Worker 中定义的 task name 一致
            # args=[code]
        )
        return {
            "task_id": task.id,
            "message": "任务已提交，正在后台执行..."
        }

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
