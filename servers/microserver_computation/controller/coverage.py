from datetime import datetime, timedelta
from typing import List, Dict, Optional

import arrow
import requests
from lxml import etree
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from pydantic import ValidationError

from common.default import DEFAULT_CODE
from common.enums import TyphoonGroupEnum, FloodAreaLevelEnum
from common.exceptions import NoExistTargetTyphoon
from core.jobs import JobGenerateTyphoonPathFile
from dao.coverage import CoverageDao
from dao.jobs import TaskDao
from dao.typhoon import TyphoonDao
from models.mid_models import TyDetailMidModel, TyPathMidModel
from schema.common import ResponseModel
from schema.task import TyGroupTaskSchema
from schema.typhoon import TyphoonPathSchema, TyphoonPathComplexSchema, TyphoonDistGroupSchema

app = APIRouter()


def get_flood_area_tif_name(val: FloodAreaLevelEnum) -> str:
    """
        TODO:[*] 25-06-10
        根据 淹没等级获取对应的geotiff地址(静态)
    @param val:
    @return:
    """
    switch_dict = {FloodAreaLevelEnum.GTE100: 'output_processed_gt100.tif',
                   FloodAreaLevelEnum.GTE150: 'output_processed_gt150.tif',
                   FloodAreaLevelEnum.GTE200: 'output_processed_gt200.tif', }
    return switch_dict.get(val)


# @app.get('/surge/max/url',
#          summary="根据group获取对应的增水场url", response_model=str)
# async def get(ty_code: str, task_id: int, group: TyphoonGroupEnum = TyphoonGroupEnum.GROUP_CENTER):
#     """
#         根据 ty_code 获取对应台风的路径(实况|预报)
#     :param params:
#     :return:
#     """
#     try:
#         # 将集合路径 type转换为对应 enmu
#         group_type_enum: TyphoonGroupEnum = TyphoonGroupEnum(group)
#         """集合路径对应的类型"""
#         coverage_dao = CoverageDao()
#         res = coverage_dao.get_tif_file_url(ty_code, task_id, group_type_enum)
#         return res
#
#     except Exception as e:
#         # 异常处理
#         raise HTTPException(
#             status_code=500,
#             detail=str(e)
#         )

@app.get('/surge/max/url',
         summary="根据group获取对应的增水场url", response_model=str)
async def get(ty_code: str, issue_ts: int, group: TyphoonGroupEnum = TyphoonGroupEnum.GROUP_CENTER):
    """
        根据 ty_code 获取对应台风的路径(实况|预报)
    :param params:
    :return:
    """
    try:
        # 将集合路径 type转换为对应 enmu
        group_type_enum: TyphoonGroupEnum = TyphoonGroupEnum(group)
        """集合路径对应的类型"""
        coverage_dao = CoverageDao()
        res = coverage_dao.get_tif_file_url(ty_code, issue_ts, group_type_enum)
        return res

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@app.get('/flood/grid/url',
         summary="漫滩预报淹没范围", response_model=str)
async def get(ty_code: str, issue_ts: int):
    try:
        # TODO:[*]
        # geotiff_url: str = 'http://localhost:82/images/TYPHOON\data/user1/flood_path/2025/2106/1746777768768/output_processed.tif'
        geotiff_url: str = 'http://localhost:82/images/TYPHOON\data/user1/flood_path/2025/2106/1746777768768/ningbo_test_250611_masked.tif'
        # geotiff_url: str = 'http://localhost:82/images/TYPHOON\data/user1/flood_path/2025/2106/1746777768768/output.tif'
        return geotiff_url

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@app.get('/flood/grid/level/url',
         summary="根据淹没的等级(level)获取对应的淹没范围geotiff", response_model=str)
async def get(ty_code: str, issue_ts: int, gt_level_val: int):
    try:
        gt_level: FloodAreaLevelEnum = FloodAreaLevelEnum(gt_level_val)
        # TODO:[*]
        geotiff_url: str = 'http://localhost:82/images/TYPHOON\data/user1/flood_path/2025/2106/1746777768768/'
        file_name: str = get_flood_area_tif_name(gt_level)
        geotiff_url = geotiff_url + file_name
        return geotiff_url

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
