from http.client import HTTPException
from typing import List

from fastapi import APIRouter

from common.enums import FloodAreaLevelEnum
from dao.floodplain import FloodPlainDao
from schema.geo import GeoPolygonSchema, GeoFloodPolygonSchema, FloodPolygonFeatureCollectionSchema

app = APIRouter()


@app.get('/flood/polygons',
         summary="根据group获取对应的增水场url", response_model=List[GeoPolygonSchema])
async def get(ty_code: str, issue_ts: int):
    """
        根据 ty_code 获取对应台风的路径(实况|预报)
    :param params:
    :return:
    """
    try:
        """集合路径对应的类型"""
        flood_dao = FloodPlainDao()
        schemas = flood_dao.get_polygons_by_typhoon(ty_code, issue_ts)
        return schemas

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@app.get('/flood/geotiff',
         summary="获取漫滩淹没范围geotiff", response_model=str)
async def get(ty_code: str, issue_ts: int):
    """
        根据 ty_code 获取对应台风的路径(实况|预报)
    :param params:
    :return:
    """
    try:
        # TODO:[*] 25-06-04 此部分暂时写死
        geotiff_url: str = ''
        return geotiff_url

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@app.get('/flood/grid/level/polygon',
         summary="根据淹没的等级(level)获取对应的淹没范围geotiff",
         response_model=FloodPolygonFeatureCollectionSchema)
async def get(ty_code: str, issue_ts: int, gt_level_val: int):
    try:
        gt_level: FloodAreaLevelEnum = FloodAreaLevelEnum(gt_level_val)
        # TODO:[*]
        dao = FloodPlainDao()
        floodPolygonSchema = dao.get_fooldlevel_polygons_by_ty(ty_code, issue_ts, gt_level)
        schemas = FloodPolygonFeatureCollectionSchema(type='FeatureCollection', features=floodPolygonSchema)
        return schemas

    except Exception as e:
        # 异常处理
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
