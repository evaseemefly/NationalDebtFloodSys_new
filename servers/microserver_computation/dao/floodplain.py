from typing import List, Optional, Any

from sqlalchemy import distinct, select, text

from common.default import MS_UNIT
from common.enums import TyphoonGroupEnum, FloodAreaLevelEnum
from core.jobs import JobGenerateTyphoonPathFile, JobGenerateSurgeRasterPathFile
from dao.base import BaseDao
from models.models import TyphoonForecastGrouppath, TyphoonForecastRealdata, GeoPolygon, GeoFloodLevelPolygon
from schema.geo import GeoJSONPolygon, GeoPolygonSchema, GeoFloodPolygonSchema
from schema.task import TyGroupTaskSchema
from schema.typhoon import TyphoonPathComplexSchema, TyphoonDistGroupSchema, TyphoonPointSchema
from util.geo_util import convert_coordinate_order


class FloodPlainDao(BaseDao):
    def get_polygons_by_typhoon(self, ty_code: str, issue_ts: int):
        try:
            with self.session as session:
                """根据台风编号获取多边形"""
                """
                     (1305, 'FUNCTION sys_flood_nationaldebt.ST_AsEWKB does not exist')
                """
                # stmt = select(GeoPolygon).where(GeoPolygon.ty_code == ty_code,
                #                                 GeoPolygon.issue_time == issue_ts)
                # res = session.execute(stmt).all()
                # schemas = [GeoPolygonSchema(value=temp.value, ty_code=temp.ty_code, name=temp.name,
                #                             description=temp.description,
                #                             properties=temp.properties, issue_time=temp.issue_time, geom=temp.geom) for
                #            temp in res]
                #
                # return schemas
                # 方式2:
                # ERROR: geom
                #   不支持的几何数据类型: <class 'geoalchemy2.elements.WKTElement'> (type=value_error)

                # 使用原始 SQL 查询
                sql = text("""
                        SELECT id, value, ty_code, name, description, properties, 
                               ST_AsText(geom) as geom_wkt, 
                               gmt_create_time, gmt_update_time, issue_time
                        FROM geo_polygons
                        WHERE ty_code = :ty_code AND issue_time = :issue_ts
                    """)

                result = session.execute(sql, {"ty_code": ty_code, "issue_ts": issue_ts}).all()

                # 处理结果
                polygons = []
                for row in result:
                    # 创建 GeoPolygon 对象
                    polygon = GeoPolygon(
                        id=row.id,
                        value=row.value,
                        ty_code=row.ty_code,
                        name=row.name,
                        description=row.description,
                        properties=row.properties,
                        # 不设置 geom 字段，因为它需要特殊处理
                        gmt_create_time=row.gmt_create_time,
                        gmt_update_time=row.gmt_update_time,
                        issue_time=row.issue_time
                    )
                    # 将 WKT 转换为 GeoAlchemy2 可以处理的格式
                    from geoalchemy2.elements import WKTElement
                    # 'POLYGON((29.969271 122.081828,29.969271 122.082908,29.968191 122.082908,29.968191 122.081828,29.969271 122.081828))'
                    # 注意此处为 (lat,lng), 标准geo格式应为(lng,lat)，此处需要进行转换
                    # TODO:[*] 25-06-04
                    """
                        2 validation errors for GeoPolygonSchema
                        properties
                          str type expected (type=type_error.str)
                        geom
                          不支持的几何数据类型: <class 'geoalchemy2.elements.WKTElement'> (type=value_error)
                    """
                    source_geom_wkt = convert_coordinate_order(row.geom_wkt)
                    # {'coordinates': (((122.081828, 29.969271), (122.082908, 29.969271), (122.082908, 29.968191), (122.081828, 29.968191), (122.081828, 29.969271)),), 'type': 'Polygon'}
                    polygon.geom = WKTElement(source_geom_wkt, srid=4326)
                    polygons.append(polygon)
                # return polygons
                schemas = [GeoPolygonSchema(value=temp.value, ty_code=temp.ty_code, name=temp.name,
                                            description=temp.description,
                                            properties=temp.properties, issue_time=temp.issue_time, geom=temp.geom) for
                           temp in polygons]
                return schemas


        except Exception as ex:
            print(ex)
        pass

    def get_fooldlevel_polygons_by_ty(self, ty_code: str, issue_ts: int, flood_level: FloodAreaLevelEnum):
        try:
            with self.session as session:
                """根据台风编号获取多边形"""
                """
                     (1305, 'FUNCTION sys_flood_nationaldebt.ST_AsEWKB does not exist')
                """

                # 使用原始 SQL 查询
                sql = text("""
                        SELECT id, value, ty_code, name, description, properties, 
                               ST_AsText(geom) as geom_wkt, 
                               gmt_create_time, gmt_update_time, issue_time,flood_level
                        FROM geo_floodlevel_polygon
                        WHERE ty_code = :ty_code AND issue_time = :issue_ts AND flood_level = :flood_level
                    """)

                result = session.execute(sql, {"ty_code": ty_code, "issue_ts": issue_ts,
                                               "flood_level": flood_level.value}).all()

                # 处理结果
                polygons = []
                for row in result:
                    # 创建 GeoPolygon 对象
                    polygon = GeoFloodLevelPolygon(
                        id=row.id,
                        value=row.value,
                        ty_code=row.ty_code,
                        name=row.name,
                        description=row.description,
                        properties=row.properties,
                        # 不设置 geom 字段，因为它需要特殊处理
                        gmt_create_time=row.gmt_create_time,
                        gmt_update_time=row.gmt_update_time,
                        issue_time=row.issue_time,
                        flood_level=flood_level.value
                    )
                    # 将 WKT 转换为 GeoAlchemy2 可以处理的格式
                    from geoalchemy2.elements import WKTElement
                    # 'POLYGON((29.969271 122.081828,29.969271 122.082908,29.968191 122.082908,29.968191 122.081828,29.969271 122.081828))'
                    # 注意此处为 (lat,lng), 标准geo格式应为(lng,lat)，此处需要进行转换
                    # TODO:[*] 25-06-04
                    """
                        2 validation errors for GeoPolygonSchema
                        properties
                          str type expected (type=type_error.str)
                        geom
                          不支持的几何数据类型: <class 'geoalchemy2.elements.WKTElement'> (type=value_error)
                    """
                    source_geom_wkt = convert_coordinate_order(row.geom_wkt)
                    # {'coordinates': (((122.081828, 29.969271), (122.082908, 29.969271), (122.082908, 29.968191), (122.081828, 29.968191), (122.081828, 29.969271)),), 'type': 'Polygon'}
                    polygon.geom = WKTElement(source_geom_wkt, srid=4326)
                    polygons.append(polygon)
                # return polygons
                schemas = [GeoFloodPolygonSchema(value=temp.value, ty_code=temp.ty_code, name=temp.name,
                                                 description=temp.description,
                                                 properties=temp.properties, issue_time=temp.issue_time, geom=temp.geom,
                                                 flood_level=temp.flood_level)
                           for
                           temp in polygons]
                return schemas


        except Exception as ex:
            print(ex)
        pass
