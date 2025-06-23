import arrow
import pandas as pd
import numpy as np
import pathlib
import json

from geoalchemy2 import WKTElement
from shapely import wkt as shapely_wkt
from shapely.ops import transform
import shapely.wkb
from sqlalchemy.sql import text
import geopandas as gpd
from commons.enums import TYGroupTypeEnum
from db_factory import session_yield_scope
from models import Station, StationForecastRealdataModel, GeoPolygon
from schemas import StationSurgeFileSchema
from util.file_util import FileExplorer
from util.geo_util import swap_xy

READPATH: str = r'E:\01data\99test\flood_geo\geojson_file_center_sparse.json'


def load_geojson_to_mysql(geojson_path, ty_code, issue_ts: int, value_name: str):
    """
    读取本地 GeoJSON 文件并将其写入 MySQL 数据库

    参数:
        geojson_path: GeoJSON 文件的路径
        db_connection_string: 数据库连接字符串，例如 'mysql+pymysql://username:password@localhost/dbname'
        ty_code: 类型代码，用于标识这批数据的类型
    """
    with session_yield_scope() as session:

        try:
            # 使用 GeoPandas 读取 GeoJSON 文件
            gdf = gpd.read_file(geojson_path)
            # 遍历 GeoJSON 中的每个特征
            for idx, row in gdf.iterrows():
                """
                    最大淹没深度(cm) 91 
                    geometry POLYGON ((121.925228 29.322351, 121.926308 29.322351, 121.926308 29.321271, 121.925228 29.321271, 121.925228 29.322351))
                    
                    POLYGON ((122.081828 29.969271, 122.082908 29.969271, 122.082908 29.968191, 122.081828 29.968191, 122.081828 29.969271))

                """
                # 获取属性
                properties = row.drop('geometry').to_dict()

                # 提取名称和描述
                name = properties.get('name', f"Feature_{idx}")
                value = properties.get(value_name)
                # TODO:[-] 25-06-03 若不存在 description的话赋值为value_name
                description = properties.get('description', value_name)

                # 将剩余的 properties 存储为 JSON 字符串
                properties_json = json.dumps(properties)

                # 获取几何对象的 WKT (Well-Known Text) 表示
                # 解析 WKT 字符串
                geom = shapely_wkt.loads(row.geometry.wkt)
                swapped_geom = transform(swap_xy, geom)
                swapped_wkt = swapped_geom.wkt
                # 打印调试信息
                """
                    原始 WKT: POLYGON ((122.081828 29.969271, 122.082908 29.969271, 122.082908 29.968191, 122.081828 29.968191, 122.081828 29.969271))
                    
                    交换后 WKT: POLYGON ((29.969271 122.081828, 29.969271 122.082908, 29.968191 122.082908, 29.968191 122.081828, 29.969271 122.081828))
                """
                print(f"原始 WKT: {geom.wkt}")
                print(f"交换后 WKT: {swapped_wkt}")
                # wkt = row.geometry.wkt

                # 创建 WKTElement 对象，指定 SRID
                # 使用 WKTElement 而不是直接使用 WKT 字符串
                wkt_element = WKTElement(swapped_wkt, srid=4326)

                # 使用 SQL 表达式插入数据
                # 使用model的形式
                # Only one Column may be marked autoincrement=True, found both value and id.
                # (MySQLdb._exceptions.OperationalError) (1305, 'FUNCTION sys_flood_nationaldebt.ST_GeomFromEWKT does not exist')
                # 解决方案: wkt_element = WKTElement(wkt, srid=4326)
                # (3617, 'Latitude 122.081828 is out of range in function st_geomfromtext. It must be within [-90.000000, 90.000000].')

                # (1292, "Incorrect datetime value: '1747125125' for column 'issue_time' at row 1")
                temp_model = GeoPolygon(value=value, ty_code=ty_code, name=name, description=description,
                                        properties=properties_json, geom=wkt_element, issue_time=issue_ts)
                session.add(temp_model)
                # session.commit()
                # stmt = text("""
                #     INSERT INTO geo_polygons (ty_code,value,  name, description, properties, geom,issue_time)
                #     VALUES (:ty_code,:value, :description, :properties, ST_GeomFromText(:wkt, 4326),:issue_ts)
                # """)
                #
                # session.execute(stmt, {
                #     'ty_code': ty_code,
                #     'value': value,
                #     'name': name,
                #     'description': description,
                #     'properties': properties_json,
                #     'wkt': wkt,
                #     'issue_ts': issue_ts
                # })

                # 提交事务

            session.commit()
            print(f"成功导入 {len(gdf)} 个多边形到数据库")
        except Exception as e:
            """
                [SQL: 
                    INSERT INTO geo_polygons (ty_code,value,  name, description, properties, geom,issue_time) 
                    VALUES (%s,%s, %s, %s, ST_GeomFromText(%s, 4326),%s)
                ]
                [parameters: ('2106', 91, None, '{"\\u6700\\u5927\\u6df9\\u6ca1\\u6df1\\u5ea6(cm)": 91}', 'POLYGON ((122.081828 29.969271, 122.082908 29.969271, 122.082908 29.968191, 122.081828 29.968191, 122.081828 29.969271))', 1747125125)]
            """
            session.rollback()
            print(f"导入过程中发生错误: {e}")
            raise
        finally:
            session.close()


def main():
    ty_code = "2106"  # 台风编号
    issue_ts: int = 1747125125

    value_name: str = ''
    # 执行导入
    load_geojson_to_mysql(READPATH, ty_code, issue_ts, '最大淹没深度(cm)')


if __name__ == "__main__":
    main()
